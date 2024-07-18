/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Result;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.Listeners;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.db.TypeSizes.SHORT_SIZE;

public class SavedCommand
{
    public static final ValueSerializer<JournalKey, Object> serializer = new SavedCommandSerializer();

    // This enum is order-dependent
    private enum HasFields
    {
        TXN_ID,
        EXECUTE_AT,
        SAVE_STATUS,
        DURABILITY,
        ACCEPTED,
        PROMISED,
        ROUTE,
        PARTIAL_TXN,
        PARTIAL_DEPS,
        ADDITIONAL_KEYS,
        WAITING_ON,
        WRITES,
        LISTENERS
    }

    public final TxnId txnId;

    public final Timestamp executeAt;
    public final SaveStatus saveStatus;
    public final Status.Durability durability;

    public final Ballot acceptedOrCommitted;
    public final Ballot promised;

    public final Route<?> route;
    public final PartialTxn partialTxn;
    public final PartialDeps partialDeps;
    public final Seekables<?, ?> additionalKeysOrRanges;

    public final Writes writes;
    public final Listeners.Immutable<Command.DurableAndIdempotentListener> listeners;

    public SavedCommand(TxnId txnId,
                        Timestamp executeAt,
                        SaveStatus saveStatus,
                        Status.Durability durability,

                        Ballot acceptedOrCommitted,
                        Ballot promised,

                        Route<?> route,
                        PartialTxn partialTxn,
                        PartialDeps partialDeps,
                        Seekables<?, ?> additionalKeysOrRanges,

                        Writes writes,
                        Listeners.Immutable<Command.DurableAndIdempotentListener> listeners)
    {
        this.txnId = txnId;
        this.executeAt = executeAt;
        this.saveStatus = saveStatus;
        this.durability = durability;

        this.acceptedOrCommitted = acceptedOrCommitted;
        this.promised = promised;

        this.route = route;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.additionalKeysOrRanges = additionalKeysOrRanges;

        this.writes = writes;
        this.listeners = listeners;
    }

    public static SavedDiff diff(Command before, Command after)
    {
        if (before == after)
            return null;

        // TODO: we do not need to save `waitingOn` _every_ time.
        Command.WaitingOn waitingOn = getWaitingOn(after);
        return new SavedDiff(after.txnId(),
                             ifNotEqual(before, after, Command::executeAt, true),
                             ifNotEqual(before, after, Command::saveStatus, false),
                             ifNotEqual(before, after, Command::durability, false),

                             ifNotEqual(before, after, Command::acceptedOrCommitted, false),
                             ifNotEqual(before, after, Command::promised, false),

                             ifNotEqual(before, after, Command::route, true),
                             ifNotEqual(before, after, Command::partialTxn, false),
                             ifNotEqual(before, after, Command::partialDeps, false),
                             ifNotEqual(before, after, Command::additionalKeysOrRanges, false),

                             waitingOn,
                             ifNotEqual(before, after, Command::writes, false),
                             ifNotEqual(before, after, Command::durableListeners, true));
    }

    static Command reconstructFromDiff(List<LoadedDiff> diffs)
    {
        return reconstructFromDiff(diffs, CommandSerializers.APPLIED);
    }

    /**
     * @param result is exposed because we are _not_ persisting result, since during loading or replay
     *               we do not expect we will have to send a result to the client, and data results
     *               can potentially contain a large number of entries, so it's best if they are not
     *               written into the log.
     */
    @VisibleForTesting
    static Command reconstructFromDiff(List<LoadedDiff> diffs, Result result)
    {
        TxnId txnId = null;

        Timestamp executeAt = null;
        SaveStatus saveStatus = null;
        Status.Durability durability = null;

        Ballot acceptedOrCommitted = Ballot.ZERO;
        Ballot promised = null;

        Route<?> route = null;
        PartialTxn partialTxn = null;
        PartialDeps partialDeps = null;
        Seekables<?, ?> additionalKeysOrRanges = null;

        WaitingOnProvider waitingOnProvider = null;
        Writes writes = null;
        Listeners.Immutable listeners = null;

        for (LoadedDiff diff : diffs)
        {
            if (diff.txnId != null)
                txnId = diff.txnId;
            if (diff.executeAt != null)
                executeAt = diff.executeAt;
            if (diff.saveStatus != null)
                saveStatus = diff.saveStatus;
            if (diff.durability != null)
                durability = diff.durability;

            if (diff.acceptedOrCommitted != null)
                acceptedOrCommitted = diff.acceptedOrCommitted;
            if (diff.promised != null)
                promised = diff.promised;

            if (diff.route != null)
                route = diff.route;
            if (diff.partialTxn != null)
                partialTxn = diff.partialTxn;
            if (diff.partialDeps != null)
                partialDeps = diff.partialDeps;
            if (diff.additionalKeysOrRanges != null)
                additionalKeysOrRanges = diff.additionalKeysOrRanges;

            if (diff.waitingOn != null)
                waitingOnProvider = diff.waitingOn;
            if (diff.writes != null)
                writes = diff.writes;
            if (diff.listeners != null)
                listeners = diff.listeners;
        }

        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        if (partialTxn != null)
            attrs.partialTxn(partialTxn);
        if (durability != null)
            attrs.durability(durability);
        if (route != null)
            attrs.route(route);
        if (partialDeps != null &&
            (saveStatus.known.deps != Status.KnownDeps.NoDeps &&
             saveStatus.known.deps != Status.KnownDeps.DepsErased &&
             saveStatus.known.deps != Status.KnownDeps.DepsUnknown))
            attrs.partialDeps(partialDeps);
        if (additionalKeysOrRanges != null)
            attrs.additionalKeysOrRanges(additionalKeysOrRanges);
        if (listeners != null && !listeners.isEmpty())
            attrs.setListeners(listeners);

        Command.WaitingOn waitingOn = null;
        if (waitingOnProvider != null)
            waitingOn = waitingOnProvider.provide(txnId, partialDeps);

        Invariants.checkState(saveStatus != null,
                              "Save status is null after applying %s", diffs);
        switch (saveStatus.status)
        {
            case NotDefined:
                return saveStatus == SaveStatus.Uninitialised ? Command.NotDefined.uninitialised(attrs.txnId())
                                                              : Command.NotDefined.notDefined(attrs, promised);
            case PreAccepted:
                return Command.PreAccepted.preAccepted(attrs, executeAt, promised);
            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                return Command.Accepted.accepted(attrs, saveStatus, executeAt, promised, acceptedOrCommitted);
            case Committed:
            case Stable:
                return Command.Committed.committed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn);
            case PreApplied:
            case Applied:
                return Command.Executed.executed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn, writes, result);
            case Truncated:
            case Invalidated:
            default:
                throw new IllegalStateException();
        }
    }

    // TODO (required): this convert function was added only because AsyncOperationTest was failing without it; maybe after switching to loading from the log we can just pass l and r directly or remove != null checks.
    private static <OBJ, VAL> VAL ifNotEqual(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, boolean allowClassMismatch)
    {
        VAL l = null;
        VAL r = null;
        if (lo != null) l = convert.apply(lo);
        if (ro != null) r = convert.apply(ro);

        if (l == r)
            return null;
        if (l == null || r == null)
            return r;
        assert allowClassMismatch || l.getClass() == r.getClass() : String.format("%s != %s", l.getClass(), r.getClass());

        if (l.equals(r))
            return null;

        return r;
    }

    static Command.WaitingOn getWaitingOn(Command command)
    {
        if (command instanceof Command.Committed)
            return command.asCommitted().waitingOn();

        return null;
    }

    public static class SavedDiff extends SavedCommand
    {
        public final Command.WaitingOn waitingOn;

        public SavedDiff(TxnId txnId,
                         Timestamp executeAt,
                         SaveStatus saveStatus,
                         Status.Durability durability,

                         Ballot acceptedOrCommitted,
                         Ballot promised,

                         Route<?> route,
                         PartialTxn partialTxn,
                         PartialDeps partialDeps,
                         Seekables<?, ?> additionalKeysOrRanges,

                         Command.WaitingOn waitingOn,
                         Writes writes,
                         Listeners.Immutable<Command.DurableAndIdempotentListener> listeners)
        {
            super(txnId, executeAt, saveStatus, durability, acceptedOrCommitted, promised, route, partialTxn, partialDeps, additionalKeysOrRanges, writes, listeners);
            this.waitingOn = waitingOn;
        }

        @Override
        public String toString()
        {
            return "SavedDiff{" +
                   " txnId=" + txnId +
                   ", executeAt=" + executeAt +
                   ", saveStatus=" + saveStatus +
                   ", durability=" + durability +
                   ", acceptedOrCommitted=" + acceptedOrCommitted +
                   ", promised=" + promised +
                   ", route=" + route +
                   ", partialTxn=" + partialTxn +
                   ", partialDeps=" + partialDeps +
                   ", writes=" + writes +
                   ", waitingOn=" + waitingOn +
                   '}';
        }
    }

    public static class LoadedDiff extends SavedCommand
    {
        public final WaitingOnProvider waitingOn;

        public LoadedDiff(TxnId txnId,
                          Timestamp executeAt,
                          SaveStatus saveStatus,
                          Status.Durability durability,

                          Ballot acceptedOrCommitted,
                          Ballot promised,

                          Route<?> route,
                          PartialTxn partialTxn,
                          PartialDeps partialDeps,
                          Seekables<?, ?> additionalKeysOrRanges,

                          WaitingOnProvider waitingOn,
                          Writes writes,
                          Listeners.Immutable<Command.DurableAndIdempotentListener> listeners)
        {
            super(txnId, executeAt, saveStatus, durability, acceptedOrCommitted, promised, route, partialTxn, partialDeps, additionalKeysOrRanges, writes, listeners);
            this.waitingOn = waitingOn;
        }

        public String toString()
        {
            return "LoadedDiff{" +
                   "waitingOn=" + waitingOn +
                   '}';
        }
    }
    
    final static class SavedCommandSerializer implements ValueSerializer<JournalKey, Object>
    {
        @Override
        public int serializedSize(JournalKey key, Object value, int userVersion)
        {
            SavedDiff diff = (SavedDiff) value;
            long size = 0;
            size += SHORT_SIZE; // flags

            if (diff.txnId != null)
                size += CommandSerializers.txnId.serializedSize(diff.txnId, userVersion);
            if (diff.executeAt != null)
                size += CommandSerializers.timestamp.serializedSize(diff.executeAt, userVersion);
            if (diff.saveStatus != null)
                size += Integer.BYTES;
            if (diff.durability != null)
                size += Integer.BYTES;

            if (diff.acceptedOrCommitted != null)
                size += CommandSerializers.ballot.serializedSize(diff.acceptedOrCommitted, userVersion);
            if (diff.promised != null)
                size += CommandSerializers.ballot.serializedSize(diff.promised, userVersion);

            if (diff.route != null)
                size += AccordKeyspace.LocalVersionedSerializers.route.serializedSize(diff.route);
            if (diff.partialTxn != null)
                CommandSerializers.partialTxn.serializedSize(diff.partialTxn, userVersion);
            if (diff.partialDeps != null)
                DepsSerializer.partialDeps.serializedSize(diff.partialDeps, userVersion);
            if (diff.additionalKeysOrRanges != null)
                KeySerializers.seekables.serializedSize(diff.additionalKeysOrRanges, userVersion);

            if (diff.waitingOn != null)
            {
                size += Integer.BYTES;
                size += WaitingOnSerializer.serializedSize(diff.waitingOn);
            }

            if (diff.writes != null)
                CommandSerializers.writes.serializedSize(diff.writes, userVersion);

            if (diff.listeners != null && !diff.listeners.isEmpty())
            {
                size += Byte.BYTES;
                for (Command.DurableAndIdempotentListener listener : diff.listeners)
                    size += AccordKeyspace.LocalVersionedSerializers.listeners.serializedSize(listener);
            }
            return (int) size;
        }

        @Override
        public void serialize(JournalKey key, Object value, DataOutputPlus out, int userVersion) throws IOException
        {
            SavedDiff diff = (SavedDiff) value;
            int flags = getFlags(diff);

            out.writeShort(flags);

            if (diff.txnId != null)
                CommandSerializers.txnId.serialize(diff.txnId, out, userVersion);
            if (diff.executeAt != null)
                CommandSerializers.timestamp.serialize(diff.executeAt, out, userVersion);
            if (diff.saveStatus != null)
                out.writeInt(diff.saveStatus.ordinal());
            if (diff.durability != null)
                out.writeInt(diff.durability.ordinal());

            if (diff.acceptedOrCommitted != null)
                CommandSerializers.ballot.serialize(diff.acceptedOrCommitted, out, userVersion);
            if (diff.promised != null)
                CommandSerializers.ballot.serialize(diff.promised, out, userVersion);

            if (diff.route != null)
                AccordKeyspace.LocalVersionedSerializers.route.serialize(diff.route, out); // TODO (required): user version
            if (diff.partialTxn != null)
                CommandSerializers.partialTxn.serialize(diff.partialTxn, out, userVersion);
            if (diff.partialDeps != null)
                DepsSerializer.partialDeps.serialize(diff.partialDeps, out, userVersion);
            if (diff.additionalKeysOrRanges != null)
                KeySerializers.seekables.serialize(diff.additionalKeysOrRanges, out, userVersion);

            if (diff.waitingOn != null)
            {
                long size = WaitingOnSerializer.serializedSize(diff.waitingOn);
                ByteBuffer serialized = WaitingOnSerializer.serialize(diff.txnId, diff.waitingOn);
                out.writeInt((int) size);
                out.write(serialized);
            }

            if (diff.writes != null)
                CommandSerializers.writes.serialize(diff.writes, out, userVersion);

            if (diff.listeners != null && !diff.listeners.isEmpty())
            {
                out.writeByte(diff.listeners.size());
                for (Command.DurableAndIdempotentListener listener : diff.listeners)
                    AccordKeyspace.LocalVersionedSerializers.listeners.serialize(listener, out);
            }

        }

        private static int getFlags(SavedDiff diff)
        {
            int flags = 0;

            if (diff.txnId != null)
                flags = setBit(flags, HasFields.TXN_ID.ordinal());
            if (diff.executeAt != null)
                flags = setBit(flags, HasFields.EXECUTE_AT.ordinal());
            if (diff.saveStatus != null)
                flags = setBit(flags, HasFields.SAVE_STATUS.ordinal());
            if (diff.durability != null)
                flags = setBit(flags, HasFields.DURABILITY.ordinal());

            if (diff.acceptedOrCommitted != null)
                flags = setBit(flags, HasFields.ACCEPTED.ordinal());
            if (diff.promised != null)
                flags = setBit(flags, HasFields.PROMISED.ordinal());

            if (diff.route != null)
                flags = setBit(flags, HasFields.ROUTE.ordinal());
            if (diff.partialTxn != null)
                flags = setBit(flags, HasFields.PARTIAL_TXN.ordinal());
            if (diff.partialDeps != null)
                flags = setBit(flags, HasFields.PARTIAL_DEPS.ordinal());
            if (diff.additionalKeysOrRanges != null)
                flags = setBit(flags, HasFields.ADDITIONAL_KEYS.ordinal());

            if (diff.waitingOn != null)
                flags = setBit(flags, HasFields.WAITING_ON.ordinal());
            if (diff.writes != null)
                flags = setBit(flags, HasFields.WRITES.ordinal());
            if (diff.listeners != null && !diff.listeners.isEmpty())
                flags = setBit(flags, HasFields.LISTENERS.ordinal());
            return flags;
        }

        @Override
        public Object deserialize(JournalKey key, DataInputPlus in, int userVersion) throws IOException
        {
            int flags = in.readShort();

            TxnId txnId = null;
            Timestamp executedAt = null;
            SaveStatus saveStatus = null;
            Status.Durability durability = null;

            Ballot acceptedOrCommitted = null;
            Ballot promised = null;
            Route<?> route = null;

            PartialTxn partialTxn = null;
            PartialDeps partialDeps = null;
            Seekables<?, ?> additionalKeysOrRanges = null;

            WaitingOnProvider waitingOn = (txn, deps) -> null;
            Writes writes = null;
            Listeners.Immutable listeners = null;

            if (isSet(flags, HasFields.TXN_ID.ordinal()))
                txnId = CommandSerializers.txnId.deserialize(in, userVersion);
            if (isSet(flags, HasFields.EXECUTE_AT.ordinal()))
                executedAt = CommandSerializers.timestamp.deserialize(in, userVersion);
            if (isSet(flags, HasFields.SAVE_STATUS.ordinal()))
                saveStatus = SaveStatus.values()[in.readInt()];
            if (isSet(flags, HasFields.DURABILITY.ordinal()))
                durability = Status.Durability.values()[in.readInt()];

            if (isSet(flags, HasFields.ACCEPTED.ordinal()))
                acceptedOrCommitted = CommandSerializers.ballot.deserialize(in, userVersion);
            if (isSet(flags, HasFields.PROMISED.ordinal()))
                promised = CommandSerializers.ballot.deserialize(in, userVersion);

            if (isSet(flags, HasFields.ROUTE.ordinal()))
                route = AccordKeyspace.LocalVersionedSerializers.route.deserialize(in);
            if (isSet(flags, HasFields.PARTIAL_TXN.ordinal()))
                partialTxn = CommandSerializers.partialTxn.deserialize(in, userVersion);
            if (isSet(flags, HasFields.PARTIAL_DEPS.ordinal()))
                partialDeps = DepsSerializer.partialDeps.deserialize(in, userVersion);
            if (isSet(flags, HasFields.ADDITIONAL_KEYS.ordinal()))
                additionalKeysOrRanges = KeySerializers.seekables.deserialize(in, userVersion);

            if (isSet(flags, HasFields.WAITING_ON.ordinal()))
            {
                int size = in.readInt();
                byte[] bytes = new byte[size];
                in.readFully(bytes);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                waitingOn = (localTxnId, deps) -> {
                    try
                    {
                        return WaitingOnSerializer.deserialize(localTxnId, deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps, buffer);
                    }
                    catch (IOException e)
                    {
                        throw Throwables.unchecked(e);
                    }
                };
            }
            if (isSet(flags, HasFields.WRITES.ordinal()))
                writes = CommandSerializers.writes.deserialize(in, userVersion);

            if (isSet(flags, HasFields.LISTENERS.ordinal()))
            {
                Listeners builder = Listeners.Immutable.EMPTY.mutable();
                int cnt = in.readByte();
                for (int i = 0; i < cnt; i++)
                    builder.add(AccordKeyspace.LocalVersionedSerializers.listeners.deserialize(in));
                listeners = new Listeners.Immutable(builder);
            }

            return new LoadedDiff(txnId,
                                  executedAt,
                                  saveStatus,
                                  durability,

                                  acceptedOrCommitted,
                                  promised,

                                  route,
                                  partialTxn,
                                  partialDeps,
                                  additionalKeysOrRanges,

                                  waitingOn,
                                  writes,
                                  listeners);
        }
    }

    static int setBit(int value, int bit)
    {
        return value | (1 << bit);
    }

    static boolean isSet(int value, int bit)
    {
        return (value & (1 << bit)) != 0;
    }

    public interface WaitingOnProvider
    {
        Command.WaitingOn provide(TxnId txnId, PartialDeps deps);
    }
}