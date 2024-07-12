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

import java.util.List;

import accord.api.Result;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

import accord.utils.Invariants;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializer;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.db.TypeSizes.SHORT_SIZE;

public class SavedCommand
{
    public static final ValueSerializer<AccordJournal.Key, Object> serializer = new SavedCommandSerializer();

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
        WAITING_ON,
        WRITES
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

    public final Writes writes;

    public SavedCommand(TxnId txnId,
                        Timestamp executeAt,
                        SaveStatus saveStatus,
                        Status.Durability durability,

                        Ballot acceptedOrCommitted,
                        Ballot promised,

                        Route<?> route,
                        PartialTxn partialTxn,
                        PartialDeps partialDeps,

                        Writes writes)
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

        this.writes = writes;
    }

    static SavedDiff diff(Command before, Command after)
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
                             waitingOn,
                             ifNotEqual(before, after, Command::writes, false));
    }

    static Command reconstructFromDiff(List<LoadedDiff> diffs, SaveStatus cap)
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

        WaitingOnProvider waitingOnProvider = null;

        Writes writes = null;

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

            if (diff.waitingOn != null)
                waitingOnProvider = diff.waitingOn;

            if (diff.writes != null)
                writes = diff.writes;
        }

        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        if (partialTxn != null)
            attrs.partialTxn(partialTxn);
        if (durability != null)
            attrs.durability(durability);
        if (route != null)
            attrs.route(route);
        if (partialDeps != null)
            attrs.partialDeps(partialDeps);

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
                Result result = CommandSerializers.APPLIED;
                return Command.Executed.executed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn, writes, result);
            case Truncated:
            case Invalidated:
            default:
                throw new IllegalStateException();
        }
    }

    // TODO: this convert function was added only because AsyncOperationTest was failing without it; maybe after switching to loading from the log we can just pass l and r directly or remove != null checks.
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
                         Command.WaitingOn waitingOn,

                         Writes writes)
        {
            super(txnId, executeAt, saveStatus, durability, acceptedOrCommitted, promised, route, partialTxn, partialDeps, writes);
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
                          WaitingOnProvider waitingOn,

                          Writes writes)
        {
            super(txnId, executeAt, saveStatus, durability, acceptedOrCommitted, promised, route, partialTxn, partialDeps, writes);
            this.waitingOn = waitingOn;
        }

        @Override
        public String toString()
        {
            return "LoadedDiff{" +
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
    
    final static class SavedCommandSerializer implements ValueSerializer<AccordJournal.Key, Object>
    {
        @Override
        public int serializedSize(AccordJournal.Key key, Object value, int userVersion)
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

            if (diff.waitingOn != null)
            {
                size += Integer.BYTES;
                size += WaitingOnSerializer.serializedSize(diff.txnId, diff.waitingOn);
            }

            if (diff.writes != null)
                CommandSerializers.writes.serializedSize(diff.writes, userVersion);

            return (int) size;
        }

        @Override
        public void serialize(AccordJournal.Key key, Object value, DataOutputPlus out, int userVersion) throws IOException
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
                AccordKeyspace.LocalVersionedSerializers.route.serialize(diff.route, out); // TODO: user version

            if (diff.partialTxn != null)
                CommandSerializers.partialTxn.serialize(diff.partialTxn, out, userVersion);

            if (diff.partialDeps != null)
                DepsSerializer.partialDeps.serialize(diff.partialDeps, out, userVersion);

            if (diff.waitingOn != null)
            {
                long size = WaitingOnSerializer.serializedSize(diff.txnId, diff.waitingOn);
                ByteBuffer serialized = WaitingOnSerializer.serialize(diff.txnId, diff.waitingOn);
                out.writeInt((int) size);
                out.write(serialized);
            }

            if (diff.writes != null)
                CommandSerializers.writes.serialize(diff.writes, out, userVersion);
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
            if (diff.waitingOn != null)
                flags = setBit(flags, HasFields.WAITING_ON.ordinal());

            if (diff.writes != null)
                flags = setBit(flags, HasFields.WRITES.ordinal());
            return flags;
        }

        @Override
        public Object deserialize(AccordJournal.Key key, DataInputPlus in, int userVersion) throws IOException
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
            WaitingOnProvider waitingOn = (txn, deps) -> null;
            Writes writes = null;

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
            if (isSet(flags, HasFields.WAITING_ON.ordinal()))
            {
                int size = in.readInt();
                byte[] bytes = new byte[size];
                in.readFully(bytes);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                waitingOn = (localTxnId, deps) -> {
                    try
                    {
                        return WaitingOnSerializer.deserialize(localTxnId, deps.keyDeps.keys(), deps.rangeDeps.txnIds(), buffer);
                    }
                    catch (IOException e)
                    {
                        throw Throwables.unchecked(e);
                    }
                };
            }
            if (isSet(flags, HasFields.WRITES.ordinal()))
                writes = CommandSerializers.writes.deserialize(in, userVersion);

            return new LoadedDiff(txnId,
                                  executedAt,
                                  saveStatus,
                                  durability,
                                  acceptedOrCommitted,
                                  promised,
                                  route,
                                  partialTxn,
                                  partialDeps,
                                  waitingOn,
                                  writes);
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

    // TODO: the other interface with same name should go away after read-path and reconstruct are hooked up
    public interface WaitingOnProvider
    {
        Command.WaitingOn provide(TxnId txnId, PartialDeps deps);
    }
}