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
import java.util.NavigableMap;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Result;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.CommonAttributes;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MockJournal implements IJournal
{
    private final Map<JournalKey, List<LoadedDiff>> commands = new HashMap<>();
    private final Map<Integer, RedundantBefore> redundantBefores = new HashMap<>();
    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId)
    {
        JournalKey key = new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, commandStoreId);
        List<LoadedDiff> saved = commands.get(key);
        if (saved == null)
            return null;
        return reconstructFromDiff(new ArrayList<>(saved));
    }

    @Override
    public RedundantBefore loadRedundantBefore(int commandStoreId)
    {
        return redundantBefores.get(commandStoreId);
    }

    @Override
    public DurableBefore loadDurableBefore(int commandStoreId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReducingRangeMap<Timestamp> loadRejectBefore(int commandStoreId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommandStores.RangesForEpoch.Snapshot loadRangesForEpoch(int commandStoreId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendCommand(int store, SavedCommand.DiffWriter value, Runnable onFlush)
    {
        append(new JournalKey(value.after().txnId(), JournalKey.Type.COMMAND_DIFF, store), value, onFlush);
    }

    @Override
    public void appendRedundantBefore(int store, RedundantBefore newValue, Runnable onFlush)
    {
        redundantBefores.compute(store, (integer, oldValue) -> {
            if (oldValue == null)
                return newValue;
            return RedundantBefore.merge(oldValue, newValue);
        });
    }

    @Override
    public void persistStoreState(int store, AccordSafeCommandStore.FieldUpdates fieldUpdates, Runnable onFlush)
    {
        throw new IllegalStateException();
    }

    @Override
    public void append(JournalKey key, Object value, Runnable onFlush)
    {
        SavedCommand.DiffWriter diff = (SavedCommand.DiffWriter) value;
        commands.computeIfAbsent(key, (ignore_) -> new ArrayList<>())
                .add(diff(diff.before(), diff.after()));
    }

    /**
     * Emulating journal behaviour
     */

    public static LoadedDiff diff(Command before, Command after)
    {
        if (before == after)
            return null;

        // TODO: we do not need to save `waitingOn` _every_ time.
        Command.WaitingOn waitingOn = getWaitingOn(after);
        return new LoadedDiff(after.txnId(),
                             ifNotEqual(before, after, Command::executeAt, true),
                             ifNotEqual(before, after, Command::saveStatus, false),
                             ifNotEqual(before, after, Command::durability, false),

                             ifNotEqual(before, after, Command::acceptedOrCommitted, false),
                             ifNotEqual(before, after, Command::promised, false),

                             ifNotEqual(before, after, Command::route, true),
                             ifNotEqual(before, after, Command::partialTxn, false),
                             ifNotEqual(before, after, Command::partialDeps, false),
                             ifNotEqual(before, after, Command::additionalKeysOrRanges, false),

                             new NewValue<>((k, deps) -> waitingOn),
                             ifNotEqual(before, after, Command::writes, false));
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

        SavedCommand.WaitingOnProvider waitingOnProvider = null;
        Writes writes = null;

        for (LoadedDiff diff : diffs)
        {
            if (diff.txnId != null)
                txnId = diff.txnId;
            if (diff.executeAt != null)
                executeAt = diff.executeAt.get();
            if (diff.saveStatus != null)
                saveStatus = diff.saveStatus.get();
            if (diff.durability != null)
                durability = diff.durability.get();

            if (diff.acceptedOrCommitted != null)
                acceptedOrCommitted = diff.acceptedOrCommitted.get();
            if (diff.promised != null)
                promised = diff.promised.get();

            if (diff.route != null)
                route = diff.route.get();
            if (diff.partialTxn != null)
                partialTxn = diff.partialTxn.get();
            if (diff.partialDeps != null)
                partialDeps = diff.partialDeps.get();
            if (diff.additionalKeysOrRanges != null)
                additionalKeysOrRanges = diff.additionalKeysOrRanges.get();

            if (diff.waitingOn != null)
                waitingOnProvider = diff.waitingOn.get();
            if (diff.writes != null)
                writes = diff.writes.get();
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

    // TODO (required): this convert function was added only because AsyncOperationTest was failing without it;
    //  maybe after switching to loading from the log we can just pass l and r directly or remove != null checks.
    private static <OBJ, VAL> NewValue<VAL> ifNotEqual(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, boolean allowClassMismatch)
    {
        VAL l = null;
        VAL r = null;
        if (lo != null) l = convert.apply(lo);
        if (ro != null) r = convert.apply(ro);

        if (l == r)
            return null; // null here means there was no change

        if (l == null || r == null)
            return NewValue.of(r);

        assert allowClassMismatch || l.getClass() == r.getClass() : String.format("%s != %s", l.getClass(), r.getClass());

        if (l.equals(r))
            return null;

        return NewValue.of(r);
    }


    public static class NewValue<T>
    {
        final T value;

        private NewValue(T value)
        {
            this.value = value;
        }

        public boolean isNull()
        {
            return value == null;
        }

        public T get()
        {
            return value;
        }

        public static <T> NewValue<T> of(T value)
        {
            return new NewValue<>(value);
        }

        public String toString()
        {
            return "" + value;
        }
    }

    static Command.WaitingOn getWaitingOn(Command command)
    {
        if (command instanceof Command.Committed)
            return command.asCommitted().waitingOn();

        return null;
    }

    public static class LoadedDiff extends SavedCommand
    {
        public final TxnId txnId;

        public final NewValue<Timestamp> executeAt;
        public final NewValue<SaveStatus> saveStatus;
        public final NewValue<Status.Durability> durability;

        public final NewValue<Ballot> acceptedOrCommitted;
        public final NewValue<Ballot> promised;

        public final NewValue<Route<?>> route;
        public final NewValue<PartialTxn> partialTxn;
        public final NewValue<PartialDeps> partialDeps;
        public final NewValue<Seekables<?, ?>> additionalKeysOrRanges;

        public final NewValue<Writes> writes;
        public final NewValue<WaitingOnProvider> waitingOn;

        public LoadedDiff(TxnId txnId,
                          NewValue<Timestamp> executeAt,
                          NewValue<SaveStatus> saveStatus,
                          NewValue<Status.Durability> durability,

                          NewValue<Ballot> acceptedOrCommitted,
                          NewValue<Ballot> promised,

                          NewValue<Route<?>> route,
                          NewValue<PartialTxn> partialTxn,
                          NewValue<PartialDeps> partialDeps,
                          NewValue<Seekables<?, ?>> additionalKeysOrRanges,

                          NewValue<SavedCommand.WaitingOnProvider> waitingOn,
                          NewValue<Writes> writes)
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

            this.waitingOn = waitingOn;
        }

        public String toString()
        {
            return "LoadedDiff{" +
                   "waitingOn=" + waitingOn +
                   '}';
        }
    }
}
