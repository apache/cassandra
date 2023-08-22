/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.metrics.PaxosMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.paxos.uncommitted.PaxosBallotTracker;
import org.apache.cassandra.service.paxos.uncommitted.PaxosStateTracker;
import org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTracker;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Nemesis;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_DISABLE_COORDINATOR_LOCKING;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.config.Config.PaxosStatePurging.gc_grace;
import static org.apache.cassandra.config.Config.PaxosStatePurging.legacy;
import static org.apache.cassandra.config.DatabaseDescriptor.paxosStatePurging;
import static org.apache.cassandra.service.paxos.Commit.*;
import static org.apache.cassandra.service.paxos.PaxosState.MaybePromise.Outcome.*;
import static org.apache.cassandra.service.paxos.Commit.Accepted.latestAccepted;
import static org.apache.cassandra.service.paxos.Commit.Committed.latestCommitted;
import static org.apache.cassandra.service.paxos.Commit.isAfter;

/**
 * We save to memory the result of each operation before persisting to disk, however each operation that performs
 * the update does not return a result to the coordinator until the result is fully persisted.
 */
public class PaxosState implements PaxosOperationLock
{
    private static volatile boolean DISABLE_COORDINATOR_LOCKING = PAXOS_DISABLE_COORDINATOR_LOCKING.getBoolean();
    public static final ConcurrentHashMap<Key, PaxosState> ACTIVE = new ConcurrentHashMap<>();
    public static final Map<Key, Snapshot> RECENT = Caffeine.newBuilder()
                                                            .maximumWeight(DatabaseDescriptor.getPaxosCacheSizeInMiB() << 20)
                                                            .<Key, Snapshot>weigher((k, v) -> Ints.saturatedCast((v.accepted != null ? v.accepted.update.unsharedHeapSize() : 0L) + v.committed.update.unsharedHeapSize()))
                                                            .executor(ImmediateExecutor.INSTANCE)
                                                            .build().asMap();

    private static class TrackerHandle
    {
        static final PaxosStateTracker tracker;

        static
        {
            try
            {
                tracker = PaxosStateTracker.create(Directories.dataDirectories);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static void setDisableCoordinatorLocking(boolean disable)
    {
        DISABLE_COORDINATOR_LOCKING = disable;
    }

    public static boolean getDisableCoordinatorLocking()
    {
        return DISABLE_COORDINATOR_LOCKING;
    }

    public static PaxosUncommittedTracker uncommittedTracker()
    {
        return TrackerHandle.tracker.uncommitted();
    }

    public static PaxosBallotTracker ballotTracker()
    {
        return TrackerHandle.tracker.ballots();
    }

    public static void initializeTrackers()
    {
        Preconditions.checkState(TrackerHandle.tracker != null);
        PaxosMetrics.initialize();
    }

    public static void maybeRebuildUncommittedState() throws IOException
    {
        TrackerHandle.tracker.maybeRebuild();
    }

    public static void startAutoRepairs()
    {
        TrackerHandle.tracker.uncommitted().startAutoRepairs();
    }

    public static class Key
    {
        final DecoratedKey partitionKey;
        final TableMetadata metadata;

        public Key(DecoratedKey partitionKey, TableMetadata metadata)
        {
            this.partitionKey = partitionKey;
            this.metadata = metadata;
        }

        public int hashCode()
        {
            return partitionKey.hashCode() * 31 + metadata.id.hashCode();
        }

        public boolean equals(Object that)
        {
            return that instanceof Key && equals((Key) that);
        }

        public boolean equals(Key that)
        {
            return this.partitionKey.equals(that.partitionKey)
                    && this.metadata.id.equals(that.metadata.id);
        }
    }

    public static class Snapshot
    {
        public final @Nonnull
        Ballot promised;
        public final @Nonnull
        Ballot promisedWrite; // <= promised
        public final @Nullable Accepted  accepted; // if already committed, this will be null
        public final @Nonnull  Committed committed;

        public Snapshot(@Nonnull Ballot promised, @Nonnull Ballot promisedWrite, @Nullable Accepted accepted, @Nonnull Committed committed)
        {
            assert isAfter(promised, promisedWrite) || promised == promisedWrite;
            assert accepted == null || accepted.update.partitionKey().equals(committed.update.partitionKey());
            assert accepted == null || accepted.update.metadata().id.equals(committed.update.metadata().id);
            assert accepted == null || committed.isBefore(accepted.ballot);

            this.promised = promised;
            this.promisedWrite = promisedWrite;
            this.accepted = accepted;
            this.committed = committed;
        }

        public @Nonnull
        Ballot latestWitnessedOrLowBound(Ballot latestWriteOrLowBound)
        {
            return promised == promisedWrite ? latestWriteOrLowBound : latest(promised, latestWriteOrLowBound);
        }

        public @Nonnull
        Ballot latestWitnessedOrLowBound()
        {
            // warn: if proposal has same timestamp as promised, we should prefer accepted
            // since (if different) it reached a quorum of promises; this means providing it as first argument
            Ballot latest;
            latest = latest(accepted, committed).ballot;
            latest = latest(latest, promised);
            latest = latest(latest, ballotTracker().getLowBound());
            return latest;
        }

        public @Nonnull
        Ballot latestWriteOrLowBound()
        {
            // warn: if proposal has same timestamp as promised, we should prefer accepted
            // since (if different) it reached a quorum of promises; this means providing it as first argument
            Ballot latest = accepted != null && !accepted.update.isEmpty() ? accepted.ballot : null;
            latest = latest(latest, committed.ballot);
            latest = latest(latest, promisedWrite);
            latest = latest(latest, ballotTracker().getLowBound());
            return latest;
        }

        public static Snapshot merge(Snapshot a, Snapshot b)
        {
            if (a == null || b == null)
                return a == null ? b : a;

            Committed committed = latestCommitted(a.committed, b.committed);
            if (a instanceof UnsafeSnapshot && b instanceof UnsafeSnapshot)
                return new UnsafeSnapshot(committed);

            Accepted accepted;
            Ballot promised, promisedWrite;
            if (a instanceof UnsafeSnapshot || b instanceof UnsafeSnapshot)
            {
                if (a instanceof UnsafeSnapshot)
                    a = b; // we already have the winning Committed saved above, so just want the full snapshot (if either)

                if (committed == a.committed)
                    return a;

                promised = a.promised;
                promisedWrite = a.promisedWrite;
                accepted = isAfter(a.accepted, committed) ? a.accepted : null;
            }
            else
            {
                accepted = latestAccepted(a.accepted, b.accepted);
                accepted = isAfter(accepted, committed) ? accepted : null;
                promised = latest(a.promised, b.promised);
                promisedWrite = latest(a.promisedWrite, b.promisedWrite);
            }

            return new Snapshot(promised, promisedWrite, accepted, committed);
        }

        Snapshot removeExpired(long nowInSec)
        {
            boolean isAcceptedExpired = accepted != null && accepted.isExpired(nowInSec);
            boolean isCommittedExpired = committed.isExpired(nowInSec);

            if (paxosStatePurging() == gc_grace)
            {
                long expireOlderThan = SECONDS.toMicros(nowInSec - committed.update.metadata().params.gcGraceSeconds);
                isAcceptedExpired |= accepted != null && accepted.ballot.unixMicros() < expireOlderThan;
                isCommittedExpired |= committed.ballot.unixMicros() < expireOlderThan;
            }

            if (!isAcceptedExpired && !isCommittedExpired)
                return this;

            return new Snapshot(promised, promisedWrite,
                                isAcceptedExpired ? null : accepted,
                                isCommittedExpired
                                    ? Committed.none(committed.update.partitionKey(), committed.update.metadata())
                                    : committed);
        }
    }

    // used to permit recording Committed outcomes without waiting for initial read
    public static class UnsafeSnapshot extends Snapshot
    {
        public UnsafeSnapshot(@Nonnull Committed committed)
        {
            super(Ballot.none(), Ballot.none(), null, committed);
        }

        public UnsafeSnapshot(@Nonnull Commit committed)
        {
            this(new Committed(committed.ballot, committed.update));
        }
    }

    @VisibleForTesting
    public static class MaybePromise
    {
        public enum Outcome { REJECT, PERMIT_READ, PROMISE }

        final Snapshot before;
        final Snapshot after;
        final Ballot supersededBy;
        final Outcome outcome;

        MaybePromise(Snapshot before, Snapshot after, Ballot supersededBy, Outcome outcome)
        {
            this.before = before;
            this.after = after;
            this.supersededBy = supersededBy;
            this.outcome = outcome;
        }

        static MaybePromise promise(Snapshot before, Snapshot after)
        {
            return new MaybePromise(before, after, null, PROMISE);
        }

        static MaybePromise permitRead(Snapshot before, Ballot supersededBy)
        {
            return new MaybePromise(before, before, supersededBy, PERMIT_READ);
        }

        static MaybePromise reject(Snapshot snapshot, Ballot supersededBy)
        {
            return new MaybePromise(snapshot, snapshot, supersededBy, REJECT);
        }

        public Outcome outcome()
        {
            return outcome;
        }

        public Ballot supersededBy()
        {
            return supersededBy;
        }
    }

    @Nemesis private static final AtomicReferenceFieldUpdater<PaxosState, Snapshot> currentUpdater = AtomicReferenceFieldUpdater.newUpdater(PaxosState.class, Snapshot.class, "current");

    final Key key;
    private int active; // current number of active referents (once drops to zero, we remove the global entry)
    @Nemesis private volatile Snapshot current;
    @Nemesis private volatile Thread lockedBy;
    @Nemesis private volatile int waiting;

    private static final AtomicReferenceFieldUpdater<PaxosState, Thread> lockedByUpdater = AtomicReferenceFieldUpdater.newUpdater(PaxosState.class, Thread.class, "lockedBy");

    private PaxosState(Key key, Snapshot current)
    {
        this.key = key;
        this.current = current;
    }

    @VisibleForTesting
    public static PaxosState get(Commit commit)
    {
        return get(commit.update.partitionKey(), commit.update.metadata());
    }

    public static PaxosState get(DecoratedKey partitionKey, TableMetadata table)
    {
        // TODO would be nice to refactor verb handlers to support re-submitting to executor if waiting for another thread to read state
        return getUnsafe(partitionKey, table).maybeLoad();
    }

    // does not increment total number of accessors, since we would accept null (so only access if others are, not for own benefit)
    private static PaxosState tryGetUnsafe(DecoratedKey partitionKey, TableMetadata metadata)
    {
        return ACTIVE.compute(new Key(partitionKey, metadata), (key, cur) -> {
            if (cur == null)
            {
                Snapshot saved = RECENT.remove(key);
                if (saved != null)
                    //noinspection resource
                    cur = new PaxosState(key, saved);
            }
            if (cur != null)
                ++cur.active;
            return cur;
        });
    }

    private static PaxosState getUnsafe(DecoratedKey partitionKey, TableMetadata metadata)
    {
        return ACTIVE.compute(new Key(partitionKey, metadata), (key, cur) -> {
            if (cur == null)
            {
                //noinspection resource
                cur = new PaxosState(key, RECENT.remove(key));
            }
            ++cur.active;
            return cur;
        });
    }

    private static PaxosState getUnsafe(Commit commit)
    {
        return getUnsafe(commit.update.partitionKey(), commit.update.metadata());
    }

    // don't increment the total count, as we are only using this for locking purposes when coordinating
    @VisibleForTesting
    public static PaxosOperationLock lock(DecoratedKey partitionKey, TableMetadata metadata, long deadline, ConsistencyLevel consistencyForConsensus, boolean isWrite) throws RequestTimeoutException
    {
        if (DISABLE_COORDINATOR_LOCKING)
            return PaxosOperationLock.noOp();

        PaxosState lock = ACTIVE.compute(new Key(partitionKey, metadata), (key, cur) -> {
            if (cur == null)
                cur = new PaxosState(key, RECENT.remove(key));
            ++cur.active;
            return cur;
        });

        try
        {
            if (!lock.lock(deadline))
                throw throwTimeout(metadata, consistencyForConsensus, isWrite);
            return lock;
        }
        catch (Throwable t)
        {
            lock.close();
            throw t;
        }
    }
    
    private static RequestTimeoutException throwTimeout(TableMetadata metadata, ConsistencyLevel consistencyForConsensus, boolean isWrite)
    {
        int blockFor = consistencyForConsensus.blockFor(Keyspace.open(metadata.keyspace).getReplicationStrategy());
        throw isWrite
                ? new WriteTimeoutException(WriteType.CAS, consistencyForConsensus, 0, blockFor)
                : new ReadTimeoutException(consistencyForConsensus, 0, blockFor, false);
    }

    private PaxosState maybeLoad()
    {
        try
        {
            Snapshot current = this.current;
            if (current == null || current instanceof UnsafeSnapshot)
            {
                synchronized (this)
                {
                    current = this.current;
                    if (current == null || current instanceof UnsafeSnapshot)
                    {
                        Snapshot snapshot = SystemKeyspace.loadPaxosState(key.partitionKey, key.metadata, 0);
                        currentUpdater.accumulateAndGet(this, snapshot, Snapshot::merge);
                    }
                }
            }
        }
        catch (Throwable t)
        {
            try { close(); } catch (Throwable t2) { t.addSuppressed(t2); }
            throw t;
        }

        return this;
    }

    private boolean lock(long deadline)
    {
        try
        {
            Thread thread = Thread.currentThread();
            if (lockedByUpdater.compareAndSet(this, null, thread))
                return true;

            synchronized (this)
            {
                waiting++;

                try
                {
                    while (true)
                    {
                        if (lockedByUpdater.compareAndSet(this, null, thread))
                            return true;

                        while (lockedBy != null)
                        {
                            long now = nanoTime();
                            if (now >= deadline)
                                return false;

                            wait(1 + ((deadline - now) - 1) / 1000000);
                        }
                    }
                }
                finally
                {
                    waiting--;
                }
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void maybeUnlock()
    {
        // no visibility requirements, as if we hold the lock it was last updated by us
        if (lockedBy == null)
            return;

        Thread thread = Thread.currentThread();

        if (lockedBy == thread)
        {
            lockedBy = null;
            if (waiting > 0)
            {
                synchronized (this)
                {
                    notify();
                }
            }
        }
    }

    public void close()
    {
        maybeUnlock();
        ACTIVE.compute(key, (key, cur) ->
        {
            assert cur != null;
            if (--cur.active > 0)
                return cur;

            Snapshot stash = cur.current;
            if (stash != null && stash.getClass() == Snapshot.class)
                RECENT.put(key, stash);
            return null;
        });
    }

    Snapshot current(Ballot ballot)
    {
        return current((int)ballot.unix(SECONDS));
    }

    Snapshot current(long nowInSec)
    {
        // CASSANDRA-12043 is not an issue for v2, as we perform Commit+Prepare and PrepareRefresh
        // which are able to make progress whether or not the old commit is shadowed by the TTL (since they
        // depend only on the write being successful, not the data being read again later).
        // However, we still use nowInSec to guard reads to ensure we do not log any linearizability violations
        // due to discrepancies in gc grace handling

        Snapshot current = this.current;
        if (current == null || current.getClass() != Snapshot.class)
            throw new IllegalStateException();
        return current.removeExpired(nowInSec);
    }

    @VisibleForTesting
    public Snapshot currentSnapshot()
    {
        return current;
    }

    @VisibleForTesting
    public void updateStateUnsafe(Function<Snapshot, Snapshot> f)
    {
        current = f.apply(current);
    }

    /**
     * Record the requested ballot as promised if it is newer than our current promise; otherwise do nothing.
     * @return a PromiseResult containing the before and after state for this operation
     */
    public MaybePromise promiseIfNewer(Ballot ballot, boolean isWrite)
    {
        Snapshot before, after;
        while (true)
        {
            Snapshot realBefore = current;
            before = realBefore.removeExpired((int)ballot.unix(SECONDS));
            Ballot latestWriteOrLowBound = before.latestWriteOrLowBound();
            Ballot latest = before.latestWitnessedOrLowBound(latestWriteOrLowBound);
            if (isAfter(ballot, latest))
            {
                after = new Snapshot(ballot, isWrite ? ballot : before.promisedWrite, before.accepted, before.committed);
                if (currentUpdater.compareAndSet(this, before, after))
                {
                    // It doesn't matter if a later operation witnesses this before it's persisted,
                    // as it can only lead to rejecting a promise which leaves no persistent state
                    // (and it's anyway safe to arbitrarily reject promises)
                    if (isWrite)
                    {
                        Tracing.trace("Promising read/write ballot {}", ballot);
                        SystemKeyspace.savePaxosWritePromise(key.partitionKey, key.metadata, ballot);
                    }
                    else
                    {
                        Tracing.trace("Promising read ballot {}", ballot);
                        SystemKeyspace.savePaxosReadPromise(key.partitionKey, key.metadata, ballot);
                    }
                    return MaybePromise.promise(before, after);
                }
            }
            else if (isAfter(ballot, latestWriteOrLowBound))
            {
                Tracing.trace("Permitting only read by ballot {}", ballot);
                return MaybePromise.permitRead(before, latest);
            }
            else
            {
                Tracing.trace("Promise rejected; {} older than {}", ballot, latest);
                return MaybePromise.reject(before, latest);
            }

            Snapshot realAfter = new Snapshot(ballot, isWrite ? ballot : realBefore.promisedWrite, realBefore.accepted, realBefore.committed);
            after = new Snapshot(ballot, realAfter.promisedWrite, before.accepted, before.committed);
            if (currentUpdater.compareAndSet(this, realBefore, realAfter))
                break;
        }

        // It doesn't matter if a later operation witnesses this before it's persisted,
        // as it can only lead to rejecting a promise which leaves no persistent state
        // (and it's anyway safe to arbitrarily reject promises)
        Tracing.trace("Promising ballot {}", ballot);
        if (isWrite) SystemKeyspace.savePaxosWritePromise(key.partitionKey, key.metadata, ballot);
        else SystemKeyspace.savePaxosReadPromise(key.partitionKey, key.metadata, ballot);
        return MaybePromise.promise(before, after);
    }

    /**
     * Record an acceptance of the proposal if there is no newer promise; otherwise inform the caller of the newer ballot
     */
    public Ballot acceptIfLatest(Proposal proposal)
    {
        if (paxosStatePurging() == legacy && !(proposal instanceof AcceptedWithTTL))
            proposal = AcceptedWithTTL.withDefaultTTL(proposal);

        // state.promised can be null, because it is invalidated by committed;
        // we may also have accepted a newer proposal than we promised, so we confirm that we are the absolute newest
        // (or that we have the exact same ballot as our promise, which is the typical case)
        Snapshot before, after;
        while (true)
        {
            Snapshot realBefore = current;
            before = realBefore.removeExpired((int)proposal.ballot.unix(SECONDS));
            Ballot latest = before.latestWitnessedOrLowBound();
            if (!proposal.isSameOrAfter(latest))
            {
                Tracing.trace("Rejecting proposal {}; latest is now {}", proposal.ballot, latest);
                return latest;
            }

            if (proposal.hasSameBallot(before.committed)) // TODO: consider not answering
                return null; // no need to save anything, or indeed answer at all

            after = new Snapshot(realBefore.promised, realBefore.promisedWrite, proposal.accepted(), realBefore.committed);
            if (currentUpdater.compareAndSet(this, realBefore, after))
                break;
        }

        // It is more worrisome to permit witnessing an accepted proposal before we have persisted it
        // because this has more tangible effects on the recipient, but again it is safe: either it is
        //  - witnessed to reject (which is always safe, as it prevents rather than creates an outcome); or
        //  - witnessed as an in progress proposal
        // in the latter case, for there to be any effect on the state the proposal must be re-proposed, or not,
        // on its own terms, and must
        // be persisted by the re-proposer, and so it remains a non-issue
        // though this
        Tracing.trace("Accepting proposal {}", proposal);
        SystemKeyspace.savePaxosProposal(proposal);
        return null;
    }

    public void commit(Agreed commit)
    {
        applyCommit(commit, this, (apply, to) ->
            currentUpdater.accumulateAndGet(to, new UnsafeSnapshot(apply), Snapshot::merge)
        );
    }

    public static void commitDirect(Commit commit)
    {
        applyCommit(commit, null, (apply, ignore) -> {
            try (PaxosState state = tryGetUnsafe(apply.update.partitionKey(), apply.update.metadata()))
            {
                if (state != null)
                    currentUpdater.accumulateAndGet(state, new UnsafeSnapshot(apply), Snapshot::merge);
            }
        });
    }

    private static void applyCommit(Commit commit, PaxosState state, BiConsumer<Commit, PaxosState> postCommit)
    {
        if (paxosStatePurging() == legacy && !(commit instanceof CommittedWithTTL))
            commit = CommittedWithTTL.withDefaultTTL(commit);

        long start = nanoTime();
        try
        {
            // TODO: run Paxos Repair before truncate so we can excise this
            // The table may have been truncated since the proposal was initiated. In that case, we
            // don't want to perform the mutation and potentially resurrect truncated data
            if (commit.ballot.unixMicros() >= SystemKeyspace.getTruncatedAt(commit.update.metadata().id))
            {
                Tracing.trace("Committing proposal {}", commit);
                Mutation mutation = commit.makeMutation();
                Keyspace.open(mutation.getKeyspaceName()).apply(mutation, true);
            }
            else
            {
                Tracing.trace("Not committing proposal {} as ballot timestamp predates last truncation time", commit);
            }

            // for commits we save to disk first, because we can; even here though it is safe to permit later events to
            // witness the state before it is persisted. The only tricky situation is that we use the witnessing of
            // a quorum of nodes having witnessed the latest commit to decide if we need to disseminate a commit
            // again before proceeding with any new operation, but in this case we have already persisted the relevant
            // information, namely the base table mutation.  So this fact is persistent, even if knowldge of this fact
            // is not (and if this is lost, it may only lead to a future operation unnecessarily committing again)
            SystemKeyspace.savePaxosCommit(commit);
            postCommit.accept(commit, state);
        }
        finally
        {
            Keyspace.openAndGetStore(commit.update.metadata()).metric.casCommit.addNano(nanoTime() - start);
        }
    }

    public static PrepareResponse legacyPrepare(Commit toPrepare)
    {
        long start = nanoTime();
        try (PaxosState unsafeState = getUnsafe(toPrepare))
        {
            synchronized (unsafeState.key)
            {
                unsafeState.maybeLoad();
                assert unsafeState.current != null;

                while (true)
                {
                    // ignore nowInSec when merging as this can only be an issue during the transition period, so the unbounded
                    // problem of CASSANDRA-12043 is not an issue
                    Snapshot realBefore = unsafeState.current;
                    Snapshot before = realBefore.removeExpired(toPrepare.ballot.unix(SECONDS));
                    Ballot latest = before.latestWitnessedOrLowBound();
                    if (toPrepare.isAfter(latest))
                    {
                        Snapshot after = new Snapshot(toPrepare.ballot, toPrepare.ballot, realBefore.accepted, realBefore.committed);
                        if (currentUpdater.compareAndSet(unsafeState, realBefore, after))
                        {
                            Tracing.trace("Promising ballot {}", toPrepare.ballot);
                            DecoratedKey partitionKey = toPrepare.update.partitionKey();
                            TableMetadata metadata = toPrepare.update.metadata();
                            SystemKeyspace.savePaxosWritePromise(partitionKey, metadata, toPrepare.ballot);
                            return new PrepareResponse(true, before.accepted == null ? Accepted.none(partitionKey, metadata) : before.accepted, before.committed);
                        }
                    }
                    else
                    {
                        Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, before.promised);
                        // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                        return new PrepareResponse(false, new Commit(before.promised, toPrepare.update), before.committed);
                    }
                }
            }
        }
        finally
        {
            Keyspace.openAndGetStore(toPrepare.update.metadata()).metric.casPrepare.addNano(nanoTime() - start);
        }
    }

    public static Boolean legacyPropose(Commit proposal)
    {
        if (paxosStatePurging() == legacy && !(proposal instanceof AcceptedWithTTL))
            proposal = AcceptedWithTTL.withDefaultTTL(proposal);

        long start = nanoTime();
        try (PaxosState unsafeState = getUnsafe(proposal))
        {
            synchronized (unsafeState.key)
            {
                unsafeState.maybeLoad();
                assert unsafeState.current != null;

                while (true)
                {
                    Snapshot realBefore = unsafeState.current;
                    Snapshot before = realBefore.removeExpired((int)proposal.ballot.unix(SECONDS));
                    boolean accept = proposal.isSameOrAfter(before.latestWitnessedOrLowBound());
                    if (accept)
                    {
                        if (proposal.hasSameBallot(before.committed) ||
                            currentUpdater.compareAndSet(unsafeState, realBefore,
                                                         new Snapshot(realBefore.promised, realBefore.promisedWrite,
                                                                      new Accepted(proposal), realBefore.committed)))
                        {
                            Tracing.trace("Accepting proposal {}", proposal);
                            SystemKeyspace.savePaxosProposal(proposal);
                            return true;
                        }
                    }
                    else
                    {
                        Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, before.promised);
                        return false;
                    }
                }
            }
        }
        finally
        {
            Keyspace.openAndGetStore(proposal.update.metadata()).metric.casPropose.addNano(nanoTime() - start);
        }
    }

    public static void unsafeReset()
    {
        ACTIVE.clear();
        RECENT.clear();
        ballotTracker().truncate();
    }

    public static Snapshot unsafeGetIfPresent(DecoratedKey partitionKey, TableMetadata metadata)
    {
        Key key = new Key(partitionKey, metadata);
        PaxosState cur = ACTIVE.get(key);
        if (cur != null) return cur.current;
        return RECENT.get(key);
    }
}
