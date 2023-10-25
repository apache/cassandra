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

package org.apache.cassandra.tcm.log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.listeners.ClientNotificationListener;
import org.apache.cassandra.tcm.listeners.InitializationListener;
import org.apache.cassandra.tcm.listeners.LegacyStateListener;
import org.apache.cassandra.tcm.listeners.LogListener;
import org.apache.cassandra.tcm.listeners.MetadataSnapshotListener;
import org.apache.cassandra.tcm.listeners.PlacementsChangeListener;
import org.apache.cassandra.tcm.listeners.SchemaListener;
import org.apache.cassandra.tcm.listeners.UpgradeMigrationListener;
import org.apache.cassandra.tcm.transformations.cms.PreInitialize;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static java.util.Comparator.comparing;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.tcm.Epoch.EMPTY;
import static org.apache.cassandra.tcm.Epoch.FIRST;
import static org.apache.cassandra.utils.Clock.Global.nextUnixMicros;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

// TODO metrics for contention/buffer size/etc
public abstract class LocalLog implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(LocalLog.class);

    public final static class CommittedValue
    {
        public final ClusterMetadata metadata;
        public final long timestampMicros;

        public CommittedValue(ClusterMetadata metadata, long timestampMicros)
        {
            this.metadata = metadata;
            this.timestampMicros = timestampMicros;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CommittedValue that = (CommittedValue) o;
            return timestampMicros == that.timestampMicros && Objects.equals(metadata, that.metadata);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(metadata, timestampMicros);
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", CommittedValue.class.getSimpleName() + "[", "]")
                   .add("metadata=" + metadata)
                   .add("timestampMicros=" + timestampMicros)
                   .toString();
        }
    }

    protected final AtomicReference<CommittedValue> committed;

    /**
     * Custom comparator for pending entries. In general, we would like entries in the pending set to be ordered by epoch,
     * from smallest to highest, so that `#first()` call would return the smallest entry.
     *
     * However, snapshots should be applied out of order, and snapshots with higher epoch should be applied before snapshots
     * with a lower epoch in cases when there are multiple snapshots present.
     */
    protected final ConcurrentSkipListSet<Entry> pending = new ConcurrentSkipListSet<>((Entry e1, Entry e2) -> {
        if (e1.transform.kind() == Transformation.Kind.FORCE_SNAPSHOT && e2.transform.kind() == Transformation.Kind.FORCE_SNAPSHOT)
            return e2.epoch.compareTo(e1.epoch);

        if (e1.transform.kind() == Transformation.Kind.FORCE_SNAPSHOT)
            return -1;

        if (e2.transform.kind() == Transformation.Kind.FORCE_SNAPSHOT)
            return 1;

        return e1.epoch.compareTo(e2.epoch);
    });
    protected final LogStorage persistence;
    protected final Set<LogListener> listeners;
    protected final Set<ChangeListener> changeListeners;
    protected final Set<ChangeListener.Async> asyncChangeListeners;

    private LocalLog(LogStorage persistence, ClusterMetadata initial, boolean addListeners, boolean isReset)
    {
        assert initial.epoch.is(EMPTY) || initial.epoch.is(Epoch.UPGRADE_STARTUP) || isReset;
        committed = new AtomicReference<>(new CommittedValue(initial, 0L));
        this.persistence = persistence;
        listeners = Sets.newConcurrentHashSet();
        changeListeners = Sets.newConcurrentHashSet();
        asyncChangeListeners = Sets.newConcurrentHashSet();
        if (addListeners)
            addListeners();
    }

    public void bootstrap(InetAddressAndPort addr)
    {
        ClusterMetadata metadata = metadata();
        assert metadata.epoch.isBefore(FIRST) : String.format("Metadata epoch %s should be before first", metadata.epoch);
        Transformation transform = PreInitialize.withFirstCMS(addr);
        append(new Entry(Entry.Id.NONE, FIRST, transform, nextUnixMicros()));
        waitForHighestConsecutive();
        assert metadata().epoch.is(Epoch.FIRST) : ClusterMetadata.current().epoch + " " + ClusterMetadata.current().placements.get(ReplicationParams.meta());
    }

    public ClusterMetadata metadata()
    {
        return committed.get().metadata;
    }

    public CommittedValue committedValue()
    {
        return committed.get();
    }

    public boolean unsafeSetCommittedFromGossip(ClusterMetadata expected, ClusterMetadata updated)
    {
        if (!(expected.epoch.isEqualOrBefore(Epoch.UPGRADE_GOSSIP) && updated.epoch.is(Epoch.UPGRADE_GOSSIP)))
            throw new IllegalStateException(String.format("Illegal epochs for setting from gossip; expected: %s, updated: %s",
                                                          expected.epoch, updated.epoch));
        CommittedValue cur = committed.get();
        if (cur.metadata.equals(expected))
            return committed.compareAndSet(cur, new CommittedValue(updated, nextUnixMicros()));
        else
            return false;
    }

    public void unsafeSetCommittedFromGossip(ClusterMetadata updated)
    {
        if (!updated.epoch.is(Epoch.UPGRADE_GOSSIP))
            throw new IllegalStateException(String.format("Illegal epoch for setting from gossip; updated: %s",
                                                          updated.epoch));
        committed.set(new CommittedValue(updated, nextUnixMicros()));
    }

    public int pendingBufferSize()
    {
        return pending.size();
    }

    @VisibleForTesting
    public static LocalLog sync(ClusterMetadata initial, LogStorage logStorage, boolean addListeners, boolean isReset)
    {
        return new Sync(initial, logStorage, addListeners, isReset);
    }

    @VisibleForTesting
    public static LocalLog sync(ClusterMetadata initial, LogStorage logStorage, boolean addListeners)
    {
        return new Sync(initial, logStorage, addListeners, false);
    }

    @VisibleForTesting
    public static LocalLog asyncForTests(LogStorage logStorage, ClusterMetadata initial, boolean addListeners)
    {
        return new Async(logStorage, initial, addListeners, false);
    }

    @VisibleForTesting
    public static LocalLog async(ClusterMetadata initial)
    {
        return new Async(LogStorage.SystemKeyspace, initial, true, false);
    }

    public static LocalLog async(ClusterMetadata initial, boolean isReset)
    {
        return new Async(LogStorage.SystemKeyspace, initial, true, isReset);
    }

    public boolean hasGaps()
    {
        Epoch start = committed.get().metadata.epoch;
        for (Entry entry : pending)
        {
            if (!entry.epoch.isDirectlyAfter(start))
                return true;
            else
                start = entry.epoch;
        }
        return false;
    }

    public Optional<Epoch> highestPending()
    {
        try
        {
            return Optional.of(pending.last().epoch);
        }
        catch (NoSuchElementException eag)
        {
            return Optional.empty();
        }
    }

    public Replication getCommittedEntries(Epoch since)
    {
        return persistence.getReplication(since);
    }

    public ClusterMetadata waitForHighestConsecutive()
    {
        runOnce();
        return metadata();
    }

    public void append(Collection<Entry> entries)
    {
        if (!entries.isEmpty())
        {
            if (logger.isDebugEnabled())
                logger.debug("Appending entries to the pending buffer: {}", entries.stream().map(e -> e.epoch).collect(Collectors.toList()));
            pending.addAll(entries);
            processPending();
        }
    }

    public void append(Entry entry)
    {
        logger.debug("Appending entry to the pending buffer: {}", entry.epoch);
        pending.add(entry);
        processPending();
    }

    /**
     * Append log state snapshot. Does _not_ give any guarantees about visibility of the highest consecutive epoch.
     */
    public void append(LogState logState)
    {
        logger.debug("Appending log state with snapshot to the pending buffer: {}", logState);
        // If we receive a base state (snapshot), we need to construct a synthetic ForceSnapshot transformation that will serve as
        // a base for application of the rest of the entries. If the log state contains any additional transformations that follow
        // the base state, we can simply apply them to the log after.
        if (logState.baseState != null)
        {
            Epoch epoch = logState.baseState.epoch;

            // Create a synthetic "force snapshot" transformation to instruct the log to pick up given metadata
            ForceSnapshot transformation = new ForceSnapshot(logState.baseState);
            Entry newEntry = new Entry(Entry.Id.NONE, epoch, transformation, -1);
            pending.add(newEntry);
        }

        // Finally, append any additional transformations in the snapshot. Some or all of these could be earlier than the
        // currently enacted epoch (if we'd already moved on beyond the epoch of the base state for instance, or if newer
        // entries have been received via normal replication), but this is fine as entries will be put in the reorder
        // log, and duplicates will be dropped.
        pending.addAll(logState.transformations.entries());
        processPending();
    }

    public abstract ClusterMetadata awaitAtLeast(Epoch epoch) throws InterruptedException, TimeoutException;

    /**
     * Makes sure that the pending queue is processed _at least once_.
     */
    void runOnce()
    {
        try
        {
            runOnce(null);
        }
        catch (InterruptedException | TimeoutException e)
        {
            throw new RuntimeException("Should not have happened, since we await uninterruptibly", e);
        }
    }

    abstract void runOnce(DurationSpec durationSpec) throws InterruptedException, TimeoutException;
    abstract void processPending();

    private Entry peek()
    {
        try
        {
            return pending.first();
        }
        catch (NoSuchElementException ignore)
        {
            return null;
        }
    }

    /**
     * Called by implementations of {@link #processPending()}.
     *
     * Implementations have to guarantee there can be no more than one caller of {@link #processPendingInternal()}
     * at a time, as we are making calls to pre- and post- commit hooks. In other words, this method should be called
     * _exclusively_ from the implementation, outside of it there's no way to ensure mutual exclusion without
     * additional guards.
     *
     * Please note that we are using a custom comparator for pending entries, which ensures that FORCE_SNAPSHOT entries
     * are going to be prioritised over other entry kinds. After application of the snapshot entry, any entry with epoch
     * lower than the one that snapshot has enacted, are simply going to be dropped. The rest of entries (i.e. ones
     * that have epoch higher than the snapshot entry), are going to be processed in a regular fashion.
     */
    void processPendingInternal()
    {
        while (true)
        {
            Entry pendingEntry = peek();

            if (pendingEntry == null)
                return;

            CommittedValue prevCommitted = committed.get();
            // ForceSnapshot + Bootstrap entries can "jump" epoch
            boolean isPreInit = pendingEntry.transform.kind() == Transformation.Kind.PRE_INITIALIZE_CMS;
            boolean isSnapshot = pendingEntry.transform.kind() == Transformation.Kind.FORCE_SNAPSHOT;

            // for ForceSnapshot transformations, we take the timestamp of the last transformation that was applied
            long commitTimestampMicros = isSnapshot ? prevCommitted.timestampMicros : pendingEntry.timestampMicros;

            if (pendingEntry.epoch.isDirectlyAfter(prevCommitted.metadata.epoch)
                || ((isPreInit || isSnapshot) && pendingEntry.epoch.isAfter(prevCommitted.metadata.epoch)))
            {
                try
                {
                    Transformation.Result transformed;

                    try
                    {
                        transformed = pendingEntry.transform.execute(prevCommitted.metadata, commitTimestampMicros);
                    }
                    catch (Throwable t)
                    {
                        logger.error(String.format("Caught an exception while processing entry %s. This can mean that this node is configured differently from CMS.", prevCommitted), t);
                        throw new StopProcessingException(t);
                    }

                    if (!transformed.isSuccess())
                    {
                        logger.error("Error while processing entry {}. Transformation returned result of {}. This can mean that this node is configured differently from CMS.", prevCommitted, transformed.rejected());
                        throw new StopProcessingException();
                    }

                    ClusterMetadata next = transformed.success().metadata;
                    assert pendingEntry.epoch.is(next.epoch) :
                    String.format("Entry epoch %s does not match metadata epoch %s", pendingEntry.epoch, next.epoch);
                    assert next.epoch.isDirectlyAfter(prevCommitted.metadata.epoch) || isSnapshot || pendingEntry.transform.kind() == Transformation.Kind.PRE_INITIALIZE_CMS :
                    String.format("Epoch %s for %s can either force snapshot, or immediately follow %s",
                                  next.epoch, pendingEntry.transform, prevCommitted.metadata.epoch);

                    persistence.append(transformed.success().metadata.period, pendingEntry.maybeUnwrapExecuted());

                    notifyPreCommit(prevCommitted.metadata, next, isSnapshot);

                    if (committed.compareAndSet(prevCommitted, new CommittedValue(next, commitTimestampMicros)))
                    {
                        logger.info("Enacted {}. New tail is {}", pendingEntry.transform, next.epoch);
                        maybeNotifyListeners(pendingEntry, transformed);
                    }
                    else
                    {
                        // Since we disallow concurrent calls to `processPendingInternal` (as declared in the interface),
                        // we might have made an erroneous extra initialization of keyspaces by now, and, unless we
                        // throw here, we may in addition call to `afterCommit`.
                        throw new IllegalStateException(String.format("CAS conflict while trying to commit entry with seq %s, old version tail: %s current version tail: %s",
                                                                      next.epoch, prevCommitted.metadata.epoch, metadata().epoch));
                    }

                    notifyPostCommit(prevCommitted.metadata, next, isSnapshot);
                }
                catch (StopProcessingException t)
                {
                    throw t;
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.error("Could not process the entry", t);
                }
                finally
                {
                    // if we did succeed performing the commit, or have experienced an exception, remove from the buffer
                    pending.remove(pendingEntry);
                }
            }
            else if (!pendingEntry.epoch.isAfter(metadata().epoch))
            {
                logger.debug(String.format("An already appended entry %s discovered in the pending buffer, ignoring. Max consecutive: %s",
                                           pendingEntry.epoch, prevCommitted.metadata.epoch));
                pending.remove(pendingEntry);
            }
            else
            {
                Entry tmp = pending.first();
                if (tmp.epoch.is(pendingEntry.epoch))
                {
                    logger.debug("Smallest entry is non-consecutive {} to {}", pendingEntry.epoch, prevCommitted.metadata.epoch);
                    // if this one was not consecutive, subsequent won't be either
                    return;
                }
            }
        }
    }

    public Epoch replayPersisted()
    {
        LogState state = persistence.getLogState(metadata().epoch);
        append(state);
        return waitForHighestConsecutive().epoch;
    }

    private void maybeNotifyListeners(Entry entry, Transformation.Result result)
    {
        for (LogListener listener : listeners)
            listener.notify(entry, result);
    }

    public void addListener(LogListener listener)
    {
        this.listeners.add(listener);
    }

    public void addListener(ChangeListener listener)
    {
        if (listener instanceof ChangeListener.Async)
            this.asyncChangeListeners.add((ChangeListener.Async) listener);
        else
            this.changeListeners.add(listener);
    }

    public void removeListener(ChangeListener listener)
    {
        this.changeListeners.remove(listener);
    }

    public void notifyListeners(ClusterMetadata emptyFromSystemTables)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        notifyPreCommit(emptyFromSystemTables, metadata, true);
        notifyPostCommit(emptyFromSystemTables, metadata, true);
    }

    private void notifyPreCommit(ClusterMetadata before, ClusterMetadata after, boolean fromSnapshot)
    {
        for (ChangeListener listener : changeListeners)
            listener.notifyPreCommit(before, after, fromSnapshot);
        for (ChangeListener.Async listener : asyncChangeListeners)
            ScheduledExecutors.optionalTasks.submit(() -> listener.notifyPreCommit(before, after, fromSnapshot));
    }

    private void notifyPostCommit(ClusterMetadata before, ClusterMetadata after, boolean fromSnapshot)
    {
        for (ChangeListener listener : changeListeners)
            listener.notifyPostCommit(before, after, fromSnapshot);
        for (ChangeListener.Async listener : asyncChangeListeners)
            ScheduledExecutors.optionalTasks.submit(() -> listener.notifyPostCommit(before, after, fromSnapshot));
    }


    private static class Async extends LocalLog
    {
        private final AsyncRunnable runnable;
        private final Interruptible executor;

        private Async(LogStorage storage, ClusterMetadata initial, boolean addListeners, boolean isReset)
        {
            super(storage, initial, addListeners, isReset);
            this.runnable = new AsyncRunnable();
            this.executor = ExecutorFactory.Global.executorFactory().infiniteLoop("GlobalLogFollower", runnable, SAFE, NON_DAEMON, UNSYNCHRONIZED);
        }

        @Override
        public ClusterMetadata awaitAtLeast(Epoch epoch) throws InterruptedException, TimeoutException
        {
            ClusterMetadata lastSeen = committed.get().metadata;
            return lastSeen.epoch.compareTo(epoch) >= 0
                   ? lastSeen
                   : new AwaitCommit(epoch).get();
        }

        @Override
        public void runOnce(DurationSpec duration) throws InterruptedException, TimeoutException
        {
            Condition ours = Condition.newOneTimeCondition();
            for (int i = 0; i < 2; i++)
            {
                Condition current = runnable.subscriber.get();

                // If another thread has already initiated the follower runnable to execute, this will be non-null.
                // If so, we'll wait for it to ensure that the inflight, partial execution of the runnable's loop is
                // complete.
                if (current != null)
                {
                    if (duration == null)
                    {
                        current.awaitUninterruptibly();
                    }
                    else if (!current.await(duration.to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
                    {
                        throw new TimeoutException(String.format("Timed out waiting for follower to run at least once. " +
                                                                 "Pending is %s and current is now at epoch %s.",
                                                                 pending.stream().map((re) -> re.epoch).collect(Collectors.toList()),
                                                                 metadata().epoch));
                    }
                }

                // Either the runnable was already running (but we cannot know at what point in its processing it
                // was when we started to wait on the current condition), or the runnable was not running when we
                // entered this loop.
                // If the CAS here succeeds, we know that waiting on our condition will guarantee a full
                // execution of the runnable.
                // If we fail to CAS, that's also ok as it means another thread beat us to it and we can just go around
                // again and wait for the condition _it_ set to complete as this also guarantees a full execution of the
                // runnable's loop.

                // If we reach this point on our second iteration we can exit, even if current was null both times
                // as it means that the condition we lost the CAS to on the first iteration has completed and therefore
                // a full execution of the runnable has completed.
                if (i == 1)
                    return;

                if (runnable.subscriber.compareAndSet(null, ours))
                {
                    runnable.logNotifier.signalAll();
                    ours.awaitUninterruptibly();
                    return;
                }
            }
        }

        @Override
        void processPending()
        {
            runnable.logNotifier.signalAll();
        }

        @Override
        public void close()
        {
            executor.shutdownNow();

            Condition condition = runnable.subscriber.get();
            if (condition != null)
                condition.signalAll();
            runnable.logNotifier.signalAll();
            try
            {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                logger.error(e.getMessage(), e);
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        class AsyncRunnable implements Interruptible.Task
        {
            private final AtomicReference<Condition> subscriber;
            private final WaitQueue logNotifier;

            private AsyncRunnable()
            {
                this.logNotifier = newWaitQueue();
                subscriber = new AtomicReference<>();
            }

            public void run(Interruptible.State state) throws InterruptedException
            {
                try
                {
                    if (state != Interruptible.State.SHUTTING_DOWN)
                    {
                        Condition condition = subscriber.getAndSet(null);
                        // Grab a ticket ahead of time, so that we can't get into race with the exit from process pending
                        WaitQueue.Signal signal = logNotifier.register();
                        processPendingInternal();
                        if (condition != null)
                            condition.signalAll();
                        // if no new threads have subscribed since we started running, await
                        // otherwise, run again to process whatever work they may be waiting on
                        if (subscriber.get() == null)
                            signal.await();
                    }
                }
                catch (StopProcessingException t)
                {
                    logger.warn("Stopping log processing on the node... All subsequent epochs will be ignored.", t);
                    executor.shutdown();
                }
                catch (InterruptedException t)
                {
                    // ignore
                }
                catch (Throwable t)
                {
                    // TODO handle properly
                    logger.warn("Error in log follower", t);
                }
            }
        }

        private class AwaitCommit
        {
            private final Epoch waitingFor;

            private AwaitCommit(Epoch waitingFor)
            {
                this.waitingFor = waitingFor;
            }

            public ClusterMetadata get() throws InterruptedException, TimeoutException
            {
                return get(DatabaseDescriptor.getCmsAwaitTimeout());
            }

            public ClusterMetadata get(DurationSpec duration) throws InterruptedException, TimeoutException
            {
                ClusterMetadata lastSeen = metadata();
                while (!isCommitted(lastSeen))
                {
                    runOnce(duration);
                    lastSeen = metadata();

                    if (executor.isTerminated() && !isCommitted(lastSeen))
                        throw new Interruptible.TerminateException();
                }

                return lastSeen;
            }

            private boolean isCommitted(ClusterMetadata metadata)
            {
                return metadata.epoch.isEqualOrAfter(waitingFor);
            }
        }
    }

    private static class Sync extends LocalLog
    {
        private Sync(ClusterMetadata initial, LogStorage logStorage, boolean addListeners, boolean isReset)
        {
            super(logStorage, initial, addListeners, isReset);
        }

        void runOnce(DurationSpec durationSpec)
        {
            processPendingInternal();
        }

        synchronized void processPending()
        {
            processPendingInternal();
        }

        public ClusterMetadata awaitAtLeast(Epoch epoch)
        {
            processPending();
            if (metadata().epoch.isBefore(epoch))
                 throw new IllegalStateException(String.format("Could not reach %s after replay. Highest epoch after replay: %s.", epoch, metadata().epoch));

            return metadata();
        }

        public void close()
        {
        }
    }

    private void addListeners()
    {
        addListener(snapshotListener());
        addListener(new InitializationListener());
        addListener(SchemaListener.INSTANCE_FOR_STARTUP);
        addListener(new LegacyStateListener());
        addListener(new PlacementsChangeListener());
        addListener(new MetadataSnapshotListener());
        addListener(new ClientNotificationListener());
        addListener(new UpgradeMigrationListener());
    }

    private LogListener snapshotListener()
    {
        return (entry, metadata) -> {
            if (ClusterMetadataService.state() != ClusterMetadataService.State.LOCAL)
                return;

            if ((entry.epoch.getEpoch() % DatabaseDescriptor.getMetadataSnapshotFrequency()) == 0)
            {
                List<InetAddressAndPort> list = new ArrayList<>(ClusterMetadata.current().fullCMSMembers());
                list.sort(comparing(i -> i.addressBytes[i.addressBytes.length - 1]));
                if (list.get(0).equals(FBUtilities.getBroadcastAddressAndPort()))
                    ScheduledExecutors.nonPeriodicTasks.submit(() -> ClusterMetadataService.instance().sealPeriod());
            }
        };
    }

    private static class StopProcessingException extends RuntimeException
    {
        private StopProcessingException()
        {
            super();
        }

        private StopProcessingException(Throwable cause)
        {
            super(cause);
        }
    }
}