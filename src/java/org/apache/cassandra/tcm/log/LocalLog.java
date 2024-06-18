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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Startup;
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
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.tcm.transformations.cms.PreInitialize;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.tcm.Epoch.EMPTY;
import static org.apache.cassandra.tcm.Epoch.FIRST;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

// TODO metrics for contention/buffer size/etc

/**
 * LocalLog is an entity responsible for collecting replicated entries and enacting new epochs locally as soon
 * as the node has enough information to reconstruct ClusterMetadata instance at that epoch.
 *
 * Since ClusterMetadata can be replicated to the node by different means (commit response, replication after
 * commit by other node, CMS or peer log replay), it may happen that replicated entries arrive out-of-order
 * and may even contain gaps. For example, if node1 has registered at epoch 10, and node2 has registered at
 * epoch 11, it may happen that node3 will receive entry for epoch 11 before it receives the entry for epoch 10.
 * To reconstruct the history, LocalLog has a reorder buffer, which holds entries until the one that is consecutive
 * to the highest known epoch is available, at which point it (and all subsequent entries whose predecessors appear in the
 * pending buffer) is enacted.
 */
public abstract class LocalLog implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(LocalLog.class);

    protected final AtomicReference<ClusterMetadata> committed;
    // Indicates that, during process startup, the intial replay of persisted log entries has been performed
    // and the log made ready for use. This involves adding the listeners and firing a one time post-commit
    // notification to them all.
    private final AtomicBoolean replayComplete = new AtomicBoolean();

    public static LogSpec logSpec()
    {
        return new LogSpec();
    }

    public static class LogSpec
    {
        private ClusterMetadata initial;
        private ClusterMetadata prev;
        private List<Startup.AfterReplay> afterReplay = Collections.emptyList();
        private LogStorage storage = LogStorage.None;
        private boolean async = true;
        private boolean defaultListeners = false;
        private boolean isReset = false;
        private boolean loadSSTables = true;

        private final Set<LogListener> listeners = new HashSet<>();
        private final Set<ChangeListener> changeListeners = new HashSet<>();
        private final Set<ChangeListener.Async> asyncChangeListeners = new HashSet<>();

        private LogSpec()
        {
        }

        /**
         * create a sync log - only used for tests and tools
         * @return
         */
        public LogSpec sync()
        {
            this.async = false;
            return this;
        }

        public LogSpec async()
        {
            this.async = true;
            return this;
        }

        public LogSpec withDefaultListeners()
        {
            return withDefaultListeners(true);
        }

        public LogSpec loadSSTables(boolean loadSSTables)
        {
            this.loadSSTables = loadSSTables;
            return this;
        }

        public LogSpec withDefaultListeners(boolean withDefaultListeners)
        {
            if (withDefaultListeners &&
                !(listeners.isEmpty() && changeListeners.isEmpty() && asyncChangeListeners.isEmpty()))
            {
                throw new IllegalStateException("LogSpec can only require all listeners OR specific listeners");
            }

            defaultListeners = withDefaultListeners;
            return this;
        }

        public LogSpec withLogListener(LogListener listener)
        {
            if (defaultListeners)
                throw new IllegalStateException("LogSpec can only require all listeners OR specific listeners");
            listeners.add(listener);
            return this;
        }

        public LogSpec withListener(ChangeListener listener)
        {
            if (defaultListeners)
                throw new IllegalStateException("LogSpec can only require all listeners OR specific listeners");
            if (listener instanceof ChangeListener.Async)
                asyncChangeListeners.add((ChangeListener.Async) listener);
            else
                changeListeners.add(listener);
            return this;
        }

        public LogSpec isReset(boolean isReset)
        {
            this.isReset = isReset;
            return this;
        }

        public boolean isReset()
        {
            return this.isReset;
        }

        public LogStorage storage()
        {
            return storage;
        }

        public LogSpec withStorage(LogStorage storage)
        {
            this.storage = storage;
            return this;
        }

        public LogSpec afterReplay(Startup.AfterReplay ... afterReplay)
        {
            this.afterReplay = Lists.newArrayList(afterReplay);
            return this;
        }

        public LogSpec withInitialState(ClusterMetadata initial)
        {
            this.initial = initial;
            return this;
        }

        public LogSpec withPreviousState(ClusterMetadata prev)
        {
            this.prev = prev;
            return this;
        }

        public final LocalLog createLog()
        {
            if (async)
                return new Async(this);
            else
                return new Sync(this);
        }
    }

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

    protected final LogStorage storage;
    protected final Set<LogListener> listeners;
    protected final Set<ChangeListener> changeListeners;
    protected final Set<ChangeListener.Async> asyncChangeListeners;
    protected final LogSpec spec;

    // for testing - used to inject filters which cause entries to be dropped before appending
    protected final List<Predicate<Entry>> entryFilters;

    private LocalLog(LogSpec logSpec)
    {
        this.spec = logSpec;
        if (spec.initial == null)
            spec.initial = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        if (spec.prev == null)
            spec.prev = new ClusterMetadata(spec.initial.partitioner);
        assert spec.initial.epoch.is(EMPTY) || spec.initial.epoch.is(Epoch.UPGRADE_STARTUP) || spec.isReset :
        String.format(String.format("Should start with empty epoch, unless we're in upgrade or reset mode: %s (isReset: %s)", spec.initial, spec.isReset));

        this.committed = new AtomicReference<>(logSpec.initial);
        this.storage = logSpec.storage;
        listeners = Sets.newConcurrentHashSet();
        changeListeners = Sets.newConcurrentHashSet();
        asyncChangeListeners = Sets.newConcurrentHashSet();
        entryFilters = Lists.newCopyOnWriteArrayList();
    }

    @VisibleForTesting
    public void unsafeBootstrapForTesting(InetAddressAndPort addr)
    {
        bootstrap(addr, "");
    }

    /**
     *
     * @param addr
     * @param datacenter
     */
    public void bootstrap(InetAddressAndPort addr, String datacenter)
    {
        ClusterMetadata metadata = metadata();
        assert metadata.epoch.isBefore(FIRST) : String.format("Metadata epoch %s should be before first", metadata.epoch);
        Transformation transform = PreInitialize.withFirstCMS(addr, datacenter);
        append(new Entry(Entry.Id.NONE, FIRST, transform));
        metadata = waitForHighestConsecutive();
        assert metadata.epoch.is(Epoch.FIRST) : String.format("Epoch: %s. CMS: %s", metadata.epoch, metadata.fullCMSMembers());
    }

    public ClusterMetadata metadata()
    {
        return committed.get();
    }

    public boolean unsafeSetCommittedFromGossip(ClusterMetadata expected, ClusterMetadata updated)
    {
        if (!(expected.epoch.isEqualOrBefore(Epoch.UPGRADE_GOSSIP) && updated.epoch.is(Epoch.UPGRADE_GOSSIP)))
            throw new IllegalStateException(String.format("Illegal epochs for setting from gossip; expected: %s, updated: %s",
                                                          expected.epoch, updated.epoch));
        return committed.compareAndSet(expected, updated);
    }

    public void unsafeSetCommittedFromGossip(ClusterMetadata updated)
    {
        if (!updated.epoch.is(Epoch.UPGRADE_GOSSIP))
            throw new IllegalStateException(String.format("Illegal epoch for setting from gossip; updated: %s",
                                                          updated.epoch));
        committed.set(updated);
    }

    public int pendingBufferSize()
    {
        return pending.size();
    }

    public boolean hasGaps()
    {
        Epoch start = committed.get().epoch;
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

    public LogState getCommittedEntries(Epoch since)
    {
        return storage.getLogState(since, false);
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
            if (!entryFilters.isEmpty())
            {
                logger.debug("Appending batch of entries to the pending buffer individually due to presence of entry filters");
                entries.forEach(this::maybeAppend);
            }
            else
            {
                if (logger.isDebugEnabled())
                    logger.debug("Appending entries to the pending buffer: {}", entries.stream().map(e -> e.epoch).collect(Collectors.toList()));
                pending.addAll(entries);
            }
            processPending();
        }
    }

    public void append(Entry entry)
    {
        maybeAppend(entry);
    }

    /**
     * Append log state snapshot. Does _not_ give any guarantees about visibility of the highest consecutive epoch.
     */
    public void append(LogState logState)
    {
        if (logState.isEmpty())
            return;
        logger.debug("Appending log state with snapshot to the pending buffer: {}", logState);
        // If we receive a base state (snapshot), we need to construct a synthetic ForceSnapshot transformation that will serve as
        // a base for application of the rest of the entries. If the log state contains any additional transformations that follow
        // the base state, we can simply apply them to the log after.
        if (logState.baseState != null)
        {
            Epoch epoch = logState.baseState.epoch;

            // Create a synthetic "force snapshot" transformation to instruct the log to pick up given metadata
            ForceSnapshot transformation = new ForceSnapshot(logState.baseState);
            Entry newEntry = new Entry(Entry.Id.NONE, epoch, transformation);
            maybeAppend(newEntry);
        }

        // Finally, append any additional transformations in the snapshot. Some or all of these could be earlier than the
        // currently enacted epoch (if we'd already moved on beyond the epoch of the base state for instance, or if newer
        // entries have been received via normal replication), but this is fine as entries will be put in the reorder
        // log, and duplicates will be dropped.
        append(logState.entries);
        processPending();
    }

    private void maybeAppend(Entry entry)
    {
        for(Predicate<Entry> filter :  entryFilters)
        {
            if (filter.test(entry))
            {
                logger.debug("Not appending entry to the pending buffer due to configured filters: {}", entry.epoch);
                return;
            }
        }

        logger.debug("Appending entry to the pending buffer: {}", entry.epoch);
        pending.add(entry);
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
        catch (TimeoutException e)
        {
            // This should not happen as no duration was specified in the call to runOnce
            throw new RuntimeException("Timed out waiting for log follower to run", e);
        }
    }

    abstract void runOnce(DurationSpec durationSpec) throws TimeoutException;
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

            ClusterMetadata prev = committed.get();
            // ForceSnapshot + Bootstrap entries can "jump" epoch
            boolean isPreInit = pendingEntry.transform.kind() == Transformation.Kind.PRE_INITIALIZE_CMS;
            boolean isSnapshot = pendingEntry.transform.kind() == Transformation.Kind.FORCE_SNAPSHOT;
            if (pendingEntry.epoch.isDirectlyAfter(prev.epoch)
                || ((isPreInit || isSnapshot) && pendingEntry.epoch.isAfter(prev.epoch)))
            {
                try
                {
                    Transformation.Result transformed;

                    try
                    {
                        transformed = pendingEntry.transform.execute(prev);
                    }
                    catch (Throwable t)
                    {
                        logger.error(String.format("Caught an exception while processing entry %s. This can mean that this node is configured differently from CMS.", prev), t);
                        throw new StopProcessingException(t);
                    }

                    if (!transformed.isSuccess())
                    {
                        logger.error("Error while processing entry {}. Transformation returned result of {}. This can mean that this node is configured differently from CMS.", prev, transformed.rejected());
                        throw new StopProcessingException();
                    }

                    ClusterMetadata next = transformed.success().metadata;
                    assert pendingEntry.epoch.is(next.epoch) :
                    String.format("Entry epoch %s does not match metadata epoch %s", pendingEntry.epoch, next.epoch);
                    assert next.epoch.isDirectlyAfter(prev.epoch) || isSnapshot || pendingEntry.transform.kind() == Transformation.Kind.PRE_INITIALIZE_CMS :
                    String.format("Epoch %s for %s can either force snapshot, or immediately follow %s",
                                  next.epoch, pendingEntry.transform, prev.epoch);

                    // If replay during initialisation has completed persist to local storage unless the entry is
                    // a synthetic ForceSnapshot which is not a replicated event but enables jumping over gaps
                    if (replayComplete.get() && pendingEntry.transform.kind() != Transformation.Kind.FORCE_SNAPSHOT)
                        storage.append(pendingEntry.maybeUnwrapExecuted());

                    notifyPreCommit(prev, next, isSnapshot);

                    if (committed.compareAndSet(prev, next))
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
                                                                      next.epoch, prev.epoch, metadata().epoch));
                    }

                    notifyPostCommit(prev, next, isSnapshot);
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
                                           pendingEntry.epoch, prev.epoch));
                pending.remove(pendingEntry);
            }
            else
            {
                Entry tmp = pending.first();
                if (tmp.epoch.is(pendingEntry.epoch))
                {
                    logger.debug("Smallest entry is non-consecutive {} to {}", pendingEntry.epoch, prev.epoch);
                    // if this one was not consecutive, subsequent won't be either
                    return;
                }
            }
        }
    }

    /**
     * Replays items that were persisted during previous starts. Replayed items _will not_ be persisted again.
     */
    private ClusterMetadata replayPersisted()
    {
        if (replayComplete.get())
            throw new IllegalStateException("Can only replay persisted once.");
        LogState logState = storage.getPersistedLogState();
        append(logState.flatten());
        return waitForHighestConsecutive();
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

    public void notifyListeners(ClusterMetadata prev)
    {
        ClusterMetadata metadata = committed.get();
        logger.info("Notifying listeners, prev epoch = {}, current epoch = {}", prev.epoch, metadata.epoch);
        notifyPreCommit(prev, metadata, true);
        notifyPostCommit(prev, metadata, true);
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

    public void addFilter(Predicate<Entry> filter)
    {
        logger.debug("Adding filter to pending entry buffer");
        entryFilters.add(filter);
    }

    public void clearFilters()
    {
        logger.debug("Clearing filters from pending entry buffer");
        entryFilters.removeAll(entryFilters);
    }

    /**
     * Essentially same as `ready` but throws an unchecked exception
     */
    @VisibleForTesting
    public final ClusterMetadata readyUnchecked()
    {
        try
        {
            return ready();
        }
        catch (StartupException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ClusterMetadata ready() throws StartupException
    {
        ClusterMetadata metadata = replayPersisted();
        for (Startup.AfterReplay ar : spec.afterReplay)
            ar.accept(metadata);
        logger.info("Marking LocalLog ready at epoch {}", metadata.epoch);

        if (!replayComplete.compareAndSet(false, true))
            throw new IllegalStateException("Log is already fully initialised");

        logger.debug("Marking LocalLog ready at epoch {}", committed.get().epoch);
        if (spec.defaultListeners)
        {
            logger.info("Adding default listeners to LocalLog");
            addListeners();
        }
        else
        {
            logger.info("Adding specified listeners to LocalLog");
            spec.listeners.forEach(this::addListener);
            spec.changeListeners.forEach(this::addListener);
            spec.asyncChangeListeners.forEach(this::addListener);
        }

        logger.info("Notifying all registered listeners of both pre and post commit event");
        notifyListeners(spec.prev);
        return metadata;
    }

    private static class Async extends LocalLog
    {
        private final AsyncRunnable runnable;
        private final Interruptible executor;

        private Async(LogSpec logSpec)
        {
            super(logSpec);
            this.runnable = new AsyncRunnable();
            this.executor = ExecutorFactory.Global.executorFactory().infiniteLoop("GlobalLogFollower", runnable, SAFE, NON_DAEMON, UNSYNCHRONIZED);
        }

        @Override
        public ClusterMetadata awaitAtLeast(Epoch epoch) throws InterruptedException, TimeoutException
        {
            ClusterMetadata lastSeen = committed.get();
            return lastSeen.epoch.compareTo(epoch) >= 0
                   ? lastSeen
                   : new AwaitCommit(epoch).get();
        }

        @Override
        public void runOnce(DurationSpec duration) throws TimeoutException
        {
            if (executor.isTerminated())
                throw new IllegalStateException("Global log follower has shutdown");

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

                        current.awaitThrowUncheckedOnInterrupt();
                    }
                    else if (!current.awaitThrowUncheckedOnInterrupt(duration.to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
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
                    ours.awaitThrowUncheckedOnInterrupt();
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
                WaitQueue.Signal signal = null;
                try
                {
                    if (state != Interruptible.State.SHUTTING_DOWN)
                    {
                        Condition condition = subscriber.getAndSet(null);
                        // Grab a ticket ahead of time, so that we can't get into race with the exit from process pending
                        signal = logNotifier.register();
                        processPendingInternal();
                        if (condition != null)
                            condition.signalAll();
                        // if no new threads have subscribed since we started running, await
                        // otherwise, run again to process whatever work they may be waiting on
                        if (subscriber.get() == null)
                        {
                            signal.await();
                            signal = null;
                        }
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
                finally
                {
                    // If signal was not consumed for some reason, cancel it
                    if (signal != null)
                        signal.cancel();
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
        private Sync(LogSpec logSpec)
        {
            super(logSpec);
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

    protected void addListeners()
    {
        listeners.clear();
        changeListeners.clear();
        asyncChangeListeners.clear();

        addListener(snapshotListener());
        addListener(new InitializationListener());
        addListener(new SchemaListener(spec.loadSSTables));
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
                List<NodeId> list = new ArrayList<>(metadata.success().metadata.fullCMSMemberIds());
                list.sort(Comparator.comparingInt(NodeId::id));
                if (list.get(0).equals(metadata.success().metadata.myNodeId()))
                    ScheduledExecutors.nonPeriodicTasks.submit(() -> ClusterMetadataService.instance().triggerSnapshot());
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