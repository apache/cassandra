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
package org.apache.cassandra.db;

import java.io.File;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicate;
import com.google.common.collect.*;

import org.apache.cassandra.utils.concurrent.OpOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

    public final Collection<INotificationConsumer> subscribers = new CopyOnWriteArrayList<>();
    public final ColumnFamilyStore cfstore;
    private final AtomicReference<View> view;

    public DataTracker(ColumnFamilyStore cfstore)
    {
        this.cfstore = cfstore;
        this.view = new AtomicReference<>();
        this.init();
    }

    // get the Memtable that the ordered writeOp should be directed to
    public Memtable getMemtableFor(OpOrder.Group opGroup)
    {
        // since any new memtables appended to the list after we fetch it will be for operations started
        // after us, we can safely assume that we will always find the memtable that 'accepts' us;
        // if the barrier for any memtable is set whilst we are reading the list, it must accept us.

        // there may be multiple memtables in the list that would 'accept' us, however we only ever choose
        // the oldest such memtable, as accepts() only prevents us falling behind (i.e. ensures we don't
        // assign operations to a memtable that was retired/queued before we started)
        for (Memtable memtable : view.get().liveMemtables)
        {
            if (memtable.accepts(opGroup))
                return memtable;
        }
        throw new AssertionError(view.get().liveMemtables.toString());
    }

    public Set<SSTableReader> getSSTables()
    {
        return view.get().sstables;
    }

    public Set<SSTableReader> getUncompactingSSTables()
    {
        return view.get().nonCompactingSStables();
    }

    public Iterable<SSTableReader> getUncompactingSSTables(Iterable<SSTableReader> candidates)
    {
        final View v = view.get();
        return Iterables.filter(candidates, new Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader sstable)
            {
                return !v.compacting.contains(sstable);
            }
        });
    }

    public View getView()
    {
        return view.get();
    }

    /**
     * Switch the current memtable. This atomically appends a new memtable to the end of the list of active memtables,
     * returning the previously last memtable. It leaves the previous Memtable in the list of live memtables until
     * discarding(memtable) is called. These two methods must be synchronized/paired, i.e. m = switchMemtable
     * must be followed by discarding(m), they cannot be interleaved.
     *
     * @return the previously active memtable
     */
    public Memtable switchMemtable(boolean truncating)
    {
        Memtable newMemtable = new Memtable(cfstore);
        Memtable toFlushMemtable;
        View currentView, newView;
        do
        {
            currentView = view.get();
            toFlushMemtable = currentView.getCurrentMemtable();
            newView = currentView.switchMemtable(newMemtable);
        }
        while (!view.compareAndSet(currentView, newView));

        if (truncating)
            notifyRenewed(newMemtable);

        return toFlushMemtable;
    }

    public void markFlushing(Memtable memtable)
    {
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.markFlushing(memtable);
        }
        while (!view.compareAndSet(currentView, newView));
    }

    public void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        // sstable may be null if we flushed batchlog and nothing needed to be retained

        if (!cfstore.isValid())
        {
            View currentView, newView;
            do
            {
                currentView = view.get();
                newView = currentView.replaceFlushed(memtable, sstable);
                if (sstable != null)
                    newView = newView.replace(Arrays.asList(sstable), Collections.<SSTableReader>emptyList());
            }
            while (!view.compareAndSet(currentView, newView));
            return;
        }

        // back up before creating a new View (which makes the new one eligible for compaction)
        if (sstable != null)
            maybeIncrementallyBackup(sstable);

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replaceFlushed(memtable, sstable);
        }
        while (!view.compareAndSet(currentView, newView));

        if (sstable != null)
        {
            addNewSSTablesSize(Arrays.asList(sstable));
            notifyAdded(sstable);
        }
    }

    public void maybeIncrementallyBackup(final SSTableReader sstable)
    {
        if (!DatabaseDescriptor.isIncrementalBackupsEnabled())
            return;

        File backupsDir = Directories.getBackupsDirectory(sstable.descriptor);
        sstable.createLinks(FileUtils.getCanonicalPath(backupsDir));
    }

    /**
     * @return true if we are able to mark the given @param sstables as compacted, before anyone else
     *
     * Note that we could acquire references on the marked sstables and release them in
     * unmarkCompacting, but since we will never call markObsolete on a sstable marked
     * as compacting (unless there is a serious bug), we can skip this.
     */
    public boolean markCompacting(Iterable<SSTableReader> sstables)
    {
        assert sstables != null && !Iterables.isEmpty(sstables);

        View currentView = view.get();
        Set<SSTableReader> inactive = Sets.difference(ImmutableSet.copyOf(sstables), currentView.compacting);
        if (inactive.size() < Iterables.size(sstables))
            return false;

        View newView = currentView.markCompacting(inactive);
        return view.compareAndSet(currentView, newView);
    }

    /**
     * Removes files from compacting status: this is different from 'markObsolete'
     * because it should be run regardless of whether a compaction succeeded.
     */
    public void unmarkCompacting(Iterable<SSTableReader> unmark)
    {
        boolean isValid = cfstore.isValid();
        if (!isValid)
        {
            // The CF has been dropped.  We don't know if the original compaction suceeded or failed,
            // which makes it difficult to know if the sstable reference has already been released.
            // A "good enough" approach is to mark the sstables involved obsolete, which if compaction succeeded
            // is harmlessly redundant, and if it failed ensures that at least the sstable will get deleted on restart.
            for (SSTableReader sstable : unmark)
                sstable.markObsolete();
        }

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.unmarkCompacting(unmark);
        }
        while (!view.compareAndSet(currentView, newView));

        if (!isValid)
        {
            // when the CFS is invalidated, it will call unreferenceSSTables().  However, unreferenceSSTables only deals
            // with sstables that aren't currently being compacted.  If there are ongoing compactions that finish or are
            // interrupted after the CFS is invalidated, those sstables need to be unreferenced as well, so we do that here.
            unreferenceSSTables();
        }
    }

    public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        replace(sstables, Collections.<SSTableReader>emptyList());
        notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList(), compactionType);
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Collection<SSTableReader> replacements, OperationType compactionType)
    {
        replace(sstables, replacements);
        notifySSTablesChanged(sstables, replacements, compactionType);
    }

    public void addInitialSSTables(Collection<SSTableReader> sstables)
    {
        replace(Collections.<SSTableReader>emptyList(), sstables);
        // no notifications or backup necessary
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        replace(Collections.<SSTableReader>emptyList(), sstables);
        for (SSTableReader sstable : sstables)
        {
            maybeIncrementallyBackup(sstable);
            notifyAdded(sstable);
        }
    }

    /**
     * removes all sstables that are not busy compacting.
     */
    public void unreferenceSSTables()
    {
        Set<SSTableReader> notCompacting;

        View currentView, newView;
        do
        {
            currentView = view.get();
            notCompacting = currentView.nonCompactingSStables();
            newView = currentView.replace(notCompacting, Collections.<SSTableReader>emptySet());
        }
        while (!view.compareAndSet(currentView, newView));

        if (notCompacting.isEmpty())
        {
            // notifySSTablesChanged -> LeveledManifest.promote doesn't like a no-op "promotion"
            return;
        }
        notifySSTablesChanged(notCompacting, Collections.<SSTableReader>emptySet(), OperationType.UNKNOWN);
        postReplace(notCompacting, Collections.<SSTableReader>emptySet(), true);
    }

    /**
     * Removes every SSTable in the directory from the DataTracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void removeUnreadableSSTables(File directory)
    {
        View currentView, newView;
        List<SSTableReader> remaining = new ArrayList<>();
        do
        {
            currentView = view.get();
            for (SSTableReader r : currentView.nonCompactingSStables())
                if (!r.descriptor.directory.equals(directory))
                    remaining.add(r);

            if (remaining.size() == currentView.nonCompactingSStables().size())
                return;

            newView = currentView.replace(currentView.sstables, remaining);
        }
        while (!view.compareAndSet(currentView, newView));
        notifySSTablesChanged(remaining, Collections.<SSTableReader>emptySet(), OperationType.UNKNOWN);
    }

    /** (Re)initializes the tracker, purging all references. */
    void init()
    {
        view.set(new View(
                ImmutableList.of(new Memtable(cfstore)),
                ImmutableList.<Memtable>of(),
                Collections.<SSTableReader>emptySet(),
                Collections.<SSTableReader>emptySet(),
                SSTableIntervalTree.empty()));
    }

    /**
     * A special kind of replacement for SSTableReaders that were cloned with a new index summary sampling level (see
     * SSTableReader.cloneWithNewSummarySamplingLevel and CASSANDRA-5519).  This does not mark the old reader
     * as compacted.
     * @param oldSSTables replaced readers
     * @param newSSTables replacement readers
     */
    public void replaceReaders(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables)
    {
        // data component will be unchanged but the index summary will be a different size
        // (since we save that to make restart fast)
        long sizeIncrease = 0;
        for (SSTableReader sstable : oldSSTables)
            sizeIncrease -= sstable.bytesOnDisk();
        for (SSTableReader sstable : newSSTables)
            sizeIncrease += sstable.bytesOnDisk();

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replace(oldSSTables, newSSTables);
        }
        while (!view.compareAndSet(currentView, newView));

        StorageMetrics.load.inc(sizeIncrease);
        cfstore.metric.liveDiskSpaceUsed.inc(sizeIncrease);

        for (SSTableReader sstable : newSSTables)
            sstable.setTrackedBy(this);

        for (SSTableReader sstable : oldSSTables)
            sstable.releaseReference();
    }

    private void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        if (!cfstore.isValid())
        {
            removeOldSSTablesSize(replacements, false);
            replacements = Collections.emptyList();
        }

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replace(oldSSTables, replacements);
        }
        while (!view.compareAndSet(currentView, newView));

        postReplace(oldSSTables, replacements, false);
    }

    private void postReplace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements, boolean tolerateCompacted)
    {
        addNewSSTablesSize(replacements);
        removeOldSSTablesSize(oldSSTables, tolerateCompacted);
    }

    private void addNewSSTablesSize(Iterable<SSTableReader> newSSTables)
    {
        for (SSTableReader sstable : newSSTables)
        {
            if (logger.isDebugEnabled())
                logger.debug(String.format("adding %s to list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.keyspace.getName(), cfstore.name));
            long size = sstable.bytesOnDisk();
            StorageMetrics.load.inc(size);
            cfstore.metric.liveDiskSpaceUsed.inc(size);
            cfstore.metric.totalDiskSpaceUsed.inc(size);
            sstable.setTrackedBy(this);
        }
    }

    private void removeOldSSTablesSize(Iterable<SSTableReader> oldSSTables, boolean tolerateCompacted)
    {
        for (SSTableReader sstable : oldSSTables)
        {
            if (logger.isDebugEnabled())
                logger.debug(String.format("removing %s from list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.keyspace.getName(), cfstore.name));
            long size = sstable.bytesOnDisk();
            StorageMetrics.load.dec(size);
            cfstore.metric.liveDiskSpaceUsed.dec(size);

            // tolerateCompacted will be true when the CFS is no longer valid (dropped). If there were ongoing
            // compactions when it was invalidated, sstables may already be marked compacted, so we should
            // tolerate that (see CASSANDRA-5957)
            boolean firstToCompact = sstable.markObsolete();
            assert (tolerateCompacted || firstToCompact) : sstable + " was already marked compacted";

            sstable.releaseReference();
        }
    }

    public void spaceReclaimed(long size)
    {
        cfstore.metric.totalDiskSpaceUsed.dec(size);
    }

    public long estimatedKeys()
    {
        long n = 0;
        for (SSTableReader sstable : getSSTables())
            n += sstable.estimatedKeys();
        return n;
    }

    public int getMeanColumns()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables())
        {
            long n = sstable.getEstimatedColumnCount().count();
            sum += sstable.getEstimatedColumnCount().mean() * n;
            count += n;
        }
        return count > 0 ? (int) (sum / count) : 0;
    }

    public double getDroppableTombstoneRatio()
    {
        double allDroppable = 0;
        long allColumns = 0;
        int localTime = (int)(System.currentTimeMillis()/1000);

        for (SSTableReader sstable : getSSTables())
        {
            allDroppable += sstable.getDroppableTombstonesBefore(localTime - sstable.metadata.getGcGraceSeconds());
            allColumns += sstable.getEstimatedColumnCount().mean() * sstable.getEstimatedColumnCount().count();
        }
        return allColumns > 0 ? allDroppable / allColumns : 0;
    }

    public void notifySSTablesChanged(Collection<SSTableReader> removed, Collection<SSTableReader> added, OperationType compactionType)
    {
        INotification notification = new SSTableListChangedNotification(added, removed, compactionType);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyAdded(SSTableReader added)
    {
        INotification notification = new SSTableAddedNotification(added);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifySSTableRepairedStatusChanged(Collection<SSTableReader> repairStatusesChanged)
    {
        INotification notification = new SSTableRepairStatusChanged(repairStatusesChanged);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);

    }

    public void notifyDeleting(SSTableReader deleting)
    {
        INotification notification = new SSTableDeletingNotification(deleting);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyRenewed(Memtable renewed)
    {
        INotification notification = new MemtableRenewedNotification(renewed);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void subscribe(INotificationConsumer consumer)
    {
        subscribers.add(consumer);
    }

    public void unsubscribe(INotificationConsumer consumer)
    {
        subscribers.remove(consumer);
    }

    public static SSTableIntervalTree buildIntervalTree(Iterable<SSTableReader> sstables)
    {
        List<Interval<RowPosition, SSTableReader>> intervals = new ArrayList<>(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            intervals.add(Interval.<RowPosition, SSTableReader>create(sstable.first, sstable.last, sstable));
        return new SSTableIntervalTree(intervals);
    }

    public Set<SSTableReader> getCompacting()
    {
        return getView().compacting;
    }

    public static class SSTableIntervalTree extends IntervalTree<RowPosition, SSTableReader, Interval<RowPosition, SSTableReader>>
    {
        private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree(null);

        private SSTableIntervalTree(Collection<Interval<RowPosition, SSTableReader>> intervals)
        {
            super(intervals, null);
        }

        public static SSTableIntervalTree empty()
        {
            return EMPTY;
        }
    }

    /**
     * An immutable structure holding the current memtable, the memtables pending
     * flush, the sstables for a column family, and the sstables that are active
     * in compaction (a subset of the sstables).
     */
    public static class View
    {
        /**
         * ordinarily a list of size 1, but when preparing to flush will contain both the memtable we will flush
         * and the new replacement memtable, until all outstanding write operations on the old table complete.
         * The last item in the list is always the "current" memtable.
         */
        private final List<Memtable> liveMemtables;
        /**
         * contains all memtables that are no longer referenced for writing and are queued for / in the process of being
         * flushed. In chronologically ascending order.
         */
        private final List<Memtable> flushingMemtables;
        public final Set<SSTableReader> compacting;
        public final Set<SSTableReader> sstables;
        public final SSTableIntervalTree intervalTree;

        View(List<Memtable> liveMemtables, List<Memtable> flushingMemtables, Set<SSTableReader> sstables, Set<SSTableReader> compacting, SSTableIntervalTree intervalTree)
        {
            this.liveMemtables = liveMemtables;
            this.flushingMemtables = flushingMemtables;
            this.sstables = sstables;
            this.compacting = compacting;
            this.intervalTree = intervalTree;
        }

        public Memtable getOldestMemtable()
        {
            if (!flushingMemtables.isEmpty())
                return flushingMemtables.get(0);
            return liveMemtables.get(0);
        }

        public Memtable getCurrentMemtable()
        {
            return liveMemtables.get(liveMemtables.size() - 1);
        }

        public Iterable<Memtable> getMemtablesPendingFlush()
        {
            if (liveMemtables.size() == 1)
                return flushingMemtables;
            return Iterables.concat(liveMemtables.subList(0, 1), flushingMemtables);
        }

        /**
         * @return the active memtable and all the memtables that are pending flush.
         */
        public Iterable<Memtable> getAllMemtables()
        {
            return Iterables.concat(flushingMemtables, liveMemtables);
        }

        public Sets.SetView<SSTableReader> nonCompactingSStables()
        {
            return Sets.difference(ImmutableSet.copyOf(sstables), compacting);
        }

        View switchMemtable(Memtable newMemtable)
        {
            List<Memtable> newLiveMemtables = ImmutableList.<Memtable>builder().addAll(liveMemtables).add(newMemtable).build();
            return new View(newLiveMemtables, flushingMemtables, sstables, compacting, intervalTree);
        }

        View markFlushing(Memtable toFlushMemtable)
        {
            List<Memtable> live = liveMemtables, flushing = flushingMemtables;

            // since we can have multiple flushes queued, we may occasionally race and start a flush out of order,
            // so must locate it in the list to remove, rather than just removing from the beginning
            int i = live.indexOf(toFlushMemtable);
            assert i < live.size() - 1;
            List<Memtable> newLive = ImmutableList.<Memtable>builder()
                                                  .addAll(live.subList(0, i))
                                                  .addAll(live.subList(i + 1, live.size()))
                                                  .build();

            // similarly, if we out-of-order markFlushing once, we may afterwards need to insert a memtable into the
            // flushing list in a position other than the end, though this will be rare
            i = flushing.size();
            while (i > 0 && flushing.get(i - 1).creationTime() > toFlushMemtable.creationTime())
                i--;
            List<Memtable> newFlushing = ImmutableList.<Memtable>builder()
                                                      .addAll(flushing.subList(0, i))
                                                      .add(toFlushMemtable)
                                                      .addAll(flushing.subList(i, flushing.size()))
                                                      .build();

            return new View(newLive, newFlushing, sstables, compacting, intervalTree);
        }

        View replaceFlushed(Memtable flushedMemtable, SSTableReader newSSTable)
        {
            int index = flushingMemtables.indexOf(flushedMemtable);
            List<Memtable> newQueuedMemtables = ImmutableList.<Memtable>builder()
                                                             .addAll(flushingMemtables.subList(0, index))
                                                             .addAll(flushingMemtables.subList(index + 1, flushingMemtables.size()))
                                                             .build();
            Set<SSTableReader> newSSTables = newSSTable == null
                                             ? sstables
                                             : newSSTables(newSSTable);
            SSTableIntervalTree intervalTree = buildIntervalTree(newSSTables);
            return new View(liveMemtables, newQueuedMemtables, newSSTables, compacting, intervalTree);
        }

        View replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            Set<SSTableReader> newSSTables = newSSTables(oldSSTables, replacements);
            SSTableIntervalTree intervalTree = buildIntervalTree(newSSTables);
            return new View(liveMemtables, flushingMemtables, newSSTables, compacting, intervalTree);
        }

        View markCompacting(Collection<SSTableReader> tomark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.<SSTableReader>builder().addAll(compacting).addAll(tomark).build();
            return new View(liveMemtables, flushingMemtables, sstables, compactingNew, intervalTree);
        }

        View unmarkCompacting(Iterable<SSTableReader> tounmark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.copyOf(Sets.difference(compacting, ImmutableSet.copyOf(tounmark)));
            return new View(liveMemtables, flushingMemtables, sstables, compactingNew, intervalTree);
        }

        private Set<SSTableReader> newSSTables(SSTableReader newSSTable)
        {
            assert newSSTable != null;
            // not performance-sensitive, don't obsess over doing a selection merge here
            return newSSTables(Collections.<SSTableReader>emptyList(), Collections.singletonList(newSSTable));
        }

        private Set<SSTableReader> newSSTables(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            ImmutableSet<SSTableReader> oldSet = ImmutableSet.copyOf(oldSSTables);
            int newSSTablesSize = sstables.size() - oldSSTables.size() + Iterables.size(replacements);
            assert newSSTablesSize >= Iterables.size(replacements) : String.format("Incoherent new size %d replacing %s by %s in %s", newSSTablesSize, oldSSTables, replacements, this);
            Set<SSTableReader> newSSTables = new HashSet<>(newSSTablesSize);

            for (SSTableReader sstable : sstables)
                if (!oldSet.contains(sstable))
                    newSSTables.add(sstable);

            Iterables.addAll(newSSTables, replacements);
            assert newSSTables.size() == newSSTablesSize : String.format("Expecting new size of %d, got %d while replacing %s by %s in %s", newSSTablesSize, newSSTables.size(), oldSSTables, replacements, this);
            return ImmutableSet.copyOf(newSSTables);
        }

        @Override
        public String toString()
        {
            return String.format("View(pending_count=%d, sstables=%s, compacting=%s)", liveMemtables.size() + flushingMemtables.size() - 1, sstables, compacting);
        }
    }
}
