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

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

    public final Collection<INotificationConsumer> subscribers = new CopyOnWriteArrayList<INotificationConsumer>();
    public final ColumnFamilyStore cfstore;
    private final AtomicReference<View> view;

    public DataTracker(ColumnFamilyStore cfstore)
    {
        this.cfstore = cfstore;
        this.view = new AtomicReference<View>();
        this.init();
    }

    public Memtable getMemtable()
    {
        return view.get().memtable;
    }

    public Set<Memtable> getMemtablesPendingFlush()
    {
        return view.get().memtablesPendingFlush;
    }

    public List<SSTableReader> getSSTables()
    {
        return view.get().sstables;
    }

    public Set<SSTableReader> getUncompactingSSTables()
    {
        return view.get().nonCompactingSStables();
    }

    public View getView()
    {
        return view.get();
    }

    /**
     * Switch the current memtable.
     * This atomically adds the current memtable to the memtables pending
     * flush and replace it with a fresh memtable.
     *
     * @return the previous current memtable (the one added to the pending
     * flush)
     */
    public Memtable switchMemtable()
    {
        // atomically change the current memtable
        Memtable newMemtable = new Memtable(cfstore);
        Memtable toFlushMemtable;
        View currentView, newView;
        do
        {
            currentView = view.get();
            toFlushMemtable = currentView.memtable;
            newView = currentView.switchMemtable(newMemtable);
        }
        while (!view.compareAndSet(currentView, newView));

        return toFlushMemtable;
    }

    /**
     * Renew the current memtable without putting the old one for a flush.
     * Used when we flush but a memtable is clean (in which case we must
     * change it because it was frozen).
     */
    public void renewMemtable()
    {
        Memtable newMemtable = new Memtable(cfstore);
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.renewMemtable(newMemtable);
        }
        while (!view.compareAndSet(currentView, newView));
    }

    public void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        if (!cfstore.isValid())
        {
            View currentView, newView;
            do
            {
                currentView = view.get();
                newView = currentView.replaceFlushed(memtable, sstable).replace(Arrays.asList(sstable), Collections.<SSTableReader>emptyList());
            }
            while (!view.compareAndSet(currentView, newView));
            return;
        }

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replaceFlushed(memtable, sstable);
        }
        while (!view.compareAndSet(currentView, newView));

        addNewSSTablesSize(Arrays.asList(sstable));

        notifyAdded(sstable);
        incrementallyBackup(sstable);
    }

    public void incrementallyBackup(final SSTableReader sstable)
    {
        if (!DatabaseDescriptor.isIncrementalBackupsEnabled())
            return;

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                File backupsDir = Directories.getBackupsDirectory(sstable.descriptor);
                sstable.createLinks(FileUtils.getCanonicalPath(backupsDir));
            }
        };
        StorageService.tasks.execute(runnable);
    }

    /**
     * @return A subset of the given active sstables that have been marked compacting,
     * or null if the thresholds cannot be met: files that are marked compacting must
     * later be unmarked using unmarkCompacting.
     *
     * Note that we could acquire references on the marked sstables and release them in
     * unmarkCompacting, but since we will never call markCompacted on a sstable marked
     * as compacting (unless there is a serious bug), we can skip this.
     */
    public Set<SSTableReader> markCompacting(Collection<SSTableReader> tomark, int min, int max)
    {
        if (max < min || max < 1)
            return null;
        if (tomark == null || tomark.isEmpty())
            return null;

        View currentView, newView;
        Set<SSTableReader> subset = null;
        // order preserving set copy of the input
        Set<SSTableReader> remaining = new LinkedHashSet<SSTableReader>(tomark);
        do
        {
            currentView = view.get();

            // find the subset that is active and not already compacting
            remaining.removeAll(currentView.compacting);
            remaining.retainAll(currentView.sstables);
            if (remaining.size() < min)
                // cannot meet the min threshold
                return null;

            // cap the newly compacting items into a subset set
            subset = new HashSet<SSTableReader>();
            Iterator<SSTableReader> iter = remaining.iterator();
            for (int added = 0; added < max && iter.hasNext(); added++)
                subset.add(iter.next());

            newView = currentView.markCompacting(subset);
        }
        while (!view.compareAndSet(currentView, newView));
        return subset;
    }

    /**
     * Removes files from compacting status: this is different from 'markCompacted'
     * because it should be run regardless of whether a compaction succeeded.
     */
    public void unmarkCompacting(Collection<SSTableReader> unmark)
    {
        if (!cfstore.isValid())
        {
            // We don't know if the original compaction suceeded or failed, which makes it difficult to know
            // if the sstable reference has already been released.
            // A "good enough" approach is to mark the sstables involved compacted, which if compaction succeeded
            // is harmlessly redundant, and if it failed ensures that at least the sstable will get deleted on restart.
            for (SSTableReader sstable : unmark)
                sstable.markCompacted();
        }

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.unmarkCompacting(unmark);
        }
        while (!view.compareAndSet(currentView, newView));
    }

    public void markCompacted(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        replace(sstables, Collections.<SSTableReader>emptyList());
        notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList(), compactionType);
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements, OperationType compactionType)
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
            incrementallyBackup(sstable);
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
        postReplace(notCompacting, Collections.<SSTableReader>emptySet());
    }

    /**
     * Removes every SSTable in the directory from the DataTracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void removeUnreadableSSTables(File directory)
    {
        View currentView, newView;
        List<SSTableReader> remaining = new ArrayList<SSTableReader>();
        do
        {
            currentView = view.get();
            for (SSTableReader r : currentView.nonCompactingSStables())
            {
                if (!r.descriptor.directory.equals(directory))
                    remaining.add(r);
            }

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
        view.set(new View(new Memtable(cfstore),
                          Collections.<Memtable>emptySet(),
                          Collections.<SSTableReader>emptyList(),
                          Collections.<SSTableReader>emptySet(),
                          SSTableIntervalTree.empty()));
    }

    private void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        if (!cfstore.isValid())
        {
            removeOldSSTablesSize(replacements);
            replacements = Collections.emptyList();
        }

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replace(oldSSTables, replacements);
        }
        while (!view.compareAndSet(currentView, newView));

        postReplace(oldSSTables, replacements);
    }

    private void postReplace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        addNewSSTablesSize(replacements);
        removeOldSSTablesSize(oldSSTables);
    }

    private void addNewSSTablesSize(Iterable<SSTableReader> newSSTables)
    {
        for (SSTableReader sstable : newSSTables)
        {
            assert sstable.getKeySamples() != null;
            if (logger.isDebugEnabled())
                logger.debug(String.format("adding %s to list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.table.name, cfstore.getColumnFamilyName()));
            long size = sstable.bytesOnDisk();
            StorageMetrics.load.inc(size);
            cfstore.metric.liveDiskSpaceUsed.inc(size);
            cfstore.metric.totalDiskSpaceUsed.inc(size);
            sstable.setTrackedBy(this);
        }
    }

    private void removeOldSSTablesSize(Iterable<SSTableReader> oldSSTables)
    {
        for (SSTableReader sstable : oldSSTables)
        {
            if (logger.isDebugEnabled())
                logger.debug(String.format("removing %s from list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.table.name, cfstore.getColumnFamilyName()));
            long size = sstable.bytesOnDisk();
            StorageMetrics.load.dec(size);
            cfstore.metric.liveDiskSpaceUsed.dec(size);
            boolean firstToCompact = sstable.markCompacted();
            assert firstToCompact : sstable + " was already marked compacted";
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
        {
            n += sstable.estimatedKeys();
        }
        return n;
    }

    public int getMeanColumns()
    {
        long sum = 0;
        int count = 0;
        for (SSTableReader sstable : getSSTables())
        {
            sum += sstable.getEstimatedColumnCount().mean();
            count++;
        }
        return count > 0 ? (int) (sum / count) : 0;
    }

    public void notifySSTablesChanged(Iterable<SSTableReader> removed, Iterable<SSTableReader> added, OperationType compactionType)
    {
        for (INotificationConsumer subscriber : subscribers)
        {
            INotification notification = new SSTableListChangedNotification(added, removed, compactionType);
            subscriber.handleNotification(notification, this);
        }
    }

    public void notifyAdded(SSTableReader added)
    {
        for (INotificationConsumer subscriber : subscribers)
        {
            INotification notification = new SSTableAddedNotification(added);
            subscriber.handleNotification(notification, this);
        }
    }

    public void subscribe(INotificationConsumer consumer)
    {
        subscribers.add(consumer);
    }

    public void unsubscribe(INotificationConsumer consumer)
    {
        boolean found = subscribers.remove(consumer);
        assert found : consumer + " not subscribed";
    }

    public static SSTableIntervalTree buildIntervalTree(Iterable<SSTableReader> sstables)
    {
        List<Interval<RowPosition, SSTableReader>> intervals = new ArrayList<Interval<RowPosition, SSTableReader>>(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            intervals.add(Interval.<RowPosition, SSTableReader>create(sstable.first, sstable.last, sstable));
        return new SSTableIntervalTree(intervals);
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
    static class View
    {
        public final Memtable memtable;
        public final Set<Memtable> memtablesPendingFlush;
        public final Set<SSTableReader> compacting;
        // We can't use a SortedSet here because "the ordering maintained by a sorted set (whether or not an
        // explicit comparator is provided) must be <i>consistent with equals</i>."  In particular,
        // ImmutableSortedSet will ignore any objects that compare equally with an existing Set member.
        // Obviously, dropping sstables whose max column timestamp happens to be equal to another's
        // is not acceptable for us.  So, we use a List instead.
        public final List<SSTableReader> sstables;
        public final SSTableIntervalTree intervalTree;

        View(Memtable memtable, Set<Memtable> pendingFlush, List<SSTableReader> sstables, Set<SSTableReader> compacting, SSTableIntervalTree intervalTree)
        {
            this.memtable = memtable;
            this.memtablesPendingFlush = pendingFlush;
            this.sstables = sstables;
            this.compacting = compacting;
            this.intervalTree = intervalTree;
        }

        public Sets.SetView<SSTableReader> nonCompactingSStables()
        {
            return Sets.difference(ImmutableSet.copyOf(sstables), compacting);
        }

        public View switchMemtable(Memtable newMemtable)
        {
            Set<Memtable> newPending = ImmutableSet.<Memtable>builder().addAll(memtablesPendingFlush).add(memtable).build();
            return new View(newMemtable, newPending, sstables, compacting, intervalTree);
        }

        public View renewMemtable(Memtable newMemtable)
        {
            return new View(newMemtable, memtablesPendingFlush, sstables, compacting, intervalTree);
        }

        public View replaceFlushed(Memtable flushedMemtable, SSTableReader newSSTable)
        {
            Set<Memtable> newPending = ImmutableSet.copyOf(Sets.difference(memtablesPendingFlush, Collections.singleton(flushedMemtable)));
            List<SSTableReader> newSSTables = newSSTables(newSSTable);
            SSTableIntervalTree intervalTree = buildIntervalTree(newSSTables);
            return new View(memtable, newPending, Collections.unmodifiableList(newSSTables), compacting, intervalTree);
        }

        public View replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            List<SSTableReader> newSSTables = newSSTables(oldSSTables, replacements);
            SSTableIntervalTree intervalTree = buildIntervalTree(newSSTables);
            return new View(memtable, memtablesPendingFlush, Collections.unmodifiableList(newSSTables), compacting, intervalTree);
        }

        public View markCompacting(Collection<SSTableReader> tomark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.<SSTableReader>builder().addAll(compacting).addAll(tomark).build();
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew, intervalTree);
        }

        public View unmarkCompacting(Collection<SSTableReader> tounmark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.copyOf(Sets.difference(compacting, ImmutableSet.copyOf(tounmark)));
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew, intervalTree);
        }

        private List<SSTableReader> newSSTables(SSTableReader newSSTable)
        {
            // not performance-sensitive, don't obsess over doing a selection merge here
            return newSSTables(Collections.<SSTableReader>emptyList(), Collections.singletonList(newSSTable));
        }

        private List<SSTableReader> newSSTables(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            ImmutableSet<SSTableReader> oldSet = ImmutableSet.copyOf(oldSSTables);
            int newSSTablesSize = sstables.size() - oldSSTables.size() + Iterables.size(replacements);
            assert newSSTablesSize >= Iterables.size(replacements) : String.format("Incoherent new size %d replacing %s by %s in %s", newSSTablesSize, oldSSTables, replacements, this);
            List<SSTableReader> newSSTables = new ArrayList<SSTableReader>(newSSTablesSize);
            for (SSTableReader sstable : sstables)
            {
                if (!oldSet.contains(sstable))
                    newSSTables.add(sstable);
            }
            Iterables.addAll(newSSTables, replacements);
            assert newSSTables.size() == newSSTablesSize : String.format("Expecting new size of %d, got %d while replacing %s by %s in %s", newSSTablesSize, newSSTables.size(), oldSSTables, replacements, this);
            return newSSTables;
        }

        @Override
        public String toString()
        {
            return String.format("View(pending_count=%d, sstables=%s, compacting=%s)", memtablesPendingFlush.size(), sstables, compacting);
        }
    }
}
