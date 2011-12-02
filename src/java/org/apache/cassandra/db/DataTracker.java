/**
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
 */

package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.IntervalTree.Interval;
import org.apache.cassandra.utils.IntervalTree.IntervalTree;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

public class DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

    public Collection<INotificationConsumer> subscribers = new CopyOnWriteArrayList<INotificationConsumer>();

    public final ColumnFamilyStore cfstore;

    private final AtomicReference<View> view;

    // On disk live and total size
    private final AtomicLong liveSize = new AtomicLong();
    private final AtomicLong totalSize = new AtomicLong();

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
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replaceFlushed(memtable, sstable);
        }
        while (!view.compareAndSet(currentView, newView));

        addNewSSTablesSize(Arrays.asList(sstable));
        cfstore.updateCacheSizes();

        notifyAdded(sstable);
        incrementallyBackup(sstable);
    }

    public void incrementallyBackup(final SSTableReader sstable)
    {
        if (!DatabaseDescriptor.incrementalBackupsEnabled())
            return;

        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                File keyspaceDir = new File(sstable.getFilename()).getParentFile();
                File backupsDir = new File(keyspaceDir, "backups");
                if (!backupsDir.exists() && !backupsDir.mkdirs())
                    throw new IOException("Unable to create " + backupsDir);
                sstable.createLinks(backupsDir.getCanonicalPath());
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
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.unmarkCompacting(unmark);
        }
        while (!view.compareAndSet(currentView, newView));
    }

    public void markCompacted(Collection<SSTableReader> sstables)
    {
        replace(sstables, Collections.<SSTableReader>emptyList());
        notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList());
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements)
    {
        replace(sstables, replacements);
        notifySSTablesChanged(sstables, replacements);
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

    public void removeAllSSTables()
    {
        List<SSTableReader> sstables = getSSTables();
        replace(sstables, Collections.<SSTableReader>emptyList());
        notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList());
    }

    /** (Re)initializes the tracker, purging all references. */
    void init()
    {
        view.set(new View(new Memtable(cfstore),
                          Collections.<Memtable>emptySet(),
                          Collections.<SSTableReader>emptyList(),
                          Collections.<SSTableReader>emptySet(),
                          new IntervalTree()));
    }

    private void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replace(oldSSTables, replacements);
        }
        while (!view.compareAndSet(currentView, newView));

        addNewSSTablesSize(replacements);
        removeOldSSTablesSize(oldSSTables);

        cfstore.updateCacheSizes();
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
            liveSize.addAndGet(size);
            totalSize.addAndGet(size);
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
            liveSize.addAndGet(-sstable.bytesOnDisk());
            sstable.markCompacted();
            sstable.releaseReference();
        }
    }

    public AutoSavingCache<Pair<Descriptor,DecoratedKey>,Long> getKeyCache()
    {
        return cfstore.getKeyCache();
    }

    public long getLiveSize()
    {
        return liveSize.get();
    }

    public long getTotalSize()
    {
        return totalSize.get();
    }

    public void spaceReclaimed(long size)
    {
        totalSize.addAndGet(-size);
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

    public long[] getEstimatedRowSizeHistogram()
    {
        long[] histogram = new long[90];

        for (SSTableReader sstable : getSSTables())
        {
            long[] rowSize = sstable.getEstimatedRowSize().getBuckets(false);

            for (int i = 0; i < histogram.length; i++)
                histogram[i] += rowSize[i];
        }

        return histogram;
    }

    public long[] getEstimatedColumnCountHistogram()
    {
        long[] histogram = new long[90];

        for (SSTableReader sstable : getSSTables())
        {
            long[] columnSize = sstable.getEstimatedColumnCount().getBuckets(false);

            for (int i = 0; i < histogram.length; i++)
                histogram[i] += columnSize[i];
        }

        return histogram;
    }

    public double getCompressionRatio()
    {
        double sum = 0;
        int total = 0;
        for (SSTableReader sstable : getSSTables())
        {
            if (sstable.getCompressionRatio() != Double.MIN_VALUE)
            {
                sum += sstable.getCompressionRatio();
                total++;
            }
        }
        return total != 0 ? (double)sum/total: 0;
    }

    public long getMinRowSize()
    {
        long min = 0;
        for (SSTableReader sstable : getSSTables())
        {
            if (min == 0 || sstable.getEstimatedRowSize().min() < min)
                min = sstable.getEstimatedRowSize().min();
        }
        return min;
    }

    public long getMaxRowSize()
    {
        long max = 0;
        for (SSTableReader sstable : getSSTables())
        {
            if (sstable.getEstimatedRowSize().max() > max)
                max = sstable.getEstimatedRowSize().max();
        }
        return max;
    }

    public long getMeanRowSize()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables())
        {
            sum += sstable.getEstimatedRowSize().mean();
            count++;
        }
        return count > 0 ? sum / count : 0;
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

    public long getBloomFilterFalsePositives()
    {
        long count = 0L;
        for (SSTableReader sstable: getSSTables())
        {
            count += sstable.getBloomFilterFalsePositiveCount();
        }
        return count;
    }

    public long getRecentBloomFilterFalsePositives()
    {
        long count = 0L;
        for (SSTableReader sstable: getSSTables())
        {
            count += sstable.getRecentBloomFilterFalsePositiveCount();
        }
        return count;
    }

    public double getBloomFilterFalseRatio()
    {
        long falseCount = 0L;
        long trueCount = 0L;
        for (SSTableReader sstable: getSSTables())
        {
            falseCount += sstable.getBloomFilterFalsePositiveCount();
            trueCount += sstable.getBloomFilterTruePositiveCount();
        }
        if (falseCount == 0L && trueCount == 0L)
            return 0d;
        return (double) falseCount / (trueCount + falseCount);
    }

    public double getRecentBloomFilterFalseRatio()
    {
        long falseCount = 0L;
        long trueCount = 0L;
        for (SSTableReader sstable: getSSTables())
        {
            falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
            trueCount += sstable.getRecentBloomFilterTruePositiveCount();
        }
        if (falseCount == 0L && trueCount == 0L)
            return 0d;
        return (double) falseCount / (trueCount + falseCount);
    }

    public void notifySSTablesChanged(Iterable<SSTableReader> removed, Iterable<SSTableReader> added)
    {
        for (INotificationConsumer subscriber : subscribers)
        {
            INotification notification = new SSTableListChangedNotification(added, removed);
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
        public final IntervalTree<SSTableReader> intervalTree;

        View(Memtable memtable, Set<Memtable> pendingFlush, List<SSTableReader> sstables, Set<SSTableReader> compacting, IntervalTree<SSTableReader> intervalTree)
        {
            this.memtable = memtable;
            this.memtablesPendingFlush = pendingFlush;
            this.sstables = sstables;
            this.compacting = compacting;
            this.intervalTree = intervalTree;
        }

        private IntervalTree buildIntervalTree(List<SSTableReader> sstables)
        {
            List<Interval> intervals = new ArrayList<Interval>(sstables.size());
            for (SSTableReader sstable : sstables)
                intervals.add(new Interval<SSTableReader>(sstable.first, sstable.last, sstable));
            return new IntervalTree<SSTableReader>(intervals);
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
            IntervalTree intervalTree = buildIntervalTree(newSSTables);
            return new View(memtable, newPending, Collections.unmodifiableList(newSSTables), compacting, intervalTree);
        }

        public View replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            List<SSTableReader> newSSTables = newSSTables(oldSSTables, replacements);
            IntervalTree intervalTree = buildIntervalTree(newSSTables);
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
