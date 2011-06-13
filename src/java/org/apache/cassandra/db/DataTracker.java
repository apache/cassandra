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
import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class    DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

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

    public Set<SSTableReader> getSSTables()
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

        incrementallyBackup(sstable);
    }

    public void incrementallyBackup(SSTableReader sstable)
    {
        if (DatabaseDescriptor.incrementalBackupsEnabled())
        {
            File keyspaceDir = new File(sstable.getFilename()).getParentFile();
            File backupsDir = new File(keyspaceDir, "backups");
            try
            {
                if (!backupsDir.exists() && !backupsDir.mkdirs())
                    throw new IOException("Unable to create " + backupsDir);
                sstable.createLinks(backupsDir.getCanonicalPath());
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    /**
     * @return A subset of the given active sstables that have been marked compacting,
     * or null if the thresholds cannot be met: files that are marked compacting must
     * later be unmarked using unmarkCompacting.
     */
    public Set<SSTableReader> markCompacting(Collection<SSTableReader> tomark, int min, int max)
    {
        if (max < min || max < 1)
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
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements)
    {
        replace(sstables, replacements);
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        replace(Collections.<SSTableReader>emptyList(), sstables);
    }

    public void addStreamedSSTable(SSTableReader sstable)
    {
        addSSTables(Arrays.asList(sstable));
        incrementallyBackup(sstable);
    }

    public void removeAllSSTables()
    {
        replace(getSSTables(), Collections.<SSTableReader>emptyList());
    }

    /** (Re)initializes the tracker, purging all references. */
    void init()
    {
        view.set(new View(new Memtable(cfstore),
                          Collections.<Memtable>emptySet(),
                          Collections.<SSTableReader>emptySet(),
                          Collections.<SSTableReader>emptySet()));
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
            sstable.markCompacted();
            liveSize.addAndGet(-sstable.bytesOnDisk());
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

    /**
     * An immutable structure holding the current memtable, the memtables pending
     * flush, the sstables for a column family, and the sstables that are active
     * in compaction (a subset of the sstables).
     */
    static class View
    {
        public final Memtable memtable;
        public final Set<Memtable> memtablesPendingFlush;
        public final Set<SSTableReader> sstables;
        public final Set<SSTableReader> compacting;

        View(Memtable memtable, Set<Memtable> pendingFlush, Set<SSTableReader> sstables, Set<SSTableReader> compacting)
        {
            this.memtable = memtable;
            this.memtablesPendingFlush = pendingFlush;
            this.sstables = sstables;
            this.compacting = compacting;
        }

        public View switchMemtable(Memtable newMemtable)
        {
            Set<Memtable> newPending = ImmutableSet.<Memtable>builder().addAll(memtablesPendingFlush).add(memtable).build();
            return new View(newMemtable, newPending, sstables, compacting);
        }

        public View renewMemtable(Memtable newMemtable)
        {
            return new View(newMemtable, memtablesPendingFlush, sstables, compacting);
        }

        public View replaceFlushed(Memtable flushedMemtable, SSTableReader newSSTable)
        {
            Set<Memtable> newPending = ImmutableSet.copyOf(Sets.difference(memtablesPendingFlush, Collections.singleton(flushedMemtable)));
            Set<SSTableReader> newSSTables = ImmutableSet.<SSTableReader>builder().addAll(sstables).add(newSSTable).build();
            return new View(memtable, newPending, newSSTables, compacting);
        }

        public View replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            Sets.SetView<SSTableReader> remaining = Sets.difference(sstables, ImmutableSet.copyOf(oldSSTables));
            Set<SSTableReader> newSSTables = ImmutableSet.<SSTableReader>builder().addAll(remaining).addAll(replacements).build();
            return new View(memtable, memtablesPendingFlush, newSSTables, compacting);
        }

        public View markCompacting(Collection<SSTableReader> tomark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.<SSTableReader>builder().addAll(compacting).addAll(tomark).build();
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew);
        }

        public View unmarkCompacting(Collection<SSTableReader> tounmark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.copyOf(Sets.difference(compacting, ImmutableSet.copyOf(tounmark)));
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew);
        }
    }
}
