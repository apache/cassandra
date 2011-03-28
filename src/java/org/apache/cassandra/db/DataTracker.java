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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class DataTracker
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
        this.view = new AtomicReference<View>(new View(new Memtable(cfstore), Collections.<Memtable>emptySet(), Collections.<SSTableReader>emptySet()));
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

    public void clearUnsafe()
    {
        view.set(new View(new Memtable(cfstore), Collections.<Memtable>emptySet(), Collections.<SSTableReader>emptySet()));
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
     * flush and the sstables for a column family.
     */
    static class View
    {
        public final Memtable memtable;
        public final Set<Memtable> memtablesPendingFlush;
        public final Set<SSTableReader> sstables;

        public View(Memtable memtable, Set<Memtable> pendingFlush, Set<SSTableReader> sstables)
        {
            this.memtable = memtable;
            this.memtablesPendingFlush = Collections.unmodifiableSet(pendingFlush);
            this.sstables = Collections.unmodifiableSet(sstables);
        }

        public View switchMemtable(Memtable newMemtable)
        {
            Set<Memtable> newPending = new HashSet<Memtable>(memtablesPendingFlush);
            newPending.add(memtable);
            return new View(newMemtable, newPending, sstables);
        }

        public View renewMemtable(Memtable newMemtable)
        {
            return new View(newMemtable, memtablesPendingFlush, sstables);
        }

        public View replaceFlushed(Memtable flushedMemtable, SSTableReader newSSTable)
        {
            Set<Memtable> newPendings = new HashSet<Memtable>(memtablesPendingFlush);
            Set<SSTableReader> newSSTables = new HashSet<SSTableReader>(sstables);
            newPendings.remove(flushedMemtable);
            newSSTables.add(newSSTable);
            return new View(memtable, newPendings, newSSTables);
        }

        public View replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            Set<SSTableReader> sstablesNew = new HashSet<SSTableReader>(sstables);
            Iterables.addAll(sstablesNew, replacements);
            sstablesNew.removeAll(oldSSTables);
            return new View(memtable, memtablesPendingFlush, sstablesNew);
        }
    }
}
