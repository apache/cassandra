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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Manages the fixed-size memory pool for index summaries, periodically resizing them
 * in order to give more memory to hot sstables and less memory to cold sstables.
 */
public class IndexSummaryManager implements IndexSummaryManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryManager.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=IndexSummaries";
    public static final IndexSummaryManager instance;

    private int resizeIntervalInMinutes = 0;
    private long memoryPoolBytes;

    // The target (or ideal) number of index summary entries must differ from the actual number of
    // entries by this ratio in order to trigger an upsample or downsample of the summary.  Because
    // upsampling requires reading the primary index in order to rebuild the summary, the threshold
    // for upsampling is is higher.
    static final double UPSAMPLE_THRESHOLD = 1.5;
    static final double DOWNSAMPLE_THESHOLD = 0.75;

    private final DebuggableScheduledThreadPoolExecutor executor;

    // our next scheduled resizing run
    private ScheduledFuture future;

    static
    {
        instance = new IndexSummaryManager();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private IndexSummaryManager()
    {
        executor = new DebuggableScheduledThreadPoolExecutor(1, "IndexSummaryManager", Thread.MIN_PRIORITY);

        long indexSummarySizeInMB = DatabaseDescriptor.getIndexSummaryCapacityInMB();
        int interval = DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes();
        logger.info("Initializing index summary manager with a memory pool size of {} MB and a resize interval of {} minutes",
                    indexSummarySizeInMB, interval);

        setMemoryPoolCapacityInMB(DatabaseDescriptor.getIndexSummaryCapacityInMB());
        setResizeIntervalInMinutes(DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes());
    }

    public int getResizeIntervalInMinutes()
    {
        return resizeIntervalInMinutes;
    }

    public void setResizeIntervalInMinutes(int resizeIntervalInMinutes)
    {
        int oldInterval = this.resizeIntervalInMinutes;
        this.resizeIntervalInMinutes = resizeIntervalInMinutes;

        long initialDelay;
        if (future != null)
        {
            initialDelay = oldInterval < 0
                           ? resizeIntervalInMinutes
                           : Math.max(0, resizeIntervalInMinutes - (oldInterval - future.getDelay(TimeUnit.MINUTES)));
            future.cancel(false);
        }
        else
        {
            initialDelay = resizeIntervalInMinutes;
        }

        if (this.resizeIntervalInMinutes < 0)
        {
            future = null;
            return;
        }

        future = executor.scheduleWithFixedDelay(new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                redistributeSummaries();
            }
        }, initialDelay, resizeIntervalInMinutes, TimeUnit.MINUTES);
    }

    // for testing only
    @VisibleForTesting
    Long getTimeToNextResize(TimeUnit timeUnit)
    {
        if (future == null)
            return null;

        return future.getDelay(timeUnit);
    }

    public long getMemoryPoolCapacityInMB()
    {
        return memoryPoolBytes / 1024L / 1024L;
    }

    public Map<String, Integer> getIndexIntervals()
    {
        List<SSTableReader> sstables = getAllSSTables();
        Map<String, Integer> intervals = new HashMap<>(sstables.size());
        for (SSTableReader sstable : sstables)
            intervals.put(sstable.getFilename(), (int) Math.round(sstable.getEffectiveIndexInterval()));

        return intervals;
    }

    public double getAverageIndexInterval()
    {
        List<SSTableReader> sstables = getAllSSTables();
        double total = 0.0;
        for (SSTableReader sstable : sstables)
            total += sstable.getEffectiveIndexInterval();
        return total / sstables.size();
    }

    public void setMemoryPoolCapacityInMB(long memoryPoolCapacityInMB)
    {
        this.memoryPoolBytes = memoryPoolCapacityInMB * 1024L * 1024L;
    }

    /**
     * Returns the actual space consumed by index summaries for all sstables.
     * @return space currently used in MB
     */
    public double getMemoryPoolSizeInMB()
    {
        long total = 0;
        for (SSTableReader sstable : getAllSSTables())
            total += sstable.getIndexSummaryOffHeapSize();
        return total / 1024.0 / 1024.0;
    }

    private List<SSTableReader> getAllSSTables()
    {
        List<SSTableReader> result = new ArrayList<>();
        for (Keyspace ks : Keyspace.all())
        {
            for (ColumnFamilyStore cfStore: ks.getColumnFamilyStores())
                result.addAll(cfStore.getSSTables());
        }

        return result;
    }

    /**
     * Returns a Pair of all compacting and non-compacting sstables.  Non-compacting sstables will be marked as
     * compacting.
     */
    private Pair<List<SSTableReader>, Multimap<DataTracker, SSTableReader>> getCompactingAndNonCompactingSSTables()
    {
        List<SSTableReader> allCompacting = new ArrayList<>();
        Multimap<DataTracker, SSTableReader> allNonCompacting = HashMultimap.create();
        for (Keyspace ks : Keyspace.all())
        {
            for (ColumnFamilyStore cfStore: ks.getColumnFamilyStores())
            {
                Set<SSTableReader> nonCompacting, allSSTables;
                do
                {
                    allSSTables = cfStore.getDataTracker().getSSTables();
                    nonCompacting = Sets.newHashSet(cfStore.getDataTracker().getUncompactingSSTables(allSSTables));
                }
                while (!(nonCompacting.isEmpty() || cfStore.getDataTracker().markCompacting(nonCompacting)));
                allNonCompacting.putAll(cfStore.getDataTracker(), nonCompacting);
                allCompacting.addAll(Sets.difference(allSSTables, nonCompacting));
            }
        }
        return Pair.create(allCompacting, allNonCompacting);
    }

    public void redistributeSummaries() throws IOException
    {
        Pair<List<SSTableReader>, Multimap<DataTracker, SSTableReader>> compactingAndNonCompacting = getCompactingAndNonCompactingSSTables();
        try
        {
            redistributeSummaries(compactingAndNonCompacting.left, Lists.newArrayList(compactingAndNonCompacting.right.values()), this.memoryPoolBytes);
        }
        finally
        {
            for(DataTracker tracker : compactingAndNonCompacting.right.keySet())
                tracker.unmarkCompacting(compactingAndNonCompacting.right.get(tracker));
        }
    }

    /**
     * Attempts to fairly distribute a fixed pool of memory for index summaries across a set of SSTables based on
     * their recent read rates.
     * @param nonCompacting a list of sstables to share the memory pool across
     * @param memoryPoolBytes a size (in bytes) that the total index summary space usage should stay close to or
     *                        under, if possible
     * @return a list of new SSTableReader instances
     */
    @VisibleForTesting
    public static List<SSTableReader> redistributeSummaries(List<SSTableReader> compacting, List<SSTableReader> nonCompacting, long memoryPoolBytes) throws IOException
    {
        return CompactionManager.instance.runIndexSummaryRedistribution(new IndexSummaryRedistribution(compacting, nonCompacting, memoryPoolBytes));
    }
}
