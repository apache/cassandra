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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.MBeanWrapper;
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

    private final DebuggableScheduledThreadPoolExecutor executor;

    // our next scheduled resizing run
    private ScheduledFuture future;

    static
    {
        instance = new IndexSummaryManager();
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
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
                result.addAll(cfStore.getLiveSSTables());
        }

        return result;
    }

    /**
     * Returns a Pair of all compacting and non-compacting sstables.  Non-compacting sstables will be marked as
     * compacting.
     */
    @SuppressWarnings("resource")
    private Pair<List<SSTableReader>, Map<UUID, LifecycleTransaction>> getCompactingAndNonCompactingSSTables()
    {
        List<SSTableReader> allCompacting = new ArrayList<>();
        Map<UUID, LifecycleTransaction> allNonCompacting = new HashMap<>();
        for (Keyspace ks : Keyspace.all())
        {
            for (ColumnFamilyStore cfStore: ks.getColumnFamilyStores())
            {
                Set<SSTableReader> nonCompacting, allSSTables;
                LifecycleTransaction txn = null;
                do
                {
                    View view = cfStore.getTracker().getView();
                    allSSTables = ImmutableSet.copyOf(view.select(SSTableSet.CANONICAL));
                    nonCompacting = ImmutableSet.copyOf(view.getUncompacting(allSSTables));
                }
                while (null == (txn = cfStore.getTracker().tryModify(nonCompacting, OperationType.UNKNOWN)));

                allNonCompacting.put(cfStore.metadata.cfId, txn);
                allCompacting.addAll(Sets.difference(allSSTables, nonCompacting));
            }
        }
        return Pair.create(allCompacting, allNonCompacting);
    }

    public void redistributeSummaries() throws IOException
    {
        Pair<List<SSTableReader>, Map<UUID, LifecycleTransaction>> compactingAndNonCompacting = getCompactingAndNonCompactingSSTables();
        try
        {
            redistributeSummaries(new IndexSummaryRedistribution(compactingAndNonCompacting.left,
                                                                 compactingAndNonCompacting.right,
                                                                 this.memoryPoolBytes));
        }
        finally
        {
            for (LifecycleTransaction modifier : compactingAndNonCompacting.right.values())
                modifier.close();
        }
    }

    /**
     * Attempts to fairly distribute a fixed pool of memory for index summaries across a set of SSTables based on
     * their recent read rates.
     * @param redistribution encapsulating the transactions containing the sstables we are to redistribute the
     *                       memory pool across and a size (in bytes) that the total index summary space usage
     *                       should stay close to or under, if possible
     * @return a list of new SSTableReader instances
     */
    @VisibleForTesting
    public static List<SSTableReader> redistributeSummaries(IndexSummaryRedistribution redistribution) throws IOException
    {
        return CompactionManager.instance.runIndexSummaryRedistribution(redistribution);
    }
}
