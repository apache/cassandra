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
import java.util.Collections;
import java.util.Comparator;
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
import com.google.common.collect.Iterables;
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
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;

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
        long total = 0;
        for (SSTableReader sstable : Iterables.concat(compacting, nonCompacting))
            total += sstable.getIndexSummaryOffHeapSize();

        List<SSTableReader> oldFormatSSTables = new ArrayList<>();
        for (SSTableReader sstable : nonCompacting)
        {
            // We can't change the sampling level of sstables with the old format, because the serialization format
            // doesn't include the sampling level.  Leave this one as it is.  (See CASSANDRA-8993 for details.)
            logger.trace("SSTable {} cannot be re-sampled due to old sstable format", sstable);
            if (!sstable.descriptor.version.hasSamplingLevel)
                oldFormatSSTables.add(sstable);
        }
        nonCompacting.removeAll(oldFormatSSTables);

        logger.debug("Beginning redistribution of index summaries for {} sstables with memory pool size {} MB; current spaced used is {} MB",
                     nonCompacting.size(), memoryPoolBytes / 1024L / 1024L, total / 1024.0 / 1024.0);

        final Map<SSTableReader, Double> readRates = new HashMap<>(nonCompacting.size());
        double totalReadsPerSec = 0.0;
        for (SSTableReader sstable : nonCompacting)
        {
            if (sstable.getReadMeter() != null)
            {
                Double readRate = sstable.getReadMeter().fifteenMinuteRate();
                totalReadsPerSec += readRate;
                readRates.put(sstable, readRate);
            }
        }
        logger.trace("Total reads/sec across all sstables in index summary resize process: {}", totalReadsPerSec);

        // copy and sort by read rates (ascending)
        List<SSTableReader> sstablesByHotness = new ArrayList<>(nonCompacting);
        Collections.sort(sstablesByHotness, new ReadRateComparator(readRates));

        long remainingBytes = memoryPoolBytes;
        for (SSTableReader sstable : Iterables.concat(compacting, oldFormatSSTables))
            remainingBytes -= sstable.getIndexSummaryOffHeapSize();

        logger.trace("Index summaries for compacting SSTables are using {} MB of space",
                     (memoryPoolBytes - remainingBytes) / 1024.0 / 1024.0);
        List<SSTableReader> newSSTables = adjustSamplingLevels(sstablesByHotness, totalReadsPerSec, remainingBytes);

        total = 0;
        for (SSTableReader sstable : Iterables.concat(compacting, oldFormatSSTables, newSSTables))
            total += sstable.getIndexSummaryOffHeapSize();
        logger.debug("Completed resizing of index summaries; current approximate memory used: {} MB",
                     total / 1024.0 / 1024.0);

        return newSSTables;
    }

    private static List<SSTableReader> adjustSamplingLevels(List<SSTableReader> sstables,
                                                            double totalReadsPerSec, long memoryPoolCapacity) throws IOException
    {

        List<ResampleEntry> toDownsample = new ArrayList<>(sstables.size() / 4);
        List<ResampleEntry> toUpsample = new ArrayList<>(sstables.size() / 4);
        List<ResampleEntry> forceResample = new ArrayList<>();
        List<ResampleEntry> forceUpsample = new ArrayList<>();
        List<SSTableReader> newSSTables = new ArrayList<>(sstables.size());

        // Going from the coldest to the hottest sstables, try to give each sstable an amount of space proportional
        // to the number of total reads/sec it handles.
        long remainingSpace = memoryPoolCapacity;
        for (SSTableReader sstable : sstables)
        {
            int minIndexInterval = sstable.metadata.getMinIndexInterval();
            int maxIndexInterval = sstable.metadata.getMaxIndexInterval();

            double readsPerSec = sstable.getReadMeter() == null ? 0.0 : sstable.getReadMeter().fifteenMinuteRate();
            long idealSpace = Math.round(remainingSpace * (readsPerSec / totalReadsPerSec));

            // figure out how many entries our idealSpace would buy us, and pick a new sampling level based on that
            int currentNumEntries = sstable.getIndexSummarySize();
            double avgEntrySize = sstable.getIndexSummaryOffHeapSize() / (double) currentNumEntries;
            long targetNumEntries = Math.max(1, Math.round(idealSpace / avgEntrySize));
            int currentSamplingLevel = sstable.getIndexSummarySamplingLevel();
            int maxSummarySize = sstable.getMaxIndexSummarySize();

            // if the min_index_interval changed, calculate what our current sampling level would be under the new min
            if (sstable.getMinIndexInterval() != minIndexInterval)
            {
                int effectiveSamplingLevel = (int) Math.round(currentSamplingLevel * (minIndexInterval / (double) sstable.getMinIndexInterval()));
                maxSummarySize = (int) Math.round(maxSummarySize * (sstable.getMinIndexInterval() / (double) minIndexInterval));
                logger.trace("min_index_interval changed from {} to {}, so the current sampling level for {} is effectively now {} (was {})",
                             sstable.getMinIndexInterval(), minIndexInterval, sstable, effectiveSamplingLevel, currentSamplingLevel);
                currentSamplingLevel = effectiveSamplingLevel;
            }

            int newSamplingLevel = IndexSummaryBuilder.calculateSamplingLevel(currentSamplingLevel, currentNumEntries, targetNumEntries,
                    minIndexInterval, maxIndexInterval);
            int numEntriesAtNewSamplingLevel = IndexSummaryBuilder.entriesAtSamplingLevel(newSamplingLevel, maxSummarySize);
            double effectiveIndexInterval = sstable.getEffectiveIndexInterval();

            logger.trace("{} has {} reads/sec; ideal space for index summary: {} bytes ({} entries); considering moving " +
                    "from level {} ({} entries, {} bytes) to level {} ({} entries, {} bytes)",
                    sstable.getFilename(), readsPerSec, idealSpace, targetNumEntries, currentSamplingLevel, currentNumEntries,
                    currentNumEntries * avgEntrySize, newSamplingLevel, numEntriesAtNewSamplingLevel,
                    numEntriesAtNewSamplingLevel * avgEntrySize);

            if (effectiveIndexInterval < minIndexInterval)
            {
                // The min_index_interval was changed; re-sample to match it.
                logger.debug("Forcing resample of {} because the current index interval ({}) is below min_index_interval ({})",
                        sstable, effectiveIndexInterval, minIndexInterval);
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                forceResample.add(new ResampleEntry(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= spaceUsed;
            }
            else if (effectiveIndexInterval > maxIndexInterval)
            {
                // The max_index_interval was lowered; force an upsample to the effective minimum sampling level
                logger.debug("Forcing upsample of {} because the current index interval ({}) is above max_index_interval ({})",
                        sstable, effectiveIndexInterval, maxIndexInterval);
                newSamplingLevel = Math.max(1, (BASE_SAMPLING_LEVEL * minIndexInterval) / maxIndexInterval);
                numEntriesAtNewSamplingLevel = IndexSummaryBuilder.entriesAtSamplingLevel(newSamplingLevel, sstable.getMaxIndexSummarySize());
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                forceUpsample.add(new ResampleEntry(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= avgEntrySize * numEntriesAtNewSamplingLevel;
            }
            else if (targetNumEntries >= currentNumEntries * UPSAMPLE_THRESHOLD && newSamplingLevel > currentSamplingLevel)
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                toUpsample.add(new ResampleEntry(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= avgEntrySize * numEntriesAtNewSamplingLevel;
            }
            else if (targetNumEntries < currentNumEntries * DOWNSAMPLE_THESHOLD && newSamplingLevel < currentSamplingLevel)
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                toDownsample.add(new ResampleEntry(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= spaceUsed;
            }
            else
            {
                // keep the same sampling level
                logger.trace("SSTable {} is within thresholds of ideal sampling", sstable);
                remainingSpace -= sstable.getIndexSummaryOffHeapSize();
                newSSTables.add(sstable);
            }
            totalReadsPerSec -= readsPerSec;
        }

        if (remainingSpace > 0)
        {
            Pair<List<SSTableReader>, List<ResampleEntry>> result = distributeRemainingSpace(toDownsample, remainingSpace);
            toDownsample = result.right;
            newSSTables.addAll(result.left);
        }

        // downsample first, then upsample
        toDownsample.addAll(forceResample);
        toDownsample.addAll(toUpsample);
        toDownsample.addAll(forceUpsample);
        Multimap<DataTracker, SSTableReader> replacedByTracker = HashMultimap.create();
        Multimap<DataTracker, SSTableReader> replacementsByTracker = HashMultimap.create();
        for (ResampleEntry entry : toDownsample)
        {
            SSTableReader sstable = entry.sstable;
            logger.debug("Re-sampling index summary for {} from {}/{} to {}/{} of the original number of entries",
                         sstable, sstable.getIndexSummarySamplingLevel(), Downsampling.BASE_SAMPLING_LEVEL,
                         entry.newSamplingLevel, Downsampling.BASE_SAMPLING_LEVEL);
            ColumnFamilyStore cfs = Keyspace.open(sstable.getKeyspaceName()).getColumnFamilyStore(sstable.getColumnFamilyName());
            SSTableReader replacement = sstable.cloneWithNewSummarySamplingLevel(cfs, entry.newSamplingLevel);
            DataTracker tracker = cfs.getDataTracker();

            replacedByTracker.put(tracker, sstable);
            replacementsByTracker.put(tracker, replacement);
        }

        for (DataTracker tracker : replacedByTracker.keySet())
        {
            tracker.replaceWithNewInstances(replacedByTracker.get(tracker), replacementsByTracker.get(tracker));
            newSSTables.addAll(replacementsByTracker.get(tracker));
        }

        return newSSTables;
    }

    @VisibleForTesting
    static Pair<List<SSTableReader>, List<ResampleEntry>> distributeRemainingSpace(List<ResampleEntry> toDownsample, long remainingSpace)
    {
        // sort by the amount of space regained by doing the downsample operation; we want to try to avoid operations
        // that will make little difference.
        Collections.sort(toDownsample, new Comparator<ResampleEntry>()
        {
            public int compare(ResampleEntry o1, ResampleEntry o2)
            {
                return Double.compare(o1.sstable.getIndexSummaryOffHeapSize() - o1.newSpaceUsed,
                                      o2.sstable.getIndexSummaryOffHeapSize() - o2.newSpaceUsed);
            }
        });

        int noDownsampleCutoff = 0;
        List<SSTableReader> willNotDownsample = new ArrayList<>();
        while (remainingSpace > 0 && noDownsampleCutoff < toDownsample.size())
        {
            ResampleEntry entry = toDownsample.get(noDownsampleCutoff);

            long extraSpaceRequired = entry.sstable.getIndexSummaryOffHeapSize() - entry.newSpaceUsed;
            // see if we have enough leftover space to keep the current sampling level
            if (extraSpaceRequired <= remainingSpace)
            {
                logger.trace("Using leftover space to keep {} at the current sampling level ({})",
                             entry.sstable, entry.sstable.getIndexSummarySamplingLevel());
                willNotDownsample.add(entry.sstable);
                remainingSpace -= extraSpaceRequired;
            }
            else
            {
                break;
            }

            noDownsampleCutoff++;
        }
        return Pair.create(willNotDownsample, toDownsample.subList(noDownsampleCutoff, toDownsample.size()));
    }

    private static class ResampleEntry
    {
        public final SSTableReader sstable;
        public final long newSpaceUsed;
        public final int newSamplingLevel;

        public ResampleEntry(SSTableReader sstable, long newSpaceUsed, int newSamplingLevel)
        {
            this.sstable = sstable;
            this.newSpaceUsed = newSpaceUsed;
            this.newSamplingLevel = newSamplingLevel;
        }
    }

    /** Utility class for sorting sstables by their read rates. */
    private static class ReadRateComparator implements Comparator<SSTableReader>
    {
        private final Map<SSTableReader, Double> readRates;

        public ReadRateComparator(Map<SSTableReader, Double> readRates)
        {
            this.readRates = readRates;
        }

        @Override
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            Double readRate1 = readRates.get(o1);
            Double readRate2 = readRates.get(o2);
            if (readRate1 == null && readRate2 == null)
                return 0;
            else if (readRate1 == null)
                return -1;
            else if (readRate2 == null)
                return 1;
            else
                return Double.compare(readRate1, readRate2);
        }
    }
}