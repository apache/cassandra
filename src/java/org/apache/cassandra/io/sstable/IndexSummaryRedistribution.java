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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;

public class IndexSummaryRedistribution extends CompactionInfo.Holder
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryRedistribution.class);

    // The target (or ideal) number of index summary entries must differ from the actual number of
    // entries by this ratio in order to trigger an upsample or downsample of the summary.  Because
    // upsampling requires reading the primary index in order to rebuild the summary, the threshold
    // for upsampling is is higher.
    static final double UPSAMPLE_THRESHOLD = 1.5;
    static final double DOWNSAMPLE_THESHOLD = 0.75;

    private final List<SSTableReader> compacting;
    private final Map<UUID, LifecycleTransaction> transactions;
    private final long memoryPoolBytes;
    private final UUID compactionId;
    private volatile long remainingSpace;

    public IndexSummaryRedistribution(List<SSTableReader> compacting, Map<UUID, LifecycleTransaction> transactions, long memoryPoolBytes)
    {
        this.compacting = compacting;
        this.transactions = transactions;
        this.memoryPoolBytes = memoryPoolBytes;
        this.compactionId = UUID.randomUUID();
    }

    public List<SSTableReader> redistributeSummaries() throws IOException
    {
        logger.info("Redistributing index summaries");
        List<SSTableReader> oldFormatSSTables = new ArrayList<>();
        List<SSTableReader> redistribute = new ArrayList<>();
        for (LifecycleTransaction txn : transactions.values())
        {
            for (SSTableReader sstable : ImmutableList.copyOf(txn.originals()))
            {
                // We can't change the sampling level of sstables with the old format, because the serialization format
                // doesn't include the sampling level.  Leave this one as it is.  (See CASSANDRA-8993 for details.)
                logger.trace("SSTable {} cannot be re-sampled due to old sstable format", sstable);
                if (!sstable.descriptor.version.hasSamplingLevel())
                {
                    oldFormatSSTables.add(sstable);
                    txn.cancel(sstable);
                }
            }
            redistribute.addAll(txn.originals());
        }

        long total = 0;
        for (SSTableReader sstable : Iterables.concat(compacting, redistribute))
            total += sstable.getIndexSummaryOffHeapSize();

        logger.trace("Beginning redistribution of index summaries for {} sstables with memory pool size {} MB; current spaced used is {} MB",
                     redistribute.size(), memoryPoolBytes / 1024L / 1024L, total / 1024.0 / 1024.0);

        final Map<SSTableReader, Double> readRates = new HashMap<>(redistribute.size());
        double totalReadsPerSec = 0.0;
        for (SSTableReader sstable : redistribute)
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());

            if (sstable.getReadMeter() != null)
            {
                Double readRate = sstable.getReadMeter().fifteenMinuteRate();
                totalReadsPerSec += readRate;
                readRates.put(sstable, readRate);
            }
        }
        logger.trace("Total reads/sec across all sstables in index summary resize process: {}", totalReadsPerSec);

        // copy and sort by read rates (ascending)
        List<SSTableReader> sstablesByHotness = new ArrayList<>(redistribute);
        Collections.sort(sstablesByHotness, new ReadRateComparator(readRates));

        long remainingBytes = memoryPoolBytes;
        for (SSTableReader sstable : Iterables.concat(compacting, oldFormatSSTables))
            remainingBytes -= sstable.getIndexSummaryOffHeapSize();

        logger.trace("Index summaries for compacting SSTables are using {} MB of space",
                     (memoryPoolBytes - remainingBytes) / 1024.0 / 1024.0);
        List<SSTableReader> newSSTables;
        try (Refs<SSTableReader> refs = Refs.ref(sstablesByHotness))
        {
            newSSTables = adjustSamplingLevels(sstablesByHotness, transactions, totalReadsPerSec, remainingBytes);

            for (LifecycleTransaction txn : transactions.values())
                txn.finish();
        }
        total = 0;
        for (SSTableReader sstable : Iterables.concat(compacting, oldFormatSSTables, newSSTables))
            total += sstable.getIndexSummaryOffHeapSize();
        logger.trace("Completed resizing of index summaries; current approximate memory used: {}",
                     FBUtilities.prettyPrintMemory(total));

        return newSSTables;
    }

    private List<SSTableReader> adjustSamplingLevels(List<SSTableReader> sstables,
                                                     Map<UUID, LifecycleTransaction> transactions,
                                                     double totalReadsPerSec, long memoryPoolCapacity) throws IOException
    {
        List<ResampleEntry> toDownsample = new ArrayList<>(sstables.size() / 4);
        List<ResampleEntry> toUpsample = new ArrayList<>(sstables.size() / 4);
        List<ResampleEntry> forceResample = new ArrayList<>();
        List<ResampleEntry> forceUpsample = new ArrayList<>();
        List<SSTableReader> newSSTables = new ArrayList<>(sstables.size());

        // Going from the coldest to the hottest sstables, try to give each sstable an amount of space proportional
        // to the number of total reads/sec it handles.
        remainingSpace = memoryPoolCapacity;
        for (SSTableReader sstable : sstables)
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());

            int minIndexInterval = sstable.metadata.params.minIndexInterval;
            int maxIndexInterval = sstable.metadata.params.maxIndexInterval;

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

            logger.trace("{} has {} reads/sec; ideal space for index summary: {} ({} entries); considering moving " +
                    "from level {} ({} entries, {}) " +
                    "to level {} ({} entries, {})",
                    sstable.getFilename(), readsPerSec, FBUtilities.prettyPrintMemory(idealSpace), targetNumEntries,
                    currentSamplingLevel, currentNumEntries, FBUtilities.prettyPrintMemory((long) (currentNumEntries * avgEntrySize)),
                    newSamplingLevel, numEntriesAtNewSamplingLevel, FBUtilities.prettyPrintMemory((long) (numEntriesAtNewSamplingLevel * avgEntrySize)));

            if (effectiveIndexInterval < minIndexInterval)
            {
                // The min_index_interval was changed; re-sample to match it.
                logger.trace("Forcing resample of {} because the current index interval ({}) is below min_index_interval ({})",
                        sstable, effectiveIndexInterval, minIndexInterval);
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                forceResample.add(new ResampleEntry(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= spaceUsed;
            }
            else if (effectiveIndexInterval > maxIndexInterval)
            {
                // The max_index_interval was lowered; force an upsample to the effective minimum sampling level
                logger.trace("Forcing upsample of {} because the current index interval ({}) is above max_index_interval ({})",
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
                transactions.get(sstable.metadata.cfId).cancel(sstable);
            }
            totalReadsPerSec -= readsPerSec;
        }

        if (remainingSpace > 0)
        {
            Pair<List<SSTableReader>, List<ResampleEntry>> result = distributeRemainingSpace(toDownsample, remainingSpace);
            toDownsample = result.right;
            newSSTables.addAll(result.left);
            for (SSTableReader sstable : result.left)
                transactions.get(sstable.metadata.cfId).cancel(sstable);
        }

        // downsample first, then upsample
        toDownsample.addAll(forceResample);
        toDownsample.addAll(toUpsample);
        toDownsample.addAll(forceUpsample);
        for (ResampleEntry entry : toDownsample)
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());

            SSTableReader sstable = entry.sstable;
            logger.trace("Re-sampling index summary for {} from {}/{} to {}/{} of the original number of entries",
                         sstable, sstable.getIndexSummarySamplingLevel(), Downsampling.BASE_SAMPLING_LEVEL,
                         entry.newSamplingLevel, Downsampling.BASE_SAMPLING_LEVEL);
            ColumnFamilyStore cfs = Keyspace.open(sstable.metadata.ksName).getColumnFamilyStore(sstable.metadata.cfId);
            SSTableReader replacement = sstable.cloneWithNewSummarySamplingLevel(cfs, entry.newSamplingLevel);
            newSSTables.add(replacement);
            transactions.get(sstable.metadata.cfId).update(replacement, true);
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

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(OperationType.INDEX_SUMMARY, (memoryPoolBytes - remainingSpace), memoryPoolBytes, Unit.BYTES, compactionId);
    }

    public boolean isGlobal()
    {
        return true;
    }

    /** Utility class for sorting sstables by their read rates. */
    private static class ReadRateComparator implements Comparator<SSTableReader>
    {
        private final Map<SSTableReader, Double> readRates;

        ReadRateComparator(Map<SSTableReader, Double> readRates)
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

    private static class ResampleEntry
    {
        public final SSTableReader sstable;
        public final long newSpaceUsed;
        public final int newSamplingLevel;

        ResampleEntry(SSTableReader sstable, long newSpaceUsed, int newSamplingLevel)
        {
            this.sstable = sstable;
            this.newSpaceUsed = newSpaceUsed;
            this.newSamplingLevel = newSamplingLevel;
        }
    }
}
