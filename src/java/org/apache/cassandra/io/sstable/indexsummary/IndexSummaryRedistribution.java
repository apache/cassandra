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

package org.apache.cassandra.io.sstable.indexsummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class IndexSummaryRedistribution extends CompactionInfo.Holder
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryRedistribution.class);

    // The target (or ideal) number of index summary entries must differ from the actual number of
    // entries by this ratio in order to trigger an upsample or downsample of the summary.  Because
    // upsampling requires reading the primary index in order to rebuild the summary, the threshold
    // for upsampling is is higher.
    static final double UPSAMPLE_THRESHOLD = 1.5;
    static final double DOWNSAMPLE_THESHOLD = 0.75;

    private final Map<TableId, LifecycleTransaction> transactions;
    private final long nonRedistributingOffHeapSize;
    private final long memoryPoolBytes;
    private final TimeUUID compactionId;
    private volatile long remainingSpace;

    /**
     *
     * @param transactions the transactions for the different keyspaces/tables we are to redistribute
     * @param nonRedistributingOffHeapSize the total index summary off heap size for all sstables we were not able to mark compacting (due to them being involved in other compactions)
     * @param memoryPoolBytes size of the memory pool
     */
    public IndexSummaryRedistribution(Map<TableId, LifecycleTransaction> transactions, long nonRedistributingOffHeapSize, long memoryPoolBytes)
    {
        this.transactions = transactions;
        this.nonRedistributingOffHeapSize = nonRedistributingOffHeapSize;
        this.memoryPoolBytes = memoryPoolBytes;
        this.compactionId = nextTimeUUID();
    }

    private static <T extends SSTableReader & IndexSummarySupport<T>> List<T> getIndexSummarySupportingAndCloseOthers(LifecycleTransaction txn)
    {
        List<T> filtered = new ArrayList<>();
        List<SSTableReader> cancels = new ArrayList<>();
        for (SSTableReader sstable : txn.originals())
        {
            if (sstable instanceof IndexSummarySupport<?>)
                filtered.add((T) sstable);
            else
                cancels.add(sstable);
        }
        txn.cancel(cancels);
        return filtered;
    }

    public <T extends SSTableReader & IndexSummarySupport<T>> List<T> redistributeSummaries() throws IOException
    {
        long start = nanoTime();
        logger.info("Redistributing index summaries");
        List<T> redistribute = new ArrayList<>();

        for (LifecycleTransaction txn : transactions.values())
        {
            redistribute.addAll(getIndexSummarySupportingAndCloseOthers(txn));
        }

        long total = nonRedistributingOffHeapSize;
        for (T sstable : redistribute)
            total += sstable.getIndexSummary().getOffHeapSize();

        logger.info("Beginning redistribution of index summaries for {} sstables with memory pool size {} MiB; current spaced used is {} MiB",
                     redistribute.size(), memoryPoolBytes / 1024L / 1024L, total / 1024.0 / 1024.0);

        final Map<T, Double> readRates = new HashMap<>(redistribute.size());
        double totalReadsPerSec = 0.0;
        for (T sstable : redistribute)
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
        List<T> sstablesByHotness = new ArrayList<>(redistribute);
        Collections.sort(sstablesByHotness, new ReadRateComparator(readRates));

        long remainingBytes = memoryPoolBytes - nonRedistributingOffHeapSize;

        logger.trace("Index summaries for compacting SSTables are using {} MiB of space",
                     (memoryPoolBytes - remainingBytes) / 1024.0 / 1024.0);
        List<T> newSSTables;
        try (Refs<SSTableReader> refs = Refs.ref(sstablesByHotness))
        {
            newSSTables = adjustSamplingLevels(sstablesByHotness, transactions, totalReadsPerSec, remainingBytes);

            for (LifecycleTransaction txn : transactions.values())
                txn.finish();
        }
        total = nonRedistributingOffHeapSize;
        for (T sstable : newSSTables)
            total += sstable.getIndexSummary().getOffHeapSize();

        logger.info("Completed resizing of index summaries; current approximate memory used: {} MiB, time spent: {}ms",
                    total / 1024.0 / 1024.0, TimeUnit.NANOSECONDS.toMillis(nanoTime() - start));

        return newSSTables;
    }

    private <T extends SSTableReader & IndexSummarySupport<T>> List<T> adjustSamplingLevels(List<T> sstables,
                                                                                            Map<TableId, LifecycleTransaction> transactions,
                                                                                            double totalReadsPerSec,
                                                                                            long memoryPoolCapacity) throws IOException
    {
        List<ResampleEntry<T>> toDownsample = new ArrayList<>(sstables.size() / 4);
        List<ResampleEntry<T>> toUpsample = new ArrayList<>(sstables.size() / 4);
        List<ResampleEntry<T>> forceResample = new ArrayList<>();
        List<ResampleEntry<T>> forceUpsample = new ArrayList<>();
        List<T> newSSTables = new ArrayList<>(sstables.size());

        // Going from the coldest to the hottest sstables, try to give each sstable an amount of space proportional
        // to the number of total reads/sec it handles.
        remainingSpace = memoryPoolCapacity;
        for (T sstable : sstables)
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());

            int minIndexInterval = sstable.metadata().params.minIndexInterval;
            int maxIndexInterval = sstable.metadata().params.maxIndexInterval;

            double readsPerSec = sstable.getReadMeter() == null ? 0.0 : sstable.getReadMeter().fifteenMinuteRate();
            long idealSpace = Math.round(remainingSpace * (readsPerSec / totalReadsPerSec));

            // figure out how many entries our idealSpace would buy us, and pick a new sampling level based on that
            int currentNumEntries = sstable.getIndexSummary().size();
            double avgEntrySize = sstable.getIndexSummary().getOffHeapSize() / (double) currentNumEntries;
            long targetNumEntries = Math.max(1, Math.round(idealSpace / avgEntrySize));
            int currentSamplingLevel = sstable.getIndexSummary().getSamplingLevel();
            int maxSummarySize = sstable.getIndexSummary().getMaxNumberOfEntries();

            // if the min_index_interval changed, calculate what our current sampling level would be under the new min
            if (sstable.getIndexSummary().getMinIndexInterval() != minIndexInterval)
            {
                int effectiveSamplingLevel = (int) Math.round(currentSamplingLevel * (minIndexInterval / (double) sstable.getIndexSummary().getMinIndexInterval()));
                maxSummarySize = (int) Math.round(maxSummarySize * (sstable.getIndexSummary().getMinIndexInterval() / (double) minIndexInterval));
                logger.trace("min_index_interval changed from {} to {}, so the current sampling level for {} is effectively now {} (was {})",
                             sstable.getIndexSummary().getMinIndexInterval(), minIndexInterval, sstable, effectiveSamplingLevel, currentSamplingLevel);
                currentSamplingLevel = effectiveSamplingLevel;
            }

            int newSamplingLevel = IndexSummaryBuilder.calculateSamplingLevel(currentSamplingLevel, currentNumEntries, targetNumEntries,
                    minIndexInterval, maxIndexInterval);
            int numEntriesAtNewSamplingLevel = IndexSummaryBuilder.entriesAtSamplingLevel(newSamplingLevel, maxSummarySize);
            double effectiveIndexInterval = sstable.getIndexSummary().getEffectiveIndexInterval();

            if (logger.isTraceEnabled())
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
                forceResample.add(new ResampleEntry<T>(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= spaceUsed;
            }
            else if (effectiveIndexInterval > maxIndexInterval)
            {
                // The max_index_interval was lowered; force an upsample to the effective minimum sampling level
                logger.trace("Forcing upsample of {} because the current index interval ({}) is above max_index_interval ({})",
                        sstable, effectiveIndexInterval, maxIndexInterval);
                newSamplingLevel = Math.max(1, (BASE_SAMPLING_LEVEL * minIndexInterval) / maxIndexInterval);
                numEntriesAtNewSamplingLevel = IndexSummaryBuilder.entriesAtSamplingLevel(newSamplingLevel, sstable.getIndexSummary().getMaxNumberOfEntries());
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                forceUpsample.add(new ResampleEntry<T>(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= avgEntrySize * numEntriesAtNewSamplingLevel;
            }
            else if (targetNumEntries >= currentNumEntries * UPSAMPLE_THRESHOLD && newSamplingLevel > currentSamplingLevel)
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                toUpsample.add(new ResampleEntry<T>(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= avgEntrySize * numEntriesAtNewSamplingLevel;
            }
            else if (targetNumEntries < currentNumEntries * DOWNSAMPLE_THESHOLD && newSamplingLevel < currentSamplingLevel)
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                toDownsample.add(new ResampleEntry<T>(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= spaceUsed;
            }
            else
            {
                // keep the same sampling level
                logger.trace("SSTable {} is within thresholds of ideal sampling", sstable);
                remainingSpace -= sstable.getIndexSummary().getOffHeapSize();
                newSSTables.add(sstable);
                transactions.get(sstable.metadata().id).cancel(sstable);
            }
            totalReadsPerSec -= readsPerSec;
        }

        if (remainingSpace > 0)
        {
            Pair<List<T>, List<ResampleEntry<T>>> result = distributeRemainingSpace(toDownsample, remainingSpace);
            toDownsample = result.right;
            newSSTables.addAll(result.left);
            for (T sstable : result.left)
                transactions.get(sstable.metadata().id).cancel(sstable);
        }

        // downsample first, then upsample
        logger.info("index summaries: downsample: {}, force resample: {}, upsample: {}, force upsample: {}", toDownsample.size(), forceResample.size(), toUpsample.size(), forceUpsample.size());
        toDownsample.addAll(forceResample);
        toDownsample.addAll(toUpsample);
        toDownsample.addAll(forceUpsample);
        for (ResampleEntry<T> entry : toDownsample)
        {
            if (isStopRequested())
                throw new CompactionInterruptedException(getCompactionInfo());

            T sstable = entry.sstable;
            logger.trace("Re-sampling index summary for {} from {}/{} to {}/{} of the original number of entries",
                         sstable, sstable.getIndexSummary().getSamplingLevel(), Downsampling.BASE_SAMPLING_LEVEL,
                         entry.newSamplingLevel, Downsampling.BASE_SAMPLING_LEVEL);
            ColumnFamilyStore cfs = Keyspace.open(sstable.metadata().keyspace).getColumnFamilyStore(sstable.metadata().id);
            long oldSize = sstable.bytesOnDisk();
            long oldSizeUncompressed = sstable.logicalBytesOnDisk();

            T replacement = sstable.cloneWithNewSummarySamplingLevel(cfs, entry.newSamplingLevel);
            long newSize = replacement.bytesOnDisk();
            long newSizeUncompressed = replacement.logicalBytesOnDisk();

            newSSTables.add(replacement);
            transactions.get(sstable.metadata().id).update(replacement, true);
            addHooks(cfs, transactions, oldSize, newSize, oldSizeUncompressed, newSizeUncompressed);
        }

        return newSSTables;
    }

    /**
     * Add hooks to correctly update the storage load metrics once the transaction is closed/aborted
     */
    private void addHooks(ColumnFamilyStore cfs, Map<TableId, LifecycleTransaction> transactions, long oldSize, long newSize, long oldSizeUncompressed, long newSizeUncompressed)
    {
        LifecycleTransaction txn = transactions.get(cfs.metadata.id);
        txn.runOnCommit(() -> {
            // The new size will be added in Transactional.commit() as an updated SSTable, more details: CASSANDRA-13738
            StorageMetrics.load.dec(oldSize);
            StorageMetrics.uncompressedLoad.dec(oldSizeUncompressed);

            cfs.metric.liveDiskSpaceUsed.dec(oldSize);
            cfs.metric.uncompressedLiveDiskSpaceUsed.dec(oldSizeUncompressed);
            cfs.metric.totalDiskSpaceUsed.dec(oldSize);
        });
        txn.runOnAbort(() -> {
            // the local disk was modified but bookkeeping couldn't be commited, apply the delta
            long delta = oldSize - newSize; // if new is larger this will be negative, so dec will become a inc
            long deltaUncompressed = oldSizeUncompressed - newSizeUncompressed;

            StorageMetrics.load.dec(delta);
            StorageMetrics.uncompressedLoad.dec(deltaUncompressed);

            cfs.metric.liveDiskSpaceUsed.dec(delta);
            cfs.metric.uncompressedLiveDiskSpaceUsed.dec(deltaUncompressed);
            cfs.metric.totalDiskSpaceUsed.dec(delta);
        });
    }

    @VisibleForTesting
    static <T extends SSTableReader & IndexSummarySupport<T>> Pair<List<T>, List<ResampleEntry<T>>> distributeRemainingSpace(List<ResampleEntry<T>> toDownsample, long remainingSpace)
    {
        // sort by the amount of space regained by doing the downsample operation; we want to try to avoid operations
        // that will make little difference.
        Collections.sort(toDownsample, Comparator.comparingDouble(o -> o.sstable.getIndexSummary().getOffHeapSize() - o.newSpaceUsed));

        int noDownsampleCutoff = 0;
        List<T> willNotDownsample = new ArrayList<>();
        while (remainingSpace > 0 && noDownsampleCutoff < toDownsample.size())
        {
            ResampleEntry<T> entry = toDownsample.get(noDownsampleCutoff);

            long extraSpaceRequired = entry.sstable.getIndexSummary().getOffHeapSize() - entry.newSpaceUsed;
            // see if we have enough leftover space to keep the current sampling level
            if (extraSpaceRequired <= remainingSpace)
            {
                logger.trace("Using leftover space to keep {} at the current sampling level ({})",
                             entry.sstable, entry.sstable.getIndexSummary().getSamplingLevel());
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
        return CompactionInfo.withoutSSTables(null, OperationType.INDEX_SUMMARY, (memoryPoolBytes - remainingSpace), memoryPoolBytes, Unit.BYTES, compactionId);
    }

    public boolean isGlobal()
    {
        return true;
    }

    /** Utility class for sorting sstables by their read rates. */
    private static class ReadRateComparator implements Comparator<SSTableReader>
    {
        private final Map<? extends SSTableReader, Double> readRates;

        ReadRateComparator(Map<? extends SSTableReader, Double> readRates)
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

    private static class ResampleEntry<T extends SSTableReader & IndexSummarySupport<T>>
    {
        public final T sstable;
        public final long newSpaceUsed;
        public final int newSamplingLevel;

        ResampleEntry(T sstable, long newSpaceUsed, int newSamplingLevel)
        {
            this.sstable = sstable;
            this.newSpaceUsed = newSpaceUsed;
            this.newSamplingLevel = newSamplingLevel;
        }
    }
}
