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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.CFPropDefs;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class TimeWindowCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TimeWindowCompactionStrategy.class);

    private final TimeWindowCompactionStrategyOptions options;
    protected volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private long lastExpiredCheck;

    public TimeWindowCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.options = new TimeWindowCompactionStrategyOptions(options);
        logger.debug("TWCS enabled with Window Time Unit {} , Window Size {} , SSTable Timestamp Unit {}", this.options.sstableWindowUnit, this.options.sstableWindowSize, this.options.timestampResolution);
        if (!options.containsKey(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.containsKey(AbstractCompactionStrategy.TOMBSTONE_THRESHOLD_OPTION))
        {
            disableTombstoneCompactions = true;
            logger.debug("Disabling tombstone compactions for TWCS");
        }
        else
            logger.debug("Enabling tombstone compactions for TWCS");

    }

    @Override
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        while (true)
        {
            List<SSTableReader> latestBucket = getNextBackgroundSSTables(gcBefore);

            if (latestBucket.isEmpty())
                return null;

            if (cfs.getDataTracker().markCompacting(latestBucket))
                return new CompactionTask(cfs, latestBucket, gcBefore, false);
        }
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        if (!isEnabled() || cfs.getSSTables().isEmpty())
            return Collections.emptyList();

        Set<SSTableReader> uncompacting = Sets.intersection(sstables, cfs.getUncompactingSSTables());

        Set<SSTableReader> expired = Collections.emptySet();

        if (System.currentTimeMillis() - lastExpiredCheck > options.expiredSSTableCheckFrequency)
        {
            // Find fully expired SSTables. Those will be included no matter what.
            expired = CompactionController.getFullyExpiredSSTables(cfs, uncompacting, cfs.getOverlappingSSTables(uncompacting), gcBefore);
            lastExpiredCheck = System.currentTimeMillis();
        }

        Set<SSTableReader> candidates = Sets.newHashSet(filterSuspectSSTables(uncompacting));

        List<SSTableReader> compactionCandidates = new ArrayList<SSTableReader>(getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore));

        if (!expired.isEmpty())
        {
            logger.debug("Including expired sstables: {}", expired);
            compactionCandidates.addAll(expired);
        }

        return compactionCandidates;
    }

    private List<SSTableReader> getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, final int gcBefore)
    {
        /*
         * Not all SSTables have TTLs that drop around the same time. In some cases there are tables that have 99% of records that
         * drop at the same time and 1% that will live significatly longer. These SSTables tend to get starved for compaction and end up
         * sitting round taking up more space than the system can handle. This first check prioritizes files with very high tombstone ratios
         * so that we don't eat up all the disk on a machine.
         */
        SSTableReader sstablesWithMaxTombstoneRatio = getHigestMaxedOutTombstoneRatioSstable(nonExpiringSSTables, gcBefore);

        if (sstablesWithMaxTombstoneRatio != null) return Collections.singletonList(sstablesWithMaxTombstoneRatio);

        // Now check for the moast interesting tables.
        List<SSTableReader> mostInteresting = getCompactionCandidates(nonExpiringSSTables);

        if (mostInteresting != null)
        {
            return mostInteresting;
        }

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        // Compact the tables with the highest tombstone ratios first.
        return Collections.singletonList(Collections.max(sstablesWithTombstones, new Comparator<SSTableReader>()
        {
            public int compare(SSTableReader o1, SSTableReader o2)
            {
                return Double.compare(o1.getEstimatedDroppableTombstoneRatio(gcBefore), o2.getEstimatedDroppableTombstoneRatio(gcBefore));
            }
        }));
    }

    /***
     * @param nonExpiringSSTables
     * @param gcBefore
     * @return The SSTableReader with the highest tombstone ratio that is above the threashold
     */
    private SSTableReader getHigestMaxedOutTombstoneRatioSstable(Iterable<SSTableReader> nonExpiringSSTables, final int gcBefore)
    {
        List<SSTableReader> sstablesWithMaxTombstoneRatio = Lists.newArrayList();

        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (reachedMaxTombstonesRatio(sstable, gcBefore))
                sstablesWithMaxTombstoneRatio.add(sstable);
        }

        if(sstablesWithMaxTombstoneRatio.size() > 0)
        {
            return Collections.max(sstablesWithMaxTombstoneRatio, new Comparator<SSTableReader>()
            {
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    return Double.compare(o1.getEstimatedDroppableTombstoneRatio(gcBefore), o2.getEstimatedDroppableTombstoneRatio(gcBefore));
                }
            });
        }

        return null;
    }

    private List<SSTableReader> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables)
    {
        HashMultimap<Long, SSTableReader> buckets = getBuckets(candidateSSTables, options.sstableWindowUnit, options.sstableWindowSize, options.timestampResolution);
        updateEstimatedCompactionsByTasks(buckets);
        List<SSTableReader> mostInteresting = newestBucket(buckets,
                                                           cfs.getMinimumCompactionThreshold(),
                                                           cfs.getMaximumCompactionThreshold(),
                                                           options.sstableWindowUnit,
                                                           options.sstableWindowSize,
                                                           options.stcsOptions);
        if (!mostInteresting.isEmpty())
            return mostInteresting;
        return null;
    }

    @Override
    public void addSSTable(SSTableReader sstable)
    {
        sstables.add(sstable);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    /**
     * Find the lowest and highest timestamps in a given timestamp/unit pair
     * Returns milliseconds, caller should adjust accordingly
     */
    public static Pair<Long,Long> getWindowBoundsInMillis(TimeUnit windowTimeUnit, int windowTimeSize, long timestampInMillis)
    {
        long lowerTimestamp;
        long upperTimestamp;
        long timestampInSeconds = timestampInMillis / 1000L;

        switch(windowTimeUnit)
        {
            case MINUTES:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (60 * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (60L * (windowTimeSize - 1L))) + 59L;
                break;
            case HOURS:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (3600 * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (3600L * (windowTimeSize - 1L))) + 3599L;
                break;
            case DAYS:
            default:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (86400 * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (86400L * (windowTimeSize - 1L))) + 86399L;
                break;
        }

        return Pair.create(lowerTimestamp * 1000L, upperTimestamp * 1000L);

    }

    /**
     * Group files with similar max timestamp into buckets.
     *
     * @param files collection of sstablereaders
     * @param sstableWindowUnit
     * @param sstableWindowSize
     * @param timestampResolution
     * @return a map of buckets of files. The map keys are seconds since epoch representing the beginning of the window.
     *
     */
    @VisibleForTesting
    static HashMultimap getBuckets(Iterable<SSTableReader> files, TimeUnit sstableWindowUnit, int sstableWindowSize, TimeUnit timestampResolution)
    {
        HashMultimap<Long, SSTableReader> buckets = HashMultimap.create();

        // Create hash map to represent buckets
        // For each sstable, add sstable to the time bucket
        // Where the bucket is the file's max timestamp rounded to the nearest window bucket
        for (SSTableReader f : files)
        {
            long tStamp = f.getMaxTimestamp();
            if (timestampResolution.equals(TimeUnit.MICROSECONDS))
                tStamp = tStamp / 1000L;
            else if (timestampResolution.equals(TimeUnit.NANOSECONDS))
                tStamp = tStamp / 1000000L;
            else if (timestampResolution.equals(TimeUnit.SECONDS))
                tStamp = tStamp * 1000L;
            else // Better be milliseconds
                assert TimeWindowCompactionStrategyOptions.validTimestampTimeUnits.contains(timestampResolution);

            Pair<Long,Long> bounds = getWindowBoundsInMillis(sstableWindowUnit, sstableWindowSize, tStamp);
            buckets.put(bounds.left, f );
        }

        logger.debug("buckets {}", buckets);
        return buckets;
    }

    private void updateEstimatedCompactionsByTasks(HashMultimap<Long, SSTableReader> tasks)
    {
        int n = 0;
        long now = System.currentTimeMillis();

        Pair<Long,Long> nowBounds = getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, now);
        now = nowBounds.left;
        for(Long key : tasks.keySet())
        {
            // For current window, make sure it's compactable
            if (key.compareTo(now) >= 0 && tasks.get(key).size() >= cfs.getMinimumCompactionThreshold())
            {
                n++;
            }
            else if (key.compareTo(now) < 0 && tasks.get(key).size() >= 2)
            {
                n++;
            }
        }
        this.estimatedRemainingTasks = n;
    }


    /**
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @param sstableWindowUnit - TimeUnit to compact sstables into
     * @param sstableWindowSize - Number of TimeUnits for compaction window
     * @param stcsOptions - STCS Options for first time window
     * @return a bucket (list) of sstables to compact.
     */
    @VisibleForTesting
    static List<SSTableReader> newestBucket(HashMultimap<Long, SSTableReader> buckets, int minThreshold, int maxThreshold, TimeUnit sstableWindowUnit, int sstableWindowSize, SizeTieredCompactionStrategyOptions stcsOptions)
    {
        // If the current bucket has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.

        long now = System.currentTimeMillis();

        Pair<Long,Long> nowBounds = getWindowBoundsInMillis(sstableWindowUnit, sstableWindowSize, now);
        now = nowBounds.left;

        TreeSet<Long> allKeys = new TreeSet<>(buckets.keySet());

        Iterator<Long> it = allKeys.descendingIterator();
        while(it.hasNext())
        {
            Long key = it.next();
            Set<SSTableReader> bucket = buckets.get(key);
            if (bucket.size() >= minThreshold && key >= now)
            {

                // If we're in the newest bucket, we'll use STCS to prioritize sstables
                List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(bucket);
                List<List<SSTableReader>> stcsBuckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                                stcsOptions.bucketHigh,
                                                                                                stcsOptions.bucketLow,
                                                                                                stcsOptions.minSSTableSize);
                logger.debug("Using STCS compaction for first window of bucket: data files {} , options {}", pairs, stcsOptions);
                List<SSTableReader> stcsInterestingBucket =  SizeTieredCompactionStrategy.mostInterestingBucket(stcsBuckets, minThreshold, maxThreshold);

                // If the tables in the current bucket aren't eligible in the STCS strategy, we'll skip it and look for other buckets
                if(!stcsInterestingBucket.isEmpty())
                    return stcsInterestingBucket;
            }
            else if (bucket.size() >= 2 && key < now)
            {
                // If we're not in the newest bucket, compact as many sstables as quickly as possible until there's only 1 left
                logger.debug("bucket size {} >= 2 and not in current bucket, compacting what's here: {}", bucket.size(), bucket);
                return trimToThreshold(bucket, maxThreshold);
            }
            else
            {
                logger.debug("No compaction necessary for bucket size {} , key {}, now {}", bucket.size(), key, now);
            }
        }
        return Collections.emptyList();
    }

    /**
     * @param bucket set of sstables
     * @param maxThreshold maximum number of sstables in a single compaction task.
     * @return A bucket trimmed to the maxThreshold newest sstables.
     */
    @VisibleForTesting
    static List<SSTableReader> trimToThreshold(Set<SSTableReader> bucket, int maxThreshold)
    {
        List<SSTableReader> ssTableReaders = new ArrayList<>(bucket);

        // Trim the largest sstables off the end to meet the maxThreshold
        Collections.sort(ssTableReaders, new SSTableReader.SizeComparator());

        return ImmutableList.copyOf(Iterables.limit(ssTableReaders, maxThreshold));
    }

    @Override
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore)
    {
        Iterable<SSTableReader> sstables = cfs.markAllCompacting();
        if (sstables == null)
            return null;

        return Arrays.<AbstractCompactionTask>asList(new CompactionTask(cfs, sstables, gcBefore, false));
    }

    @Override
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        if (!cfs.getDataTracker().markCompacting(sstables))
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new CompactionTask(cfs, sstables, gcBefore, false).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return this.estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }


    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = TimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD);
        uncheckedOptions.remove(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD);

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("TimeWindowCompactionStrategy[%s/%s]",
                cfs.getMinimumCompactionThreshold(),
                cfs.getMaximumCompactionThreshold());
    }

    /**
     * Check if given sstable is worth dropping tombstones at gcBefore.
     * Check is skipped if tombstone_compaction_interval time does not elapse since sstable creation and returns false.
     *
     * @param sstable SSTable to check
     * @param gcBefore time to drop tombstones
     * @return true if given sstable's tombstones are expected to be removed
     */
    protected boolean reachedMaxTombstonesRatio(SSTableReader sstable, int gcBefore)
    {
        if (disableTombstoneCompactions)
            return false;
        // since we use estimations to calculate, there is a chance that compaction will not drop tombstones actually.
        // if that happens we will end up in infinite compaction loop, so first we check enough if enough time has
        // elapsed since SSTable created.
        if (System.currentTimeMillis() < sstable.getCreationTimeFor(Component.DATA) + tombstoneCompactionInterval * 1000)
            return false;

        double droppableRatio = sstable.getEstimatedDroppableTombstoneRatio(gcBefore);

        if (droppableRatio <= options.tombstoneMaxRatio)
            return false;

        //sstable range overlap check is disabled. See CASSANDRA-6563.
        if (uncheckedTombstoneCompaction)
            return true;

        Collection<SSTableReader> overlaps = cfs.getOverlappingSSTables(Collections.singleton(sstable));
        if (overlaps.isEmpty())
        {
            // there is no overlap, tombstones are safely droppable
            return true;
        }
        else if (CompactionController.getFullyExpiredSSTables(cfs, Collections.singleton(sstable), overlaps, gcBefore).size() > 0)
        {
            return true;
        }
        else
        {
            // what percentage of columns do we expect to compact outside of overlap?
            if (sstable.getIndexSummarySize() < 2)
            {
                // we have too few samples to estimate correct percentage
                return false;
            }
            // first, calculate estimated keys that do not overlap
            long keys = sstable.estimatedKeys();
            Set<Range<Token>> ranges = new HashSet<Range<Token>>(overlaps.size());
            for (SSTableReader overlap : overlaps)
                ranges.add(new Range<Token>(overlap.first.getToken(), overlap.last.getToken(), overlap.partitioner));
            long remainingKeys = keys - sstable.estimatedKeysForRanges(ranges);
            // next, calculate what percentage of columns we have within those keys
            long columns = sstable.getEstimatedColumnCount().mean() * remainingKeys;
            double remainingColumnsRatio = ((double) columns) / (sstable.getEstimatedColumnCount().count() * sstable.getEstimatedColumnCount().mean());

            // return if we still expect to have droppable tombstones in rest of columns
            return remainingColumnsRatio * droppableRatio > tombstoneThreshold;
        }
    }

}