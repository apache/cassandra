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
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;

import static com.google.common.collect.Iterables.filter;
import static org.apache.cassandra.db.compaction.CompactionStrategyOptions.TOMBSTONE_COMPACTION_INTERVAL_OPTION;
import static org.apache.cassandra.db.compaction.CompactionStrategyOptions.TOMBSTONE_THRESHOLD_OPTION;
import static org.apache.cassandra.db.compaction.CompactionStrategyOptions.UNCHECKED_TOMBSTONE_COMPACTION_OPTION;

public class TimeWindowCompactionStrategy extends LegacyAbstractCompactionStrategy.WithAggregates
{
    private static final Logger logger = LoggerFactory.getLogger(TimeWindowCompactionStrategy.class);

    private final TimeWindowCompactionStrategyOptions twcsOptions;
    private final Set<CompactionSSTable> sstables = new HashSet<>();
    private long lastExpiredCheck;
    private long highestWindowSeen;

    public TimeWindowCompactionStrategy(CompactionStrategyFactory factory, Map<String, String> options)
    {
        super(factory, options);
        this.twcsOptions = new TimeWindowCompactionStrategyOptions(options);
        String[] tsOpts = { UNCHECKED_TOMBSTONE_COMPACTION_OPTION, TOMBSTONE_COMPACTION_INTERVAL_OPTION, TOMBSTONE_THRESHOLD_OPTION };
        if (Arrays.stream(tsOpts).map(options::get).filter(Objects::nonNull).anyMatch(v -> !v.equals("false")))
        {
            logger.debug("Enabling tombstone compactions for TWCS");
        }
        else
        {
            logger.debug("Disabling tombstone compactions for TWCS");
            super.options.setDisableTombstoneCompactions(true);
        }
    }

    @Override
    public AbstractCompactionTask createCompactionTask(final int gcBefore,
                                                       LifecycleTransaction txn,
                                                       boolean isMaximal,
                                                       boolean splitOutput)
    {
        return new TimeWindowCompactionTask(realm, txn, gcBefore, ignoreOverlaps(), this);
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    @Override
    protected synchronized CompactionAggregate getNextBackgroundAggregate(final int gcBefore)
    {
        if (realm.getLiveSSTables().isEmpty())
            return null;

        Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();
        Set<CompactionSSTable> noncompacting;
        synchronized (sstables)
        {
            noncompacting = ImmutableSet.copyOf(filter(sstables, sstable -> !compacting.contains(sstable)));
        }

        // Find fully expired SSTables. Those will be included no matter what.
        Set<CompactionSSTable> expired = Collections.emptySet();

        if (System.currentTimeMillis() - lastExpiredCheck > twcsOptions.expiredSSTableCheckFrequency)
        {
            logger.debug("TWCS expired check sufficiently far in the past, checking for fully expired SSTables");
            expired = CompactionController.getFullyExpiredSSTables(realm,
                                                                   noncompacting,
                                                                   twcsOptions.ignoreOverlaps
                                                                       ? Collections.emptySet()
                                                                       : realm.getOverlappingLiveSSTables(noncompacting),
                                                                   gcBefore,
                                                                   twcsOptions.ignoreOverlaps);
            lastExpiredCheck = System.currentTimeMillis();
        }
        else
        {
            logger.debug("TWCS skipping check for fully expired SSTables");
        }

        Set<CompactionSSTable> candidates = Sets.newHashSet(Iterables.filter(noncompacting, sstable -> !sstable.isMarkedSuspect()));

        CompactionAggregate compactionCandidate = getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore);
        if (expired.isEmpty())
            return compactionCandidate;

        logger.debug("Including expired sstables: {}", expired);
        if (compactionCandidate == null)
        {
            long timestamp = getWindowBoundsInMillis(twcsOptions.sstableWindowUnit, twcsOptions.sstableWindowSize,
                                                     Collections.max(expired, Comparator.comparing(CompactionSSTable::getMaxTimestamp)).getMaxTimestamp());
            return CompactionAggregate.createTimeTiered(expired, timestamp);
        }

        return compactionCandidate.withExpired(expired);
    }

    private CompactionAggregate getNextNonExpiredSSTables(Iterable<CompactionSSTable> nonExpiringSSTables, final int gcBefore)
    {
        List<CompactionAggregate> candidates = getCompactionCandidates(nonExpiringSSTables);
        backgroundCompactions.setPending(this, candidates);

        CompactionAggregate ret = candidates.isEmpty() ? null : candidates.get(0);

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        if (ret == null || ret.isEmpty())
            ret = makeTombstoneCompaction(gcBefore, nonExpiringSSTables, list -> Collections.min(list, CompactionSSTable.sizeComparator));

        return ret;
    }

    private List<CompactionAggregate> getCompactionCandidates(Iterable<CompactionSSTable> candidateSSTables)
    {
        NavigableMap<Long, List<CompactionSSTable>> buckets = getBuckets(candidateSSTables, twcsOptions.sstableWindowUnit, twcsOptions.sstableWindowSize, twcsOptions.timestampResolution);
        // Update the highest window seen, if necessary
        if (!buckets.isEmpty())
        {
            long maxKey = buckets.lastKey();
            if (maxKey > this.highestWindowSeen)
                this.highestWindowSeen = maxKey;
        }

        return getBucketAggregates(buckets,
                                   realm.getMinimumCompactionThreshold(),
                                   realm.getMaximumCompactionThreshold(),
                                   twcsOptions.stcsOptions,
                                   this.highestWindowSeen);
    }


    @Override
    public void replaceSSTables(Collection<CompactionSSTable> removed, Collection<CompactionSSTable> added)
    {
        synchronized (sstables)
        {
            for (CompactionSSTable remove : removed)
                sstables.remove(remove);
            sstables.addAll(added);
        }
    }

    @Override
    public void addSSTable(CompactionSSTable sstable)
    {
        synchronized (sstables)
        {
            sstables.add(sstable);
        }
    }

    @Override
    void removeDeadSSTables()
    {
        removeDeadSSTables(sstables);
    }

    @Override
    public void removeSSTable(CompactionSSTable sstable)
    {
        synchronized (sstables)
        {
            sstables.remove(sstable);
        }
    }

    @Override
    public Set<CompactionSSTable> getSSTables()
    {
        synchronized (sstables)
        {
            return ImmutableSet.copyOf(sstables);
        }
    }

    /**
     * Find the lowest timestamp in a given window/unit pair and
     * return it expressed as milliseconds, the caller should adjust accordingly
     */
    static long getWindowBoundsInMillis(TimeUnit windowTimeUnit, int windowTimeSize, long timestampInMillis)
    {

        long sizeInMillis = TimeUnit.MILLISECONDS.convert(windowTimeSize, windowTimeUnit);
        return (timestampInMillis / sizeInMillis) * sizeInMillis;
    }

    /**
     * Group files with similar max timestamp into buckets.
     * <p/>
     * The max timestamp of each sstable is converted into the timestamp resolution and then the window bounds are
     * calculated by calling {@link #getWindowBoundsInMillis(TimeUnit, int, long)}. The sstable is added to the bucket
     * with the same lower timestamp bound. If the lower timestamp bound is higher than any other seen, then it is recorded
     * as the max timestamp seen that will be returned.
     *
     * @param files the candidate sstables
     * @param sstableWindowUnit the time unit for {@code sstableWindowSize}
     * @param sstableWindowSize the size of the time window by which sstables are grouped
     * @param timestampResolution the time unit for converting the sstable timestamp
     * @return A pair, where the left element is the bucket representation (multi-map of lower bound timestamp to sstables),
     *         and the right is the highest lower bound timestamp seen
     */
    @VisibleForTesting
    static NavigableMap<Long, List<CompactionSSTable>> getBuckets(Iterable<CompactionSSTable> files, TimeUnit sstableWindowUnit, int sstableWindowSize, TimeUnit timestampResolution)
    {
        NavigableMap<Long, List<CompactionSSTable>> buckets = new TreeMap<>(Long::compare);

        // For each sstable, add sstable to the time bucket
        // Where the bucket is the file's max timestamp rounded to the nearest window bucket
        for (CompactionSSTable f : files)
        {
            assert TimeWindowCompactionStrategyOptions.validTimestampTimeUnits.contains(timestampResolution);
            long tStamp = TimeUnit.MILLISECONDS.convert(f.getMaxTimestamp(), timestampResolution);
            addToBuckets(buckets, f, tStamp, sstableWindowUnit, sstableWindowSize);
        }

        logger.trace("buckets {}, max timestamp {}", buckets, buckets.isEmpty() ? "none" : buckets.lastKey().toString());
        return buckets;
    }

    @VisibleForTesting
    static void addToBuckets(NavigableMap<Long, List<CompactionSSTable>> buckets, CompactionSSTable f, long tStamp, TimeUnit sstableWindowUnit, int sstableWindowSize)
    {
        long bound = getWindowBoundsInMillis(sstableWindowUnit, sstableWindowSize, tStamp);
        buckets.computeIfAbsent(bound,
                                key -> new ArrayList<>())
               .add(f);
    }

    /**
     * If the current bucket has at least minThreshold SSTables, choose that one. For any other bucket, at least 2 SSTables is enough.
     * In any case, limit to maxThreshold SSTables.
     *
     * @param buckets A map from a bucket id to a set of tables, sorted by id and then by table size
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @param stcsOptions the options for {@link SizeTieredCompactionStrategy} to be used in the newest bucket
     * @param now the latest timestamp in milliseconds
     *
     * @return a list of compaction aggregates, one per time bucket
     */
    @VisibleForTesting
    static List<CompactionAggregate> getBucketAggregates(NavigableMap<Long, List<CompactionSSTable>> buckets,
                                                         int minThreshold,
                                                         int maxThreshold,
                                                         SizeTieredCompactionStrategyOptions stcsOptions,
                                                         long now)
    {
        List<CompactionAggregate> ret = new ArrayList<>(buckets.size());
        boolean nextCompactionFound = false; // set to true once the first bucket with a compaction is found

        for (Map.Entry<Long, List<CompactionSSTable>> entry : buckets.descendingMap().entrySet())
        {
            Long key = entry.getKey();
            List<CompactionSSTable> bucket = entry.getValue();
            logger.trace("Key {}, now {}", key, now);

            CompactionPick selected = CompactionPick.EMPTY;
            List<CompactionPick> pending = new ArrayList<>(1);

            if (bucket.size() >= minThreshold && key >= now)
            {
                // If we're in the newest bucket, we'll use STCS to prioritize sstables
                SizeTieredCompactionStrategy.SizeTieredBuckets stcsBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(bucket,
                                                                                                                                stcsOptions,
                                                                                                                                minThreshold,
                                                                                                                                maxThreshold);
                stcsBuckets.aggregate();

                for (CompactionAggregate stcsAggregate : stcsBuckets.getAggregates())
                {
                    if (selected.isEmpty())
                    {
                        selected = CompactionPick.create(key, stcsAggregate.getSelected());
                        for (CompactionPick comp : stcsAggregate.getActive())
                        {
                            if (comp != stcsAggregate.getSelected())
                                pending.add(comp);
                        }
                    }
                    else
                    {
                        pending.addAll(stcsAggregate.getActive());
                    }
                }

                if (!selected.isEmpty())
                    logger.debug("Newest window has STCS compaction candidates, {}, data files {} , options {}",
                                 nextCompactionFound ? "eligible but not selected due to prior candidate" : "will be selected for compaction",
                                 stcsBuckets.pairs(),
                                 stcsOptions);
                else
                    logger.debug("No STCS compactions found for first window, data files {}, options {}", stcsBuckets.pairs(), stcsOptions);

                if (!nextCompactionFound && !selected.isEmpty())
                {
                    nextCompactionFound = true;
                    ret.add(0, CompactionAggregate.createTimeTiered(bucket, selected, pending, key)); // the first one will be submitted for compaction
                }
                else
                {
                    ret.add(CompactionAggregate.createTimeTiered(bucket, selected, pending, key));
                }
            }
            else if (bucket.size() >= 2 && key < now)
            {
                List<CompactionSSTable> sstables = bucket;

                // Sort the largest sstables off the end before splitting by maxThreshold
                Collections.sort(sstables, CompactionSSTable.sizeComparator);

                int i = 0;
                while ((bucket.size() - i) >= 2)
                {
                    List<CompactionSSTable> pick = sstables.subList(i, i + Math.min(bucket.size() - i, maxThreshold));
                    if (selected.isEmpty())
                        selected = CompactionPick.create(key, pick);
                    else
                        pending.add(CompactionPick.create(key, pick));

                    i += pick.size();
                }

                if (!nextCompactionFound)
                {
                    logger.debug("bucket size {} >= 2 and not in current bucket, compacting what's here: {}", bucket.size(), bucket);
                    nextCompactionFound = true;
                    ret.add(0, CompactionAggregate.createTimeTiered(bucket, selected, pending, key)); // the first one will be submitted for compaction
                }
                else
                {
                    logger.trace("bucket size {} >= 2 and not in current bucket, eligible but not selected: {}", bucket.size(), bucket);
                    ret.add(CompactionAggregate.createTimeTiered(bucket, selected, pending, key));
                }
            }
            else
            {
                logger.trace("No compaction necessary for bucket size {} , key {}, now {}", bucket.size(), key, now);
                ret.add(CompactionAggregate.createTimeTiered(bucket, selected, pending, key)); // add an empty aggregate anyway so we get a full view
            }
        }
        return ret;
    }

    boolean ignoreOverlaps()
    {
        return twcsOptions.ignoreOverlaps;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }


    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = CompactionStrategyOptions.validateOptions(options);
        uncheckedOptions = TimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("TimeWindowCompactionStrategy[%s/%s]",
                             realm.getMinimumCompactionThreshold(),
                             realm.getMaximumCompactionThreshold());
    }
}
