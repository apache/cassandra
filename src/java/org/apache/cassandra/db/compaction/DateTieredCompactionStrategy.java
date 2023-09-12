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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

/**
 * @deprecated in favour of {@link TimeWindowCompactionStrategy}
 */
@Deprecated
public class DateTieredCompactionStrategy extends LegacyAbstractCompactionStrategy.WithSSTableList
{
    private static final Logger logger = LoggerFactory.getLogger(DateTieredCompactionStrategy.class);

    private final DateTieredCompactionStrategyOptions dtOptions;
    protected volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private long lastExpiredCheck;
    private final SizeTieredCompactionStrategyOptions stcsOptions;

    public DateTieredCompactionStrategy(CompactionStrategyFactory factory, Map<String, String> options)
    {
        super(factory, options);
        this.estimatedRemainingTasks = 0;
        this.dtOptions = new DateTieredCompactionStrategyOptions(options);
        if (!options.containsKey(CompactionStrategyOptions.TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.containsKey(CompactionStrategyOptions.TOMBSTONE_THRESHOLD_OPTION))
        {
            super.options.setDisableTombstoneCompactions(true);
            logger.trace("Disabling tombstone compactions for DTCS");
        }
        else
            logger.trace("Enabling tombstone compactions for DTCS");

        this.stcsOptions = new SizeTieredCompactionStrategyOptions(options);
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    protected synchronized List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        Set<SSTableReader> uncompacting;
        synchronized (sstables)
        {
            if (sstables.isEmpty())
                return ImmutableList.of();

            uncompacting = ImmutableSet.copyOf(cfs.getNoncompactingSSTables(sstables));
        }

        Set<SSTableReader> expired = Collections.emptySet();
        // we only check for expired sstables every 10 minutes (by default) due to it being an expensive operation
        if (System.currentTimeMillis() - lastExpiredCheck > dtOptions.expiredSSTableCheckFrequency)
        {
            // Find fully expired SSTables. Those will be included no matter what.
            expired = CompactionController.getFullyExpiredSSTables(cfs, uncompacting, cfs.getOverlappingLiveSSTables(uncompacting), gcBefore);
            lastExpiredCheck = System.currentTimeMillis();
        }
        Set<SSTableReader> candidates = Sets.newHashSet(Iterables.filter(uncompacting, sstable -> !sstable.isMarkedSuspect()));

        List<SSTableReader> compactionCandidates = new ArrayList<>(getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore));
        if (!expired.isEmpty())
        {
            logger.trace("Including expired sstables: {}", expired);
            compactionCandidates.addAll(expired);
        }
        return compactionCandidates;
    }

    private List<SSTableReader> getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, final int gcBefore)
    {
        int base = cfs.getMinimumCompactionThreshold();
        long now = getNow();
        List<SSTableReader> mostInteresting = getCompactionCandidates(nonExpiringSSTables, now, base);
        if (mostInteresting != null)
        {
            return mostInteresting;
        }

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = Lists.newArrayList();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.min(sstablesWithTombstones, SSTableReader.sizeComparator));
    }

    private List<SSTableReader> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables, long now, int base)
    {
        Iterable<SSTableReader> candidates = filterOldSSTables(Lists.newArrayList(candidateSSTables), dtOptions.maxSSTableAge, now);

        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndMinTimestampPairs(candidates), dtOptions.baseTime, base, now, dtOptions.maxWindowSize);
        logger.debug("Compaction buckets are {}", buckets);
        updateEstimatedCompactionsByTasks(buckets);
        List<SSTableReader> mostInteresting = newestBucket(buckets,
                                                           cfs.getMinimumCompactionThreshold(),
                                                           cfs.getMaximumCompactionThreshold(),
                                                           now,
                                                           dtOptions.baseTime,
                                                           dtOptions.maxWindowSize,
                                                           stcsOptions);
        if (!mostInteresting.isEmpty())
            return mostInteresting;
        return null;
    }

    /**
     * Gets the timestamp that DateTieredCompactionStrategy considers to be the "current time".
     * @return the maximum timestamp across all SSTables.
     * @throws java.util.NoSuchElementException if there are no SSTables.
     */
    private long getNow()
    {
        // no need to convert to collection if had an Iterables.max(), but not present in standard toolkit, and not worth adding
        List<SSTableReader> list = new ArrayList<>();
        Iterables.addAll(list, cfs.getSSTables(SSTableSet.LIVE));
        if (list.isEmpty())
            return 0;
        return Collections.max(list, (o1, o2) -> Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp()))
                          .getMaxTimestamp();
    }

    /**
     * Removes all sstables with max timestamp older than maxSSTableAge.
     * @param sstables all sstables to consider
     * @param maxSSTableAge the age in milliseconds when an SSTable stops participating in compactions
     * @param now current time. SSTables with max timestamp less than (now - maxSSTableAge) are filtered.
     * @return a list of sstables with the oldest sstables excluded
     */
    @VisibleForTesting
    static Iterable<SSTableReader> filterOldSSTables(List<SSTableReader> sstables, long maxSSTableAge, long now)
    {
        if (maxSSTableAge == 0)
            return sstables;
        final long cutoff = now - maxSSTableAge;
        return filter(sstables, new Predicate<SSTableReader>()
        {
            @Override
            public boolean apply(SSTableReader sstable)
            {
                return sstable.getMaxTimestamp() >= cutoff;
            }
        });
    }

    public static List<Pair<SSTableReader, Long>> createSSTableAndMinTimestampPairs(Iterable<SSTableReader> sstables)
    {
        List<Pair<SSTableReader, Long>> sstableMinTimestampPairs = Lists.newArrayListWithCapacity(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            sstableMinTimestampPairs.add(Pair.create(sstable, sstable.getMinTimestamp()));
        return sstableMinTimestampPairs;
    }

    public void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        synchronized (sstables)
        {
            for (SSTableReader remove : removed)
                removeSSTable(remove);
            addSSTables(added);
        }
    }

    @Override
    public void addSSTable(SSTableReader sstable)
    {
        synchronized (sstables)
        {
            sstables.add(sstable);
        }
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        synchronized (sstables)
        {
            sstables.remove(sstable);
        }
    }

    @Override
    void removeDeadSSTables()
    {
        removeDeadSSTables(sstables);
    }

    @Override
    public Set<SSTableReader> getSSTables()
    {
        synchronized (sstables)
        {
            return ImmutableSet.copyOf(sstables);
        }
    }

    /**
     * A target time span used for bucketing SSTables based on timestamps.
     */
    private static class Target
    {
        // How big a range of timestamps fit inside the target.
        public final long size;
        // A timestamp t hits the target iff t / size == divPosition.
        public final long divPosition;

        public final long maxWindowSize;

        public Target(long size, long divPosition, long maxWindowSize)
        {
            this.size = size;
            this.divPosition = divPosition;
            this.maxWindowSize = maxWindowSize;
        }

        /**
         * Compares the target to a timestamp.
         * @param timestamp the timestamp to compare.
         * @return a negative integer, zero, or a positive integer as the target lies before, covering, or after than the timestamp.
         */
        public int compareToTimestamp(long timestamp)
        {
            return Long.compare(divPosition, timestamp / size);
        }

        /**
         * Tells if the timestamp hits the target.
         * @param timestamp the timestamp to test.
         * @return <code>true</code> iff timestamp / size == divPosition.
         */
        public boolean onTarget(long timestamp)
        {
            return compareToTimestamp(timestamp) == 0;
        }

        /**
         * Gets the next target, which represents an earlier time span.
         * @param base The number of contiguous targets that will have the same size. Targets following those will be <code>base</code> times as big.
         * @return
         */
        public Target nextTarget(int base)
        {
            if (divPosition % base > 0 || size * base > maxWindowSize)
                return new Target(size, divPosition - 1, maxWindowSize);
            else
                return new Target(size * base, divPosition / base - 1, maxWindowSize);
        }
    }


    /**
     * Group files with similar min timestamp into buckets. Files with recent min timestamps are grouped together into
     * buckets designated to short timespans while files with older timestamps are grouped into buckets representing
     * longer timespans.
     * @param files pairs consisting of a file and its min timestamp
     * @param timeUnit
     * @param base
     * @param now
     * @return a list of buckets of files. The list is ordered such that the files with newest timestamps come first.
     *         Each bucket is also a list of files ordered from newest to oldest.
     */
    @VisibleForTesting
    static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, long timeUnit, int base, long now, long maxWindowSize)
    {
        // Sort files by age. Newest first.
        final List<Pair<T, Long>> sortedFiles = Lists.newArrayList(files);
        Collections.sort(sortedFiles, Collections.reverseOrder(new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        }));

        List<List<T>> buckets = Lists.newArrayList();
        Target target = getInitialTarget(now, timeUnit, maxWindowSize);
        PeekingIterator<Pair<T, Long>> it = Iterators.peekingIterator(sortedFiles.iterator());

        outerLoop:
        while (it.hasNext())
        {
            while (!target.onTarget(it.peek().right))
            {
                // If the file is too new for the target, skip it.
                if (target.compareToTimestamp(it.peek().right) < 0)
                {
                    it.next();

                    if (!it.hasNext())
                        break outerLoop;
                }
                else // If the file is too old for the target, switch targets.
                    target = target.nextTarget(base);
            }
            List<T> bucket = Lists.newArrayList();
            while (target.onTarget(it.peek().right))
            {
                bucket.add(it.next().left);

                if (!it.hasNext())
                    break;
            }
            buckets.add(bucket);
        }

        return buckets;
    }

    @VisibleForTesting
    static Target getInitialTarget(long now, long timeUnit, long maxWindowSize)
    {
        return new Target(timeUnit, now / timeUnit, maxWindowSize);
    }


    private void updateEstimatedCompactionsByTasks(List<List<SSTableReader>> tasks)
    {
        int n = 0;
        for (List<SSTableReader> bucket : tasks)
        {
            for (List<SSTableReader> stcsBucket : getSTCSBuckets(bucket, stcsOptions, cfs.getMinimumCompactionThreshold(), cfs.getMaximumCompactionThreshold()))
                if (stcsBucket.size() >= cfs.getMinimumCompactionThreshold())
                    n += Math.ceil((double)stcsBucket.size() / cfs.getMaximumCompactionThreshold());
        }
        estimatedRemainingTasks = n;
        getCompactionLogger().pending(this, n);
    }


    /**
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @return a bucket (list) of sstables to compact.
     */
    @VisibleForTesting
    static List<SSTableReader> newestBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold, long now, long baseTime, long maxWindowSize, SizeTieredCompactionStrategyOptions stcsOptions)
    {
        // If the "incoming window" has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.
        Target incomingWindow = getInitialTarget(now, baseTime, maxWindowSize);
        for (List<SSTableReader> bucket : buckets)
        {
            boolean inFirstWindow = incomingWindow.onTarget(bucket.get(0).getMinTimestamp());
            if (bucket.size() >= minThreshold || (bucket.size() >= 2 && !inFirstWindow))
            {
                List<SSTableReader> stcsSSTables = getSSTablesForSTCS(bucket, stcsOptions, inFirstWindow ? minThreshold : 2, maxThreshold);
                if (!stcsSSTables.isEmpty())
                    return stcsSSTables;
            }
        }
        return Collections.emptyList();
    }

    private static List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables, SizeTieredCompactionStrategyOptions stcsOptions, int minThreshold, int maxThreshold)
    {
        SizeTieredCompactionStrategy.SizeTieredBuckets sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstables,
                                                                                                                              stcsOptions,
                                                                                                                              minThreshold,
                                                                                                                              maxThreshold);

        sizeTieredBuckets.aggregate();
        List<SSTableReader> s = new ArrayList<>(CompactionAggregate.getSelected(sizeTieredBuckets.getAggregates()).sstables);
        logger.debug("Got sstables {} for STCS from {}", s, sstables);
        return s;
    }

    private static List<List<SSTableReader>> getSTCSBuckets(Collection<SSTableReader> sstables, SizeTieredCompactionStrategyOptions stcsOptions, int minThreshold, int maxThreshold)
    {
        SizeTieredCompactionStrategy.SizeTieredBuckets sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstables,
                                                                                                                              stcsOptions,
                                                                                                                              minThreshold,
                                                                                                                              maxThreshold);
        return sizeTieredBuckets.buckets();
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    /**
     * DTCS should not group sstables for anticompaction - this can mix new and old data
     */
    @Override
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        Collection<Collection<SSTableReader>> groups = new ArrayList<>(sstablesToGroup.size());
        for (SSTableReader sstable : sstablesToGroup)
        {
            groups.add(Collections.singleton(sstable));
        }
        return groups;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = CompactionStrategyOptions.validateOptions(options);
        uncheckedOptions = DateTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("DateTieredCompactionStrategy[%s/%s]",
                cfs.getMinimumCompactionThreshold(),
                cfs.getMaximumCompactionThreshold());
    }
}
