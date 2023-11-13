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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.ShardedMultiWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.TimeUUID;

/**
 * The design of the unified compaction strategy is described in the accompanying UnifiedCompactionStrategy.md.
 *
 * See CEP-26: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-26%3A+Unified+Compaction+Strategy
 */
public class UnifiedCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(UnifiedCompactionStrategy.class);

    static final int MAX_LEVELS = 32;   // This is enough for a few petabytes of data (with the worst case fan factor
    // at W=0 this leaves room for 2^32 sstables, presumably of at least 1MB each).

    private static final Pattern SCALING_PARAMETER_PATTERN = Pattern.compile("(N)|L(\\d+)|T(\\d+)|([+-]?\\d+)");
    private static final String SCALING_PARAMETER_PATTERN_SIMPLIFIED = SCALING_PARAMETER_PATTERN.pattern()
                                                                                                .replaceAll("[()]", "")
                                                                                                .replace("\\d", "[0-9]");

    private final Controller controller;

    private volatile ShardManager shardManager;

    private long lastExpiredCheck;

    protected volatile int estimatedRemainingTasks;
    @VisibleForTesting
    protected final Set<SSTableReader> sstables = new HashSet<>();

    public UnifiedCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        this(cfs, options, Controller.fromOptions(cfs, options));
    }

    public UnifiedCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options, Controller controller)
    {
        super(cfs, options);
        this.controller = controller;
        estimatedRemainingTasks = 0;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return Controller.validateOptions(AbstractCompactionStrategy.validateOptions(options));
    }

    public static int fanoutFromScalingParameter(int w)
    {
        return w < 0 ? 2 - w : 2 + w; // see formula in design doc
    }

    public static int thresholdFromScalingParameter(int w)
    {
        return w <= 0 ? 2 : 2 + w; // see formula in design doc
    }

    public static int parseScalingParameter(String value)
    {
        Matcher m = SCALING_PARAMETER_PATTERN.matcher(value);
        if (!m.matches())
            throw new ConfigurationException("Scaling parameter " + value + " must match " + SCALING_PARAMETER_PATTERN_SIMPLIFIED);

        if (m.group(1) != null)
            return 0;
        else if (m.group(2) != null)
            return 2 - atLeast2(Integer.parseInt(m.group(2)), value);
        else if (m.group(3) != null)
            return atLeast2(Integer.parseInt(m.group(3)), value) - 2;
        else
            return Integer.parseInt(m.group(4));
    }

    private static int atLeast2(int value, String str)
    {
        if (value < 2)
            throw new ConfigurationException("Fan factor cannot be lower than 2 in " + str);
        return value;
    }

    public static String printScalingParameter(int w)
    {
        if (w < 0)
            return "L" + Integer.toString(2 - w);
        else if (w > 0)
            return "T" + Integer.toString(w + 2);
        else
            return "N";
    }

    @Override
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(long gcBefore, boolean splitOutput)
    {
        maybeUpdateShardManager();
        // The tasks are split by repair status and disk, as well as in non-overlapping sections to enable some
        // parallelism (to the amount that L0 sstables are split, i.e. at least base_shard_count). The result will be
        // split across shards according to its density. Depending on the parallelism, the operation may require up to
        // 100% extra space to complete.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        List<Set<SSTableReader>> nonOverlapping = splitInNonOverlappingSets(filterSuspectSSTables(getSSTables()));
        for (Set<SSTableReader> set : nonOverlapping)
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(set, OperationType.COMPACTION);
            if (txn != null)
                tasks.add(createCompactionTask(txn, gcBefore));
        }
        return tasks;
    }

    private static List<Set<SSTableReader>> splitInNonOverlappingSets(Collection<SSTableReader> sstables)
    {
        List<Set<SSTableReader>> overlapSets = Overlaps.constructOverlapSets(new ArrayList<>(sstables),
                                                                             UnifiedCompactionStrategy::startsAfter,
                                                                             SSTableReader.firstKeyComparator,
                                                                             SSTableReader.lastKeyComparator);
        if (overlapSets.isEmpty())
            return overlapSets;

        Set<SSTableReader> group = overlapSets.get(0);
        List<Set<SSTableReader>> groups = new ArrayList<>();
        for (int i = 1; i < overlapSets.size(); ++i)
        {
            Set<SSTableReader> current = overlapSets.get(i);
            if (Sets.intersection(current, group).isEmpty())
            {
                groups.add(group);
                group = current;
            }
            else
            {
                group.addAll(current);
            }
        }
        groups.add(group);
        return groups;
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final long gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return createCompactionTask(transaction, gcBefore).setUserDefined(true);
    }

    /**
     * Returns a compaction task to run next.
     *
     * This method is synchronized because task creation is significantly more expensive in UCS; the strategy is
     * stateless, therefore it has to compute the shard/bucket structure on each call.
     *
     * @param gcBefore throw away tombstones older than this
     */
    @Override
    public synchronized UnifiedCompactionTask getNextBackgroundTask(long gcBefore)
    {
        while (true)
        {
            CompactionPick pick = getNextCompactionPick(gcBefore);
            if (pick == null)
                return null;
            UnifiedCompactionTask task = createCompactionTask(pick, gcBefore);
            if (task != null)
                return task;
        }
    }

    private UnifiedCompactionTask createCompactionTask(CompactionPick pick, long gcBefore)
    {
        Preconditions.checkNotNull(pick);
        Preconditions.checkArgument(!pick.isEmpty());

        LifecycleTransaction transaction = cfs.getTracker().tryModify(pick,
                                                                      OperationType.COMPACTION);
        if (transaction != null)
        {
            return createCompactionTask(transaction, gcBefore);
        }
        else
        {
            // This can happen e.g. due to a race with upgrade tasks.
            logger.warn("Failed to submit compaction {} because a transaction could not be created. If this happens frequently, it should be reported", pick);
            // This may be an indication of an SSTableReader reference leak. See CASSANDRA-18342.
            return null;
        }
    }

    /**
     * Create the sstable writer used for flushing.
     *
     * @return an sstable writer that will split sstables into a number of shards as calculated by the controller for
     *         the expected flush density.
     */
    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       TimeUUID pendingRepair,
                                                       boolean isTransient,
                                                       IntervalSet<CommitLogPosition> commitLogPositions,
                                                       int sstableLevel,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        ShardManager shardManager = getShardManager();
        double flushDensity = cfs.metric.flushSizeOnDisk.get() * shardManager.shardSetCoverage() / shardManager.localSpaceCoverage();
        ShardTracker boundaries = shardManager.boundaries(controller.getNumShards(flushDensity));
        return new ShardedMultiWriter(cfs,
                                      descriptor,
                                      keyCount,
                                      repairedAt,
                                      pendingRepair,
                                      isTransient,
                                      commitLogPositions,
                                      header,
                                      indexGroups,
                                      lifecycleNewTracker,
                                      boundaries);
    }

    /**
     * Create the task that in turns creates the sstable writer used for compaction.
     *
     * @return a sharded compaction task that in turn will create a sharded compaction writer.
     */
    private UnifiedCompactionTask createCompactionTask(LifecycleTransaction transaction, long gcBefore)
    {
        return new UnifiedCompactionTask(cfs, this, transaction, gcBefore, getShardManager());
    }

    private void maybeUpdateShardManager()
    {
        if (shardManager != null && !shardManager.isOutOfDate(StorageService.instance.getTokenMetadata().getRingVersion()))
            return; // the disk boundaries (and thus the local ranges too) have not changed since the last time we calculated

        synchronized (this)
        {
            // Recheck after entering critical section, another thread may have beaten us to it.
            while (shardManager == null || shardManager.isOutOfDate(StorageService.instance.getTokenMetadata().getRingVersion()))
                shardManager = ShardManager.create(cfs);
            // Note: this can just as well be done without the synchronization (races would be benign, just doing some
            // redundant work). For the current usages of this blocking is fine and expected to perform no worse.
        }
    }

    @VisibleForTesting
    ShardManager getShardManager()
    {
        maybeUpdateShardManager();
        return shardManager;
    }

    /**
     * Selects a compaction to run next.
     */
    @VisibleForTesting
    CompactionPick getNextCompactionPick(long gcBefore)
    {
        SelectionContext context = new SelectionContext(controller);
        List<SSTableReader> suitable = getCompactableSSTables(getSSTables(), UnifiedCompactionStrategy::isSuitableForCompaction);
        Set<SSTableReader> expired = maybeGetExpiredSSTables(gcBefore, suitable);
        suitable.removeAll(expired);

        CompactionPick selected = chooseCompactionPick(suitable, context);
        estimatedRemainingTasks = context.estimatedRemainingTasks;
        if (selected == null)
        {
            if (expired.isEmpty())
                return null;
            else
                return new CompactionPick(-1, -1, expired);
        }

        selected.addAll(expired);
        return selected;
    }

    private Set<SSTableReader> maybeGetExpiredSSTables(long gcBefore, List<SSTableReader> suitable)
    {
        Set<SSTableReader> expired;
        long ts = Clock.Global.currentTimeMillis();
        if (ts - lastExpiredCheck > controller.getExpiredSSTableCheckFrequency())
        {
            lastExpiredCheck = ts;
            expired = CompactionController.getFullyExpiredSSTables(cfs,
                                                                   suitable,
                                                                   cfs.getOverlappingLiveSSTables(suitable),
                                                                   gcBefore,
                                                                   controller.getIgnoreOverlapsInExpirationCheck());
            if (logger.isTraceEnabled() && !expired.isEmpty())
                logger.trace("Expiration check for {}.{} found {} fully expired SSTables",
                             cfs.getKeyspaceName(),
                             cfs.getTableName(),
                             expired.size());
        }
        else
            expired = Collections.emptySet();
        return expired;
    }

    private CompactionPick chooseCompactionPick(List<SSTableReader> suitable, SelectionContext context)
    {
        // Select the level with the highest overlap; when multiple levels have the same overlap, prefer the lower one
        // (i.e. reduction of RA for bigger token coverage).
        int maxOverlap = -1;
        CompactionPick selected = null;
        for (Level level : formLevels(suitable))
        {
            CompactionPick pick = level.getCompactionPick(context);
            int levelOverlap = level.maxOverlap;
            if (levelOverlap > maxOverlap)
            {
                maxOverlap = levelOverlap;
                selected = pick;
            }
        }
        if (logger.isDebugEnabled() && selected != null)
            logger.debug("Selected compaction on level {} overlap {} sstables {}",
                         selected.level, selected.overlap, selected.size());

        return selected;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    @VisibleForTesting
    public Controller getController()
    {
        return controller;
    }

    public static boolean isSuitableForCompaction(SSTableReader rdr)
    {
        return !rdr.isMarkedSuspect() && rdr.openReason != SSTableReader.OpenReason.EARLY;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    @Override
    protected synchronized Set<SSTableReader> getSSTables()
    {
        // Filter the set of sstables through the live set. This is to ensure no zombie sstables are picked for
        // compaction (see CASSANDRA-18342).
        return ImmutableSet.copyOf(Iterables.filter(cfs.getLiveSSTables(), sstables::contains));
    }

    /**
     * @return a list of the levels in the compaction hierarchy
     */
    @VisibleForTesting
    List<Level> getLevels()
    {
        return getLevels(getSSTables(), UnifiedCompactionStrategy::isSuitableForCompaction);
    }

    /**
     * Groups the sstables passed in into levels. This is used by the strategy to determine
     * new compactions, and by external tools to analyze the strategy decisions.
     *
     * @param sstables a collection of the sstables to be assigned to levels
     * @param compactionFilter a filter to exclude CompactionSSTables,
     *                         e.g., {@link #isSuitableForCompaction}
     *
     * @return a list of the levels in the compaction hierarchy
     */
    public List<Level> getLevels(Collection<SSTableReader> sstables,
                                 Predicate<SSTableReader> compactionFilter)
    {
        List<SSTableReader> suitable = getCompactableSSTables(sstables, compactionFilter);
        return formLevels(suitable);
    }

    private List<Level> formLevels(List<SSTableReader> suitable)
    {
        maybeUpdateShardManager();
        List<Level> levels = new ArrayList<>(MAX_LEVELS);
        suitable.sort(shardManager::compareByDensity);

        double maxDensity = controller.getMaxLevelDensity(0, controller.getBaseSstableSize(controller.getFanout(0)) / shardManager.localSpaceCoverage());
        int index = 0;
        Level level = new Level(controller, index, 0, maxDensity);
        for (SSTableReader candidate : suitable)
        {
            final double density = shardManager.density(candidate);
            if (density < level.max)
            {
                level.add(candidate);
                continue;
            }

            level.complete();
            levels.add(level); // add even if empty

            while (true)
            {
                ++index;
                double minDensity = maxDensity;
                maxDensity = controller.getMaxLevelDensity(index, minDensity);
                level = new Level(controller, index, minDensity, maxDensity);
                if (density < level.max)
                {
                    level.add(candidate);
                    break;
                }
                else
                {
                    levels.add(level); // add the empty level
                }
            }
        }

        if (!level.sstables.isEmpty())
        {
            level.complete();
            levels.add(level);
        }

        return levels;
    }

    private List<SSTableReader> getCompactableSSTables(Collection<SSTableReader> sstables,
                                                       Predicate<SSTableReader> compactionFilter)
    {
        Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        List<SSTableReader> suitable = new ArrayList<>(sstables.size());
        for (SSTableReader rdr : sstables)
        {
            if (compactionFilter.test(rdr) && !compacting.contains(rdr))
                suitable.add(rdr);
        }
        return suitable;
    }

    public TableMetadata getMetadata()
    {
        return cfs.metadata();
    }

    private static boolean startsAfter(SSTableReader a, SSTableReader b)
    {
        // Strict comparison because the span is end-inclusive.
        return a.getFirst().compareTo(b.getLast()) > 0;
    }

    @Override
    public String toString()
    {
        return String.format("Unified strategy %s", getMetadata());
    }

    /**
     * A level: index, sstables and some properties.
     */
    public static class Level
    {
        final List<SSTableReader> sstables;
        final int index;
        final double survivalFactor;
        final int scalingParameter; // scaling parameter used to calculate fanout and threshold
        final int fanout; // fanout factor between levels
        final int threshold; // number of SSTables that trigger a compaction
        final double min; // min density of sstables for this level
        final double max; // max density of sstables for this level
        int maxOverlap = -1; // maximum number of overlapping sstables, i.e. maximum number of sstables that need
                             // to be queried on this level for any given key

        Level(Controller controller, int index, double minSize, double maxSize)
        {
            this.index = index;
            this.survivalFactor = controller.getSurvivalFactor(index);
            this.scalingParameter = controller.getScalingParameter(index);
            this.fanout = controller.getFanout(index);
            this.threshold = controller.getThreshold(index);
            this.sstables = new ArrayList<>(threshold);
            this.min = minSize;
            this.max = maxSize;
        }

        public Collection<SSTableReader> getSSTables()
        {
            return sstables;
        }

        public int getIndex()
        {
            return index;
        }

        void add(SSTableReader sstable)
        {
            this.sstables.add(sstable);
        }

        void complete()
        {
            if (logger.isTraceEnabled())
                logger.trace("Level: {}", this);
        }

        /**
         * Return the compaction pick for this level.
         * <p>
         * This is done by splitting the level into buckets that we can treat as independent regions for compaction.
         * We then use the maxOverlap value (i.e. the maximum number of sstables that can contain data for any covered
         * key) of each bucket to determine if compactions are needed, and to prioritize the buckets that contribute
         * most to the complexity of queries: if maxOverlap is below the level's threshold, no compaction is needed;
         * otherwise, we choose one from the buckets that have the highest maxOverlap.
         */
        CompactionPick getCompactionPick(SelectionContext context)
        {
            List<Bucket> buckets = getBuckets(context);
            if (buckets == null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Level {} sstables {} max overlap {} buckets with compactions {} tasks {}",
                                 index, sstables.size(), maxOverlap, 0, 0);
                return null;    // nothing crosses the threshold in this level, nothing to do
            }

            int estimatedRemainingTasks = 0;
            int overlapMatchingCount = 0;
            Bucket selectedBucket = null;
            Controller controller = context.controller;
            for (Bucket bucket : buckets)
            {
                // We can have just one pick in each level. Pick one bucket randomly out of the ones with
                // the highest overlap.
                // The random() part below implements reservoir sampling with size 1, giving us a uniformly random selection.
                if (bucket.maxOverlap == maxOverlap && controller.random().nextInt(++overlapMatchingCount) == 0)
                    selectedBucket = bucket;
                // The estimated remaining tasks is a measure of the remaining amount of work, thus we prefer to
                // calculate the number of tasks we would do in normal operation, even though we may compact in bigger
                // chunks when we are late.
                estimatedRemainingTasks += bucket.maxOverlap / threshold;
            }
            context.estimatedRemainingTasks += estimatedRemainingTasks;
            assert selectedBucket != null;

            if (logger.isDebugEnabled())
                logger.debug("Level {} sstables {} max overlap {} buckets with compactions {} tasks {}",
                             index, sstables.size(), maxOverlap, buckets.size(), estimatedRemainingTasks);

            CompactionPick selected = selectedBucket.constructPick(controller);

            if (logger.isTraceEnabled())
                logger.trace("Returning compaction pick with selected compaction {}",
                             selected);
            return selected;
        }

        /**
         * Group the sstables in this level into buckets.
         * <p>
         * The buckets are formed by grouping sstables that overlap at some key together, and then expanded to cover
         * any overlapping sstable according to the overlap inclusion method. With the usual TRANSITIVE method this
         * results into non-overlapping buckets that can't affect one another and can be compacted in parallel without
         * any loss of efficiency.
         * <p>
         * Other overlap inclusion methods are provided to cover situations where we may be okay with compacting
         * sstables partially and doing more than the strictly necessary amount of compaction to solve a problem: e.g.
         * after an upgrade from LCS where transitive overlap may cause a complete level to be compacted together
         * (creating an operation that will take a very long time to complete) and we want to make some progress as
         * quickly as possible at the cost of redoing some work.
         * <p>
         * The number of sstables that overlap at some key defines the "overlap" of a set of sstables. The maximum such
         * value in the bucket is its "maxOverlap", i.e. the highest number of sstables we need to read to find the
         * data associated with a given key.
         */
        @VisibleForTesting
        List<Bucket> getBuckets(SelectionContext context)
        {
            List<SSTableReader> liveSet = sstables;

            if (logger.isTraceEnabled())
                logger.trace("Creating compaction pick with live set {}", liveSet);

            List<Set<SSTableReader>> overlaps = Overlaps.constructOverlapSets(liveSet,
                                                                              UnifiedCompactionStrategy::startsAfter,
                                                                              SSTableReader.firstKeyComparator,
                                                                              SSTableReader.lastKeyComparator);
            for (Set<SSTableReader> overlap : overlaps)
                maxOverlap = Math.max(maxOverlap, overlap.size());
            if (maxOverlap < threshold)
                return null;

            List<Bucket> buckets = Overlaps.assignOverlapsIntoBuckets(threshold,
                                                                      context.controller.overlapInclusionMethod(),
                                                                      overlaps,
                                                                      this::makeBucket);
            return buckets;
        }

        private Bucket makeBucket(List<Set<SSTableReader>> overlaps, int startIndex, int endIndex)
        {
            return endIndex == startIndex + 1
                   ? new SimpleBucket(this, overlaps.get(startIndex))
                   : new MultiSetBucket(this, overlaps.subList(startIndex, endIndex));
        }

        @Override
        public String toString()
        {
            return String.format("W: %d, T: %d, F: %d, index: %d, min: %s, max %s, %d sstables, overlap %s",
                                 scalingParameter,
                                 threshold,
                                 fanout,
                                 index,
                                 densityAsString(min),
                                 densityAsString(max),
                                 sstables.size(),
                                 maxOverlap);
        }

        private String densityAsString(double density)
        {
            return FBUtilities.prettyPrintBinary(density, "B", " ");
        }
    }


    /**
     * A compaction bucket, i.e. a selection of overlapping sstables from which a compaction should be selected.
     */
    static abstract class Bucket
    {
        final Level level;
        final List<SSTableReader> allSSTablesSorted;
        final int maxOverlap;

        Bucket(Level level, Collection<SSTableReader> allSSTablesSorted, int maxOverlap)
        {
            // single section
            this.level = level;
            this.allSSTablesSorted = new ArrayList<>(allSSTablesSorted);
            this.allSSTablesSorted.sort(SSTableReader.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        Bucket(Level level, List<Set<SSTableReader>> overlapSections)
        {
            // multiple sections
            this.level = level;
            int maxOverlap = 0;
            Set<SSTableReader> all = new HashSet<>();
            for (Set<SSTableReader> section : overlapSections)
            {
                maxOverlap = Math.max(maxOverlap, section.size());
                all.addAll(section);
            }
            this.allSSTablesSorted = new ArrayList<>(all);
            this.allSSTablesSorted.sort(SSTableReader.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        /**
         * Select compactions from this bucket. Normally this would form a compaction out of all sstables in the
         * bucket, but if compaction is very late we may prefer to act more carefully:
         * - we should not use more inputs than the permitted maximum
         * - we should select SSTables in a way that preserves the structure of the compaction hierarchy
         * These impose a limit on the size of a compaction; to make sure we always reduce the read amplification by
         * this much, we treat this number as a limit on overlapping sstables, i.e. if A and B don't overlap with each
         * other but both overlap with C and D, all four will be selected to form a limit-three compaction. A limit-two
         * one may choose CD, ABC or ABD.
         * Also, the subset is selected by max timestamp order, oldest first, to avoid violating sstable time order. In
         * the example above, if B is oldest and C is older than D, the limit-two choice would be ABC (if A is older
         * than D) or BC (if A is younger, avoiding combining C with A skipping D).
         *
         * @param controller The compaction controller.
         * @return A compaction pick to execute next.
         */
        CompactionPick constructPick(Controller controller)
        {
            int count = maxOverlap;
            int threshold = level.threshold;
            int fanout = level.fanout;
            int index = level.index;
            int maxSSTablesToCompact = Math.max(fanout, controller.maxSSTablesToCompact());

            assert count >= threshold;
            if (count <= fanout)
            {
                /**
                 * Happy path. We are not late or (for levelled) we are only so late that a compaction now will
                 * have the same effect as doing levelled compactions one by one. Compact all. We do not cap
                 * this pick at maxSSTablesToCompact due to an assumption that maxSSTablesToCompact is much
                 * greater than F. See {@link Controller#MAX_SSTABLES_TO_COMPACT_OPTION} for more details.
                 */
                return new CompactionPick(index, count, allSSTablesSorted);
            }
            else if (count <= fanout * controller.getFanout(index + 1) || maxSSTablesToCompact == fanout)
            {
                // Compaction is a bit late, but not enough to jump levels via layout compactions. We need a special
                // case to cap compaction pick at maxSSTablesToCompact.
                if (count <= maxSSTablesToCompact)
                    return new CompactionPick(index, count, allSSTablesSorted);

                return new CompactionPick(index, maxSSTablesToCompact, pullOldestSSTables(maxSSTablesToCompact));
            }
            else
            {
                // We may, however, have accumulated a lot more than T if compaction is very late.
                // In this case we pick a compaction in such a way that the result of doing it spreads the data in
                // a similar way to how compaction would lay them if it was able to keep up. This means:
                // - for tiered compaction (w >= 0), compact in sets of as many as required to get to a level.
                //   for example, for w=2 and 55 sstables, pick a compaction of 16 sstables (on the next calls, given no
                //   new files, 2 more of 16, 1 of 4, and leaving the other 3 sstables alone).
                // - for levelled compaction (w < 0), compact all that would reach a level.
                //   for w=-2 and 55, this means pick a compaction of 48 (on the next calls, given no new files, one of
                //   4, and one of 3 sstables).
                int pickSize = selectPickSize(controller, maxSSTablesToCompact);
                return new CompactionPick(index, pickSize, pullOldestSSTables(pickSize));
            }
        }

        private int selectPickSize(Controller controller, int maxSSTablesToCompact)
        {
            int pickSize;
            int fanout = level.fanout;
            int nextStep = fanout;
            int index = level.index;
            int limit = Math.min(maxSSTablesToCompact, maxOverlap);
            do
            {
                pickSize = nextStep;
                fanout = controller.getFanout(++index);
                nextStep *= fanout;
            }
            while (nextStep <= limit);

            if (level.scalingParameter < 0)
            {
                // For levelled compaction all the sstables that would reach this level need to be compacted to one,
                // so select the highest multiple of step that fits.
                pickSize *= limit / pickSize;
                assert pickSize > 0;
            }
            return pickSize;
        }

        /**
         * Pull the oldest sstables to get at most limit-many overlapping sstables to compact in each overlap section.
         */
        abstract Collection<SSTableReader> pullOldestSSTables(int overlapLimit);
    }

    public static class SimpleBucket extends Bucket
    {
        public SimpleBucket(Level level, Collection<SSTableReader> sstables)
        {
            super(level, sstables, sstables.size());
        }

        Collection<SSTableReader> pullOldestSSTables(int overlapLimit)
        {
            if (allSSTablesSorted.size() <= overlapLimit)
                return allSSTablesSorted;
            return Overlaps.pullLast(allSSTablesSorted, overlapLimit);
        }
    }

    public static class MultiSetBucket extends Bucket
    {
        final List<Set<SSTableReader>> overlapSets;

        public MultiSetBucket(Level level, List<Set<SSTableReader>> overlapSets)
        {
            super(level, overlapSets);
            this.overlapSets = overlapSets;
        }

        Collection<SSTableReader> pullOldestSSTables(int overlapLimit)
        {
            return Overlaps.pullLastWithOverlapLimit(allSSTablesSorted, overlapSets, overlapLimit);
        }
    }

    /**
     * Utility class holding a collection of sstables for compaction.
     */
    static class CompactionPick extends ArrayList<SSTableReader>
    {
        final int level;
        final int overlap;

        CompactionPick(int level, int overlap, Collection<SSTableReader> sstables)
        {
            super(sstables);
            this.level = level;
            this.overlap = overlap;
        }
    }

    static class SelectionContext
    {
        final Controller controller;
        int estimatedRemainingTasks = 0;

        SelectionContext(Controller controller)
        {
            this.controller = controller;
        }
    }
}
