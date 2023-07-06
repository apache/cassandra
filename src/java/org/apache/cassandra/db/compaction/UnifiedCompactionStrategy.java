/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.DiskBoundaries;
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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Overlaps;

import static org.apache.cassandra.utils.Throwables.perform;

/**
 * The unified compaction strategy is described in this design document:
 *
 * See CEP-26: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-26%3A+Unified+Compaction+Strategy
 */
public class UnifiedCompactionStrategy extends AbstractCompactionStrategy
{
    public static final Class<? extends CompactionStrategyContainer> CONTAINER_CLASS = UnifiedCompactionContainer.class;

    private static final Logger logger = LoggerFactory.getLogger(UnifiedCompactionStrategy.class);

    public static final int MAX_LEVELS = 32;   // This is enough for a few petabytes of data (with the worst case fan factor
    // at W=0 this leaves room for 2^32 sstables, presumably of at least 1MB each).

    private static final Pattern SCALING_PARAMETER_PATTERN = Pattern.compile("(N)|L(\\d+)|T(\\d+)|([+-]?\\d+)");
    private static final String SCALING_PARAMETER_PATTERN_SIMPLIFIED = SCALING_PARAMETER_PATTERN.pattern()
                                                                                                .replaceAll("[()]", "")

                                                                                                .replace("\\d", "[0-9]");

    private final Controller controller;

    private volatile ArenaSelector arenaSelector;
    private volatile ShardManager shardManager;

    private long lastExpiredCheck;

    static final Level EXPIRED_TABLES_LEVEL = new Level(-1, 0, 0, 0, 0, 0, 0)
    {
        @Override
        public String toString()
        {
            return "expired";
        }
    };

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options)
    {
        this(factory, backgroundCompactions, options, Controller.fromOptions(factory.getRealm(), options));
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Controller controller)
    {
        this(factory, backgroundCompactions, new HashMap<>(), controller);
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options, Controller controller)
    {
        super(factory, backgroundCompactions, options);
        this.controller = controller;
    }

    @VisibleForTesting
    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, Controller controller)
    {
        this(factory, new BackgroundCompactions(factory.getRealm()), new HashMap<>(), controller);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return Controller.validateOptions(CompactionStrategyOptions.validateOptions(options));
    }

    public void storeControllerConfig()
    {
        getController().storeControllerConfig();
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
    public Collection<Collection<CompactionSSTable>> groupSSTablesForAntiCompaction(Collection<? extends CompactionSSTable> sstablesToGroup)
    {
        Collection<Collection<CompactionSSTable>> groups = new ArrayList<>();
        for (Arena arena : getCompactionArenas(sstablesToGroup, false))
        {
            groups.addAll(super.groupSSTablesForAntiCompaction(arena.sstables));
        }

        return groups;
    }

    @Override
    public synchronized CompactionTasks getUserDefinedTasks(Collection<? extends CompactionSSTable> sstables, int gcBefore)
    {
        // The tasks need to be split by repair status and disk, but otherwise we must assume the user knows what they
        // are doing.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Arena arena : getCompactionArenas(sstables, true))
            tasks.addAll(super.getUserDefinedTasks(arena.sstables, gcBefore));
        return CompactionTasks.create(tasks);
    }

    @Override
    public synchronized CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        maybeUpdateSelector();
        // The tasks are split by repair status and disk, as well as in non-overlapping sections to enable some
        // parallelism (to the amount that L0 sstables are split, i.e. at least base_shard_count). The result will be
        // split across shards according to its density. Depending on the parallelism, the operation may require up to
        // 100% extra space to complete.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Arena arena : getCompactionArenas(realm.getLiveSSTables(), true))
        {
            List<Set<CompactionSSTable>> nonOverlapping = splitInNonOverlappingSets(arena.sstables);
            for (Set<CompactionSSTable> set : nonOverlapping)
            {
                LifecycleTransaction txn = realm.tryModify(set, OperationType.COMPACTION);
                if (txn != null)
                    tasks.add(createCompactionTask(gcBefore, txn, true, splitOutput));
            }
        }
        return CompactionTasks.create(tasks);
    }

    /**
     * Utility method to split a list of sstables into non-overlapping sets. Used by CNDB.
     *
     *
     * @param sstables A list of items to distribute in overlap sets. This is assumed to be a transient list and the
     *                 method may modify or consume it. It is assumed that the start and end positions of an item are
     *                 ordered, and the items are non-empty.
     * @return list of non-overlapping sets of sstables
     */
    public static List<Set<CompactionSSTable>> splitInNonOverlappingSets(List<CompactionSSTable> sstables)
    {
        List<Set<CompactionSSTable>> overlapSets = Overlaps.constructOverlapSets(sstables,
                                                                                 UnifiedCompactionStrategy::startsAfter,
                                                                                 CompactionSSTable.firstKeyComparator,
                                                                                 CompactionSSTable.lastKeyComparator);
        Set<CompactionSSTable> group = overlapSets.get(0);
        List<Set<CompactionSSTable>> groups = new ArrayList<>();
        for (int i = 1; i < overlapSets.size(); ++i)
        {
            Set<CompactionSSTable> current = overlapSets.get(i);
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
    public void startup()
    {
        perform(super::startup,
                () -> controller.startup(this, ScheduledExecutors.scheduledTasks));
    }

    @Override
    public void shutdown()
    {
        perform(super::shutdown,
                controller::shutdown);
    }

    /**
     * Returns a collections of compaction tasks.
     *
     * This method is synchornized because task creation is significantly more expensive in UCS; the strategy is
     * stateless, therefore it has to compute the shard/bucket structure on each call.
     *
     * @param gcBefore throw away tombstones older than this
     * @return collection of AbstractCompactionTask, which could be either a CompactionTask or an UnifiedCompactionTask
     */
    @Override
    public synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        // TODO - we should perhaps consider executing this code less frequently than legacy strategies
        // since it's more expensive, and we should therefore prevent a second concurrent thread from executing at all

        return getNextBackgroundTasks(getNextCompactionAggregates(gcBefore), gcBefore);
    }

    /**
     * Used by CNDB where compaction aggregates come from etcd rather than the strategy
     * @return collection of AbstractCompactionTask, which could be either a CompactionTask or an UnifiedCompactionTask
     */
    public synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(Collection<CompactionAggregate> aggregates, int gcBefore)
    {
        controller.onStrategyBackgroundTaskRequest();
        return createCompactionTasks(aggregates, gcBefore);
    }

    private synchronized Collection<AbstractCompactionTask> createCompactionTasks(Collection<CompactionAggregate> aggregates, int gcBefore)
    {
        Collection<AbstractCompactionTask> tasks = new ArrayList<>(aggregates.size());
        for (CompactionAggregate aggregate : aggregates)
        {
            CompactionPick selected = aggregate.getSelected();
            Preconditions.checkNotNull(selected);
            Preconditions.checkArgument(!selected.isEmpty());

            LifecycleTransaction transaction = realm.tryModify(selected.sstables(),
                                                               OperationType.COMPACTION,
                                                               selected.id());
            if (transaction != null)
            {
                backgroundCompactions.setSubmitted(this, transaction.opId(), aggregate);
                tasks.add(createCompactionTask(transaction, gcBefore));
            }
            else
            {
                // This can happen e.g. due to a race with upgrade tasks
                logger.error("Failed to submit compaction {} because a transaction could not be created. If this happens frequently, it should be reported", aggregate);
            }
        }

        return tasks;
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
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       IntervalSet<CommitLogPosition> commitLogPositions,
                                                       int sstableLevel,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        ShardManager currentShardManager = getShardManager();
        double flushDensity = realm.metrics().flushSizeOnDisk().get() * shardManager.shardSetCoverage() / currentShardManager.localSpaceCoverage();
        ShardTracker boundaries = currentShardManager.boundaries(controller.getNumShards(flushDensity));
        return new ShardedMultiWriter(realm,
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
    private UnifiedCompactionTask createCompactionTask(LifecycleTransaction transaction, int gcBefore)
    {
        return new UnifiedCompactionTask(realm, this, transaction, gcBefore, getShardManager());
    }

    @Override
    protected UnifiedCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, boolean isMaximal, boolean splitOutput)
    {
        return createCompactionTask(txn, gcBefore);
    }

    @Override
    public UnifiedCompactionTask createCompactionTask(LifecycleTransaction txn, final int gcBefore, long maxSSTableBytes)
    {
        return createCompactionTask(txn, gcBefore);
    }

    private void maybeUpdateSelector()
    {
        if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
            return; // the disk boundaries (and thus the local ranges too) have not changed since the last time we calculated

        synchronized (this)
        {
            if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
                return; // another thread beat us to the update

            DiskBoundaries currentBoundaries = realm.getDiskBoundaries();
            shardManager = ShardManager.create(currentBoundaries);
            arenaSelector = new ArenaSelector(controller, currentBoundaries);
            // Note: this can just as well be done without the synchronization (races would be benign, just doing some
            // redundant work). For the current usages of this blocking is fine and expected to perform no worse.
        }
    }

    @VisibleForTesting
    ShardManager getShardManager()
    {
        maybeUpdateSelector();
        return shardManager;
    }

    private CompactionLimits getCurrentLimits(int maxConcurrentCompactions)
    {
        // Calculate the running compaction limits, i.e. the overall number of compactions permitted, which is either
        // the compaction thread count, or the compaction throughput divided by the compaction rate (to prevent slowing
        // down individual compaction progress).
        String rateLimitLog = "";

        // identify space limit
        long spaceOverheadLimit = controller.maxCompactionSpaceBytes();

        // identify throughput limit
        double throughputLimit = controller.maxThroughput();
        int maxCompactions;
        if (throughputLimit < Double.MAX_VALUE)
        {
            int maxCompactionsForThroughput;

            double compactionRate = backgroundCompactions.compactionRate.get();
            if (compactionRate > 0)
            {
                // Start as many as can saturate the limit, making sure to also account for compactions that have
                // already been started but don't have progress yet.

                // Note: the throughput limit is adjusted here because the limiter won't let compaction proceed at more
                // than the given rate, and small hiccups or rounding errors could cause this to go above the current
                // running count when we are already at capacity.
                // Allow up to 5% variability, or if we are permitted more than 20 concurrent compactions, one/maxcount
                // so that we don't issue less tasks than we should.
                double adjustment = Math.min(0.05, 1.0 / maxConcurrentCompactions);
                maxCompactionsForThroughput = (int) Math.ceil(throughputLimit * (1 - adjustment) / compactionRate);
            }
            else
            {
                // If we don't have running compactions we don't know the effective rate.
                // Allow only one compaction; this will be called again soon enough to recheck.
                maxCompactionsForThroughput = 1;
            }

            rateLimitLog = String.format(" rate-based limit %d (rate %s/%s)",
                                         maxCompactionsForThroughput,
                                         FBUtilities.prettyPrintMemoryPerSecond((long) compactionRate),
                                         FBUtilities.prettyPrintMemoryPerSecond((long) throughputLimit));
            maxCompactions = Math.min(maxConcurrentCompactions, maxCompactionsForThroughput);
        }
        else
            maxCompactions = maxConcurrentCompactions;

        // Now that we have a count, make sure it is spread close to equally among levels. In other words, reserve
        // floor(permitted / levels) compactions for each level and don't permit more than ceil(permitted / levels) on
        // any, to make sure that no level hogs all threads and thus lowest-level ops (which need to run more often but
        // complete quickest) have a chance to run frequently. Also, running compactions can't go above the specified
        // space overhead limit.
        // To do this we count the number and size of already running compactions on each level and make sure any new
        // ones we select satisfy these constraints.
        int[] perLevel = new int[MAX_LEVELS];
        int levelCount = 1; // Start at 1 to avoid division by zero if the aggregates list is empty.
        int runningCompactions = 0;
        long spaceAvailable = spaceOverheadLimit;
        int remainingAdaptiveCompactions = controller.getMaxRecentAdaptiveCompactions(); //limit for number of compactions triggered by new W value
        if (remainingAdaptiveCompactions == -1)
            remainingAdaptiveCompactions = Integer.MAX_VALUE;
        for (CompactionPick compaction : backgroundCompactions.getCompactionsInProgress())
        {
            final int level = levelOf(compaction);
            if (level < 0)  // expire-only compactions are allowed to run outside of the limits
                continue;
            ++perLevel[level];
            ++runningCompactions;
            levelCount = Math.max(levelCount, level + 1);
            spaceAvailable -= controller.getOverheadSizeInBytes(compaction);
            if (controller.isRecentAdaptive(compaction))
                --remainingAdaptiveCompactions;
        }

        CompactionLimits limits = new CompactionLimits(runningCompactions,
                                                       maxCompactions,
                                                       maxConcurrentCompactions,
                                                       perLevel,
                                                       levelCount,
                                                       spaceAvailable,
                                                       rateLimitLog,
                                                       remainingAdaptiveCompactions);
        logger.debug("Selecting up to {} new compactions of up to {}, concurrency limit {}{}",
                     Math.max(0, limits.maxCompactions - limits.runningCompactions),
                     FBUtilities.prettyPrintMemory(limits.spaceAvailable),
                     limits.maxConcurrentCompactions,
                     limits.rateLimitLog);
        return limits;
    }

    private Collection<CompactionAggregate> updateLevelCountWithParentAndGetSelection(final CompactionLimits limits,
                                                                                      List<CompactionAggregate.UnifiedAggregate> pending)
    {
        long totalCompactionLimit = controller.maxCompactionSpaceBytes();
        for (CompactionAggregate.UnifiedAggregate aggregate : pending)
        {
            warnIfSizeAbove(aggregate, totalCompactionLimit);

            CompactionPick selected = aggregate.getSelected();
            if (selected != null)
                limits.levelCount = Math.max(limits.levelCount, levelOf(selected));
        }

        final List<CompactionAggregate> selection = getSelection(pending,
                                                                 controller,
                                                                 limits.maxCompactions,
                                                                 limits.levelCount,
                                                                 limits.perLevel,
                                                                 limits.spaceAvailable,
                                                                 limits.remainingAdaptiveCompactions);
        return selection;
    }

    /**
     * Selects compactions to run next.
     *
     * @param gcBefore
     * @return a subset of compaction aggregates to run next
     */
    private Collection<CompactionAggregate> getNextCompactionAggregates(int gcBefore)
    {
        final CompactionLimits limits = getCurrentLimits(controller.maxConcurrentCompactions());

        List<CompactionAggregate.UnifiedAggregate> pending = getPendingCompactionAggregates(limits.spaceAvailable, gcBefore);
        setPendingCompactionAggregates(pending);

        for (CompactionAggregate.UnifiedAggregate aggregate : pending)
        {
            // Make sure the level count includes all levels for which we have sstables (to be ready to compact
            // as soon as the threshold is crossed)...
            limits.levelCount = Math.max(limits.levelCount, aggregate.bucketIndex() + 1);
            CompactionPick selected = aggregate.getSelected();
            if (selected != null)
            {
                // ... and also the levels that a layout-preserving selection would create.
                limits.levelCount = Math.max(limits.levelCount, levelOf(selected) + 1);
            }
        }

        return updateLevelCountWithParentAndGetSelection(limits, pending);
    }

    /**
     * Selects compactions to run next from the passed aggregates.
     *
     * The intention here is to use this method directly from outside processes, to run compactions from a set
     * of pre-existing aggregates, that have been generated out of process.
     *
     * @param aggregates a collection of aggregates from which to select the next compactions
     * @param maxConcurrentCompactions the maximum number of concurrent compactions
     * @return a subset of compaction aggregates to run next
     */
    public Collection<CompactionAggregate> getNextCompactionAggregates(Collection<CompactionAggregate.UnifiedAggregate> aggregates,
                                                                       int maxConcurrentCompactions)
    {
        final CompactionLimits limits = getCurrentLimits(maxConcurrentCompactions);
        maybeUpdateSelector();
        return updateLevelCountWithParentAndGetSelection(limits, new ArrayList<>(aggregates));
    }

    /**
     * Returns all pending compaction aggregates.
     *
     * This method is used by CNDB to find all pending compactions and put them to etcd.
     *
     * @param gcBefore
     * @return all pending compaction aggregates
     **/
    public Collection<CompactionAggregate.UnifiedAggregate> getPendingCompactionAggregates(int gcBefore)
    {
        return getPendingCompactionAggregates(controller.maxCompactionSpaceBytes(), gcBefore);
    }

    /**
     * Set the compaction aggregates passed in as pending in {@link BackgroundCompactions}. This ensures
     * that the compaction statistics will be accurate.
     * <p/>
     * This is called by {@link UnifiedCompactionStrategy#getNextCompactionAggregates(int)}
     * and externally after calling {@link UnifiedCompactionStrategy#getPendingCompactionAggregates(int)}
     * or before submitting tasks.
     *
     * Also, note that skipping the call to {@link BackgroundCompactions#setPending(CompactionStrategy, Collection)}
     * would result in memory leaks: the aggregates added in {@link BackgroundCompactions#setSubmitted(CompactionStrategy, UUID, CompactionAggregate)}
     * would never be removed, and the aggregates hold references to the compaction tasks, so they retain a significant
     * size of heap memory.
     *
     * @param pending the aggregates that should be set as pending compactions
     */
    public void setPendingCompactionAggregates(Collection<? extends CompactionAggregate> pending)
    {
        backgroundCompactions.setPending(this, pending);
    }

    private List<CompactionAggregate.UnifiedAggregate> getPendingCompactionAggregates(long spaceAvailable, int gcBefore)
    {
        maybeUpdateSelector();

        List<CompactionAggregate.UnifiedAggregate> pending = new ArrayList<>();
        long ts = System.currentTimeMillis();
        boolean expiredCheck = ts - lastExpiredCheck > controller.getExpiredSSTableCheckFrequency();
        if (expiredCheck)
            lastExpiredCheck = ts;

        Set<CompactionSSTable> expired = Collections.emptySet();
        for (Map.Entry<Arena, List<Level>> entry : getLevels().entrySet())
        {
            Arena arena = entry.getKey();
            if (expiredCheck)
            {
                expired = arena.getExpiredSSTables(gcBefore, controller.getIgnoreOverlapsInExpirationCheck());
                if (!expired.isEmpty())
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Expiration check for arena {} found {} fully expired SSTables", arena.name(), expired.size());
                    pending.add(CompactionAggregate.createUnified(expired, 0, CompactionPick.create(-1, expired, expired), Collections.emptySet(), arena, EXPIRED_TABLES_LEVEL));
                }
            }

            for (Level level : entry.getValue())
            {
                Collection<CompactionAggregate.UnifiedAggregate> aggregates = level.getCompactionAggregates(arena, expired, controller, spaceAvailable);
                // Note: We allow empty aggregates into the list of pending compactions. The pending compactions list
                // is for progress tracking only, and it is helpful to see empty levels there.
                pending.addAll(aggregates);
            }
        }

        return pending;
    }

    /**
     * This method logs a warning related to the fact that the space overhead limit also applies when a
     * single compaction is above that limit. This should prevent running out of space at the expense of ending up
     * with several extra sstables at the highest-level (compared to the number of sstables that we should have
     * as per config of the strategy), i.e. slightly higher read amplification. This is a sensible tradeoff but
     * the operators must be warned if this happens, and that's the purpose of this warning.
     */
    private void warnIfSizeAbove(CompactionAggregate.UnifiedAggregate aggregate, long spaceOverheadLimit)
    {
        if (controller.getOverheadSizeInBytes(aggregate.selected) > spaceOverheadLimit)
            logger.warn("Compaction needs to perform an operation that is bigger than the current space overhead " +
                        "limit - size {} (compacting {} sstables in arena {}/bucket {}); limit {} = {}% of dataset size {}. " +
                        "To honor the limit, this operation will not be performed, which may result in degraded performance.\n" +
                        "Please verify the compaction parameters, specifically {} and {}.",
                        FBUtilities.prettyPrintMemory(controller.getOverheadSizeInBytes(aggregate.selected)),
                        aggregate.selected.sstables().size(),
                        aggregate.getArena().name(),
                        aggregate.bucketIndex(),
                        FBUtilities.prettyPrintMemory(spaceOverheadLimit),
                        controller.getMaxSpaceOverhead() * 100,
                        FBUtilities.prettyPrintMemory(controller.getDataSetSizeBytes()),
                        Controller.DATASET_SIZE_OPTION_GB,
                        Controller.MAX_SPACE_OVERHEAD_OPTION);
    }

    /**
     * Returns a selection of the compactions to be submitted. The selection will be chosen so that the total
     * number of compactions is at most totalCount, where each level gets a share that is the whole part of the ratio
     * between the total permitted number of compactions, and the remainder gets distributed among the levels
     * according to the preferences of the {@link Controller#prioritize} method. Usually this means preferring
     * compaction picks with a higher max overlap, with a random selection when multiple picks have the same maximum.
     * Note that if a level does not have tasks to fill its share, its quota will remain unused in this
     * allocation.
     *
     * The selection also limits the size of the newly scheduled compactions to be below spaceAvailable by not
     * scheduling compactions if they would push the combined size above that limit.
     *
     * @param pending list of all current aggregates with possible selection for each bucket
     * @param totalCount maximum number of compactions permitted to run
     * @param levelCount number of levels in use
     * @param perLevel int array with the number of in-progress compactions per level
     * @param spaceAvailable amount of space in bytes available for the new compactions
     * @param remainingAdaptiveCompactions number of adaptive compactions (i.e. ones triggered by scaling parameter
     *                                     change by the adaptive controller) that can still be scheduled
     *
     */
    static List<CompactionAggregate> getSelection(List<CompactionAggregate.UnifiedAggregate> pending,
                                                  Controller controller,
                                                  int totalCount,
                                                  int levelCount,
                                                  int[] perLevel,
                                                  long spaceAvailable,
                                                  int remainingAdaptiveCompactions)
    {
        // Prepare parameters for the selection.
        int reservedThreadsTarget = controller.getReservedThreadsPerLevel();
        // Each level has this number of tasks reserved for it.
        int perLevelCount = Math.min(totalCount / levelCount, reservedThreadsTarget);
        // The remainder is distributed according to the prioritization.
        int remainder = totalCount - perLevelCount * levelCount;
        // If the user requested more than we can give, do not allow more than one extra per level.
        boolean oneRemainderPerLevel = perLevelCount < reservedThreadsTarget;
        // If the inclusion method is not transitive, we may have multiple buckets/selections for the same sstable.
        boolean shouldCheckSSTableSelected = controller.overlapInclusionMethod() != Overlaps.InclusionMethod.TRANSITIVE;
        // If so, make sure we only select one such compaction.
        Set<CompactionSSTable> selectedSSTables = shouldCheckSSTableSelected ? new HashSet<>() : null;

        // Calculate how many new ones we can add in each level, and how many we can assign randomly.
        int remaining = totalCount;
        for (int i = 0; i < levelCount; ++i)
        {
            remaining -= perLevel[i];
            if (perLevel[i] > perLevelCount)
                remainder -= perLevel[i] - perLevelCount;
        }
        // Note: if we are in the middle of changes in the parameters or level count, remainder might become negative.
        // This is okay, some buckets will temporarily not get their rightful share until these tasks complete.

        // Let the controller prioritize the compactions.
        pending = controller.prioritize(pending);
        int proposed = 0;

        // Select the first ones, permitting only the specified number per level.
        List<CompactionAggregate> selected = new ArrayList<>(pending.size());
        for (CompactionAggregate.UnifiedAggregate aggregate : pending)
        {
            final CompactionPick pick = aggregate.getSelected();
            if (pick.isEmpty())
                continue;
            if (pick.hasExpiredOnly())
            {
                selected.add(aggregate);    // always add expired-only compactions, they are not subject to any limits
                continue;
            }
            ++proposed;
            long overheadSizeInBytes = controller.getOverheadSizeInBytes(pick);
            if (overheadSizeInBytes > spaceAvailable)
                continue; // compaction is too large for current cycle

            int currentLevel = levelOf(pick);
            assert currentLevel >= 0 : "Invalid level in " + pick + ": level -1 is only allowed for expired-only compactions";

            boolean isAdaptive = controller.isRecentAdaptive(pick);
            if (isAdaptive && remainingAdaptiveCompactions <= 0)
                continue; // do not allow more than remainingAdaptiveCompactions to limit latency spikes upon changing W

            if (shouldCheckSSTableSelected && !Collections.disjoint(selectedSSTables, pick.sstables()))
                continue; // do not allow multiple selections on the same sstable

            if (perLevel[currentLevel] >= perLevelCount)
            {
                if (remainder <= 0)
                    continue;  // share used up and no remainder to distribute
                if (oneRemainderPerLevel && perLevel[currentLevel] > perLevelCount)
                    continue;  // this level is already using up all its share + one, we can ignore candidate altogether
                --remainder;
            }
            // Note: if any additional checks are added, make sure remainder is not decreased if they fail.

            if (isAdaptive)
                remainingAdaptiveCompactions--;
            --remaining;
            ++perLevel[currentLevel];
            spaceAvailable -= overheadSizeInBytes;
            selected.add(aggregate);
            if (shouldCheckSSTableSelected)
                selectedSSTables.addAll(pick.sstables());

            if (remaining == 0)
                break;
        }

        logger.debug("Selected {} compactions (out of {} pending). Compactions per level {} (reservations {}{}) remaining reserved {} non-reserved {}.",
                     selected.size(), proposed, perLevel, perLevelCount, oneRemainderPerLevel ? "+1" : "", remaining - remainder, remainder);

        return selected;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return backgroundCompactions.getEstimatedRemainingTasks();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Set<? extends CompactionSSTable> getSSTables()
    {
        return realm.getLiveSSTables();
    }

    @VisibleForTesting
    public int getW(int index)
    {
        return controller.getScalingParameter(index);
    }

    @VisibleForTesting
    public Controller getController()
    {
        return controller;
    }

    /**
     * Group candidate sstables into compaction arenas.
     * Each compaction arena is obtained by comparing using a compound comparator for the equivalence classes
     * configured in the arena selector of this strategy.
     *
     * @param sstables a collection of the sstables to be assigned to arenas
     * @param compactionFilter a filter to exclude CompactionSSTables,
     *                         e.g., {@link CompactionSSTable#isSuitableForCompaction()}
     * @return a list of arenas, where each arena contains sstables that belong to that arena
     */
    public Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables,
                                                 Predicate<CompactionSSTable> compactionFilter)
    {
        return getCompactionArenas(sstables, compactionFilter, this.arenaSelector,true);
    }

    Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables, boolean filterUnsuitable)
    {
        return getCompactionArenas(sstables,
                                   CompactionSSTable::isSuitableForCompaction,
                                   this.arenaSelector,
                                   filterUnsuitable);
    }

    Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables,
                                          Predicate<CompactionSSTable> compactionFilter,
                                          ArenaSelector arenaSelector,
                                          boolean filterUnsuitable)
    {
        Map<CompactionSSTable, Arena> arenasBySSTables = new TreeMap<>(arenaSelector);
        Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();
        for (CompactionSSTable sstable : sstables)
            if (!filterUnsuitable || compactionFilter.test(sstable) && !compacting.contains(sstable))
                arenasBySSTables.computeIfAbsent(sstable, t -> new Arena(arenaSelector, realm))
                      .add(sstable);

        return arenasBySSTables.values();
    }

    // used by CNDB to deserialize aggregates
    public Arena getCompactionArena(Collection<? extends CompactionSSTable> sstables)
    {
        maybeUpdateSelector();
        Arena arena = new Arena(arenaSelector, realm);
        for (CompactionSSTable table : sstables)
            arena.add(table);
        return arena;
    }

    // used by CNDB to deserialize aggregates
    public Level getLevel(int index, double min, double max)
    {
        if (index == EXPIRED_TABLES_LEVEL.index)
            return EXPIRED_TABLES_LEVEL;
        return new Level(controller, index, min, max);
    }

    /**
     * @return a LinkedHashMap of arenas with buckets where order of arenas are preserved
     */
    @VisibleForTesting
    Map<Arena, List<Level>> getLevels()
    {
        return getLevels(realm.getLiveSSTables(), CompactionSSTable::isSuitableForCompaction);
    }

    /**
     * Groups the sstables passed in into arenas and buckets. This is used by the strategy to determine
     * new compactions, and by external tools in CNDB to analyze the strategy decisions.
     *
     * @param sstables a collection of the sstables to be assigned to arenas
     * @param compactionFilter a filter to exclude CompactionSSTables,
     *                         e.g., {@link CompactionSSTable#isSuitableForCompaction()}
     *
     * @return a map of arenas to their buckets
     */
    public Map<Arena, List<Level>> getLevels(Collection<? extends CompactionSSTable> sstables,
                                             Predicate<CompactionSSTable> compactionFilter)
    {
        maybeUpdateSelector();
        Collection<Arena> arenas = getCompactionArenas(sstables, compactionFilter);
        Map<Arena, List<Level>> ret = new LinkedHashMap<>(); // should preserve the order of arenas

        for (Arena arena : arenas)
        {
            List<Level> levels = new ArrayList<>(MAX_LEVELS);
            arena.sstables.sort(shardManager::compareByDensity);

            double maxSize = controller.getMaxLevelDensity(0, controller.getBaseSstableSize(controller.getFanout(0)) / shardManager.localSpaceCoverage());
            int index = 0;
            Level level = new Level(controller, index, 0, maxSize);
            for (CompactionSSTable candidate : arena.sstables)
            {
                final double size = shardManager.density(candidate);
                if (size < level.max)
                {
                    level.add(candidate);
                    continue;
                }

                level.complete();
                levels.add(level); // add even if empty

                while (true)
                {
                    ++index;
                    double minSize = maxSize;
                    maxSize = controller.getMaxLevelDensity(index, minSize);
                    level = new Level(controller, index, minSize, maxSize);
                    if (size < level.max)
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

            if (!levels.isEmpty())
                ret.put(arena, levels);

            if (logger.isTraceEnabled())
                logger.trace("Arena {} has {} levels", arena, levels.size());
        }

        logger.debug("Found {} arenas with buckets for {}.{}", ret.size(), realm.getKeyspaceName(), realm.getTableName());
        return ret;
    }

    private static int levelOf(CompactionPick pick)
    {
        return (int) pick.parent();
    }

    public TableMetadata getMetadata()
    {
        return realm.metadata();
    }

    private static boolean startsAfter(CompactionSSTable a, CompactionSSTable b)
    {
        // Strict comparison because the span is end-inclusive.
        return a.getFirst().compareTo(b.getLast()) > 0;
    }

    /**
     * A compaction arena contains the list of sstables that belong to this arena as well as the arena
     * selector used for comparison.
     */
    public static class Arena implements Comparable<Arena>
    {
        final List<CompactionSSTable> sstables;
        final ArenaSelector selector;
        private final CompactionRealm realm;

        Arena(ArenaSelector selector, CompactionRealm realm)
        {
            this.realm = realm;
            this.sstables = new ArrayList<>();
            this.selector = selector;
        }

        void add(CompactionSSTable ssTableReader)
        {
            sstables.add(ssTableReader);
        }

        public String name()
        {
            CompactionSSTable t = sstables.get(0);
            return selector.name(t);
        }

        @Override
        public int compareTo(Arena o)
        {
            return selector.compare(this.sstables.get(0), o.sstables.get(0));
        }

        @Override
        public String toString()
        {
            return String.format("%s, %d sstables", name(), sstables.size());
        }

        @VisibleForTesting
        public List<CompactionSSTable> getSSTables()
        {
            return sstables;
        }

        /**
         * Find fully expired SSTables. Those will be included in the aggregate no matter what.
         * @param gcBefore
         * @param ignoreOverlaps
         * @return expired SSTables
         */
        Set<CompactionSSTable> getExpiredSSTables(int gcBefore, boolean ignoreOverlaps)
        {
            return CompactionController.getFullyExpiredSSTables(realm,
                                                                sstables,
                                                                realm.getOverlappingLiveSSTables(sstables),
                                                                gcBefore,
                                                                ignoreOverlaps);
        }
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
        final List<CompactionSSTable> sstables;
        final int index;
        final double survivalFactor;
        final int scalingParameter; // scaling parameter used to calculate fanout and threshold
        final int fanout; // fanout factor between levels
        final int threshold; // number of SSTables that trigger a compaction
        final double min; // min density of sstables for this level
        final double max; // max density of sstables for this level
        double avg = 0; // avg size of sstables in this level
        int maxOverlap = -1; // maximum number of overlapping sstables

        Level(int index, int scalingParameter, int fanout, int threshold, double survivalFactor, double min, double max)
        {
            this.index = index;
            this.scalingParameter = scalingParameter;
            this.fanout = fanout;
            this.threshold = threshold;
            this.survivalFactor = survivalFactor;
            this.min = min;
            this.max = max;
            this.sstables = new ArrayList<>(threshold);
        }

        Level(Controller controller, int index, double min, double max)
        {
            this(index,
                 controller.getScalingParameter(index),
                 controller.getFanout(index),
                 controller.getThreshold(index),
                 controller.getSurvivalFactor(index),
                 min,
                 max);
        }

        public Collection<CompactionSSTable> getSSTables()
        {
            return sstables;
        }

        public int getIndex()
        {
            return index;
        }

        void add(CompactionSSTable sstable)
        {
            this.sstables.add(sstable);
            this.avg += (sstable.onDiskLength() - avg) / sstables.size();
        }

        void complete()
        {
            if (logger.isTraceEnabled())
                logger.trace("Level: {}", this);
        }

        /**
         * Return the compaction aggregate
         */
        Collection<CompactionAggregate.UnifiedAggregate> getCompactionAggregates(Arena arena,
                                                                                 Set<CompactionSSTable> expired,
                                                                                 Controller controller,
                                                                                 long spaceAvailable)
        {
            sstables.removeAll(expired);

            if (logger.isTraceEnabled())
                logger.trace("Creating compaction aggregate with sstable set {}", sstables);


            List<Set<CompactionSSTable>> overlaps = Overlaps.constructOverlapSets(sstables,
                                                                                  UnifiedCompactionStrategy::startsAfter,
                                                                                  CompactionSSTable.firstKeyComparator,
                                                                                  CompactionSSTable.lastKeyComparator);
            for (Set<CompactionSSTable> overlap : overlaps)
                maxOverlap = Math.max(maxOverlap, overlap.size());
            List<CompactionSSTable> unbucketed = new ArrayList<>();

            List<Bucket> buckets = Overlaps.assignOverlapsIntoBuckets(threshold,
                                                                      controller.overlapInclusionMethod(),
                                                                      overlaps,
                                                                      this::makeBucket,
                                                                      unbucketed::addAll);

            List<CompactionAggregate.UnifiedAggregate> aggregates = new ArrayList<>();
            for (Bucket bucket : buckets)
                aggregates.add(bucket.constructAggregate(controller, spaceAvailable, arena));

            // Add all unbucketed sstables separately. Note that this will list the level (with its set of sstables)
            // even if it does not need compaction.
            if (!unbucketed.isEmpty())
                aggregates.add(CompactionAggregate.createUnified(unbucketed,
                                                                 maxOverlap,
                                                                 CompactionPick.EMPTY,
                                                                 Collections.emptySet(),
                                                                 arena,
                                                                 this));

            if (logger.isTraceEnabled())
                logger.trace("Returning compaction aggregates {} for level {} of arena {}",
                             aggregates, this, arena);
            return aggregates;
        }

        private Bucket makeBucket(List<Set<CompactionSSTable>> overlaps, int startIndex, int endIndex)
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
        final List<CompactionSSTable> allSSTablesSorted;
        final int maxOverlap;

        Bucket(Level level, Collection<CompactionSSTable> allSSTablesSorted, int maxOverlap)
        {
            // single section
            this.level = level;
            this.allSSTablesSorted = new ArrayList<>(allSSTablesSorted);
            this.allSSTablesSorted.sort(CompactionSSTable.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        Bucket(Level level, List<Set<CompactionSSTable>> overlapSections)
        {
            // multiple sections
            this.level = level;
            int maxOverlap = 0;
            Set<CompactionSSTable> all = new HashSet<>();
            for (Set<CompactionSSTable> section : overlapSections)
            {
                maxOverlap = Math.max(maxOverlap, section.size());
                all.addAll(section);
            }
            this.allSSTablesSorted = new ArrayList<>(all);
            this.allSSTablesSorted.sort(CompactionSSTable.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        /**
         * Select compactions from this bucket. Normally this would form a compaction out of all sstables in the
         * bucket, but if compaction is very late we may prefer to act more carefully:
         * - we should not use more inputs than the permitted maximum
         * - we should not select a compaction whose execution will use more temporary space than is available
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
         * @param spaceAvailable The amount of space available for compaction, limits the maximum number of sstables
         *                       that can be selected.
         * @return A compaction pick to execute next.
         */
        CompactionAggregate.UnifiedAggregate constructAggregate(Controller controller, long spaceAvailable, Arena arena)
        {
            int count = maxOverlap;
            int threshold = level.threshold;
            int fanout = level.fanout;
            int index = level.index;
            int maxSSTablesToCompact = Math.max(fanout, (int) Math.min(spaceAvailable / level.avg, controller.maxSSTablesToCompact()));

            assert count >= threshold;
            if (count <= fanout)
            {
                /**
                 * Happy path. We are not late or (for levelled) we are only so late that a compaction now will
                 * have the  same effect as doing levelled compactions one by one. Compact all. We do not cap
                 * this pick at maxSSTablesToCompact due to an assumption that maxSSTablesToCompact is much
                 * greater than F. See {@link Controller#MAX_SSTABLES_TO_COMPACT_OPTION} for more details.
                 */
                return CompactionAggregate.createUnified(allSSTablesSorted,
                                                         count,
                                                         CompactionPick.create(index, allSSTablesSorted),
                                                         Collections.emptySet(),
                                                         arena,
                                                         level);
            }
            // The choices below assume that pulling the oldest sstables will reduce maxOverlap by the selected
            // number of sstables. This is not always true (we may, e.g. select alternately from different overlap
            // sections if the structure is complex enough), but is good enough heuristic that results in usable
            // compaction sets.
            else if (count <= fanout * controller.getFanout(index + 1) || maxSSTablesToCompact == fanout)
            {
                // Compaction is a bit late, but not enough to jump levels via layout compactions. We need a special
                // case to cap compaction pick at maxSSTablesToCompact.
                if (count <= maxSSTablesToCompact)
                    return CompactionAggregate.createUnified(allSSTablesSorted,
                                                             count,
                                                             CompactionPick.create(index, allSSTablesSorted),
                                                             Collections.emptySet(),
                                                             arena,
                                                             level);

                CompactionPick pick = CompactionPick.create(index, pullOldestSSTables(maxSSTablesToCompact));
                count -= maxSSTablesToCompact;
                List<CompactionPick> pending = new ArrayList<>();
                while (count >= threshold)
                {
                    pending.add(CompactionPick.create(index, pullOldestSSTables(maxSSTablesToCompact)));
                    count -= maxSSTablesToCompact;
                }

                return CompactionAggregate.createUnified(allSSTablesSorted, count, pick, pending, arena, level);
            }
            // We may, however, have accumulated a lot more than T if compaction is very late, or a set of small
            // tables was dumped on us (e.g. when converting from legacy LCS or for tests).
            else
            {
                // We need to pick the compactions in such a way that the result of doing them all spreads the data in
                // a similar way to how compaction would lay them if it was able to keep up. This means:
                // - for tiered compaction (w >= 0), compact in sets of as many as required to get to a level.
                //   for example, for w=2 and 55 sstables, do 3 compactions of 16 sstables, 1 of 4, and leave the other 3 alone
                // - for levelled compaction (w < 0), compact all that would reach a level.
                //   for w=-2 and 55, this means one compaction of 48, one of 4, and one of 3 sstables.
                List<CompactionPick> picks = layoutCompactions(controller, maxSSTablesToCompact);
                // Out of the set of necessary compactions, choose the one to run randomly. This gives a better
                // distribution among levels and should result in more compactions running in parallel in a big data
                // dump.
                assert !picks.isEmpty();  // we only enter this if count > F: layoutCompactions must have selected something to run
                CompactionPick selected = picks.remove(controller.random().nextInt(picks.size()));
                return CompactionAggregate.createUnified(allSSTablesSorted, count, selected, picks, arena, level);
            }
        }

        private List<CompactionPick> layoutCompactions(Controller controller, int maxSSTablesToCompact)
        {
            List<CompactionPick> pending = new ArrayList<>();
            int pos = layoutCompactions(controller, level.index + 1, level.fanout, maxSSTablesToCompact, pending);
            int size = maxOverlap;
            if (size - pos >= level.threshold) // can only happen in the levelled case.
            {
                assert size - pos < maxSSTablesToCompact; // otherwise it should have already been picked
                pending.add(CompactionPick.create(level.index, allSSTablesSorted));
            }
            return pending;
        }

        /**
         * Collects in {@param list} compactions of {@param sstables} such that they land in {@param level} and higher.
         *
         * Recursively combines SSTables into {@link CompactionPick}s in way that up to {@param maxSSTablesToCompact}
         * SSTables are combined to reach the highest possible level, then the rest is combined for the level before,
         * etc up to {@param level}.
         *
         * To agree with what compaction normally does, the first sstables from the list are placed in the picks that
         * combine to reach the highest levels.
         *
         * @param controller
         * @param level minimum target level for compactions to land
         * @param step - number of source SSTables required to reach level
         * @param maxSSTablesToCompact limit on the number of sstables per compaction
         * @param list - result list of layout-preserving compaction picks
         * @return index of the last used SSTable from {@param sstables}; the number of remaining sstables will be lower
         *         than step
         */
        private int layoutCompactions(Controller controller,
                                      int level,
                                      int step,
                                      int maxSSTablesToCompact,
                                      List<CompactionPick> list)
        {
            if (step > maxOverlap || step > maxSSTablesToCompact)
                return 0;

            int w = controller.getScalingParameter(level);
            int f = controller.getFanout(level);
            int pos = layoutCompactions(controller,
                                        level + 1,
                                        step * f,
                                        maxSSTablesToCompact,
                                        list);

            int total = maxOverlap;
            // step defines the number of source sstables that are needed to reach this level (ignoring overwrites
            // and deletions).
            // For tiered compaction we will select batches of this many.
            int pickSize = step;
            if (w < 0)
            {
                // For levelled compaction all the sstables that would reach this level need to be compacted to one,
                // so select the highest multiple of step that is available, but make sure we don't do a compaction
                // bigger than the limit.
                pickSize *= Math.min(total - pos, maxSSTablesToCompact) / pickSize;

                if (pickSize == 0)  // Not enough sstables to reach this level, we can skip the processing below.
                    return pos;     // Note: this cannot happen on the top level, but can on lower ones.
            }

            while (pos + pickSize <= total)
            {
                // Note that we assign these compactions to the level that would normally produce them, which means that
                // they won't be taking up threads dedicated to the busy level.
                // Normally sstables end up on a level when a compaction on the previous brings their size to the
                // threshold (which corresponds to pickSize == step, always the case for tiered); in the case of
                // levelled compaction, when we compact more than 1 but less than F sstables on a level (which
                // corresponds to pickSize > step), it is an operation that is triggered on the same level.
                list.add(CompactionPick.create(pickSize > step ? level : level - 1,
                                               pullOldestSSTables(pickSize)));
                pos += pickSize;
            }

            // In the levelled case, if we had to adjust pickSize due to maxSSTablesToCompact, there may
            // still be enough sstables to reach this level (e.g. if max was enough for 2*step, but we had 3*step).
            if (pos + step <= total)
            {
                pickSize = ((total - pos) / step) * step;
                list.add(CompactionPick.create(pickSize > step ? level : level - 1,
                                               pullOldestSSTables(pickSize)));
                pos += pickSize;
            }
            return pos;
        }

        static <T> List<T> pullLast(List<T> source, int limit)
        {
            List<T> result = new ArrayList<>(limit);
            while (--limit >= 0)
                result.add(source.remove(source.size() - 1));
            return result;
        }

        /**
         * Pull the oldest sstables to get at most limit-many overlapping sstables to compact in each overlap section.
         */
        abstract Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit);
    }

    public static class SimpleBucket extends Bucket
    {
        public SimpleBucket(Level level, Collection<CompactionSSTable> sstables)
        {
            super(level, sstables, sstables.size());
        }

        Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit)
        {
            if (allSSTablesSorted.size() <= overlapLimit)
                return allSSTablesSorted;
            return pullLast(allSSTablesSorted, overlapLimit);
        }
    }

    public static class MultiSetBucket extends Bucket
    {
        final List<Set<CompactionSSTable>> overlapSets;

        public MultiSetBucket(Level level, List<Set<CompactionSSTable>> overlapSets)
        {
            super(level, overlapSets);
            this.overlapSets = overlapSets;
        }

        Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit)
        {
            return Overlaps.pullLastWithOverlapLimit(allSSTablesSorted, overlapSets, overlapLimit);
        }
    }

    static class CompactionLimits
    {
        final int runningCompactions;
        final int maxConcurrentCompactions;
        final int maxCompactions;
        final int[] perLevel;
        int levelCount;
        final long spaceAvailable;
        final String rateLimitLog;
        final int remainingAdaptiveCompactions;

        public CompactionLimits(int runningCompactions,
                                int maxCompactions,
                                int maxConcurrentCompactions,
                                int[] perLevel,
                                int levelCount,
                                long spaceAvailable,
                                String rateLimitLog,
                                int remainingAdaptiveCompactions)
        {
            this.runningCompactions = runningCompactions;
            this.maxCompactions = maxCompactions;
            this.maxConcurrentCompactions = maxConcurrentCompactions;
            this.perLevel = perLevel;
            this.levelCount = levelCount;
            this.spaceAvailable = spaceAvailable;
            this.rateLimitLog = rateLimitLog;
            this.remainingAdaptiveCompactions = remainingAdaptiveCompactions;
        }

        @Override
        public String toString()
        {
            return String.format("Current limits: running=%d, max=%d, maxConcurrent=%d, perLevel=%s, levelCount=%d, spaceAvailable=%s, rateLimitLog=%s, remainingAdaptiveCompactions=%d",
                                 runningCompactions, maxCompactions, maxConcurrentCompactions, Arrays.toString(perLevel), levelCount,
                                 FBUtilities.prettyPrintMemory(spaceAvailable), rateLimitLog, remainingAdaptiveCompactions);
        }
    }
}
