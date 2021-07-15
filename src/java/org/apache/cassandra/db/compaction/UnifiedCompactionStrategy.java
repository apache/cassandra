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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.ShardedMultiWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Throwables.perform;

/**
 * The unified compaction strategy is described in this design document:
 *
 * TODO: link to design doc or SEP
 */
public class UnifiedCompactionStrategy extends AbstractCompactionStrategy
{
    public static final Class<? extends CompactionStrategyContainer> CONTAINER_CLASS = UnifiedCompactionContainer.class;

    private static final Logger logger = LoggerFactory.getLogger(UnifiedCompactionStrategy.class);

    static final int MAX_LEVELS = 32;   // This is enough for a few petabytes of data (with the worst case fan factor
                                        // at W=0 this leaves room for 2^32 sstables, presumably of at least 1MB each).

    private final Controller controller;

    private volatile ArenaSelector arenaSelector;

    private long lastExpiredCheck;

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options)
    {
        this(factory, backgroundCompactions, options, Controller.fromOptions(factory.getCfs(), options));
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
        this(factory, new BackgroundCompactions(factory.getCfs()), new HashMap<>(), controller);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return Controller.validateOptions(CompactionStrategyOptions.validateOptions(options));
    }

    @Override
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        Collection<Collection<SSTableReader>> groups = new ArrayList<>();
        for (Shard shard : getCompactionShards(sstablesToGroup))
        {
            groups.addAll(super.groupSSTablesForAntiCompaction(shard.sstables));
        }

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
        controller.onStrategyBackgroundTaskRequest();

        Collection<CompactionAggregate> compactionAggregates = getNextCompactionAggregates(gcBefore);

        Collection<AbstractCompactionTask> tasks = new ArrayList<>(compactionAggregates.size());
        for (CompactionAggregate aggregate : compactionAggregates)
        {
            LifecycleTransaction transaction = dataTracker.tryModify(aggregate.getSelected().sstables, OperationType.COMPACTION);
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
     * @return either a normal sstable writer, if there are no shards, or a sharded sstable writer that will
     *         create multiple sstables if a shard has a sufficiently large sstable.
     */
    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector meta,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        if (controller.getNumShards() <= 1)
            return super.createSSTableMultiWriter(descriptor,
                                                  keyCount,
                                                  repairedAt,
                                                  pendingRepair,
                                                  isTransient,
                                                  meta,
                                                  header,
                                                  indexGroups,
                                                  lifecycleNewTracker);

        return new ShardedMultiWriter(cfs,
                                      descriptor,
                                      keyCount,
                                      repairedAt,
                                      pendingRepair,
                                      isTransient,
                                      meta,
                                      header,
                                      indexGroups,
                                      lifecycleNewTracker,
                                      controller.getMinSstableSizeBytes(),
                                      getShardBoundaries());
    }

    /**
     * Create the task that in turns creates the sstable writer used for compaction.
     *
     * @return either a normal compaction task, if there are no shards, or a sharded compaction task that in turn will
     * create a sharded compaction writer.
     */
    private CompactionTask createCompactionTask(LifecycleTransaction transaction, int gcBefore)
    {
        if (controller.getNumShards() <= 1)
            return new CompactionTask(cfs, transaction, gcBefore, false, this);

        return new UnifiedCompactionTask(cfs, this, transaction, gcBefore, controller.getMinSstableSizeBytes(), getShardBoundaries());
    }

    private void maybeUpdateSelector()
    {
        if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
            return; // the disk boundaries (and thus the local ranges too) have not changed since the last time we calculated

        synchronized (this)
        {
            if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
                return; // another thread beat us to the update

            DiskBoundaries currentBoundaries = cfs.getDiskBoundaries();
            List<PartitionPosition> shardBoundaries = computeShardBoundaries(currentBoundaries.getLocalRanges(),
                                                                             currentBoundaries.getPositions(),
                                                                             controller.getNumShards(),
                                                                             cfs.getPartitioner());
            arenaSelector = new ArenaSelector(currentBoundaries, shardBoundaries);
            // Note: this can just as well be done without the synchronization (races would be benign, just doing some
            // redundant work). For the current usages of this blocking is fine and expected to perform no worse.
        }
    }

    /**
     * We want to split the local token range in shards, aiming for close to equal share for each shard.
     * If there are no disk boundaries, we just split the token space equally, but if multiple disks have been defined
     * (each with its own share of the local range), we can't have shards spanning disk boundaries. This means that
     * shards need to be selected within the disk's portion of the local ranges.
     *
     * As an example of what this means, consider a 3-disk node and 10 shards. The range is split equally between
     * disks, but we can only split shards within a disk range, thus we end up with 6 shards taking 1/3*1/3=1/9 of the
     * token range, and 4 smaller shards taking 1/3*1/4=1/12 of the token range.
     */
    @VisibleForTesting
    static List<PartitionPosition> computeShardBoundaries(SortedLocalRanges localRanges,
                                                          List<PartitionPosition> diskBoundaries,
                                                          int numShards,
                                                          IPartitioner partitioner)
    {
        Optional<Splitter> splitter = partitioner.splitter();
        if (diskBoundaries != null && !splitter.isPresent())
            return diskBoundaries;
        else if (!splitter.isPresent()) // C* 2i case, just return 1 boundary at min token
            return ImmutableList.of(partitioner.getMinimumToken().minKeyBound());

        // this should only happen in tests that change partitioners, but we don't want UCS to throw
        // where other strategies work even if the situations are unrealistic.
        if (localRanges.getRanges().isEmpty() || !localRanges.getRanges()
                                                             .get(0)
                                                             .range()
                                                             .left
                                                             .getPartitioner()
                                                             .equals(partitioner))
            localRanges = new SortedLocalRanges(StorageService.instance,
                                                localRanges.getCfs(),
                                                localRanges.getRingVersion(),
                                                ImmutableList.of(new Splitter.WeightedRange(1.0,
                                                                                            new Range<>(partitioner.getMinimumToken(),
                                                                                                        partitioner.getMaximumToken()))));

        if (diskBoundaries == null || diskBoundaries.size() <= 1)
            return localRanges.split(numShards);

        if (numShards <= diskBoundaries.size())
            return diskBoundaries;

        return splitPerDiskRanges(localRanges,
                                  diskBoundaries,
                                  getRangesTotalSize(localRanges.getRanges()),
                                  numShards,
                                  splitter.get());
    }

    /**
     * Split the per-disk ranges and generate the required number of shard boundaries.
     * This works by accumulating the size after each disk's share, multiplying by shardNum/totalSize and rounding to
     * produce an integer number of total shards needed by the disk boundary, which in turns defines how many need to be
     * added for this disk.
     *
     * For example, for a total size of 1, 2 disks (each of 0.5 share) and 3 shards, this will:
     * -process disk 1:
     * -- calculate 1/2 as the accumulated size
     * -- map this to 3/2 and round to 2 shards
     * -- split the disk's ranges into two equally-sized shards
     * -process disk 2:
     * -- calculate 1 as the accumulated size
     * -- map it to 3 and round to 3 shards
     * -- assign the disk's ranges to one shard
     *
     * The resulting shards will not be of equal size and this works best if the disk shares are distributed evenly
     * (which the current code always ensures).
     */
    private static List<PartitionPosition> splitPerDiskRanges(SortedLocalRanges localRanges,
                                                              List<PartitionPosition> diskBoundaries,
                                                              double totalSize,
                                                              int numShards,
                                                              Splitter splitter)
    {
        double perShard = totalSize / numShards;
        List<PartitionPosition> shardBoundaries = new ArrayList<>(numShards);
        double processedSize = 0;
        Token left = diskBoundaries.get(0).getToken().getPartitioner().getMinimumToken();
        for (PartitionPosition boundary : diskBoundaries)
        {
            Token right = boundary.getToken();
            List<Splitter.WeightedRange> disk = localRanges.subrange(new Range<>(left, right));

            processedSize += getRangesTotalSize(disk);
            int targetCount = (int) Math.round(processedSize / perShard);
            List<Token> splits = splitter.splitOwnedRanges(Math.max(targetCount - shardBoundaries.size(), 1), disk, Splitter.SplitType.ALWAYS_SPLIT).boundaries;
            shardBoundaries.addAll(Collections2.transform(splits, Token::maxKeyBound));
            // The splitting always results in maxToken as the last boundary. Replace it with the disk's upper bound.
            shardBoundaries.set(shardBoundaries.size() - 1, boundary);

            left = right;
        }
        assert shardBoundaries.size() == numShards;
        return shardBoundaries;
    }

    private static double getRangesTotalSize(List<Splitter.WeightedRange> ranges)
    {
        double totalSize = 0;
        for (Splitter.WeightedRange range : ranges)
            totalSize += range.left().size(range.right());
        return totalSize;
    }

    @VisibleForTesting
    List<PartitionPosition> getShardBoundaries()
    {
        maybeUpdateSelector();
        return arenaSelector.shardBoundaries;
    }

    private Collection<CompactionAggregate> getNextCompactionAggregates(int gcBefore)
    {
        // Calculate the running compaction limits, i.e. the overall number of compactions permitted, which is either
        // the compaction thread count, or the compaction throughput divided by the compaction rate (to prevent slowing
        // down individual compaction progress).
        String rateLimitLog = "";

        // identify parallel compactions limit
        int maxConcurrentCompactions = controller.maxConcurrentCompactions();
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
        for (CompactionPick compaction : backgroundCompactions.getCompactionsInProgress())
        {
            final int level = levelOf(compaction);
            ++perLevel[level];
            ++runningCompactions;
            levelCount = Math.max(levelCount, level + 1);
            spaceAvailable -= compaction.totSizeInBytes;
        }

        logger.debug("Selecting up to {} new compactions of up to {}, concurrency limit {}{}",
                     maxCompactions - runningCompactions,
                     FBUtilities.prettyPrintMemory(spaceAvailable),
                     maxConcurrentCompactions,
                     rateLimitLog);

        List<CompactionAggregate.UnifiedAggregate> pending = new ArrayList<>();
        long ts = System.currentTimeMillis();
        boolean expiredCheck = ts - lastExpiredCheck > controller.getExpiredSSTableCheckFrequency();
        if (expiredCheck)
            lastExpiredCheck = ts;

        for (Map.Entry<Shard, List<Bucket>> entry : getShardsWithBuckets().entrySet())
        {
            Shard shard = entry.getKey();
            Set<SSTableReader> expired;
            if (expiredCheck)
            {
                expired = shard.getExpiredSSTables(gcBefore, controller.getIgnoreOverlapsInExpirationCheck());
                if (logger.isTraceEnabled() && expired.size() > 0)
                    logger.trace("Expiration check for shard {} found {} fully expired SSTables", shard.name(), expired.size());
            }
            else
                expired = Collections.emptySet();

            for (Bucket bucket : entry.getValue())
            {
                CompactionAggregate.UnifiedAggregate aggregate = bucket.getCompactionAggregate(shard, expired, controller, spaceAvailable);
                // Note: We allow empty aggregates into the list of pending compactions. The pending compactions list
                // is for progress tracking only, and it is helpful to see empty levels there.
                pending.add(aggregate);

                // Make sure the level count includes all levels for which we have sstables (to be ready to compact
                // as soon as the threshold is crossed)...
                levelCount = Math.max(levelCount, aggregate.bucketIndex() + 1);
                if (aggregate.selected != null)
                {
                    // ... and also the levels that a layout-preserving selection would create.
                    levelCount = Math.max(levelCount, levelOf(aggregate.selected) + 1);
                }

                // The space overhead limit also applies when a single compaction is above that limit. This should
                // prevent running out of space at the expense of several highest-level tables extra, i.e. slightly
                // higher read amplification, which I think is a sensible tradeoff; however, operators must be warned
                // if this happens.
                warnIfSizeAbove(aggregate, spaceOverheadLimit);
            }
        }

        // Update the tracked background tasks.
        backgroundCompactions.setPending(this, pending);

        final List<CompactionAggregate> selection = getSelection(pending, maxCompactions, levelCount, perLevel, spaceAvailable);
        logger.debug("Starting {} compactions.", selection.size());
        return selection;
    }

    private void warnIfSizeAbove(CompactionAggregate.UnifiedAggregate aggregate, long spaceOverheadLimit)
    {
        if (aggregate.selected.totSizeInBytes > spaceOverheadLimit)
            logger.warn("Compaction needs to perform an operation that is bigger than the current space overhead " +
                        "limit - size {} (compacting {} sstables in shard {}/bucket {}); limit {} = {}% of dataset size {}. " +
                        "To honor the limit, this operation will not be performed, which may result in degraded performance.\n" +
                        "Please verify the compaction parameters, specifically {} and {}.",
                        FBUtilities.prettyPrintMemory(aggregate.selected.totSizeInBytes),
                        aggregate.selected.sstables.size(),
                        aggregate.getShard().name(),
                        aggregate.bucketIndex(),
                        FBUtilities.prettyPrintMemory(spaceOverheadLimit),
                        controller.getMaxSpaceOverhead() * 100,
                        FBUtilities.prettyPrintMemory(controller.getDataSetSizeBytes()),
                        Controller.DATASET_SIZE_OPTION_GB,
                        Controller.MAX_SPACE_OVERHEAD_OPTION);
    }

    /**
     * Returns a random selection of the compactions to be submitted. The selection will be chosen so that the total
     * number of compactions is at most totalCount, where each level gets a share that is the whole part of the ratio
     * between the the total permitted number of compactions, and the remainder gets distributed randomly among the
     * levels. Note that if a level does not have tasks to fill its share, its quota will remain unused in this
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
     */
    List<CompactionAggregate> getSelection(List<CompactionAggregate.UnifiedAggregate> pending,
                                           int totalCount,
                                           int levelCount,
                                           int[] perLevel,
                                           long spaceAvailable)
    {
        int perLevelCount = totalCount / levelCount;   // each level has this number of tasks reserved for it
        int remainder = totalCount % levelCount;       // and the remainder is distributed randomly, up to 1 per level

        // List the indexes of all compaction picks, adding several entries for compactions that span multiple shards.
        IntArrayList list = new IntArrayList(pending.size(), -1);
        IntArrayList expired = new IntArrayList(pending.size(), -1);
        for (int aggregateIndex = 0; aggregateIndex < pending.size(); ++aggregateIndex)
        {
            CompactionAggregate.UnifiedAggregate aggregate = pending.get(aggregateIndex);
            final CompactionPick pick = aggregate.selected;
            if (pick.isEmpty())
                continue;
            if (pick.hasExpiredOnly())
            {
                expired.add(aggregateIndex);
                continue;
            }
            if (pick.totSizeInBytes > spaceAvailable)
                continue;
            if (perLevel[levelOf(pick)] > perLevelCount)
                continue;  // this level is already using up all its share + one, we can ignore candidate altogether

            int shardsSpanned = shardsSpanned(pick);
            for (int i = 0; i < shardsSpanned; ++i)  // put an entry for each spanned shard
                list.addInt(aggregateIndex);
        }
        if (list.isEmpty() && expired.isEmpty())
            return ImmutableList.of();

        BitSet selection = new BitSet(pending.size());

        // Always include expire-only aggregates
        for (int i = 0; i < expired.size(); i++)
            selection.set(expired.get(i));

        int selectedSize = 0;
        if (!list.isEmpty())
        {
            // Randomize the list.
            Collections.shuffle(list, controller.random());

            // Calculate how many new ones we can add in each level, and how many we can assign randomly.
            int remaining = totalCount;
            for (int i = 0; i < levelCount; ++i)
            {
                remaining -= perLevel[i];
                if (perLevel[i] > perLevelCount)
                    remainder -= perLevel[i] - perLevelCount;
            }
            int toAdd = remaining;
            // Note: if we are in the middle of changes in the parameters or level count, remainder might become negative.
            // This is okay, some buckets will temporarily not get their rightful share until these tasks complete.

            // Select the first ones, skipping over duplicates and permitting only the specified number per level.
            for (int i = 0; remaining > 0 && i < list.size(); ++i)
            {
                final int aggregateIndex = list.getInt(i);
                if (selection.get(aggregateIndex))
                    continue; // this is a repeat
                CompactionAggregate.UnifiedAggregate aggregate = pending.get(aggregateIndex);
                if (aggregate.selected.totSizeInBytes > spaceAvailable)
                    continue; // compaction is too large for current cycle
                int level = levelOf(aggregate.selected);

                if (perLevel[level] > perLevelCount)
                    continue;   // share + one already used
                else if (perLevel[level] == perLevelCount)
                {
                    if (remainder <= 0)
                        continue;   // share used up, no remainder to distribute
                    --remainder;
                }

                --remaining;
                ++perLevel[level];
                spaceAvailable -= aggregate.selected.totSizeInBytes;
                selection.set(aggregateIndex);
            }

            selectedSize = toAdd - remaining;
        }

        // Return in the order of the pending aggregates to satisfy tests.
        List<CompactionAggregate> aggregates = new ArrayList<>(selectedSize + expired.size());
        for (int i = selection.nextSetBit(0); i >= 0; i = selection.nextSetBit(i+1))
            aggregates.add(pending.get(i));

        return aggregates;
    }

    private int shardsSpanned(CompactionPick pick)
    {
        DecoratedKey min = pick.sstables.stream().map(SSTableReader::getFirst).min(Ordering.natural()).get();
        DecoratedKey max = pick.sstables.stream().map(SSTableReader::getLast).max(Ordering.natural()).get();
        return arenaSelector.shardFor(max) - arenaSelector.shardFor(min) + 1;
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
    public Set<SSTableReader> getSSTables()
    {
        return dataTracker.getLiveSSTables();
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
     * Group candidate sstables (non suspect and not already compacting, and not an early version of a compaction
     * result) into one or more compaction shards. Each compaction shard is obtained by comparing using a compound
     * comparator for the equivalence classes.
     *
     * @return a list of shards, where each shard contains sstables that are eligible for being compacted together
     */
    @VisibleForTesting
    Collection<Shard> getCompactionShards()
    {
        return getCompactionShards(dataTracker.getLiveSSTables());
    }

    Collection<Shard> getCompactionShards(Collection<SSTableReader> sstables)
    {
        final ArenaSelector arenaSelector = this.arenaSelector;
        Map<SSTableReader, Shard> tables = new TreeMap<>(arenaSelector);
        for (SSTableReader table : sstables)
            if (isSuitableForCompaction(table))
                tables.computeIfAbsent(table, t -> new Shard(arenaSelector, cfs))
                      .add(table);

        return tables.values();
    }

    private boolean isSuitableForCompaction(SSTableReader r)
    {
        return !r.isMarkedSuspect()
               && r.openReason != SSTableReader.OpenReason.EARLY
               && !dataTracker.getCompacting().contains(r);
    }

    /**
     * @return a LinkedHashMap of shards with buckets where order of shards are preserved
     */
    @VisibleForTesting
    Map<Shard, List<Bucket>> getShardsWithBuckets()
    {
        maybeUpdateSelector();
        Collection<Shard> shards = getCompactionShards();
        Map<Shard, List<Bucket>> ret = new LinkedHashMap<>(); // should preserve the order of shards

        for (Shard shard : shards)
        {
            List<Bucket> buckets = new ArrayList<>(MAX_LEVELS);
            shard.sstables.sort(arenaSelector::compareByShardAdjustedSize);

            int index = 0;
            Bucket bucket = new Bucket(controller, index, 0);
            for (SSTableReader candidate : shard.sstables)
            {
                final long size = arenaSelector.shardAdjustedSize(candidate);
                if (size < bucket.max)
                {
                    bucket.add(candidate);
                    continue;
                }

                bucket.sort();
                buckets.add(bucket); // add even if empty

                while (true)
                {
                    bucket = new Bucket(controller, ++index, bucket.max);
                    if (size < bucket.max)
                    {
                        bucket.add(candidate);
                        break;
                    }
                    else
                    {
                        buckets.add(bucket); // add the empty bucket
                    }
                }
            }

            if (!bucket.sstables.isEmpty())
            {
                bucket.sort();
                buckets.add(bucket);
            }

            if (!buckets.isEmpty())
                ret.put(shard, buckets);

            if (logger.isTraceEnabled())
                logger.trace("Shard {} has {} buckets", shard, buckets.size());
        }

        logger.debug("Found {} shards with buckets for {}.{}", ret.size(), cfs.getKeyspaceName(), cfs.getTableName());
        return ret;
    }

    private static int levelOf(CompactionPick pick)
    {
        return (int) pick.parent;
    }

    public TableMetadata getMetadata()
    {
        return cfs.metadata();
    }

    /**
     * A compaction shard contains the list of sstables that belong to this shard as well as the arena
     * selector used for comparison.
     */
    final static class Shard implements Comparable<Shard>
    {
        final List<SSTableReader> sstables;
        final ArenaSelector selector;
        private final ColumnFamilyStore cfs;

        Shard(ArenaSelector selector, ColumnFamilyStore cfs)
        {
            this.cfs = cfs;
            this.sstables = new ArrayList<>();
            this.selector = selector;
        }

        void add(SSTableReader ssTableReader)
        {
            sstables.add(ssTableReader);
        }

        public String name()
        {
            SSTableReader t = sstables.get(0);
            return selector.name(t);
        }

        @Override
        public int compareTo(Shard o)
        {
            return selector.compare(this.sstables.get(0), o.sstables.get(0));
        }

        @Override
        public String toString()
        {
            return String.format("%s, %d sstables", name(), sstables.size());
        }

        /**
         * Find fully expired SSTables. Those will be included in the aggregate no matter what.
         * @param gcBefore
         * @param ignoreOverlaps
         * @return expired SSTables
         */
        Set<SSTableReader> getExpiredSSTables(int gcBefore, boolean ignoreOverlaps)
        {
            return CompactionController.getFullyExpiredSSTables(cfs,
                                                                sstables,
                                                                cfs.getOverlappingLiveSSTables(sstables),
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
     * A bucket: index, sstables and some properties.
     */
    static class Bucket
    {
        final List<SSTableReader> sstables;
        final int index;
        final double survivalFactor;
        final int scalingParameter; // scaling parameter used to calculate fanout and threshold
        final int fanout; // fanout factor between buckets
        final int threshold; // number of SSTables that trigger a compaction
        final long min; // min size of sstables for this bucket
        final long max; // max size of sstables for this bucket
        double avg = 0; // avg size of sstables in this bucket

        Bucket(Controller controller, int index, long minSize)
        {
            this.index = index;
            this.survivalFactor = controller.getSurvivalFactor();
            this.scalingParameter = controller.getScalingParameter(index);
            this.fanout = controller.getFanout(index);
            this.threshold = controller.getThreshold(index);
            this.sstables = new ArrayList<>(threshold);
            this.min = minSize;

            double baseSize = minSize;
            if (minSize == 0)
                baseSize = controller.getBaseSstableSize(fanout);

            this.max = (long) Math.floor(baseSize * fanout * controller.getSurvivalFactor());
        }

        void add(SSTableReader sstable)
        {
            this.sstables.add(sstable);
            this.avg += (sstable.onDiskLength() - avg) / sstables.size();
        }

        void sort()
        {
            // Always sort by timestamp, older sstables first. If only a subset of the tables is compacted, let it
            // be from a contiguous time span to aid whole-sstable expiration.
            sstables.sort(Comparator.comparing(SSTableReader::getMaxTimestamp));

            if (logger.isTraceEnabled())
                logger.trace("Bucket: {}", this);
        }

        /**
         * Return the compaction aggregate
         */
        CompactionAggregate.UnifiedAggregate getCompactionAggregate(Shard shard,
                                                                    Set<SSTableReader> allExpiredSSTables,
                                                                    Controller controller,
                                                                    long spaceAvailable)
        {
            List<SSTableReader> expiredSet = Collections.emptyList();
            List<SSTableReader> liveSet = sstables;
            if (!allExpiredSSTables.isEmpty())
            {
                liveSet = new ArrayList<>();
                expiredSet = new ArrayList<>();
                bipartitionSSTables(sstables, allExpiredSSTables, liveSet, expiredSet);
            }

            List<CompactionPick> pending = ImmutableList.of();
            CompactionPick selected;
            int count = liveSet.size();
            int maxSSTablesToCompact = Math.max(fanout, controller.maxSSTablesToCompact());

            if (count < threshold)
            {
                // We do not have enough sstables for a compaction.
                selected = CompactionPick.EMPTY;
            }
            else if (count <= fanout)
            {
                /**
                 * Happy path. We are not late or (for levelled) we are only so late that a compaction now will
                 * have the  same effect as doing levelled compactions one by one. Compact all. We do not cap
                 * this pick at maxSSTablesToCompact due to an assumption that maxSSTablesToCompact is much
                 * greater than F. See {@link Controller#MAX_SSTABLES_TO_COMPACT_OPTION} for more details.
                 */
                selected = CompactionPick.create(index, liveSet);
            }
            else if (count <= fanout * controller.getFanout(index + 1))
            {
                // Compaction is a bit late, but not enough to jump levels via layout compactions. We need a special
                // case to cap compaction pick at maxSSTablesToCompact.
                selected = CompactionPick.create(index, liveSet.subList(0, Math.min(maxSSTablesToCompact, count)));
                if (count - maxSSTablesToCompact >= threshold)
                {
                    pending = new ArrayList<>();
                    int start = maxSSTablesToCompact;
                    int end = Math.min(2 * maxSSTablesToCompact, count);
                    while (end - start > threshold)
                    {
                        pending.add(CompactionPick.create(index, liveSet.subList(start, end)));
                        start = end;
                        end = Math.min(end + maxSSTablesToCompact, count);
                    }
                }
            }
            // We may, however, have accumulated a lot more than T if compaction is very late, or a set of small
            // tables was dumped on us (e.g. when converting from legacy LCS or for tests).
            else
            {
                // We need to pick the compactions in such a way that the result of doing them all spreads the data in
                // a similar way to how compaction would lay them if it was able to keep up. This means:
                // - for tiered compaction (W >= 0), compact in sets of as many as required to get to a level.
                //   for example, for W=2 and 55 sstables, do 3 compactions of 16 sstables, 1 of 4, and leave the other 3 alone
                // - for levelled compaction (W < 0), compact all that would reach a level.
                //   for W=-2 and 55, this means one compaction of 48, one of 4, and one of 3 sstables.
                pending = layoutCompactions(controller, liveSet, (int) Math.min(spaceAvailable / avg, maxSSTablesToCompact));
                // Out of the set of necessary compactions, choose the one to run randomly. This gives a better
                // distribution among levels and should result in more compactions running in parallel in a big data
                // dump.
                assert !pending.isEmpty();  // we only enter this if count > F: layoutCompactions must have selected something to run
                int index = controller.random().nextInt(pending.size());
                selected = pending.remove(index);
            }

            boolean hasExpiredSSTables = !expiredSet.isEmpty();
            if (hasExpiredSSTables && selected.equals(CompactionPick.EMPTY))
                // overrides default CompactionPick.EMPTY with parent equal to -1
                selected = CompactionPick.create(index, expiredSet, expiredSet);
            else if (hasExpiredSSTables)
                selected = selected.withExpiredSSTables(expiredSet);

            return CompactionAggregate.createUnified(sstables, selected, pending, shard, this);
        }

        /**
         * Bipartitions SSTables into liveSet and expiredSet, depending on whether they are present in allExpiredSSTables.
         *
         * @param sstables list of SSTables in a bucket
         * @param allExpiredSSTables set of expired SSTables for all shards/buckets
         * @param liveSet empty list that is going to be filled up with SSTables that are not present in {@param allExpiredSSTables}
         * @param expiredSet empty list that is going to be filled up with SSTables that are present in {@param allExpiredSSTables}
         */
        private static void bipartitionSSTables(List<SSTableReader> sstables,
                                                Set<SSTableReader> allExpiredSSTables,
                                                List<SSTableReader> liveSet,
                                                List<SSTableReader> expiredSet)
        {
            for (SSTableReader sstable : sstables)
            {
                if (allExpiredSSTables.contains(sstable))
                    expiredSet.add(sstable);
                else
                    liveSet.add(sstable);
            }
        }

        private List<CompactionPick> layoutCompactions(Controller controller, List<SSTableReader> liveSet, int maxSSTablesToCompact)
        {
            List<CompactionPick> pending = new ArrayList<>();
            int pos = layoutCompactions(controller, liveSet, index + 1, fanout, maxSSTablesToCompact, pending);
            int size = liveSet.size();
            if (size - pos >= threshold) // can only happen in the levelled case.
            {
                assert size - pos < maxSSTablesToCompact; // otherwise it should have already been picked
                pending.add(CompactionPick.create(index, liveSet.subList(pos, size)));
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
         * @param sstables SSTables to compact, sorted by age from old to new
         * @param level minimum target level for compactions to land
         * @param step - number of source SSTables required to reach level
         * @param maxSSTablesToCompact limit on the number of sstables per compaction
         * @param list - result list of layout-preserving compaction picks
         * @return index of the last used SSTable from {@param sstables}; the number of remaining sstables will be lower
         *         than step
         */
        private int layoutCompactions(Controller controller,
                                      List<SSTableReader> sstables,
                                      int level,
                                      int step,
                                      int maxSSTablesToCompact,
                                      List<CompactionPick> list)
        {
            if (step > sstables.size() || step > maxSSTablesToCompact)
                return 0;

            int W = controller.getScalingParameter(level);
            int F = controller.getFanout(level);
            int pos = layoutCompactions(controller,
                                        sstables,
                                        level + 1,
                                        step * F,
                                        maxSSTablesToCompact,
                                        list);

            int total = sstables.size();
            // step defines the number of source sstables that are needed to reach this level (ignoring overwrites
            // and deletions).
            // For tiered compaction we will select batches of this many.
            int pickSize = step;
            if (W < 0)
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
                                               sstables.subList(pos, pos + pickSize)));
                pos += pickSize;
            }

            // In the levelled case, if we had to adjust pickSize due to maxSSTablesToCompact, there may
            // still be enough sstables to reach this level (e.g. if max was enough for 2*step, but we had 3*step).
            if (pos + step <= total)
            {
                pickSize = ((total - pos) / step) * step;
                list.add(CompactionPick.create(pickSize > step ? level : level - 1,
                                               sstables.subList(pos, pos + pickSize)));
                pos += pickSize;
            }
            return pos;
        }

        @Override
        public String toString()
        {
            return String.format("W: %d, T: %d, F: %d, index: %d, min: %s, max %s, %d sstables",
                                 scalingParameter, threshold, fanout, index, FBUtilities.prettyPrintMemory(min), FBUtilities.prettyPrintMemory(max), sstables.size());
        }
    }
}
