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

package org.apache.cassandra.repair.autorepair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableScanner;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.split;

/**
 * In Apache Cassandra, tuning the repair ranges has three main goals:
 * <p>
 * 1. **Create smaller, consistent repair times**: Long repairs, such as those lasting 15 hours, can be problematic.
 * If a node fails 14 hours into the repair, the entire process must be restarted. The goal is to reduce the impact
 * of disturbances or failures. However, making the repairs too short can lead to overhead from repair orchestration
 * becoming the main bottleneck.
 * <p>
 * 2. **Minimize the impact on hosts**: Repairs should not heavily affect the host systems. For incremental repairs,
 * this might involve anti-compaction work. In full repairs, streaming large amounts of data—especially with wide
 * partitions—can lead to issues with disk usage and higher compaction costs.
 * <p>
 * 3. **Reduce overstreaming**: The Merkle tree, which represents data within each partition and range, has a maximum size.
 * If a repair covers too many partitions, the tree’s leaves represent larger data ranges. Even a small change in a leaf
 * can trigger excessive data streaming, making the process inefficient.
 * <p>
 * Additionally, if there are many small tables, it's beneficial to batch these tables together under a single parent repair.
 * This prevents the repair overhead from becoming a bottleneck, especially when dealing with hundreds of tables. Running
 * individual repairs for each table can significantly impact performance and efficiency.
 * <p>
 * To manage these issues, the strategy involves estimating the size and number of partitions within a range and splitting
 * it accordingly to bound the size of the range splits.
 */

public class RepairRangeSplitter implements IAutoRepairTokenRangeSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(RepairRangeSplitter.class);

    static final String SUBRANGE_SIZE = "bytes_per_assignment";
    static final String PARTITION_COUNT = "partitions_per_assignment";
    static final String TABLE_BATCH_LIMIT = "max_tables_per_assignment";
    static final String MAX_BYTES_PER_SCHEDULE = "max_bytes_per_schedule";

    private final int tablesPerAssignmentLimit;
    private final long maxBytesPerSchedule;
    private final long bytesPerSubrange;
    private final long partitionsPerSubrange;

    private static final long DEFAULT_SUBRANGE_SIZE = new DataStorageSpec.LongBytesBound("100GiB").toBytes();
    private static final long DEFAULT_MAX_BYTES_PER_SCHEDULE = Long.MAX_VALUE;
    private static final long DEFAULT_PARTITION_LIMIT = (long) Math.pow(2, DatabaseDescriptor.getRepairSessionMaxTreeDepth());
    private static final int DEFAULT_TABLE_BATCH_LIMIT = 64;

    public RepairRangeSplitter(Map<String, String> parameters)
    {
        if (parameters.containsKey(SUBRANGE_SIZE))
        {
            bytesPerSubrange = new DataStorageSpec.LongBytesBound(parameters.get(SUBRANGE_SIZE)).toBytes();
        }
        else
        {
            bytesPerSubrange = DEFAULT_SUBRANGE_SIZE;
        }

        if (parameters.containsKey(MAX_BYTES_PER_SCHEDULE))
        {
            maxBytesPerSchedule = new DataStorageSpec.LongBytesBound(parameters.get(MAX_BYTES_PER_SCHEDULE)).toBytes();
        }
        else
        {
            maxBytesPerSchedule = DEFAULT_MAX_BYTES_PER_SCHEDULE;
        }

        if (parameters.containsKey(PARTITION_COUNT))
        {
            partitionsPerSubrange = Long.parseLong(parameters.get(PARTITION_COUNT));
        }
        else
        {
            partitionsPerSubrange = DEFAULT_PARTITION_LIMIT;
        }

        if (parameters.containsKey(TABLE_BATCH_LIMIT))
        {
            tablesPerAssignmentLimit = Integer.parseInt(parameters.get(TABLE_BATCH_LIMIT));
        }
        else
        {
            tablesPerAssignmentLimit = DEFAULT_TABLE_BATCH_LIMIT;
        }

        if (bytesPerSubrange > maxBytesPerSchedule)
        {
            throw new IllegalArgumentException(String.format("%s='%s' cannot be greater than %s='%s'",
                                                             SUBRANGE_SIZE,
                                                             FileUtils.stringifyFileSize(bytesPerSubrange),
                                                             MAX_BYTES_PER_SCHEDULE,
                                                             FileUtils.stringifyFileSize(maxBytesPerSchedule)));
        }

        logger.info("Configured {} with {}={}, {}={}, {}={}, {}={}", RepairRangeSplitter.class.getName(),
                    SUBRANGE_SIZE, FileUtils.stringifyFileSize(bytesPerSubrange),
                    MAX_BYTES_PER_SCHEDULE, FileUtils.stringifyFileSize(maxBytesPerSchedule),
                    PARTITION_COUNT, partitionsPerSubrange, TABLE_BATCH_LIMIT, tablesPerAssignmentLimit);
    }

    @Override
    public Iterator<KeyspaceRepairAssignments> getRepairAssignments(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
    {
        return new RepairAssignmentIterator(repairType, primaryRangeOnly, repairPlans);
    }

    private class RepairAssignmentIterator implements Iterator<KeyspaceRepairAssignments>
    {

        private final AutoRepairConfig.RepairType repairType;
        private final boolean primaryRangeOnly;

        private final Iterator<PrioritizedRepairPlan> repairPlanIterator;

        private Iterator<KeyspaceRepairPlan> currentIterator = null;
        private PrioritizedRepairPlan currentPlan = null;
        private long bytesSoFar = 0;

        RepairAssignmentIterator(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
        {
            this.repairType = repairType;
            this.primaryRangeOnly = primaryRangeOnly;
            this.repairPlanIterator = repairPlans.iterator();
        }

        private synchronized Iterator<KeyspaceRepairPlan> currentIterator()
        {
            if (currentIterator == null || !currentIterator.hasNext())
            {
                // Advance the repair plan iterator if the current repair plan is exhausted, but only
                // if there are more repair plans.
                if (repairPlanIterator.hasNext())
                {
                    currentPlan = repairPlanIterator.next();
                    currentIterator = currentPlan.getKeyspaceRepairPlans().iterator();
                }
            }

            return currentIterator;
        }

        @Override
        public boolean hasNext()
        {
            return currentIterator().hasNext();
        }

        @Override
        public KeyspaceRepairAssignments next()
        {
            // Should not happen unless violating the contract of iterator of checking hasNext first.
            if (!currentIterator.hasNext())
            {
                throw new NoSuchElementException("No remaining repair plans");
            }

            final KeyspaceRepairPlan repairPlan = currentIterator().next();
            Collection<Range<Token>> tokenRanges = getTokenRanges(primaryRangeOnly, repairPlan.getKeyspaceName());
            List<SizedRepairAssignment> repairAssignments = getRepairAssignmentsForKeyspace(repairType, repairPlan.getKeyspaceName(), repairPlan.getTableNames(), tokenRanges);
            FilteredRepairAssignments filteredRepairAssignments = filterRepairAssignments(repairType, currentPlan.getPriority(), repairPlan.getKeyspaceName(), repairAssignments, bytesSoFar);
            bytesSoFar = filteredRepairAssignments.newBytesSoFar;
            return new KeyspaceRepairAssignments(currentPlan.getPriority(), repairPlan.getKeyspaceName(), filteredRepairAssignments.repairAssignments);
        }
    }

    @VisibleForTesting
    List<SizedRepairAssignment> getRepairAssignmentsForKeyspace(AutoRepairConfig.RepairType repairType, String keyspaceName, List<String> tableNames, Collection<Range<Token>> tokenRanges)
    {
        List<SizedRepairAssignment> repairAssignments = new ArrayList<>();
        // this is used for batching minimal single assignment tables together
        List<SizedRepairAssignment> currentAssignments = new ArrayList<>();

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();

        // If we can repair by keyspace, sort the tables by size so can batch the smallest ones together
        boolean repairByKeyspace = config.getRepairByKeyspace(repairType);
        if (repairByKeyspace)
        {
            tableNames.sort((t1, t2) -> {
                ColumnFamilyStore cfs1 = ColumnFamilyStore.getIfExists(keyspaceName, t1);
                ColumnFamilyStore cfs2 = ColumnFamilyStore.getIfExists(keyspaceName, t2);
                if (cfs1 == null || cfs2 == null)
                    throw new IllegalArgumentException(String.format("Could not resolve ColumnFamilyStore from %s.%s", keyspaceName, t1));
                return Long.compare(cfs1.metric.totalDiskSpaceUsed.getCount(), cfs2.metric.totalDiskSpaceUsed.getCount());
            });
        }

        for (String tableName : tableNames)
        {
            List<SizedRepairAssignment> tableAssignments = getRepairAssignmentsForTable(repairType, keyspaceName, tableName, tokenRanges);

            if (tableAssignments.isEmpty())
                continue;

            // if not repairing by keyspace don't attempt to batch them with others.
            if (!repairByKeyspace)
            {
                repairAssignments.addAll(tableAssignments);
            }
            // If the table assignments are for the same token range, and we have room to add more tables to the current assignment
            else if (tableAssignments.size() == 1 &&
                currentAssignments.size() < tablesPerAssignmentLimit &&
                (currentAssignments.isEmpty() || currentAssignments.get(0).getTokenRange().equals(tableAssignments.get(0).getTokenRange())))
            {
                long currentAssignmentsBytes = getEstimatedBytes(currentAssignments);
                long tableAssignmentsBytes = getEstimatedBytes(tableAssignments);
                // only add assignments together if they don't exceed max bytes per schedule.
                if (currentAssignmentsBytes + tableAssignmentsBytes < maxBytesPerSchedule) {
                    currentAssignments.addAll(tableAssignments);
                }
                else
                {
                    // add table assignments by themselves
                    repairAssignments.addAll(tableAssignments);
                }
            }
            else
            {
                if (!currentAssignments.isEmpty())
                {
                    repairAssignments.add(merge(currentAssignments));
                    currentAssignments.clear();
                }
                repairAssignments.addAll(tableAssignments);
            }
        }

        if (!currentAssignments.isEmpty())
            repairAssignments.add(merge(currentAssignments));

        return repairAssignments;
    }

    /**
     * Given a repair type and map of sized-based repair assignments, confine them by <code>maxBytesPerSchedule</code>.
     * @param repairType used to determine underyling table priorities
     * @param repairAssignments the assignments to filter.
     * @param bytesSoFar repair assignment bytes accumulated so far.
     * @return A list of repair assignments confined by <code>maxBytesPerSchedule</code>.
     */
    @VisibleForTesting
    FilteredRepairAssignments filterRepairAssignments(AutoRepairConfig.RepairType repairType, int priority, String keyspaceName, List<SizedRepairAssignment> repairAssignments, long bytesSoFar)
    {
        // Confine repair assignments by maxBytesPerSchedule.
        long bytesSoFarThisIteration = 0L;
        long bytesNotRepaired = 0L;
        int assignmentsNotRepaired = 0;
        int assignmentsToRepair = 0;
        int totalAssignments = 0;

        List<RepairAssignment> assignmentsToReturn = new ArrayList<>(repairAssignments.size());
        for (SizedRepairAssignment repairAssignment : repairAssignments)
        {
            totalAssignments++;
            // skip any repair assignments that would accumulate us past the maxBytesPerSchedule
            if (bytesSoFar + repairAssignment.getEstimatedBytes() > maxBytesPerSchedule)
            {
                // log that repair assignment was skipped.
                bytesNotRepaired += repairAssignment.getEstimatedBytes();
                assignmentsNotRepaired++;
                logger.warn("Skipping {} because it would increase total repair bytes to {}",
                            repairAssignment,
                            getBytesOfMaxBytesPerSchedule(bytesSoFar + repairAssignment.getEstimatedBytes()));
            }
            else
            {
                bytesSoFar += repairAssignment.getEstimatedBytes();
                bytesSoFarThisIteration += repairAssignment.getEstimatedBytes();
                assignmentsToRepair++;
                logger.info("Adding {}, increasing repair bytes to {}",
                            repairAssignment,
                            getBytesOfMaxBytesPerSchedule(bytesSoFar));
                assignmentsToReturn.add(repairAssignment);
            }
        }

        String message = "Returning {} assignment(s) for priorityBucket {} and keyspace {}, totaling {} ({} overall)";
        if (assignmentsNotRepaired != 0)
        {
            message += ". Skipping {} of {} assignment(s), totaling {}";
            if (repairType != AutoRepairConfig.RepairType.INCREMENTAL)
            {
                message += ". The entire primary range will not be repaired this schedule. " +
                           "Consider increasing maxBytesPerSchedule, reducing node density or monitoring to ensure " +
                           "all ranges do get repaired within gc_grace_seconds";
                logger.warn(message, assignmentsToRepair, priority, keyspaceName,
                            FileUtils.stringifyFileSize(bytesSoFarThisIteration),
                            getBytesOfMaxBytesPerSchedule(bytesSoFar),
                            assignmentsNotRepaired, totalAssignments,
                            FileUtils.stringifyFileSize(bytesNotRepaired));
            }
            else
            {
                logger.info(message, assignmentsToRepair, priority, keyspaceName,
                            FileUtils.stringifyFileSize(bytesSoFarThisIteration),
                            getBytesOfMaxBytesPerSchedule(bytesSoFar),
                            assignmentsNotRepaired, totalAssignments,
                            FileUtils.stringifyFileSize(bytesNotRepaired));
            }
        }
        else
        {
            logger.info(message, assignmentsToRepair, priority, keyspaceName,
                        FileUtils.stringifyFileSize(bytesSoFarThisIteration),
                        getBytesOfMaxBytesPerSchedule(bytesSoFar));
        }

        return new FilteredRepairAssignments(assignmentsToReturn, bytesSoFar);
    }

    private static class FilteredRepairAssignments
    {
        private final List<RepairAssignment> repairAssignments;
        private final long newBytesSoFar;

        private FilteredRepairAssignments(List<RepairAssignment> repairAssignments, long newBytesSoFar)
        {
            this.repairAssignments = repairAssignments;
            this.newBytesSoFar = newBytesSoFar;
        }
    }

    private String getBytesOfMaxBytesPerSchedule(long bytes) {
        if (maxBytesPerSchedule == Long.MAX_VALUE)
            return FileUtils.stringifyFileSize(bytes);
        else
            return String.format("%s of %s", FileUtils.stringifyFileSize(bytes), FileUtils.stringifyFileSize(maxBytesPerSchedule));
    }

    /**
     * @return The sum of {@link SizedRepairAssignment#getEstimatedBytes()} of all given
     * repairAssignments.
     * @param repairAssignments The assignments to sum
     */
    @VisibleForTesting
    protected static long getEstimatedBytes(List<SizedRepairAssignment> repairAssignments)
    {
        return repairAssignments
               .stream()
               .mapToLong(SizedRepairAssignment::getEstimatedBytes)
               .sum();
    }

    @VisibleForTesting
    static SizedRepairAssignment merge(List<SizedRepairAssignment> assignments)
    {
        if (assignments.isEmpty())
            throw new IllegalStateException("Cannot merge empty assignments");

        Set<String> mergedTableNames = new HashSet<>();
        Range<Token> referenceTokenRange = assignments.get(0).getTokenRange();
        String referenceKeyspaceName = assignments.get(0).getKeyspaceName();

        for (SizedRepairAssignment assignment : assignments)
        {
            // These checks _should_ be unnecessary but are here to ensure that the assignments are consistent
            if (!assignment.getTokenRange().equals(referenceTokenRange))
                throw new IllegalStateException("All assignments must have the same token range");
            if (!assignment.getKeyspaceName().equals(referenceKeyspaceName))
                throw new IllegalStateException("All assignments must have the same keyspace name");

            mergedTableNames.addAll(assignment.getTableNames());
        }

        long sizeForAssignment = getEstimatedBytes(assignments);
        return new SizedRepairAssignment(referenceTokenRange, referenceKeyspaceName, new ArrayList<>(mergedTableNames),
                                         "full primary range for " + mergedTableNames.size() + " tables", sizeForAssignment);
    }

    @VisibleForTesting
    protected List<SizedRepairAssignment> getRepairAssignmentsForTable(AutoRepairConfig.RepairType repairType, String keyspaceName, String tableName, Collection<Range<Token>> tokenRanges)
    {
        List<SizeEstimate> sizeEstimates = getRangeSizeEstimate(repairType, keyspaceName, tableName, tokenRanges);
        return getRepairAssignments(sizeEstimates);
    }

    @VisibleForTesting
    protected List<SizedRepairAssignment> getRepairAssignments(List<SizeEstimate> sizeEstimates)
    {
        List<SizedRepairAssignment> repairAssignments = new ArrayList<>();

        // since its possible for us to hit maxBytesPerSchedule before seeing all ranges, shuffle so there is chance
        // at least of hitting all the ranges _eventually_ for the worst case scenarios
        Collections.shuffle(sizeEstimates);
        for (SizeEstimate estimate : sizeEstimates)
        {
            if (estimate.sizeForRepair == 0)
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(estimate.keyspace, estimate.table);
                long memtableSize = cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                if (memtableSize > 0L)
                {
                    logger.debug("Included {}.{} range {}, had no unrepaired SSTables, but memtableSize={}, adding single repair assignment", estimate.keyspace, estimate.table, estimate.tokenRange, memtableSize);
                    SizedRepairAssignment assignment = new SizedRepairAssignment(estimate.tokenRange, estimate.keyspace, Collections.singletonList(estimate.table), "memtable only", memtableSize);
                    repairAssignments.add(assignment);
                }
                else
                {
                    logger.debug("Skipping {}.{} for range {} because it had no unrepaired SSTables and no memtable data", estimate.keyspace, estimate.table, estimate.tokenRange);
                }
            }
            else
            {
                // Check if the estimate needs splitting based on the criteria
                boolean needsSplitting = estimate.sizeForRepair > bytesPerSubrange || estimate.partitions > partitionsPerSubrange;
                if (needsSplitting)
                {
                    int numberOfSplits = calculateNumberOfSplits(estimate);
                    long approximateBytesPerSplit = estimate.sizeForRepair / numberOfSplits;
                    Collection<Range<Token>> subranges = split(estimate.tokenRange, numberOfSplits);
                    for (Range<Token> subrange : subranges)
                    {
                        SizedRepairAssignment assignment = new SizedRepairAssignment(subrange, estimate.keyspace, Collections.singletonList(estimate.table),
                                                                                     String.format("subrange %d of %d", repairAssignments.size()+1, numberOfSplits),
                                                                                     approximateBytesPerSplit);
                        repairAssignments.add(assignment);
                    }
                }
                else
                {
                    // No splitting needed, repair the entire range as-is
                    SizedRepairAssignment assignment = new SizedRepairAssignment(estimate.tokenRange, estimate.keyspace,
                                                                                 Collections.singletonList(estimate.table),
                                                                                 "full primary range for table", estimate.sizeForRepair);
                    repairAssignments.add(assignment);
                }
            }
        }
        return repairAssignments;
    }

    private int calculateNumberOfSplits(SizeEstimate estimate)
    {
        // Calculate the number of splits needed for size and partitions
        int splitsForSize = (int) Math.ceil((double) estimate.sizeForRepair / bytesPerSubrange);
        int splitsForPartitions = (int) Math.ceil((double) estimate.partitions / partitionsPerSubrange);

        // Split the token range into subranges based on whichever (partitions, bytes) would generate the most splits.
        boolean splitBySize = splitsForSize > splitsForPartitions;
        int splits = splitBySize ? splitsForSize : splitsForPartitions;

        // calculate approximation for logging purposes
        long approximateBytesPerSplit = estimate.sizeForRepair / splits;
        long approximatePartitionsPerSplit = estimate.partitions / splits;

        logger.info("Splitting {}.{} for range {} into {} sub ranges by {} (splitsForSize={}, splitsForPartitions={}, " +
                    "approximateBytesInRange={}, approximatePartitionsInRange={}, " +
                    "approximateBytesPerSplit={}, approximatePartitionsPerSplit={})",
                    estimate.keyspace, estimate.table, estimate.tokenRange,
                    splits, splitBySize ? "size" : "partitions",
                    splitsForSize, splitsForPartitions,
                    FileUtils.stringifyFileSize(estimate.sizeForRepair), estimate.partitions,
                    FileUtils.stringifyFileSize(approximateBytesPerSplit), approximatePartitionsPerSplit
        );
        return splits;
    }

    private Collection<Range<Token>> getTokenRanges(boolean primaryRangeOnly, String keyspaceName)
    {
        // Collect all applicable token ranges
        Collection<Range<Token>> wrappedRanges;
        if (primaryRangeOnly)
        {
            wrappedRanges = StorageService.instance.getPrimaryRanges(keyspaceName);
        }
        else
        {
            wrappedRanges = StorageService.instance.getLocalRanges(keyspaceName);
        }

        // Unwrap each range as we need to account for ranges that overlap the ring
        List<Range<Token>> ranges = new ArrayList<>();
        for (Range<Token> wrappedRange : wrappedRanges)
        {
            ranges.addAll(wrappedRange.unwrap());
        }
        return ranges;
    }

    private List<SizeEstimate> getRangeSizeEstimate(AutoRepairConfig.RepairType repairType, String keyspace, String table, Collection<Range<Token>> tokenRanges)
    {
        List<SizeEstimate> sizeEstimates = new ArrayList<>();
        for (Range<Token> tokenRange : tokenRanges)
        {
            logger.debug("Calculating size estimate for {}.{} for range {}", keyspace, table, tokenRange);
            try (Refs<SSTableReader> refs = getSSTableReaderRefs(repairType, keyspace, table, tokenRange))
            {
                SizeEstimate estimate = getSizesForRangeOfSSTables(repairType, keyspace, table, tokenRange, refs);
                logger.debug("Generated size estimate {}", estimate);
                sizeEstimates.add(estimate);
            }
        }
        return sizeEstimates;
    }

    @VisibleForTesting
    static SizeEstimate getSizesForRangeOfSSTables(AutoRepairConfig.RepairType repairType, String keyspace, String table, Range<Token> tokenRange, Refs<SSTableReader> refs)
    {
        ICardinality cardinality = new HyperLogLogPlus(13, 25);
        long approxBytesInRange = 0L;
        long totalBytes = 0L;

        for (SSTableReader reader : refs)
        {
            try
            {
                if (reader.openReason == SSTableReader.OpenReason.EARLY)
                    continue;
                CompactionMetadata metadata = (CompactionMetadata) reader.descriptor.getMetadataSerializer().deserialize(reader.descriptor, MetadataType.COMPACTION);
                if (metadata != null)
                    cardinality = cardinality.merge(metadata.cardinalityEstimator);

                long sstableSize = reader.bytesOnDisk();
                totalBytes += sstableSize;
                // TODO since reading the index file anyway may be able to get more accurate ratio for partition count,
                // still better to use the cardinality estimator then the index since it wont count duplicates.
                // get the bounds of the sstable for this range using the index file but do not actually read it.
                List<AbstractBounds<PartitionPosition>> bounds = BigTableScanner.makeBounds(reader, Collections.singleton(tokenRange));

                ISSTableScanner rangeScanner = BigTableScanner.getScanner((BigTableReader) reader, Collections.singleton(tokenRange));
                // Type check scanner returned as it may be an EmptySSTableScanner if the range is not covered in the
                // SSTable, in this case we will avoid incrementing approxBytesInRange.
                if (rangeScanner instanceof BigTableScanner)
                {
                    try (BigTableScanner scanner = (BigTableScanner) rangeScanner)
                    {
                        assert bounds.size() == 1;

                        AbstractBounds<PartitionPosition> bound = bounds.get(0);
                        long startPosition = scanner.getDataPosition(bound.left);
                        long endPosition = scanner.getDataPosition(bound.right);
                        // If end position is 0 we can assume the sstable ended before that token, bound at size of file
                        if (endPosition == 0)
                        {
                            endPosition = sstableSize;
                        }

                        long approximateRangeBytesInSSTable = Math.max(0, endPosition - startPosition);
                        approxBytesInRange += Math.min(approximateRangeBytesInSSTable, sstableSize);
                    }
                }

            }
            catch (IOException | CardinalityMergeException e)
            {
                logger.error("Error calculating size estimate for {}.{} for range {} on {}", keyspace, table, tokenRange, reader, e);
            }
        }
        double ratio = approxBytesInRange / (double) totalBytes;
        // use the ratio from size to estimate the partitions in the range as well
        long partitions = (long) Math.max(1, Math.ceil(cardinality.cardinality() * ratio));
        return new SizeEstimate(repairType, keyspace, table, tokenRange, partitions, approxBytesInRange, totalBytes);
    }

    @VisibleForTesting
    static Refs<SSTableReader> getSSTableReaderRefs(AutoRepairConfig.RepairType repairType, String keyspaceName, String tableName, Range<Token> tokenRange)
    {
        final ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspaceName, tableName);

        if (cfs == null)
        {
            throw new IllegalArgumentException(String.format("Could not resolve ColumnFamilyStore from %s.%s", keyspaceName, tableName));
        }

        Refs<SSTableReader> refs = null;

        while (refs == null)
        {
            Iterable<SSTableReader> sstables = cfs.getTracker().getView().select(SSTableSet.CANONICAL);
            SSTableIntervalTree tree = SSTableIntervalTree.build(sstables);
            Range<PartitionPosition> r = Range.makeRowRange(tokenRange);
            List<SSTableReader> canonicalSSTables = View.sstablesInBounds(r.left, r.right, tree);
            if (repairType == AutoRepairConfig.RepairType.INCREMENTAL)
            {
                canonicalSSTables = canonicalSSTables.stream().filter((sstable) -> !sstable.isRepaired()).collect(Collectors.toList());
            }
            refs = Refs.tryRef(canonicalSSTables);
        }
        return refs;
    }

    @VisibleForTesting
    protected static class SizeEstimate
    {
        public final AutoRepairConfig.RepairType repairType;
        public final String keyspace;
        public final String table;
        public final Range<Token> tokenRange;
        public final long partitions;
        public final long sizeInRange;
        public final long totalSize;
        /**
         * Size to consider in the repair.  For incremental repair, we want to consider the total size
         * of the estimate as we have to factor in anticompacting the entire SSTable.
         * For full repair, just use the size containing the range.
         */
        public final long sizeForRepair;

        public SizeEstimate(AutoRepairConfig.RepairType repairType,
                            String keyspace, String table, Range<Token> tokenRange,
                            long partitions, long sizeInRange, long totalSize)
        {
            this.repairType = repairType;
            this.keyspace = keyspace;
            this.table = table;
            this.tokenRange = tokenRange;
            this.partitions = partitions;
            this.sizeInRange = sizeInRange;
            this.totalSize = totalSize;

            this.sizeForRepair = repairType == AutoRepairConfig.RepairType.INCREMENTAL ? totalSize : sizeInRange;
        }

        @Override
        public String toString()
        {
            return "SizeEstimate{" +
                   "repairType=" + repairType +
                   ", keyspace='" + keyspace + '\'' +
                   ", table='" + table + '\'' +
                   ", tokenRange=" + tokenRange +
                   ", partitions=" + partitions +
                   ", sizeInRange=" + sizeInRange +
                   ", totalSize=" + totalSize +
                   ", sizeForRepair=" + sizeForRepair +
                   '}';
        }
    }

    /**
     * Implementation of RepairAssignment that also assigns an estimation of bytes involved
     * in the repair.
     */
    @VisibleForTesting
    protected static class SizedRepairAssignment extends RepairAssignment {

        final String description;
        final long estimatedBytes;

        public SizedRepairAssignment(Range<Token> tokenRange, String keyspaceName, List<String> tableNames)
        {
            this(tokenRange, keyspaceName, tableNames, "", 0L);
        }

        public SizedRepairAssignment(Range<Token> tokenRange, String keyspaceName, List<String> tableNames,
                                     String description,
                                     long estimatedBytes)
        {
            super(tokenRange, keyspaceName, tableNames);
            this.description = description;
            this.estimatedBytes = estimatedBytes;
        }

        /**
         * @return Additional metadata about the repair assignment.
         */
        public String getDescription() {
            return description;
        }

        /**
         * Estimated bytes involved in the assignment.  Typically Derived from {@link SizeEstimate#sizeForRepair}.
         * @return estimated bytes involved in the assignment.
         */
        public long getEstimatedBytes()
        {
            return estimatedBytes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            SizedRepairAssignment that = (SizedRepairAssignment) o;
            return estimatedBytes == that.estimatedBytes && Objects.equals(description, that.description);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), description, estimatedBytes);
        }

        @Override
        public String toString()
        {
            return "SizedRepairAssignment{" +
                   "description='" + description + '\'' +
                   ", tokenRange=" + tokenRange +
                   ", keyspaceName='" + keyspaceName + '\'' +
                   ", tableNames=" + tableNames +
                   ", estimatedBytes=" + FileUtils.stringifyFileSize(estimatedBytes) +
                   '}';
        }
    }
}
