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
import java.util.List;
import java.util.Map;
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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableScanner;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.FileUtils;
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

    private static final DataStorageSpec.LongBytesBound DEFAULT_SUBRANGE_SIZE = new DataStorageSpec.LongBytesBound("100GiB");
    private static final long DEFAULT_MAX_BYTES_PER_SCHEDULE = Long.MAX_VALUE;
    private static final long DEFAULT_PARTITION_LIMIT = (long) Math.pow(2, DatabaseDescriptor.getRepairSessionMaxTreeDepth());
    private static final int DEFAULT_TABLE_BATCH_LIMIT = 64;

    public RepairRangeSplitter(Map<String, String> parameters)
    {
        DataStorageSpec.LongBytesBound subrangeSize;
        if (parameters.containsKey(SUBRANGE_SIZE))
        {
            subrangeSize = new DataStorageSpec.LongBytesBound(parameters.get(SUBRANGE_SIZE));
        }
        else
        {
            subrangeSize = DEFAULT_SUBRANGE_SIZE;
        }
        bytesPerSubrange = subrangeSize.toBytes();

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
            throw new RuntimeException(String.format("bytesPerSubrange '%s' cannot be greater than maxBytesPerSchedule '%s'",
                                                     FileUtils.stringifyFileSize(bytesPerSubrange), FileUtils.stringifyFileSize(maxBytesPerSchedule)));
        }

        logger.info("Configured {} with {}={}, {}={}, {}={}, {}={}", RepairRangeSplitter.class.getName(),
                    SUBRANGE_SIZE, subrangeSize, MAX_BYTES_PER_SCHEDULE, maxBytesPerSchedule,
                    PARTITION_COUNT, partitionsPerSubrange, TABLE_BATCH_LIMIT, tablesPerAssignmentLimit);
    }

    @Override
    public List<RepairAssignment> getRepairAssignments(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, String keyspaceName, List<String> tableNames)
    {
        logger.debug("Calculating token range splits for repairType={} primaryRangeOnly={} keyspaceName={} tableNames={}", repairType, primaryRangeOnly, keyspaceName, tableNames);
        Collection<Range<Token>> tokenRanges = getTokenRanges(primaryRangeOnly, keyspaceName);
        return getRepairAssignments(repairType, keyspaceName, tableNames, tokenRanges);
    }

    @VisibleForTesting
    List<RepairAssignment> getRepairAssignments(AutoRepairConfig.RepairType repairType, String keyspaceName, List<String> tableNames, Collection<Range<Token>> tokenRanges)
    {
        List<SizedRepairAssignment> repairAssignments = new ArrayList<>();
        // this is used for batching minimal single assignment tables together
        List<SizedRepairAssignment> currentAssignments = new ArrayList<>();

        // sort the tables by size so can batch the smallest ones together
        tableNames.sort((t1, t2) -> {
            ColumnFamilyStore cfs1 = ColumnFamilyStore.getIfExists(keyspaceName, t1);
            ColumnFamilyStore cfs2 = ColumnFamilyStore.getIfExists(keyspaceName, t2);
            if (cfs1 == null || cfs2 == null)
                throw new IllegalArgumentException(String.format("Could not resolve ColumnFamilyStore from %s.%s", keyspaceName, t1));
            return Long.compare(cfs1.metric.totalDiskSpaceUsed.getCount(), cfs2.metric.totalDiskSpaceUsed.getCount());
        });

        for (String tableName : tableNames)
        {
            List<SizedRepairAssignment> tableAssignments = getRepairAssignmentsForTable(repairType, keyspaceName, tableName, tokenRanges);

            if (tableAssignments.isEmpty())
                continue;

            // If the table assignments are for the same token range, and we have room to add more tables to the current assignment
            if (tableAssignments.size() == 1 &&
                currentAssignments.size() < tablesPerAssignmentLimit &&
                (currentAssignments.isEmpty() || currentAssignments.get(0).getTokenRange().equals(tableAssignments.get(0).getTokenRange())))
            {
                long currentAssignmentsBytes = getBytesInAssignments(currentAssignments);
                long tableAssignmentsBytes = getBytesInAssignments(tableAssignments);
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

        return filterRepairAssignments(repairType, repairAssignments);
    }

    /**
     * Given a repair type and list of sized-based repair assignments order them by priority of the
     * assignments' underlying tables and confine them by <code>maxBytesPerSchedule</code>.
     * @param repairType used to determine underyling table priorities
     * @param repairAssignments the assignments to filter.
     * @return A list of repair assignments ordered by priority and confined by <code>maxBytesPerSchedule</code>.
     */
    protected List<RepairAssignment> filterRepairAssignments(AutoRepairConfig.RepairType repairType, List<SizedRepairAssignment> repairAssignments)
    {
        // Reorder the repair assignments
        reorderByPriority(repairAssignments, repairType);

        // Confine repair assignemnts by maxBytesPer Schedule if greater than maxBytesPerSchedule.
        long assignmentBytes = getBytesInAssignments(repairAssignments);
        if (assignmentBytes < maxBytesPerSchedule)
            return repairAssignments.stream().map((a) -> (RepairAssignment)a).collect(Collectors.toList());
        else
        {
            long bytesSoFar = 0L;
            List<RepairAssignment> assignmentsToReturn = new ArrayList<>(repairAssignments.size());
            for (SizedRepairAssignment repairAssignment : repairAssignments) {
                // skip any repair assignments that would accumulate us past the maxBytesPerSchedule
                if (bytesSoFar + repairAssignment.getEstimatedSizeInBytes() > maxBytesPerSchedule)
                {
                    // log that repair assignment was skipped.
                    warnMaxBytesPerSchedule(repairType, repairAssignment);
                }
                else
                {
                    assignmentsToReturn.add(repairAssignment);
                    bytesSoFar += repairAssignment.getEstimatedSizeInBytes();
                }
            }
            return assignmentsToReturn;
        }
    }

    @VisibleForTesting
    protected static long getBytesInAssignments(List<SizedRepairAssignment> repairAssignments) {
        return repairAssignments
               .stream()
               .mapToLong(SizedRepairAssignment::getEstimatedSizeInBytes).sum();
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

        long sizeForAssignment = getBytesInAssignments(assignments);
        return new SizedRepairAssignment(referenceTokenRange, referenceKeyspaceName, new ArrayList<>(mergedTableNames), sizeForAssignment);
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
                    SizedRepairAssignment assignment = new SizedRepairAssignment(estimate.tokenRange, estimate.keyspace, Collections.singletonList(estimate.table), estimate.sizeInRange);
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
                        logger.info("Added repair assignment for {}.{} for subrange {} (#{}/{})",
                                    estimate.keyspace, estimate.table, subrange, repairAssignments.size() + 1, numberOfSplits);
                        SizedRepairAssignment assignment = new SizedRepairAssignment(subrange, estimate.keyspace, Collections.singletonList(estimate.table), approximateBytesPerSplit);
                        repairAssignments.add(assignment);
                    }
                }
                else
                {
                    logger.info("Using 1 repair assignment for {}.{} for range {} as rangeBytes={} is less than {}={} and partitionEstimate={} is less than {}={}",
                                estimate.keyspace, estimate.table, estimate.tokenRange,
                                FileUtils.stringifyFileSize(estimate.sizeForRepair),
                                SUBRANGE_SIZE, FileUtils.stringifyFileSize(bytesPerSubrange),
                                estimate.partitions,
                                PARTITION_COUNT, partitionsPerSubrange);
                    // No splitting needed, repair the entire range as-is
                    SizedRepairAssignment assignment = new SizedRepairAssignment(estimate.tokenRange, estimate.keyspace, Collections.singletonList(estimate.table), estimate.sizeForRepair);
                    repairAssignments.add(assignment);
                }
            }
        }
        return repairAssignments;
    }

    private void warnMaxBytesPerSchedule(AutoRepairConfig.RepairType repairType, SizedRepairAssignment repairAssignment)
    {
        String warning = "Refusing to add repair assignment of size {} for {}.{} because it would increase total repair bytes to a value greater than {}";
        if (repairType == AutoRepairConfig.RepairType.FULL)
        {
            warning = warning + ", everything will not be repaired this schedule. Consider increasing maxBytesPerSchedule, reducing node density or monitoring to ensure all ranges do get repaired within gc_grace_seconds";
        }
        logger.warn(warning, repairAssignment.getEstimatedSizeInBytes(), repairAssignment.keyspaceName, repairAssignment.tableNames, FileUtils.stringifyFileSize(maxBytesPerSchedule));
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

    public Collection<Range<Token>> getTokenRanges(boolean primaryRangeOnly, String keyspaceName)
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
                logger.debug("Size estimate for {}.{} for range {} is {}", keyspace, table, tokenRange, estimate);
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
                try (BigTableScanner scanner = (BigTableScanner) BigTableScanner.getScanner((BigTableReader) reader, Collections.singleton(tokenRange)))
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
    }

    @VisibleForTesting
    protected static class SizedRepairAssignment extends RepairAssignment {

        final long estimatedSizedInBytes;

        public SizedRepairAssignment(Range<Token> tokenRange, String keyspaceName, List<String> tableNames, long estimatedSizeInBytes)
        {
            super(tokenRange, keyspaceName, tableNames);
            this.estimatedSizedInBytes = estimatedSizeInBytes;
        }

        public long getEstimatedSizeInBytes() {
            return estimatedSizedInBytes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            SizedRepairAssignment that = (SizedRepairAssignment) o;
            return estimatedSizedInBytes == that.estimatedSizedInBytes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), estimatedSizedInBytes);
        }

        @Override
        public String toString()
        {
            return "SizedRepairAssignement{" +
                   "estimatedSizedInBytes=" + FileUtils.stringifyFileSize(estimatedSizedInBytes) +
                   ", keyspaceName='" + keyspaceName + '\'' +
                   ", tokenRange=" + tokenRange +
                   ", tableNames=" + tableNames +
                   '}';
        }

    }
}
