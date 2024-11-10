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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    private final long maxBytesPerScheduleBytes;
    private final long bytesPerSubrange;
    private final long partitionsPerSubrange;

    private static final DataStorageSpec.LongBytesBound DEFAULT_SUBRANGE_SIZE = new DataStorageSpec.LongBytesBound("100GiB");
    private static final long DEFAULT_MAX_BYTES_PER_SCHEDULE = Long.MAX_VALUE;
    private static final long DEFAULT_PARTITION_LIMIT = (long) Math.pow(2, DatabaseDescriptor.getRepairSessionMaxTreeDepth());
    private static final int DEFAULT_TABLE_BATCH_LIMIT = 64;

    public RepairRangeSplitter(Map<String, String> parameters)
    {
        // Demonstrates parameterizing a range splitter so we can have splitter specific options.
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
            maxBytesPerScheduleBytes = new DataStorageSpec.LongBytesBound(parameters.get(MAX_BYTES_PER_SCHEDULE)).toBytes();
        }
        else
        {
            maxBytesPerScheduleBytes = DEFAULT_MAX_BYTES_PER_SCHEDULE;
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

        logger.info("Configured {} with {}={}, {}={}, {}={}, {}={}", RepairRangeSplitter.class.getName(),
                    SUBRANGE_SIZE, subrangeSize, MAX_BYTES_PER_SCHEDULE, maxBytesPerScheduleBytes,
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
        List<RepairAssignment> repairAssignments = new ArrayList<>();
        // this is used for batching minimal single assignment tables together
        List<RepairAssignment> currentAssignments = new ArrayList<>();

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
            List<RepairAssignment> tableAssignments = getRepairAssignmentsForTable(repairType, keyspaceName, tableName, tokenRanges);

            if (tableAssignments.isEmpty())
                continue;

            // If the table assignments are for the same token range and we have room to add more tables to the current assignment
            if (tableAssignments.size() == 1 && currentAssignments.size() < tablesPerAssignmentLimit &&
                (currentAssignments.isEmpty() || currentAssignments.get(0).getTokenRange().equals(tableAssignments.get(0).getTokenRange())))
            {
                currentAssignments.addAll(tableAssignments);
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

        reorderByPriority(repairAssignments, repairType);
        return repairAssignments;
    }

    @VisibleForTesting
    static RepairAssignment merge(List<RepairAssignment> assignments)
    {
        if (assignments.isEmpty())
            throw new IllegalStateException("Cannot merge empty assignments");

        Set<String> mergedTableNames = new HashSet<>();
        Range<Token> referenceTokenRange = assignments.get(0).getTokenRange();
        String referenceKeyspaceName = assignments.get(0).getKeyspaceName();

        for (RepairAssignment assignment : assignments)
        {
            // These checks _should_ be unnecessary but are here to ensure that the assignments are consistent
            if (!assignment.getTokenRange().equals(referenceTokenRange))
                throw new IllegalStateException("All assignments must have the same token range");
            if (!assignment.getKeyspaceName().equals(referenceKeyspaceName))
                throw new IllegalStateException("All assignments must have the same keyspace name");

            mergedTableNames.addAll(assignment.getTableNames());
        }

        return new RepairAssignment(referenceTokenRange, referenceKeyspaceName, new ArrayList<>(mergedTableNames));
    }

    public List<RepairAssignment> getRepairAssignmentsForTable(AutoRepairConfig.RepairType repairType, String keyspaceName, String tableName, Collection<Range<Token>> tokenRanges)
    {
        List<RepairAssignment> repairAssignments = new ArrayList<>();

        long targetBytesSoFar = 0;

        List<SizeEstimate> sizeEstimates = getRangeSizeEstimate(repairType, keyspaceName, tableName, tokenRanges);
        // since its possible for us to hit maxBytesPerScheduleBytes before seeing all ranges, shuffle so there is chance
        // at least of hitting all the ranges _eventually_ for the worst case scenarios
        Collections.shuffle(sizeEstimates);
        for (SizeEstimate estimate : sizeEstimates)
        {
            if (estimate.sizeInRange == 0)
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspaceName, tableName);
                long memtableSize = cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                if (memtableSize > 0L)
                {
                    logger.debug("Included {}.{} range {}, had no unrepaired SSTables, but memtableSize={}, adding single repair assignment", keyspaceName, tableName, estimate.tokenRange, memtableSize);
                    RepairAssignment assignment = new RepairAssignment(estimate.tokenRange, keyspaceName, Collections.singletonList(tableName));
                    repairAssignments.add(assignment);
                }
                else
                {
                    logger.debug("Skipping {}.{} for range {} because it had no unrepaired SSTables and no memtable data", keyspaceName, tableName, estimate.tokenRange);
                }
            }
            else if (targetBytesSoFar + estimate.sizeInRange < maxBytesPerScheduleBytes)
            {
                targetBytesSoFar += estimate.sizeInRange;
                // Check if the estimate needs splitting based on the criteria
                boolean needsSplitting = estimate.sizeInRange > bytesPerSubrange || estimate.partitions > partitionsPerSubrange;

                if (needsSplitting)
                {
                    // Calculate the number of splits needed for size and partitions
                    int splitsForSize = (int) Math.ceil((double) estimate.sizeInRange / bytesPerSubrange);
                    int splitsForPartitions = (int) Math.ceil((double) estimate.partitions / partitionsPerSubrange);

                    // Choose the larger of the two as the number of splits
                    int numberOfSplits = Math.max(splitsForSize, splitsForPartitions);

                    // Split the token range into subranges
                    Collection<Range<Token>> subranges = split(estimate.tokenRange, numberOfSplits);
                    for (Range<Token> subrange : subranges)
                    {
                        RepairAssignment assignment = new RepairAssignment(subrange, keyspaceName, Collections.singletonList(tableName));
                        repairAssignments.add(assignment);
                    }
                }
                else
                {
                    // No splitting needed, repair the entire range as-is
                    RepairAssignment assignment = new RepairAssignment(estimate.tokenRange, keyspaceName, Collections.singletonList(tableName));
                    repairAssignments.add(assignment);
                }
            }
            else
            {
                // Really this is "Ok" but it does mean we are relying on randomness to cover the other ranges
                logger.info("Skipping range {} for {}.{} as it would exceed maxBytesPerScheduleBytes, consider increasing maxBytesPerScheduleBytes, reducing node density or monitoring to ensure all ranges do get repaired within gc_grace_seconds", estimate.tokenRange, keyspaceName, tableName);
            }
        }
        return repairAssignments;
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
                SizeEstimate estimate = getSizesForRangeOfSSTables(keyspace, table, tokenRange, refs);
                logger.debug("Size estimate for {}.{} for range {} is {}", keyspace, table, tokenRange, estimate);
                sizeEstimates.add(estimate);
            }
        }
        return sizeEstimates;
    }

    @VisibleForTesting
    static SizeEstimate getSizesForRangeOfSSTables(String keyspace, String table, Range<Token> tokenRange, Refs<SSTableReader> refs)
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
        return new SizeEstimate(keyspace, table, tokenRange, partitions, approxBytesInRange, totalBytes);
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
                canonicalSSTables = canonicalSSTables.stream().filter(SSTableReader::isRepaired).collect(Collectors.toList());
            }
            refs = Refs.tryRef(canonicalSSTables);
        }
        return refs;
    }

    @VisibleForTesting
    protected static class SizeEstimate
    {
        public final String keyspace;
        public final String table;
        public final Range<Token> tokenRange;
        public final long partitions;
        public final long sizeInRange;
        public final long totalSize;

        public SizeEstimate(String keyspace, String table, Range<Token> tokenRange, long partitions, long sizeInRange, long totalSize)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.tokenRange = tokenRange;
            this.partitions = partitions;
            this.sizeInRange = sizeInRange;
            this.totalSize = totalSize;
        }
    }
}
