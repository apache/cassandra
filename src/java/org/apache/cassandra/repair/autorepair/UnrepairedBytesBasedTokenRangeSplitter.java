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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
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
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.splitEvenly;

public class UnrepairedBytesBasedTokenRangeSplitter implements IAutoRepairTokenRangeSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(UnrepairedBytesBasedTokenRangeSplitter.class);

    static final String SUBRANGE_SIZE = "subrange_size";
    static final String MAX_BYTES_PER_SCHEDULE = "max_bytes_per_schedule";

    // target bytes per subrange
    private final DataStorageSpec.LongBytesBound subrangeSize;

    // maximum target bytes to repair
    private final DataStorageSpec.LongBytesBound maxBytesPerSchedule;

    private final long subrangeBytes;

    private final long maxBytesPerScheduleBytes;

    private static final DataStorageSpec.LongBytesBound DEFAULT_SUBRANGE_SIZE = new DataStorageSpec.LongBytesBound("100GiB");
    private static final DataStorageSpec.LongBytesBound DEFAULT_MAX_BYTES_PER_SCHEDULE = new DataStorageSpec.LongBytesBound("500GiB");

    public UnrepairedBytesBasedTokenRangeSplitter(Map<String, String> parameters)
    {
        // Demonstrates parameterizing a range splitter so we can have splitter specific options.
        if (parameters.containsKey(SUBRANGE_SIZE))
        {
            subrangeSize = new DataStorageSpec.LongBytesBound(parameters.get(SUBRANGE_SIZE));
        }
        else
        {
            subrangeSize = DEFAULT_SUBRANGE_SIZE;
        }
        subrangeBytes = subrangeSize.toBytes();

        if (parameters.containsKey(MAX_BYTES_PER_SCHEDULE))
        {
            maxBytesPerSchedule = new DataStorageSpec.LongBytesBound(parameters.get(MAX_BYTES_PER_SCHEDULE));
        }
        else
        {
            maxBytesPerSchedule = DEFAULT_MAX_BYTES_PER_SCHEDULE;
        }
        maxBytesPerScheduleBytes = maxBytesPerSchedule.toBytes();

        logger.info("Configured {} with {}={}, {}={}", UnrepairedBytesBasedTokenRangeSplitter.class.getName(),
                    SUBRANGE_SIZE, subrangeSize, MAX_BYTES_PER_SCHEDULE, maxBytesPerSchedule);
    }

    @Override
    public List<RepairAssignment> getRepairAssignments(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, String keyspaceName, List<String> tableNames)
    {
        List<RepairAssignment> repairAssignments = new ArrayList<>();

        logger.info("Calculating token range splits for repairType={} primaryRangeOnly={} keyspaceName={} tableNames={}", repairType, primaryRangeOnly, keyspaceName, tableNames);
        if (repairType != AutoRepairConfig.RepairType.incremental)
        {
            throw new IllegalArgumentException(this.getClass().getName() + " only supports " + AutoRepairConfig.RepairType.incremental + " repair");
        }

        // TODO: create a custom repair assignment that indicates number of bytes in repair and join tables by byte size.
        Collection<Range<Token>> tokenRanges = getTokenRanges(primaryRangeOnly, keyspaceName);
        for (String tableName : tableNames)
        {
            repairAssignments.addAll(getRepairAssignmentsForTable(keyspaceName, tableName, tokenRanges));
        }
        return repairAssignments;
    }

    public List<RepairAssignment> getRepairAssignmentsForTable(String keyspaceName, String tableName, Collection<Range<Token>> tokenRanges)
    {
        List<RepairAssignment> repairAssignments = new ArrayList<>();

        long targetBytesSoFar = 0;

        for (Range<Token> tokenRange : tokenRanges)
        {
            logger.info("Calculating unrepaired bytes for {}.{} for range {}", keyspaceName, tableName, tokenRange);
            // Capture the amount of unrepaired bytes for range
            long approximateUnrepairedBytesForRange = 0L;
            // Capture the total bytes in read sstables, this will be useful for calculating the ratio
            // of data in SSTables including this range and also useful to know how much anticompaction there will be.
            long totalBytesInUnrepairedSSTables = 0L;
            try (Refs<SSTableReader> refs = getSSTableReaderRefs(keyspaceName, tableName, tokenRange))
            {
                for (SSTableReader reader : refs)
                {
                    // Only evaluate unrepaired SSTables.
                    if (!reader.isRepaired())
                    {
                        long sstableSize = reader.bytesOnDisk();
                        totalBytesInUnrepairedSSTables += sstableSize;
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
                            // get the fraction of the sstable belonging to the range.
                            approximateUnrepairedBytesForRange += Math.min(approximateRangeBytesInSSTable, sstableSize);
                            double ratio = approximateRangeBytesInSSTable / (double) sstableSize;
                            logger.info("Calculations for {}.{} {}: sstableSize={}, rangeBytesInSSTable={}, startPosition={}, endPosition={}, ratio={}",
                                        keyspaceName, tableName, reader.descriptor.baseFile().name(),
                                        FileUtils.stringifyFileSize(sstableSize), FileUtils.stringifyFileSize(approximateRangeBytesInSSTable), startPosition, endPosition, ratio);
                        }
                    }
                    else
                    {
                        logger.info("Skipping over {}.{} {} ({}) because it is repaired", keyspaceName, tableName, reader.descriptor.baseFile().name(), FileUtils.stringifyFileSize(reader.bytesOnDisk()));
                    }
                }
            }

            // Only consider token range if it had unrepaired sstables or live data in memtables.
            if (totalBytesInUnrepairedSSTables > 0L)
            {
                // TODO: Possibly some anticompaction configuration we want here, where if we detect a large amount of anticompaction we want to reduce the work we do.
                double ratio = approximateUnrepairedBytesForRange / (double) totalBytesInUnrepairedSSTables;
                logger.info("Calculated unrepaired bytes for {}.{} for range {}: sstableSize={}, rangeBytesInSSTables={}, ratio={}", keyspaceName, tableName, tokenRange,
                            FileUtils.stringifyFileSize(totalBytesInUnrepairedSSTables), FileUtils.stringifyFileSize(approximateUnrepairedBytesForRange), ratio);

                // TODO: split on byte size here, this is currently a bit naive in assuming that data is evenly distributed among the range which may not be the
                // right assumption.  May want to consider when splitting on these ranges to reevaluate how much data is in the range, but for this
                // exists as a demonstration.
                if (approximateUnrepairedBytesForRange < subrangeBytes)
                {
                    // accept range as is if less than bytes.
                    logger.info("Using 1 repair assignment for {}.{} for range {} as {} is less than {}", keyspaceName, tableName, tokenRange,
                                FileUtils.stringifyFileSize(approximateUnrepairedBytesForRange), subrangeSize);
                    // TODO: this is a bit repetitive see if can reduce more.
                    RepairAssignment assignment = new BytesBasedRepairAssignment(tokenRange, keyspaceName, Collections.singletonList(tableName), approximateUnrepairedBytesForRange);
                    if (canAddAssignment(assignment, targetBytesSoFar, approximateUnrepairedBytesForRange))
                    {
                        repairAssignments.add(assignment);
                        targetBytesSoFar += approximateUnrepairedBytesForRange;
                    }
                    else
                        return repairAssignments;
                }
                else
                {
                    long targetRanges = approximateUnrepairedBytesForRange / subrangeBytes;
                    // TODO: approximation per range, this is a bit lossy since targetRanges rounds down.
                    long approximateBytesPerSplit = approximateUnrepairedBytesForRange / targetRanges;
                    logger.info("Splitting {}.{} for range {} into {} sub ranges, approximateBytesPerSplit={}", keyspaceName, tableName, tokenRange, targetRanges, FileUtils.stringifyFileSize(approximateBytesPerSplit));
                    List<Range<Token>> splitRanges = splitEvenly(tokenRange, (int) targetRanges);
                    int splitRangeCount = 0;
                    for (Range<Token> splitRange : splitRanges)
                    {
                        RepairAssignment assignment = new BytesBasedRepairAssignment(splitRange, keyspaceName, Collections.singletonList(tableName), approximateBytesPerSplit);
                        if (canAddAssignment(assignment, targetBytesSoFar, approximateBytesPerSplit))
                        {
                            logger.info("Added repair assignment for {}.{} for subrange {} (#{}/{}) with approximateBytes={}",
                                        keyspaceName, tableName, splitRange, ++splitRangeCount, splitRanges.size(), FileUtils.stringifyFileSize(approximateBytesPerSplit));
                            repairAssignments.add(assignment);
                            targetBytesSoFar += approximateBytesPerSplit;
                        }
                        else
                            return repairAssignments;
                    }
                }
            }
            else
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspaceName, tableName);
                long memtableSize = cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                if (memtableSize > 0L)
                {
                    logger.info("Included {}.{} range {}, had no unrepaired SSTables, but memtableSize={}, adding single repair assignment", keyspaceName, tableName, tokenRange, memtableSize);
                    RepairAssignment assignment = new BytesBasedRepairAssignment(tokenRange, keyspaceName, Collections.singletonList(tableName), memtableSize);
                    if (targetBytesSoFar >= maxBytesPerScheduleBytes)
                    {
                        return repairAssignments;
                    }
                    repairAssignments.add(assignment);
                    targetBytesSoFar += memtableSize;
                }
                else
                {
                    logger.info("Skipping {}.{} for range {} because it had no unrepaired SSTables and no memtable data", keyspaceName, tableName, tokenRange);
                }
            }
        }
        return repairAssignments;
    }

    private boolean canAddAssignment(RepairAssignment repairAssignment, long targetBytesSoFar, long bytesToBeAdded)
    {
        if (targetBytesSoFar + bytesToBeAdded < maxBytesPerScheduleBytes)
        {
            return true;
        }
        logger.warn("Refusing to add {} with a target size of {} because it would increase total repair bytes to {} which is greater than {}={}",
                    repairAssignment, FileUtils.stringifyFileSize(bytesToBeAdded), FileUtils.stringifyFileSize(targetBytesSoFar + bytesToBeAdded), MAX_BYTES_PER_SCHEDULE, maxBytesPerSchedule);
        return false;
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
        Collection<Range<Token>> ranges = new ArrayList<>();
        for (Range<Token> wrappedRange : wrappedRanges)
        {
            ranges.addAll(wrappedRange.unwrap());
        }

        return ranges;
    }

    public Refs<SSTableReader> getSSTableReaderRefs(String keyspaceName, String tableName, Range<Token> tokenRange)
    {
        final ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspaceName, tableName);

        if (cfs == null)
        {
            throw new IllegalArgumentException(String.format("Could not resolve ColumnFamilyStore from %s.%s", keyspaceName, tableName));
        }

        Iterable<SSTableReader> sstables = cfs.getTracker().getView().select(SSTableSet.CANONICAL);
        SSTableIntervalTree tree = SSTableIntervalTree.build(sstables);
        Range<PartitionPosition> r = Range.makeRowRange(tokenRange);
        Iterable<SSTableReader> canonicalSSTables = View.sstablesInBounds(r.left, r.right, tree);

        // TODO: may need to reason about this not working.
        return Refs.ref(canonicalSSTables);
    }

    public static class BytesBasedRepairAssignment extends RepairAssignment
    {
        private final long approximateBytes;

        public BytesBasedRepairAssignment(Range<Token> tokenRange, String keyspaceName, List<String> tableNames, long approximateBytes)
        {
            super(tokenRange, keyspaceName, tableNames);
            this.approximateBytes = approximateBytes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            BytesBasedRepairAssignment that = (BytesBasedRepairAssignment) o;
            return approximateBytes == that.approximateBytes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), approximateBytes);
        }

        @Override
        public String toString()
        {
            return "BytesBasedRepairAssignment{" +
                   "keyspaceName='" + keyspaceName + '\'' +
                   ", approximateBytes=" + approximateBytes +
                   ", tokenRange=" + tokenRange +
                   ", tableNames=" + tableNames +
                   '}';
        }

        public long getApproximateBytes()
        {
            return approximateBytes;
        }
    }
}
