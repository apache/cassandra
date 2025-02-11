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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.split;

/**
 * In Apache Cassandra, tuning repair ranges has four main goals:
 * <ol>
 * <li>
 *     <b>Create smaller, consistent repair times</b>: Long repairs, such as those lasting 15 hours, can be problematic.
 *     If a node fails 14 hours into the repair, the entire process must be restarted. The goal is to reduce the impact
 *     of disturbances or failures. However, making the repairs too short can lead to overhead from repair orchestration
 *     becoming the main bottleneck.
 * </li>
 * <li>
 *     <b>Minimize the impact on hosts</b>: Repairs should not heavily affect the host systems. For incremental repairs,
 *     this might involve anti-compaction work. In full repairs, streaming large amounts of data—especially with wide
 *     partitions—can lead to issues with disk usage and higher compaction costs.
 * </li>
 * <li>
 *     <b>Reduce overstreaming</b>: The Merkle tree, which represents data within each partition and range,
 *     has a maximum size. If a repair covers too many partitions, the tree’s leaves represent larger data ranges.
 *     Even a small change in a leaf can trigger excessive data streaming, making the process inefficient.
 * </li>
 * <li>
 *     <b>Reduce number of repairs</b>: If there are many small tables, it's beneficial to batch these tables together
 *     under a single parent repair. This prevents the repair overhead from becoming a bottleneck, especially when
 *     dealing with hundreds of tables. Running individual repairs for each table can significantly impact performance
 *     and efficiency.
 * </li>
 * </ol>
 * To manage these issues, the strategy involves estimating the size and number of partitions within a range and
 * splitting it accordingly to bound the size of the range splits. This is established by iterating over SSTable
 * index files to estimate the amount of bytes and partitions involved in the ranges being repaired and by what
 * repair type is being invoked.
 * <p/>
 * While this splitter has a lot of tuning parameters, the expectation is that the established default configuration
 * shall be sensible for all {@link org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType}'s. The following
 * configuration parameters are offered.
 * <ul>
 *     <li>
 *         <b>bytes_per_assigment</b>: The target and maximum amount of bytes that should be included in a repair
 *         assignment. This is meant to scope the amount of work involved in a repair. For incremental repair, this
 *         involves the total number of bytes in all SSTables containing unrepaired data involving the ranges being
 *         repaired, including data that doesn't cover the range. This is to account for the amount of anticompaction
 *         that is expected. For all other repair types, this involves the amount of data covering the range being
 *         repaired.
 *     </li>
 *     <li>
 *         <b>partitions_per_assignment</b>: The maximum number of partitions that should be included in a repair
 *         assignment. This configuration exists to reduce excessive overstreaming by attempting to limit the number
 *         of partitions present in a merkle tree leaf node.
 *     </li>
 *     <li>
 *         <b>max_tables_per_assignment</b>: The maximum number of tables that can be included in a repair assignment.
 *         This aims to reduce the number of repairs, especially in cases where a large amount of tables exists for
 *         a keyspace. Note that the splitter will avoid batching tables together if they exceed the other
 *         configuration parameters such as <code>bytes_per_assignment</code> and <code>partitions_per_assignment</code>.
 *     </li>
 *     <li>
 *         <b>max_bytes_per_schedule</b>: The maximum number of bytes to cover an individual schedule. This serves
 *         as a mechanism for throttling the amount of work that can be done on each repair cycle. One may opt to
 *         reduce this value if the impact of repairs is causing too many load on the cluster, or increase it if
 *         writes outpace the amount of data being repaired. Alternatively, one may want to choose tuning down or up
 *         the <code>min_repair_interval</code>.
 *     </li>
 * </ul>
 * Given the impact of what each repair type accomplishes, different defaults are established per repair type.
 * <ul>
 *     <li>
 *         <b>full</b>:  Configured in a way that attempts to accomplish repairing all data in a schedule, with
 *         individual repairs targeting at most 200GiB of data and 1048576 partitions.
 *         <b>max_bytes_per_schedule</b> is set to a large value for full repair to attempt to repair all data per
 *         repair schedule.
 *         <ul>
 *             <li><b>bytes_per_assignment</b>: 200GiB</li>
 *             <li><b>partitions_per_assignment</b>: 1048576</li>
 *             <li><b>max_tables_per_assignment</b>: 64</li>
 *             <li><b>max_bytes_per_schedule</b>: 100TiB</li>
 *         </ul>
 *     </li>
 *     <li>
 *         <b>incremental</b>: Configured in a way that attempts to repair 50GiB of data per repair, and 100GiB per
 *         schedule. This attempts to throttle the amount of IR and anticompaction done per schedule after turning
 *         incremental on for the first time. You may want to consider increasing <code>max_bytes_per_schedule</code>
 *         more than this much data is written per <code>min_repair_interval</code>.
 *         <ul>
 *             <li><b>bytes_per_assignment</b>: 50GiB</li>
 *             <li><b>partitions_per_assignment</b>: 1048576</li>
 *             <li><b>max_tables_per_assignment</b>: 64</li>
 *             <li><b>max_bytes_per_schedule</b>: 100GiB</li>
 *         </ul>
 *     </li>
 *     <li>
 *         <b>preview_repaired</b>:  Configured in a way that attempts to accomplish previewing all data in a schedule,
 *         with previews targeting at most 200GiB of data and 1048576 partitions.
 *         <ul>
 *             <li><b>bytes_per_assignment</b>: 200GiB</li>
 *             <li><b>partitions_per_assignment</b>: 1048576</li>
 *             <li><b>max_tables_per_assignment</b>: 64</li>
 *             <li><b>max_bytes_per_schedule</b>: 100TiB</li>
 *         </ul>
 *     </li>
 * </ul>
 */
public class RepairTokenRangeSplitter implements IAutoRepairTokenRangeSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(RepairTokenRangeSplitter.class);

    // Default max bytes to 100TiB, which is much more readable than Long.MAX_VALUE
    private static final DataStorageSpec.LongBytesBound MAX_BYTES = new DataStorageSpec.LongBytesBound(100_000, DataStorageSpec.DataStorageUnit.GIBIBYTES);

    static final String BYTES_PER_ASSIGNMENT = "bytes_per_assignment";
    static final String PARTITIONS_PER_ASSIGNMENT = "partitions_per_assignment";
    static final String MAX_TABLES_PER_ASSIGNMENT = "max_tables_per_assignment";
    static final String MAX_BYTES_PER_SCHEDULE = "max_bytes_per_schedule";

    static final List<String> PARAMETERS = Arrays.asList(BYTES_PER_ASSIGNMENT, PARTITIONS_PER_ASSIGNMENT, MAX_TABLES_PER_ASSIGNMENT, MAX_BYTES_PER_SCHEDULE);

    private final AutoRepairConfig.RepairType repairType;

    private final Map<String, String> givenParameters = new HashMap<>();

    private DataStorageSpec.LongBytesBound bytesPerAssignment;
    private long partitionsPerAssignment;
    private int maxTablesPerAssignment;
    private DataStorageSpec.LongBytesBound maxBytesPerSchedule;

    /**
     * Established default for each {@link org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType}, meant to
     * choose sensible defaults for each.
     * <p>
     * Defaults if not specified for the given repair type:
     * <li>
     *     <ul><b>bytes_per_assignment</b>: 200GiB</ul>
     *     <ul><b>partitions_per_assignment</b>: 2^repair_session_max_tree_depth</ul>
     *     <ul><b>max_tables_per_assignment</b>: 64</ul>
     *     <ul><b>max_bytes_per_schedule</b>: 1000GiB</ul>
     * </li>
     * It's expected that these defaults should work well for everything except incremental, so we confine
     * bytes_per_assignment to 50GiB and max_bytes_per_schedule to 100GiB. This should strike a good balance
     * between the amount of data that will be repaired during an initial migration to incremental repair and should
     * move the entire repaired set from unrepaired to repaired at steady state, assuming not more the 100GiB of
     * data is written to a node per min_repair_interval.
     */
    private static final Map<AutoRepairConfig.RepairType, RepairTypeDefaults> DEFAULTS_BY_REPAIR_TYPE = new EnumMap<>(AutoRepairConfig.RepairType.class)
    {{
        put(AutoRepairConfig.RepairType.FULL, RepairTypeDefaults.builder(AutoRepairConfig.RepairType.FULL)
                                                                .build());
        // Restrict incremental repair to 50GB bytes per assignment to confine the amount of possible autocompaction.
        put(AutoRepairConfig.RepairType.INCREMENTAL, RepairTypeDefaults.builder(AutoRepairConfig.RepairType.INCREMENTAL)
                                                                       .withBytesPerAssignment(new DataStorageSpec.LongBytesBound("50GiB"))
                                                                       .withMaxBytesPerSchedule(new DataStorageSpec.LongBytesBound("100GiB"))
                                                                       .build());
        put(AutoRepairConfig.RepairType.PREVIEW_REPAIRED, RepairTypeDefaults.builder(AutoRepairConfig.RepairType.PREVIEW_REPAIRED)
                                                                            .build());
    }};

    public RepairTokenRangeSplitter(AutoRepairConfig.RepairType repairType, Map<String, String> parameters)
    {
        this.repairType = repairType;
        this.givenParameters.putAll(parameters);

        reinitParameters();
    }

    private void reinitParameters()
    {
        RepairTypeDefaults defaults = DEFAULTS_BY_REPAIR_TYPE.get(repairType);

        DataStorageSpec.LongBytesBound bytesPerAssignmentTmp = getPropertyOrDefault(BYTES_PER_ASSIGNMENT, DataStorageSpec.LongBytesBound::new, defaults.bytesPerAssignment);
        DataStorageSpec.LongBytesBound maxBytesPerScheduleTmp = getPropertyOrDefault(MAX_BYTES_PER_SCHEDULE, DataStorageSpec.LongBytesBound::new, defaults.maxBytesPerSchedule);

        // Validate that bytesPerAssignment <= maxBytesPerSchedule
        if (bytesPerAssignmentTmp.toBytes() > maxBytesPerScheduleTmp.toBytes())
        {
            throw new IllegalArgumentException(String.format("%s='%s' cannot be greater than %s='%s' for %s",
                                                             BYTES_PER_ASSIGNMENT,
                                                             bytesPerAssignmentTmp,
                                                             MAX_BYTES_PER_SCHEDULE,
                                                             maxBytesPerScheduleTmp,
                                                             repairType.getConfigName()));
        }

        bytesPerAssignment = bytesPerAssignmentTmp;
        maxBytesPerSchedule = maxBytesPerScheduleTmp;

        partitionsPerAssignment = getPropertyOrDefault(PARTITIONS_PER_ASSIGNMENT, Long::parseLong, defaults.partitionsPerAssignment);
        maxTablesPerAssignment = getPropertyOrDefault(MAX_TABLES_PER_ASSIGNMENT, Integer::parseInt, defaults.maxTablesPerAssignment);

        logger.info("Configured {}[{}] with {}={}, {}={}, {}={}, {}={}", RepairTokenRangeSplitter.class.getName(),
                    repairType.getConfigName(),
                    BYTES_PER_ASSIGNMENT, bytesPerAssignment,
                    PARTITIONS_PER_ASSIGNMENT, partitionsPerAssignment,
                    MAX_TABLES_PER_ASSIGNMENT, maxTablesPerAssignment,
                    MAX_BYTES_PER_SCHEDULE, maxBytesPerSchedule);
    }

    private <T> T getPropertyOrDefault(String propertyName, Function<String, T> mapper, T defaultValue)
    {
        return Optional.ofNullable(this.givenParameters.get(propertyName)).map(mapper).orElse(defaultValue);
    }

    @Override
    public Iterator<KeyspaceRepairAssignments> getRepairAssignments(boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
    {
        return new BytesBasedRepairAssignmentIterator(primaryRangeOnly, repairPlans);
    }

    /**
     * A custom {@link RepairAssignmentIterator} that confines the number of repair assignments to
     * <code>max_bytes_per_schedule</code>.
     */
    private class BytesBasedRepairAssignmentIterator extends RepairAssignmentIterator {

        private final boolean primaryRangeOnly;
        private long bytesSoFar = 0;

        BytesBasedRepairAssignmentIterator(boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
        {
            super(repairPlans);
            this.primaryRangeOnly = primaryRangeOnly;
        }

        @Override
        protected KeyspaceRepairAssignments next(int priority, KeyspaceRepairPlan repairPlan)
        {
            // short circuit if we've accumulated too many bytes by returning a KeyspaceRepairAssignments with
            // no assignments. We do this rather than returning false in hasNext() because we want to signal
            // to AutoRepair that a keyspace generated no assignments.
            if (bytesSoFar >= maxBytesPerSchedule.toBytes())
            {
                return new KeyspaceRepairAssignments(priority, repairPlan.getKeyspaceName(), Collections.emptyList());
            }

            Collection<Range<Token>> tokenRanges = getTokenRanges(primaryRangeOnly, repairPlan.getKeyspaceName());
            List<SizedRepairAssignment> repairAssignments = getRepairAssignmentsForKeyspace(repairType, repairPlan.getKeyspaceName(), repairPlan.getTableNames(), tokenRanges);
            FilteredRepairAssignments filteredRepairAssignments = filterRepairAssignments(priority, repairPlan.getKeyspaceName(), repairAssignments, bytesSoFar);
            bytesSoFar = filteredRepairAssignments.newBytesSoFar;
            return new KeyspaceRepairAssignments(priority, repairPlan.getKeyspaceName(), filteredRepairAssignments.repairAssignments);
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
                // If for whatever reason the CFS is not retrievable, we can assume it has been deleted, so give the
                // other cfs precedence.
                if (cfs1 == null)
                {
                    // cfs1 is lesser than because its null
                    return -1;
                }
                else if (cfs2 == null)
                {
                    // cfs1 is greather than because cfs2 is null
                    return 1;
                }
                return Long.compare(cfs1.metric.totalDiskSpaceUsed.getCount(), cfs2.metric.totalDiskSpaceUsed.getCount());
            });
        }

        for (String tableName : tableNames)
        {
            List<SizedRepairAssignment> tableAssignments = getRepairAssignmentsForTable(keyspaceName, tableName, tokenRanges);

            if (tableAssignments.isEmpty())
                continue;

            // if not repairing by keyspace don't attempt to batch them with others.
            if (!repairByKeyspace)
            {
                repairAssignments.addAll(tableAssignments);
            }
            // If the table assignments are for the same token range, and we have room to add more tables to the current assignment
            else if (tableAssignments.size() == 1 &&
                     currentAssignments.size() < maxTablesPerAssignment &&
                     (currentAssignments.isEmpty() || currentAssignments.get(0).getTokenRange().equals(tableAssignments.get(0).getTokenRange())))
            {
                long currentAssignmentsBytes = getEstimatedBytes(currentAssignments);
                long tableAssignmentsBytes = getEstimatedBytes(tableAssignments);
                // only add assignments together if they don't exceed max bytes per schedule.
                if (currentAssignmentsBytes + tableAssignmentsBytes < maxBytesPerSchedule.toBytes())
                {
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
     * @param repairAssignments the assignments to filter.
     * @param bytesSoFar repair assignment bytes accumulated so far.
     * @return A list of repair assignments confined by <code>maxBytesPerSchedule</code>.
     */
    @VisibleForTesting
    FilteredRepairAssignments filterRepairAssignments(int priority, String keyspaceName, List<SizedRepairAssignment> repairAssignments, long bytesSoFar)
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
            if (bytesSoFar + repairAssignment.getEstimatedBytes() > maxBytesPerSchedule.toBytes())
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

    @VisibleForTesting
    static class FilteredRepairAssignments
    {
        final List<RepairAssignment> repairAssignments;
        final long newBytesSoFar;

        private FilteredRepairAssignments(List<RepairAssignment> repairAssignments, long newBytesSoFar)
        {
            this.repairAssignments = repairAssignments;
            this.newBytesSoFar = newBytesSoFar;
        }
    }

    private String getBytesOfMaxBytesPerSchedule(long bytes)
    {
        if (maxBytesPerSchedule.equals(MAX_BYTES))
            return FileUtils.stringifyFileSize(bytes);
        else
            return String.format("%s of %s", FileUtils.stringifyFileSize(bytes), maxBytesPerSchedule);
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
    protected List<SizedRepairAssignment> getRepairAssignmentsForTable(String keyspaceName, String tableName, Collection<Range<Token>> tokenRanges)
    {
        List<SizeEstimate> sizeEstimates = getRangeSizeEstimate(keyspaceName, tableName, tokenRanges);
        return getRepairAssignments(sizeEstimates);
    }

    private static void logSkippingTable(String keyspaceName, String tableName)
    {
        logger.warn("Could not resolve table data for {}.{} assuming it has since been deleted, skipping", keyspaceName, tableName);
    }

    @VisibleForTesting
    protected List<SizedRepairAssignment> getRepairAssignments(List<SizeEstimate> sizeEstimates)
    {
        List<SizedRepairAssignment> repairAssignments = new ArrayList<>();

        // since its possible for us to hit maxBytesPerSchedule before seeing all ranges, shuffle so there is chance
        // at least of hitting all the ranges _eventually_ for the worst case scenarios
        Collections.shuffle(sizeEstimates);
        int totalExpectedSubRanges = 0;
        for (SizeEstimate estimate : sizeEstimates)
        {
            if (estimate.sizeForRepair != 0)
            {
                boolean needsSplitting = estimate.sizeForRepair > bytesPerAssignment.toBytes() || estimate.partitions > partitionsPerAssignment;
                if (needsSplitting)
                {
                    totalExpectedSubRanges += calculateNumberOfSplits(estimate);
                }
            }
        }
        for (SizeEstimate estimate : sizeEstimates)
        {
            if (estimate.sizeForRepair == 0)
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(estimate.keyspace, estimate.table);

                if (cfs == null)
                {
                    logSkippingTable(estimate.keyspace, estimate.table);
                    continue;
                }

                long memtableSize = cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                if (memtableSize > 0L)
                {
                    logger.debug("Included {}.{} range {}, had no unrepaired SSTables, but memtableSize={}, adding single repair assignment", estimate.keyspace, estimate.table, estimate.tokenRange, memtableSize);
                    SizedRepairAssignment assignment = new SizedRepairAssignment(estimate.tokenRange, estimate.keyspace, Collections.singletonList(estimate.table), "full primary rangee for table with memtable only detected", memtableSize);
                    repairAssignments.add(assignment);
                }
                else
                {
                    logger.debug("Included {}.{} range {}, has no SSTables or memtable data, but adding single repair assignment for entire range in case writes were missed", estimate.keyspace, estimate.table, estimate.tokenRange);
                    SizedRepairAssignment assignment = new SizedRepairAssignment(estimate.tokenRange, estimate.keyspace, Collections.singletonList(estimate.table), "full primary range for table with no data detected", 0L);
                    repairAssignments.add(assignment);
                }
            }
            else
            {
                // Check if the estimate needs splitting based on the criteria
                boolean needsSplitting = estimate.sizeForRepair > bytesPerAssignment.toBytes() || estimate.partitions > partitionsPerAssignment;
                if (needsSplitting)
                {
                    int numberOfSplits = calculateNumberOfSplits(estimate);
                    long approximateBytesPerSplit = estimate.sizeForRepair / numberOfSplits;
                    Collection<Range<Token>> subranges = split(estimate.tokenRange, numberOfSplits);
                    for (Range<Token> subrange : subranges)
                    {
                        SizedRepairAssignment assignment = new SizedRepairAssignment(subrange, estimate.keyspace, Collections.singletonList(estimate.table),
                                                                                     String.format("subrange %d of %d", repairAssignments.size()+1, totalExpectedSubRanges),
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
        int splitsForSize = (int) Math.ceil((double) estimate.sizeForRepair / bytesPerAssignment.toBytes());
        int splitsForPartitions = (int) Math.ceil((double) estimate.partitions / partitionsPerAssignment);

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
            wrappedRanges = TokenRingUtils.getPrimaryRangesForEndpoint(keyspaceName, FBUtilities.getBroadcastAddressAndPort());
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

    private List<SizeEstimate> getRangeSizeEstimate(String keyspace, String table, Collection<Range<Token>> tokenRanges)
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
        List<Range<Token>> singletonRange = Collections.singletonList(tokenRange);
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

                // use onDiskLength, which is the actual size of the SSTable data file.
                long sstableSize = reader.onDiskLength();
                totalBytes += sstableSize;

                // get the on disk size for the token range, note for compressed data this includes the full
                // chunks the start and end ranges are found in.
                long approximateRangeBytesInSSTable = reader.onDiskSizeForPartitionPositions(reader.getPositionsForRanges(singletonRange));
                approxBytesInRange += Math.min(approximateRangeBytesInSSTable, sstableSize);
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
            logSkippingTable(keyspaceName, tableName);
            return Refs.ref(Collections.emptyList());
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

    @Override
    public void setParameter(String key, String value)
    {
        if (!PARAMETERS.contains(key))
        {
            throw new IllegalArgumentException("Unexpected parameter '" + key + "', must be one of " + PARAMETERS);
        }

        logger.info("Setting {} to {} for repair type {}", key, value, repairType);
        givenParameters.put(key, value);
        reinitParameters();
    }

    @Override
    public Map<String, String> getParameters()
    {
        final Map<String, String> parameters = new LinkedHashMap<>();
        for (String parameter : PARAMETERS)
        {
            // Use the parameter as provided if present.
            if (givenParameters.containsKey(parameter))
            {
                parameters.put(parameter, givenParameters.get(parameter));
                continue;
            }

            switch (parameter)
            {
                case BYTES_PER_ASSIGNMENT:
                    parameters.put(parameter, bytesPerAssignment.toString());
                    continue;
                case PARTITIONS_PER_ASSIGNMENT:
                    parameters.put(parameter, Long.toString(partitionsPerAssignment));
                    continue;
                case MAX_TABLES_PER_ASSIGNMENT:
                    parameters.put(parameter, Integer.toString(maxTablesPerAssignment));
                    continue;
                case MAX_BYTES_PER_SCHEDULE:
                    parameters.put(parameter, maxBytesPerSchedule.toString());
                    continue;
                default:
                    // not expected
                    parameters.put(parameter, "");
            }
        }
        return Collections.unmodifiableMap(parameters);
    }

    /**
     * Represents a size estimate by both bytes and partition count for a given keyspace and table for a token range.
     */
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
         * Size to consider in the repair. For incremental repair, we want to consider the total size
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
        public String getDescription()
        {
            return description;
        }

        /**
         * Estimated bytes involved in the assignment. Typically Derived from {@link SizeEstimate#sizeForRepair}.
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

    /**
     * Conveinence builder for establishing defaults by repair type.
     */
    protected static class RepairTypeDefaults
    {
        final AutoRepairConfig.RepairType repairType;
        final DataStorageSpec.LongBytesBound bytesPerAssignment;
        final long partitionsPerAssignment;
        final int maxTablesPerAssignment;
        final DataStorageSpec.LongBytesBound maxBytesPerSchedule;

        public RepairTypeDefaults(AutoRepairConfig.RepairType repairType,
                                  DataStorageSpec.LongBytesBound bytesPerAssignment,
                                  long partitionsPerAssignment,
                                  int maxTablesPerAssignment,
                                  DataStorageSpec.LongBytesBound maxBytesPerSchedule)
        {
            this.repairType = repairType;
            this.bytesPerAssignment = bytesPerAssignment;
            this.partitionsPerAssignment = partitionsPerAssignment;
            this.maxTablesPerAssignment = maxTablesPerAssignment;
            this.maxBytesPerSchedule = maxBytesPerSchedule;
        }

        static RepairTypeDefaultsBuilder builder(AutoRepairConfig.RepairType repairType)
        {
            return new RepairTypeDefaultsBuilder(repairType);
        }

        static class RepairTypeDefaultsBuilder
        {
            private final AutoRepairConfig.RepairType repairType;
            private DataStorageSpec.LongBytesBound bytesPerAssignment = new DataStorageSpec.LongBytesBound("200GiB");
            // Aims to target at most 1 partitons per leaf assuming a merkle tree of depth 20  (2^20 = 1,048,576)
            private long partitionsPerAssignment = 1_048_576;
            private int maxTablesPerAssignment = 64;
            private DataStorageSpec.LongBytesBound maxBytesPerSchedule = MAX_BYTES;

            private RepairTypeDefaultsBuilder(AutoRepairConfig.RepairType repairType)
            {
                this.repairType = repairType;
            }

            public RepairTypeDefaultsBuilder withBytesPerAssignment(DataStorageSpec.LongBytesBound bytesPerAssignment)
            {
                this.bytesPerAssignment = bytesPerAssignment;
                return this;
            }

            @SuppressWarnings("unused")
            public RepairTypeDefaultsBuilder withPartitionsPerAssignment(long partitionsPerAssignment)
            {
                this.partitionsPerAssignment = partitionsPerAssignment;
                return this;
            }

            @SuppressWarnings("unused")
            public RepairTypeDefaultsBuilder withMaxTablesPerAssignment(int maxTablesPerAssignment)
            {
                this.maxTablesPerAssignment = maxTablesPerAssignment;
                return this;
            }

            public RepairTypeDefaultsBuilder withMaxBytesPerSchedule(DataStorageSpec.LongBytesBound maxBytesPerSchedule)
            {
                this.maxBytesPerSchedule = maxBytesPerSchedule;
                return this;
            }

            public RepairTokenRangeSplitter.RepairTypeDefaults build()
            {
                return new RepairTypeDefaults(repairType, bytesPerAssignment, partitionsPerAssignment, maxTablesPerAssignment, maxBytesPerSchedule);
            }
        }
    }
}
