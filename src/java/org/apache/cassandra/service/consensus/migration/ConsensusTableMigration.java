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

package org.apache.cassandra.service.consensus.migration;

import java.util.*;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.transformations.BeginConsensusMigrationForTableAndRange;
import org.apache.cassandra.tcm.transformations.MaybeFinishConsensusMigrationForTableAndRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.utils.CollectionSerializers.newListSerializer;

/**
 * Track and update the migration state of individual table and ranges within those tables
 */
public abstract class ConsensusTableMigration
{
    private static final Logger logger = LoggerFactory.getLogger(ConsensusTableMigration.class);

    public static final MetadataSerializer<List<Range<Token>>> rangesSerializer = newListSerializer(Range.serializer);

    public static final FutureCallback<RepairResult> completedRepairJobHandler = new FutureCallback<RepairResult>()
    {
        @Override
        public void onSuccess(@Nullable RepairResult repairResult)
        {
            checkNotNull(repairResult, "repairResult should not be null");
            ConsensusMigrationRepairResult migrationResult = repairResult.consensusMigrationRepairResult;

            // Need to repair both Paxos and base table state
            // Could track them separately, but doesn't seem worth the effort
            if (migrationResult.type == ConsensusMigrationRepairType.ineligible)
                return;

            RepairJobDesc desc = repairResult.desc;
            TableMetadata tm = Schema.instance.getTableMetadata(desc.keyspace, desc.columnFamily);
            if (tm == null)
                return;
            TableMigrationState tms = ClusterMetadata.current().consensusMigrationState.tableStates.get(tm.id);
            if (tms == null || !Range.intersects(tms.migratingRanges, desc.ranges))
                return;

            if (!tms.targetProtocol.isMigratedBy(repairResult.consensusMigrationRepairResult.type))
                return;

            ClusterMetadataService.instance().commit(
                new MaybeFinishConsensusMigrationForTableAndRange(
                    desc.keyspace, desc.columnFamily, ImmutableList.copyOf(desc.ranges),
                    migrationResult.minEpoch, migrationResult.type));
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            // Only successes drive forward progress
        }
    };

    private ConsensusTableMigration() {}

    public static @Nullable TableMigrationState getTableMigrationState(TableId tableId)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        return cm.consensusMigrationState.tableStates.get(tableId);
    }
    // Used by callers to avoid looking up the TMS multiple times
    public static @Nullable TableMigrationState getTableMigrationState(long epoch, TableId tableId)
    {
        ClusterMetadata cm = ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(epoch));
        return cm.consensusMigrationState.tableStates.get(tableId);
    }

    public static void startMigrationToConsensusProtocol(@Nonnull String targetProtocolName,
                                                         @Nullable List<String> keyspaceNames,
                                                         @Nonnull Optional<List<String>> maybeTables,
                                                         @Nonnull Optional<String> maybeRangesStr)
    {
        checkArgument(!maybeTables.isPresent() || !maybeTables.get().isEmpty(), "Must provide at least 1 table if Optional is not empty");
        ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromString(targetProtocolName);

        if (keyspaceNames == null || keyspaceNames.isEmpty())
        {
            keyspaceNames = ImmutableList.copyOf(StorageService.instance.getNonLocalStrategyKeyspaces());
        }
        checkState(keyspaceNames.size() == 1 || !maybeTables.isPresent(), "Can't specify tables with multiple keyspaces");
        List<TableId> ids = keyspacesAndTablesToTableIds(keyspaceNames, maybeTables);

        // TODO (review): should this perform the schema change to make these tables accord tables?
        List<TableId> tableIds = new ArrayList<>();
        for (TableId tableId : ids)
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
            if (metadata == null || !metadata.params.transactionalMigrationFrom.isMigrating())
                continue;
            TransactionalMode transactionalMode = metadata.params.transactionalMode;
            if (!transactionalMode.writesThroughAccord && transactionalMode != TransactionalMode.unsafe_writes)
                throw new IllegalStateException("non-SERIAL writes need to be routed through Accord before attempting migration, or enable mixed mode");
            tableIds.add(tableId);
        }

        if (!Paxos.useV2())
            throw new IllegalStateException("Can't do any consensus migrations to/from PaxosV1, switch to V2 first");

        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Optional<List<Range<Token>>> maybeParsedRanges = maybeRangesStr.map(rangesStr -> ImmutableList.copyOf(RepairOption.parseRanges(rangesStr, partitioner)));
        Token minToken = partitioner.getMinimumToken();
        List<Range<Token>> ranges = maybeParsedRanges.orElse(ImmutableList.of(new Range(minToken, minToken)));


        ClusterMetadataService.instance().commit(new BeginConsensusMigrationForTableAndRange(targetProtocol, ranges, tableIds));
    }

    public static List<Integer> finishMigrationToConsensusProtocol(@Nonnull String keyspace,
                                                                   @Nonnull Optional<List<String>> maybeTables,
                                                                   @Nonnull Optional<String> maybeRangesStr)
    {
        checkArgument(!maybeTables.isPresent() || !maybeTables.get().isEmpty(), "Must provide at least 1 table if Optional is not empty");

        Optional<List<Range<Token>>> localKeyspaceRanges = Optional.of(ImmutableList.copyOf(StorageService.instance.getLocalReplicas(keyspace).onlyFull().ranges()));
        List<Range<Token>> ranges = maybeRangesToRanges(maybeRangesStr, localKeyspaceRanges);
        Map<TableId, TableMigrationState> allTableMigrationStates = ClusterMetadata.current().consensusMigrationState.tableStates;
        List<TableId> tableIds = keyspacesAndTablesToTableIds(ImmutableList.of(keyspace), maybeTables, Optional.of(allTableMigrationStates::containsKey));

        checkState(tableIds.stream().allMatch(allTableMigrationStates::containsKey), "All tables need to be migrating");
        List<TableMigrationState> tableMigrationStates = new ArrayList<>();
        tableIds.forEach(table -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(table);
            if (cfs == null)
            {
                logger.warn("Table {} does not exist or was dropped", cfs);
                return;
            }
            TableMigrationState tms = allTableMigrationStates.get(table);
            if (tms == null)
            {
                logger.warn("Table {} does not have any migration state", cfs.name);
                return;
            }
            if(!Range.intersects(ranges, tms.migratingRanges))
            {
                logger.warn("Table {} with migrating ranges {} does not intersect with any requested ranges {}", cfs.name, tms.migratingRanges, ranges);
                return;
            }
            tableMigrationStates.add(tms);
        });

        List<TableMigrationState> migratingToAccord = tableMigrationStates.stream().filter(tms -> tms.targetProtocol == ConsensusMigrationTarget.accord).collect(toImmutableList());
        List<TableMigrationState> migratingToPaxos = tableMigrationStates.stream().filter(tms -> tms.targetProtocol == ConsensusMigrationTarget.paxos).collect(toImmutableList());;

        Integer accordRepairCmd = finishMigrationToAccord(keyspace, migratingToAccord, ranges);
        Integer paxosRepairCmd = finishMigrationToPaxos(keyspace, migratingToPaxos, ranges);
        List<Integer> result = new ArrayList<>();
        if (accordRepairCmd != null)
            result.add(accordRepairCmd);
        if (paxosRepairCmd != null)
            result.add(paxosRepairCmd);
        return result;
    }

    private interface MigrationFinisher
    {
        Integer finish(Collection<TableMigrationState> tables, List<Range<Token>> ranges);
    }

    private static Integer finishMigrationTo(String name, List<TableMigrationState> tableMigrationStates, List<Range<Token>> requestedRanges, MigrationFinisher migrationFinisher)
    {
        logger.info("Begin finish migration to {} for ranges {} and tables {}", name, requestedRanges, tableMigrationStates);
        List<Range<Token>> intersectingRanges = new ArrayList<>();
        tableMigrationStates.stream().map(TableMigrationState::migratingRanges).forEach(intersectingRanges::addAll);
        intersectingRanges = Range.normalize(intersectingRanges);
        intersectingRanges = Range.intersectionOfNormalizedRanges(intersectingRanges, requestedRanges);
        if (intersectingRanges.isEmpty())
        {
            logger.warn("No requested ranges {} intersect any migrating ranges in any table in keyspace {}");
            return null;
        }

        // Repair requires that the ranges once again be grouped by the ranges provided originally which all
        // fall within local range boundaries. This was already checked in maybeRangesToRanges.
        List<Range<Token>> intersectingRangesGrouped = new ArrayList<>();
        for (Range<Token> r : requestedRanges)
        {
            List<Range<Token>> intersectionsForGroup = new ArrayList<>();
            for (Range<Token> intersectedRange : intersectingRanges)
                intersectionsForGroup.addAll(r.intersectionWith(intersectedRange));
            intersectingRangesGrouped.addAll(normalize(intersectionsForGroup));
        }
        return migrationFinisher.finish(tableMigrationStates, intersectingRangesGrouped);
    }

    /*
     * This is basically just invoking classic Cassandra repair and is pretty redundant with invoking repair
     * directly which would also work without issue. It's include so the same interface works for both migrating to/from
     * Accord, but it's not great in that repair has a lot of options that might need to be forwarded.
     *
     * Still maybe more valuable to put this layer of abstraction in so we can change how it works later and it's less
     * tightly coupled with the Repair interface which is pretty orthogonal to consensus migration.
     */
    private static Integer finishMigrationToAccord(String keyspace, List<TableMigrationState> migratingToAccord, List<Range<Token>> requestedRanges)
    {
        return finishMigrationTo("Accord", migratingToAccord, requestedRanges, (tables, intersectingRanges) -> {
            RepairOption repairOption = getRepairOption(tables, intersectingRanges, false);
            return StorageService.instance.repair(keyspace, repairOption, emptyList()).left;
        });
    }

    private static Integer finishMigrationToPaxos(String keyspace, List<TableMigrationState> migratingToPaxos, List<Range<Token>> requestedRanges)
    {
        return finishMigrationTo("Paxos", migratingToPaxos, requestedRanges, (tables, intersectingRanges) -> {
            RepairOption repairOption = getRepairOption(tables, intersectingRanges, true);
            return StorageService.instance.repair(keyspace, repairOption, emptyList()).left;
        });
    }


    private static List<TableId> keyspacesAndTablesToTableIds(@Nonnull List<String> keyspaceNames, @Nonnull Optional<List<String>> maybeTables)
    {
        return keyspacesAndTablesToTableIds(keyspaceNames, maybeTables, Optional.empty());
    }

    private static List<TableId> keyspacesAndTablesToTableIds(@Nonnull List<String> keyspaceNames, @Nonnull Optional<List<String>> maybeTables, @Nonnull Optional<Predicate<TableId>> includeTable)
    {
        List<TableId> tableIds = new ArrayList<>();
        for (String keyspaceName : keyspaceNames)
        {
            Optional<Collection<TableId>> maybeTableIds = maybeTables.map(tableNames ->
                    tableNames
                            .stream()
                            .map(tableName -> {
                                TableMetadata tm = Schema.instance.getTableMetadata(keyspaceName, tableName);
                                if (tm == null)
                                    throw new IllegalArgumentException("Unknown table %s.%s".format(keyspaceName, tableName));
                                return tm.id;
                            })
                            .collect(toImmutableList()));
            tableIds.addAll(
                    maybeTableIds.orElseGet(() ->
                            Schema.instance.getKeyspaceInstance(keyspaceName).getColumnFamilyStores()
                                    .stream()
                                    .map(ColumnFamilyStore::getTableId)
                                    .filter(includeTable.orElse(Predicates.alwaysTrue())) // Filter out non-migrating so they don't generate an error
                                    .collect(toImmutableList())));
        }
        return tableIds;
    }

    @Nonnull
    private static RepairOption getRepairOption(Collection<TableMigrationState> tables, List<Range<Token>> intersectingRanges, boolean accordRepair)
    {
        boolean primaryRange = false;
        // TODO (review): Should disabling incremental repair be exposed for the Paxos repair in case someone explicitly does not do incremental repair?
        boolean incremental = !accordRepair;
        boolean trace = false;
        int numJobThreads = 1;
        boolean pullRepair = false;
        boolean forceRepair = false;
        boolean optimiseStreams = false;
        boolean ignoreUnreplicatedKeyspaces = true;
        boolean repairPaxos = !accordRepair;
        boolean paxosOnly = false;
        boolean accordOnly = false;
        RepairOption repairOption = new RepairOption(RepairParallelism.PARALLEL, primaryRange, incremental, trace, numJobThreads, intersectingRanges, pullRepair, forceRepair, PreviewKind.NONE, optimiseStreams, ignoreUnreplicatedKeyspaces, repairPaxos, paxosOnly, accordOnly, true);
        tables.forEach(table -> repairOption.getColumnFamilies().add(table.tableName));
        return repairOption;
    }


    // Repair is restricted to local ranges, but manipulating CMS migration state doesn't need to be restricted
    private static @Nonnull List<Range<Token>> maybeRangesToRanges(@Nonnull Optional<String> maybeRangesStr)
    {
        return maybeRangesToRanges(maybeRangesStr, Optional.empty());
    }

    private static @Nonnull List<Range<Token>> maybeRangesToRanges(@Nonnull Optional<String> maybeRangesStr, Optional<List<Range<Token>>> restrictToRanges)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Optional<List<Range<Token>>> maybeParsedRanges = maybeRangesStr.map(rangesStr -> ImmutableList.copyOf(RepairOption.parseRanges(rangesStr, partitioner)));
        Token minToken = partitioner.getMinimumToken();
        List<Range<Token>> defaultRanges = restrictToRanges.orElse(ImmutableList.of(new Range(minToken, minToken)));
        List<Range<Token>> ranges = maybeParsedRanges.orElse(defaultRanges);
        checkArgument(ranges.stream().allMatch(range -> defaultRanges.stream().anyMatch(defaultRange -> defaultRange.contains(range))),
                "If ranges are specified each range must be contained within a local range (" + defaultRanges + ") for this node to allow for precise repairs. Specified " + ranges);
        return ranges;
    }
}
