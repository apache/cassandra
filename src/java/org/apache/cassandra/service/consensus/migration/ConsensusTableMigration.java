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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.BeginConsensusMigrationForTableAndRange;
import org.apache.cassandra.tcm.transformations.MaybeFinishConsensusMigrationForTableAndRange;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.apache.cassandra.dht.NormalizedRanges.normalizedRanges;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;

/**
 * Track and update the migration state of individual table and ranges within those tables
 */
public abstract class ConsensusTableMigration
{
    private static final Logger logger = LoggerFactory.getLogger(ConsensusTableMigration.class);

    public static final MetadataSerializer<NormalizedRanges<Token>> rangesSerializer = new MetadataSerializer<NormalizedRanges<Token>>()
    {

        @Override
        public void serialize(NormalizedRanges<Token> t, DataOutputPlus out, Version version) throws IOException
        {
            serializeCollection(t, out, version, Range.serializer);
        }

        @Override
        public NormalizedRanges<Token> deserialize(DataInputPlus in, Version version) throws IOException
        {
            return normalizedRanges(deserializeList(in, version, Range.serializer));
        }

        @Override
        public long serializedSize(NormalizedRanges<Token> t, Version version)
        {
            return serializedCollectionSize(t, version, Range.serializer);
        }
    };

    public static final FutureCallback<RepairResult> completedRepairJobHandler = new FutureCallback<RepairResult>()
    {
        @Override
        public void onSuccess(@Nullable RepairResult repairResult)
        {
            checkNotNull(repairResult, "repairResult should not be null");
            ConsensusMigrationRepairResult migrationResult = repairResult.consensusMigrationRepairResult;
            ConsensusMigrationRepairType repairType = migrationResult.type;

            // Need to repair both Paxos and base table state
            // Could track them separately, but doesn't seem worth the effort
            if (repairType.ineligibleForMigration())
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

            NormalizedRanges<Token> paxosRepairedRanges = NormalizedRanges.empty();
            if (repairType.migrationToAccordEligible())
                // Paxos always repairs all ranges requested by the repair although there should be nothing
                // repaired in the migrated and Accord managed ranges
                paxosRepairedRanges = normalizedRanges(desc.ranges);

            NormalizedRanges<Token> accordBarrieredRanges = NormalizedRanges.empty();
            if (repairType.migrationToPaxosEligible())
                // Accord only barriers ranges it thinks it manages and repair collects which it barriered
                // precisely which doesn't have to match what the entire repair covers
                accordBarrieredRanges = normalizedRanges(migrationResult.barrieredRanges.stream()
                                                                       .map(range -> ((TokenRange)range).toKeyspaceRange())
                                                                       .collect(toImmutableList()));
            accordBarrieredRanges = normalizedRanges(accordBarrieredRanges);

            ClusterMetadataService.instance().commit(
                new MaybeFinishConsensusMigrationForTableAndRange(
                    desc.keyspace, desc.columnFamily, paxosRepairedRanges, accordBarrieredRanges,
                    migrationResult.minEpoch, repairType.repairedData, repairType.repairedPaxos, repairType.repairedAccord));
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
            if (!transactionalMode.nonSerialWritesThroughAccord && transactionalMode != TransactionalMode.unsafe_writes)
                throw new IllegalStateException("non-SERIAL writes need to be routed through Accord before attempting migration, or enable mixed mode");
            tableIds.add(tableId);
        }

        if (!Paxos.useV2())
            throw new IllegalStateException("Can't do any consensus migrations to/from PaxosV1, switch to V2 first");

        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Optional<List<Range<Token>>> maybeParsedRanges = maybeRangesStr.map(rangesStr -> ImmutableList.copyOf(RepairOption.parseRanges(rangesStr, partitioner)));
        Token minToken = partitioner.getMinimumToken();
        NormalizedRanges<Token> ranges = normalizedRanges(maybeParsedRanges.orElse(ImmutableList.of(new Range(minToken, minToken))));


        ClusterMetadataService.instance().commit(new BeginConsensusMigrationForTableAndRange(targetProtocol, ranges, tableIds));
    }

    public static Integer finishMigrationToConsensusProtocol(@Nonnull String keyspace,
                                                                   @Nonnull Optional<List<String>> maybeTables,
                                                                   @Nonnull Optional<String> maybeRangesStr,
                                                                   ConsensusMigrationTarget target)
    {
        checkArgument(!maybeTables.isPresent() || !maybeTables.get().isEmpty(), "Must provide at least 1 table if Optional is not empty");
        checkNotNull(target);

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

        switch (target)
        {
            case accord:
                List<TableMigrationState> migratingToAccord = tableMigrationStates.stream().filter(tms -> tms.targetProtocol == ConsensusMigrationTarget.accord).collect(toImmutableList());
                Integer accordDataRepairCmd = finishMigrationToAccordDataRepair(keyspace, migratingToAccord, ranges);
                // All ranges are already repaired and ready for Paxos repair
                // so kick that off instead
                if (accordDataRepairCmd == null)
                    return finishMigrationToAccordPaxosRepair(keyspace, migratingToAccord, ranges);
                return accordDataRepairCmd;
            case paxos:
                List<TableMigrationState> migratingToPaxos = tableMigrationStates.stream().filter(tms -> tms.targetProtocol == ConsensusMigrationTarget.paxos).collect(toImmutableList());;
                return finishMigrationToPaxos(keyspace, migratingToPaxos, ranges);
            default:
                throw new IllegalArgumentException("Unsupported target: " + target);
        }
    }

    private interface MigrationFinisher
    {
        Integer finish(Collection<TableMigrationState> tables, List<Range<Token>> ranges);
    }

    private static Integer finishMigrationTo(String name, List<TableMigrationState> tableMigrationStates, List<Range<Token>> requestedRanges, Function<TableMigrationState, List<Range<Token>>> migratingRanges, MigrationFinisher migrationFinisher)
    {
        logger.info("Begin finish migration to {} for ranges {} and tables {}", name, requestedRanges, tableMigrationStates);
        List<Range<Token>> intersectingRangesList = new ArrayList<>();
        tableMigrationStates.stream().map(migratingRanges).forEach(intersectingRangesList::addAll);
        NormalizedRanges<Token> intersectingRanges = normalizedRanges(intersectingRangesList);
        intersectingRanges = intersectingRanges.intersection(normalizedRanges(requestedRanges));
        if (intersectingRanges.isEmpty())
        {
            logger.warn("No requested ranges {} intersect any migrating ranges in any table for migration: {}", requestedRanges, name);
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
     *
     * This first repair is necessary to allow Accord to read data that was written non-serially because we can't do key
     * migration for those operations because there is no metadata like we have with Paxos.
     */
    private static Integer finishMigrationToAccordDataRepair(String keyspace, List<TableMigrationState> migratingToAccord, List<Range<Token>> requestedRanges)
    {
        return finishMigrationTo("Accord Data Repair", migratingToAccord, requestedRanges, TableMigrationState::repairPendingRanges, (tables, intersectingRanges) -> {
            RepairOption repairOption = getRepairOption(tables, intersectingRanges, true, false, false);
            return StorageService.instance.repair(keyspace, repairOption, emptyList()).left;
        });
    }

    /*
     * Need to perform a second repair that is Paxos and then data so that when migrating to FULL mode Accord can read
     * the result of any Paxos operation from any replica. This should only be done on the migrating ranges that are no longer pending data repair
     */
    private static Integer finishMigrationToAccordPaxosRepair(String keyspace, List<TableMigrationState> migratingToAccord, List<Range<Token>> requestedRanges)
    {
        return finishMigrationTo("Accord Paxos Repair", migratingToAccord, requestedRanges, tms -> tms.migratingRanges.subtract(tms.repairPendingRanges), (tables, intersectingRanges) -> {
            RepairOption repairOption = getRepairOption(tables, intersectingRanges, true, true, false);
            return StorageService.instance.repair(keyspace, repairOption, emptyList()).left;
        });
    }

    /*
     * Migration back to Paxos is pretty simple since Accord can bring all replicas up to date by running barriers and
     * supports key migration immediately without any repair. Paxos doesn't have the same sensitivity to non-deterministic
     * data reads.
     */
    private static Integer finishMigrationToPaxos(String keyspace, List<TableMigrationState> migratingToPaxos, List<Range<Token>> requestedRanges)
    {
        return finishMigrationTo("Paxos", migratingToPaxos, requestedRanges, TableMigrationState::migratingRanges, (tables, intersectingRanges) -> {
            RepairOption repairOption = getRepairOption(tables, intersectingRanges, false, false, true);
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
                                    throw new IllegalArgumentException(format("Unknown table %s.%s", keyspaceName, tableName));
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
    private static RepairOption getRepairOption(Collection<TableMigrationState> tables, List<Range<Token>> intersectingRanges, boolean repairData, boolean repairPaxos, boolean repairAccord)
    {
        boolean primaryRange = false;
        // TODO (review): Should disabling incremental repair be exposed for the Paxos repair in case someone explicitly does not do incremental repair?
        boolean incremental = repairData;
        boolean trace = false;
        int numJobThreads = 1;
        boolean pullRepair = false;
        boolean forceRepair = false;
        boolean optimiseStreams = false;
        boolean ignoreUnreplicatedKeyspaces = true;
        RepairOption repairOption = new RepairOption(RepairParallelism.PARALLEL, primaryRange, incremental, trace, numJobThreads, intersectingRanges, pullRepair, forceRepair, PreviewKind.NONE, optimiseStreams, ignoreUnreplicatedKeyspaces, repairData, repairPaxos, repairAccord);
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
