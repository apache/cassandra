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

package org.apache.cassandra.service;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.SignedBytes;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.LegacyPaxosStrategy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IPartitionerDependentSerializer;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.BeginConsensusMigrationForTableAndRange;
import org.apache.cassandra.tcm.transformations.MaybeFinishConsensusMigrationForTableAndRange;
import org.apache.cassandra.tcm.transformations.SetConsensusMigrationTargetProtocol;
import org.apache.cassandra.utils.NullableSerializer;
import org.apache.cassandra.utils.PojoToString;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.dht.Range.intersectionOfNormalizedRanges;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.dht.Range.rangeSerializer;
import static org.apache.cassandra.dht.Range.subtract;
import static org.apache.cassandra.dht.Range.subtractNormalizedRanges;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeSet;
import static org.apache.cassandra.utils.CollectionSerializers.newCollectionSerializer;
import static org.apache.cassandra.utils.CollectionSerializers.newHashMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

/**
 * Track and update the migration state of individual table and ranges within those tables
 */
public abstract class ConsensusTableMigrationState
{
    private static final Logger logger = LoggerFactory.getLogger(ConsensusTableMigrationState.class);

    public static final IPartitionerDependentSerializer<List<Range<Token>>> rangesSerializer = newCollectionSerializer(rangeSerializer);

    public static final FutureCallback<RepairResult> completedRepairJobHandler = new FutureCallback<RepairResult>()
    {
        @Override
        public void onSuccess(@Nullable RepairResult repairResult)
        {
            checkNotNull(repairResult, "repairResult should not be null");
            ConsensusMigrationRepairResult migrationResult = repairResult.consensusMigrationRepairResult;

            // Need to repair both Paxos and base table state
            // Could track them separately, but doesn't seem worth the effort
            if (migrationResult.consensusMigrationRepairType == ConsensusMigrationRepairType.ineligible)
                return;

            RepairJobDesc desc = repairResult.desc;
            ClusterMetadataService.instance.commit(
                new MaybeFinishConsensusMigrationForTableAndRange(
                    desc.keyspace, desc.columnFamily, ImmutableList.copyOf(desc.ranges),
                    migrationResult.minEpoch, migrationResult.consensusMigrationRepairType));
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            // Only successes drive forward progress
        }
    };

    @VisibleForTesting
    public static void reset()
    {
        ClusterMetadataService.reset();
    }

    public enum ConsensusMigrationRepairType
    {
        ineligible(0),
        paxos(1),
        accord(2);

        public final byte value;

        ConsensusMigrationRepairType(int value)
        {
            this.value = SignedBytes.checkedCast(value);
        }

        public static ConsensusMigrationRepairType fromString(String repairType)
        {
            return ConsensusMigrationRepairType.valueOf(repairType.toLowerCase());
        }

        public static ConsensusMigrationRepairType fromValue(byte value)
        {
            switch (value)
            {
                default:
                    throw new IllegalArgumentException(value + " is not recognized");
                case 0:
                    return ConsensusMigrationRepairType.paxos;
                case 1:
                    return ConsensusMigrationRepairType.accord;
            }
        }
    }

    public enum ConsensusMigrationTarget {
        paxos(0),
        accord(1);

        public final byte value;

        ConsensusMigrationTarget(int value)
        {
            this.value = SignedBytes.checkedCast(value);
        }

        public static ConsensusMigrationTarget fromString(String targetProtocol)
        {
            return ConsensusMigrationTarget.valueOf(targetProtocol.toLowerCase());
        }

        public static ConsensusMigrationTarget fromValue(byte value)
        {
            switch (value)
            {
                default:
                    throw new IllegalArgumentException(value + " is not recognized");
                case 0:
                    return paxos;
                case 1:
                    return accord;
            }
        }
    }

    public static class ConsensusMigrationRepairResult
    {
        private final ConsensusMigrationRepairType consensusMigrationRepairType;
        private final Epoch minEpoch;

        private ConsensusMigrationRepairResult(ConsensusMigrationRepairType consensusMigrationRepairType, Epoch minEpoch)
        {
            this.consensusMigrationRepairType = consensusMigrationRepairType;
            this.minEpoch = minEpoch;
        }

        public static ConsensusMigrationRepairResult fromCassandraRepair(Epoch minEpoch, boolean didPaxosAndRegularRepair)
        {
            checkArgument(!didPaxosAndRegularRepair || minEpoch.isAfter(Epoch.EMPTY), "Epoch should not be empty if Paxos and regular repairs were performed");
            if (didPaxosAndRegularRepair)
                return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.paxos, minEpoch);
            else
                return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.ineligible, Epoch.EMPTY);
        }

        public static ConsensusMigrationRepairResult fromAccordRepair(Epoch minEpoch)
        {
            checkArgument(minEpoch.isAfter(Epoch.EMPTY), "Accord repairs should always occur at an Epoch");
            return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.accord, minEpoch);
        }
    }

    public static class ConsensusMigratedAt
    {
        public static final IVersionedSerializer<ConsensusMigratedAt> serializer = NullableSerializer.wrap(new IVersionedSerializer<ConsensusMigratedAt>()
        {
            @Override
            public void serialize(ConsensusMigratedAt t, DataOutputPlus out, int version) throws IOException
            {
                Epoch.messageSerializer.serialize(t.migratedAtEpoch, out, version);
                out.writeByte(t.migratedAtTarget.value);
            }

            @Override
            public ConsensusMigratedAt deserialize(DataInputPlus in, int version) throws IOException
            {
                Epoch migratedAtEpoch = Epoch.messageSerializer.deserialize(in, version);
                ConsensusMigrationTarget target = ConsensusMigrationTarget.fromValue(in.readByte());
                return new ConsensusMigratedAt(migratedAtEpoch, target);
            }

            @Override
            public long serializedSize(ConsensusMigratedAt t, int version)
            {
                return TypeSizes.sizeof(ConsensusMigrationTarget.accord.value)
                       + Epoch.messageSerializer.serializedSize(t.migratedAtEpoch, version);
            }
        });

        // Fields are not nullable when used for messaging
        @Nullable
        public final Epoch migratedAtEpoch;

        @Nullable
        public final ConsensusMigrationTarget migratedAtTarget;

        public ConsensusMigratedAt(Epoch migratedAtEpoch, ConsensusMigrationTarget migratedAtTarget)
        {
            this.migratedAtEpoch = migratedAtEpoch;
            this.migratedAtTarget = migratedAtTarget;
        }
    }

    // TODO Move this into the schema for the table once this is based off of TrM
    public static class TableMigrationState
    {
        @Nonnull
        public final String keyspaceName;

        @Nonnull
        public final String tableName;

        @Nonnull
        public final TableId tableId;

        @Nonnull
        public final ConsensusMigrationTarget targetProtocol;

        @Nonnull
        public final List<Range<Token>> migratedRanges;

        /*
         * Necessary to track which ranges started migrating at which epoch
         * in order to know whether a repair qualifies in terms of finishing
         * migration of the range.
         */
        @Nonnull
        public final NavigableMap<Epoch, List<Range<Token>>> migratingRangesByEpoch;

        public static final IPartitionerDependentSerializer<TableMigrationState> serializer = new IPartitionerDependentSerializer<TableMigrationState>()
        {
            @Override
            public void serialize(TableMigrationState t, DataOutputPlus out, int version) throws IOException
            {
                out.write(t.targetProtocol.value);
                out.writeUTF(t.keyspaceName);
                out.writeUTF(t.tableName);
                t.tableId.serialize(out);
                serializeCollection(t.migratedRanges, out, version, rangeSerializer);
                serializeMap(t.migratingRangesByEpoch, out, version, Epoch.messageSerializer, rangesSerializer);
            }

            @Override
            public TableMigrationState deserialize(DataInputPlus in, IPartitioner partitioner, int version) throws IOException
            {
                ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromValue(in.readByte());
                String keyspaceName = in.readUTF();
                String tableName = in.readUTF();
                TableId tableId = TableId.deserialize(in);
                Set<Range<Token>> migratedRanges = deserializeSet(in, partitioner, version, rangeSerializer);
                Map<Epoch, List<Range<Token>>> migratingRangesByEpoch = deserializeMap(in, partitioner, version, Epoch.messageSerializer, rangesSerializer, newHashMap());
                return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, migratingRangesByEpoch);
            }

            @Override
            public long serializedSize(TableMigrationState t, int version)
            {
                return sizeof(t.targetProtocol.value)
                        + sizeof(t.keyspaceName)
                        + sizeof(t.tableName)
                        + t.tableId.serializedSize()
                        + serializedCollectionSize(t.migratedRanges, version, rangeSerializer)
                        + serializedMapSize(t.migratingRangesByEpoch, version, Epoch.messageSerializer, rangesSerializer);
            }
        };

        @Nonnull
        public final List<Range<Token>> migratingRanges;

        @Nonnull
        public final List<Range<Token>> migratingAndMigratedRanges;

        public TableMigrationState(@Nonnull String keyspaceName,
                                   @Nonnull String tableName,
                                   @Nonnull TableId tableId,
                                   @Nonnull ConsensusMigrationTarget targetProtocol,
                                   @Nonnull Collection<Range<Token>> migratedRanges,
                                   @Nonnull Map<Epoch, List<Range<Token>>> migratingRangesByEpoch)
        {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.tableId = tableId;
            this.targetProtocol = targetProtocol;
            this.migratedRanges = ImmutableList.copyOf(normalize(migratedRanges));
            this.migratingRangesByEpoch = ImmutableSortedMap.copyOf(
            migratingRangesByEpoch.entrySet()
                                  .stream()
                                  .map( entry -> new SimpleEntry<>(entry.getKey(), ImmutableList.copyOf(normalize(entry.getValue()))))
                                  .collect(Collectors.toList()));
            this.migratingRanges = ImmutableList.copyOf(normalize(migratingRangesByEpoch.values().stream().flatMap(Collection::stream).collect(Collectors.toList())));
            this.migratingAndMigratedRanges = ImmutableList.copyOf(normalize(ImmutableList.<Range<Token>>builder().addAll(migratedRanges).addAll(migratingRanges).build()));
        }

        public TableMigrationState withRangesMigrating(@Nonnull Collection<Range<Token>> ranges,
                                                       @Nonnull ConsensusMigrationTarget target,
                                                       @Nonnull Epoch epoch)
        {
            checkArgument(epoch.isAfter(Epoch.EMPTY), "Epoch shouldn't be empty");

            // Doesn't matter which epoch the range started migrating in for this context so merge them all
            Collection<Range<Token>> migratingRanges = normalize(migratingRangesByEpoch.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
            checkArgument(target == targetProtocol, "Requested migration to target protocol " + target + " conflicts with in progress migration to protocol " + targetProtocol);
            List<Range<Token>> normalizedRanges = normalize(ranges);
            checkArgument(!subtract(normalizedRanges, migratingRanges).isEmpty(), "Range " + ranges + " is already being migrated");
            Set<Range<Token>> withoutAlreadyMigrated = subtract(normalizedRanges, migratedRanges);
            checkArgument(!withoutAlreadyMigrated.isEmpty(), "Range " + ranges + " is already migrated");
            Set<Range<Token>> withoutBoth = subtract(withoutAlreadyMigrated, migratingRanges);
            checkArgument(!withoutBoth.isEmpty(), "Range " + ranges + " is already migrating/migrated");

            if (!Range.equals(normalizedRanges, withoutBoth))
                logger.warn("Ranges " + normalizedRanges + " to start migrating is already partially migrating/migrated " + withoutBoth);

            Map<Epoch, List<Range<Token>>> newMigratingRanges = new HashMap<>(migratingRangesByEpoch.size() + 1);
            newMigratingRanges.putAll(migratingRangesByEpoch);
            newMigratingRanges.put(epoch, normalizedRanges);

            return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, newMigratingRanges);
        }

        public TableMigrationState withRangesRepairedAtEpoch(@Nonnull Collection<Range<Token>> ranges,
                                                             @Nonnull Epoch epoch)
        {
            checkArgument(epoch.isAfter(Epoch.EMPTY), "Epoch shouldn't be empty");

            List<Range<Token>> normalizedRepairedRanges = normalize(ranges);
            // This should be inclusive because the epoch we store in the map is the epoch in which the range has been marked migrating
            // in startMigrationToConsensusProtocol
            NavigableMap<Epoch, List<Range<Token>>> coveredEpochs = migratingRangesByEpoch.headMap(epoch, true);
            List<Range<Token>> normalizedMigratingRanges = normalize(coveredEpochs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
            List<Range<Token>> normalizedRepairedIntersection = intersectionOfNormalizedRanges(normalizedRepairedRanges, normalizedMigratingRanges);
            checkState(!normalizedRepairedIntersection.isEmpty(), "None of Ranges " + ranges + " were being migrated");

            Map<Epoch, List<Range<Token>>> newMigratingRangesByEpoch = new HashMap<>();

            // Everything in this epoch or later can't have been migrated so re-add all of them
            newMigratingRangesByEpoch.putAll(migratingRangesByEpoch.tailMap(epoch, false));

            // Include anything still remaining to be migrated after subtracting what was repaired
            for (Map.Entry<Epoch, List<Range<Token>>> e : coveredEpochs.entrySet())
            {
                // Epoch when these ranges started migrating
                Epoch rangesEpoch = e.getKey();
                List<Range<Token>> epochMigratingRanges = e.getValue();
                List<Range<Token>> remainingRanges = subtractNormalizedRanges(epochMigratingRanges, normalizedRepairedIntersection);
                if (!remainingRanges.isEmpty())
                    newMigratingRangesByEpoch.put(rangesEpoch, remainingRanges);
            }

            List<Range<Token>> newMigratedRanges = new ArrayList<>(normalizedMigratingRanges.size() + ranges.size());
            newMigratedRanges.addAll(migratedRanges);
            newMigratedRanges.addAll(normalizedRepairedIntersection);
            return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, newMigratedRanges, newMigratingRangesByEpoch);
        }

        public boolean paxosReadSatisfiedByKeyMigrationAtEpoch(DecoratedKey key, ConsensusMigratedAt consensusMigratedAt)
        {
            // This check is being done from a Paxos read attempt which needs to
            // check if Accord needs to resolve any in flight accord transactions
            // if the migration target is Accord then nothing needs to be done
            if (targetProtocol != ConsensusMigrationTarget.paxos)
                return true;

            return satisfiedByKeyMigrationAtEpoch(key, consensusMigratedAt);
        }

        public boolean satisfiedByKeyMigrationAtEpoch(@Nonnull DecoratedKey key, @Nullable ConsensusMigratedAt consensusMigratedAt)
        {
            if (consensusMigratedAt == null)
            {
                // It hasn't been migrated and needs migration if it is in a migrating range
                return Range.isInNormalizedRanges(key.getToken(), migratingRanges);
            }
            else
            {
                // It has been migrated and might be from a late enough epoch to satisfy this migration
                return consensusMigratedAt.migratedAtTarget == targetProtocol
                       && migratingRangesByEpoch.headMap(consensusMigratedAt.migratedAtEpoch, true).values()
                                                .stream()
                                                .flatMap(List::stream)
                                                .anyMatch(range -> range.contains(key.getToken()));
            }
        }

        public Epoch minMigrationEpoch(Token token)
        {
            // TODO should there be an index to make this more efficient?
            for (Map.Entry<Epoch, List<Range<Token>>> e : migratingRangesByEpoch.entrySet())
            {
                if (Range.isInNormalizedRanges(token, e.getValue()))
                    return e.getKey();
            }
            return Epoch.EMPTY;
        }


        public @Nonnull TableId getTableId()
        {
            return tableId;
        }

        public TableMigrationState withMigrationTarget(ConsensusMigrationTarget targetProtocol, Epoch epoch)
        {
            if (this.targetProtocol == targetProtocol)
                return this;

            // Migrating ranges remain migrating because individual keys may have already been migrated
            // So for correctness we need to perform key migration
            // We do need to update the epoch so that a new repair is required to drive the migration
            Map<Epoch, List<Range<Token>>> migratingRangesByEpoch = ImmutableMap.of(epoch, migratingRanges);

            Token minToken = ColumnFamilyStore.getIfExists(tableId).getPartitioner().getMinimumToken();
            Range<Token> fullRange = new Range(minToken, minToken);
            // What is migrated already is anything that was never migrated/migrating before (untouched)
            List<Range<Token>> migratedRanges = ImmutableList.copyOf(normalize(fullRange.subtractAll(migratingAndMigratedRanges)));

            return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, migratingRangesByEpoch);
        }

        public Map<String, Object> toMap()
        {
            Builder<String, Object> builder = ImmutableMap.builder();
            builder.put("keyspace", keyspaceName);
            builder.put("table", tableName);
            builder.put("tableId", tableId.toString());
            builder.put("targetProtocol", targetProtocol.toString());
            builder.put("migratedRanges", migratedRanges.stream().map(Objects::toString).collect(toImmutableList()));
            Map<Long, List<String>> rangesByEpoch = new LinkedHashMap<>();
            for (Map.Entry<Epoch, List<Range<Token>>> entry : migratingRangesByEpoch.entrySet())
            {
                rangesByEpoch.put(entry.getKey().getEpoch(), entry.getValue().stream().map(Objects::toString).collect(toImmutableList()));
            }
            builder.put("migratingRangesByEpoch", rangesByEpoch);
            return builder.build();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableMigrationState that = (TableMigrationState) o;
            return keyspaceName.equals(that.keyspaceName) && tableName.equals(that.tableName) && tableId.equals(that.tableId) && targetProtocol == that.targetProtocol && migratedRanges.equals(that.migratedRanges) && migratingRangesByEpoch.equals(that.migratingRangesByEpoch) && migratingRanges.equals(that.migratingRanges) && migratingAndMigratedRanges.equals(that.migratingAndMigratedRanges);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, migratingRangesByEpoch, migratingRanges, migratingAndMigratedRanges);
        }
    }

    // TODO this will mostly go away once we can move TableMigrationState into the table schema
    public static class MigrationStateSnapshot
    {
        @Nonnull
        public final Map<TableId, TableMigrationState> tableStates;

        public final Epoch epoch;

        public static final IVersionedSerializer<MigrationStateSnapshot> messagingSerializer = new IVersionedSerializer<MigrationStateSnapshot>()
        {
            @Override
            public void serialize(MigrationStateSnapshot t, DataOutputPlus out, int version) throws IOException
            {
                serializer.serialize(t, out, version);
            }

            @Override
            public MigrationStateSnapshot deserialize(DataInputPlus in, int version) throws IOException
            {
                return serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version);
            }

            @Override
            public long serializedSize(MigrationStateSnapshot t, int version)
            {
                return serializer.serializedSize(t, version);
            }
        };

        public static final IPartitionerDependentSerializer<MigrationStateSnapshot> serializer = new IPartitionerDependentSerializer<MigrationStateSnapshot>()
        {
            @Override
            public void serialize(MigrationStateSnapshot migrationStateSnapshot, DataOutputPlus out, int version) throws IOException
            {
                Epoch.messageSerializer.serialize(migrationStateSnapshot.epoch, out, version);
                serializeMap(migrationStateSnapshot.tableStates, out, version, TableId.serializer, TableMigrationState.serializer);
            }

            @Override
            public MigrationStateSnapshot deserialize(DataInputPlus in, IPartitioner p, int version) throws IOException
            {
                Epoch epoch = Epoch.messageSerializer.deserialize(in, version);
                Map<TableId, TableMigrationState> tableMigrationStates = deserializeMap(in, p, version, TableId.serializer, TableMigrationState.serializer, newHashMap());
                return new MigrationStateSnapshot(tableMigrationStates, epoch);
            }

            @Override
            public long serializedSize(MigrationStateSnapshot migrationStateSnapshot, int version)
            {
                return Epoch.messageSerializer.serializedSize(migrationStateSnapshot.epoch, version) + serializedMapSize(migrationStateSnapshot.tableStates, version, TableId.serializer, TableMigrationState.serializer);
            }
        };

        public MigrationStateSnapshot(@Nonnull Map<TableId, TableMigrationState> tableStates, @Nonnull Epoch epoch)
        {
            checkNotNull(tableStates, "tableStates is null");
            checkNotNull(epoch, "epoch is null");
            this.tableStates = ImmutableMap.copyOf(tableStates);
            this.epoch = epoch;
        }

        public Map<String, Object> toMap(@Nullable Set<String> keyspaceNames, @Nullable Set<String> tableNames)
        {
            return ImmutableMap.of("epoch", epoch.getEpoch(),
                                   "tableStates", tableStatesAsMaps(keyspaceNames, tableNames),
                                   "version", PojoToString.CURRENT_VERSION);
        }

        private List<Map<String, Object>> tableStatesAsMaps(@Nullable Set<String> keyspaceNames,
                                                            @Nullable Set<String> tableNames)
        {
            ImmutableList.Builder<Map<String, Object>> builder = ImmutableList.builder();
            for (TableMigrationState tms : tableStates.values())
            {
                if (keyspaceNames != null && !keyspaceNames.contains(tms.keyspaceName))
                    continue;
                if (tableNames != null && !tableNames.contains(tms.tableName))
                    continue;
                builder.add(tms.toMap());
            }
            return builder.build();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MigrationStateSnapshot that = (MigrationStateSnapshot) o;
            return tableStates.equals(that.tableStates) && epoch.equals(that.epoch);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableStates, epoch);
        }
    }

    private ConsensusTableMigrationState() {}

    // Used by callers to avoid looking up the TMS multiple times
    public static @Nullable TableMigrationState getTableMigrationState(long epoch, TableId tableId)
    {
        ClusterMetadata cm = ClusterMetadataService.instance.maybeCatchup(Epoch.create(0, epoch));
        TableMigrationState tms = cm.migrationStateSnapshot.tableStates.get(tableId);
        return tms;
    }

    /*
     * Set or change the migration target for the keyspaces and tables. Can be used to reverse the direction of a migration
     * or instantly migrate a table to a new protocol.
     */
    public static void setConsensusMigrationTargetProtocol(@Nonnull String targetProtocolName,
                                                           @Nonnull List<String> keyspaceNames,
                                                           @Nonnull Optional<List<String>> maybeTables)
    {
        checkArgument(!keyspaceNames.isEmpty(), "At least one keyspace must be specified");
        checkArgument(keyspaceNames.size() == 1 || !maybeTables.isPresent(), "Can't specify tables with multiple keyspaces");
        checkArgument(!maybeTables.isPresent() || !maybeTables.get().isEmpty(), "Must provide at least 1 table if Optional is not empty");
        ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromString(targetProtocolName);

        if (DatabaseDescriptor.getLegacyPaxosStrategy() == LegacyPaxosStrategy.accord)
            throw new IllegalStateException("Mixing a hard coded strategy with migration is unsupported");

        if (!Paxos.useV2())
            throw new IllegalStateException("Can't do any consensus migrations from/to PaxosV1, switch to V2 first");

        List<TableId> tableIds = keyspacesAndTablesToTableIds(keyspaceNames, maybeTables);
        ClusterMetadataService.instance.commit(new SetConsensusMigrationTargetProtocol(targetProtocol, tableIds));
    }

    public static void startMigrationToConsensusProtocol(@Nonnull String targetProtocolName,
                                                         @Nonnull List<String> keyspaceNames,
                                                         @Nonnull Optional<List<String>> maybeTables,
                                                         @Nonnull Optional<String> maybeRangesStr)
    {
        checkArgument(!keyspaceNames.isEmpty(), "At least one keyspace must be specified");
        checkArgument(keyspaceNames.size() == 1 || !maybeTables.isPresent(), "Can't specify tables with multiple keyspaces");
        checkArgument(!maybeTables.isPresent() || !maybeTables.get().isEmpty(), "Must provide at least 1 table if Optional is not empty");
        ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromString(targetProtocolName);

        if (DatabaseDescriptor.getLegacyPaxosStrategy() == LegacyPaxosStrategy.accord)
            throw new IllegalStateException("Mixing a hard coded strategy with migration is unsupported");

        if (!Paxos.useV2())
            throw new IllegalStateException("Can't do any consensus migrations to/from PaxosV1, switch to V2 first");

        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Optional<List<Range<Token>>> maybeParsedRanges = maybeRangesStr.map(rangesStr -> ImmutableList.copyOf(RepairOption.parseRanges(rangesStr, partitioner)));
        Token minToken = partitioner.getMinimumToken();
        List<Range<Token>> ranges = maybeParsedRanges.orElse(ImmutableList.of(new Range(minToken, minToken)));

        List<TableId> tableIds = keyspacesAndTablesToTableIds(keyspaceNames, maybeTables);

        ClusterMetadataService.instance.commit(new BeginConsensusMigrationForTableAndRange(targetProtocol, ranges, tableIds));
    }

    private static List<TableId> keyspacesAndTablesToTableIds(List<String> keyspaceNames, Optional<List<String>> maybeTables)
    {
        List<TableId> tableIds = new ArrayList<>();
        // TODO What about indexes, any CFS that should/shouldn't participate?
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
                                                   .collect(toImmutableList())));
        }
        return tableIds;
    }
}
