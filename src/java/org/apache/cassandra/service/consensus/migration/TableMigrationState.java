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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.dht.NormalizedRanges.normalizedRanges;
import static org.apache.cassandra.dht.Range.subtract;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeSet;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

// TODO Move this into the schema for the table once this is based off of TrM
public class TableMigrationState
{
    private static final Logger logger = LoggerFactory.getLogger(TableMigrationState.class);

    @Nonnull
    public final String keyspaceName;

    @Nonnull
    public final String tableName;

    @Nonnull
    public final TableId tableId;

    @Nonnull
    public final ConsensusMigrationTarget targetProtocol;

    /**
     * Migrated means that both phases are completed when migrating to Accord. Paxos only has one phase.
     */
    @Nonnull
    public final NormalizedRanges<Token> migratedRanges;

    /*
     * Necessary to track which ranges started migrating at which epoch
     * in order to know whether a repair qualifies in terms of finishing
     * migration of the range.
     */
    @Nonnull
    public final NavigableMap<Epoch, NormalizedRanges<Token>> migratingRangesByEpoch;

    /**
     * Ranges that are migrating and could be in either phase when migrating to Accord. Paxos only has one phase.
     */
    @Nonnull
    public final NormalizedRanges<Token> migratingRanges;

    /**
     * These are ranges that are migrating and have not been repaired yet when migrating to Accord so Accord can't read from them. These ranges
     * should continue to be operated on by Paxos
     *
     * When migrating to Accord a repair can only move the range to migrated if it is already in repairCompletedRanges
     *
     * Additionally Paxos continues to operate on a migrating range and key migration is not performed
     *
     * This is skipped when migrating from Accord to Paxos.
     */
    @Nonnull
    public final NormalizedRanges<Token> repairPendingRanges;

    /**
     * Ranges that are migrating could be in either phase when migrating to Accord. PAxos only has one phase.
     */
    @Nonnull
    public final NormalizedRanges<Token> migratingAndMigratedRanges;

    public TableMigrationState(@Nonnull String keyspaceName,
                               @Nonnull String tableName,
                               @Nonnull TableId tableId,
                               @Nonnull ConsensusMigrationTarget targetProtocol,
                               @Nonnull Collection<Range<Token>> migratedRanges,
                               @Nonnull Collection<Range<Token>> repairPendingRanges,
                               @Nonnull Map<Epoch, ? extends List<Range<Token>>> migratingRangesByEpoch)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.targetProtocol = targetProtocol;
        this.migratedRanges = normalizedRanges(migratedRanges);
        this.repairPendingRanges = normalizedRanges(repairPendingRanges);
        this.migratingRangesByEpoch = ImmutableSortedMap.copyOf(
        migratingRangesByEpoch.entrySet()
                              .stream()
                              .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), normalizedRanges(entry.getValue())))
                              .collect(Collectors.toList()));
        this.migratingRanges = normalizedRanges(migratingRangesByEpoch.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        this.migratingAndMigratedRanges = normalizedRanges(ImmutableList.<Range<Token>>builder().addAll(migratedRanges).addAll(migratingRanges).build());
    }

    static List<Range<Token>> initialRepairPendingRanges(ConsensusMigrationTarget target, List<Range<Token>> initialMigratingRanges)
    {
        return target == ConsensusMigrationTarget.accord ? ImmutableList.copyOf(initialMigratingRanges) : ImmutableList.of();
    }

    public TableMigrationState reverseMigration(ConsensusMigrationTarget target, Epoch epoch)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        List<Range<Token>> allTouched = new ArrayList<>(migratedRanges);
        allTouched.addAll(migratingRanges);
        allTouched = Range.deoverlap(allTouched);
        return new TableMigrationState(keyspaceName, tableName, tableId, target,
                                       Range.normalize(fullRange.subtractAll(allTouched)),
                                       initialRepairPendingRanges(target, migratingRanges),
                                       Collections.singletonMap(epoch, migratingRanges));
    }

    public boolean hasMigratedFullTokenRange(IPartitioner partitioner)
    {
        // migrated ranges are normalized
        if (!migratingRanges.isEmpty() || migratedRanges.size() > 1)
            return false;

        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        return migratedRanges.get(0).contains(fullRange);
    }

    @Nonnull
    public List<Range<Token>> migratingRanges() {

        return migratingRanges;
    }

    @Nonnull
    public List<Range<Token>> repairPendingRanges()
    {
        return repairPendingRanges;
    }

    public TableMigrationState withRangesMigrating(@Nonnull Collection<Range<Token>> ranges,
                                                   @Nonnull ConsensusMigrationTarget target)
    {
        checkState(!migratingRangesByEpoch.containsKey(Epoch.EMPTY), "Shouldn't already have an entry for the empty epoch");
        // Doesn't matter which epoch the range started migrating in for this context so merge them all
        Collection<Range<Token>> migratingRanges = normalize(migratingRangesByEpoch.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        checkArgument(target == targetProtocol, "Requested migration to target protocol " + target + " conflicts with in progress migration to protocol " + targetProtocol);
        List<Range<Token>> normalizedRanges = normalize(ranges);
        if (subtract(normalizedRanges, migratingRanges).isEmpty())
            logger.warn("Range " + ranges + " is already being migrated");
        Set<Range<Token>> withoutAlreadyMigrated = subtract(normalizedRanges, migratedRanges);
        if (withoutAlreadyMigrated.isEmpty())
            logger.warn("Range " + ranges + " is already migrated");
        Set<Range<Token>> withoutBoth = subtract(withoutAlreadyMigrated, migratingRanges);
        if (withoutBoth.isEmpty())
            logger.warn("Range " + ranges + " is already migrating/migrated");

        if (!Range.equals(normalizedRanges, withoutBoth))
            logger.warn("Ranges " + normalizedRanges + " to start migrating is already partially migrating/migrated " + withoutBoth);

        Map<Epoch, List<Range<Token>>> newMigratingRanges = new HashMap<>(migratingRangesByEpoch.size() + 1);
        newMigratingRanges.putAll(migratingRangesByEpoch);
        newMigratingRanges.put(Epoch.EMPTY, normalizedRanges);

        List<Range<Token>> newRepairPendingRanges = new ArrayList<>(repairPendingRanges);
        if (target == ConsensusMigrationTarget.accord)
            newRepairPendingRanges.addAll(withoutBoth);

        return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, newRepairPendingRanges, newMigratingRanges);
    }

    public TableMigrationState withReplacementForEmptyEpoch(@Nonnull Epoch replacementEpoch)
    {
        if (!migratingRangesByEpoch.containsKey(Epoch.EMPTY))
            return this;
        Map<Epoch, List<Range<Token>>> newMigratingRangesByEpoch = new HashMap<>(migratingRangesByEpoch.size());
        migratingRangesByEpoch.forEach((epoch, ranges) -> {
            if (epoch.equals(Epoch.EMPTY))
                newMigratingRangesByEpoch.put(replacementEpoch, ranges);
            else
                newMigratingRangesByEpoch.put(epoch, ranges);
        });

        if (newMigratingRangesByEpoch != null)
            return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, repairPendingRanges, newMigratingRangesByEpoch);
        else
            return this;
    }

    public TableMigrationState withRangesRepairedAtEpoch(@Nonnull Collection<Range<Token>> ranges,
                                                         @Nonnull Epoch epoch,
                                                         @Nonnull ConsensusMigrationRepairType repairType)
    {
        checkState(!migratingRangesByEpoch.containsKey(Epoch.EMPTY), "Shouldn't have an entry for the empty epoch");
        checkArgument(epoch.isAfter(Epoch.EMPTY), "Epoch shouldn't be empty");

        NormalizedRanges<Token> normalizedRepairedRanges = normalizedRanges(ranges);
        // This should be inclusive because the epoch we store in the map is the epoch in which the range has been marked migrating
        // in startMigrationToConsensusProtocol
        NavigableMap<Epoch, NormalizedRanges<Token>> coveredEpochs = migratingRangesByEpoch.headMap(epoch, true);
        NormalizedRanges<Token> normalizedMigratingRanges = normalizedRanges(coveredEpochs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        // These are the ranges that are impacted by this repair based on the epoch filtering needed to make sure this repair applies to this migration
        NormalizedRanges<Token> normalizedRepairedIntersection = normalizedRepairedRanges.intersection(normalizedMigratingRanges);
        checkState(!normalizedRepairedIntersection.isEmpty(), "None of Ranges " + ranges + " were being migrated");

        // Any ranges that were repair pending can't actually be fully migrated yet, they will be subtracted from repairPendingRanges after
        // the new migratingRangesByEpoch and migratedRanges are constructed
        NormalizedRanges<Token> actuallyMigratedRanges = normalizedRepairedIntersection.subtract(repairPendingRanges);

        List<Range<Token>> newMigratedRanges = migratedRanges;
        Map<Epoch, NormalizedRanges<Token>> newMigratingRangesByEpoch = migratingRangesByEpoch;

        // Not all repairs are capable of completing the migration to a given target
        if ((targetProtocol == ConsensusMigrationTarget.accord && repairType.repairsPaxos())
            || (targetProtocol == ConsensusMigrationTarget.paxos && repairType.repairedAccord))
        {
            newMigratingRangesByEpoch = new HashMap<>();
            // Everything in this epoch or later can't have been migrated so re-add all of them
            newMigratingRangesByEpoch.putAll(migratingRangesByEpoch.tailMap(epoch, false));
            // Include anything still remaining to be migrated after subtracting what was repaired (and not excluded to due repairPendingRanges)
            for (Map.Entry<Epoch, NormalizedRanges<Token>> e : coveredEpochs.entrySet())
            {
                // Epoch when these ranges started migrating
                Epoch rangesEpoch = e.getKey();
                NormalizedRanges<Token> epochMigratingRanges = e.getValue();
                NormalizedRanges<Token> remainingRanges = epochMigratingRanges.subtract(actuallyMigratedRanges);
                if (!remainingRanges.isEmpty())
                    newMigratingRangesByEpoch.put(rangesEpoch, remainingRanges);
            }

            newMigratedRanges = new ArrayList<>(normalizedMigratingRanges.size() + ranges.size());
            newMigratedRanges.addAll(migratedRanges);
            newMigratedRanges.addAll(actuallyMigratedRanges);
        }

        // After this repair any ranges in normalizedRepairedIntersection is repaired and no longer repair pending
        // Accord can safely read from them if this is a migration to Accord (after Paxos key migration)
        List<Range<Token>> newRepairPendingRanges = repairPendingRanges;
        if (repairType.repairedData)
        {
            List<Range<Token>> repairedRangesSatisfyingEpoch = new ArrayList<>();
            for (Map.Entry<Epoch, NormalizedRanges<Token>> e : coveredEpochs.entrySet())
                repairedRangesSatisfyingEpoch.addAll(normalizedRepairedRanges.intersection(e.getValue()));
            newRepairPendingRanges = repairPendingRanges.subtract(normalizedRanges(repairedRangesSatisfyingEpoch));
        }

        return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, newMigratedRanges, newRepairPendingRanges, newMigratingRangesByEpoch);
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
            return migratingRanges.intersects(key.getToken());
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
        for (Map.Entry<Epoch, NormalizedRanges<Token>> e : migratingRangesByEpoch.entrySet())
        {
            if (e.getValue().intersects(token))
                return e.getKey();
        }
        return Epoch.EMPTY;
    }


    public @Nonnull TableId getTableId()
    {
        return tableId;
    }

    public Map<String, Object> toMap()
    {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("keyspace", keyspaceName);
        builder.put("table", tableName);
        builder.put("tableId", tableId.toString());
        builder.put("targetProtocol", targetProtocol.toString());
        builder.put("migratedRanges", migratedRanges.stream().map(Objects::toString).collect(toImmutableList()));
        Map<Long, List<String>> rangesByEpoch = new LinkedHashMap<>();
        for (Map.Entry<Epoch, NormalizedRanges<Token>> entry : migratingRangesByEpoch.entrySet())
        {
            rangesByEpoch.put(entry.getKey().getEpoch(), entry.getValue().stream().map(Objects::toString).collect(toImmutableList()));
        }
        builder.put("migratingRangesByEpoch", rangesByEpoch);
        builder.put("repairPendingRanges", repairPendingRanges.stream().map(Objects::toString).collect(toImmutableList()));
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

    public static final MetadataSerializer<TableMigrationState> serializer = new MetadataSerializer<TableMigrationState>()
    {
        @Override
        public void serialize(TableMigrationState t, DataOutputPlus out, Version version) throws IOException
        {
            out.write(t.targetProtocol.value);
            out.writeUTF(t.keyspaceName);
            out.writeUTF(t.tableName);
            t.tableId.serialize(out);
            serializeCollection(t.migratedRanges, out, version, Range.serializer);
            serializeCollection(t.repairPendingRanges, out, version, Range.serializer);
            serializeMap(t.migratingRangesByEpoch, out, version, Epoch.serializer, ConsensusTableMigration.rangesSerializer);
        }

        @Override
        public TableMigrationState deserialize(DataInputPlus in, Version version) throws IOException
        {
            ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromValue(in.readByte());
            String keyspaceName = in.readUTF();
            String tableName = in.readUTF();
            TableId tableId = TableId.deserialize(in);
            Set<Range<Token>> migratedRanges = deserializeSet(in, version, Range.serializer);
            Set<Range<Token>> repairPendingRanges = deserializeSet(in, version, Range.serializer);
            Map<Epoch, NormalizedRanges<Token>> migratingRangesByEpoch = deserializeMap(in, version, Epoch.serializer, ConsensusTableMigration.rangesSerializer, Maps::newHashMapWithExpectedSize);
            return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, repairPendingRanges, migratingRangesByEpoch);
        }

        @Override
        public long serializedSize(TableMigrationState t, Version version)
        {
            return sizeof(t.targetProtocol.value)
                   + sizeof(t.keyspaceName)
                   + sizeof(t.tableName)
                   + t.tableId.serializedSize()
                   + serializedCollectionSize(t.migratedRanges, version, Range.serializer)
                   + serializedCollectionSize(t.repairPendingRanges, version, Range.serializer)
                   + serializedMapSize(t.migratingRangesByEpoch, version, Epoch.serializer, ConsensusTableMigration.rangesSerializer);
        }
    };

    @Override
    public String toString()
    {
        return "TableMigrationState{" +
               "keyspaceName='" + keyspaceName + '\'' +
               ", tableName='" + tableName + '\'' +
               ", tableId=" + tableId +
               ", targetProtocol=" + targetProtocol +
               ", migratedRanges=" + migratedRanges +
               ", migratingRangesByEpoch=" + migratingRangesByEpoch +
               ", migratingRanges=" + migratingRanges +
               ", repairPendingRanges=" + repairPendingRanges +
               ", migratingAndMigratedRanges=" + migratingAndMigratedRanges +
               '}';
    }
}
