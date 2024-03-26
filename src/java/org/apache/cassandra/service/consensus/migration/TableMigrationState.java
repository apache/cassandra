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
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
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
import static org.apache.cassandra.dht.Range.intersectionOfNormalizedRanges;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.dht.Range.subtract;
import static org.apache.cassandra.dht.Range.subtractNormalizedRanges;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeSet;
import static org.apache.cassandra.utils.CollectionSerializers.newHashMap;
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

    @Nonnull
    public final List<Range<Token>> migratedRanges;

    /*
     * Necessary to track which ranges started migrating at which epoch
     * in order to know whether a repair qualifies in terms of finishing
     * migration of the range.
     */
    @Nonnull
    public final NavigableMap<Epoch, List<Range<Token>>> migratingRangesByEpoch;

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
                              .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), ImmutableList.copyOf(normalize(entry.getValue()))))
                              .collect(Collectors.toList()));
        this.migratingRanges = ImmutableList.copyOf(normalize(migratingRangesByEpoch.values().stream().flatMap(Collection::stream).collect(Collectors.toList())));
        this.migratingAndMigratedRanges = ImmutableList.copyOf(normalize(ImmutableList.<Range<Token>>builder().addAll(migratedRanges).addAll(migratingRanges).build()));
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

        return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, newMigratingRanges);
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
            return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, newMigratingRangesByEpoch);
        else
            return this;
    }

    public TableMigrationState withRangesRepairedAtEpoch(@Nonnull Collection<Range<Token>> ranges,
                                                         @Nonnull Epoch epoch)
    {
        checkState(!migratingRangesByEpoch.containsKey(Epoch.EMPTY), "Shouldn't have an entry for the empty epoch");
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

    public TableMigrationState withMigrationTarget(ConsensusMigrationTarget newTargetProtocol)
    {
        checkState(!migratingRangesByEpoch.containsKey(Epoch.EMPTY), "Shouldn't have an entry for the empty epoch");
        if (targetProtocol == newTargetProtocol)
            return this;

        // Migrating ranges remain migrating because individual keys may have already been migrated
        // So for correctness we need to perform key migration
        // We do need to update the epoch so that a new repair is required to drive the migration
        Map<Epoch, List<Range<Token>>> migratingRangesByEpoch = ImmutableMap.of(Epoch.EMPTY, migratingRanges);

        Token minToken = ColumnFamilyStore.getIfExists(tableId).getPartitioner().getMinimumToken();
        Range<Token> fullRange = new Range(minToken, minToken);
        // What is migrated already is anything that was never migrated/migrating before (untouched)
        List<Range<Token>> migratedRanges = ImmutableList.copyOf(normalize(fullRange.subtractAll(migratingAndMigratedRanges)));

        return new TableMigrationState(keyspaceName, tableName, tableId, newTargetProtocol, migratedRanges, migratingRangesByEpoch);
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
            Map<Epoch, List<Range<Token>>> migratingRangesByEpoch = deserializeMap(in, version, Epoch.serializer, ConsensusTableMigration.rangesSerializer, newHashMap());
            return new TableMigrationState(keyspaceName, tableName, tableId, targetProtocol, migratedRanges, migratingRangesByEpoch);
        }

        @Override
        public long serializedSize(TableMigrationState t, Version version)
        {
            return sizeof(t.targetProtocol.value)
                   + sizeof(t.keyspaceName)
                   + sizeof(t.tableName)
                   + t.tableId.serializedSize()
                   + serializedCollectionSize(t.migratedRanges, version, Range.serializer)
                   + serializedMapSize(t.migratingRangesByEpoch, version, Epoch.serializer, ConsensusTableMigration.rangesSerializer);
        }
    };
}
