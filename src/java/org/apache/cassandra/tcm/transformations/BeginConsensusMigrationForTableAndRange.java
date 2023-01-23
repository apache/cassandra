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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.cassandra.service.ConsensusTableMigrationState.ConsensusMigrationTarget;
import static org.apache.cassandra.service.ConsensusTableMigrationState.TableMigrationState;
import static org.apache.cassandra.tcm.ClusterMetadata.Transformer;
import static org.apache.cassandra.utils.Collectors3.toImmutableMap;

public class BeginConsensusMigrationForTableAndRange implements Transformation
{
    public static Serializer serializer = new Serializer();

    @Nonnull
    public final ConsensusMigrationTarget targetProtocol;

    @Nonnull
    public final List<Range<Token>> ranges;

    @Nonnull
    public final List<TableId> tables;

    public BeginConsensusMigrationForTableAndRange(@Nonnull ConsensusMigrationTarget targetProtocol,
                                                   @Nonnull List<Range<Token>> ranges,
                                                   @Nonnull List<TableId> tables)
    {
        checkNotNull(targetProtocol, "targetProtocol should not be null");
        checkNotNull(ranges, "ranges should not be null");
        checkArgument(!ranges.isEmpty(), "ranges should not be empty");
        checkNotNull(tables, "tables should not be null");
        checkArgument(!tables.isEmpty(), "tables should not be empty");
        this.targetProtocol = targetProtocol;
        this.ranges = ranges;
        this.tables = tables;
    }

    public Kind kind()
    {
        return Kind.BEGIN_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE;
    }

    public Result execute(ClusterMetadata metadata)
    {
        Map<TableId, TableMigrationState> tableStates = metadata.migrationStateSnapshot.tableStates;
        List<ColumnFamilyStore> columnFamilyStores = tables.stream().map(Schema.instance::getColumnFamilyStoreInstance).collect(toImmutableList());

        Transformer transformer = metadata.transformer();

        Map<TableId, TableMigrationState> newStates = columnFamilyStores
                .stream()
                .map(cfs ->
                        tableStates.containsKey(cfs.getTableId()) ?
                                tableStates.get(cfs.getTableId()).withRangesMigrating(ranges, targetProtocol, transformer.epoch) :
                                new TableMigrationState(cfs.keyspace.getName(), cfs.name, cfs.getTableId(), targetProtocol, ImmutableSet.of(), ImmutableMap.of(transformer.epoch, ranges)))
                .collect(toImmutableMap(TableMigrationState::getTableId, Function.identity()));

        return new Success(transformer.withConsensusTableMigrationStates(newStates).build(), LockedRanges.AffectedRanges.EMPTY);
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, BeginConsensusMigrationForTableAndRange>
    {

        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            BeginConsensusMigrationForTableAndRange v = (BeginConsensusMigrationForTableAndRange)t;
            out.writeUTF(v.targetProtocol.toString());
            out.writeInt(v.ranges.size());
            for (Range<Token> r : v.ranges)
            {
                Token.metadataSerializer.serialize(r.left, out, version);
                Token.metadataSerializer.serialize(r.right, out, version);
            }
            out.writeInt(v.tables.size());
            for (TableId tableId : v.tables)
                tableId.serialize(out);
        }

        public BeginConsensusMigrationForTableAndRange deserialize(DataInputPlus in, Version version) throws IOException
        {
            ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromString(in.readUTF());
            int numRanges = in.readInt();
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            List<Range<Token>> ranges = new ArrayList<>(numRanges);
            for (int i = 0; i < numRanges; i++)
            {
                ranges.add(new Range<>(Token.metadataSerializer.deserialize(in, partitioner, version),
                                       Token.metadataSerializer.deserialize(in, partitioner, version)));
            }
            int numTables = in.readInt();
            List<TableId> tables = new ArrayList<>(numTables);
            for (int i = 0; i < numTables; i++)
                tables.add(TableId.deserialize(in));
            return new BeginConsensusMigrationForTableAndRange(targetProtocol, ranges, tables);
        }

        public long serializedSize(Transformation t, Version version)
        {
            BeginConsensusMigrationForTableAndRange v = (BeginConsensusMigrationForTableAndRange) t;
            long size = TypeSizes.sizeof(v.targetProtocol.toString())
                    + TypeSizes.sizeof(v.ranges.size())
                    + TypeSizes.sizeof(v.tables.size());
            for (Range<Token> range : v.ranges)
            {
                size += Token.metadataSerializer.serializedSize(range.left, version);
                size += Token.metadataSerializer.serializedSize(range.right, version);
            }
            for (TableId tableId : v.tables)
                size += tableId.serializedSize();
            return size;
        }
    }
}