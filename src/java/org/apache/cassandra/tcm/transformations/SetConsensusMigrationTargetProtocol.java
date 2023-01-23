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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationTarget;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.TableMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadata.Transformer;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationTarget.reset;
import static org.apache.cassandra.tcm.Transformation.Kind.SET_CONSENSUS_MIGRATION_TARGET_PROTOCOL;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.Collectors3.toImmutableMap;

/*
 * Narrowly focused on setting or changing the consensus migration protocol. The real use case
 * is when a migration is already in progress or done and you want to change the target.
 */
public class SetConsensusMigrationTargetProtocol implements Transformation
{
    public static Serializer serializer = new Serializer();

    @Nonnull
    public final ConsensusMigrationTarget targetProtocol;

    @Nonnull
    public final List<TableId> tables;

    public SetConsensusMigrationTargetProtocol(@Nonnull ConsensusMigrationTarget targetProtocol,
                                               @Nonnull List<TableId> tables)
    {
        this.targetProtocol = targetProtocol;
        this.tables = tables;
    }

    @Override
    public Kind kind()
    {
        return SET_CONSENSUS_MIGRATION_TARGET_PROTOCOL;
    }

    @Override
    public Result execute(ClusterMetadata metadata)
    {
        Map<TableId, TableMigrationState> tableStates = metadata.consensusMigrationState.tableStates;
        List<ColumnFamilyStore> columnFamilyStores = tables.stream().map(Schema.instance::getColumnFamilyStoreInstance).collect(toImmutableList());

        Transformer transformer = metadata.transformer();

        Map<TableId, TableMigrationState> newStates;

        if (targetProtocol == reset)
        {
            newStates = tableStates.entrySet().stream().filter(entry -> !tables.contains(entry.getKey())).collect(toImmutableMap());
        }
        else
        {
            newStates = columnFamilyStores
                .stream()
                .map(cfs ->
                     tableStates.containsKey(cfs.getTableId()) ?
                         tableStates.get(cfs.getTableId()).withMigrationTarget(targetProtocol) :
                         new TableMigrationState(cfs.keyspace.getName(), cfs.name, cfs.getTableId(), targetProtocol, ImmutableSet.of(), ImmutableMap.of()))
                .collect(toImmutableMap(TableMigrationState::getTableId, Function.identity()));
        }

        return success(transformer.with(newStates, targetProtocol == reset ? false : true), LockedRanges.AffectedRanges.EMPTY);
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, SetConsensusMigrationTargetProtocol>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            SetConsensusMigrationTargetProtocol v = (SetConsensusMigrationTargetProtocol)t;
            out.writeUTF(v.targetProtocol.toString());
            serializeCollection(v.tables, out, version, TableId.metadataSerializer);
        }

        public SetConsensusMigrationTargetProtocol deserialize(DataInputPlus in, Version version) throws IOException
        {
            ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromString(in.readUTF());
            List<TableId> tables = deserializeList(in, version, TableId.metadataSerializer);
            return new SetConsensusMigrationTargetProtocol(targetProtocol, tables);
        }

        public long serializedSize(Transformation t, Version version)
        {
            SetConsensusMigrationTargetProtocol v = (SetConsensusMigrationTargetProtocol) t;
            return TypeSizes.sizeof(v.targetProtocol.toString())
                 + serializedCollectionSize(v.tables, version, TableId.metadataSerializer);
        }
    }
}
