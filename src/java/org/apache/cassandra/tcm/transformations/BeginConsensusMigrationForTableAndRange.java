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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationTarget;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigration;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.tcm.ClusterMetadata.Transformer;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;

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

    public Result execute(ClusterMetadata prev)
    {
        Transformer transformer = prev.transformer();
        Collection<TableMetadata> metadata = tables.stream().map(Schema.instance::getTableMetadata).collect(Collectors.toList());
        ConsensusMigrationState consensusMigrationState = prev.consensusMigrationState.withRangesMigrating(metadata, ranges, false);
        return Transformation.success(transformer.with(consensusMigrationState), LockedRanges.AffectedRanges.EMPTY);
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, BeginConsensusMigrationForTableAndRange>
    {

        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            BeginConsensusMigrationForTableAndRange v = (BeginConsensusMigrationForTableAndRange)t;
            out.writeUTF(v.targetProtocol.toString());
            ConsensusTableMigration.rangesSerializer.serialize(v.ranges, out, version);
            serializeCollection(v.tables, out, version, TableId.metadataSerializer);
        }

        public BeginConsensusMigrationForTableAndRange deserialize(DataInputPlus in, Version version) throws IOException
        {
            ConsensusMigrationTarget targetProtocol = ConsensusMigrationTarget.fromString(in.readUTF());
            List<Range<Token>> ranges = ConsensusTableMigration.rangesSerializer.deserialize(in, version);
            List<TableId> tables = deserializeList(in, version, TableId.metadataSerializer);
           return new BeginConsensusMigrationForTableAndRange(targetProtocol, ranges, tables);
        }

        public long serializedSize(Transformation t, Version version)
        {
            BeginConsensusMigrationForTableAndRange v = (BeginConsensusMigrationForTableAndRange) t;
            return TypeSizes.sizeof(v.targetProtocol.toString())
                   + ConsensusTableMigration.rangesSerializer.serializedSize(v.ranges, version)
                   + serializedCollectionSize(v.tables, version, TableId.metadataSerializer);
        }
    }
}