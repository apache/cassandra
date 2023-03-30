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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class AlterSchema implements Transformation
{
    public static final Serializer serializer = new Serializer();

    public final SchemaTransformation schemaTransformation;
    protected final SchemaProvider schemaProvider;

    public AlterSchema(SchemaTransformation schemaTransformation,
                       SchemaProvider schemaProvider)
    {
        this.schemaTransformation = schemaTransformation;
        this.schemaProvider = schemaProvider;
    }

    @Override
    public Kind kind()
    {
        return Kind.SCHEMA_CHANGE;
    }

    @Override
    public final Result execute(ClusterMetadata prev)
    {
        // TODO: this not necessarily should be the case, we should optimise this, just be careful not to override
        if (!prev.lockedRanges.locked.isEmpty())
            return new Rejected("Can't have schema changes during ring movements: " + prev.lockedRanges.locked);

        Keyspaces newKeyspaces;
        try
        {
            newKeyspaces = schemaTransformation.apply(prev, prev.schema.getKeyspaces());
            newKeyspaces.forEach(ksm -> {
               ksm.tables.forEach(tm -> {
                   if (tm.epoch.isAfter(prev.nextEpoch()))
                       throw new InvalidRequestException(String.format("Invalid schema transformation. " +
                                                                       "Resultant epoch for table metadata of %s.%s (%d) " +
                                                                       "is greater than for cluster metadata (%d)",
                                                                       ksm.name, tm.name, tm.epoch.getEpoch(),
                                                                       prev.nextEpoch().getEpoch()));
               });
            });
        }
        catch (ConfigurationException | InvalidRequestException t)
        {
            return new Rejected(t.getMessage());
        }

        // Ensure that any new or modified TableMetadata has the correct epoch
        Epoch nextEpoch = prev.nextEpoch();
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(prev.schema.getKeyspaces(), newKeyspaces);

        for (KeyspaceMetadata newKSM : diff.created)
        {
            Tables tables = Tables.of(normaliseEpochs(nextEpoch, newKSM.tables.stream()));
            newKeyspaces = newKeyspaces.withAddedOrUpdated(newKSM.withSwapped(tables));
        }

        for (KeyspaceMetadata.KeyspaceDiff alteredKSM : diff.altered)
        {
            Tables tables = Tables.of(alteredKSM.after.tables);
            for (TableMetadata created : normaliseEpochs(nextEpoch, alteredKSM.tables.created.stream()))
                tables = tables.withSwapped(created);

            for (TableMetadata altered : normaliseEpochs(nextEpoch, alteredKSM.tables.altered.stream().map(altered -> altered.after)))
                tables = tables.withSwapped(altered);
            newKeyspaces = newKeyspaces.withAddedOrUpdated(alteredKSM.after.withSwapped(tables));
        }

        DistributedSchema snapshotAfter = new DistributedSchema(newKeyspaces);

        // state.schema is a DistributedSchema, so doesn't include local keyspaces. If we don't explicitly include those
        // here, their placements won't be calculated, effectively dropping them from the new versioned state
        Keyspaces allKeyspaces = prev.schema.getKeyspaces().withAddedOrReplaced(snapshotAfter.getKeyspaces());

        DataPlacements newPlacement = ClusterMetadataService.instance()
                                                            .placementProvider()
                                                            .calculatePlacements(prev.tokenMap.toRanges(), prev, allKeyspaces);

        ClusterMetadata.Transformer next = prev.transformer().with(snapshotAfter).with(newPlacement);

        return success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    private static Iterable<TableMetadata> normaliseEpochs(Epoch nextEpoch, Stream<TableMetadata> tables)
    {
        return tables.map(tm -> tm.epoch.is(nextEpoch)
                                ? tm
                                : tm.unbuild().epoch(nextEpoch).build())
                     .collect(Collectors.toList());
    }


    static class Serializer implements AsymmetricMetadataSerializer<Transformation, AlterSchema>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            SchemaTransformation.serializer.serialize(((AlterSchema) t).schemaTransformation, out, version);
        }

        @Override
        public AlterSchema deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AlterSchema(SchemaTransformation.serializer.deserialize(in, version),
                                   Schema.instance);

        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            return SchemaTransformation.serializer.serializedSize(((AlterSchema) t).schemaTransformation, version);
        }
    }

    @Override
    public String toString()
    {
        return "SchemaChangeRequest{" +
               ", schemaTransformation=" + schemaTransformation +
               '}';
    }
}
