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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.exceptions.ExceptionCode.ALREADY_EXISTS;
import static org.apache.cassandra.exceptions.ExceptionCode.CONFIG_ERROR;
import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.exceptions.ExceptionCode.SERVER_ERROR;
import static org.apache.cassandra.exceptions.ExceptionCode.SYNTAX_ERROR;

public class AlterSchema implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(AlterSchema.class);
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
        Keyspaces newKeyspaces;
        try
        {
            // Applying the schema transformation may produce client warnings. If this is being executed by a follower
            // of the cluster metadata log, there is no client or ClientState, so warning collection is a no-op.
            // When a DDL statement is received from an actual client, the transformation is checked for validation
            // and warnings are captured at that point, before being submitted to the CMS.
            // If the coordinator is a CMS member, then this method will be called as part of committing to the metadata
            // log. In this case, there is a connected client and associated ClientState, so to avoid duplicate warnings
            // pause capture and resume after in applying the schema change.
            schemaTransformation.enterExecution();

            // Guard against an invalid SchemaTransformation supplying a TableMetadata with a future epoch
            newKeyspaces = schemaTransformation.apply(prev);
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
        catch (AlreadyExistsException t)
        {
            return new Rejected(ALREADY_EXISTS, t.getMessage());
        }
        catch (ConfigurationException t)
        {
            return new Rejected(CONFIG_ERROR, t.getMessage());
        }
        catch (InvalidRequestException t)
        {
            return new Rejected(INVALID, t.getMessage());
        }
        catch (SyntaxException t)
        {
            return new Rejected(SYNTAX_ERROR, t.getMessage());
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            return new Rejected(SERVER_ERROR, t.getMessage());
        }
        finally
        {
            schemaTransformation.exitExecution();
        }

        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(prev.schema.getKeyspaces(), newKeyspaces);

        // Used to ensure that any new or modified TableMetadata has the correct epoch
        Epoch nextEpoch = prev.nextEpoch();

        // Used to determine whether this schema change impacts data placements in any way.
        // If so, then reject the change if there are data movement operations inflight, i.e. if any ranges are locked.
        // If not, or if no ranges are locked then the change is permitted and placements recalculated as part of this
        // transformation.
        // Impact on data placements is determined as:
        // * Any new keyspace configured with a previously unused set of replication params
        // * Any existing keyspace with an altered set of replication params
        // * Dropping all keyspaces with a specific set of replication params
        Set<KeyspaceMetadata> affectsPlacements = new HashSet<>();
        Map<ReplicationParams, Set<KeyspaceMetadata>> keyspacesByReplication = groupByReplication(prev.schema.getKeyspaces());

        // Scan dropped keyspaces to check if any existing replication scheme will become unused after this change
        Map<ReplicationParams, Set<KeyspaceMetadata>> intendedToDrop = groupByReplication(diff.dropped);
        intendedToDrop.forEach((replication, keyspaces) -> {
            if (keyspaces.containsAll(keyspacesByReplication.get(replication)))
                affectsPlacements.addAll(keyspaces);
        });

        // Scan new keyspaces to check for any new replication schemes and to ensure that the metadata of new tables
        // in those keyspaces has the correct epoch
        for (KeyspaceMetadata newKSM : diff.created)
        {
            if (!keyspacesByReplication.containsKey(newKSM.params.replication))
                affectsPlacements.add(newKSM);

            Tables tables = Tables.of(normaliseEpochs(nextEpoch, newKSM.tables.stream()));
            newKeyspaces = newKeyspaces.withAddedOrUpdated(newKSM.withSwapped(tables));
        }

        // Scan modified keyspaces to check for replication changes and to ensure that any modified table metadata
        // has the correct epoch
        for (KeyspaceMetadata.KeyspaceDiff alteredKSM : diff.altered)
        {
            if (!alteredKSM.before.params.replication.equals(alteredKSM.after.params.replication))
                affectsPlacements.add(alteredKSM.before);

            Tables tables = Tables.of(alteredKSM.after.tables);
            for (TableMetadata created : normaliseEpochs(nextEpoch, alteredKSM.tables.created.stream()))
                tables = tables.withSwapped(created);

            for (TableMetadata altered : normaliseEpochs(nextEpoch, alteredKSM.tables.altered.stream().map(altered -> altered.after)))
                tables = tables.withSwapped(altered);
            newKeyspaces = newKeyspaces.withAddedOrUpdated(alteredKSM.after.withSwapped(tables));
        }

        // Changes which affect placement (i.e. new, removed or altered replication settings) are not allowed if there
        // are ongoing range movements, including node replacements and partial joins (nodes in write survey mode).
        if (!affectsPlacements.isEmpty())
        {
            logger.debug("Schema change affects data placements, relevant keyspaces: {}", affectsPlacements);
            if (!prev.lockedRanges.locked.isEmpty())
                return new Rejected(INVALID,
                                    String.format("The requested schema changes cannot be executed as they conflict " +
                                                  "with ongoing range movements. The changes for keyspaces %s are blocked " +
                                                  "by the locked ranges %s",
                                                  affectsPlacements.stream().map(k -> k.name).collect(Collectors.joining(",", "[", "]")),
                                                  prev.lockedRanges.locked));

        }

        DistributedSchema snapshotAfter = new DistributedSchema(newKeyspaces);
        ClusterMetadata.Transformer next = prev.transformer().with(snapshotAfter);
        if (!affectsPlacements.isEmpty())
        {
            // state.schema is a DistributedSchema, so doesn't include local keyspaces. If we don't explicitly include those
            // here, their placements won't be calculated, effectively dropping them from the new versioned state
            Keyspaces allKeyspaces = prev.schema.getKeyspaces().withAddedOrReplaced(snapshotAfter.getKeyspaces());
            DataPlacements calculatedPlacements = ClusterMetadataService.instance()
                                                                       .placementProvider()
                                                                       .calculatePlacements(prev.nextEpoch(), prev.tokenMap.toRanges(), prev, allKeyspaces);

            DataPlacements.Builder newPlacementsBuilder = DataPlacements.builder(calculatedPlacements.size());
            calculatedPlacements.forEach((params, newPlacement) -> {
                DataPlacement previousPlacement = prev.placements.get(params);
                // Preserve placement versioning that has resulted from natural application where possible
                if (previousPlacement.equals(newPlacement))
                    newPlacementsBuilder.with(params, previousPlacement);
                else
                    newPlacementsBuilder.with(params, newPlacement);
            });
            next = next.with(newPlacementsBuilder.build());
        }

        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    private static Map<ReplicationParams, Set<KeyspaceMetadata>> groupByReplication(Keyspaces keyspaces)
    {
        Map<ReplicationParams, Set<KeyspaceMetadata>> byReplication = new HashMap<>();
        for (KeyspaceMetadata ksm : keyspaces)
        {
            ReplicationParams params = ksm.params.replication;
            Set<KeyspaceMetadata> forReplication = byReplication.computeIfAbsent(params, p -> new HashSet<>());
            forReplication.add(ksm);
        }
        return byReplication;
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
        return "AlterSchema{" +
               "schemaTransformation=" + schemaTransformation +
               '}';
    }
}
