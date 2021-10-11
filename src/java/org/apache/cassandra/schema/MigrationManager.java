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
package org.apache.cassandra.schema;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.concurrent.Stage.MIGRATION;

public class MigrationManager
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    private MigrationManager() {}

    public static KeyspacesDiff announce(SchemaTransformation transformation, boolean locally)
    {
        long now = FBUtilities.timestampMicros();

        Future<SchemaTransformationResult> future =
            MIGRATION.submit(() -> SchemaManager.instance.transform(transformation, locally, now));

        SchemaTransformationResult result = FBUtilities.waitOnFuture(future);

        if (locally || result.diff.isEmpty())
            return result.diff;

        Pair<Set<InetAddressAndPort>, Set<InetAddressAndPort>> endpoints = MigrationCoordinator.instance.pushSchemaMutations(result.mutations);
        SchemaAnnouncementDiagnostics.schemaTransformationAnnounced(endpoints.left(), endpoints.right(), transformation);

        return result.diff;
    }

    /**
     * Clear all locally stored schema information and reset schema to initial state.
     * Called by user (via JMX) who wants to get rid of schema disagreement.
     */
    public static void resetLocalSchema()
    {
        logger.info("Starting local schema reset...");

        logger.debug("Truncating schema tables...");

        SchemaMigrationDiagnostics.resetLocalSchema();

        SchemaKeyspace.truncate();

        logger.debug("Clearing local schema keyspace definitions...");

        SchemaManager.instance.clear();

        Set<InetAddressAndPort> liveEndpoints = Gossiper.instance.getLiveMembers();
        liveEndpoints.remove(FBUtilities.getBroadcastAddressAndPort());

        // force migration if there are nodes around
        for (InetAddressAndPort node : liveEndpoints)
        {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(node);
            Future<Void> pull = MigrationCoordinator.instance.reportEndpointVersion(node, state);
            if (pull != null)
                FBUtilities.waitOnFuture(pull);
        }

        logger.info("Local schema reset is complete.");
    }

    /**
     * We have a set of non-local, distributed system keyspaces, e.g. system_traces, system_auth, etc.
     * (see {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES}), that need to be created on cluster initialisation,
     * and later evolved on major upgrades (sometimes minor too). This method compares the current known definitions
     * of the tables (if the keyspace exists) to the expected, most modern ones expected by the running version of C*;
     * if any changes have been detected, a schema Mutation will be created which, when applied, should make
     * cluster's view of that keyspace aligned with the expected modern definition.
     *
     * @param keyspace   the expected modern definition of the keyspace
     * @param generation timestamp to use for the table changes in the schema mutation
     *
     * @return empty Optional if the current definition is up to date, or an Optional with the Mutation that would
     *         bring the schema in line with the expected definition.
     */
    public static Optional<Mutation> evolveSystemKeyspace(KeyspaceMetadata keyspace, long generation)
    {
        Mutation.SimpleBuilder builder = null;

        KeyspaceMetadata definedKeyspace = SchemaManager.instance.getKeyspaceMetadata(keyspace.name);
        Tables definedTables = null == definedKeyspace ? Tables.none() : definedKeyspace.tables;

        for (TableMetadata table : keyspace.tables)
        {
            if (table.equals(definedTables.getNullable(table.name)))
                continue;

            if (null == builder)
            {
                // for the keyspace definition itself (name, replication, durability) always use generation 0;
                // this ensures that any changes made to replication by the user will never be overwritten.
                builder = SchemaKeyspace.makeCreateKeyspaceMutation(keyspace.name, keyspace.params, 0);

                // now set the timestamp to generation, so the tables have the expected timestamp
                builder.timestamp(generation);
            }

            // for table definitions always use the provided generation; these tables, unlike their containing
            // keyspaces, are *NOT* meant to be altered by the user; if their definitions need to change,
            // the schema must be updated in code, and the appropriate generation must be bumped.
            SchemaKeyspace.addTableToSchemaMutation(table, true, builder);
        }

        return builder == null ? Optional.empty() : Optional.of(builder.build());
    }

}
