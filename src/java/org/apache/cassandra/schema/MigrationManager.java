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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.lang.management.ManagementFactory;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.concurrent.Stage.MIGRATION;
import static org.apache.cassandra.net.Verb.SCHEMA_PUSH_REQ;

public class MigrationManager
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    public static final MigrationManager instance = new MigrationManager();

    private static LongSupplier getUptimeFn = () -> ManagementFactory.getRuntimeMXBean().getUptime();

    @VisibleForTesting
    public static void setUptimeFn(LongSupplier supplier)
    {
        getUptimeFn = supplier;
    }

    private static final int MIGRATION_DELAY_IN_MS = 60000;

    private static final int MIGRATION_TASK_WAIT_IN_SECONDS = Integer.parseInt(System.getProperty("cassandra.migration_task_wait_in_seconds", "1"));

    private MigrationManager() {}

    private static boolean shouldPushSchemaTo(InetAddressAndPort endpoint)
    {
        // only push schema to nodes with known and equal versions
        return !endpoint.equals(FBUtilities.getBroadcastAddressAndPort())
               && MessagingService.instance().versions.knows(endpoint)
               && MessagingService.instance().versions.getRaw(endpoint) == MessagingService.current_version;
    }

    public static void announceNewKeyspace(KeyspaceMetadata ksm) throws ConfigurationException
    {
        announceNewKeyspace(ksm, false);
    }

    public static void announceNewKeyspace(KeyspaceMetadata ksm, boolean announceLocally) throws ConfigurationException
    {
        announceNewKeyspace(ksm, FBUtilities.timestampMicros(), announceLocally);
    }

    public static void announceNewKeyspace(KeyspaceMetadata ksm, long timestamp, boolean announceLocally) throws ConfigurationException
    {
        ksm.validate();

        if (SchemaManager.instance.getKeyspaceMetadata(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info("Create new Keyspace: {}", ksm);
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm, timestamp), announceLocally);
    }

    public static void announceNewTable(TableMetadata cfm)
    {
        announceNewTable(cfm, true, FBUtilities.timestampMicros());
    }

    private static void announceNewTable(TableMetadata cfm, boolean throwOnDuplicate, long timestamp)
    {
        cfm.validate();

        KeyspaceMetadata ksm = SchemaManager.instance.getKeyspaceMetadata(cfm.keyspace);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", cfm.name, cfm.keyspace));
        // If we have a table or a view which has the same name, we can't add a new one
        else if (throwOnDuplicate && ksm.getTableOrViewNullable(cfm.name) != null)
            throw new AlreadyExistsException(cfm.keyspace, cfm.name);

        logger.info("Create new table: {}", cfm);
        announce(SchemaKeyspace.makeCreateTableMutation(ksm, cfm, timestamp), false);
    }

    static void announceKeyspaceUpdate(KeyspaceMetadata ksm)
    {
        ksm.validate();

        KeyspaceMetadata oldKsm = SchemaManager.instance.getKeyspaceMetadata(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info("Update Keyspace '{}' From {} To {}", ksm.name, oldKsm, ksm);
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros()), false);
    }

    public static void announceTableUpdate(TableMetadata tm)
    {
        announceTableUpdate(tm, false);
    }

    public static void announceTableUpdate(TableMetadata updated, boolean announceLocally)
    {
        updated.validate();

        TableMetadata current = SchemaManager.instance.getTableMetadata(updated.keyspace, updated.name);
        if (current == null)
            throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", updated.name, updated.keyspace));
        KeyspaceMetadata ksm = SchemaManager.instance.getKeyspaceMetadata(current.keyspace);

        updated.validateCompatibility(current);

        long timestamp = FBUtilities.timestampMicros();

        logger.info("Update table '{}/{}' From {} To {}", current.keyspace, current.name, current, updated);
        Mutation.SimpleBuilder builder = SchemaKeyspace.makeUpdateTableMutation(ksm, current, updated, timestamp);

        announce(builder, announceLocally);
    }

    static void announceKeyspaceDrop(String ksName)
    {
        KeyspaceMetadata oldKsm = SchemaManager.instance.getKeyspaceMetadata(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info("Drop Keyspace '{}'", oldKsm.name);
        announce(SchemaKeyspace.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), false);
    }

    public static void announceTableDrop(String ksName, String cfName, boolean announceLocally)
    {
        TableMetadata tm = SchemaManager.instance.getTableMetadata(ksName, cfName);
        if (tm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", cfName, ksName));
        KeyspaceMetadata ksm = SchemaManager.instance.getKeyspaceMetadata(ksName);

        logger.info("Drop table '{}/{}'", tm.keyspace, tm.name);
        announce(SchemaKeyspace.makeDropTableMutation(ksm, tm, FBUtilities.timestampMicros()), announceLocally);
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    private static void announce(Mutation.SimpleBuilder schema, boolean announceLocally)
    {
        List<Mutation> mutations = Collections.singletonList(schema.build());

        if (announceLocally)
            SchemaManager.instance.merge(mutations);
        else
            announce(mutations);
    }

    public static void announce(Mutation change)
    {
        announce(Collections.singleton(change));
    }

    public static void announce(Collection<Mutation> schema)
    {
        Future<?> f = announceWithoutPush(schema);

        Set<InetAddressAndPort> schemaDestinationEndpoints = new HashSet<>();
        Set<InetAddressAndPort> schemaEndpointsIgnored = new HashSet<>();
        Message<Collection<Mutation>> message = Message.out(SCHEMA_PUSH_REQ, schema);
        for (InetAddressAndPort endpoint : Gossiper.instance.getLiveMembers())
        {
            if (shouldPushSchemaTo(endpoint))
            {
                MessagingService.instance().send(message, endpoint);
                schemaDestinationEndpoints.add(endpoint);
            }
            else
            {
                schemaEndpointsIgnored.add(endpoint);
            }
        }

        SchemaAnnouncementDiagnostics.schemaMutationsAnnounced(schemaDestinationEndpoints, schemaEndpointsIgnored);
        FBUtilities.waitOnFuture(f);
    }

    public static Future<?> announceWithoutPush(Collection<Mutation> schema)
    {
        return MIGRATION.submit(() -> SchemaManager.instance.mergeAndAnnounceVersion(schema));
    }

    public static KeyspacesDiff announce(SchemaTransformation transformation, boolean locally)
    {
        long now = FBUtilities.timestampMicros();

        Future<SchemaManager.TransformationResult> future =
            MIGRATION.submit(() -> SchemaManager.instance.transform(transformation, locally, now));

        SchemaManager.TransformationResult result = Futures.getUnchecked(future);
        if (!result.success)
            throw result.exception;

        if (locally || result.diff.isEmpty())
            return result.diff;

        Set<InetAddressAndPort> schemaDestinationEndpoints = new HashSet<>();
        Set<InetAddressAndPort> schemaEndpointsIgnored = new HashSet<>();
        Message<Collection<Mutation>> message = Message.out(SCHEMA_PUSH_REQ, result.mutations);
        for (InetAddressAndPort endpoint : Gossiper.instance.getLiveMembers())
        {
            if (shouldPushSchemaTo(endpoint))
            {
                MessagingService.instance().send(message, endpoint);
                schemaDestinationEndpoints.add(endpoint);
            }
            else
            {
                schemaEndpointsIgnored.add(endpoint);
            }
        }

        SchemaAnnouncementDiagnostics.schemaTransformationAnnounced(schemaDestinationEndpoints, schemaEndpointsIgnored,
                                                                    transformation);

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

    public static class MigrationsSerializer implements IVersionedSerializer<Collection<Mutation>>
    {
        public static MigrationsSerializer instance = new MigrationsSerializer();

        public void serialize(Collection<Mutation> schema, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(schema.size());
            for (Mutation mutation : schema)
                Mutation.serializer.serialize(mutation, out, version);
        }

        public Collection<Mutation> deserialize(DataInputPlus in, int version) throws IOException
        {
            int count = in.readInt();
            Collection<Mutation> schema = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
                schema.add(Mutation.serializer.deserialize(in, version));

            return schema;
        }

        public long serializedSize(Collection<Mutation> schema, int version)
        {
            int size = TypeSizes.sizeof(schema.size());
            for (Mutation mutation : schema)
                size += mutation.serializedSize(version);
            return size;
        }
    }
}
