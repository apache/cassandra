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
import java.lang.management.RuntimeMXBean;

import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.utils.FBUtilities;

public class MigrationManager
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    public static final MigrationManager instance = new MigrationManager();

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    private static final int MIGRATION_DELAY_IN_MS = 60000;

    private static final int MIGRATION_TASK_WAIT_IN_SECONDS = Integer.parseInt(System.getProperty("cassandra.migration_task_wait_in_seconds", "1"));

    private MigrationManager() {}

    public static void scheduleSchemaPull(InetAddressAndPort endpoint, EndpointState state)
    {
        UUID schemaVersion = state.getSchemaVersion();
        if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort()) && schemaVersion != null)
            maybeScheduleSchemaPull(schemaVersion, endpoint, state.getApplicationState(ApplicationState.RELEASE_VERSION).value);
    }

    /**
     * If versions differ this node sends request with local migration list to the endpoint
     * and expecting to receive a list of migrations to apply locally.
     */
    private static void maybeScheduleSchemaPull(final UUID theirVersion, final InetAddressAndPort endpoint, String releaseVersion)
    {
        String ourMajorVersion = FBUtilities.getReleaseVersionMajor();
        if (!releaseVersion.startsWith(ourMajorVersion))
        {
            logger.debug("Not pulling schema because release version in Gossip is not major version {}, it is {}", ourMajorVersion, releaseVersion);
            return;
        }
        if (Schema.instance.getVersion() == null)
        {
            logger.debug("Not pulling schema from {}, because local schema version is not known yet",
                         endpoint);
            SchemaMigrationDiagnostics.unknownLocalSchemaVersion(endpoint, theirVersion);
            return;
        }
        if (Schema.instance.isSameVersion(theirVersion))
        {
            logger.debug("Not pulling schema from {}, because schema versions match ({})",
                         endpoint,
                         Schema.schemaVersionToString(theirVersion));
            SchemaMigrationDiagnostics.versionMatch(endpoint, theirVersion);
            return;
        }
        if (!shouldPullSchemaFrom(endpoint))
        {
            logger.debug("Not pulling schema from {}, because versions match ({}/{}), or shouldPullSchemaFrom returned false",
                         endpoint, Schema.instance.getVersion(), theirVersion);
            SchemaMigrationDiagnostics.skipPull(endpoint, theirVersion);
            return;
        }

        if (Schema.instance.isEmpty() || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Immediately submitting migration task for {}, " +
                         "schema versions: local={}, remote={}",
                         endpoint,
                         Schema.schemaVersionToString(Schema.instance.getVersion()),
                         Schema.schemaVersionToString(theirVersion));
            submitMigrationTask(endpoint);
        }
        else
        {
            // Include a delay to make sure we have a chance to apply any changes being
            // pushed out simultaneously. See CASSANDRA-5025
            Runnable runnable = () ->
            {
                // grab the latest version of the schema since it may have changed again since the initial scheduling
                UUID epSchemaVersion = Gossiper.instance.getSchemaVersion(endpoint);
                if (epSchemaVersion == null)
                {
                    logger.debug("epState vanished for {}, not submitting migration task", endpoint);
                    return;
                }
                if (Schema.instance.isSameVersion(epSchemaVersion))
                {
                    logger.debug("Not submitting migration task for {} because our versions match ({})", endpoint, epSchemaVersion);
                    return;
                }
                logger.debug("Submitting migration task for {}, schema version mismatch: local={}, remote={}",
                             endpoint,
                             Schema.schemaVersionToString(Schema.instance.getVersion()),
                             Schema.schemaVersionToString(epSchemaVersion));
                submitMigrationTask(endpoint);
            };
            ScheduledExecutors.nonPeriodicTasks.schedule(runnable, MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
    }

    private static Future<?> submitMigrationTask(InetAddressAndPort endpoint)
    {
        /*
         * Do not de-ref the future because that causes distributed deadlock (CASSANDRA-3832) because we are
         * running in the gossip stage.
         */
        return StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(endpoint));
    }

    static boolean shouldPullSchemaFrom(InetAddressAndPort endpoint)
    {
        /*
         * Don't request schema from nodes with a differnt or unknonw major version (may have incompatible schema)
         * Don't request schema from fat clients
         */
        return MessagingService.instance().knowsVersion(endpoint)
                && MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version
                && !Gossiper.instance.isGossipOnlyMember(endpoint);
    }

    private static boolean shouldPushSchemaTo(InetAddressAndPort endpoint)
    {
        // only push schema to nodes with known and equal versions
        return !endpoint.equals(FBUtilities.getBroadcastAddressAndPort())
               && MessagingService.instance().knowsVersion(endpoint)
               && MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version;
    }

    public static boolean isReadyForBootstrap()
    {
        return MigrationTask.getInflightTasks().isEmpty();
    }

    public static void waitUntilReadyForBootstrap()
    {
        CountDownLatch completionLatch;
        while ((completionLatch = MigrationTask.getInflightTasks().poll()) != null)
        {
            try
            {
                if (!completionLatch.await(MIGRATION_TASK_WAIT_IN_SECONDS, TimeUnit.SECONDS))
                    logger.error("Migration task failed to complete");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                logger.error("Migration task was interrupted");
            }
        }
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

        if (Schema.instance.getKeyspaceMetadata(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info("Create new Keyspace: {}", ksm);
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm, timestamp), announceLocally);
    }

    public static void announceNewTable(TableMetadata cfm)
    {
        announceNewTable(cfm, true, FBUtilities.timestampMicros());
    }

    /**
     * Announces the table even if the definition is already know locally.
     * This should generally be avoided but is used internally when we want to force the most up to date version of
     * a system table schema (Note that we don't know if the schema we force _is_ the most recent version or not, we
     * just rely on idempotency to basically ignore that announce if it's not. That's why we can't use announceTableUpdate
     * it would for instance delete new columns if this is not called with the most up-to-date version)
     *
     * Note that this is only safe for system tables where we know the id is fixed and will be the same whatever version
     * of the definition is used.
     */
    public static void forceAnnounceNewTable(TableMetadata cfm)
    {
        announceNewTable(cfm, false, 0);
    }

    private static void announceNewTable(TableMetadata cfm, boolean throwOnDuplicate, long timestamp)
    {
        cfm.validate();

        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(cfm.keyspace);
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

        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksm.name);
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

        TableMetadata current = Schema.instance.getTableMetadata(updated.keyspace, updated.name);
        if (current == null)
            throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", updated.name, updated.keyspace));
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(current.keyspace);

        updated.validateCompatibility(current);

        long timestamp = FBUtilities.timestampMicros();

        logger.info("Update table '{}/{}' From {} To {}", current.keyspace, current.name, current, updated);
        Mutation.SimpleBuilder builder = SchemaKeyspace.makeUpdateTableMutation(ksm, current, updated, timestamp);

        announce(builder, announceLocally);
    }

    static void announceKeyspaceDrop(String ksName)
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info("Drop Keyspace '{}'", oldKsm.name);
        announce(SchemaKeyspace.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), false);
    }

    public static void announceTableDrop(String ksName, String cfName, boolean announceLocally)
    {
        TableMetadata tm = Schema.instance.getTableMetadata(ksName, cfName);
        if (tm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", cfName, ksName));
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);

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
            Schema.instance.merge(mutations);
        else
            announce(mutations);
    }

    private static void pushSchemaMutation(InetAddressAndPort endpoint, Collection<Mutation> schema)
    {
        MessageOut<Collection<Mutation>> msg = new MessageOut<>(MessagingService.Verb.DEFINITIONS_UPDATE,
                                                                schema,
                                                                MigrationsSerializer.instance);
        MessagingService.instance().sendOneWay(msg, endpoint);
    }

    // Returns a future on the local application of the schema
    private static void announce(Collection<Mutation> schema)
    {
        Future<?> f = StageManager.getStage(Stage.MIGRATION).submit(() -> Schema.instance.mergeAndAnnounceVersion(schema));

        Set<InetAddressAndPort> schemaDestinationEndpoints = new HashSet<>();
        Set<InetAddressAndPort> schemaEndpointsIgnored = new HashSet<>();
        for (InetAddressAndPort endpoint : Gossiper.instance.getLiveMembers())
        {
            if (shouldPushSchemaTo(endpoint))
            {
                pushSchemaMutation(endpoint, schema);
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

    public static KeyspacesDiff announce(SchemaTransformation transformation, boolean locally)
    {
        long now = FBUtilities.timestampMicros();

        Future<Schema.TransformationResult> future =
            StageManager.getStage(Stage.MIGRATION).submit(() -> Schema.instance.transform(transformation, locally, now));

        Schema.TransformationResult result = Futures.getUnchecked(future);
        if (!result.success)
            throw result.exception;

        if (locally || result.diff.isEmpty())
            return result.diff;

        Set<InetAddressAndPort> schemaDestinationEndpoints = new HashSet<>();
        Set<InetAddressAndPort> schemaEndpointsIgnored = new HashSet<>();
        for (InetAddressAndPort endpoint : Gossiper.instance.getLiveMembers())
        {
            if (shouldPushSchemaTo(endpoint))
            {
                pushSchemaMutation(endpoint, result.mutations);
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

        Schema.instance.clear();

        Set<InetAddressAndPort> liveEndpoints = Gossiper.instance.getLiveMembers();
        liveEndpoints.remove(FBUtilities.getBroadcastAddressAndPort());

        // force migration if there are nodes around
        for (InetAddressAndPort node : liveEndpoints)
        {
            if (shouldPullSchemaFrom(node))
            {
                logger.debug("Requesting schema from {}", node);
                FBUtilities.waitOnFuture(submitMigrationTask(node));
                break;
            }
        }

        logger.info("Local schema reset is complete.");
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
                size += Mutation.serializer.serializedSize(mutation, version);
            return size;
        }
    }
}
