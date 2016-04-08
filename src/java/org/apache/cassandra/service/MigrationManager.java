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
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

public class MigrationManager
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    public static final MigrationManager instance = new MigrationManager();

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    public static final int MIGRATION_DELAY_IN_MS = 60000;

    private static final int MIGRATION_TASK_WAIT_IN_SECONDS = Integer.parseInt(System.getProperty("cassandra.migration_task_wait_in_seconds", "1"));

    private final List<MigrationListener> listeners = new CopyOnWriteArrayList<>();

    private MigrationManager() {}

    public void register(MigrationListener listener)
    {
        listeners.add(listener);
    }

    public void unregister(MigrationListener listener)
    {
        listeners.remove(listener);
    }

    public static void scheduleSchemaPull(InetAddress endpoint, EndpointState state)
    {
        VersionedValue value = state.getApplicationState(ApplicationState.SCHEMA);

        if (!endpoint.equals(FBUtilities.getBroadcastAddress()) && value != null)
            maybeScheduleSchemaPull(UUID.fromString(value.value), endpoint);
    }

    /**
     * If versions differ this node sends request with local migration list to the endpoint
     * and expecting to receive a list of migrations to apply locally.
     */
    private static void maybeScheduleSchemaPull(final UUID theirVersion, final InetAddress endpoint)
    {
        if ((Schema.instance.getVersion() != null && Schema.instance.getVersion().equals(theirVersion)) || !shouldPullSchemaFrom(endpoint))
        {
            logger.debug("Not pulling schema because versions match or shouldPullSchemaFrom returned false");
            return;
        }

        if (Schema.emptyVersion.equals(Schema.instance.getVersion()) || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Submitting migration task for {}", endpoint);
            submitMigrationTask(endpoint);
        }
        else
        {
            // Include a delay to make sure we have a chance to apply any changes being
            // pushed out simultaneously. See CASSANDRA-5025
            Runnable runnable = () ->
            {
                // grab the latest version of the schema since it may have changed again since the initial scheduling
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (epState == null)
                {
                    logger.debug("epState vanished for {}, not submitting migration task", endpoint);
                    return;
                }
                VersionedValue value = epState.getApplicationState(ApplicationState.SCHEMA);
                UUID currentVersion = UUID.fromString(value.value);
                if (Schema.instance.getVersion().equals(currentVersion))
                {
                    logger.debug("not submitting migration task for {} because our versions match", endpoint);
                    return;
                }
                logger.debug("submitting migration task for {}", endpoint);
                submitMigrationTask(endpoint);
            };
            ScheduledExecutors.optionalTasks.schedule(runnable, MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
    }

    private static Future<?> submitMigrationTask(InetAddress endpoint)
    {
        /*
         * Do not de-ref the future because that causes distributed deadlock (CASSANDRA-3832) because we are
         * running in the gossip stage.
         */
        return StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(endpoint));
    }

    public static boolean shouldPullSchemaFrom(InetAddress endpoint)
    {
        /*
         * Don't request schema from nodes with a differnt or unknonw major version (may have incompatible schema)
         * Don't request schema from fat clients
         */
        return MessagingService.instance().knowsVersion(endpoint)
                && MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version
                && !Gossiper.instance.isGossipOnlyMember(endpoint);
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

    public void notifyCreateKeyspace(KeyspaceMetadata ksm)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateKeyspace(ksm.name);
    }

    public void notifyCreateColumnFamily(CFMetaData cfm)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateColumnFamily(cfm.ksName, cfm.cfName);
    }

    public void notifyCreateView(ViewDefinition view)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateView(view.ksName, view.viewName);
    }

    public void notifyCreateUserType(UserType ut)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateUserType(ut.keyspace, ut.getNameAsString());
    }

    public void notifyCreateFunction(UDFunction udf)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateFunction(udf.name().keyspace, udf.name().name, udf.argTypes());
    }

    public void notifyCreateAggregate(UDAggregate udf)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateAggregate(udf.name().keyspace, udf.name().name, udf.argTypes());
    }

    public void notifyUpdateKeyspace(KeyspaceMetadata ksm)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateKeyspace(ksm.name);
    }

    public void notifyUpdateColumnFamily(CFMetaData cfm, boolean columnsDidChange)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateColumnFamily(cfm.ksName, cfm.cfName, columnsDidChange);
    }

    public void notifyUpdateView(ViewDefinition view, boolean columnsDidChange)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateView(view.ksName, view.viewName, columnsDidChange);
    }

    public void notifyUpdateUserType(UserType ut)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateUserType(ut.keyspace, ut.getNameAsString());

        // FIXME: remove when we get rid of AbstractType in metadata. Doesn't really belong anywhere.
        Schema.instance.getKSMetaData(ut.keyspace).functions.udfs().forEach(f -> f.userTypeUpdated(ut.keyspace, ut.getNameAsString()));
    }

    public void notifyUpdateFunction(UDFunction udf)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateFunction(udf.name().keyspace, udf.name().name, udf.argTypes());
    }

    public void notifyUpdateAggregate(UDAggregate udf)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateAggregate(udf.name().keyspace, udf.name().name, udf.argTypes());
    }

    public void notifyDropKeyspace(KeyspaceMetadata ksm)
    {
        for (MigrationListener listener : listeners)
            listener.onDropKeyspace(ksm.name);
    }

    public void notifyDropColumnFamily(CFMetaData cfm)
    {
        for (MigrationListener listener : listeners)
            listener.onDropColumnFamily(cfm.ksName, cfm.cfName);
    }

    public void notifyDropView(ViewDefinition view)
    {
        for (MigrationListener listener : listeners)
            listener.onDropView(view.ksName, view.viewName);
    }

    public void notifyDropUserType(UserType ut)
    {
        for (MigrationListener listener : listeners)
            listener.onDropUserType(ut.keyspace, ut.getNameAsString());
    }

    public void notifyDropFunction(UDFunction udf)
    {
        for (MigrationListener listener : listeners)
            listener.onDropFunction(udf.name().keyspace, udf.name().name, udf.argTypes());
    }

    public void notifyDropAggregate(UDAggregate udf)
    {
        for (MigrationListener listener : listeners)
            listener.onDropAggregate(udf.name().keyspace, udf.name().name, udf.argTypes());
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

        if (Schema.instance.getKSMetaData(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info(String.format("Create new Keyspace: %s", ksm));
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm, timestamp), announceLocally);
    }

    public static void announceNewColumnFamily(CFMetaData cfm) throws ConfigurationException
    {
        announceNewColumnFamily(cfm, false);
    }

    public static void announceNewColumnFamily(CFMetaData cfm, boolean announceLocally) throws ConfigurationException
    {
        announceNewColumnFamily(cfm, announceLocally, true);
    }

    /**
     * Announces the table even if the definition is already know locally.
     * This should generally be avoided but is used internally when we want to force the most up to date version of
     * a system table schema (Note that we don't know if the schema we force _is_ the most recent version or not, we
     * just rely on idempotency to basically ignore that announce if it's not. That's why we can't use announceUpdateColumnFamily,
     * it would for instance delete new columns if this is not called with the most up-to-date version)
     *
     * Note that this is only safe for system tables where we know the cfId is fixed and will be the same whatever version
     * of the definition is used.
     */
    public static void forceAnnounceNewColumnFamily(CFMetaData cfm) throws ConfigurationException
    {
        announceNewColumnFamily(cfm, false, false);
    }

    private static void announceNewColumnFamily(CFMetaData cfm, boolean announceLocally, boolean throwOnDuplicate) throws ConfigurationException
    {
        cfm.validate();

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(cfm.ksName);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", cfm.cfName, cfm.ksName));
        // If we have a table or a view which has the same name, we can't add a new one
        else if (throwOnDuplicate && ksm.getTableOrViewNullable(cfm.cfName) != null)
            throw new AlreadyExistsException(cfm.ksName, cfm.cfName);

        logger.info(String.format("Create new table: %s", cfm));
        announce(SchemaKeyspace.makeCreateTableMutation(ksm, cfm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewView(ViewDefinition view, boolean announceLocally) throws ConfigurationException
    {
        view.metadata.validate();

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(view.ksName);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", view.viewName, view.ksName));
        else if (ksm.getTableOrViewNullable(view.viewName) != null)
            throw new AlreadyExistsException(view.ksName, view.viewName);

        logger.info(String.format("Create new view: %s", view));
        announce(SchemaKeyspace.makeCreateViewMutation(ksm, view, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewType(UserType newType, boolean announceLocally)
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(newType.keyspace);
        announce(SchemaKeyspace.makeCreateTypeMutation(ksm, newType, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewFunction(UDFunction udf, boolean announceLocally)
    {
        logger.info(String.format("Create scalar function '%s'", udf.name()));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(SchemaKeyspace.makeCreateFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewAggregate(UDAggregate udf, boolean announceLocally)
    {
        logger.info(String.format("Create aggregate function '%s'", udf.name()));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(SchemaKeyspace.makeCreateAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceKeyspaceUpdate(KeyspaceMetadata ksm) throws ConfigurationException
    {
        announceKeyspaceUpdate(ksm, false);
    }

    public static void announceKeyspaceUpdate(KeyspaceMetadata ksm, boolean announceLocally) throws ConfigurationException
    {
        ksm.validate();

        KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info(String.format("Update Keyspace '%s' From %s To %s", ksm.name, oldKsm, ksm));
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm, boolean fromThrift) throws ConfigurationException
    {
        announceColumnFamilyUpdate(cfm, fromThrift, false);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm, boolean fromThrift, boolean announceLocally) throws ConfigurationException
    {
        cfm.validate();

        CFMetaData oldCfm = Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", cfm.cfName, cfm.ksName));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(cfm.ksName);

        oldCfm.validateCompatibility(cfm);

        logger.info(String.format("Update table '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
        announce(SchemaKeyspace.makeUpdateTableMutation(ksm, oldCfm, cfm, FBUtilities.timestampMicros(), fromThrift), announceLocally);
    }

    public static void announceViewUpdate(ViewDefinition view, boolean announceLocally) throws ConfigurationException
    {
        view.metadata.validate();

        ViewDefinition oldView = Schema.instance.getView(view.ksName, view.viewName);
        if (oldView == null)
            throw new ConfigurationException(String.format("Cannot update non existing materialized view '%s' in keyspace '%s'.", view.viewName, view.ksName));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(view.ksName);

        oldView.metadata.validateCompatibility(view.metadata);

        logger.info(String.format("Update view '%s/%s' From %s To %s", view.ksName, view.viewName, oldView, view));
        announce(SchemaKeyspace.makeUpdateViewMutation(ksm, oldView, view, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceTypeUpdate(UserType updatedType, boolean announceLocally)
    {
        logger.info(String.format("Update type '%s.%s' to %s", updatedType.keyspace, updatedType.getNameAsString(), updatedType));
        announceNewType(updatedType, announceLocally);
    }

    public static void announceKeyspaceDrop(String ksName) throws ConfigurationException
    {
        announceKeyspaceDrop(ksName, false);
    }

    public static void announceKeyspaceDrop(String ksName, boolean announceLocally) throws ConfigurationException
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info(String.format("Drop Keyspace '%s'", oldKsm.name));
        announce(SchemaKeyspace.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceColumnFamilyDrop(String ksName, String cfName) throws ConfigurationException
    {
        announceColumnFamilyDrop(ksName, cfName, false);
    }

    public static void announceColumnFamilyDrop(String ksName, String cfName, boolean announceLocally) throws ConfigurationException
    {
        CFMetaData oldCfm = Schema.instance.getCFMetaData(ksName, cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", cfName, ksName));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);

        logger.info(String.format("Drop table '%s/%s'", oldCfm.ksName, oldCfm.cfName));
        announce(SchemaKeyspace.makeDropTableMutation(ksm, oldCfm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceViewDrop(String ksName, String viewName, boolean announceLocally) throws ConfigurationException
    {
        ViewDefinition view = Schema.instance.getView(ksName, viewName);
        if (view == null)
            throw new ConfigurationException(String.format("Cannot drop non existing materialized view '%s' in keyspace '%s'.", viewName, ksName));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);

        logger.info(String.format("Drop table '%s/%s'", view.ksName, view.viewName));
        announce(SchemaKeyspace.makeDropViewMutation(ksm, view, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceTypeDrop(UserType droppedType)
    {
        announceTypeDrop(droppedType, false);
    }

    public static void announceTypeDrop(UserType droppedType, boolean announceLocally)
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(droppedType.keyspace);
        announce(SchemaKeyspace.dropTypeFromSchemaMutation(ksm, droppedType, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceFunctionDrop(UDFunction udf, boolean announceLocally)
    {
        logger.info(String.format("Drop scalar function overload '%s' args '%s'", udf.name(), udf.argTypes()));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(SchemaKeyspace.makeDropFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceAggregateDrop(UDAggregate udf, boolean announceLocally)
    {
        logger.info(String.format("Drop aggregate function overload '%s' args '%s'", udf.name(), udf.argTypes()));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(SchemaKeyspace.makeDropAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    private static void announce(Mutation schema, boolean announceLocally)
    {
        if (announceLocally)
            SchemaKeyspace.mergeSchema(Collections.singletonList(schema));
        else
            FBUtilities.waitOnFuture(announce(Collections.singletonList(schema)));
    }

    private static void pushSchemaMutation(InetAddress endpoint, Collection<Mutation> schema)
    {
        MessageOut<Collection<Mutation>> msg = new MessageOut<>(MessagingService.Verb.DEFINITIONS_UPDATE,
                                                                schema,
                                                                MigrationsSerializer.instance);
        MessagingService.instance().sendOneWay(msg, endpoint);
    }

    // Returns a future on the local application of the schema
    private static Future<?> announce(final Collection<Mutation> schema)
    {
        Future<?> f = StageManager.getStage(Stage.MIGRATION).submit(new WrappedRunnable()
        {
            protected void runMayThrow() throws ConfigurationException
            {
                SchemaKeyspace.mergeSchemaAndAnnounceVersion(schema);
            }
        });

        for (InetAddress endpoint : Gossiper.instance.getLiveMembers())
        {
            // only push schema to nodes with known and equal versions
            if (!endpoint.equals(FBUtilities.getBroadcastAddress()) &&
                    MessagingService.instance().knowsVersion(endpoint) &&
                    MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version)
                pushSchemaMutation(endpoint, schema);
        }

        return f;
    }

    /**
     * Announce my version passively over gossip.
     * Used to notify nodes as they arrive in the cluster.
     *
     * @param version The schema version to announce
     */
    public static void passiveAnnounce(UUID version)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
        logger.debug("Gossiping my schema version {}", version);
    }

    /**
     * Clear all locally stored schema information and reset schema to initial state.
     * Called by user (via JMX) who wants to get rid of schema disagreement.
     */
    public static void resetLocalSchema()
    {
        logger.info("Starting local schema reset...");

        logger.debug("Truncating schema tables...");

        SchemaKeyspace.truncate();

        logger.debug("Clearing local schema keyspace definitions...");

        Schema.instance.clear();

        Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
        liveEndpoints.remove(FBUtilities.getBroadcastAddress());

        // force migration if there are nodes around
        for (InetAddress node : liveEndpoints)
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
