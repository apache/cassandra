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
import org.apache.cassandra.schema.Tables;
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
        UUID schemaVersion = state.getSchemaVersion();
        if (!endpoint.equals(FBUtilities.getBroadcastAddress()) && schemaVersion != null)
            maybeScheduleSchemaPull(schemaVersion, endpoint, state.getApplicationState(ApplicationState.RELEASE_VERSION).value);
    }

    /**
     * If versions differ this node sends request with local migration list to the endpoint
     * and expecting to receive a list of migrations to apply locally.
     */
    private static void maybeScheduleSchemaPull(final UUID theirVersion, final InetAddress endpoint, String  releaseVersion)
    {
        String ourMajorVersion = FBUtilities.getReleaseVersionMajor();
        if (!releaseVersion.startsWith(ourMajorVersion))
        {
            logger.debug("Not pulling schema because release version in Gossip is not major version {}, it is {}", ourMajorVersion, releaseVersion);
            return;
        }
        if (Schema.instance.getVersion() == null)
        {
            logger.debug("Not pulling schema from {}, because local schama version is not known yet",
                         endpoint);
            return;
        }
        if (Schema.instance.isSameVersion(theirVersion))
        {
            logger.debug("Not pulling schema from {}, because schema versions match: " +
                         "local/real={}, local/compatible={}, remote={}",
                         endpoint,
                         Schema.schemaVersionToString(Schema.instance.getRealVersion()),
                         Schema.schemaVersionToString(Schema.instance.getAltVersion()),
                         Schema.schemaVersionToString(theirVersion));
            return;
        }
        if (!shouldPullSchemaFrom(endpoint))
        {
            logger.debug("Not pulling schema because versions match or shouldPullSchemaFrom returned false");
            return;
        }

        if (Schema.instance.isEmpty() || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Immediately submitting migration task for {}, " +
                         "schema versions: local/real={}, local/compatible={}, remote={}",
                         endpoint,
                         Schema.schemaVersionToString(Schema.instance.getRealVersion()),
                         Schema.schemaVersionToString(Schema.instance.getAltVersion()),
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
                logger.debug("submitting migration task for {}, schema version mismatch: local/real={}, local/compatible={}, remote={}",
                             endpoint,
                             Schema.schemaVersionToString(Schema.instance.getRealVersion()),
                             Schema.schemaVersionToString(Schema.instance.getAltVersion()),
                             Schema.schemaVersionToString(epSchemaVersion));
                submitMigrationTask(endpoint);
            };
            ScheduledExecutors.nonPeriodicTasks.schedule(runnable, MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
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
                && is30Compatible(MessagingService.instance().getRawVersion(endpoint))
                && !Gossiper.instance.isGossipOnlyMember(endpoint);
    }

    // Since 3.0.14 protocol contains only a CASSANDRA-13004 bugfix, it is safe to accept schema changes
    // from both 3.0 and 3.0.14.
    private static boolean is30Compatible(int version)
    {
        return version == MessagingService.current_version || version == MessagingService.VERSION_3014;
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

        logger.info("Create new Keyspace: {}", ksm);
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

    private static void announceNewColumnFamily(CFMetaData cfm, boolean announceLocally, boolean throwOnDuplicate) throws ConfigurationException
    {
        announceNewColumnFamily(cfm, announceLocally, throwOnDuplicate, FBUtilities.timestampMicros());
    }

    private static void announceNewColumnFamily(CFMetaData cfm, boolean announceLocally, boolean throwOnDuplicate, long timestamp) throws ConfigurationException
    {
        cfm.validate();

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(cfm.ksName);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", cfm.cfName, cfm.ksName));
        // If we have a table or a view which has the same name, we can't add a new one
        else if (throwOnDuplicate && ksm.getTableOrViewNullable(cfm.cfName) != null)
            throw new AlreadyExistsException(cfm.ksName, cfm.cfName);

        logger.info("Create new table: {}", cfm);
        announce(SchemaKeyspace.makeCreateTableMutation(ksm, cfm, timestamp), announceLocally);
    }

    public static void announceNewView(ViewDefinition view, boolean announceLocally) throws ConfigurationException
    {
        view.metadata.validate();

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(view.ksName);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", view.viewName, view.ksName));
        else if (ksm.getTableOrViewNullable(view.viewName) != null)
            throw new AlreadyExistsException(view.ksName, view.viewName);

        logger.info("Create new view: {}", view);
        announce(SchemaKeyspace.makeCreateViewMutation(ksm, view, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewType(UserType newType, boolean announceLocally)
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(newType.keyspace);
        announce(SchemaKeyspace.makeCreateTypeMutation(ksm, newType, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewFunction(UDFunction udf, boolean announceLocally)
    {
        logger.info("Create scalar function '{}'", udf.name());
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(SchemaKeyspace.makeCreateFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewAggregate(UDAggregate udf, boolean announceLocally)
    {
        logger.info("Create aggregate function '{}'", udf.name());
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

        logger.info("Update Keyspace '{}' From {} To {}", ksm.name, oldKsm, ksm);
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm) throws ConfigurationException
    {
        announceColumnFamilyUpdate(cfm, false);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm, boolean announceLocally) throws ConfigurationException
    {
        announceColumnFamilyUpdate(cfm, null, announceLocally);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm, Collection<ViewDefinition> views, boolean announceLocally) throws ConfigurationException
    {
        cfm.validate();

        CFMetaData oldCfm = Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", cfm.cfName, cfm.ksName));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(cfm.ksName);

        oldCfm.validateCompatibility(cfm);

        long timestamp = FBUtilities.timestampMicros();

        logger.info("Update table '{}/{}' From {} To {}", cfm.ksName, cfm.cfName, oldCfm, cfm);
        Mutation.SimpleBuilder builder = SchemaKeyspace.makeUpdateTableMutation(ksm, oldCfm, cfm, timestamp);

        if (views != null)
            views.forEach(view -> addViewUpdateToMutationBuilder(view, builder));

        announce(builder, announceLocally);
    }

    public static void announceViewUpdate(ViewDefinition view, boolean announceLocally) throws ConfigurationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(view.ksName);
        long timestamp = FBUtilities.timestampMicros();
        Mutation.SimpleBuilder builder = SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, timestamp);
        addViewUpdateToMutationBuilder(view, builder);
        announce(builder, announceLocally);
    }

    private static void addViewUpdateToMutationBuilder(ViewDefinition view, Mutation.SimpleBuilder builder)
    {
        view.metadata.validate();

        ViewDefinition oldView = Schema.instance.getView(view.ksName, view.viewName);
        if (oldView == null)
            throw new ConfigurationException(String.format("Cannot update non existing materialized view '%s' in keyspace '%s'.", view.viewName, view.ksName));

        oldView.metadata.validateCompatibility(view.metadata);

        logger.info("Update view '{}/{}' From {} To {}", view.ksName, view.viewName, oldView, view);
        SchemaKeyspace.makeUpdateViewMutation(builder, oldView, view);
    }

    public static void announceTypeUpdate(UserType updatedType, boolean announceLocally)
    {
        logger.info("Update type '{}.{}' to {}", updatedType.keyspace, updatedType.getNameAsString(), updatedType);
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

        logger.info("Drop Keyspace '{}'", oldKsm.name);
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

        logger.info("Drop table '{}/{}'", oldCfm.ksName, oldCfm.cfName);
        announce(SchemaKeyspace.makeDropTableMutation(ksm, oldCfm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceViewDrop(String ksName, String viewName, boolean announceLocally) throws ConfigurationException
    {
        ViewDefinition view = Schema.instance.getView(ksName, viewName);
        if (view == null)
            throw new ConfigurationException(String.format("Cannot drop non existing materialized view '%s' in keyspace '%s'.", viewName, ksName));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);

        logger.info("Drop table '{}/{}'", view.ksName, view.viewName);
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
        logger.info("Drop scalar function overload '{}' args '{}'", udf.name(), udf.argTypes());
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(SchemaKeyspace.makeDropFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceAggregateDrop(UDAggregate udf, boolean announceLocally)
    {
        logger.info("Drop aggregate function overload '{}' args '{}'", udf.name(), udf.argTypes());
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(SchemaKeyspace.makeDropAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    static void announceGlobally(Mutation change)
    {
        announceGlobally(Collections.singletonList(change));
    }

    static void announceGlobally(Collection<Mutation> change)
    {
        FBUtilities.waitOnFuture(announce(change));
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    private static void announce(Mutation.SimpleBuilder schema, boolean announceLocally)
    {
        List<Mutation> mutations = Collections.singletonList(schema.build());

        if (announceLocally)
            SchemaKeyspace.mergeSchema(mutations);
        else
            FBUtilities.waitOnFuture(announce(mutations));
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
                is30Compatible(MessagingService.instance().getRawVersion(endpoint)))
                pushSchemaMutation(endpoint, schema);
        }

        return f;
    }

    /**
     * Announce my version passively over gossip.
     * Used to notify nodes as they arrive in the cluster.
     *
     * @param version The schema version to announce
     * @param compatible flag whether {@code version} is a 3.0 compatible version
     */
    public static void passiveAnnounce(UUID version, boolean compatible)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
        logger.debug("Gossiping my {} schema version {}",
                     compatible ? "3.0 compatible" : "3.11",
                     Schema.schemaVersionToString(version));
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

    /**
     * We have a set of non-local, distributed system keyspaces, e.g. system_traces, system_auth, etc.
     * (see {@link Schema#REPLICATED_SYSTEM_KEYSPACE_NAMES}), that need to be created on cluster initialisation,
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
    static Optional<Mutation> evolveSystemKeyspace(KeyspaceMetadata keyspace, long generation)
    {
        Mutation.SimpleBuilder builder = null;

        KeyspaceMetadata definedKeyspace = Schema.instance.getKSMetaData(keyspace.name);
        Tables definedTables = null == definedKeyspace ? Tables.none() : definedKeyspace.tables;

        for (CFMetaData table : keyspace.tables)
        {
            if (table.equals(definedTables.getNullable(table.cfName)))
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
                size += Mutation.serializer.serializedSize(mutation, version);
            return size;
        }
    }
}
