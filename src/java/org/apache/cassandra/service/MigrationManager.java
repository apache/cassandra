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

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.*;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

public class MigrationManager
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    public static final MigrationManager instance = new MigrationManager();

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    public static final int MIGRATION_DELAY_IN_MS = 60000;

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

    public void scheduleSchemaPull(InetAddress endpoint, EndpointState state)
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
            Runnable runnable = new Runnable()
            {
                public void run()
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
                }
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
                && MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version
                && !Gossiper.instance.isGossipOnlyMember(endpoint);
    }

    public static boolean isReadyForBootstrap()
    {
        return ((ThreadPoolExecutor) StageManager.getStage(Stage.MIGRATION)).getActiveCount() == 0;
    }

    public void notifyCreateKeyspace(KSMetaData ksm)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateKeyspace(ksm.name);
    }

    public void notifyCreateColumnFamily(CFMetaData cfm)
    {
        for (MigrationListener listener : listeners)
            listener.onCreateColumnFamily(cfm.ksName, cfm.cfName);
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

    public void notifyUpdateKeyspace(KSMetaData ksm)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateKeyspace(ksm.name);
    }

    public void notifyUpdateColumnFamily(CFMetaData cfm, boolean columnsDidChange)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateColumnFamily(cfm.ksName, cfm.cfName, columnsDidChange);
    }

    public void notifyUpdateUserType(UserType ut)
    {
        for (MigrationListener listener : listeners)
            listener.onUpdateUserType(ut.keyspace, ut.getNameAsString());
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

    public void notifyDropKeyspace(KSMetaData ksm)
    {
        for (MigrationListener listener : listeners)
            listener.onDropKeyspace(ksm.name);
    }

    public void notifyDropColumnFamily(CFMetaData cfm)
    {
        for (MigrationListener listener : listeners)
            listener.onDropColumnFamily(cfm.ksName, cfm.cfName);
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

    public static void announceNewKeyspace(KSMetaData ksm) throws ConfigurationException
    {
        announceNewKeyspace(ksm, false);
    }

    public static void announceNewKeyspace(KSMetaData ksm, boolean announceLocally) throws ConfigurationException
    {
        announceNewKeyspace(ksm, FBUtilities.timestampMicros(), announceLocally);
    }

    public static void announceNewKeyspace(KSMetaData ksm, long timestamp, boolean announceLocally) throws ConfigurationException
    {
        ksm.validate();

        if (Schema.instance.getKSMetaData(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info(String.format("Create new Keyspace: %s", ksm));
        announce(LegacySchemaTables.makeCreateKeyspaceMutation(ksm, timestamp), announceLocally);
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

        KSMetaData ksm = Schema.instance.getKSMetaData(cfm.ksName);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", cfm.cfName, cfm.ksName));
        else if (throwOnDuplicate && ksm.cfMetaData().containsKey(cfm.cfName))
            throw new AlreadyExistsException(cfm.ksName, cfm.cfName);

        logger.info(String.format("Create new table: %s", cfm));
        announce(LegacySchemaTables.makeCreateTableMutation(ksm, cfm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewType(UserType newType, boolean announceLocally)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(newType.keyspace);
        announce(LegacySchemaTables.makeCreateTypeMutation(ksm, newType, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewFunction(UDFunction udf, boolean announceLocally)
    {
        logger.info(String.format("Create scalar function '%s'", udf.name()));
        KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(LegacySchemaTables.makeCreateFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceNewAggregate(UDAggregate udf, boolean announceLocally)
    {
        logger.info(String.format("Create aggregate function '%s'", udf.name()));
        KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(LegacySchemaTables.makeCreateAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceKeyspaceUpdate(KSMetaData ksm) throws ConfigurationException
    {
        announceKeyspaceUpdate(ksm, false);
    }

    public static void announceKeyspaceUpdate(KSMetaData ksm, boolean announceLocally) throws ConfigurationException
    {
        ksm.validate();

        KSMetaData oldKsm = Schema.instance.getKSMetaData(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info(String.format("Update Keyspace '%s' From %s To %s", ksm.name, oldKsm, ksm));
        announce(LegacySchemaTables.makeCreateKeyspaceMutation(ksm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm) throws ConfigurationException
    {
        announceColumnFamilyUpdate(cfm, false);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm, boolean announceLocally) throws ConfigurationException
    {
        cfm.validate();

        CFMetaData oldCfm = Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", cfm.cfName, cfm.ksName));
        KSMetaData ksm = Schema.instance.getKSMetaData(cfm.ksName);

        oldCfm.validateCompatility(cfm);

        logger.info(String.format("Update table '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
        announce(LegacySchemaTables.makeUpdateTableMutation(ksm, oldCfm, cfm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceTypeUpdate(UserType updatedType, boolean announceLocally)
    {
        announceNewType(updatedType, announceLocally);
    }

    public static void announceKeyspaceDrop(String ksName) throws ConfigurationException
    {
        announceKeyspaceDrop(ksName, false);
    }

    public static void announceKeyspaceDrop(String ksName, boolean announceLocally) throws ConfigurationException
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info(String.format("Drop Keyspace '%s'", oldKsm.name));
        announce(LegacySchemaTables.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), announceLocally);
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
        KSMetaData ksm = Schema.instance.getKSMetaData(ksName);

        logger.info(String.format("Drop table '%s/%s'", oldCfm.ksName, oldCfm.cfName));
        announce(LegacySchemaTables.makeDropTableMutation(ksm, oldCfm, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceTypeDrop(UserType droppedType)
    {
        announceTypeDrop(droppedType, false);
    }

    public static void announceTypeDrop(UserType droppedType, boolean announceLocally)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(droppedType.keyspace);
        announce(LegacySchemaTables.dropTypeFromSchemaMutation(ksm, droppedType, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceFunctionDrop(UDFunction udf, boolean announceLocally)
    {
        logger.info(String.format("Drop scalar function overload '%s' args '%s'", udf.name(), udf.argTypes()));
        KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(LegacySchemaTables.makeDropFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static void announceAggregateDrop(UDAggregate udf, boolean announceLocally)
    {
        logger.info(String.format("Drop aggregate function overload '%s' args '%s'", udf.name(), udf.argTypes()));
        KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
        announce(LegacySchemaTables.makeDropAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    private static void announce(Mutation schema, boolean announceLocally)
    {
        if (announceLocally)
        {
            try
            {
                LegacySchemaTables.mergeSchema(Collections.singletonList(schema), false);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            FBUtilities.waitOnFuture(announce(Collections.singletonList(schema)));
        }
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
            protected void runMayThrow() throws IOException, ConfigurationException
            {
                LegacySchemaTables.mergeSchema(schema);
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
     *
     * @throws IOException if schema tables truncation fails
     */
    public static void resetLocalSchema() throws IOException
    {
        logger.info("Starting local schema reset...");

        logger.debug("Truncating schema tables...");

        LegacySchemaTables.truncateSchemaTables();

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

        public Collection<Mutation> deserialize(DataInput in, int version) throws IOException
        {
            int count = in.readInt();
            Collection<Mutation> schema = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
                schema.add(Mutation.serializer.deserialize(in, version));

            return schema;
        }

        public long serializedSize(Collection<Mutation> schema, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(schema.size());
            for (Mutation mutation : schema)
                size += Mutation.serializer.serializedSize(mutation, version);
            return size;
        }
    }
}
