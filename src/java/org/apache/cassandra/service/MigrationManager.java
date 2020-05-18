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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    // Since 3.0.14 protocol contains only a CASSANDRA-13004 bugfix, it is safe to accept schema changes
    // from both 3.0 and 3.0.14.
    private static boolean is30Compatible(int version)
    {
        return version == MessagingService.current_version || version == MessagingService.VERSION_3014;
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

        logger.info(String.format("Create new table: %s", cfm));
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

        logger.info(String.format("Update table '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
        Mutation mutation = SchemaKeyspace.makeUpdateTableMutation(ksm, oldCfm, cfm, timestamp);

        if (views != null)
            views.forEach(view -> addViewUpdateToMutation(view, mutation, timestamp));

        announce(mutation, announceLocally);
    }

    public static void announceViewUpdate(ViewDefinition view, boolean announceLocally) throws ConfigurationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(view.ksName);
        long timestamp = FBUtilities.timestampMicros();
        Mutation mutation = SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, timestamp);
        addViewUpdateToMutation(view, mutation, timestamp);
        announce(mutation, announceLocally);
    }

    private static void addViewUpdateToMutation(ViewDefinition view, Mutation mutation, long timestamp)
    {
        view.metadata.validate();

        ViewDefinition oldView = Schema.instance.getView(view.ksName, view.viewName);
        if (oldView == null)
            throw new ConfigurationException(String.format("Cannot update non existing materialized view '%s' in keyspace '%s'.", view.viewName, view.ksName));

        oldView.metadata.validateCompatibility(view.metadata);

        logger.info(String.format("Update view '%s/%s' From %s To %s", view.ksName, view.viewName, oldView, view));
        SchemaKeyspace.makeUpdateViewMutation(mutation, oldView, view, timestamp);
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

    static void announceGlobally(Mutation schema)
    {
        announce(Collections.singletonList(schema), false);
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    static void announce(Mutation schema, boolean announceLocally)
    {
        announce(Collections.singletonList(schema), announceLocally);
    }

    static void announce(Collection<Mutation> schema, boolean announceLocally)
    {
        if (announceLocally)
            SchemaKeyspace.mergeSchema(schema);
        else
            FBUtilities.waitOnFuture(announce(schema));
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

        MigrationCoordinator.instance.reset();

        // force migration if there are nodes around
        for (InetAddress node : liveEndpoints)
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
        Mutation mutation = null;

        KeyspaceMetadata definedKeyspace = Schema.instance.getKSMetaData(keyspace.name);
        Tables definedTables = null == definedKeyspace ? Tables.none() : definedKeyspace.tables;

        for (CFMetaData table : keyspace.tables)
        {
            if (table.equals(definedTables.getNullable(table.cfName)))
                continue;

            if (null == mutation)
            {
                // for the keyspace definition itself (name, replication, durability) always use generation 0;
                // this ensures that any changes made to replication by the user will never be overwritten.
                mutation = SchemaKeyspace.makeCreateKeyspaceMutation(keyspace.name, keyspace.params, 0);
            }

            // for table definitions always use the provided generation; these tables, unlike their containing
            // keyspaces, are *NOT* meant to be altered by the user; if their definitions need to change,
            // the schema must be updated in code, and the appropriate generation must be bumped.
            SchemaKeyspace.addTableToSchemaMutation(table, generation, true, mutation);
        }

        return Optional.ofNullable(mutation);
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
