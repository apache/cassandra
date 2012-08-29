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

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class MigrationManager implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    private static final ByteBuffer LAST_MIGRATION_KEY = ByteBufferUtil.bytes("Last Migration");

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {}

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.SCHEMA || endpoint.equals(FBUtilities.getBroadcastAddress()))
            return;

        rectifySchema(UUID.fromString(value.value), endpoint);
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        VersionedValue value = state.getApplicationState(ApplicationState.SCHEMA);

        if (value != null)
            rectifySchema(UUID.fromString(value.value), endpoint);
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {}

    public void onRestart(InetAddress endpoint, EndpointState state)
    {}

    public void onRemove(InetAddress endpoint)
    {}

    private static void rectifySchema(UUID theirVersion, final InetAddress endpoint)
    {
        // Can't request migrations from nodes with versions younger than 1.1
        if (MessagingService.instance().getVersion(endpoint) < MessagingService.VERSION_11)
            return;

        if (Schema.instance.getVersion().equals(theirVersion))
            return;

        /**
         * if versions differ this node sends request with local migration list to the endpoint
         * and expecting to receive a list of migrations to apply locally.
         *
         * Do not de-ref the future because that causes distributed deadlock (CASSANDRA-3832) because we are
         * running in the gossip stage.
         */
        StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(endpoint));
    }

    public static boolean isReadyForBootstrap()
    {
        return StageManager.getStage(Stage.MIGRATION).getActiveCount() == 0;
    }

    public static void announceNewKeyspace(KSMetaData ksm) throws ConfigurationException
    {
        ksm.validate();

        if (Schema.instance.getTableDefinition(ksm.name) != null)
            throw new ConfigurationException(String.format("Cannot add already existing keyspace '%s'.", ksm.name));

        logger.info(String.format("Create new Keyspace: %s", ksm));
        announce(ksm.toSchema(FBUtilities.timestampMicros()));
    }

    public static void announceNewColumnFamily(CFMetaData cfm) throws ConfigurationException
    {
        cfm.validate();

        KSMetaData ksm = Schema.instance.getTableDefinition(cfm.ksName);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add column family '%s' to non existing keyspace '%s'.", cfm.cfName, cfm.ksName));
        else if (ksm.cfMetaData().containsKey(cfm.cfName))
            throw new ConfigurationException(String.format("Cannot add already existing column family '%s' to keyspace '%s'.", cfm.cfName, cfm.ksName));

        logger.info(String.format("Create new ColumnFamily: %s", cfm));
        announce(cfm.toSchema(FBUtilities.timestampMicros()));
    }

    public static void announceKeyspaceUpdate(KSMetaData ksm) throws ConfigurationException
    {
        ksm.validate();

        KSMetaData oldKsm = Schema.instance.getKSMetaData(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info(String.format("Update Keyspace '%s' From %s To %s", ksm.name, oldKsm, ksm));
        announce(oldKsm.toSchemaUpdate(ksm, FBUtilities.timestampMicros()));
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm) throws ConfigurationException
    {
        cfm.validate();

        CFMetaData oldCfm = Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot update non existing column family '%s' in keyspace '%s'.", cfm.cfName, cfm.ksName));

        logger.info(String.format("Update ColumnFamily '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
        announce(oldCfm.toSchemaUpdate(cfm, FBUtilities.timestampMicros()));
    }

    public static void announceKeyspaceDrop(String ksName) throws ConfigurationException
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info(String.format("Drop Keyspace '%s'", oldKsm.name));
        announce(oldKsm.dropFromSchema(FBUtilities.timestampMicros()));
    }

    public static void announceColumnFamilyDrop(String ksName, String cfName) throws ConfigurationException
    {
        CFMetaData oldCfm = Schema.instance.getCFMetaData(ksName, cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing column family '%s' in keyspace '%s'.", cfName, ksName));

        logger.info(String.format("Drop ColumnFamily '%s/%s'", oldCfm.ksName, oldCfm.cfName));
        announce(oldCfm.dropFromSchema(FBUtilities.timestampMicros()));
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    private static void announce(RowMutation schema)
    {
        FBUtilities.waitOnFuture(announce(Collections.singletonList(schema)));
    }

    private static void pushSchemaMutation(InetAddress endpoint, Collection<RowMutation> schema)
    {
        MessageOut<Collection<RowMutation>> msg = new MessageOut<Collection<RowMutation>>(MessagingService.Verb.DEFINITIONS_UPDATE,
                                                                                          schema,
                                                                                          MigrationsSerializer.instance);
        MessagingService.instance().sendOneWay(msg, endpoint);
    }

    // Returns a future on the local application of the schema
    private static Future<?> announce(final Collection<RowMutation> schema)
    {
        Future<?> f = StageManager.getStage(Stage.MIGRATION).submit(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                DefsTable.mergeSchema(schema);
                return null;
            }
        });

        for (InetAddress endpoint : Gossiper.instance.getLiveMembers())
        {
            if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                continue; // we've delt with localhost already

            // don't send migrations to the nodes with the versions older than < 1.1
            if (MessagingService.instance().getVersion(endpoint) < MessagingService.VERSION_11)
                continue;

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
        assert Gossiper.instance.isEnabled();
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
        logger.debug("Gossiping my schema version " + version);
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

        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Truncating schema tables...");

            // truncate schema tables
            FBUtilities.waitOnFutures(new ArrayList<Future<?>>(3)
            {{
                SystemTable.schemaCFS(SystemTable.SCHEMA_KEYSPACES_CF).truncate();
                SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNFAMILIES_CF).truncate();
                SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNS_CF).truncate();
            }});

            if (logger.isDebugEnabled())
                logger.debug("Clearing local schema keyspace definitions...");

            Schema.instance.clear();

            Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
            liveEndpoints.remove(FBUtilities.getBroadcastAddress());

            // force migration is there are nodes around, first of all
            // check if there are nodes with versions >= 1.1 to request migrations from,
            // because migration format of the nodes with versions < 1.1 is incompatible with older versions
            for (InetAddress node : liveEndpoints)
            {
                if (MessagingService.instance().getVersion(node) >= MessagingService.VERSION_11)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Requesting schema from " + node);

                    FBUtilities.waitOnFuture(StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(node)));
                    break;
                }
            }

            logger.info("Local schema reset is complete.");
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used only in case node has old style migration schema (newly updated)
     * @return the UUID identifying version of the last applied migration
     */
    @Deprecated
    public static UUID getLastMigrationId()
    {
        DecoratedKey dkey = StorageService.getPartitioner().decorateKey(LAST_MIGRATION_KEY);
        Table defs = Table.open(Table.SYSTEM_KS);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(DefsTable.OLD_SCHEMA_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(dkey, new QueryPath(DefsTable.OLD_SCHEMA_CF), LAST_MIGRATION_KEY);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        if (cf == null || cf.getColumnNames().size() == 0)
            return null;
        else
            return UUIDGen.getUUID(cf.getColumn(LAST_MIGRATION_KEY).value());
    }

    public static class MigrationsSerializer implements IVersionedSerializer<Collection<RowMutation>>
    {
        public static MigrationsSerializer instance = new MigrationsSerializer();

        public void serialize(Collection<RowMutation> schema, DataOutput out, int version) throws IOException
        {
            out.writeInt(schema.size());
            for (RowMutation rm : schema)
                RowMutation.serializer.serialize(rm, out, version);
        }

        public Collection<RowMutation> deserialize(DataInput in, int version) throws IOException
        {
            int count = in.readInt();
            Collection<RowMutation> schema = new ArrayList<RowMutation>(count);

            for (int i = 0; i < count; i++)
                schema.add(RowMutation.serializer.deserialize(in, version));

            return schema;
        }

        public long serializedSize(Collection<RowMutation> schema, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(schema.size());
            for (RowMutation rm : schema)
                size += RowMutation.serializer.serializedSize(rm, version);
            return size;
        }
    }
}
