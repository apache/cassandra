/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.commons.lang.ArrayUtils;

public class MigrationManager implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    // try that many times to send migration request to the node before giving up
    private static final int MIGRATION_REQUEST_RETRIES = 3;

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
        if (Gossiper.instance.getVersion(endpoint) < MessagingService.VERSION_11)
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

    private static void pushSchemaMutation(InetAddress endpoint, Collection<RowMutation> schema)
    {
        try
        {
            Message msg = makeMigrationMessage(schema, Gossiper.instance.getVersion(endpoint));
            MessagingService.instance().sendOneWay(msg, endpoint);
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The list of schema mutations to be applied on the recipient
     */
    public static void announce(Collection<RowMutation> schema)
    {
        for (InetAddress endpoint : Gossiper.instance.getLiveMembers())
        {
            if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                continue; // don't push schema mutation to self

            // don't send migrations to the nodes with the versions older than < 1.1
            if (Gossiper.instance.getVersion(endpoint) < MessagingService.VERSION_11)
                continue;

            pushSchemaMutation(endpoint, schema);
        }
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
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.migration(version));
        logger.debug("Gossiping my schema version " + version);
    }

    /**
     * Serialize given row mutations into raw bytes and make a migration message
     * (other half of transformation is in DefinitionsUpdateResponseVerbHandler.)
     *
     * @param schema The row mutations to send to remote nodes
     * @param version The version to use for message
     *
     * @return Serialized migration containing schema mutations
     *
     * @throws IOException on failed serialization
     */
    private static Message makeMigrationMessage(Collection<RowMutation> schema, int version) throws IOException
    {
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.DEFINITIONS_UPDATE, serializeSchema(schema, version), version);
    }

    /**
     * Serialize given row mutations into raw bytes
     *
     * @param schema The row mutations to serialize
     * @param version The version of the message service to use for serialization
     *
     * @return serialized mutations
     *
     * @throws IOException on failed serialization
     */
    public static byte[] serializeSchema(Collection<RowMutation> schema, int version) throws IOException
    {
        FastByteArrayOutputStream bout = new FastByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeInt(schema.size());

        for (RowMutation mutation : schema)
            RowMutation.serializer().serialize(mutation, dout, version);

        dout.close();

        return bout.toByteArray();
    }

    /**
     * Deserialize migration message considering data compatibility starting from version 1.1
     *
     * @param data The data of the message from coordinator which hold schema mutations to apply
     * @param version The version of the message
     *
     * @return The collection of the row mutations to apply on the node (aka schema)
     *
     * @throws IOException if message is of incompatible version or data is corrupted
     */
    public static Collection<RowMutation> deserializeMigrationMessage(byte[] data, int version) throws IOException
    {
        Collection<RowMutation> schema = new ArrayList<RowMutation>();
        DataInputStream in = new DataInputStream(new FastByteArrayInputStream(data));

        int count = in.readInt();

        for (int i = 0; i < count; i++)
            schema.add(RowMutation.serializer().deserialize(in, version));

        return schema;
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
            FBUtilities.waitOnFutures(new ArrayList<Future<?>>()
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
                if (Gossiper.instance.getVersion(node) >= MessagingService.VERSION_11)
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

    static class MigrationTask extends WrappedRunnable
    {
        private final InetAddress endpoint;

        MigrationTask(InetAddress endpoint)
        {
            this.endpoint = endpoint;
        }

        public void runMayThrow() throws Exception
        {
            Message message = new Message(FBUtilities.getBroadcastAddress(),
                                          StorageService.Verb.MIGRATION_REQUEST,
                                          ArrayUtils.EMPTY_BYTE_ARRAY,
                                          Gossiper.instance.getVersion(endpoint));

            int retries = 0;
            while (retries < MIGRATION_REQUEST_RETRIES)
            {
                if (!FailureDetector.instance.isAlive(endpoint))
                {
                    logger.error("Can't send migration request: node {} is down.", endpoint);
                    return;
                }

                IAsyncResult iar = MessagingService.instance().sendRR(message, endpoint);

                try
                {
                    byte[] reply = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);

                    DefsTable.mergeRemoteSchema(reply, message.getVersion());
                    return;
                }
                catch(TimeoutException e)
                {
                    retries++;
                }
            }
        }
    }
}
