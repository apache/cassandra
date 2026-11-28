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

package org.apache.cassandra.distributed.test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Covers the race between a client establishing a connection and {@code nodetool disablebinary}.
 *
 * A client channel is only registered in {@code Server.ConnectionTracker.allChannels} when the STARTUP
 * message is processed, not when the TCP connection is accepted, so a client which completed its TCP
 * handshake before {@code Server.stop()} but sends STARTUP after it is invisible to the sweep done by
 * {@code closeAll()}. It used to end up as a fully usable connection on a node whose native transport is
 * disabled, which then serves UnavailableException to clients once its datacenter is removed from a
 * keyspace's replication. The tracker now latches itself closed so such a channel is closed on
 * registration, and reopens on {@code start()} so that enablebinary keeps working.
 */
public class DisableBinaryConnectionRaceTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(DisableBinaryConnectionRaceTest.class);

    private static final String KS = "ks_disable_binary_race";
    private static final String DC1 = "datacenter1";
    private static final String DC2 = "datacenter2";

    @BeforeClass
    public static void initClientConfig()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void connectionCompletingHandshakeAfterDisableBinaryMustBeClosed() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP))
                                      .start())
        {
            IInvokableInstance node = cluster.get(1);
            assertTrue("native transport should be running at the start of the test",
                       isNativeTransportRunning(node));

            SimpleClient client = clientFor(node);
            try
            {
                // Step 1: complete only the TCP handshake. The server has accepted the channel and
                // configured its initial pipeline, but no Connection object exists yet, so the channel
                // is not in ConnectionTracker.allChannels.
                establishConnection(client);
                logger.info("TCP connection established, STARTUP not yet sent. Server-side tracked channels: {}",
                            trackedChannels(node));

                // Step 2: disable the native transport. closeAll() only sees the bind channel.
                disableBinary(node);
                logger.info("Native transport stopped. Server-side tracked channels: {}", trackedChannels(node));

                // Control: the listening socket really is closed, so brand new clients cannot connect.
                boolean freshClientConnected = canConnect(node);
                logger.info("CONTROL - a brand new client was able to connect after disablebinary: {} " +
                            "(expected false)", freshClientConnected);
                assertFalse("a brand new client must not be able to connect after disablebinary",
                            freshClientConnected);

                // Step 3: finish the CQL handshake on the socket opened in step 1. This constructs the
                // Connection, which registers the channel into the already-closed ChannelGroup.
                boolean stillUsable = completeHandshakeAndQuery(client);

                logger.info("Server-side tracked channels after the stopped server accepted STARTUP: {}",
                            trackedChannels(node));

                assertFalse("Leaked connection: a client that connected before disablebinary but sent " +
                            "STARTUP after it was able to complete the handshake and execute queries " +
                            "against a node whose native transport is disabled",
                            stillUsable);
            }
            finally
            {
                closeQuietly(client);
            }
        }
    }

    /**
     * The operational consequence of the leak in a two datacenter cluster.
     *
     * An operator drains datacenter2 by disabling the binary protocol on its nodes and then removes
     * datacenter2 from the keyspace replication. Any session that survived the drain because of the
     * race is now pinned to a coordinator whose local datacenter holds no replicas, so every
     * LOCAL_QUORUM query on that session fails with UnavailableException even though the cluster
     * itself is perfectly healthy.
     */
    @Test
    public void leakedConnectionFailsLocalQuorumAfterLocalDcReplicationRemoved() throws Throwable
    {
        try (Cluster cluster = Cluster.build()
                                      .withRacks(2, 1, 2) // nodes 1-2 in datacenter1, nodes 3-4 in datacenter2
                                      .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP, Feature.NETWORK))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = " +
                                 "{'class':'NetworkTopologyStrategy','" + DC1 + "':2,'" + DC2 + "':2}");
            cluster.schemaChange("CREATE TABLE " + KS + ".tbl (pk int PRIMARY KEY, v int)");
            cluster.coordinator(1).execute("INSERT INTO " + KS + ".tbl (pk, v) VALUES (1, 1)",
                                           org.apache.cassandra.distributed.api.ConsistencyLevel.ALL);

            IInvokableInstance drained = cluster.get(3); // datacenter2
            IInvokableInstance healthy = cluster.get(1); // datacenter1

            SimpleClient leaked = clientFor(drained);
            SimpleClient dc1Client = clientFor(healthy);
            try
            {
                // Race the CQL handshake against the drain of datacenter2.
                establishConnection(leaked);
                disableBinary(drained);
                boolean survivedDrain = completeHandshakeAndSelect(leaked);
                logger.info("CONTROL 1 - a session on the drained datacenter2 node is still usable " +
                            "after disablebinary: {} (expected false); channels tracked by that node: {}",
                            survivedDrain, trackedChannels(drained));

                // A normal client connected to datacenter1, unaffected by the drain.
                dc1Client.connect(false);

                // The operator now removes datacenter2 from the keyspace replication.
                cluster.schemaChange("ALTER KEYSPACE " + KS + " WITH replication = " +
                                     "{'class':'NetworkTopologyStrategy','" + DC1 + "':2}");
                logger.info("CONTROL 2 - the drained coordinator now sees {}", localView(drained));

                // Control: the cluster is healthy, datacenter1 clients are unaffected.
                Message.Response fromDc1 = localQuorumSelect(dc1Client);
                logger.info("CONTROL 3 - LOCAL_QUORUM from a datacenter1 client AFTER the ALTER " +
                            "returned {}", describe(fromDc1));
                assertTrue("a datacenter1 client must still serve LOCAL_QUORUM, got " + describe(fromDc1),
                           fromDc1 instanceof ResultMessage);

                // A session that survived the drain is pinned to a coordinator whose local datacenter
                // now holds no replicas, so LOCAL_QUORUM on it fails while the cluster is healthy.
                if (survivedDrain)
                {
                    logger.info("RESULT - LOCAL_QUORUM on the surviving datacenter2 session AFTER the " +
                                "ALTER returned {}", describe(localQuorumSelect(leaked)));
                }

                assertFalse("A session survived disablebinary on the drained datacenter2 node. Once " +
                            "datacenter2 is removed from the keyspace replication, that session serves " +
                            "UnavailableException for every LOCAL_QUORUM query even though the cluster " +
                            "is healthy.",
                            survivedDrain);
            }
            finally
            {
                closeQuietly(leaked);
                closeQuietly(dc1Client);
            }
        }
    }

    /**
     * The tracker latches itself closed on shutdown so that late registrations are closed rather than
     * leaked, which means enablebinary has to clear that latch again. Without the reset the latch would
     * still be set after a restart, and since {@code addConnection} closes any channel that registers
     * while it is set, every client would be disconnected as its STARTUP was handled. The listening
     * socket itself would be unaffected, because {@code start()} adds the bind channel to the group
     * directly rather than through {@code addConnection}, so the node would report the native transport
     * as running and would still accept TCP connections while no client could complete a handshake.
     */
    @Test
    public void nativeTransportCanBeRestartedAfterDisableBinary() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP))
                                      .start())
        {
            IInvokableInstance node = cluster.get(1);

            disableBinary(node);
            assertFalse("clients must not be able to connect while the native transport is disabled",
                        canConnect(node));

            node.runOnInstance((IIsolatedExecutor.SerializableRunnable)
                               () -> StorageService.instance.startNativeTransport());
            assertTrue("native transport should be running again after enablebinary",
                       isNativeTransportRunning(node));

            SimpleClient client = clientFor(node);
            try
            {
                client.connect(false);
                ResultMessage result = client.execute("SELECT key FROM system.local", ConsistencyLevel.ONE);
                logger.info("Query after enablebinary returned {}", result);
                assertTrue("a client must be able to connect and query after enablebinary", result != null);
            }
            finally
            {
                closeQuietly(client);
            }
        }
    }

    /**
     * Completes the CQL handshake and runs one LOCAL_QUORUM query, reporting whether the session is
     * usable at all rather than throwing. A connection closed by the server shows up here as a failure
     * to read a response.
     */
    private static boolean completeHandshakeAndSelect(SimpleClient client)
    {
        try
        {
            completeHandshake(client);
            return localQuorumSelect(client) instanceof ResultMessage;
        }
        catch (Throwable t)
        {
            logger.info("Session on the drained node is unusable: {}", t.toString());
            return false;
        }
    }

    private static Message.Response localQuorumSelect(SimpleClient client)
    {
        QueryMessage query = new QueryMessage("SELECT * FROM " + KS + ".tbl WHERE pk = 1",
                                              QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_QUORUM,
                                                                            Collections.emptyList()));
        return client.execute(query, false);
    }

    /**
     * What the given node believes its own datacenter is, and the replication it has for the test
     * keyspace. Proves the schema change reached the drained node and that its local DC has no replicas.
     */
    private static String localView(IInvokableInstance node)
    {
        return node.callOnInstance((IIsolatedExecutor.SerializableCallable<String>)
                                   () -> "localDc=" + DatabaseDescriptor.getLocalDataCenter() +
                                         ", replication=" + Schema.instance.getKeyspaceMetadata(KS).params.replication);
    }

    private static String describe(Message.Response response)
    {
        if (response instanceof ErrorMessage)
        {
            Throwable error = (Throwable) ((ErrorMessage) response).error;
            return error.getClass().getName() + ": " + error.getMessage();
        }
        return String.valueOf(response);
    }

    private static void disableBinary(IInvokableInstance node)
    {
        node.runOnInstance((IIsolatedExecutor.SerializableRunnable)
                           () -> StorageService.instance.stopNativeTransport());
        assertFalse("native transport should be reported as stopped", isNativeTransportRunning(node));
    }

    private static void completeHandshake(SimpleClient client)
    {
        Map<String, String> options = new HashMap<>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        client.execute(new StartupMessage(options));
    }

    private static boolean completeHandshakeAndQuery(SimpleClient client)
    {
        try
        {
            completeHandshake(client);
            ResultMessage result = client.execute("SELECT key FROM system.local", ConsistencyLevel.ONE);
            logger.info("Query on the connection returned {}", result);
            return result != null;
        }
        catch (Throwable t)
        {
            logger.info("Connection was rejected after the native transport was disabled", t);
            return false;
        }
    }

    private static SimpleClient clientFor(IInvokableInstance node)
    {
        return SimpleClient.builder(node.config().broadcastAddress().getAddress().getHostAddress(),
                                    node.config().getInt("native_transport_port"))
                           .build();
    }

    private static boolean canConnect(IInvokableInstance node)
    {
        SimpleClient client = clientFor(node);
        try
        {
            client.connect(false);
            return true;
        }
        catch (Throwable t)
        {
            return false;
        }
        finally
        {
            closeQuietly(client);
        }
    }

    private static void closeQuietly(SimpleClient client)
    {
        try
        {
            client.close();
        }
        catch (Throwable t)
        {
            // nothing useful to do, the connection is being torn down at the end of the test anyway
        }
    }

    /**
     * {@link SimpleClient#establishConnection()} performs the TCP connect without sending STARTUP,
     * which is exactly the state the race depends on. It is package private, hence the reflection.
     */
    private static void establishConnection(SimpleClient client) throws Exception
    {
        Method establishConnection = SimpleClient.class.getDeclaredMethod("establishConnection");
        establishConnection.setAccessible(true);
        establishConnection.invoke(client);
    }

    private static boolean isNativeTransportRunning(IInvokableInstance node)
    {
        return node.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                   () -> StorageService.instance.isNativeTransportRunning());
    }

    /**
     * Number of channels the node still tracks in {@code Server.ConnectionTracker.allChannels}, summed
     * over every native transport server. While the server is running this includes the bind channel.
     */
    private static int trackedChannels(IInvokableInstance node)
    {
        return node.callOnInstance((IIsolatedExecutor.SerializableCallable<Integer>) () -> {
            try
            {
                Field daemonField = StorageService.class.getDeclaredField("daemon");
                daemonField.setAccessible(true);
                Object daemon = daemonField.get(StorageService.instance);

                Field serviceField = daemon.getClass().getDeclaredField("nativeTransportService");
                serviceField.setAccessible(true);
                Object service = serviceField.get(daemon);

                Field serversField = service.getClass().getDeclaredField("servers");
                serversField.setAccessible(true);
                Collection<?> servers = (Collection<?>) serversField.get(service);

                int total = 0;
                for (Object server : servers)
                {
                    Field trackerField = server.getClass().getDeclaredField("connectionTracker");
                    trackerField.setAccessible(true);
                    Object tracker = trackerField.get(server);

                    Field channelsField = tracker.getClass().getDeclaredField("allChannels");
                    channelsField.setAccessible(true);
                    total += ((Set<?>) channelsField.get(tracker)).size();
                }
                return total;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }
}
