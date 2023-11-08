/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.service;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.gms.GossiperEvent;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class MoveTest
{
    private static final IPartitioner partitioner = RandomPartitioner.instance;
    private static IPartitioner oldPartitioner;
    //Simple Strategy Keyspaces
    private static final String Simple_RF1_KeyspaceName = "MoveTestKeyspace1";
    private static final String Simple_RF2_KeyspaceName = "MoveTestKeyspace5";
    private static final String Simple_RF3_KeyspaceName = "MoveTestKeyspace4";
    private static final String KEYSPACE2 = "MoveTestKeyspace2";
    private static final String KEYSPACE3 = "MoveTestKeyspace3";

    //Network Strategy Keyspace with RF DC1=1 and DC2=1 and so on.
    private static final String Network_11_KeyspaceName = "MoveTestNetwork11";
    private static final String Network_22_KeyspaceName = "MoveTestNetwork22";
    private static final String Network_33_KeyspaceName = "MoveTestNetwork33";

    /*
     * NOTE: the tests above uses RandomPartitioner, which is not the default
     * test partitioner. Changing the partitioner should be done before
     * loading the schema as loading the schema trigger the write of sstables.
     * So instead of extending SchemaLoader, we call it's method below.
     */
    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(partitioner);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition("MoveTest");
        addNetworkTopologyKeyspace(Network_11_KeyspaceName, 1, 1);
        addNetworkTopologyKeyspace(Network_22_KeyspaceName, 2, 2);
        addNetworkTopologyKeyspace(Network_33_KeyspaceName, 3, 3);
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
    }

    @AfterClass
    public static void tearDown()
    {
        StorageService.instance.setPartitionerUnsafe(oldPartitioner);
    }

    @Before
    public void clearTokenMetadata() throws InterruptedException
    {
        // we expect to have a single endpoint before running each test method,
        // so we have to wait for the GossipStage thread to evict stale endpoints
        // from membership before moving on, otherwise it may break other tests as
        // things change in the background
        final int endpointCount = Gossiper.instance.getEndpointCount() - 1;
        final CountDownLatch latch = new CountDownLatch(endpointCount);
        Consumer onEndpointEvicted = event -> latch.countDown();
        DiagnosticEventService.instance().subscribe(GossiperEvent.class,
                                                    GossiperEvent.GossiperEventType.EVICTED_FROM_MEMBERSHIP,
                                                    onEndpointEvicted);

        PendingRangeCalculatorService.instance.blockUntilFinished();
        StorageService.instance.getTokenMetadata().clearUnsafe();

        try
        {
            if (!latch.await(1, TimeUnit.MINUTES))
                throw new RuntimeException("Took too long to evict stale endpoints.");
        }
        finally
        {
            DiagnosticEventService.instance().unsubscribe(onEndpointEvicted);
        }
    }

    private static void addNetworkTopologyKeyspace(String keyspaceName, Integer... replicas) throws Exception
    {

        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            //Odd IPs are in DC1 and Even are in DC2. Endpoints upto .14 will have unique racks and
            // then will be same for a set of three.
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                int ipLastPart = getIPLastPart(endpoint);
                if (ipLastPart <= 14)
                    return UUID.randomUUID().toString();
                else
                    return "RAC" + (ipLastPart % 3);
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                if (getIPLastPart(endpoint) % 2 == 0)
                    return "DC2";
                else
                    return "DC1";
            }

            private int getIPLastPart(InetAddressAndPort endpoint)
            {
                String str = endpoint.toString();
                int index = str.lastIndexOf(".");
                return Integer.parseInt(str.substring(index + 1).trim().split(":")[0]);
            }
        });

        final TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        tmd.clearUnsafe();
        tmd.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.2"));

        KeyspaceMetadata keyspace =  KeyspaceMetadata.create(keyspaceName,
                                                             KeyspaceParams.nts(configOptions(replicas)),
                                                             Tables.of(TableMetadata.builder(keyspaceName, "CF1")
                                                                                    .addPartitionKeyColumn("key", BytesType.instance)
                                                                                    .build()));

        SchemaTestUtil.announceNewKeyspace(keyspace);
    }

    private static Object[] configOptions(Integer[] replicas)
    {
        Object[] configOptions = new Object[(replicas.length * 2)];
        int i = 1, j=0;
        for(Integer replica : replicas)
        {
            if(replica == null)
                continue;
            configOptions[j++] = "DC" + i++;
            configOptions[j++] = replica;
        }
        return configOptions;
    }

    @Test
    public void testMoveWithPendingRangesNetworkStrategyRackAwareThirtyNodes() throws Exception
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 60;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        for(int i=0; i < RING_SIZE/2; i++)
        {
            endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            endpointTokens.add(new BigIntegerToken(String.valueOf((10 * i) + 1)));
        }
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);
        PendingRangeCalculatorService.instance.blockUntilFinished();

        //Moving Endpoint 127.0.0.37 in RAC1 with current token 180
        int MOVING_NODE = 36;
        moveHost(hosts.get(MOVING_NODE), 215, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(150, 151, "127.0.0.43"),
                generatePendingMapEntry(151, 160, "127.0.0.43"),generatePendingMapEntry(160, 161, "127.0.0.43"),
                generatePendingMapEntry(161, 170, "127.0.0.43"), generatePendingMapEntry(170, 171, "127.0.0.43"),
                generatePendingMapEntry(171, 180, "127.0.0.43"), generatePendingMapEntry(210, 211, "127.0.0.37"),
                generatePendingMapEntry(211, 215, "127.0.0.37")), Network_33_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 215, tmd);

        //Moving it back to original spot
        moveHost(hosts.get(MOVING_NODE), 180, tmd, valueFactory);
        finishMove(hosts.get(MOVING_NODE), 180, tmd);

    }

    @Test
    public void testMoveWithPendingRangesNetworkStrategyTenNode() throws Exception
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 14;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        for(int i=0; i < RING_SIZE/2; i++)
        {
            endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            endpointTokens.add(new BigIntegerToken(String.valueOf((10 * i) + 1)));
        }

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);
        PendingRangeCalculatorService.instance.blockUntilFinished();

        int MOVING_NODE = 0;
        moveHost(hosts.get(MOVING_NODE), 5, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 1, "127.0.0.1"),
                generatePendingMapEntry(1, 5, "127.0.0.1")), Network_11_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 1, "127.0.0.1"),
                generatePendingMapEntry(1, 5, "127.0.0.1")), Network_22_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 1, "127.0.0.1"),
                generatePendingMapEntry(1, 5, "127.0.0.1")), Network_33_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 5, tmd);

        moveHost(hosts.get(MOVING_NODE), 0, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 1, "127.0.0.3"),
                generatePendingMapEntry(1, 5, "127.0.0.3")), Network_11_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 1, "127.0.0.5"),
                generatePendingMapEntry(1, 5, "127.0.0.5")), Network_22_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 1, "127.0.0.7"),
                generatePendingMapEntry(1, 5, "127.0.0.7")), Network_33_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 0, tmd);

        MOVING_NODE = 1;
        moveHost(hosts.get(MOVING_NODE), 5, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 5, "127.0.0.2")), Network_11_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 5, "127.0.0.2")), Network_22_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 5, "127.0.0.2")), Network_33_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 5, tmd);

        moveHost(hosts.get(MOVING_NODE), 1, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 5, "127.0.0.4")), Network_11_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 5, "127.0.0.6")), Network_22_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 5, "127.0.0.8")), Network_33_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 1, tmd);

        MOVING_NODE = 3;
        moveHost(hosts.get(MOVING_NODE), 25, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 10, "127.0.0.6"),
                generatePendingMapEntry(10, 11, "127.0.0.6"), generatePendingMapEntry(21, 25, "127.0.0.4")), Network_11_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(61, 0, "127.0.0.6"),
                generatePendingMapEntry(0, 1, "127.0.0.6"), generatePendingMapEntry(21, 25, "127.0.0.4"),
                generatePendingMapEntry(11, 20, "127.0.0.4"),generatePendingMapEntry(20, 21, "127.0.0.4")), Network_22_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(51, 60, "127.0.0.6"),
                generatePendingMapEntry(60, 61, "127.0.0.6"), generatePendingMapEntry(21, 25, "127.0.0.4"),
                generatePendingMapEntry(11, 20, "127.0.0.4"), generatePendingMapEntry(20, 21, "127.0.0.4")), Network_33_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 25, tmd);

        moveHost(hosts.get(MOVING_NODE), 11, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1, 10, "127.0.0.4"),
                generatePendingMapEntry(10, 11, "127.0.0.4"), generatePendingMapEntry(21, 25, "127.0.0.8")), Network_11_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(61, 0, "127.0.0.4"),
                generatePendingMapEntry(0, 1, "127.0.0.4"), generatePendingMapEntry(11, 20, "127.0.0.8"),
                generatePendingMapEntry(20, 21, "127.0.0.8"), generatePendingMapEntry(21, 25, "127.0.0.10")), Network_22_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(51, 60, "127.0.0.4"),
                generatePendingMapEntry(60, 61, "127.0.0.4"), generatePendingMapEntry(21, 25, "127.0.0.12"),
                generatePendingMapEntry(11, 20, "127.0.0.10"), generatePendingMapEntry(20, 21, "127.0.0.10")), Network_33_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 11, tmd);
    }

    @Test
    public void testMoveWithPendingRangesSimpleStrategyTenNode() throws Exception
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 10;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);
        PendingRangeCalculatorService.instance.blockUntilFinished();

        final int MOVING_NODE = 0; // index of the moving node
        moveHost(hosts.get(MOVING_NODE), 2, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.1")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.1")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.1")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 2, tmd);


        moveHost(hosts.get(MOVING_NODE), 0, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.2")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.3")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.4")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 0, tmd);

        moveHost(hosts.get(MOVING_NODE), 1000, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1000, 0, "127.0.0.2")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1000, 0, "127.0.0.3")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1000, 0, "127.0.0.4")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 1000, tmd);

        moveHost(hosts.get(MOVING_NODE), 0, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1000, 0, "127.0.0.1")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1000, 0, "127.0.0.1")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1000, 0, "127.0.0.1")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 0, tmd);

        moveHost(hosts.get(MOVING_NODE), 35, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 35, "127.0.0.1"), generatePendingMapEntry(90, 0, "127.0.0.2")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 35, "127.0.0.1"), generatePendingMapEntry(20, 30, "127.0.0.1"),
                generatePendingMapEntry(80, 90, "127.0.0.2"), generatePendingMapEntry(90, 0, "127.0.0.3")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 35, "127.0.0.1"), generatePendingMapEntry(20, 30, "127.0.0.1"),
                generatePendingMapEntry(80, 90, "127.0.0.3"), generatePendingMapEntry(90, 0, "127.0.0.4"),
                generatePendingMapEntry(10, 20, "127.0.0.1"), generatePendingMapEntry(70, 80, "127.0.0.2")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 35, tmd);

    }

    @Test
    public void testMoveWithPendingRangesForSimpleStrategyFourNode() throws Exception
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 4;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);
        PendingRangeCalculatorService.instance.blockUntilFinished();

        int MOVING_NODE = 0; // index of the moving node
        moveHost(hosts.get(MOVING_NODE), 2, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.1")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.1")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.1")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 2, tmd);


        moveHost(hosts.get(MOVING_NODE), 0, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.2")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.3")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 2, "127.0.0.4")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 0, tmd);

        moveHost(hosts.get(MOVING_NODE), 1500, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1500, 0, "127.0.0.2")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1500, 0, "127.0.0.3")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1500, 0, "127.0.0.4")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 1500, tmd);

        moveHost(hosts.get(MOVING_NODE), 0, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1500, 0, "127.0.0.1")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1500, 0, "127.0.0.1")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(1500, 0, "127.0.0.1")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 0, tmd);

        moveHost(hosts.get(MOVING_NODE), 15, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(10, 15, "127.0.0.1"), generatePendingMapEntry(30, 0, "127.0.0.2")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(20, 30, "127.0.0.2"), generatePendingMapEntry(10, 15, "127.0.0.1"),
                generatePendingMapEntry(0, 10, "127.0.0.1")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(15, 20, "127.0.0.2"),
                generatePendingMapEntry(0, 10, "127.0.0.1")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 15, tmd);

        moveHost(hosts.get(MOVING_NODE), 0, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 0, "127.0.0.1"),
                generatePendingMapEntry(10, 15, "127.0.0.3")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(20, 30, "127.0.0.1"),
                generatePendingMapEntry(10, 15, "127.0.0.4"), generatePendingMapEntry(0, 10, "127.0.0.3")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(15, 20, "127.0.0.1"),
                generatePendingMapEntry(0, 10, "127.0.0.4")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 0, tmd);

        moveHost(hosts.get(MOVING_NODE), 26, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(20, 26, "127.0.0.1"),
                generatePendingMapEntry(30, 0, "127.0.0.2")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(26, 30, "127.0.0.2"),
                generatePendingMapEntry(30, 0, "127.0.0.3"), generatePendingMapEntry(10, 20, "127.0.0.1")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(0, 10, "127.0.0.1"),
                generatePendingMapEntry(26, 30, "127.0.0.3")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 26, tmd);

        moveHost(hosts.get(MOVING_NODE), 0, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(20, 26, "127.0.0.4"),
                generatePendingMapEntry(30, 0, "127.0.0.1")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 0, "127.0.0.1"),
                generatePendingMapEntry(26, 30, "127.0.0.1"), generatePendingMapEntry(10, 20, "127.0.0.4")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(26, 30, "127.0.0.1"),
                generatePendingMapEntry(0, 10, "127.0.0.4")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 0, tmd);

        MOVING_NODE = 3;

        moveHost(hosts.get(MOVING_NODE), 33, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 33, "127.0.0.4")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 33, "127.0.0.4")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 33, "127.0.0.4")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 33, tmd);

        moveHost(hosts.get(MOVING_NODE), 30, tmd, valueFactory);

        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 33, "127.0.0.1")), Simple_RF1_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 33, "127.0.0.2")), Simple_RF2_KeyspaceName);
        assertPendingRanges(tmd, generatePendingRanges(generatePendingMapEntry(30, 33, "127.0.0.3")), Simple_RF3_KeyspaceName);

        finishMove(hosts.get(MOVING_NODE), 30, tmd);
    }

    private void moveHost(InetAddressAndPort host, int token, TokenMetadata tmd, VersionedValue.VersionedValueFactory valueFactory )
    {
        StorageService.instance.onChange(host, ApplicationState.STATUS, valueFactory.moving(new BigIntegerToken(String.valueOf(token))));
        PendingRangeCalculatorService.instance.blockUntilFinished();
        assertTrue(tmd.isMoving(host));
    }

    private void finishMove(InetAddressAndPort host, int token, TokenMetadata tmd)
    {
        tmd.removeFromMoving(host);
        assertTrue(!tmd.isMoving(host));
        Token newToken = new BigIntegerToken(String.valueOf(token));
        tmd.updateNormalToken(newToken, host);
        // As well as upating TMD, update the host's tokens in gossip. Since CASSANDRA-15120, status changing to MOVING
        // ensures that TMD is up to date with token assignments according to gossip. So we need to make sure gossip has
        // the correct new token, as the moving node itself would do upon successful completion of the move operation.
        // Without this, the next movement for that host will set the token in TMD's back to the old value from gossip
        // and incorrect range movements will follow
        Gossiper.instance.injectApplicationState(host,
                                                 ApplicationState.TOKENS,
                                                 new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(newToken)));
    }

    private Map.Entry<Range<Token>, EndpointsForRange> generatePendingMapEntry(int start, int end, String... endpoints) throws UnknownHostException
    {
        Map<Range<Token>, EndpointsForRange> pendingRanges = new HashMap<>();
        Range<Token> range = generateRange(start, end);
        pendingRanges.put(range, makeReplicas(range, endpoints));
        return pendingRanges.entrySet().iterator().next();
    }

    private Map<Range<Token>, EndpointsForRange> generatePendingRanges(Map.Entry<Range<Token>, EndpointsForRange>... entries)
    {
        Map<Range<Token>, EndpointsForRange> pendingRanges = new HashMap<>();
        for(Map.Entry<Range<Token>, EndpointsForRange> entry : entries)
        {
            pendingRanges.put(entry.getKey(), entry.getValue());
        }
        return pendingRanges;
    }

    private void assertPendingRanges(TokenMetadata tmd, Map<Range<Token>, EndpointsForRange> pendingRanges, String keyspaceName) throws ConfigurationException
    {
        boolean keyspaceFound = false;
        for (String nonSystemKeyspaceName : Schema.instance.distributedKeyspaces().names())
        {
            if(!keyspaceName.equals(nonSystemKeyspaceName))
                continue;
            assertMaps(pendingRanges, tmd.getPendingRanges(keyspaceName));
            keyspaceFound = true;
        }

        assert keyspaceFound;
    }

    private void assertMaps(Map<Range<Token>, EndpointsForRange> expected, PendingRangeMaps actual)
    {
        int sizeOfActual = 0;
        Iterator<Map.Entry<Range<Token>, EndpointsForRange.Builder>> iterator = actual.iterator();
        while(iterator.hasNext())
        {
            Map.Entry<Range<Token>, EndpointsForRange.Builder> actualEntry = iterator.next();
            assertNotNull(expected.get(actualEntry.getKey()));
            assertEquals(ImmutableSet.copyOf(expected.get(actualEntry.getKey())), ImmutableSet.copyOf(actualEntry.getValue()));
            sizeOfActual++;
        }

        assertEquals(expected.size(), sizeOfActual);
    }

    /*
     * Test whether write endpoints is correct when the node is moving. Uses
     * StorageService.onChange and does not manipulate token metadata directly.
     */
    @Test
    public void newTestWriteEndpointsDuringMove() throws Exception
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 10;
        final int MOVING_NODE = 3; // index of the moving node

        TokenMetadata tmd = ss.getTokenMetadata();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);

        Map<Token, List<InetAddressAndPort>> expectedEndpoints = new HashMap<>();
        for (Token token : keyTokens)
        {
            List<InetAddressAndPort> endpoints = new ArrayList<>();
            Iterator<Token> tokenIter = TokenMetadata.ringIterator(tmd.sortedTokens(), token, false);
            while (tokenIter.hasNext())
            {
                endpoints.add(tmd.getEndpoint(tokenIter.next()));
            }
            expectedEndpoints.put(token, endpoints);
        }

        // node LEAVING_NODE should move to this token
        Token newToken = positionToken(MOVING_NODE);

        // Third node leaves
        ss.onChange(hosts.get(MOVING_NODE), ApplicationState.STATUS, valueFactory.moving(newToken));
        PendingRangeCalculatorService.instance.blockUntilFinished();

        assertTrue(tmd.isMoving(hosts.get(MOVING_NODE)));

        AbstractReplicationStrategy strategy;
        for (String keyspaceName : Schema.instance.distributedKeyspaces().names())
        {
            strategy = getStrategy(keyspaceName, tmd);
            if(strategy instanceof NetworkTopologyStrategy)
                continue;
            int numMoved = 0;
            for (Token token : keyTokens)
            {
                int replicationFactor = strategy.getReplicationFactor().allReplicas;

                EndpointsForToken actual = tmd.getWriteEndpoints(token, keyspaceName, strategy.calculateNaturalReplicas(token, tmd.cloneOnlyTokenMap()).forToken(token));
                HashSet<InetAddressAndPort> expected = new HashSet<>();

                for (int i = 0; i < replicationFactor; i++)
                {
                    expected.add(expectedEndpoints.get(token).get(i));
                }

                if (expected.size() == actual.size()) {
                	assertEquals("mismatched endpoint sets", expected, actual.endpoints());
                } else {
                	expected.add(hosts.get(MOVING_NODE));
                	assertEquals("mismatched endpoint sets", expected, actual.endpoints());
                	numMoved++;
                }
            }
            assertEquals("mismatched number of moved token", 1, numMoved);
        }

        // moving endpoint back to the normal state
        ss.onChange(hosts.get(MOVING_NODE), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(newToken)));
    }

    /*
     * Test ranges and write endpoints when multiple nodes are on the move simultaneously
     */
    @Test
    public void testSimultaneousMove() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 10;
        TokenMetadata tmd = ss.getTokenMetadata();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 10 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);

        // nodes 6, 8 and 9 leave
        final int[] MOVING = new int[] {6, 8, 9};

        Map<Integer, Token> newTokens = new HashMap<Integer, Token>();

        for (int movingIndex : MOVING)
        {
            Token newToken = positionToken(movingIndex);
            ss.onChange(hosts.get(movingIndex), ApplicationState.STATUS, valueFactory.moving(newToken));

            // storing token associated with a node index
            newTokens.put(movingIndex, newToken);
        }

        tmd = tmd.cloneAfterAllSettled();
        ss.setTokenMetadataUnsafe(tmd);

        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        InetAddressAndPort boot1 = InetAddressAndPort.getByName("127.0.1.1");
        Gossiper.instance.initializeNodeUnsafe(boot1, UUID.randomUUID(), 1);
        Gossiper.instance.injectApplicationState(boot1, ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(5))));
        ss.onChange(boot1,
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(5))));
        PendingRangeCalculatorService.instance.blockUntilFinished();

        InetAddressAndPort boot2 = InetAddressAndPort.getByName("127.0.1.2");
        Gossiper.instance.initializeNodeUnsafe(boot2, UUID.randomUUID(), 1);
        Gossiper.instance.injectApplicationState(boot2, ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(7))));
        ss.onChange(boot2,
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(7))));
        PendingRangeCalculatorService.instance.blockUntilFinished();

        // don't require test update every time a new keyspace is added to test/conf/cassandra.yaml
        Map<String, AbstractReplicationStrategy> keyspaceStrategyMap = new HashMap<String, AbstractReplicationStrategy>();
        for (int i = 1; i <= 4; i++)
        {
            keyspaceStrategyMap.put("MoveTestKeyspace" + i, getStrategy("MoveTestKeyspace" + i, tmd));
        }

       /**
        *  Keyspace1 & Keyspace2 RF=1
        *  {
        *      /127.0.0.1=[(97,0]],
        *      /127.0.0.2=[(0,10]],
        *      /127.0.0.3=[(10,20]],
        *      /127.0.0.4=[(20,30]],
        *      /127.0.0.5=[(30,40]],
        *      /127.0.0.6=[(40,50]],
        *      /127.0.0.7=[(50,67]],
        *      /127.0.0.8=[(67,70]],
        *      /127.0.0.9=[(70,87]],
        *      /127.0.0.10=[(87,97]]
        *  }
        */

        RangesByEndpoint keyspace1ranges = keyspaceStrategyMap.get(Simple_RF1_KeyspaceName).getAddressReplicas();

        assertRanges(keyspace1ranges, "127.0.0.1", 97, 0);
        assertRanges(keyspace1ranges, "127.0.0.2", 0, 10);
        assertRanges(keyspace1ranges, "127.0.0.3", 10, 20);
        assertRanges(keyspace1ranges, "127.0.0.4", 20, 30);
        assertRanges(keyspace1ranges, "127.0.0.5", 30, 40);
        assertRanges(keyspace1ranges, "127.0.0.6", 40, 50);
        assertRanges(keyspace1ranges, "127.0.0.7", 50, 67);
        assertRanges(keyspace1ranges, "127.0.0.8", 67, 70);
        assertRanges(keyspace1ranges, "127.0.0.9", 70, 87);
        assertRanges(keyspace1ranges, "127.0.0.10", 87, 97);


        /**
        * Keyspace3 RF=5
        * {
        *      /127.0.0.1=[(97,0], (70,87], (50,67], (87,97], (67,70]],
        *      /127.0.0.2=[(97,0], (70,87], (87,97], (0,10], (67,70]],
        *      /127.0.0.3=[(97,0], (70,87], (87,97], (0,10], (10,20]],
        *      /127.0.0.4=[(97,0], (20,30], (87,97], (0,10], (10,20]],
        *      /127.0.0.5=[(97,0], (30,40], (20,30], (0,10], (10,20]],
        *      /127.0.0.6=[(40,50], (30,40], (20,30], (0,10], (10,20]],
        *      /127.0.0.7=[(40,50], (30,40], (50,67], (20,30], (10,20]],
        *      /127.0.0.8=[(40,50], (30,40], (50,67], (20,30], (67,70]],
        *      /127.0.0.9=[(40,50], (70,87], (30,40], (50,67], (67,70]],
        *      /127.0.0.10=[(40,50], (70,87], (50,67], (87,97], (67,70]]
        * }
        */

        RangesByEndpoint keyspace3ranges = keyspaceStrategyMap.get(KEYSPACE3).getAddressReplicas();
        assertRanges(keyspace3ranges, "127.0.0.1", 97, 0, 70, 87, 50, 67, 87, 97, 67, 70);
        assertRanges(keyspace3ranges, "127.0.0.2", 97, 0, 70, 87, 87, 97, 0, 10, 67, 70);
        assertRanges(keyspace3ranges, "127.0.0.3", 97, 0, 70, 87, 87, 97, 0, 10, 10, 20);
        assertRanges(keyspace3ranges, "127.0.0.4", 97, 0, 20, 30, 87, 97, 0, 10, 10, 20);
        assertRanges(keyspace3ranges, "127.0.0.5", 97, 0, 30, 40, 20, 30, 0, 10, 10, 20);
        assertRanges(keyspace3ranges, "127.0.0.6", 40, 50, 30, 40, 20, 30, 0, 10, 10, 20);
        assertRanges(keyspace3ranges, "127.0.0.7", 40, 50, 30, 40, 50, 67, 20, 30, 10, 20);
        assertRanges(keyspace3ranges, "127.0.0.8", 40, 50, 30, 40, 50, 67, 20, 30, 67, 70);
        assertRanges(keyspace3ranges, "127.0.0.9", 40, 50, 70, 87, 30, 40, 50, 67, 67, 70);
        assertRanges(keyspace3ranges, "127.0.0.10", 40, 50, 70, 87, 50, 67, 87, 97, 67, 70);


        /**
         * Keyspace4 RF=3
         * {
         *      /127.0.0.1=[(97,0], (70,87], (87,97]],
         *      /127.0.0.2=[(97,0], (87,97], (0,10]],
         *      /127.0.0.3=[(97,0], (0,10], (10,20]],
         *      /127.0.0.4=[(20,30], (0,10], (10,20]],
         *      /127.0.0.5=[(30,40], (20,30], (10,20]],
         *      /127.0.0.6=[(40,50], (30,40], (20,30]],
         *      /127.0.0.7=[(40,50], (30,40], (50,67]],
         *      /127.0.0.8=[(40,50], (50,67], (67,70]],
         *      /127.0.0.9=[(70,87], (50,67], (67,70]],
         *      /127.0.0.10=[(70,87], (87,97], (67,70]]
         *  }
         */
        RangesByEndpoint keyspace4ranges = keyspaceStrategyMap.get(Simple_RF3_KeyspaceName).getAddressReplicas();

        assertRanges(keyspace4ranges, "127.0.0.1", 97, 0, 70, 87, 87, 97);
        assertRanges(keyspace4ranges, "127.0.0.2", 97, 0, 87, 97, 0, 10);
        assertRanges(keyspace4ranges, "127.0.0.3", 97, 0, 0, 10, 10, 20);
        assertRanges(keyspace4ranges, "127.0.0.4", 20, 30, 0, 10, 10, 20);
        assertRanges(keyspace4ranges, "127.0.0.5", 30, 40, 20, 30, 10, 20);
        assertRanges(keyspace4ranges, "127.0.0.6", 40, 50, 30, 40, 20, 30);
        assertRanges(keyspace4ranges, "127.0.0.7", 40, 50, 30, 40, 50, 67);
        assertRanges(keyspace4ranges, "127.0.0.8", 40, 50, 50, 67, 67, 70);
        assertRanges(keyspace4ranges, "127.0.0.9", 70, 87, 50, 67, 67, 70);
        assertRanges(keyspace4ranges, "127.0.0.10", 70, 87, 87, 97, 67, 70);

        // pre-calculate the results.
        Map<String, Multimap<Token, InetAddressAndPort>> expectedEndpoints = new HashMap<>();
        expectedEndpoints.put(Simple_RF1_KeyspaceName, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.7"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.9"));
        expectedEndpoints.get(Simple_RF1_KeyspaceName).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.10"));
        expectedEndpoints.put(KEYSPACE2, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.7"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.9"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.10"));
        expectedEndpoints.put(KEYSPACE3, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.put(Simple_RF3_KeyspaceName, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.1.2"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.1.2"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1"));
        expectedEndpoints.get(Simple_RF3_KeyspaceName).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2"));

        for (Map.Entry<String, AbstractReplicationStrategy> keyspaceStrategy : keyspaceStrategyMap.entrySet())
        {
            String keyspaceName = keyspaceStrategy.getKey();
            AbstractReplicationStrategy strategy = keyspaceStrategy.getValue();

            for (Token token : keyTokens)
            {
                Collection<InetAddressAndPort> endpoints = tmd.getWriteEndpoints(token, keyspaceName, strategy.getNaturalReplicasForToken(token)).endpoints();
                assertEquals(expectedEndpoints.get(keyspaceName).get(token).size(), endpoints.size());
                assertTrue(expectedEndpoints.get(keyspaceName).get(token).containsAll(endpoints));
            }

            // just to be sure that things still work according to the old tests, run them:
            if (strategy.getReplicationFactor().allReplicas != 3)
                continue;

            ReplicaCollection<?> replicas = null;
            // tokens 5, 15 and 25 should go three nodes
            for (int i = 0; i < 3; i++)
            {
                replicas = tmd.getWriteEndpoints(keyTokens.get(i), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(i)));
                assertEquals(3, replicas.size());
                assertTrue(replicas.endpoints().contains(hosts.get(i + 1)));
                assertTrue(replicas.endpoints().contains(hosts.get(i + 2)));
                assertTrue(replicas.endpoints().contains(hosts.get(i + 3)));
            }

            // token 35 should go to nodes 4, 5, 6 and boot1
            replicas = tmd.getWriteEndpoints(keyTokens.get(3), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(3)));
            assertEquals(4, replicas.size());
            assertTrue(replicas.endpoints().contains(hosts.get(4)));
            assertTrue(replicas.endpoints().contains(hosts.get(5)));
            assertTrue(replicas.endpoints().contains(hosts.get(6)));
            assertTrue(replicas.endpoints().contains(boot1));

            // token 45 should go to nodes 5, 6, 7 boot1
            replicas = tmd.getWriteEndpoints(keyTokens.get(4), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(4)));
            assertEquals(4, replicas.size());
            assertTrue(replicas.endpoints().contains(hosts.get(5)));
            assertTrue(replicas.endpoints().contains(hosts.get(6)));
            assertTrue(replicas.endpoints().contains(hosts.get(7)));
            assertTrue(replicas.endpoints().contains(boot1));

            // token 55 should go to nodes 6, 7, 8 boot1 and boot2
            replicas = tmd.getWriteEndpoints(keyTokens.get(5), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(5)));
            assertEquals(5, replicas.size());
            assertTrue(replicas.endpoints().contains(hosts.get(6)));
            assertTrue(replicas.endpoints().contains(hosts.get(7)));
            assertTrue(replicas.endpoints().contains(hosts.get(8)));
            assertTrue(replicas.endpoints().contains(boot1));
            assertTrue(replicas.endpoints().contains(boot2));

            // token 65 should go to nodes 6, 7, 8 and boot2
            replicas = tmd.getWriteEndpoints(keyTokens.get(6), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(6)));
            assertEquals(4, replicas.size());
            assertTrue(replicas.endpoints().contains(hosts.get(6)));
            assertTrue(replicas.endpoints().contains(hosts.get(7)));
            assertTrue(replicas.endpoints().contains(hosts.get(8)));
            assertTrue(replicas.endpoints().contains(boot2));

            // token 75 should to go nodes 8, 9, 0 and boot2
            replicas = tmd.getWriteEndpoints(keyTokens.get(7), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(7)));
            assertEquals(4, replicas.size());
            assertTrue(replicas.endpoints().contains(hosts.get(8)));
            assertTrue(replicas.endpoints().contains(hosts.get(9)));
            assertTrue(replicas.endpoints().contains(hosts.get(0)));
            assertTrue(replicas.endpoints().contains(boot2));

            // token 85 should go to nodes 8, 9 and 0
            replicas = tmd.getWriteEndpoints(keyTokens.get(8), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(8)));
            assertEquals(3, replicas.size());
            assertTrue(replicas.endpoints().contains(hosts.get(8)));
            assertTrue(replicas.endpoints().contains(hosts.get(9)));
            assertTrue(replicas.endpoints().contains(hosts.get(0)));

            // token 95 should go to nodes 9, 0 and 1
            replicas = tmd.getWriteEndpoints(keyTokens.get(9), keyspaceName, strategy.getNaturalReplicasForToken(keyTokens.get(9)));
            assertEquals(3, replicas.size());
            assertTrue(replicas.endpoints().contains(hosts.get(9)));
            assertTrue(replicas.endpoints().contains(hosts.get(0)));
            assertTrue(replicas.endpoints().contains(hosts.get(1)));
        }

        // all moving nodes are back to the normal state
        for (Integer movingIndex : MOVING)
        {
            ss.onChange(hosts.get(movingIndex),
                        ApplicationState.STATUS,
                        valueFactory.normal(Collections.singleton(newTokens.get(movingIndex))));
        }
    }

    @Test
    public void testStateJumpToNormal() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 6 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 6);

        // node 2 leaves
        Token newToken = positionToken(7);
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.moving(newToken));

        assertTrue(tmd.isMoving(hosts.get(2)));
        assertEquals(endpointTokens.get(2), tmd.getTokens(hosts.get(2)).iterator().next());

        // back to normal
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(newToken)));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(newToken)));

        assertTrue(tmd.getSizeOfMovingEndpoints() == 0);
        assertEquals(newToken, tmd.getTokens(hosts.get(2)).iterator().next());

        newToken = positionToken(8);
        // node 2 goes through leave and left and then jumps to normal at its new token
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.moving(newToken));
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(newToken)));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(newToken)));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
        assertTrue(tmd.getSizeOfMovingEndpoints() == 0);
        assertEquals(newToken, tmd.getTokens(hosts.get(2)).iterator().next());
    }

    private static Collection<InetAddressAndPort> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddressAndPort> addrs = new ArrayList<>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddressAndPort.getByName(host));
        return addrs;
    }

    private static EndpointsForRange makeReplicas(Range<Token> range, String... hosts) throws UnknownHostException
    {
        EndpointsForRange.Builder replicas = EndpointsForRange.builder(range, hosts.length);
        for (String host : hosts)
            replicas.add(Replica.fullReplica(InetAddressAndPort.getByName(host), range));
        return replicas.build();
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName, TokenMetadata tmd)
    {
        KeyspaceMetadata ksmd = Schema.instance.getKeyspaceMetadata(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(
                keyspaceName,
                ksmd.params.replication.klass,
                tmd,
                new SimpleSnitch(),
                ksmd.params.replication.options);
    }

    private Token positionToken(int position)
    {
        return new BigIntegerToken(String.valueOf(10 * position + 7));
    }

    private static int collectionSize(Collection<?> collection)
    {
        if (collection.isEmpty())
            return 0;

        Iterator<?> iterator = collection.iterator();

        int count = 0;
        while (iterator.hasNext())
        {
            iterator.next();
            count++;
        }

        return count;
    }

    private Collection<Range<Token>> generateRanges(int... rangePairs)
    {
        if (rangePairs.length % 2 == 1)
            throw new RuntimeException("generateRanges argument count should be even");

        Set<Range<Token>> ranges = new HashSet<Range<Token>>();

        for (int i = 0; i < rangePairs.length; i+=2)
        {
            ranges.add(generateRange(rangePairs[i], rangePairs[i+1]));
        }

        return ranges;
    }

    private static Token tk(int v)
    {
        return new BigIntegerToken(String.valueOf(v));
    }

    private static Range<Token> generateRange(int left, int right)
    {
        return new Range<Token>(tk(left), tk(right));
    }

    private static Replica replica(InetAddressAndPort endpoint, int left, int right, boolean full)
    {
        return new Replica(endpoint, tk(left), tk(right), full);
    }

    private static InetAddressAndPort inet(String name)
    {
        try
        {
            return InetAddressAndPort.getByName(name);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static Replica replica(InetAddressAndPort endpoint, int left, int right)
    {
        return replica(endpoint, left, right, true);
    }

    private static void assertRanges(RangesByEndpoint epReplicas, String endpoint, int... rangePairs)
    {
        if (rangePairs.length % 2 == 1)
            throw new RuntimeException("assertRanges argument count should be even");

        InetAddressAndPort ep = inet(endpoint);
        List<Replica> expected = new ArrayList<>(rangePairs.length/2);
        for (int i=0; i<rangePairs.length; i+=2)
            expected.add(replica(ep, rangePairs[i], rangePairs[i+1]));

        RangesAtEndpoint actual = epReplicas.get(ep);
        assertEquals(expected.size(), actual.size());
        for (Replica replica : expected)
            if (!actual.contains(replica))
                assertEquals(RangesAtEndpoint.copyOf(expected), actual);
    }
}
