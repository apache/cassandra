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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Schema;
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
    public static void setup() throws ConfigurationException
    {
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(partitioner);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition("MoveTest");
        addNetworkTopologyKeyspace(Network_11_KeyspaceName, 1, 1);
        addNetworkTopologyKeyspace(Network_22_KeyspaceName, 2, 2);
        addNetworkTopologyKeyspace(Network_33_KeyspaceName, 3, 3);
    }

    @AfterClass
    public static void tearDown()
    {
        StorageService.instance.setPartitionerUnsafe(oldPartitioner);
    }

    @Before
    public void clearTokenMetadata()
    {
        PendingRangeCalculatorService.instance.blockUntilFinished();
        StorageService.instance.getTokenMetadata().clearUnsafe();
    }

    private static void addNetworkTopologyKeyspace(String keyspaceName, Integer... replicas) throws ConfigurationException
    {

        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            //Odd IPs are in DC1 and Even are in DC2. Endpoints upto .14 will have unique racks and
            // then will be same for a set of three.
            @Override
            public String getRack(InetAddress endpoint)
            {
                int ipLastPart = getIPLastPart(endpoint);
                if (ipLastPart <= 14)
                    return UUID.randomUUID().toString();
                else
                    return "RAC" + (ipLastPart % 3);
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                if (getIPLastPart(endpoint) % 2 == 0)
                    return "DC2";
                else
                    return "DC1";
            }

            private int getIPLastPart(InetAddress endpoint)
            {
                String str = endpoint.toString();
                int index = str.lastIndexOf(".");
                return Integer.parseInt(str.substring(index + 1).trim());
            }
        });

        KeyspaceMetadata keyspace =  KeyspaceMetadata.create(keyspaceName,
                                                             KeyspaceParams.nts(configOptions(replicas)),
                                                             Tables.of(CFMetaData.Builder.create(keyspaceName, "CF1")
                                                                                         .addPartitionKey("key", BytesType.instance).build()));
        MigrationManager.announceNewKeyspace(keyspace);
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
        List<InetAddress> hosts = new ArrayList<>();
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
        List<InetAddress> hosts = new ArrayList<>();
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
        List<InetAddress> hosts = new ArrayList<>();
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
        List<InetAddress> hosts = new ArrayList<>();
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

    private void moveHost(InetAddress host, int token, TokenMetadata tmd, VersionedValue.VersionedValueFactory valueFactory )
    {
        StorageService.instance.onChange(host, ApplicationState.STATUS, valueFactory.moving(new BigIntegerToken(String.valueOf(token))));
        PendingRangeCalculatorService.instance.blockUntilFinished();
        assertTrue(tmd.isMoving(host));
    }

    private void finishMove(InetAddress host, int token, TokenMetadata tmd)
    {
        tmd.removeFromMoving(host);
        assertTrue(!tmd.isMoving(host));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(token)), host);
    }

    private Map.Entry<Range<Token>, Collection<InetAddress>> generatePendingMapEntry(int start, int end, String... endpoints) throws UnknownHostException
    {
        Map<Range<Token>, Collection<InetAddress>> pendingRanges = new HashMap<>();
        pendingRanges.put(generateRange(start, end), makeAddrs(endpoints));
        return pendingRanges.entrySet().iterator().next();
    }

    private Map<Range<Token>, Collection<InetAddress>> generatePendingRanges(Map.Entry<Range<Token>, Collection<InetAddress>>... entries)
    {
        Map<Range<Token>, Collection<InetAddress>> pendingRanges = new HashMap<>();
        for(Map.Entry<Range<Token>, Collection<InetAddress>> entry : entries)
        {
            pendingRanges.put(entry.getKey(), entry.getValue());
        }
        return pendingRanges;
    }

    private void assertPendingRanges(TokenMetadata tmd, Map<Range<Token>,  Collection<InetAddress>> pendingRanges, String keyspaceName) throws ConfigurationException
    {
        boolean keyspaceFound = false;
        for (String nonSystemKeyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            if(!keyspaceName.equals(nonSystemKeyspaceName))
                continue;
            assertMaps(pendingRanges, tmd.getPendingRanges(keyspaceName));
            keyspaceFound = true;
        }

        assert keyspaceFound;
    }

    private void assertMaps(Map<Range<Token>, Collection<InetAddress>> expected, PendingRangeMaps actual)
    {
        int sizeOfActual = 0;
        Iterator<Map.Entry<Range<Token>, List<InetAddress>>> iterator = actual.iterator();
        while(iterator.hasNext())
        {
            Map.Entry<Range<Token>, List<InetAddress>> actualEntry = iterator.next();
            assertNotNull(expected.get(actualEntry.getKey()));
            assertEquals(new HashSet<>(expected.get(actualEntry.getKey())), new HashSet<>(actualEntry.getValue()));
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
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);

        Map<Token, List<InetAddress>> expectedEndpoints = new HashMap<Token, List<InetAddress>>();
        for (Token token : keyTokens)
        {
            List<InetAddress> endpoints = new ArrayList<InetAddress>();
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
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            strategy = getStrategy(keyspaceName, tmd);
            if(strategy instanceof NetworkTopologyStrategy)
                continue;
            int numMoved = 0;
            for (Token token : keyTokens)
            {
                int replicationFactor = strategy.getReplicationFactor();

                HashSet<InetAddress> actual = new HashSet<InetAddress>(tmd.getWriteEndpoints(token, keyspaceName, strategy.calculateNaturalEndpoints(token, tmd.cloneOnlyTokenMap())));
                HashSet<InetAddress> expected = new HashSet<InetAddress>();

                for (int i = 0; i < replicationFactor; i++)
                {
                    expected.add(expectedEndpoints.get(token).get(i));
                }

                if (expected.size() == actual.size()) {
                	assertEquals("mismatched endpoint sets", expected, actual);
                } else {
                	expected.add(hosts.get(MOVING_NODE));
                	assertEquals("mismatched endpoint sets", expected, actual);
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
        List<InetAddress> hosts = new ArrayList<InetAddress>();
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

        Collection<InetAddress> endpoints;

        tmd = tmd.cloneAfterAllSettled();
        ss.setTokenMetadataUnsafe(tmd);

        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        InetAddress boot1 = InetAddress.getByName("127.0.1.1");
        Gossiper.instance.initializeNodeUnsafe(boot1, UUID.randomUUID(), 1);
        Gossiper.instance.injectApplicationState(boot1, ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(5))));
        ss.onChange(boot1,
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(5))));
        PendingRangeCalculatorService.instance.blockUntilFinished();

        InetAddress boot2 = InetAddress.getByName("127.0.1.2");
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

        Multimap<InetAddress, Range<Token>> keyspace1ranges = keyspaceStrategyMap.get(Simple_RF1_KeyspaceName).getAddressRanges();
        Collection<Range<Token>> ranges1 = keyspace1ranges.get(InetAddress.getByName("127.0.0.1"));
        assertEquals(1, collectionSize(ranges1));
        assertEquals(generateRange(97, 0), ranges1.iterator().next());
        Collection<Range<Token>> ranges2 = keyspace1ranges.get(InetAddress.getByName("127.0.0.2"));
        assertEquals(1, collectionSize(ranges2));
        assertEquals(generateRange(0, 10), ranges2.iterator().next());
        Collection<Range<Token>> ranges3 = keyspace1ranges.get(InetAddress.getByName("127.0.0.3"));
        assertEquals(1, collectionSize(ranges3));
        assertEquals(generateRange(10, 20), ranges3.iterator().next());
        Collection<Range<Token>> ranges4 = keyspace1ranges.get(InetAddress.getByName("127.0.0.4"));
        assertEquals(1, collectionSize(ranges4));
        assertEquals(generateRange(20, 30), ranges4.iterator().next());
        Collection<Range<Token>> ranges5 = keyspace1ranges.get(InetAddress.getByName("127.0.0.5"));
        assertEquals(1, collectionSize(ranges5));
        assertEquals(generateRange(30, 40), ranges5.iterator().next());
        Collection<Range<Token>> ranges6 = keyspace1ranges.get(InetAddress.getByName("127.0.0.6"));
        assertEquals(1, collectionSize(ranges6));
        assertEquals(generateRange(40, 50), ranges6.iterator().next());
        Collection<Range<Token>> ranges7 = keyspace1ranges.get(InetAddress.getByName("127.0.0.7"));
        assertEquals(1, collectionSize(ranges7));
        assertEquals(generateRange(50, 67), ranges7.iterator().next());
        Collection<Range<Token>> ranges8 = keyspace1ranges.get(InetAddress.getByName("127.0.0.8"));
        assertEquals(1, collectionSize(ranges8));
        assertEquals(generateRange(67, 70), ranges8.iterator().next());
        Collection<Range<Token>> ranges9 = keyspace1ranges.get(InetAddress.getByName("127.0.0.9"));
        assertEquals(1, collectionSize(ranges9));
        assertEquals(generateRange(70, 87), ranges9.iterator().next());
        Collection<Range<Token>> ranges10 = keyspace1ranges.get(InetAddress.getByName("127.0.0.10"));
        assertEquals(1, collectionSize(ranges10));
        assertEquals(generateRange(87, 97), ranges10.iterator().next());


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

        Multimap<InetAddress, Range<Token>> keyspace3ranges = keyspaceStrategyMap.get(KEYSPACE3).getAddressRanges();
        ranges1 = keyspace3ranges.get(InetAddress.getByName("127.0.0.1"));
        assertEquals(collectionSize(ranges1), 5);
        assertTrue(ranges1.equals(generateRanges(97, 0, 70, 87, 50, 67, 87, 97, 67, 70)));
        ranges2 = keyspace3ranges.get(InetAddress.getByName("127.0.0.2"));
        assertEquals(collectionSize(ranges2), 5);
        assertTrue(ranges2.equals(generateRanges(97, 0, 70, 87, 87, 97, 0, 10, 67, 70)));
        ranges3 = keyspace3ranges.get(InetAddress.getByName("127.0.0.3"));
        assertEquals(collectionSize(ranges3), 5);
        assertTrue(ranges3.equals(generateRanges(97, 0, 70, 87, 87, 97, 0, 10, 10, 20)));
        ranges4 = keyspace3ranges.get(InetAddress.getByName("127.0.0.4"));
        assertEquals(collectionSize(ranges4), 5);
        assertTrue(ranges4.equals(generateRanges(97, 0, 20, 30, 87, 97, 0, 10, 10, 20)));
        ranges5 = keyspace3ranges.get(InetAddress.getByName("127.0.0.5"));
        assertEquals(collectionSize(ranges5), 5);
        assertTrue(ranges5.equals(generateRanges(97, 0, 30, 40, 20, 30, 0, 10, 10, 20)));
        ranges6 = keyspace3ranges.get(InetAddress.getByName("127.0.0.6"));
        assertEquals(collectionSize(ranges6), 5);
        assertTrue(ranges6.equals(generateRanges(40, 50, 30, 40, 20, 30, 0, 10, 10, 20)));
        ranges7 = keyspace3ranges.get(InetAddress.getByName("127.0.0.7"));
        assertEquals(collectionSize(ranges7), 5);
        assertTrue(ranges7.equals(generateRanges(40, 50, 30, 40, 50, 67, 20, 30, 10, 20)));
        ranges8 = keyspace3ranges.get(InetAddress.getByName("127.0.0.8"));
        assertEquals(collectionSize(ranges8), 5);
        assertTrue(ranges8.equals(generateRanges(40, 50, 30, 40, 50, 67, 20, 30, 67, 70)));
        ranges9 = keyspace3ranges.get(InetAddress.getByName("127.0.0.9"));
        assertEquals(collectionSize(ranges9), 5);
        assertTrue(ranges9.equals(generateRanges(40, 50, 70, 87, 30, 40, 50, 67, 67, 70)));
        ranges10 = keyspace3ranges.get(InetAddress.getByName("127.0.0.10"));
        assertEquals(collectionSize(ranges10), 5);
        assertTrue(ranges10.equals(generateRanges(40, 50, 70, 87, 50, 67, 87, 97, 67, 70)));


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
        Multimap<InetAddress, Range<Token>> keyspace4ranges = keyspaceStrategyMap.get(Simple_RF3_KeyspaceName).getAddressRanges();
        ranges1 = keyspace4ranges.get(InetAddress.getByName("127.0.0.1"));
        assertEquals(collectionSize(ranges1), 3);
        assertTrue(ranges1.equals(generateRanges(97, 0, 70, 87, 87, 97)));
        ranges2 = keyspace4ranges.get(InetAddress.getByName("127.0.0.2"));
        assertEquals(collectionSize(ranges2), 3);
        assertTrue(ranges2.equals(generateRanges(97, 0, 87, 97, 0, 10)));
        ranges3 = keyspace4ranges.get(InetAddress.getByName("127.0.0.3"));
        assertEquals(collectionSize(ranges3), 3);
        assertTrue(ranges3.equals(generateRanges(97, 0, 0, 10, 10, 20)));
        ranges4 = keyspace4ranges.get(InetAddress.getByName("127.0.0.4"));
        assertEquals(collectionSize(ranges4), 3);
        assertTrue(ranges4.equals(generateRanges(20, 30, 0, 10, 10, 20)));
        ranges5 = keyspace4ranges.get(InetAddress.getByName("127.0.0.5"));
        assertEquals(collectionSize(ranges5), 3);
        assertTrue(ranges5.equals(generateRanges(30, 40, 20, 30, 10, 20)));
        ranges6 = keyspace4ranges.get(InetAddress.getByName("127.0.0.6"));
        assertEquals(collectionSize(ranges6), 3);
        assertTrue(ranges6.equals(generateRanges(40, 50, 30, 40, 20, 30)));
        ranges7 = keyspace4ranges.get(InetAddress.getByName("127.0.0.7"));
        assertEquals(collectionSize(ranges7), 3);
        assertTrue(ranges7.equals(generateRanges(40, 50, 30, 40, 50, 67)));
        ranges8 = keyspace4ranges.get(InetAddress.getByName("127.0.0.8"));
        assertEquals(collectionSize(ranges8), 3);
        assertTrue(ranges8.equals(generateRanges(40, 50, 50, 67, 67, 70)));
        ranges9 = keyspace4ranges.get(InetAddress.getByName("127.0.0.9"));
        assertEquals(collectionSize(ranges9), 3);
        assertTrue(ranges9.equals(generateRanges(70, 87, 50, 67, 67, 70)));
        ranges10 = keyspace4ranges.get(InetAddress.getByName("127.0.0.10"));
        assertEquals(collectionSize(ranges10), 3);
        assertTrue(ranges10.equals(generateRanges(70, 87, 87, 97, 67, 70)));

        // pre-calculate the results.
        Map<String, Multimap<Token, InetAddress>> expectedEndpoints = new HashMap<String, Multimap<Token, InetAddress>>();
        expectedEndpoints.put(Simple_RF1_KeyspaceName, HashMultimap.<Token, InetAddress>create());
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
        expectedEndpoints.put(KEYSPACE2, HashMultimap.<Token, InetAddress>create());
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
        expectedEndpoints.put(KEYSPACE3, HashMultimap.<Token, InetAddress>create());
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
        expectedEndpoints.put(Simple_RF3_KeyspaceName, HashMultimap.<Token, InetAddress>create());
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
                endpoints = tmd.getWriteEndpoints(token, keyspaceName, strategy.getNaturalEndpoints(token));
                assertEquals(expectedEndpoints.get(keyspaceName).get(token).size(), endpoints.size());
                assertTrue(expectedEndpoints.get(keyspaceName).get(token).containsAll(endpoints));
            }

            // just to be sure that things still work according to the old tests, run them:
            if (strategy.getReplicationFactor() != 3)
                continue;

            // tokens 5, 15 and 25 should go three nodes
            for (int i = 0; i < 3; i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertEquals(3, endpoints.size());
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 should go to nodes 4, 5, 6 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(3)));
            assertEquals(4, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(boot1));

            // token 45 should go to nodes 5, 6, 7 boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(4)));
            assertEquals(4, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot1));

            // token 55 should go to nodes 6, 7, 8 boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(5)));
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 65 should go to nodes 6, 7, 8 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(6)));
            assertEquals(4, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(boot2));

            // token 75 should to go nodes 8, 9, 0 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(7)));
            assertEquals(4, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(boot2));

            // token 85 should go to nodes 8, 9 and 0
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(8)));
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));

            // token 95 should go to nodes 9, 0 and 1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(9)));
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
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
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 6 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 6);

        // node 2 leaves
        Token newToken = positionToken(7);
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.moving(newToken));

        assertTrue(tmd.isMoving(hosts.get(2)));
        assertEquals(endpointTokens.get(2), tmd.getToken(hosts.get(2)));

        // back to normal
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(newToken)));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(newToken)));

        assertTrue(tmd.getMovingEndpoints().isEmpty());
        assertEquals(newToken, tmd.getToken(hosts.get(2)));

        newToken = positionToken(8);
        // node 2 goes through leave and left and then jumps to normal at its new token
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.moving(newToken));
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(newToken)));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(newToken)));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
        assertTrue(tmd.getMovingEndpoints().isEmpty());
        assertEquals(newToken, tmd.getToken(hosts.get(2)));
    }

    private static Collection<InetAddress> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddress> addrs = new ArrayList<InetAddress>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddress.getByName(host));
        return addrs;
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName, TokenMetadata tmd)
    {
        KeyspaceMetadata ksmd = Schema.instance.getKSMetaData(keyspaceName);
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

    private int collectionSize(Collection<?> collection)
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

    private Range<Token> generateRange(int left, int right)
    {
        return new Range<Token>(new BigIntegerToken(String.valueOf(left)), new BigIntegerToken(String.valueOf(right)));
    }
}
