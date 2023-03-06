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
package org.apache.cassandra.locator;

import org.junit.*;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.tcm.ClusterMetadataService;

public class ReplicationStrategyEndpointCacheTest
{
    private Token searchToken;

    public static final String KEYSPACE = "replication_strategy_endpoint_cache_test";

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(RandomPartitioner.instance);
    }

    @Before
    public void beforeEach()
    {
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.instanceForTest());
    }

    public void setup() throws Exception
    {
        searchToken = new BigIntegerToken(String.valueOf(15));
        ClusterMetadataTestHelper.createKeyspace("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION = {" +
                                                 "   'class' : 'SimpleStrategy'," +
                                                 "   'replication_factor': 5} ;");

        for (int i = 1; i <= 8; i++)
            ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0." + i), new BigIntegerToken(String.valueOf(i * 10)));
    }

    @Test
    public void testCacheRespectsTokenChanges() throws Exception
    {
        setup();
        EndpointsForToken initial;
        EndpointsForToken replicas;

        replicas = ClusterMetadataTestHelper.getNaturalReplicasForToken(KEYSPACE, searchToken);
        Assert.assertEquals(replicas.size(), 5);

        // test token addition, in DC2 before existing token
        initial = ClusterMetadataTestHelper.getNaturalReplicasForToken(KEYSPACE, searchToken);
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.5"), new BigIntegerToken(String.valueOf(35)));
        replicas = ClusterMetadataTestHelper.getNaturalReplicasForToken(KEYSPACE, searchToken);
        Assert.assertEquals(replicas.size(), 5);
        Util.assertNotRCEquals(replicas, initial);

        // test token removal, newly created token
        initial = ClusterMetadataTestHelper.getNaturalReplicasForToken(KEYSPACE, searchToken);
        ClusterMetadataTestHelper.removeEndpoint(InetAddressAndPort.getByName("127.0.0.5"), true);
        replicas = ClusterMetadataTestHelper.getNaturalReplicasForToken(KEYSPACE, searchToken);
        Assert.assertEquals(replicas.size(), 5);
        Assert.assertFalse(replicas.endpoints().contains(InetAddressAndPort.getByName("127.0.0.5")));
        Util.assertNotRCEquals(replicas, initial);

        // test token change
        initial = ClusterMetadataTestHelper.getNaturalReplicasForToken(KEYSPACE, searchToken);
        //move .8 after search token but before other DC3
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.8"), new BigIntegerToken(String.valueOf(25)));
        replicas = ClusterMetadataTestHelper.getNaturalReplicasForToken(KEYSPACE, searchToken);
        Assert.assertEquals(replicas.size(), 5);
        Util.assertNotRCEquals(replicas, initial);
    }
}
