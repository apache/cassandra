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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.*;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.tcm.ClusterMetadata;

public class ViewUtilsTest
{
    private final String KS = "Keyspace1";

    @BeforeClass
    public static void setUp() throws ConfigurationException, IOException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(OrderPreservingPartitioner.instance);
        ServerTestUtils.cleanupAndLeaveDirs();
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
    }

    @Before
    public void beforeEach()
    {
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.instanceForTest());
    }

    @Test
    public void testGetIndexNaturalEndpoint() throws Exception
    {
        // DC1
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("A"), "DC1", "RACK1");
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("C"), "DC1", "RACK1");

        // DC2
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("B"), "DC2", "RACK1");
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("D"), "DC2", "RACK1");


        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "1");
        replicationMap.put("DC2", "1");

        recreateKeyspace(replicationMap);

        Optional<Replica> naturalEndpoint = ViewUtils.getViewNaturalEndpoint(ClusterMetadata.current(),
                                                                             KS,
                                                                             new StringToken("CA"),
                                                                             new StringToken("BB"));

        Assert.assertTrue(naturalEndpoint.isPresent());
        Assert.assertEquals(InetAddressAndPort.getByName("127.0.0.2"), naturalEndpoint.get().endpoint());
    }


    @Test
    public void testLocalHostPreference() throws Exception
    {
        // DC1
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("A"), "DC1", "RACK1");
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("C"), "DC1", "RACK1");

        // DC2
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("B"), "DC2", "RACK1");
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("D"), "DC2", "RACK1");

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "2");
        replicationMap.put("DC2", "2");

        recreateKeyspace(replicationMap);

        Optional<Replica> naturalEndpoint = ViewUtils.getViewNaturalEndpoint(ClusterMetadata.current(),
                                                                             KS,
                                                                             new StringToken("CA"),
                                                                             new StringToken("BB"));

        Assert.assertTrue(naturalEndpoint.isPresent());
        Assert.assertEquals(InetAddressAndPort.getByName("127.0.0.1"), naturalEndpoint.get().endpoint());
    }

    @Test
    public void testBaseTokenDoesNotBelongToLocalReplicaShouldReturnEmpty() throws Exception
    {
        // DC1
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("A"), "DC1", "RACK1");
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("C"), "DC1", "RACK1");

        // DC2
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("B"), "DC2", "RACK1");
        ClusterMetadataTestHelper.addEndpoint(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("D"), "DC2", "RACK1");


        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "1");
        replicationMap.put("DC2", "1");

        recreateKeyspace(replicationMap);

        Optional<Replica> naturalEndpoint = ViewUtils.getViewNaturalEndpoint(ClusterMetadata.current(),
                                                                             KS,
                                                                             new StringToken("AB"),
                                                                             new StringToken("BB"));

        Assert.assertFalse(naturalEndpoint.isPresent());
    }

    private void recreateKeyspace(Map<String, String> replicationMap)
    {
        SchemaTestUtil.dropKeyspaceIfExist(KS, true);
        KeyspaceMetadata meta = KeyspaceMetadata.create(KS, KeyspaceParams.create(false, replicationMap));
        SchemaTestUtil.addOrUpdateKeyspace(meta, true);
    }
}
