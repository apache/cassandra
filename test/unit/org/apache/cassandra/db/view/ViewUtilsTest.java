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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.service.StorageService;

public class ViewUtilsTest
{
    private final String KS = "Keyspace1";

    @BeforeClass
    public static void setUp() throws ConfigurationException, IOException
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.cleanupAndLeaveDirs();
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
    }

    @Test
    public void testGetIndexNaturalEndpoint() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "1");
        replicationMap.put("DC2", "1");

        recreateKeyspace(replicationMap);

        Optional<Replica> naturalEndpoint = ViewUtils.getViewNaturalEndpoint(Keyspace.open(KS).getReplicationStrategy(),
                                                                             new StringToken("CA"),
                                                                             new StringToken("BB"));

        Assert.assertTrue(naturalEndpoint.isPresent());
        Assert.assertEquals(InetAddressAndPort.getByName("127.0.0.2"), naturalEndpoint.get().endpoint());
    }


    @Test
    public void testLocalHostPreference() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "2");
        replicationMap.put("DC2", "2");

        recreateKeyspace(replicationMap);

        Optional<Replica> naturalEndpoint = ViewUtils.getViewNaturalEndpoint(Keyspace.open(KS).getReplicationStrategy(),
                                                                             new StringToken("CA"),
                                                                             new StringToken("BB"));

        Assert.assertTrue(naturalEndpoint.isPresent());
        Assert.assertEquals(InetAddressAndPort.getByName("127.0.0.1"), naturalEndpoint.get().endpoint());
    }

    @Test
    public void testBaseTokenDoesNotBelongToLocalReplicaShouldReturnEmpty() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "1");
        replicationMap.put("DC2", "1");

        recreateKeyspace(replicationMap);

        Optional<Replica> naturalEndpoint = ViewUtils.getViewNaturalEndpoint(Keyspace.open(KS).getReplicationStrategy(),
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
