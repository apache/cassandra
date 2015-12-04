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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.StorageService;

public class ViewUtilsTest
{
    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
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
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "1");
        replicationMap.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, replicationMap));
        Schema.instance.setKeyspaceMetadata(meta);

        Optional<InetAddress> naturalEndpoint = ViewUtils.getViewNaturalEndpoint("Keyspace1",
                                                                       new StringToken("CA"),
                                                                       new StringToken("BB"));

        Assert.assertTrue(naturalEndpoint.isPresent());
        Assert.assertEquals(InetAddress.getByName("127.0.0.2"), naturalEndpoint.get());
    }


    @Test
    public void testLocalHostPreference() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "2");
        replicationMap.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, replicationMap));
        Schema.instance.setKeyspaceMetadata(meta);

        Optional<InetAddress> naturalEndpoint = ViewUtils.getViewNaturalEndpoint("Keyspace1",
                                                                       new StringToken("CA"),
                                                                       new StringToken("BB"));

        Assert.assertTrue(naturalEndpoint.isPresent());
        Assert.assertEquals(InetAddress.getByName("127.0.0.1"), naturalEndpoint.get());
    }

    @Test
    public void testBaseTokenDoesNotBelongToLocalReplicaShouldReturnEmpty() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", "1");
        replicationMap.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, replicationMap));
        Schema.instance.setKeyspaceMetadata(meta);

        Optional<InetAddress> naturalEndpoint = ViewUtils.getViewNaturalEndpoint("Keyspace1",
                                                                       new StringToken("AB"),
                                                                       new StringToken("BB"));

        Assert.assertFalse(naturalEndpoint.isPresent());
    }
}
