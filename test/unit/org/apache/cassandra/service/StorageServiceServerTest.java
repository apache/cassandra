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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.WithPartitioner;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.Startup;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StorageServiceServerTest
{
    static final String DC1 = "DC1";
    static final String DC2 = "DC2";
    static final String RACK = "rack1";

    static InetAddressAndPort id1;
    static InetAddressAndPort id2;
    static InetAddressAndPort id3;
    static InetAddressAndPort id4;
    static InetAddressAndPort id5;

    @BeforeClass
    public static void setUp() throws ConfigurationException, UnknownHostException
    {
        GOSSIP_DISABLE_THREAD_VALIDATION.setBoolean(true);
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(OrderPreservingPartitioner.instance);
        IEndpointSnitch snitch = new AbstractNetworkTopologySnitch()
        {
             @Override
             public String getRack(InetAddressAndPort endpoint)
             {
                 return location(endpoint).rack;
             }

             @Override
             public String getDatacenter(InetAddressAndPort endpoint)
             {
                 return location(endpoint).datacenter;
             }

             private Location location(InetAddressAndPort endpoint)
             {
                 ClusterMetadata metadata = ClusterMetadata.current();
                 NodeId id = metadata.directory.peerId(endpoint);
                 if (id == null)
                     throw new IllegalArgumentException("Unknown endpoint " + endpoint);
                 return metadata.directory.location(id);
             }
        };
        ServerTestUtils.prepareServerNoRegister();
        DatabaseDescriptor.setEndpointSnitch(snitch);

        id1 = InetAddressAndPort.getByName("127.0.0.1");
        id2 = InetAddressAndPort.getByName("127.0.0.2");
        id3 = InetAddressAndPort.getByName("127.0.0.3");
        id4 = InetAddressAndPort.getByName("127.0.0.4");
        id5 = InetAddressAndPort.getByName("127.0.0.5");
        registerNodes();
        ServerTestUtils.markCMS();
    }

    private static void registerNodes()
    {
        ClusterMetadataTestHelper.register(id1, DC1, RACK);
        ClusterMetadataTestHelper.register(id2, DC1, RACK);
        ClusterMetadataTestHelper.register(id3, DC1, RACK);

        ClusterMetadataTestHelper.register(id4, DC2, RACK);
        ClusterMetadataTestHelper.register(id5, DC2, RACK);
    }

    private static void setupDefaultPlacements()
    {
        // DC1
        ClusterMetadataTestHelper.join(id1, new StringToken("A"));
        ClusterMetadataTestHelper.join(id2, new StringToken("C"));
        // DC2
        ClusterMetadataTestHelper.join(id4, new StringToken("B"));
        ClusterMetadataTestHelper.join(id5, new StringToken("D"));
    }

    @Before
    public void resetCMS()
    {
        ServerTestUtils.resetCMS();
    }

    @Test
    public void testGetAllRangesEmpty()
    {
        List<Token> toks = Collections.emptyList();
        assertEquals(Collections.<Range<Token>>emptyList(), StorageService.instance.getAllRanges(toks));
    }

    @Test
    public void testSnapshotWithFlush() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeSnapshot(UUID.randomUUID().toString());
    }

    @Test
    public void testTableSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeTableSnapshot(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.KEYSPACES, UUID.randomUUID().toString());
    }

    @Test
    public void testSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeSnapshot(UUID.randomUUID().toString(), SchemaConstants.SCHEMA_KEYSPACE_NAME);
    }

    @Test
    public void testLocalPrimaryRangeForEndpointWithNetworkTopologyStrategy() throws Exception
    {
        setupDefaultPlacements();

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "2");
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("C")));

        primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("B")));

        primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategy() throws Exception
    {
        setupDefaultPlacements();
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name,
                                                                                                            InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategy() throws Exception
    {
        setupDefaultPlacements();
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        setupDefaultPlacements();
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges).isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges).isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        setupDefaultPlacements();
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, id1);
        Assertions.assertThat(primaryRanges).isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, id2);
        Assertions.assertThat(primaryRanges).isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, id4);
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, id5);
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testPrimaryRangesWithVnodes() throws Exception
    {
        // DC1
        ClusterMetadataTestHelper.join(id1, Sets.newHashSet(new StringToken("A"), new StringToken("E"), new StringToken("H")));
        ClusterMetadataTestHelper.join(id2, Sets.newHashSet(new StringToken("C"), new StringToken("I"), new StringToken("J")));
        // DC2
        ClusterMetadataTestHelper.join(id4, Sets.newHashSet(new StringToken("B"), new StringToken("G"), new StringToken("L")));
        ClusterMetadataTestHelper.join(id5, Sets.newHashSet(new StringToken("D"), new StringToken("F"), new StringToken("K")));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges).isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges).isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(4);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("F"), new StringToken("G")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("K"), new StringToken("L")));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("L"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(8);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("E"), new StringToken("F")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("J"), new StringToken("K")));
        // ranges from /127.0.0.1
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("E")));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("G"), new StringToken("H")));
        // ranges from /127.0.0.2
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("H"), new StringToken("I")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("I"), new StringToken("J")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithVnodes() throws Exception
    {
        // DC1
        ClusterMetadataTestHelper.join(id1, Sets.newHashSet(new StringToken("A"), new StringToken("E"), new StringToken("H")));
        ClusterMetadataTestHelper.join(id2, Sets.newHashSet(new StringToken("C"), new StringToken("I"), new StringToken("J")));
        // DC2
        ClusterMetadataTestHelper.join(id4, Sets.newHashSet(new StringToken("B"), new StringToken("G"), new StringToken("L")));
        ClusterMetadataTestHelper.join(id5, Sets.newHashSet(new StringToken("D"), new StringToken("F"), new StringToken("K")));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        // endpoints in DC1 should have primary ranges which also cover DC2
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(8);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("J"), new StringToken("K")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("K"), new StringToken("L")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("L"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("E")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("E"), new StringToken("F")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("F"), new StringToken("G")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("G"), new StringToken("H")));

        // endpoints in DC1 should have primary ranges which also cover DC2
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(4);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("H"), new StringToken("I")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("I"), new StringToken("J")));

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(4);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("F"), new StringToken("G")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("K"), new StringToken("L")));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("L"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(8);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("E"), new StringToken("F")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("J"), new StringToken("K")));
        // ranges from /127.0.0.1
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("D"), new StringToken("E")));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("G"), new StringToken("H")));
        // ranges from /127.0.0.2
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("H"), new StringToken("I")));
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("I"), new StringToken("J")));
    }

    @Test
    public void testPrimaryRangesWithSimpleStrategy() throws Exception
    {
        ClusterMetadataTestHelper.join(id1, new StringToken("A"));
        ClusterMetadataTestHelper.join(id2, new StringToken("B"));
        ClusterMetadataTestHelper.join(id3, new StringToken("C"));

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.simpleTransient(2));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.3"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
    }

    /* Does not make much sense to use -local and -pr with simplestrategy, but just to prevent human errors */
    @Test
    public void testPrimaryRangeForEndpointWithinDCWithSimpleStrategy() throws Exception
    {
        ClusterMetadataTestHelper.join(id1, new StringToken("A"));
        ClusterMetadataTestHelper.join(id2, new StringToken("B"));
        ClusterMetadataTestHelper.join(id3, new StringToken("C"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.simpleTransient(2));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.3"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testCreateRepairRangeFrom() throws Exception
    {
        try (WithPartitioner m3p = new WithPartitioner(Murmur3Partitioner.instance))
        {
            registerNodes();
            ClusterMetadataTestHelper.join(id1, new LongToken(1000L));
            ClusterMetadataTestHelper.join(id2, new LongToken(2000L));
            ClusterMetadataTestHelper.join(id3, new LongToken(3000L));
            ClusterMetadataTestHelper.join(id4, new LongToken(4000L));

            Collection<Range<Token>> repairRangeFrom = StorageService.instance.createRepairRangeFrom("1500", "3700");
            Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(3);
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(1500L), new LongToken(2000L)));
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(2000L), new LongToken(3000L)));
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(3000L), new LongToken(3700L)));

            repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "700");
            Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(1);
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(500L), new LongToken(700L)));

            repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "1700");
            Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(2);
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(500L), new LongToken(1000L)));
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(1000L), new LongToken(1700L)));

            repairRangeFrom = StorageService.instance.createRepairRangeFrom("2500", "2300");
            Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(5);
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(2500L), new LongToken(3000L)));
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(3000L), new LongToken(4000L)));
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(4000L), new LongToken(1000L)));
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(1000L), new LongToken(2000L)));
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(2000L), new LongToken(2300L)));

            repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "3000");
            Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(1);
            Assertions.assertThat(repairRangeFrom).contains(new Range<>(new LongToken(2000L), new LongToken(3000L)));

            repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "2000");
            Assertions.assertThat(repairRangeFrom).isEmpty();
        }
    }

    /**
     * Test that StorageService.getNativeAddress returns the correct value based on cluster metadata
     *
     * @throws Exception
     */
    @Test
    public void testGetNativeAddress() throws Exception
    {
        NodeId node2 = ClusterMetadata.current().directory.peerId(id2);
        NodeAddresses oldAddresses = ClusterMetadata.current().directory.getNodeAddresses(node2);
        assertEquals("127.0.0.2:7012", StorageService.instance.getNativeaddress(id2, true));

        String newNativeString = "127.1.1.2:19012";
        InetAddressAndPort newNativeAddress = InetAddressAndPort.getByName(newNativeString);
        NodeAddresses newAddresses = new NodeAddresses(UUID.randomUUID(), oldAddresses.broadcastAddress, oldAddresses.localAddress, newNativeAddress);
        ClusterMetadataService.instance().commit(new Startup(node2, newAddresses, NodeVersion.CURRENT));
        assertEquals(newNativeString, StorageService.instance.getNativeaddress(id2, true));
    }

    @Test
    public void testGetNativeAddressIPV6() throws Exception
    {
        // Ensure IPv6 addresses are properly bracketed in RFC2732 (https://datatracker.ietf.org/doc/html/rfc2732) format when including ports.
        // See https://issues.apache.org/jira/browse/CASSANDRA-17945 for more context.
        NodeId node2 = ClusterMetadata.current().directory.peerId(id2);
        NodeAddresses oldAddresses = ClusterMetadata.current().directory.getNodeAddresses(node2);
        assertEquals("127.0.0.2:7012", StorageService.instance.getNativeaddress(id2, true));

        String newNativeString = "[0:0:0:0:0:0:0:3]:666";
        InetAddressAndPort newNativeAddress = InetAddressAndPort.getByName(newNativeString);
        NodeAddresses newAddresses = new NodeAddresses(UUID.randomUUID(), oldAddresses.broadcastAddress, oldAddresses.localAddress, newNativeAddress);
        ClusterMetadataService.instance().commit(new Startup(node2, newAddresses, NodeVersion.CURRENT));
        assertEquals(newNativeString, StorageService.instance.getNativeaddress(id2, true));
        //Default to using the provided address with the configured port
        assertEquals(newNativeString, StorageService.instance.getNativeaddress(id2, true));
    }

    @Test
    public void testAuditLogEnableLoggerNotFound() throws Exception
    {
        StorageService.instance.enableAuditLog(null, null, null, null, null, null, null, null);
        assertTrue(AuditLogManager.instance.isEnabled());
        try
        {
            StorageService.instance.enableAuditLog("foobar", null, null, null, null, null, null, null);
            Assert.fail();
        }
        catch (ConfigurationException | IllegalStateException ex)
        {
            StorageService.instance.disableAuditLog();
        }
    }

    @Test
    public void testAuditLogEnableLoggerTransitions() throws Exception
    {
        StorageService.instance.enableAuditLog(null, null, null, null, null, null, null, null);
        assertTrue(AuditLogManager.instance.isEnabled());

        try
        {
            StorageService.instance.enableAuditLog("foobar", null, null, null, null, null, null, null);
        }
        catch (ConfigurationException | IllegalStateException e)
        {
            e.printStackTrace();
        }

        StorageService.instance.enableAuditLog(null, null, null, null, null, null, null, null);
        assertTrue(AuditLogManager.instance.isEnabled());
        StorageService.instance.disableAuditLog();
    }
}
