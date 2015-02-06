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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StorageServiceServerTest
{
    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
    }

    @Test
    public void testRegularMode() throws ConfigurationException
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        StorageService.instance.initServer(0);
        for (String path : DatabaseDescriptor.getAllDataFileLocations())
        {
            // verify that storage directories are there.
            assertTrue(new File(path).exists());
        }
        // a proper test would be to call decommission here, but decommission() mixes both shutdown and datatransfer
        // calls.  This test is only interested in the shutdown-related items which a properly handled by just
        // stopping the client.
        //StorageService.instance.decommission();
        StorageService.instance.stopClient();
    }

    @Test
    public void testGetAllRangesEmpty()
    {
        List<Token> toks = Collections.emptyList();
        assertEquals(Collections.<Range<Token>>emptyList(), StorageService.instance.getAllRanges(toks));
    }

    @Test
    public void testSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeSnapshot("snapshot");
    }

    @Test
    public void testColumnFamilySnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeColumnFamilySnapshot(SystemKeyspace.NAME, LegacySchemaTables.KEYSPACES, "cf_snapshot");
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name,
                                                                                                            InetAddress.getByName("127.0.0.1"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.2"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.4"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.5"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.1"));
        assertTrue(primaryRanges.isEmpty());

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name,
                                                                                   InetAddress.getByName("127.0.0.2"));
        assertTrue(primaryRanges.isEmpty());

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.4"));
        assertTrue(primaryRanges.size() == 2);
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.5"));
        assertTrue(primaryRanges.size() == 2);
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
    }

    @Test
    public void testPrimaryRangesWithVnodes() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        Multimap<InetAddress, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("A"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("E"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("H"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("C"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("I"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("J"));
        metadata.updateNormalTokens(dc1);
        // DC2
        Multimap<InetAddress, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("B"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("G"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("L"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("D"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("F"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("K"));
        metadata.updateNormalTokens(dc2);

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 4;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("F"), new StringToken("G")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("K"), new StringToken("L")));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        assert primaryRanges.contains(new Range<Token>(new StringToken("L"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 8;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("E"), new StringToken("F")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("J"), new StringToken("K")));
        // ranges from /127.0.0.1
        assert primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("E")));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        assert primaryRanges.contains(new Range<Token>(new StringToken("G"), new StringToken("H")));
        // ranges from /127.0.0.2
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("H"), new StringToken("I")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("I"), new StringToken("J")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithVnodes() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        Multimap<InetAddress, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("A"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("E"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("H"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("C"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("I"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("J"));
        metadata.updateNormalTokens(dc1);

        // DC2
        Multimap<InetAddress, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("B"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("G"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("L"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("D"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("F"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("K"));
        metadata.updateNormalTokens(dc2);

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should have primary ranges which also cover DC2
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.1"));
        assertEquals(8, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("J"), new StringToken("K"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("K"), new StringToken("L"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("L"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("E"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("E"), new StringToken("F"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("F"), new StringToken("G"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("G"), new StringToken("H"))));

        // endpoints in DC1 should have primary ranges which also cover DC2
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.2"));
        assertEquals(4, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("H"), new StringToken("I"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("I"), new StringToken("J"))));

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.4"));
        assertEquals(4, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("F"), new StringToken("G"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("K"), new StringToken("L"))));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("L"), new StringToken("A"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.5"));
        assertTrue(primaryRanges.size() == 8);
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("E"), new StringToken("F"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("J"), new StringToken("K"))));
        // ranges from /127.0.0.1
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("E"))));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("G"), new StringToken("H"))));
        // ranges from /127.0.0.2
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("H"), new StringToken("I"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("I"), new StringToken("J"))));
    }

    @Test
    public void testPrimaryRangesWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.3"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "SimpleStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.3"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    /* Does not make much sense to use -local and -pr with simplestrategy, but just to prevent human errors */
    @Test
    public void testPrimaryRangeForEndpointWithinDCWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.3"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "SimpleStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.3"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testCreateRepairRangeFrom() throws Exception
    {
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new LongToken(1000L), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new LongToken(2000L), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new LongToken(3000L), InetAddress.getByName("127.0.0.3"));
        metadata.updateNormalToken(new LongToken(4000L), InetAddress.getByName("127.0.0.4"));

        Collection<Range<Token>> repairRangeFrom = StorageService.instance.createRepairRangeFrom("1500", "3700");
        assert repairRangeFrom.size() == 3;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(1500L), new LongToken(2000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2000L), new LongToken(3000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(3000L), new LongToken(3700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "700");
        assert repairRangeFrom.size() == 1;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(500L), new LongToken(700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "1700");
        assert repairRangeFrom.size() == 2;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(500L), new LongToken(1000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(1000L), new LongToken(1700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2500", "2300");
        assert repairRangeFrom.size() == 5;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2500L), new LongToken(3000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(3000L), new LongToken(4000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(4000L), new LongToken(1000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(1000L), new LongToken(2000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2000L), new LongToken(2300L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "3000");
        assert repairRangeFrom.size() == 1;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2000L), new LongToken(3000L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "2000");
        assert repairRangeFrom.size() == 0;
    }
}
