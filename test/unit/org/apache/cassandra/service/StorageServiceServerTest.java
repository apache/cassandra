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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.ServerTestUtils.cleanup;
import static org.apache.cassandra.ServerTestUtils.mkdirs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StorageServiceServerTest
{
    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
        mkdirs();
        cleanup();
        StorageService.instance.initServer(0);
    }

    @AfterClass
    public static void tearDown()
    {
        // a proper test would be to call decommission here, but decommission() mixes both shutdown and datatransfer
        // calls.  This test is only interested in the shutdown-related items which a properly handled by just
        // stopping the client.
        //StorageService.instance.decommission();
        StorageService.instance.stopClient();
    }

    @Test
    public void testRegularMode() throws ConfigurationException
    {
        for (String path : DatabaseDescriptor.getAllDataFileLocations())
        {
            // verify that storage directories are there.
            assertTrue(new File(path).exists());
        }
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

    private void checkTempFilePresence(File f, boolean exist)
    {
        for (int i = 0; i < 5; i++)
        {
            File subdir = new File(f, Integer.toString(i));
            subdir.mkdir();
            for (int j = 0; j < 5; j++)
            {
                File subF = new File(subdir, Integer.toString(j));
                assert(exist ? subF.exists() : !subF.exists());
            }
        }
    }

    @Test
    public void testSnapshotFailureHandler() throws IOException
    {
        assumeTrue(FBUtilities.isWindows);

        // Initial "run" of Cassandra, nothing in failed snapshot file
        WindowsFailedSnapshotTracker.deleteOldSnapshots();

        File f = new File(System.getenv("TEMP") + File.separator + Integer.toString(new Random().nextInt()));
        f.mkdir();
        f.deleteOnExit();
        for (int i = 0; i < 5; i++)
        {
            File subdir = new File(f, Integer.toString(i));
            subdir.mkdir();
            for (int j = 0; j < 5; j++)
                new File(subdir, Integer.toString(j)).createNewFile();
        }

        checkTempFilePresence(f, true);

        // Confirm deletion is recursive
        for (int i = 0; i < 5; i++)
            WindowsFailedSnapshotTracker.handleFailedSnapshot(new File(f, Integer.toString(i)));

        assertTrue(new File(WindowsFailedSnapshotTracker.TODELETEFILE).exists());

        // Simulate shutdown and restart of C* node, closing out the list of failed snapshots.
        WindowsFailedSnapshotTracker.resetForTests();

        // Perform new run, mimicking behavior of C* at startup
        WindowsFailedSnapshotTracker.deleteOldSnapshots();
        checkTempFilePresence(f, false);

        // Check to make sure we don't delete non-temp, non-datafile locations
        WindowsFailedSnapshotTracker.resetForTests();
        PrintWriter tempPrinter = new PrintWriter(new FileWriter(WindowsFailedSnapshotTracker.TODELETEFILE, true));
        tempPrinter.println(".safeDir");
        tempPrinter.close();

        File protectedDir = new File(".safeDir");
        protectedDir.mkdir();
        File protectedFile = new File(protectedDir, ".safeFile");
        protectedFile.createNewFile();

        WindowsFailedSnapshotTracker.handleFailedSnapshot(protectedDir);
        WindowsFailedSnapshotTracker.deleteOldSnapshots();

        assertTrue(protectedDir.exists());
        assertTrue(protectedFile.exists());

        protectedFile.delete();
        protectedDir.delete();
    }

    @Test
    public void testTableSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeTableSnapshot(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.KEYSPACES, UUID.randomUUID().toString());
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
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "2");
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("C")));

        primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("B")));

        primaryRanges = StorageService.instance.getLocalPrimaryRangeForEndpoint(InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

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
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

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
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddressAndPort.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.create(false, configOptions));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges).isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name,
                                                                                   InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges).isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(2);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testPrimaryRangesWithVnodes() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        Multimap<InetAddressAndPort, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("A"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("E"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("H"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("C"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("I"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("J"));
        metadata.updateNormalTokens(dc1);
        // DC2
        Multimap<InetAddressAndPort, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("B"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("G"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("L"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("D"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("F"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("K"));
        metadata.updateNormalTokens(dc2);

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
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("F"), new StringToken("G")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("K"), new StringToken("L")));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("L"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(8);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("E"), new StringToken("F")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("J"), new StringToken("K")));
        // ranges from /127.0.0.1
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("E")));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("G"), new StringToken("H")));
        // ranges from /127.0.0.2
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("H"), new StringToken("I")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("I"), new StringToken("J")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithVnodes() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        Multimap<InetAddressAndPort, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("A"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("E"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new StringToken("H"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("C"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("I"));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new StringToken("J"));
        metadata.updateNormalTokens(dc1);

        // DC2
        Multimap<InetAddressAndPort, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("B"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("G"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new StringToken("L"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("D"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("F"));
        dc2.put(InetAddressAndPort.getByName("127.0.0.5"), new StringToken("K"));
        metadata.updateNormalTokens(dc2);

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
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("J"), new StringToken("K")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("K"), new StringToken("L")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("L"), new StringToken("A")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("E")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("E"), new StringToken("F")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("F"), new StringToken("G")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("G"), new StringToken("H")));

        // endpoints in DC1 should have primary ranges which also cover DC2
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(4);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("H"), new StringToken("I")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("I"), new StringToken("J")));

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.4"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(4);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("F"), new StringToken("G")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("K"), new StringToken("L")));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("L"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.5"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(8);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("E"), new StringToken("F")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("J"), new StringToken("K")));
        // ranges from /127.0.0.1
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("D"), new StringToken("E")));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("G"), new StringToken("H")));
        // ranges from /127.0.0.2
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("H"), new StringToken("I")));
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("I"), new StringToken("J")));
    }

    @Test
    public void testPrimaryRangesWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.3"));

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.simpleTransient(2));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddressAndPort.getByName("127.0.0.3"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    /* Does not make much sense to use -local and -pr with simplestrategy, but just to prevent human errors */
    @Test
    public void testPrimaryRangeForEndpointWithinDCWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A"), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B"), InetAddressAndPort.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C"), InetAddressAndPort.getByName("127.0.0.3"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        SchemaTestUtil.dropKeyspaceIfExist("Keyspace1", false);
        KeyspaceMetadata meta = KeyspaceMetadata.create("Keyspace1", KeyspaceParams.simpleTransient(2));
        SchemaTestUtil.addOrUpdateKeyspace(meta, false);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.1"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.2"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddressAndPort.getByName("127.0.0.3"));
        Assertions.assertThat(primaryRanges.size()).as(primaryRanges.toString()).isEqualTo(1);
        Assertions.assertThat(primaryRanges).contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testCreateRepairRangeFrom() throws Exception
    {
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new LongToken(1000L), InetAddressAndPort.getByName("127.0.0.1"));
        metadata.updateNormalToken(new LongToken(2000L), InetAddressAndPort.getByName("127.0.0.2"));
        metadata.updateNormalToken(new LongToken(3000L), InetAddressAndPort.getByName("127.0.0.3"));
        metadata.updateNormalToken(new LongToken(4000L), InetAddressAndPort.getByName("127.0.0.4"));

        Collection<Range<Token>> repairRangeFrom = StorageService.instance.createRepairRangeFrom("1500", "3700");
        Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(3);
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(1500L), new LongToken(2000L)));
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(2000L), new LongToken(3000L)));
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(3000L), new LongToken(3700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "700");
        Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(1);
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(500L), new LongToken(700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "1700");
        Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(2);
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(500L), new LongToken(1000L)));
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(1000L), new LongToken(1700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2500", "2300");
        Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(5);
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(2500L), new LongToken(3000L)));
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(3000L), new LongToken(4000L)));
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(4000L), new LongToken(1000L)));
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(1000L), new LongToken(2000L)));
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(2000L), new LongToken(2300L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "3000");
        Assertions.assertThat(repairRangeFrom.size()).as(repairRangeFrom.toString()).isEqualTo(1);
        Assertions.assertThat(repairRangeFrom).contains(new Range<Token>(new LongToken(2000L), new LongToken(3000L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "2000");
        Assertions.assertThat(repairRangeFrom).isEmpty();
    }

    /**
     * Test that StorageService.getNativeAddress returns the correct value based on available yaml and gossip state
     * @throws Exception
     */
    @Test
    public void testGetNativeAddress() throws Exception
    {
        String internalAddressString = "127.0.0.2:666";
        InetAddressAndPort internalAddress = InetAddressAndPort.getByName(internalAddressString);
        Gossiper.instance.addSavedEndpoint(internalAddress);
        //Default to using the provided address with the configured port
        assertEquals("127.0.0.2:" + DatabaseDescriptor.getNativeTransportPort(), StorageService.instance.getNativeaddress(internalAddress, true));

        VersionedValue.VersionedValueFactory valueFactory =  new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
        //If we don't have the port use the gossip address, but with the configured port
        Gossiper.instance.getEndpointStateForEndpoint(internalAddress).addApplicationState(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(InetAddress.getByName("127.0.0.3")));
        assertEquals("127.0.0.3:" + DatabaseDescriptor.getNativeTransportPort(), StorageService.instance.getNativeaddress(internalAddress, true));
        //If we have the address and port in gossip use that
        Gossiper.instance.getEndpointStateForEndpoint(internalAddress).addApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueFactory.nativeaddressAndPort(InetAddressAndPort.getByName("127.0.0.3:666")));
        assertEquals("127.0.0.3:666", StorageService.instance.getNativeaddress(internalAddress, true));
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
        catch (IllegalStateException ex)
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
