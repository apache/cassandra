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

package org.apache.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.GossipMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GossiperTest
{
    static
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    private static final CassandraVersion CURRENT_VERSION = new CassandraVersion(FBUtilities.getReleaseVersionString());

    static final IPartitioner partitioner = new RandomPartitioner();
    StorageService ss = StorageService.instance;
    TokenMetadata tmd = StorageService.instance.getTokenMetadata();
    ArrayList<Token> endpointTokens = new ArrayList<>();
    ArrayList<Token> keyTokens = new ArrayList<>();
    List<InetAddressAndPort> hosts = new ArrayList<>();
    List<UUID> hostIds = new ArrayList<>();

    private SeedProvider originalSeedProvider;

    @Before
    public void setup()
    {
        tmd.clearUnsafe();
        originalSeedProvider = DatabaseDescriptor.getSeedProvider();

        DatabaseDescriptor.setCompareGossipAndStorageServiceCache(true);
        DatabaseDescriptor.setSyncGossipAndStorageServiceCacheIfMismatched(true);
        DatabaseDescriptor.setGossipAndStorageServiceCacheComparisonInterval(0, TimeUnit.SECONDS);
        DatabaseDescriptor.setGossipAndStorageServiceCacheMismatchConvictionThreshold(3);

        GossipMetrics.gossipAndStorageServiceCacheCompare.dec(GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        GossipMetrics.gossipAndStorageServiceCacheMismatch.dec(GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        GossipMetrics.gossipAndStorageServiceCacheRepair.dec(GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        GossipMetrics.gossipAndStorageServiceCacheError.dec(GossipMetrics.gossipAndStorageServiceCacheError.getCount());
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setSeedProvider(originalSeedProvider);
        Gossiper.instance.endpointStateMap.clear();
        DatabaseDescriptor.setSyncGossipAndStorageServiceCacheIfMismatched(false);
    }

    @AfterClass
    public static void afterClass()
    {
        Gossiper.instance.stop();
    }

    @Test
    public void testPaddingIntact() throws Exception
    {
        // sanity check that all 10 pads still exist
        assert ApplicationState.X1 == ApplicationState.X1;
        assert ApplicationState.X2 == ApplicationState.X2;
        assert ApplicationState.X3 == ApplicationState.X3;
        assert ApplicationState.X4 == ApplicationState.X4;
        assert ApplicationState.X5 == ApplicationState.X5;
        assert ApplicationState.X6 == ApplicationState.X6;
        assert ApplicationState.X7 == ApplicationState.X7;
        assert ApplicationState.X8 == ApplicationState.X8;
        assert ApplicationState.X9 == ApplicationState.X9;
        assert ApplicationState.X10 == ApplicationState.X10;
    }

    @Test
    public void testHasVersion3Nodes() throws Exception
    {
        Gossiper.instance.start(0);
        Gossiper.instance.expireUpgradeFromVersion();

        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(null);
        EndpointState es = new EndpointState((HeartBeatState) null);
        es.addApplicationState(ApplicationState.RELEASE_VERSION, factory.releaseVersion(CURRENT_VERSION.toString()));
        Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName("127.0.0.1"), es);
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName("127.0.0.1"));


        es = new EndpointState((HeartBeatState) null);
        es.addApplicationState(ApplicationState.RELEASE_VERSION, factory.releaseVersion("3.11.3"));
        Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName("127.0.0.2"), es);
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName("127.0.0.2"));

        es = new EndpointState((HeartBeatState) null);
        es.addApplicationState(ApplicationState.RELEASE_VERSION, factory.releaseVersion("3.0.0"));
        Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName("127.0.0.3"), es);
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName("127.0.0.3"));

        assertFalse(Gossiper.instance.upgradeFromVersionSupplier.get().value().compareTo(new CassandraVersion("3.0")) < 0);
        assertTrue(Gossiper.instance.upgradeFromVersionSupplier.get().value().compareTo(new CassandraVersion("3.1")) < 0);
        assertTrue(Gossiper.instance.hasMajorVersion3OrUnknownNodes());

        Gossiper.instance.endpointStateMap.remove(InetAddressAndPort.getByName("127.0.0.3"));
        Gossiper.instance.liveEndpoints.remove(InetAddressAndPort.getByName("127.0.0.3"));

        assertFalse(Gossiper.instance.upgradeFromVersionSupplier.get().value().compareTo(new CassandraVersion("3.0")) < 0);
        assertFalse(Gossiper.instance.upgradeFromVersionSupplier.get().value().compareTo(new CassandraVersion("3.1")) < 0);
        assertTrue(Gossiper.instance.upgradeFromVersionSupplier.get().value().compareTo(new CassandraVersion("3.12")) < 0);
        assertTrue(Gossiper.instance.hasMajorVersion3OrUnknownNodes());

        Gossiper.instance.endpointStateMap.remove(InetAddressAndPort.getByName("127.0.0.2"));
        Gossiper.instance.liveEndpoints.remove(InetAddressAndPort.getByName("127.0.0.2"));

        assertEquals(SystemKeyspace.CURRENT_VERSION, Gossiper.instance.upgradeFromVersionSupplier.get().value());
    }

    @Test
    public void testHasVersion3NodesShouldReturnFalseWhenNoVersion3NodesDetectedAndCassandra4UpgradeInProgress() throws Exception
    {
        Gossiper.instance.start(0);
        Gossiper.instance.expireUpgradeFromVersion();

        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(null);
        EndpointState es = new EndpointState((HeartBeatState) null);
        es.addApplicationState(ApplicationState.RELEASE_VERSION, factory.releaseVersion(CURRENT_VERSION.toString()));
        Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName("127.0.0.1"), es);
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName("127.0.0.1"));

        es = new EndpointState((HeartBeatState) null);
        String previousPatchVersion = String.valueOf(CURRENT_VERSION.major) + '.' + (CURRENT_VERSION.minor) + '.' + Math.max(CURRENT_VERSION.patch - 1, 0);
        es.addApplicationState(ApplicationState.RELEASE_VERSION, factory.releaseVersion(previousPatchVersion));
        Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName("127.0.0.2"), es);
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName("127.0.0.2"));
        assertFalse(Gossiper.instance.hasMajorVersion3OrUnknownNodes());

        Gossiper.instance.endpointStateMap.remove(InetAddressAndPort.getByName("127.0.0.2"));
        Gossiper.instance.liveEndpoints.remove(InetAddressAndPort.getByName("127.0.0.2"));
    }

    @Test
    public void testHasVersion3NodesShouldReturnTrueWhenNoVersion3NodesDetectedButNotAllVersionsKnown() throws Exception
    {
        Gossiper.instance.start(0);
        Gossiper.instance.expireUpgradeFromVersion();

        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(null);
        EndpointState es = new EndpointState((HeartBeatState) null);
        es.addApplicationState(ApplicationState.RELEASE_VERSION, null);
        Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName("127.0.0.3"), es);
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName("127.0.0.3"));

        es = new EndpointState((HeartBeatState) null);
        String previousPatchVersion = String.valueOf(CURRENT_VERSION.major) + '.' + (CURRENT_VERSION.minor) + '.' + Math.max(CURRENT_VERSION.patch - 1, 0);
        es.addApplicationState(ApplicationState.RELEASE_VERSION, factory.releaseVersion(previousPatchVersion));
        Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName("127.0.0.2"), es);
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName("127.0.0.2"));
        assertTrue(Gossiper.instance.hasMajorVersion3OrUnknownNodes());

        Gossiper.instance.endpointStateMap.remove(InetAddressAndPort.getByName("127.0.0.2"));
        Gossiper.instance.liveEndpoints.remove(InetAddressAndPort.getByName("127.0.0.2"));
    }

    @Test
    public void testAssassinatedNodeWillNotContributeToVersionCalculation() throws Exception
    {
        int initialNodeCount = 3;
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, initialNodeCount);
        for (int i = 0; i < initialNodeCount; i++)
        {
            Gossiper.instance.injectApplicationState(hosts.get(i), ApplicationState.RELEASE_VERSION, new VersionedValue.VersionedValueFactory(null).releaseVersion(SystemKeyspace.CURRENT_VERSION.toString()));
        }
        Gossiper.instance.start(1);
        Gossiper.instance.expireUpgradeFromVersion();

        // assassinate a non-existing node
        Gossiper.instance.assassinateEndpoint("127.0.0.4");

        assertTrue(Gossiper.instance.endpointStateMap.containsKey(InetAddressAndPort.getByName("127.0.0.4")));
        assertNull(Gossiper.instance.upgradeFromVersionSupplier.get().value());
        assertTrue(Gossiper.instance.upgradeFromVersionSupplier.get().canMemoize());
        assertFalse(Gossiper.instance.hasMajorVersion3OrUnknownNodes());
        assertFalse(Gossiper.instance.isUpgradingFromVersionLowerThan(CassandraVersion.CASSANDRA_3_4));
    }

    @Test
    public void testLargeGenerationJump() throws UnknownHostException, InterruptedException
    {
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        try
        {
            InetAddressAndPort remoteHostAddress = hosts.get(1);

            EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
            HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();

            //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
            assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

            HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration() + Gossiper.MAX_GENERATION_DIFFERENCE + 1);
            EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

            //The generation should have been updated because it isn't over Gossiper.MAX_GENERATION_DIFFERENCE in the future
            HeartBeatState actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());

            //Propose a generation 10 years in the future - this should be rejected.
            HeartBeatState badProposedRemoteHeartBeat = new HeartBeatState((int) (System.currentTimeMillis() / 1000) + Gossiper.MAX_GENERATION_DIFFERENCE * 10);
            EndpointState badProposedRemoteState = new EndpointState(badProposedRemoteHeartBeat);

            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, badProposedRemoteState));

            actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();

            //The generation should not have been updated because it is over Gossiper.MAX_GENERATION_DIFFERENCE in the future
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.endpointStateMap.clear();
        }
    }

    int stateChangedNum = 0;

    @Test
    public void testDuplicatedStateUpdate() throws Exception
    {
        VersionedValue.VersionedValueFactory valueFactory =
            new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

        SimpleStateChangeListener stateChangeListener = null;
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        try
        {
            InetAddressAndPort remoteHostAddress = hosts.get(1);

            EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
            HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();

            //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
            assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

            HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration());
            EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

            final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
            VersionedValue tokensValue = valueFactory.tokens(Collections.singletonList(token));
            proposedRemoteState.addApplicationState(ApplicationState.TOKENS, tokensValue);

            stateChangeListener = new SimpleStateChangeListener();
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                assertEquals(ApplicationState.TOKENS, onChangeParams.state);
                stateChangedNum++;
            });
            Gossiper.instance.register(stateChangeListener);

            stateChangedNum = 0;
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));
            assertEquals(1, stateChangedNum);

            HeartBeatState actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());

            // Clone a new HeartBeatState
            proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration(), proposedRemoteHeartBeat.getHeartBeatVersion());
            proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

            // Bump the heartbeat version and use the same TOKENS state
            proposedRemoteHeartBeat.updateHeartBeat();
            proposedRemoteState.addApplicationState(ApplicationState.TOKENS, tokensValue);

            // The following state change should only update heartbeat without updating the TOKENS state
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));
            assertEquals(1, stateChangedNum);

            actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.endpointStateMap.clear();
            if (stateChangeListener != null)
                Gossiper.instance.unregister(stateChangeListener);
        }
    }

    // Note: This test might fail if for some reason the node broadcast address is in 127.99.0.0/16
    @Test
    public void testReloadSeeds() throws UnknownHostException
    {
        Gossiper gossiper = new Gossiper(false);
        List<String> loadedList;

        // Initialize the seed list directly to a known set to start with
        gossiper.seeds.clear();
        InetAddressAndPort addr = InetAddressAndPort.getByAddress(InetAddress.getByName("127.99.1.1"));
        int nextSize = 4;
        List<InetAddressAndPort> nextSeeds = new ArrayList<>(nextSize);
        for (int i = 0; i < nextSize; i ++)
        {
            gossiper.seeds.add(addr);
            nextSeeds.add(addr);
            addr = InetAddressAndPort.getByAddress(InetAddresses.increment(addr.getAddress()));
        }
        Assert.assertEquals(nextSize, gossiper.seeds.size());

        // Add another unique address to the list
        addr = InetAddressAndPort.getByAddress(InetAddresses.increment(addr.getAddress()));
        nextSeeds.add(addr);
        nextSize++;
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(nextSeeds));
        loadedList = gossiper.reloadSeeds();

        // Check that the new entry was added
        Assert.assertEquals(nextSize, loadedList.size());
        for (InetAddressAndPort a : nextSeeds)
            assertTrue(loadedList.contains(a.toString()));

        // Check that the return value of the reloadSeeds matches the content of the getSeeds call
        // and that they both match the internal contents of the Gossiper seeds list
        Assert.assertEquals(loadedList.size(), gossiper.getSeeds().size());
        for (InetAddressAndPort a : gossiper.seeds)
        {
            assertTrue(loadedList.contains(a.toString()));
            assertTrue(gossiper.getSeeds().contains(a.toString()));
        }

        // Add a duplicate of the last address to the seed provider list
        int uniqueSize = nextSize;
        nextSeeds.add(addr);
        nextSize++;
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(nextSeeds));
        loadedList = gossiper.reloadSeeds();

        // Check that the number of seed nodes reported hasn't increased
        Assert.assertEquals(uniqueSize, loadedList.size());
        for (InetAddressAndPort a : nextSeeds)
            assertTrue(loadedList.contains(a.toString()));

        // Create a new list that has no overlaps with the previous list
        addr = InetAddressAndPort.getByAddress(InetAddress.getByName("127.99.2.1"));
        int disjointSize = 3;
        List<InetAddressAndPort> disjointSeeds = new ArrayList<>(disjointSize);
        for (int i = 0; i < disjointSize; i ++)
        {
            disjointSeeds.add(addr);
            addr = InetAddressAndPort.getByAddress(InetAddresses.increment(addr.getAddress()));
        }
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(disjointSeeds));
        loadedList = gossiper.reloadSeeds();

        // Check that the list now contains exactly the new other list.
        Assert.assertEquals(disjointSize, gossiper.getSeeds().size());
        Assert.assertEquals(disjointSize, loadedList.size());
        for (InetAddressAndPort a : disjointSeeds)
        {
            assertTrue(gossiper.getSeeds().contains(a.toString()));
            assertTrue(loadedList.contains(a.toString()));
        }

        // Set the seed node provider to return an empty list
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(new ArrayList<InetAddressAndPort>()));
        loadedList = gossiper.reloadSeeds();

        // Check that the in memory seed node list was not modified
        Assert.assertEquals(disjointSize, loadedList.size());
        for (InetAddressAndPort a : disjointSeeds)
            assertTrue(loadedList.contains(a.toString()));

        // Change the seed provider to one that throws an unchecked exception
        DatabaseDescriptor.setSeedProvider(new ErrorSeedProvider());
        loadedList = gossiper.reloadSeeds();

        // Check for the expected null response from a reload error
        assertNull(loadedList);

        // Check that the in memory seed node list was not modified and the exception was caught
        Assert.assertEquals(disjointSize, gossiper.getSeeds().size());
        for (InetAddressAndPort a : disjointSeeds)
            assertTrue(gossiper.getSeeds().contains(a.toString()));
    }

    @Test
    public void testNotFireDuplicatedNotificationsWithUpdateContainsOldAndNewState() throws UnknownHostException
    {
        VersionedValue.VersionedValueFactory valueFactory =
            new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        SimpleStateChangeListener stateChangeListener = null;
        try
        {
            InetAddressAndPort remoteHostAddress = hosts.get(1);
            EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
            HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();
            //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
            assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

            // Test begins
            AtomicInteger notificationCount = new AtomicInteger(0);
            HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration());
            EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);
            final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
            proposedRemoteState.addApplicationState(ApplicationState.STATUS, valueFactory.normal(Collections.singletonList(token)));

            stateChangeListener = new SimpleStateChangeListener();
            Gossiper.instance.register(stateChangeListener);

            // STEP 1. register verifier and apply state with just STATUS
            // simulate applying gossip state from a v3 peer
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                notificationCount.getAndIncrement();
                assertEquals("It should fire notification for STATUS when gossiper local state not yet has STATUS_WITH_PORT",
                             ApplicationState.STATUS, onChangeParams.state);
            });
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

            // STEP 2. includes both STATUS and STATUS_WITH_PORT. The gossiper knows that the remote peer is now in v4
            // update verifier and apply state again
            proposedRemoteState.addApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(Collections.singletonList(token)));
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                notificationCount.getAndIncrement();
                assertEquals("It should only fire notification for STATUS_WITH_PORT",
                             ApplicationState.STATUS_WITH_PORT, onChangeParams.state);
            });
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

            // STEP 3. somehow, the peer send only the STATUS in the update.
            proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);
            proposedRemoteState.addApplicationState(ApplicationState.STATUS, valueFactory.normal(Collections.singletonList(token)));
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                notificationCount.getAndIncrement();
                fail("It should not fire notification for STATUS");
            });

            assertEquals("Expect exact 2 notifications with the test setup",
                         2, notificationCount.get());
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.endpointStateMap.clear();
            if (stateChangeListener != null)
                Gossiper.instance.unregister(stateChangeListener);
        }
    }

    @Test
    public void testGossipAndStorageServiceCacheMismatchExist() throws IOException
    {
        SchemaLoader.prepareServer();
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        Gossiper.instance.start(1);
        EndpointState host0State = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(0));
        EndpointState host1State = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(1));

        Gossiper.instance.injectApplicationState(hosts.get(1), ApplicationState.RELEASE_VERSION, new VersionedValue.VersionedValueFactory(null).releaseVersion(SystemKeyspace.CURRENT_VERSION.toString()));
        host0State.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.normal(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        host1State.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.normal(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        Gossiper.instance.applyStateLocally(ImmutableMap.of(hosts.get(1), host0State));
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(0)), StorageService.instance.getTokensFor(hosts.get(0)));
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(1)), StorageService.instance.getTokensFor(hosts.get(1)));

        // edit the tokens in the Gossip cache to create a mismatch between the two caches
        host0State.addApplicationState(ApplicationState.TOKENS, StorageService.instance.valueFactory.tokens(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // first mismatch
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(2, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());

        // Disable the feature; host0's cache incoherence persists
        Gossiper.instance.setCompareGossipAndStorageServiceCache(false);
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(2, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        assertNotEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(0)), StorageService.instance.getTokensFor(hosts.get(0)));
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(1)), StorageService.instance.getTokensFor(hosts.get(1)));
    }

    @Test
    public void testGossipAndStorageServiceCacheMismatchDoNotExist() throws IOException
    {
        SchemaLoader.prepareServer();
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        Gossiper.instance.start(1);

        Gossiper.instance.injectApplicationState(hosts.get(1), ApplicationState.RELEASE_VERSION, new VersionedValue.VersionedValueFactory(null).releaseVersion(SystemKeyspace.CURRENT_VERSION.toString()));

        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(0)), StorageService.instance.getTokensFor(hosts.get(0)));
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(1)), StorageService.instance.getTokensFor(hosts.get(1)));
        // no impact to the cache if there is no cache coherence
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(0)), StorageService.instance.getTokensFor(hosts.get(0)));
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(1)), StorageService.instance.getTokensFor(hosts.get(1)));
    }

    @Test
    public void testGossipAndStorageServiceCacheIgnoreNonNormalNode() throws IOException
    {
        SchemaLoader.prepareServer();
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        Gossiper.instance.start(1);

        EndpointState host0State = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(0));
        Gossiper.instance.injectApplicationState(hosts.get(1), ApplicationState.RELEASE_VERSION, new VersionedValue.VersionedValueFactory(null).releaseVersion(SystemKeyspace.CURRENT_VERSION.toString()));

        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(0)), StorageService.instance.getTokensFor(hosts.get(0)));
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(1)), StorageService.instance.getTokensFor(hosts.get(1)));

        // Change host1's status to non-normal
        host0State.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.leaving(new ArrayList<>(){{add(new ByteOrderedPartitioner.BytesToken(new byte[]{ 1}));}}));
        // Now intentionally inject inconsistency between the Gossip cache and storage service cache
        host0State.addApplicationState(ApplicationState.TOKENS, StorageService.instance.valueFactory.tokens(new ArrayList<>(){{add(new ByteOrderedPartitioner.BytesToken(new byte[]{1,2,3}));}}));

        assertNotEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(0)), StorageService.instance.getTokensFor(hosts.get(0)));
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(1)), StorageService.instance.getTokensFor(hosts.get(1)));

        // Because host1 is not yet "NORMAL", it should be skipped in fixing the caches
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertNotEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(0)), StorageService.instance.getTokensFor(hosts.get(0)));
        assertEquals(StorageService.instance.getTokenMetadata().getTokens(hosts.get(1)), StorageService.instance.getTokensFor(hosts.get(1)));
    }

    @Test
    public void testGossipAndStorageServiceCacheMismatchExistAndFixedAutomatically() throws IOException
    {
        SchemaLoader.prepareServer();
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        Gossiper.instance.start(1);
        EndpointState host0State = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(0));
        EndpointState host1State = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(1));

        Gossiper.instance.injectApplicationState(hosts.get(1), ApplicationState.RELEASE_VERSION, new VersionedValue.VersionedValueFactory(null).releaseVersion(SystemKeyspace.CURRENT_VERSION.toString()));
        host0State.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.normal(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        host1State.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.normal(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        Gossiper.instance.applyStateLocally(ImmutableMap.of(hosts.get(1), host0State));
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());

        assertEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));


        // edit the tokens in the Gossip cache to create a mismatch between the two caches
        host0State.addApplicationState(ApplicationState.TOKENS, StorageService.instance.valueFactory.tokens(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // Gossip tokens should be used as the source of truth in case of a mismtach
        Collection<Token> newGossipTokensHost0 = getGossipCacheTokens(0);

        // first mismatch
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(2, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        assertNotEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertEquals(new Long(1), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertEquals(new Long(0), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // second mismatch
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(3, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(2, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        assertNotEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertEquals(new Long(2), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertEquals(new Long(0), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // third mismatch should fix the cache
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(4, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(3, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        assertEquals(getStorageServiceCacheTokens(0), newGossipTokensHost0);
        assertEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertEquals(new Long(3), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertEquals(new Long(0), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // no-op, as the caches have already been fixed (above)
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(5, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(3, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        assertEquals(getStorageServiceCacheTokens(0), newGossipTokensHost0);
        assertEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));
    }

    @Test
    public void testCacheMismatchIfStorageServiceCacheIsMissingTheEndpoint() throws IOException
    {
        SchemaLoader.prepareServer();
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        Gossiper.instance.start(1);
        EndpointState host0State = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(0));
        EndpointState host1State = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(1));

        Gossiper.instance.injectApplicationState(hosts.get(1), ApplicationState.RELEASE_VERSION, new VersionedValue.VersionedValueFactory(null).releaseVersion(SystemKeyspace.CURRENT_VERSION.toString()));
        host0State.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.normal(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        host1State.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.normal(new ArrayList<Token>(){{add(DatabaseDescriptor.getPartitioner().getRandomToken());}}));
        Gossiper.instance.applyStateLocally(ImmutableMap.of(hosts.get(1), host0State));
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        // Gossip tokens should be used as the source of truth in case of a mismtach
        Collection<Token> gossipTokensHost0 = getGossipCacheTokens(0);
        Collection<Token> gossipTokensHost1 = getGossipCacheTokens(1);

        assertEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));

        // remove the tokens from StorageService cache to create a mismatch between the two caches
        StorageService.instance.getTokenMetadata().removeEndpoint(hosts.get(0));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // first mismatch
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(2, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        try
        {
            getStorageServiceCacheTokens(0);
            fail("Expected AssertionError");
        }
        catch (AssertionError e)
        {
            assertTrue(e.getMessage().contains("Unable to get tokens for /127.0.0.1:7012; it is not a member"));
        }
        assertEquals(getStorageServiceCacheTokens(1), gossipTokensHost1);
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertEquals(new Long(1), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertEquals(new Long(0), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // second mismatch
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(3, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(2, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(0, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        try
        {
            getStorageServiceCacheTokens(0);
            fail("Expected AssertionError");
        }
        catch (AssertionError e)
        {
            assertTrue(e.getMessage().contains("Unable to get tokens for /127.0.0.1:7012; it is not a member"));
        }
        assertEquals(getStorageServiceCacheTokens(1), gossipTokensHost1);
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertEquals(new Long(2), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertEquals(new Long(0), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // third mismatch should fix the cache
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(4, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(3, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());

        assertEquals(getStorageServiceCacheTokens(0), gossipTokensHost0);
        assertEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), gossipTokensHost1);
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertEquals(new Long(3), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertEquals(new Long(0), Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));

        // no-op, as the caches have already been fixed (above)
        Gossiper.instance.gossipAndServicecacheMismatchDetectionAndResolution();
        assertEquals(5, GossipMetrics.gossipAndStorageServiceCacheCompare.getCount());
        assertEquals(3, GossipMetrics.gossipAndStorageServiceCacheMismatch.getCount());
        assertEquals(1, GossipMetrics.gossipAndStorageServiceCacheRepair.getCount());
        assertEquals(getStorageServiceCacheTokens(0), gossipTokensHost0);
        assertEquals(getStorageServiceCacheTokens(0), getGossipCacheTokens(0));
        assertEquals(getStorageServiceCacheTokens(1), gossipTokensHost1);
        assertEquals(getStorageServiceCacheTokens(1), getGossipCacheTokens(1));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(0)));
        assertNull(Gossiper.instance.gossipAndServiceCacheMismatchOccurredTracker.get(hosts.get(1)));
    }

    private Collection<Token> getStorageServiceCacheTokens(int hostIndex)
    {
        return StorageService.instance.getTokenMetadata().getTokens(hosts.get(hostIndex));
    }

    private Collection<Token> getGossipCacheTokens(int hostIndex)
    {
        return StorageService.instance.getTokensFor(hosts.get(hostIndex));
    }

    static class SimpleStateChangeListener implements IEndpointStateChangeSubscriber
    {
        static class OnChangeParams
        {
            InetAddressAndPort endpoint;
            ApplicationState state;
            VersionedValue value;

            OnChangeParams(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
            {
                this.endpoint = endpoint;
                this.state = state;
                this.value = value;
            }
        }

        private volatile Consumer<OnChangeParams> onChangeVerifier;

        public void setOnChangeVerifier(Consumer<OnChangeParams> verifier)
        {
            onChangeVerifier = verifier;
        }

        public void onJoin(InetAddressAndPort endpoint, EndpointState epState) {}
        public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
        public void onAlive(InetAddressAndPort endpoint, EndpointState state) {}
        public void onDead(InetAddressAndPort endpoint, EndpointState state) {}
        public void onRemove(InetAddressAndPort endpoint) {}
        public void onRestart(InetAddressAndPort endpoint, EndpointState state) {}

        public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
        {
            onChangeVerifier.accept(new OnChangeParams(endpoint, state, value));
        }
    }

    static class TestSeedProvider implements SeedProvider
    {
        private List<InetAddressAndPort> seeds;

        TestSeedProvider(List<InetAddressAndPort> seeds)
        {
            this.seeds = seeds;
        }

        @Override
        public List<InetAddressAndPort> getSeeds()
        {
            return seeds;
        }
    }

    // A seed provider for testing which throws assertion errors when queried
    static class ErrorSeedProvider implements SeedProvider
    {
        @Override
        public List<InetAddressAndPort> getSeeds()
        {
            assert(false);
            return new ArrayList<>();
        }
    }
}
