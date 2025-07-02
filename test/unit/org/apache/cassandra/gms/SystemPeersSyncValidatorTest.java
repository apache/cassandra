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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ScheduledFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.GossipMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SystemPeersSyncValidatorTest
{
    private InetAddressAndPort myself;
    private InetAddressAndPort peerOrphan;
    private InetAddressAndPort peerMissing;

    private SystemPeersSyncValidator validator = SystemPeersSyncValidator.instance;
    private int defaultSyncValidatorInterval = DatabaseDescriptor.getSystemPeersSyncValidatorIntervalInMin();

    @BeforeClass
    public static void init()
    {
        // to be able to addSavedEndpoint
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();

        CommitLog.instance.start();
    }

    @Before
    public void setup() throws UnknownHostException
    {
        myself = FBUtilities.getBroadcastAddressAndPort();
        peerOrphan = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.2"));
        peerMissing = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.3"));
        GossipMetrics.orphanPeerInSystemTable.dec(GossipMetrics.orphanPeerInSystemTable.getCount());
        GossipMetrics.missingPeerInGossip.dec(GossipMetrics.missingPeerInGossip.getCount());
        DatabaseDescriptor.setSystemPeersSyncValidatorIntervalInMin(1);
    }

    @After
    public void tearDown()
    {
        Gossiper.instance.endpointStateMap.clear();
        StorageService.instance.getTokenMetadata().clearUnsafe();
        DatabaseDescriptor.setSystemPeersSyncValidatorIntervalInMin(defaultSyncValidatorInterval);
        validator.stop();
    }

    @Test
    public void testSystemPeersInSync()
    {
        // myself should be removed when comparing the gossip state and local table
        Gossiper.instance.addSavedEndpoint(myself);
        Gossiper.instance.addSavedEndpoint(peerOrphan);
        SystemKeyspace.updatePeerInfo(peerOrphan, "release_version", "4.1.3");
        Gossiper.instance.addSavedEndpoint(peerMissing);
        SystemKeyspace.updatePeerInfo(peerMissing, "release_version", "4.1.3");

        validator.run();

        assertEquals(0, GossipMetrics.orphanPeerInSystemTable.getCount());
        assertEquals(0, GossipMetrics.missingPeerInGossip.getCount());
    }

    @Test
    public void testOrphanPeers()
    {
        Gossiper.instance.addSavedEndpoint(myself);
        Gossiper.instance.addSavedEndpoint(peerMissing);
        SystemKeyspace.updatePeerInfo(peerMissing, "release_version", "4.1.3");

        // insert a stale info in system.peers_v2
        SystemKeyspace.updatePeerInfo(peerOrphan, "release_version", "4.1.3");

        validator.run();

        assertEquals(1, GossipMetrics.orphanPeerInSystemTable.getCount());
        assertEquals(0, GossipMetrics.missingPeerInGossip.getCount());
    }

    @Test
    public void testMissingPeers()
    {
        Gossiper.instance.addSavedEndpoint(myself);
        Gossiper.instance.addSavedEndpoint(peerOrphan);
        SystemKeyspace.updatePeerInfo(peerOrphan, "release_version", "4.1.3");
        Gossiper.instance.addSavedEndpoint(peerMissing);
        SystemKeyspace.updatePeerInfo(peerMissing, "release_version", "4.1.3");

        // remove a peer from peers table
        SystemKeyspace.removeEndpoint(peerMissing);

        validator.run();

        assertEquals(0, GossipMetrics.orphanPeerInSystemTable.getCount());
        assertEquals(1, GossipMetrics.missingPeerInGossip.getCount());
    }

    @Test
    public void testValidatorSetupAndStop()
    {
        assertFalse(validator.isRunning());
        validator.setup();
        assertTrue(validator.isRunning());
        ScheduledFuture<?> firstTask = validator.validateTask;

        // setup should be idempotent
        validator.setup();
        ScheduledFuture<?> secondTask = validator.validateTask;
        assertSame(firstTask, secondTask);

        // stop
        validator.stop();
        assertFalse(validator.isRunning());
        assertTrue(secondTask.isCancelled());
        assertNull(validator.validateTask);
    }
}
