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

package org.apache.cassandra.tcm.listeners;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.CMSMembership;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.OwnershipUtils;

import static org.apache.cassandra.tcm.sequences.InProgressSequenceCancellationTest.directory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.psjava.util.AssertStatus.assertTrue;

public class PlacementsChangeListenerTest
{
    private static final Logger logger = LoggerFactory.getLogger(PlacementsChangeListenerTest.class);
    static Random random;
    static Epoch e;
    static NodeId node1;
    static KeyspaceMetadata ks1;
    static Directory dir;

    @BeforeClass
    public static void setupClass()
    {
        long seed = System.nanoTime();
        logger.info("Seed: {}", seed);
        random = new Random(seed);
        e = Epoch.create(10);
        node1 = new NodeId(1);
        ks1 = KeyspaceMetadata.create("ks1", KeyspaceParams.simple(1));
        CassandraRelevantProperties.TCM_SORT_REPLICA_GROUPS.setBoolean(false);
        dir = directory(new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.1")), OwnershipUtils.replicationForRandomPlacements, random);
    }

    @Test
    public void testPlacementChange()
    {
        DataPlacements before = OwnershipUtils.randomPlacements(dir, random).withLastModified(e);
        DataPlacements.Builder builder = before.unbuild();
        before.forEach((params, placement) -> {
            Replica remove = placement.writes.byEndpoint().flattenValues().iterator().next();
            Replica add = Replica.fullReplica(MembershipUtils.endpoint(99), remove.range());
            DataPlacement newPlacement = placement.unbuild()
                                                  .withoutWriteReplica(e.nextEpoch(), remove)
                                                  .withWriteReplica(e.nextEpoch(), add).build();
            builder.with(params, newPlacement);
        });
        DataPlacements after = builder.build().withLastModified(e.nextEpoch());

        // only placements are different
        ClusterMetadata prev = metadata(e, before, Keyspaces.of(ks1), node1);
        ClusterMetadata next = metadata(e.nextEpoch(), after, Keyspaces.of(ks1), node1);
        assertNotEquals(prev.placements().lastModified(), next.placements().lastModified());
        assertFalse(prev.placements().equivalentTo(next.placements()));
        assertEquals(prev.schema.getKeyspaces().size(), next.schema.getKeyspaces().size());
        assertEquals(prev.schema.getKeyspaceMetadata("ks1").params, next.schema.getKeyspaceMetadata("ks1").params);
        assertEquals(prev.cmsMembership, next.cmsMembership);

        assertOnChangeEvent(prev, next);
    }

    @Test
    public void testKeyspaceCountChange()
    {
        DataPlacements placements = OwnershipUtils.randomPlacements(dir, random).withLastModified(e);
        KeyspaceMetadata ks2 = KeyspaceMetadata.create("ks2", KeyspaceParams.simple(1));

        // only keyspace counts are different
        ClusterMetadata prev = metadata(e, placements, Keyspaces.of(ks1), node1);
        ClusterMetadata next = metadata(e, placements, Keyspaces.of(ks1, ks2), node1);
        assertEquals(prev.placements().lastModified(), next.placements().lastModified());
        assertTrue(prev.placements().equivalentTo(next.placements()));
        assertNotEquals(prev.schema.getKeyspaces().size(), next.schema.getKeyspaces().size());
        assertEquals(prev.schema.getKeyspaceMetadata("ks1").params, next.schema.getKeyspaceMetadata("ks1").params);
        assertEquals(prev.cmsMembership, next.cmsMembership);

        assertOnChangeEvent(prev, next);
    }

    @Test
    public void testKeyspaceParamsChange()
    {
        DataPlacements placements = OwnershipUtils.randomPlacements(dir, random).withLastModified(e);
        KeyspaceMetadata ks1a = KeyspaceMetadata.create("ks1", KeyspaceParams.simple(2));

        // only keyspace params are different
        ClusterMetadata prev = metadata(e, placements, Keyspaces.of(ks1), node1);
        ClusterMetadata next = metadata(e, placements, Keyspaces.of(ks1a), node1);
        assertEquals(prev.placements().lastModified(), next.placements().lastModified());
        assertTrue(prev.placements().equivalentTo(next.placements()));
        assertEquals(prev.schema.getKeyspaces().size(), next.schema.getKeyspaces().size());
        assertNotEquals(prev.schema.getKeyspaceMetadata("ks1").params, next.schema.getKeyspaceMetadata("ks1").params);
        assertEquals(prev.cmsMembership, next.cmsMembership);

        assertOnChangeEvent(prev, next);
    }

    @Test
    public void testCMSMembershipChange()
    {
        DataPlacements placements = OwnershipUtils.randomPlacements(dir, random).withLastModified(e);
        NodeId node2 = new NodeId(2);

        // only cms memberships are different
        ClusterMetadata prev = metadata(e, placements, Keyspaces.of(ks1), node1);
        ClusterMetadata next = metadata(e, placements, Keyspaces.of(ks1), node1, node2);
        assertEquals(prev.placements().lastModified(), next.placements().lastModified());
        assertTrue(prev.placements().equivalentTo(next.placements()));
        assertEquals(prev.schema.getKeyspaces().size(), next.schema.getKeyspaces().size());
        assertEquals(prev.schema.getKeyspaceMetadata("ks1").params, next.schema.getKeyspaceMetadata("ks1").params);
        assertNotEquals(prev.cmsMembership, next.cmsMembership);

        assertOnChangeEvent(prev, next);
    }

    private static ClusterMetadata metadata(Epoch epoch,
                                            DataPlacements placements,
                                            Keyspaces keyspaces,
                                            NodeId...cmsNode)
    {
        CMSMembership cms = CMSMembership.EMPTY;
        for (NodeId n : cmsNode)
            cms = cms.startJoining(n).finishJoining(n);

        ClusterMetadata.Transformer t = ClusterMetadataTestHelper.minimalForTesting(epoch,
                                                                                    Murmur3Partitioner.instance,
                                                                                    new DistributedSchema(keyspaces, epoch),
                                                                                    cms)
                                                                 .forceEpoch(epoch)
                                                                 .transformer()
                                                                 .with(placements);

        return t.build().metadata;
    }

    private static void assertOnChangeEvent(ClusterMetadata prev, ClusterMetadata next)
    {
        AtomicInteger cnt = new AtomicInteger(0);
        PlacementsChangeListener listener = new PlacementsChangeListener(cnt::incrementAndGet);
        listener.notifyPostCommit(prev, next, false);
        assertEquals(1, cnt.get());
    }
}
