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

package org.apache.cassandra.tcm;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.tcm.membership.MembershipUtils.endpoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CMSLookupTest
{
    private static final Logger logger = LoggerFactory.getLogger(CMSLookupTest.class);

    Random random;
    int nodeCount = 10;
    Location location = new Location("dc1", "rack1");
    List<InetAddressAndPort> endpoints;

    @Before
    public void setup()
    {
        long seed = System.nanoTime();
        logger.info("seed: {}", seed);
        random =new Random(seed);
        endpoints = MembershipUtils.uniqueEndpoints(random, nodeCount * 2);
    }

    private Directory initDirectory()
    {
        Directory directory = new Directory();
        for (int i = 0; i < nodeCount; i++)
            directory = directory.with(new NodeAddresses(endpoints.get(i)), location);
        return directory;
    }

    private CMSLookup initLookup(ClusterMetadata metadata)
    {
        CMSLookup.InitialBuilder builder = CMSLookup.builder(metadata);
        for (int i = 0; i < nodeCount; i++)
        {
            InetAddressAndPort endpoint = endpoints.get(i);
            InetAddressAndPort newEndpoint = endpoints.get(i + nodeCount);
            builder = builder.withOverride(metadata.directory.peerId(endpoint), endpoint, newEndpoint);
        }
        return builder.build();
    }

    @Test
    public void prohibitInitialBuildWithoutOverrides()
    {
        Directory directory = initDirectory();
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
        metadata = metadata.transformer().with(directory).build().metadata;
        CMSLookup.InitialBuilder builder = CMSLookup.builder(metadata);
        assertFalse(builder.hasOverrides());
        try
        {
            builder.build();
            fail("Expected exception");
        }
        catch (IllegalStateException e)
        {
            // expected
        }
    }

    @Test
    public void rebuildWithAllOverridesStillRequired()
    {
        Directory directory = initDirectory();
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
        metadata = metadata.transformer().with(directory).build().metadata;
        CMSLookup l1 = initLookup(metadata);

        // Identical content, but bump the lastModified epoch
        Epoch bumpedEpoch = metadata.directory.lastModified().nextEpoch();
        ClusterMetadata next = metadata.transformer().with(metadata.directory.withLastModified(bumpedEpoch)).build().metadata;
        // Test rebuild both from a snapshot & non-snapshot
        CMSLookup l2;
        for (boolean fromSnapshot : new boolean[] {true, false})
        {
            l2 = l1.rebuild(metadata, next, fromSnapshot);
            assertTrue(l2.isActive());
            assertSame(l1, l2);
        }
    }

    @Test
    public void rebuildWithSomeOverridesStillRequired()
    {
        Directory directory = initDirectory();
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
        metadata = metadata.transformer().with(directory).build().metadata;
        CMSLookup l1 = initLookup(metadata);

        // Update directory with 2 of the overrides
        int nodesToUpdate = 2;
        Set<NodeId> updatedNodes = new HashSet<>();
        Directory newDirectory = metadata.directory;
        for (int i = 0; i < nodesToUpdate; i++)
        {
            InetAddressAndPort oldEndpoint = endpoints.get(i);
            InetAddressAndPort newEndpoint = endpoints.get(i + nodeCount);
            NodeId id = metadata.directory.peerId(oldEndpoint);
            updatedNodes.add(id);
            newDirectory = newDirectory.withNodeAddresses(id, new NodeAddresses(newEndpoint));
        }
        ClusterMetadata next = metadata.transformer().with(newDirectory).build().metadata;

        // Test rebuild both from a snapshot & non-snapshot
        CMSLookup l2;
        for (boolean fromSnapshot : new boolean[] {true, false})
        {
            l2 = l1.rebuild(metadata, next, fromSnapshot);
            assertNotSame(l1, l2);
            assertTrue(l2.isActive());
            // overrides for the updated nodes should have been removed, while the rest should still be present
            for (NodeId id : newDirectory.peerIds())
            {
                if (updatedNodes.contains(id))
                    assertFalse(l2.overrides.containsKey(id));
                else
                {
                    assertTrue(l2.overrides.containsKey(id));
                    assertEquals(l2.overrides.get(id).left(), directory.endpoint(id));
                }
            }
        }
    }

    @Test
    public void rebuildWithNoOverridesStillRequired()
    {
        Directory directory = initDirectory();
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
        metadata = metadata.transformer().with(directory).build().metadata;
        CMSLookup l1 = initLookup(metadata);

        // Update directory with all of the overrides applied
        int nodesToUpdate = nodeCount;
        Set<NodeId> updatedNodes = new HashSet<>();
        Directory newDirectory = metadata.directory;
        for (int i = 0; i < nodesToUpdate; i++)
        {
            InetAddressAndPort oldEndpoint = endpoints.get(i);
            InetAddressAndPort newEndpoint = endpoints.get(i + nodeCount);
            NodeId id = metadata.directory.peerId(oldEndpoint);
            updatedNodes.add(id);
            newDirectory = newDirectory.withNodeAddresses(id, new NodeAddresses(newEndpoint));
        }

        // Test rebuild both from a snapshot & non-snapshot
        CMSLookup l2;
        for (boolean fromSnapshot : new boolean[]{ true, false })
        {
            ClusterMetadata next = metadata.transformer().with(newDirectory).build().metadata;
            l2 = l1.rebuild(metadata, next, fromSnapshot);
            assertNotSame(l1, l2);
            assertTrue(l2.overrides.isEmpty());
            assertFalse(l2.isActive());

            // now try rebuilding again, which should be a no-op. Need to make sure the prev and next directories are
            // not identical to ensure it's actually the lookup state that determines this.
            Directory newNewDirectory = next.directory.with(new NodeAddresses(endpoint(255)), location);
            ClusterMetadata nextNext = next.transformer().with(newNewDirectory).build().metadata;
            CMSLookup l3 = l2.rebuild(next, nextNext, fromSnapshot);
            assertSame(l2, l3);
        }
    }

    @Test
    public void rebuildWithNodeRemovedFromNext()
    {
        Directory directory = initDirectory();
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
        metadata = metadata.transformer().with(directory).build().metadata;
        CMSLookup l1 = initLookup(metadata);

        // Remove 2 nodes
        Epoch removalEpoch = metadata.directory.lastModified().nextEpoch();
        Directory newDirectory = metadata.directory.without(removalEpoch, new NodeId(1))
                                                   .without(removalEpoch, new NodeId(2));
        ClusterMetadata next = metadata.transformer().with(newDirectory).build().metadata;

        // Test from both snapshot & non-snapshot
        CMSLookup l2;
        for (boolean fromSnapshot : new boolean[] {true, false})
        {
            l2 = l1.rebuild(metadata, next, fromSnapshot);
            assertNotSame(l1, l2);
            assertTrue(l2.isActive());
            for (NodeId id : metadata.directory.peerIds())
            {
                if (id.id() <= 2)
                    assertFalse(l2.overrides.containsKey(id));
                else
                {
                    assertTrue(l2.overrides.containsKey(id));
                    assertEquals(l2.overrides.get(id).left(), directory.endpoint(id));
                }
            }
        }
    }

    @Test
    public void rebuildWithNodeRemovedFromPreviousAndNext()
    {
        Directory directory = initDirectory();
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
        metadata = metadata.transformer().with(directory).build().metadata;
        CMSLookup l1 = initLookup(metadata);

        // Remove 2 nodes from both inputs into CMSLookup::rebuild (prev & next).
        // This should not be possible on the normal path, as rebuild is called on next's lookup instance in a
        // pre-commit listener and next should inherit its lookup from prev.
        Epoch removalEpoch = metadata.directory.lastModified().nextEpoch();
        Directory newDirectory = metadata.directory.without(removalEpoch, new NodeId(1))
                                                   .without(removalEpoch, new NodeId(2));
        ClusterMetadata prev = metadata.transformer().with(newDirectory).build().metadata;
        ClusterMetadata next = metadata.transformer().build().metadata;
        CMSLookup l2;
        for (boolean fromSnapshot : new boolean[] {true, false})
        {
            l2 = l1.rebuild(prev, next, fromSnapshot);
            assertNotSame(l1, l2);
            assertTrue(l2.isActive());
            // note: we are iterating over the full original set of node ids here, including the removed ones
            for (NodeId id : metadata.directory.peerIds())
            {
                if (id.id() <= 2)
                    assertFalse(l2.overrides.containsKey(id));
                else
                {
                    assertTrue(l2.overrides.containsKey(id));
                    assertEquals(l2.overrides.get(id).left(), directory.endpoint(id));
                }
            }
        }
    }

    @Test
    public void rebuildWithNodeLeftInNext()
    {
        Directory directory = initDirectory();
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
        metadata = metadata.transformer().with(directory).build().metadata;
        CMSLookup l1 = initLookup(metadata);

        // Mark 2 nodes as having left the cluster.
        ClusterMetadata next = metadata.transformer()
                                       .left(new NodeId(1))
                                       .left(new NodeId(2))
                                       .build().metadata;
        CMSLookup l2;
        for (boolean fromSnapshot : new boolean[] {true, false})
        {
            l2 = l1.rebuild(metadata, next, fromSnapshot);
            assertNotSame(l1, l2);
            assertTrue(l2.isActive());
            for (NodeId id : metadata.directory.peerIds())
            {
                if (id.id() <= 2)
                    assertFalse(l2.overrides.containsKey(id));
                else
                {
                    assertTrue(l2.overrides.containsKey(id));
                    assertEquals(l2.overrides.get(id).left(), directory.endpoint(id));
                }
            }
        }
    }
}
