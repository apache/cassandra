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

import java.util.EnumSet;
import java.util.function.BiFunction;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.Assassinate;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class UnregisterTest
{
    private final KeyspaceMetadata ksm = KeyspaceMetadata.create("ks_nts", KeyspaceParams.nts("dc1", 3, "dc2", 3));
    private final Keyspaces kss = Keyspaces.of(DistributedMetadataLogKeyspace.initialMetadata(Sets.newHashSet("dc1", "dc2")), ksm);
    private final DistributedSchema initialSchema = new DistributedSchema(kss);

    @Test
    public void testBasicUnregister()
    {
        unregisterHelper((toUnregister, metadata) -> {
            metadata = assassinate(toUnregister, metadata);
            metadata = unregister(toUnregister, metadata);
            return metadata;
        });
    }

    @Test
    public void badStateUnregister()
    {
        unregisterHelper((toUnregister, metadata) -> {
            metadata = left(toUnregister, metadata);
            metadata = unregister(toUnregister, metadata);
            return metadata;
        });
    }

    private void unregisterHelper(BiFunction<Integer, ClusterMetadata, ClusterMetadata> f)
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, initialSchema);

        int toUnregister = 5;
        for (int i = 0; i < 10; i++)
        {
            // node we're unregistering is only node in dc2 - make sure the dc is gone after unregistration
            String dc = "dc"+(i == toUnregister ? "2" :"1");
            metadata = unsafejoin(i, register(i, dc, metadata));
        }
        Token t = token(toUnregister, metadata);
        InetAddressAndPort ep = ep(toUnregister);
        NodeId nodeId = metadata.directory.peerId(ep(toUnregister));
        metadata = f.apply(toUnregister, metadata);
        assertNoTrace(ep, nodeId, t, metadata);
    }

    private ClusterMetadata left(int i, ClusterMetadata metadata)
    {
        NodeId nodeId = metadata.directory.peerId(ep(i));
        ClusterMetadata.Transformer t = metadata.transformer().withNodeState(nodeId, NodeState.LEFT);
        return t.build().metadata;
    }

    private static void assertNoTrace(InetAddressAndPort ep, NodeId nodeId, Token t, ClusterMetadata metadata)
    {
        assertNull(metadata.tokenMap.owner(t));
        assertFalse(metadata.directory.states.containsKey(nodeId));
        assertFalse(metadata.directory.peerIds().contains(nodeId));
        assertFalse(metadata.directory.allAddresses().contains(ep));
        assertFalse(metadata.directory.allJoinedEndpoints().contains(ep));
        assertFalse(metadata.directory.allDatacenterRacks().containsKey("dc2"));
        assertFalse(metadata.directory.knownDatacenters().contains("dc2"));
        metadata.placements.asMap().forEach((params, placement) -> {
            assertFalse(Streams.concat(placement.writes.endpoints.stream(), placement.reads.endpoints.stream()).anyMatch((fr) -> fr.endpoints().contains(ep)));
        });
    }

    private Token token(int i, ClusterMetadata metadata)
    {
        NodeId nodeId = metadata.directory.peerId(ep(i));
        return metadata.tokenMap.tokens(nodeId).iterator().next();
    }

    private ClusterMetadata assassinate(int i, ClusterMetadata metadata)
    {
        return new Assassinate(metadata.directory.peerId(ep(i)), new UniformRangePlacement()).execute(metadata).success().metadata;
    }

    private ClusterMetadata unregister(int i, ClusterMetadata metadata )
    {
        return new Unregister(metadata.directory.peerId(ep(i)), EnumSet.of(NodeState.LEFT), new UniformRangePlacement()).execute(metadata).success().metadata;
    }

    private ClusterMetadata register(int i, String dc, ClusterMetadata metadata)
    {
        return new Register(new NodeAddresses(ep(i)), new Location(dc, "rack1"), NodeVersion.CURRENT).execute(metadata).success().metadata;
    }

    private ClusterMetadata unsafejoin(int i, ClusterMetadata metadata)
    {
        NodeId nodeId = metadata.directory.peerId(ep(i));
        return new UnsafeJoin(nodeId, ImmutableSet.of(Murmur3Partitioner.instance.getRandomToken()), new UniformRangePlacement()).execute(metadata).success().metadata;
    }

    private InetAddressAndPort ep(int i)
    {
        return InetAddressAndPort.getByNameUnchecked("127.0.0."+i);
    }
}
