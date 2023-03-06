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

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata.Transformer.Transformed;
import org.apache.cassandra.tcm.extensions.EpochValue;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.extensions.IntValue;
import org.apache.cassandra.tcm.extensions.StringValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.Version;
import org.mockito.Mockito;

import static org.apache.cassandra.tcm.MetadataKeys.DATA_PLACEMENTS;
import static org.apache.cassandra.tcm.MetadataKeys.IN_PROGRESS_SEQUENCES;
import static org.apache.cassandra.tcm.MetadataKeys.LOCKED_RANGES;
import static org.apache.cassandra.tcm.MetadataKeys.NODE_DIRECTORY;
import static org.apache.cassandra.tcm.MetadataKeys.SCHEMA;
import static org.apache.cassandra.tcm.MetadataKeys.TOKEN_MAP;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.randomPlacements;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.token;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.affectedRanges;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.epoch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataTransformationTest
{
    @BeforeClass
    public static void init()
    {
        ServerTestUtils.initSnitch();
    }

    long seed = System.nanoTime();
    Random random = new Random(seed);

    @Test
    public void testModifyMembershipAndOwnership()
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        Transformed transformed = metadata.transformer().build();
        assertTrue(transformed.modifiedKeys.isEmpty());

        NodeAddresses addresses = MembershipUtils.nodeAddresses(random);
        transformed = metadata.transformer()
                              .register(addresses, new Location("dc1", "rack1"), NodeVersion.CURRENT)
                              .build();
        assertModifications(transformed, NODE_DIRECTORY);
        NodeId n1 = transformed.metadata.directory.peerId(addresses.broadcastAddress);

        NodeAddresses updated = getNonConflictingAddresses(random, transformed.metadata.directory);
        transformed = transformed.metadata.transformer().withNodeInformation(n1, NodeVersion.CURRENT, updated).build();
        assertModifications(transformed, NODE_DIRECTORY);

        transformed = transformed.metadata.transformer().proposeToken(n1, Collections.singleton(token(100))).build();
        assertModifications(transformed, NODE_DIRECTORY, TOKEN_MAP);

        transformed = transformed.metadata.transformer().unproposeTokens(n1).build();
        assertModifications(transformed, NODE_DIRECTORY, TOKEN_MAP);

        transformed = transformed.metadata.transformer().proposeToken(n1, Collections.singleton(token(100))).join(n1).build();
        assertModifications(transformed, NODE_DIRECTORY, TOKEN_MAP);

        transformed = transformed.metadata.transformer().withNodeState(n1, NodeState.REGISTERED).build();
        assertModifications(transformed, NODE_DIRECTORY);

        NodeAddresses replaceAddresses = getNonConflictingAddresses(random, transformed.metadata.directory);
        transformed = transformed.metadata.transformer()
                                          .register(replaceAddresses, new Location("dc1", "rack1"), NodeVersion.CURRENT)
                                          .build();
        NodeId n2 = transformed.metadata.directory.peerId(replaceAddresses.broadcastAddress);
        transformed = transformed.metadata.transformer().replaced(n1, n2).build();
        assertModifications(transformed, NODE_DIRECTORY, TOKEN_MAP);
        transformed = transformed.metadata.transformer().proposeRemoveNode(n2).build();
        assertModifications(transformed, TOKEN_MAP);

        NodeAddresses nextAddresses = getNonConflictingAddresses(random, transformed.metadata.directory);
        transformed = transformed.metadata.transformer()
                                          .register(nextAddresses, new Location("dc1", "rack1"), NodeVersion.CURRENT)
                                          .build();
        NodeId n3 = transformed.metadata.directory.peerId(nextAddresses.broadcastAddress);
        transformed = transformed.metadata.transformer()
                                          .withNodeState(n3, NodeState.BOOTSTRAPPING)
                                          .build();
        assertModifications(transformed, NODE_DIRECTORY);
        transformed = transformed.metadata.transformer().left(n3).build();
        assertModifications(transformed, NODE_DIRECTORY, TOKEN_MAP);
    }

    @Test
    public void testModifySchema()
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        Transformed transformed = metadata.transformer().with(DistributedSchema.empty()).build();
        assertModifications(transformed, SCHEMA);
    }

    @Test
    public void testModifyPlacements()
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        // Initial state has DataPlacements.empty(), so supplying the same value results in no change
        Transformed transformed = metadata.transformer().with(DataPlacements.empty()).build();
        assertModifications(transformed);

        DataPlacements trivial = DataPlacements.builder(1).with(ReplicationParams.simple(1), DataPlacement.empty()).build();
        transformed = transformed.metadata.transformer().with(trivial).build();
        assertModifications(transformed, DATA_PLACEMENTS);
    }

    @Test
    public void testModifyLockedRanges()
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        // Initial state has LockedRanges.EMPTY, so supplying the same value results in no change
        Transformed transformed = metadata.transformer().with(LockedRanges.EMPTY).build();
        assertModifications(transformed);

        LockedRanges.AffectedRanges ranges = affectedRanges(randomPlacements(random), random);
        LockedRanges trivial = transformed.metadata.lockedRanges.lock(LockedRanges.keyFor(epoch(random)), ranges);
        transformed = transformed.metadata.transformer().with(trivial).build();
        assertModifications(transformed, LOCKED_RANGES);
    }

    @Test
    public void testModifyInProgressSequences()
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        // Initial state has InProgressSequences.EMPTY, so supplying the same value results in no change
        Transformed transformed = metadata.transformer().with(InProgressSequences.EMPTY).build();
        assertModifications(transformed);

        InProgressSequences trivial = metadata.inProgressSequences.with(new NodeId(UUID.randomUUID()),
                                                                        Mockito.mock(InProgressSequence.class));
        transformed = transformed.metadata.transformer().with(trivial).build();
        assertModifications(transformed, IN_PROGRESS_SEQUENCES);
    }

    @Test
    public void testModifyExtendedMetadata()
    {
        long seed = System.nanoTime();
        Random r = new Random(seed);
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        ExtensionKey<Epoch, EpochValue> testKey = new ExtensionKey<>("foo.bar", EpochValue.class);
        Epoch start = epoch(r);

        // write initial value with extension key
        Transformed transformed = metadata.transformer().with(testKey, EpochValue.create(start)).build();
        assertModifications(transformed, testKey);
        ExtensionValue<?> stored = transformed.metadata.extensions.get(testKey);
        Epoch actual = (Epoch) stored.getValue();
        assertEquals(start, actual);

        // overwrite initial value
        Epoch updated = start.nextEpoch();
        transformed = transformed.metadata.transformer().with(testKey, EpochValue.create(updated)).build();
        assertModifications(transformed, testKey);
        stored = transformed.metadata.extensions.get(testKey);
        actual = (Epoch) stored.getValue();
        assertEquals(updated, actual);

        // don't overwrite using withIfAbsent
        Epoch updatedAgain = updated.nextEpoch();
        transformed = transformed.metadata.transformer().withIfAbsent(testKey, EpochValue.create(updatedAgain)).build();
        assertModifications(transformed);
        stored = transformed.metadata.extensions.get(testKey);
        actual = (Epoch) stored.getValue();
        assertEquals(updated, actual);

        // remove value
        transformed = transformed.metadata.transformer().without(testKey).build();
        assertModifications(transformed, testKey);
        assertNull(transformed.metadata.extensions.get(testKey));

        // now write usint withIfAbsent
        transformed = transformed.metadata.transformer().withIfAbsent(testKey, EpochValue.create(updatedAgain)).build();
        assertModifications(transformed, testKey);
        stored = transformed.metadata.extensions.get(testKey);
        actual = (Epoch) stored.getValue();
        assertEquals(updatedAgain, actual);
    }

    @Test
    public void testRoundTripMetadataExtensions() throws IOException
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        ExtensionKey<Epoch, EpochValue> epochKey = new ExtensionKey<>("test.epoch", EpochValue.class);
        EpochValue e = EpochValue.create(epoch(random));

        ExtensionKey<Integer, IntValue> intKey = new ExtensionKey<>("test.int", IntValue.class);
        IntValue i = IntValue.create(random.nextInt());

        ExtensionKey<String, StringValue> stringKey = new ExtensionKey<>("test.string", StringValue.class);
        StringValue s = StringValue.create("" + random.nextInt());

        Transformed transformed = metadata.transformer()
                                          .with(epochKey, e)
                                          .with(intKey, i)
                                          .with(stringKey, s)
                                          .build();
        assertModifications(transformed, epochKey, intKey, stringKey);

        DataOutputBuffer out = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(transformed.metadata, out, Version.V0);
        DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
        ClusterMetadata newMeta = ClusterMetadata.serializer.deserialize(in, Version.V0);

        assertEquals(e.getValue(), newMeta.extensions.get(epochKey).getValue());
        assertEquals(i.getValue(), newMeta.extensions.get(intKey).getValue());
        assertEquals(s.getValue(), newMeta.extensions.get(stringKey).getValue());
    }

    private static NodeAddresses getNonConflictingAddresses(Random random, Directory directory)
    {
        outer:
        while (true)
        {
            NodeAddresses addresses = MembershipUtils.nodeAddresses(random);
            for (NodeAddresses existing : directory.addresses.values())
                if (addresses.conflictsWith(existing))
                    continue outer;

            return addresses;
        }
    }

    private static void assertModifications(Transformed transformed, MetadataKey... expected)
    {
        assertEquals(expected.length, transformed.modifiedKeys.size());
        for (MetadataKey key : expected)
            assertTrue(transformed.modifiedKeys.contains(key));

        // anything modified by in this transformation, and therefore included in the modified keys,
        // should have the same epoch as the CM itself. Anything not modified now must have a strictly
        // earlier epoch
        for (MetadataKey key : Iterables.concat(MetadataKeys.CORE_METADATA, transformed.metadata.extensions.keySet()))
        {
            MetadataValue<?> value = valueFor(key, transformed.metadata);
            if (transformed.modifiedKeys.contains(key))
                assertEquals(transformed.metadata.epoch, value.lastModified());
            else
                assertTrue(transformed.metadata.epoch.isAfter(value.lastModified()));
        }
    }

    private static MetadataValue<?> valueFor(MetadataKey key, ClusterMetadata metadata)
    {
        if (!MetadataKeys.CORE_METADATA.contains(key))
        {
            assert key instanceof ExtensionKey<?,?>;
            return metadata.extensions.get((ExtensionKey<?, ?>)key);
        }

        if (key == SCHEMA)
            return metadata.schema;
        else if (key == NODE_DIRECTORY)
            return metadata.directory;
        else if (key == TOKEN_MAP)
            return metadata.tokenMap;
        else if (key == DATA_PLACEMENTS)
            return metadata.placements;
        else if (key == LOCKED_RANGES)
            return metadata.lockedRanges;
        else if (key == IN_PROGRESS_SEQUENCES)
            return metadata.inProgressSequences;

        throw new IllegalArgumentException("Unknown metadata key " + key);
    }
}
