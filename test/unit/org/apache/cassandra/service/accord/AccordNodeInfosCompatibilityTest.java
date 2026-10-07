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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.Status;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21687 replaced {@code AccordFastPath} and {@code AccordStaleReplicas} with {@link AccordNodeInfos}, without
 * changing the {@link ClusterMetadata} serialization {@link Version}. This verifies we can still read metadata written by
 * the prior version, and that what we write retains the legacy layout. Stale and hard removed replicas are deliberately
 * not migrated across this change, but the legacy {@code AccordStaleReplicas} must still be consumed in its entirety.
 * <p>
 * {@code accord_fast_path_and_stale_replicas_05fecbc54e6.bin} was produced by a build of 05fecbc54e6 (the parent of
 * 4da20e07e8c, which introduced {@link AccordNodeInfos}) by serializing, with
 * {@code VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, out, Version.V10)}, the metadata:
 * <pre>
 *   metadata = new ClusterMetadata(Murmur3Partitioner.instance);
 *   // register nodes 1..6 as 127.0.0.{i}:7012, dc1/rack{i}, host id new UUID(0, i), 6.0-alpha3-SNAPSHOT/V10     (epoch 1)
 *   .withFastPathStatusSince(2, SHUTDOWN, 1000, 0)
 *   .withFastPathStatusSince(3, UNAVAILABLE, 2000, 0)
 *   .withFastPathStatusSince(4, UNAVAILABLE, 3000, 0)                                                            (epoch 2)
 *   .withFastPathStatusSince(4, NORMAL, 5000, 0)                                                                 (epoch 3)
 *   .markStaleReplicas([3, 5])                                                                                   (epoch 4)
 *   .unregister(6)                                                                                               (epoch 5)
 *   .markHardRemovedReplicas([6])                                                                                (epoch 6)
 * </pre>
 * {@code accord_stale_replicas_05fecbc54e6.bin} and {@code accord_fast_path_05fecbc54e6.bin} are the standalone
 * {@code AccordStaleReplicas.serializer} and {@code AccordFastPath.serializer} encodings of the same metadata's
 * {@code accordStaleReplicas} and {@code accordFastPath}, and {@code accord_stale_replicas_empty_05fecbc54e6.bin} is
 * the {@code AccordStaleReplicas.serializer} encoding of {@code AccordStaleReplicas.EMPTY}, all at {@code Version.V10}.
 *
 * @author LLM
 */
public class AccordNodeInfosCompatibilityTest
{
    private static final String LEGACY_RESOURCE = "cluster_metadata/accord_fast_path_and_stale_replicas_05fecbc54e6.bin";
    private static final String LEGACY_STALE_REPLICAS_RESOURCE = "cluster_metadata/accord_stale_replicas_05fecbc54e6.bin";
    private static final String LEGACY_FAST_PATH_RESOURCE = "cluster_metadata/accord_fast_path_05fecbc54e6.bin";
    private static final String LEGACY_EMPTY_STALE_REPLICAS_RESOURCE = "cluster_metadata/accord_stale_replicas_empty_05fecbc54e6.bin";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static Node.Id id(int id)
    {
        return new Node.Id(id);
    }

    private static ByteBuffer bytes(String resource) throws IOException
    {
        try (InputStream in = AccordNodeInfosCompatibilityTest.class.getClassLoader().getResourceAsStream(resource))
        {
            assertNotNull("Missing resource " + resource, in);
            return ByteBuffer.wrap(in.readAllBytes());
        }
    }

    private static ClusterMetadata deserialize(ByteBuffer bytes) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, false))
        {
            ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
            assertEquals("Trailing bytes after deserializing ClusterMetadata", 0, in.available());
            return metadata;
        }
    }

    private static ByteBuffer serialize(ClusterMetadata metadata, Version version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, out, version);
            assertEquals(VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, metadata, version), out.getLength());
            return out.asNewBuffer();
        }
    }

    @Test
    public void readLegacyFastPathAndStaleReplicas() throws IOException
    {
        ClusterMetadata metadata = deserialize(bytes(LEGACY_RESOURCE));

        // the fields serialized after AccordStaleReplicas must be intact
        assertEquals(Epoch.create(6), metadata.epoch);
        assertEquals(5, metadata.directory.peerIds().size());
        assertFalse(metadata.directory.peerIds().contains(new NodeId(6)));
        assertTrue(metadata.lockedRanges.locked.isEmpty());
        assertTrue(metadata.extensions.isEmpty());

        AccordNodeInfos infos = metadata.accordNodeInfos;
        assertEquals(Epoch.create(3), infos.lastModified()); // AccordFastPath.lastModified

        // migrated from AccordFastPath
        assertNull(infos.get(id(1)));
        assertEquals(Status.NORMAL, infos.status(id(1)));
        assertEquals(Status.SHUTDOWN, infos.status(id(2)));
        assertEquals(1000, infos.getOrDefault(id(2)).updated);
        assertEquals(Status.MAYBE_DOWN, infos.status(id(3))); // legacy UNAVAILABLE
        assertEquals(2000, infos.getOrDefault(id(3)).updated);
        assertEquals(Status.NORMAL, infos.status(id(4)));
        assertEquals(5000, infos.getOrDefault(id(4)).updated);
        assertEquals(SortedArrayList.ofSorted(id(2), id(3)), infos.excludedFromFastQuorum());

        // AccordStaleReplicas (stale [3, 5], hard removed [6]) is consumed but deliberately not migrated
        assertTrue(infos.stale().isEmpty());
        assertTrue(infos.hardRemoved().isEmpty());
        assertFalse(infos.get(id(3)).isStale());
        assertNull(infos.get(id(5)));
        assertNull(infos.get(id(6)));
    }

    @Test
    public void roundTripLegacy() throws IOException
    {
        ClusterMetadata legacy = deserialize(bytes(LEGACY_RESOURCE));
        for (Version version : Version.MIN_ACCORD_VERSION.greaterThanOrEqual())
        {
            if (version.isAfter(NodeVersion.CURRENT_METADATA_VERSION))
                continue;

            ClusterMetadata roundTrip = deserialize(serialize(legacy, version));
            assertEquals(legacy.accordNodeInfos, roundTrip.accordNodeInfos);
            assertEquals(legacy.lockedRanges, roundTrip.lockedRanges);
            assertEquals(legacy.inProgressSequences, roundTrip.inProgressSequences);
        }
    }

    /**
     * The section previously occupied by AccordStaleReplicas must retain its layout: we must fully consume any written by
     * the prior version, and must write an empty one that is byte-identical to the prior version's, so that peers still
     * running the prior version can consume it.
     */
    @Test
    public void legacyStaleReplicasLayout() throws IOException
    {
        // read non-empty and empty legacy encodings
        for (String resource : new String[] { LEGACY_STALE_REPLICAS_RESOURCE, LEGACY_EMPTY_STALE_REPLICAS_RESOURCE })
        {
            try (DataInputBuffer in = new DataInputBuffer(bytes(resource), false))
            {
                AccordNodeInfos.LegacyStaleReplicas.skip(in, Version.V10);
                assertEquals(resource, 0, in.available());
            }
        }

        // write
        for (Version version : Version.MIN_ACCORD_VERSION.greaterThanOrEqual())
        {
            if (version.isAfter(NodeVersion.CURRENT_METADATA_VERSION))
                continue;

            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                AccordNodeInfos.LegacyStaleReplicas.serializeEmpty(out, version);
                assertEquals(AccordNodeInfos.LegacyStaleReplicas.serializedEmptySize(version), out.getLength());
                assertEquals(bytes(LEGACY_EMPTY_STALE_REPLICAS_RESOURCE), out.asNewBuffer());
            }
        }
    }

    /**
     * AccordNodeInfos.serializer must read a standalone AccordFastPath as written by the prior version
     */
    @Test
    public void legacyFastPath() throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes(LEGACY_FAST_PATH_RESOURCE), false))
        {
            AccordNodeInfos infos = AccordNodeInfos.serializer.deserialize(in, Version.V10);
            assertEquals(0, in.available());
            assertEquals(Epoch.create(3), infos.lastModified());
            assertNull(infos.get(id(1)));
            assertEquals(Status.SHUTDOWN, infos.status(id(2)));
            assertEquals(1000, infos.getOrDefault(id(2)).updated);
            assertEquals(Status.MAYBE_DOWN, infos.status(id(3)));
            assertEquals(2000, infos.getOrDefault(id(3)).updated);
            assertEquals(Status.NORMAL, infos.status(id(4)));
            assertEquals(5000, infos.getOrDefault(id(4)).updated);
            assertTrue(infos.stale().isEmpty());
            assertTrue(infos.hardRemoved().isEmpty());
        }
    }
}
