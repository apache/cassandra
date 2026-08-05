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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.serializers.SerializationUtils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ComponentManifestTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testSerialization()
    {
        ComponentManifest expected = new ComponentManifest(new LinkedHashMap<Component, Long>() {{ put(Components.DATA, 100L); }});
        SerializationUtils.assertSerializationCycle(expected, ComponentManifest.serializers.get(BigFormat.getInstance().name()));
    }

    // ----------------------------------------------------------------------------------------------------
    // ComponentManifest.ordered(Descriptor, Map)
    //
    // The order of a manifest is part of the wire contract of a partial (sliced) entire-sstable stream: the
    // sender writes the components in manifest order and the receiver reads them in manifest order, so a
    // manifest whose order depended on the caller's map iteration would put the two ends out of step.
    // ----------------------------------------------------------------------------------------------------

    /**
     * The output order is the format's own {@code allComponents()} order filtered by {@code type.streamable},
     * whatever order the sizes came in.
     */
    @Test
    public void testOrderedUsesTheFormatsComponentOrder() throws IOException
    {
        Descriptor descriptor = descriptorFor(BigFormat.getInstance());
        List<Component> expected = streamableComponentsOf(descriptor);

        // Sanity: the format really does declare a non-streamable component, so the filter below is not a no-op.
        assertTrue(descriptor.getFormat().allComponents().contains(Components.TOC));
        assertFalse(expected.contains(Components.TOC));

        // Feed the sizes in the exact reverse of the format's order. Nothing about the answer may depend on it.
        List<Component> reversed = new ArrayList<>(expected);
        Collections.reverse(reversed);
        LinkedHashMap<Component, Long> sizes = new LinkedHashMap<>();
        long size = 1;
        for (Component component : reversed)
            sizes.put(component, size++);

        ComponentManifest manifest = ComponentManifest.ordered(descriptor, sizes);

        assertEquals(expected, manifest.components());
        // components() and the Iterable must agree; both are walked by the streaming code
        assertEquals(expected, ImmutableList.copyOf(manifest));
        for (Component component : expected)
            assertEquals((long) sizes.get(component), manifest.sizeOf(component));
        assertEquals((long) expected.size() * (expected.size() + 1) / 2, manifest.totalSize());
    }

    /** Two maps with the same entries in different iteration orders must produce the same manifest. */
    @Test
    public void testOrderedIsIndependentOfTheInputIterationOrder() throws IOException
    {
        Descriptor descriptor = descriptorFor(BigFormat.getInstance());

        LinkedHashMap<Component, Long> oneWay = new LinkedHashMap<>();
        oneWay.put(BigFormat.Components.PRIMARY_INDEX, 2L);
        oneWay.put(Components.STATS, 3L);
        oneWay.put(Components.DATA, 1L);

        LinkedHashMap<Component, Long> otherWay = new LinkedHashMap<>();
        otherWay.put(Components.STATS, 3L);
        otherWay.put(Components.DATA, 1L);
        otherWay.put(BigFormat.Components.PRIMARY_INDEX, 2L);

        ComponentManifest first = ComponentManifest.ordered(descriptor, oneWay);
        ComponentManifest second = ComponentManifest.ordered(descriptor, otherWay);

        assertEquals(ImmutableList.of(Components.DATA, BigFormat.Components.PRIMARY_INDEX, Components.STATS),
                     first.components());
        assertEquals(first.components(), second.components());
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    /**
     * The manifest of a partial (sliced) stream, which is the whole of what the receiver is driven by: the
     * components {@link ZeroCopySSTableSlice} synthesises plus Data.db, and NOTHING else -- Digest.crc32 in
     * particular. The sender cannot digest a Data.db that leaves by {@code sendfile} without entering its process,
     * and a digest computed by the receiver from the bytes that arrived cannot tell them apart from the bytes that
     * were sent, so there is none: {@code SSTableZeroCopyWriter} writes exactly the components named here, and the
     * verifier answers the absence with an extended verification.
     * <p>
     * Data.db first is also load-bearing rather than incidental. It is the one component of a slice that is not a
     * file of its own but byte ranges of the parent's, so it is the one whose position in this list decides where in
     * the stream the {@code sendfile} ranges land.
     */
    @Test
    public void testOrderedManifestOfASliceNamesNoDigest() throws IOException
    {
        Descriptor descriptor = descriptorFor(BigFormat.getInstance());

        LinkedHashMap<Component, Long> sizes = new LinkedHashMap<>();
        long size = 1;
        for (Component component : ZeroCopySSTableSlice.componentsFor(descriptor.getFormat(), true))
            sizes.put(component, size++);
        sizes.put(Components.DATA, size);

        ComponentManifest manifest = ComponentManifest.ordered(descriptor, sizes);

        assertEquals(Components.DATA, manifest.components().get(0));
        assertFalse("a slice has no digest to send, and the receiver must not invent one",
                    manifest.components().contains(Components.DIGEST));
        assertEquals(sizes.size(), manifest.components().size());
        assertTrue(manifest.components().containsAll(sizes.keySet()));
    }

    /** TOC.txt is not streamable, so asking for it is a programming error rather than a silently dropped file. */
    @Test
    public void testOrderedRejectsANonStreamableComponent() throws IOException
    {
        Descriptor descriptor = descriptorFor(BigFormat.getInstance());
        LinkedHashMap<Component, Long> sizes = new LinkedHashMap<>();
        sizes.put(Components.DATA, 1L);
        sizes.put(Components.TOC, 2L);

        assertThatThrownBy(() -> ComponentManifest.ordered(descriptor, sizes))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot stream components")
            .hasMessageContaining(Components.TOC.name)
            .hasMessageContaining(BigFormat.NAME);
    }

    /**
     * Streamability alone is not the test: Index.db is streamable, but it is not a component of a BTI sstable,
     * so a manifest for a BTI descriptor must refuse it rather than drop it.
     */
    @Test
    public void testOrderedRejectsAComponentOfAnotherFormat() throws IOException
    {
        SSTableFormat<?, ?> bti = DatabaseDescriptor.getSSTableFormats().get(BtiFormat.NAME);
        Descriptor descriptor = descriptorFor(bti);
        assertTrue(BigFormat.Components.PRIMARY_INDEX.type.streamable);
        assertFalse(bti.allComponents().contains(BigFormat.Components.PRIMARY_INDEX));

        LinkedHashMap<Component, Long> sizes = new LinkedHashMap<>();
        sizes.put(Components.DATA, 1L);
        sizes.put(BigFormat.Components.PRIMARY_INDEX, 2L);

        assertThatThrownBy(() -> ComponentManifest.ordered(descriptor, sizes))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(BigFormat.Components.PRIMARY_INDEX.name)
            .hasMessageContaining(BtiFormat.NAME);
    }

    private static List<Component> streamableComponentsOf(Descriptor descriptor)
    {
        List<Component> components = new ArrayList<>();
        for (Component component : descriptor.getFormat().allComponents())
            if (component.type.streamable)
                components.add(component);
        return components;
    }

    private static Descriptor descriptorFor(SSTableFormat<?, ?> format) throws IOException
    {
        File directory = new File(Files.createTempDirectory("componentManifestTest"));
        return new Descriptor(format.getLatestVersion(),
                              directory,
                              "ks",
                              "tbl",
                              SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
    }

    // Propose removing this test which now fails on VIntOutOfRange
    // We don't safely check if the bytes are bad so I don't understand what is being tested
    // There is no checksum
//    @Test(expected = EOFException.class)
//    public void testSerialization_FailsOnBadBytes() throws IOException
//    {
//        ByteBuffer buf = ByteBuffer.allocate(512);
//        ComponentManifest expected = new ComponentManifest(new LinkedHashMap<Component, Long>() {{ put(Components.DATA, 100L); }});
//
//        DataOutputBufferFixed out = new DataOutputBufferFixed(buf);
//
//        ComponentManifest.serializer.serialize(expected, out, MessagingService.VERSION_40);
//
//        buf.putInt(0, -100);
//
//        DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
//        ComponentManifest actual = ComponentManifest.serializer.deserialize(in, MessagingService.VERSION_40);
//        assertNotEquals(expected, actual);
//    }
}
