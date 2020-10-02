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
package org.apache.cassandra.hints;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HintsDescriptorTest
{
    @Test
    public void testSerializerNormal() throws IOException
    {
        UUID hostId = UUID.randomUUID();
        int version = HintsDescriptor.CURRENT_VERSION;
        long timestamp = System.currentTimeMillis();
        ImmutableMap<String, Object> parameters =
                ImmutableMap.of("compression", (Object) ImmutableMap.of("class_name", LZ4Compressor.class.getName()));
        HintsDescriptor descriptor = new HintsDescriptor(hostId, version, timestamp, parameters);

        testSerializeDeserializeLoop(descriptor);
    }

    @Test
    public void testSerializerWithEmptyParameters() throws IOException
    {
        UUID hostId = UUID.randomUUID();
        int version = HintsDescriptor.CURRENT_VERSION;
        long timestamp = System.currentTimeMillis();
        ImmutableMap<String, Object> parameters = ImmutableMap.of();
        HintsDescriptor descriptor = new HintsDescriptor(hostId, version, timestamp, parameters);

        testSerializeDeserializeLoop(descriptor);
    }

    @Test
    public void testCorruptedDeserialize() throws IOException
    {
        UUID hostId = UUID.randomUUID();
        int version = HintsDescriptor.CURRENT_VERSION;
        long timestamp = System.currentTimeMillis();
        ImmutableMap<String, Object> parameters = ImmutableMap.of();
        HintsDescriptor descriptor = new HintsDescriptor(hostId, version, timestamp, parameters);

        byte[] bytes = serializeDescriptor(descriptor);

        // mess up the parameters size
        bytes[28] = (byte) 0xFF;
        bytes[29] = (byte) 0xFF;
        bytes[30] = (byte) 0xFF;
        bytes[31] = (byte) 0x7F;

        // attempt to deserialize
        try
        {
            deserializeDescriptor(bytes);
            fail("Deserializing the descriptor should but didn't");
        }
        catch (IOException e)
        {
            assertEquals("Hints Descriptor CRC Mismatch", e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("EmptyTryBlock")
    public void testReadFromFile() throws IOException
    {
        UUID hostId = UUID.randomUUID();
        int version = HintsDescriptor.CURRENT_VERSION;
        long timestamp = System.currentTimeMillis();
        ImmutableMap<String, Object> parameters = ImmutableMap.of();
        HintsDescriptor expected = new HintsDescriptor(hostId, version, timestamp, parameters);

        Path directory = Files.createTempDirectory("hints");
        try
        {
            try (HintsWriter ignored = HintsWriter.create(directory.toFile(), expected))
            {
            }
            HintsDescriptor actual = HintsDescriptor.readFromFile(directory.resolve(expected.fileName()));
            assertEquals(expected, actual);
        }
        finally
        {
            directory.toFile().deleteOnExit();
        }
    }

    @Test
    public void testHandleIOE() throws IOException
    {
        Path p = Files.createTempFile("testing", ".hints");
        // empty file;
        assertTrue(p.toFile().exists());
        Assert.assertEquals(0, Files.size(p));
        HintsDescriptor.handleDescriptorIOE(new IOException("test"), p);
        assertFalse(Files.exists(p));

        // non-empty
        p = Files.createTempFile("testing", ".hints");
        Files.write(p, Collections.singleton("hello"));
        HintsDescriptor.handleDescriptorIOE(new IOException("test"), p);
        File newFile = new File(p.getParent().toFile(), p.getFileName().toString().replace(".hints", ".corrupt.hints"));
        assertFalse(Files.exists(p));
        assertTrue(newFile.toString(), newFile.exists());
        newFile.deleteOnExit();
    }

    private static void testSerializeDeserializeLoop(HintsDescriptor descriptor) throws IOException
    {
        // serialize to a byte array
        byte[] bytes = serializeDescriptor(descriptor);
        // make sure the sizes match
        assertEquals(bytes.length, descriptor.serializedSize());
        // deserialize back
        HintsDescriptor deserializedDescriptor = deserializeDescriptor(bytes);
        // compare equality
        assertDescriptorsEqual(descriptor, deserializedDescriptor);
    }

    private static byte[] serializeDescriptor(HintsDescriptor descriptor) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        descriptor.serialize(dob);
        return dob.toByteArray();
    }

    private static HintsDescriptor deserializeDescriptor(byte[] bytes) throws IOException
    {
        DataInput in = ByteStreams.newDataInput(bytes);
        return HintsDescriptor.deserialize(in);
    }

    private static void assertDescriptorsEqual(HintsDescriptor expected, HintsDescriptor actual)
    {
        assertNotSame(expected, actual);
        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
        assertEquals(expected.hostId, actual.hostId);
        assertEquals(expected.version, actual.version);
        assertEquals(expected.timestamp, actual.timestamp);
        assertEquals(expected.parameters, actual.parameters);
    }
}
