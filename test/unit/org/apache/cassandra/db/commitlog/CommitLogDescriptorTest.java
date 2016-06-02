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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitLogDescriptorTest
{
    @Test
    public void testVersions()
    {
        assertTrue(CommitLogDescriptor.isValid("CommitLog-1340512736956320000.log"));
        assertTrue(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000.log"));
        assertFalse(CommitLogDescriptor.isValid("CommitLog--1340512736956320000.log"));
        assertFalse(CommitLogDescriptor.isValid("CommitLog--2-1340512736956320000.log"));
        assertFalse(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000-123.log"));

        assertEquals(1340512736956320000L, CommitLogDescriptor.fromFileName("CommitLog-2-1340512736956320000.log").id);

        assertEquals(MessagingService.current_version, new CommitLogDescriptor(1340512736956320000L, null).getMessagingVersion());
        String newCLName = "CommitLog-" + CommitLogDescriptor.current_version + "-1340512736956320000.log";
        assertEquals(MessagingService.current_version, CommitLogDescriptor.fromFileName(newCLName).getMessagingVersion());
    }

    private void testDescriptorPersistence(CommitLogDescriptor desc) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc);
        // Put some extra data in the stream.
        buf.putDouble(0.1);
        buf.flip();

        try (DataInputBuffer input = new DataInputBuffer(buf, false))
        {
            CommitLogDescriptor read = CommitLogDescriptor.readHeader(input);
            assertEquals("Descriptors", desc, read);
        }
    }

    @Test
    public void testDescriptorPersistence() throws IOException
    {
        testDescriptorPersistence(new CommitLogDescriptor(11, null));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_21, 13, null));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_30, 15, null));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_30, 17, new ParameterizedClass("LZ4Compressor", null)));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_30, 19,
                new ParameterizedClass("StubbyCompressor", ImmutableMap.of("parameter1", "value1", "flag2", "55", "argument3", "null"))));
    }

    @Test
    public void testDescriptorInvalidParametersSize() throws IOException
    {
        final int numberOfParameters = 65535;
        Map<String, String> params = new HashMap<>(numberOfParameters);
        for (int i=0; i<numberOfParameters; ++i)
            params.put("key"+i, Integer.toString(i, 16));
        try {
            CommitLogDescriptor desc = new CommitLogDescriptor(CommitLogDescriptor.VERSION_30,
                                                               21,
                                                               new ParameterizedClass("LZ4Compressor", params));
            ByteBuffer buf = ByteBuffer.allocate(1024000);
            CommitLogDescriptor.writeHeader(buf, desc);
            fail("Parameter object too long should fail on writing descriptor.");
        } catch (ConfigurationException e)
        {
            // correct path
        }
    }
}
