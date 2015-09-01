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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;

public class CommitLogDescriptorTest
{
    private static final byte[] iv = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    ParameterizedClass compression;
    TransparentDataEncryptionOptions enabledTdeOptions;

    // Context with enabledTdeOptions enabled
    EncryptionContext enabledEncryption;

    // Context with enabledTdeOptions disabled, with the assumption that enabledTdeOptions was never previously enabled
    EncryptionContext neverEnabledEncryption;

    // Context with enabledTdeOptions disabled, with the assumption that enabledTdeOptions was previously enabled, but now disabled
    // due to operator changing the yaml.
    EncryptionContext previouslyEnabledEncryption;

    @Before
    public void setup()
    {
        Map<String,String> params = new HashMap<>();
        compression = new ParameterizedClass(LZ4Compressor.class.getName(), params);

        enabledTdeOptions = EncryptionContextGenerator.createEncryptionOptions();
        enabledEncryption = new EncryptionContext(enabledTdeOptions, iv, false);
        
        neverEnabledEncryption = EncryptionContextGenerator.createDisabledContext();
        TransparentDataEncryptionOptions disaabledTdeOptions = new TransparentDataEncryptionOptions(false, enabledTdeOptions.cipher, enabledTdeOptions.key_alias, enabledTdeOptions.key_provider);
        previouslyEnabledEncryption = new EncryptionContext(disaabledTdeOptions);
    }

    @Test
    public void testVersions()
    {
        Assert.assertTrue(CommitLogDescriptor.isValid("CommitLog-1340512736956320000.log"));
        Assert.assertTrue(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog--1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog--2-1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000-123.log"));

        Assert.assertEquals(1340512736956320000L, CommitLogDescriptor.fromFileName("CommitLog-2-1340512736956320000.log").id);

        Assert.assertEquals(MessagingService.current_version, new CommitLogDescriptor(1340512736956320000L, null, neverEnabledEncryption).getMessagingVersion());
        String newCLName = "CommitLog-" + CommitLogDescriptor.current_version + "-1340512736956320000.log";
        Assert.assertEquals(MessagingService.current_version, CommitLogDescriptor.fromFileName(newCLName).getMessagingVersion());
    }

    // migrated from CommitLogTest
    private void testDescriptorPersistence(CommitLogDescriptor desc) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc);
        long length = buf.position();
        // Put some extra data in the stream.
        buf.putDouble(0.1);
        buf.flip();
        FileDataInput input = new FileSegmentInputStream(buf, "input", 0);
        CommitLogDescriptor read = CommitLogDescriptor.readHeader(input, neverEnabledEncryption);
        Assert.assertEquals("Descriptor length", length, input.getFilePointer());
        Assert.assertEquals("Descriptors", desc, read);
    }

    // migrated from CommitLogTest
    @Test
    public void testDescriptorPersistence() throws IOException
    {
        testDescriptorPersistence(new CommitLogDescriptor(11, null, neverEnabledEncryption));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_21, 13, null, neverEnabledEncryption));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_22, 15, null, neverEnabledEncryption));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_22, 17, new ParameterizedClass("LZ4Compressor", null), neverEnabledEncryption));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.VERSION_22, 19,
                                                          new ParameterizedClass("StubbyCompressor", ImmutableMap.of("parameter1", "value1", "flag2", "55", "argument3", "null")
                                                          ), neverEnabledEncryption));
    }

    // migrated from CommitLogTest
    @Test
    public void testDescriptorInvalidParametersSize() throws IOException
    {
        Map<String, String> params = new HashMap<>();
        for (int i=0; i<65535; ++i)
            params.put("key"+i, Integer.toString(i, 16));
        try {
            CommitLogDescriptor desc = new CommitLogDescriptor(CommitLogDescriptor.VERSION_22,
                                                               21,
                                                               new ParameterizedClass("LZ4Compressor", params),
                                                               neverEnabledEncryption);
            ByteBuffer buf = ByteBuffer.allocate(1024000);
            CommitLogDescriptor.writeHeader(buf, desc);
            Assert.fail("Parameter object too long should fail on writing descriptor.");
        } catch (ConfigurationException e)
        {
            // correct path
        }
    }

    @Test
    public void constructParametersString_NoCompressionOrEncryption()
    {
        String json = CommitLogDescriptor.constructParametersString(null, null, Collections.emptyMap());
        Assert.assertFalse(json.contains(CommitLogDescriptor.COMPRESSION_CLASS_KEY));
        Assert.assertFalse(json.contains(EncryptionContext.ENCRYPTION_CIPHER));

        json = CommitLogDescriptor.constructParametersString(null, neverEnabledEncryption, Collections.emptyMap());
        Assert.assertFalse(json.contains(CommitLogDescriptor.COMPRESSION_CLASS_KEY));
        Assert.assertFalse(json.contains(EncryptionContext.ENCRYPTION_CIPHER));
    }

    @Test
    public void constructParametersString_WithCompressionAndEncryption()
    {
        String json = CommitLogDescriptor.constructParametersString(compression, enabledEncryption, Collections.emptyMap());
        Assert.assertTrue(json.contains(CommitLogDescriptor.COMPRESSION_CLASS_KEY));
        Assert.assertTrue(json.contains(EncryptionContext.ENCRYPTION_CIPHER));
    }

    @Test
    public void writeAndReadHeader_NoCompressionOrEncryption() throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, neverEnabledEncryption);
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.flip();
        FileSegmentInputStream dataInput = new FileSegmentInputStream(buffer, null, 0);
        CommitLogDescriptor result = CommitLogDescriptor.readHeader(dataInput, neverEnabledEncryption);
        Assert.assertNotNull(result);
        Assert.assertNull(result.compression);
        Assert.assertFalse(result.getEncryptionContext().isEnabled());
    }

    @Test
    public void writeAndReadHeader_OnlyCompression() throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, neverEnabledEncryption);
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.flip();
        FileSegmentInputStream dataInput = new FileSegmentInputStream(buffer, null, 0);
        CommitLogDescriptor result = CommitLogDescriptor.readHeader(dataInput, neverEnabledEncryption);
        Assert.assertNotNull(result);
        Assert.assertEquals(compression, result.compression);
        Assert.assertFalse(result.getEncryptionContext().isEnabled());
    }

    @Test
    public void writeAndReadHeader_WithEncryptionHeader_EncryptionEnabledInYaml() throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, enabledEncryption);
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.flip();
        FileSegmentInputStream dataInput = new FileSegmentInputStream(buffer, null, 0);
        CommitLogDescriptor result = CommitLogDescriptor.readHeader(dataInput, enabledEncryption);
        Assert.assertNotNull(result);
        Assert.assertNull(result.compression);
        Assert.assertTrue(result.getEncryptionContext().isEnabled());
        Assert.assertArrayEquals(iv, result.getEncryptionContext().getIV());
    }

    /**
     * Check that even though enabledTdeOptions is disabled in the yaml, we can still read the commit log header as encrypted.
     */
    @Test
    public void writeAndReadHeader_WithEncryptionHeader_EncryptionDisabledInYaml() throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, enabledEncryption);
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.flip();
        FileSegmentInputStream dataInput = new FileSegmentInputStream(buffer, null, 0);
        CommitLogDescriptor result = CommitLogDescriptor.readHeader(dataInput, previouslyEnabledEncryption);
        Assert.assertNotNull(result);
        Assert.assertNull(result.compression);
        Assert.assertTrue(result.getEncryptionContext().isEnabled());
        Assert.assertArrayEquals(iv, result.getEncryptionContext().getIV());
    }

    /**
     * Shouldn't happen in the real world (should only have either compression or enabledTdeOptions), but the header
     * functionality should be correct
     */
    @Test
    public void writeAndReadHeader_WithCompressionAndEncryption() throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, enabledEncryption);
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.flip();
        FileSegmentInputStream dataInput = new FileSegmentInputStream(buffer, null, 0);
        CommitLogDescriptor result = CommitLogDescriptor.readHeader(dataInput, enabledEncryption);
        Assert.assertNotNull(result);
        Assert.assertEquals(compression, result.compression);
        Assert.assertTrue(result.getEncryptionContext().isEnabled());
        Assert.assertEquals(enabledEncryption, result.getEncryptionContext());
        Assert.assertArrayEquals(iv, result.getEncryptionContext().getIV());
    }

    @Test
    public void equals_NoCompressionOrEncryption()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, null);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, null);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, neverEnabledEncryption);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, neverEnabledEncryption);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, previouslyEnabledEncryption);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, previouslyEnabledEncryption);
        Assert.assertEquals(desc1, desc2);
    }

    @Test
    public void equals_OnlyCompression()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, null);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, null);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, neverEnabledEncryption);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, neverEnabledEncryption);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, previouslyEnabledEncryption);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, previouslyEnabledEncryption);
        Assert.assertEquals(desc1, desc2);
    }

    @Test
    public void equals_OnlyEncryption()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, enabledEncryption);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, enabledEncryption);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, neverEnabledEncryption);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, neverEnabledEncryption);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, previouslyEnabledEncryption);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null, previouslyEnabledEncryption);
        Assert.assertEquals(desc1, desc2);
    }

    /**
     * Shouldn't have both enabled in real life, but ensure they are correct, nonetheless
     */
    @Test
    public void equals_BothCompressionAndEncryption()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, enabledEncryption);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression, enabledEncryption);
        Assert.assertEquals(desc1, desc2);
    }

}
