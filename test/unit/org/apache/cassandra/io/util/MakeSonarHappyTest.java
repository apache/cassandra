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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.ICompressor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * this is a silly test to make sonar happy about coverage
 */
public class MakeSonarHappyTest
{
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void nonCachingRebufferersDontDoNothingOnInvalideIfCached() throws IOException
    {
        java.io.File tmpFile = tmp.newFile();
        ChannelProxy channel = new ChannelProxy(new File(tmpFile));

        EmptyRebufferer er = new EmptyRebufferer(channel);
        er.invalidateIfCached(123);

        MmapRebufferer mr = new MmapRebufferer(channel, 0, null);
        mr.invalidateIfCached(123);

        SimpleChunkReader sr = new SimpleChunkReader(channel, 0, null, 0);
        sr.invalidateIfCached(123);

        ICompressor compressor = mock(ICompressor.class);
        when(compressor.preferredBufferType()).thenReturn(BufferType.ON_HEAP);

        CompressionMetadata metadata = mock(CompressionMetadata.class);
        when(metadata.chunkLength()).thenReturn(1024);
        when(metadata.compressor()).thenReturn(compressor);

        CompressedChunkReader.Standard ccsr = new CompressedChunkReader.Standard(channel, metadata);
        ccsr.invalidateIfCached(123);

        CompressedChunkReader.Mmap ccmr = new CompressedChunkReader.Mmap(channel, metadata, null);
        ccmr.invalidateIfCached(123);
    }
}