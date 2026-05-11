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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionParamsTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testIsDictionaryCompressionEnabled()
    {
        CompressionParams noCompression = CompressionParams.noCompression();
        assertThat(noCompression.isDictionaryCompressionEnabled())
        .as("No compression should not enable dictionary compression")
        .isFalse();

        CompressionParams regularZstd = CompressionParams.zstd();
        assertThat(regularZstd.isDictionaryCompressionEnabled())
        .as("Regular Zstd compression should not enable dictionary compression")
        .isFalse();

        CompressionParams zstdDictionary = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true);
        assertThat(zstdDictionary.isDictionaryCompressionEnabled())
        .as("Zstd dictionary compression should enable dictionary compression")
        .isTrue();

        CompressionParams lz4 = CompressionParams.lz4();
        assertThat(lz4.isDictionaryCompressionEnabled())
        .as("LZ4 compression should not enable dictionary compression")
        .isFalse();

        CompressionParams snappy = CompressionParams.snappy();
        assertThat(snappy.isDictionaryCompressionEnabled())
        .as("Snappy compression should not enable dictionary compression")
        .isFalse();

        CompressionParams deflate = CompressionParams.deflate();
        assertThat(deflate.isDictionaryCompressionEnabled())
        .as("Deflate compression should not enable dictionary compression")
        .isFalse();

        CompressionParams noop = CompressionParams.noop();
        assertThat(noop.isDictionaryCompressionEnabled())
        .as("Noop compression should not enable dictionary compression")
        .isFalse();
    }

    @Test
    public void testNonBuiltInCompressorClassResolvesViaDefaultProvider()
    {
        // A 3rd-party / user-supplied compressor class is not in CompressorType, so the registry
        // has no entry for it. CompressionParams.createCompressor must fall back to the default
        // provider, which reflectively invokes the class's static create(Map) factory - otherwise
        // every existing pluggable compressor outside org.apache.cassandra.io.compress NPEs.
        Map<String, String> opts = ImmutableMap.of("class", CustomTestCompressor.class.getName(),
                                                   "chunk_length_in_kb", "8");
        CompressionParams params = CompressionParams.fromMap(opts);

        assertThat(params.getSstableCompressor()).isInstanceOf(CustomTestCompressor.class);
        assertThat(params.klass()).isEqualTo(CustomTestCompressor.class);
    }

    public static class CustomTestCompressor implements ICompressor
    {
        public static CustomTestCompressor create(Map<String, String> options)
        {
            return new CustomTestCompressor();
        }

        @Override
        public int initialCompressedBufferLength(int chunkLength)
        {
            return chunkLength;
        }

        @Override
        public int uncompress(byte[] in, int io, int il, byte[] out, int oo) throws IOException
        {
            return 0;
        }

        @Override
        public void compress(ByteBuffer in, ByteBuffer out) throws IOException
        {
        }

        @Override
        public void uncompress(ByteBuffer in, ByteBuffer out) throws IOException
        {
        }

        @Override
        public BufferType preferredBufferType()
        {
            return BufferType.OFF_HEAP;
        }

        @Override
        public boolean supports(BufferType bufferType)
        {
            return true;
        }

        @Override
        public Set<String> supportedOptions()
        {
            return Collections.emptySet();
        }
    }
}
