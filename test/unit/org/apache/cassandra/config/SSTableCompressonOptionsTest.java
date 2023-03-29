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

package org.apache.cassandra.config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.NoopCompressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.schema.CompressionParams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SSTableCompressonOptionsTest
{
    private SSTableCompressionOptions options = new SSTableCompressionOptions();
    private CompressionParams params;

    private void assertParams(boolean enabled, int chunk_length, Class<?> compressor)
    {
        assertThat(enabled).isEqualTo(params.isEnabled());
        assertThat(chunk_length).isEqualTo(params.chunkLength());
        if (compressor != null)
        {
            assertThat(params.getSstableCompressor()).isInstanceOf(compressor);
        } else
        {
            assertThat(params.getSstableCompressor()).isNull();
        }
    }

    @Test
    public void defaultTest()
    {
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, LZ4Compressor.class);
    }

    @Test
    public void lz4Test() {
        options.type = SSTableCompressionOptions.CompressorType.lz4;
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, LZ4Compressor.class);

        options.chunk_length = "5MiB";
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, LZ4Compressor.class);

        options.min_compress_ratio=0.5;
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, LZ4Compressor.class);
        Map<String,String> map = params.asMap();
        assertEquals("0.5", map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void noneTest() {
        options.type = SSTableCompressionOptions.CompressorType.none;
        params = options.getCompressionParams();
        // none is noever enabled.
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);

        options.chunk_length = "5MiB";
        params = options.getCompressionParams();
        // none does not set chunk length
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);

        options.min_compress_ratio=0.5;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
        Map<String,String> map = params.asMap();
        assertNull( map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);

    }

    @Test
    public void noopTest() {
        options.type = SSTableCompressionOptions.CompressorType.noop;
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, NoopCompressor.class);

        options.chunk_length = "5MiB";
        params = options.getCompressionParams();
        // noop does not set chunk length
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, NoopCompressor.class);

        options.min_compress_ratio=0.5;
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, NoopCompressor.class);
        Map<String,String> map = params.asMap();
        assertNull( map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void snappyTest() {
        options.type = SSTableCompressionOptions.CompressorType.snappy;
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, SnappyCompressor.class);

        options.chunk_length = "5MiB";
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, SnappyCompressor.class);

        options.min_compress_ratio=0.5;
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, SnappyCompressor.class);
        Map<String,String> map = params.asMap();
        assertEquals("0.5", map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void deflateTest() {
        options.type = SSTableCompressionOptions.CompressorType.deflate;
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, DeflateCompressor.class);

        options.chunk_length = "5MiB";
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, DeflateCompressor.class);

        options.min_compress_ratio=0.5;
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, DeflateCompressor.class);
        Map<String,String> map = params.asMap();
        assertNull( map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void zstdTest() {
        options.type = SSTableCompressionOptions.CompressorType.zstd;
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, ZstdCompressor.class);

        options.chunk_length = "5MiB";
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, ZstdCompressor.class);

        options.min_compress_ratio=0.5;
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, ZstdCompressor.class);
        Map<String,String> map = params.asMap();
        assertNull(map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void customTest() {
        options.type = SSTableCompressionOptions.CompressorType.custom;
        options.compressor = new ParameterizedClass(TestCompressor.class.getName(), Collections.emptyMap());
        params = options.getCompressionParams();
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, TestCompressor.class);

        options.chunk_length = "5MiB";
        params = options.getCompressionParams();
        assertParams(true, 5 * 1024, TestCompressor.class);

        options.enabled = false;
        params = options.getCompressionParams();
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
        options.enabled = true;

        options.compressor = new ParameterizedClass("", Collections.emptyMap());
        assertThatThrownBy(() -> options.getCompressionParams()).isInstanceOf(ConfigurationException.class)
          .hasMessageContaining("Missing or empty sub-option 'class'");

        options.compressor = new ParameterizedClass(null, Collections.emptyMap());
        assertThatThrownBy(() -> options.getCompressionParams()).isInstanceOf(ConfigurationException.class)
                                                                .hasMessageContaining("Missing or empty sub-option 'class'");

        options.compressor = null;
        assertThatThrownBy(() -> options.getCompressionParams()).isInstanceOf(ConfigurationException.class)
                                                                .hasMessageContaining("Missing sub-option 'compressor'");
    }

    public static class TestCompressor implements ICompressor
    {

        Map<String,String> options;

        static public TestCompressor create(Map<String,String> options)
        {
            return new TestCompressor(options);
        }

        private TestCompressor(Map<String,String> options)
        {
            this.options = options;
        }

        @Override
        public int initialCompressedBufferLength(int chunkLength)
        {
            return 0;
        }

        @Override
        public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
        {
            return 0;
        }

        @Override
        public void compress(ByteBuffer input, ByteBuffer output) throws IOException
        {

        }

        @Override
        public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
        {

        }

        @Override
        public BufferType preferredBufferType()
        {
            return null;
        }

        @Override
        public boolean supports(BufferType bufferType)
        {
            return false;
        }

        @Override
        public Set<String> supportedOptions()
        {
            return options.keySet();
        }
    }
}
