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
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.SSTableCompressionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.NoopCompressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CompressonParamsTest
{
    private SSTableCompressionOptions options;
    private CompressionParams params;

    @Before
    public void resetOptions() {
        options = new SSTableCompressionOptions();
    }

    @Test
    public void chunkLengthTest()
    {
        options.chunk_length = "";
        params = CompressionParams.fromOptions(options);
        assertEquals(CompressionParams.DEFAULT_CHUNK_LENGTH, params.chunkLength());

        options.chunk_length = "1MiB";
        params = CompressionParams.fromOptions(options);
        assertEquals(1024, params.chunkLength());

        options.chunk_length = "badvalue";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromOptions(options))
                                                               .withMessage("Invalid 'chunk_length' value for the 'sstable_compressor' option.");
    }

    @Test
    public void minCompressRatioTest()
    {
        // use snappy because it uses min_compress_ratio
        options.class_name = "snappy";
        options.min_compress_ratio = null;
        params = CompressionParams.fromOptions(options);
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());

        options.min_compress_ratio = CompressionParams.DEFAULT_MIN_COMPRESS_RATIO;
        params = CompressionParams.fromOptions(options);
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());

        options.min_compress_ratio = 0.3;
        params = CompressionParams.fromOptions(options);
        assertEquals(0.3, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals( (int) Math.ceil(CompressionParams.DEFAULT_CHUNK_LENGTH / 0.3), params.maxCompressedLength());

        options.min_compress_ratio = 1.3;
        params = CompressionParams.fromOptions(options);
        assertEquals(1.3, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals( (int) Math.ceil(CompressionParams.DEFAULT_CHUNK_LENGTH / 1.3), params.maxCompressedLength());

        options.min_compress_ratio = -1.0;
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromOptions(options))
                                                               .withMessage("'min_compress_ratio' may not be less than 0.0 for the 'sstable_compressor' option.");
    }

    @Test
    public void maxCompressedLengthTest()
    {
        // use lze because it uses max_compressed_length
        options.class_name = "snappy";
        options.max_compressed_length = null;
        params = CompressionParams.fromOptions(options);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);

        options.max_compressed_length = "";
        params = CompressionParams.fromOptions(options);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);

        options.max_compressed_length = "5GiB";
        params = CompressionParams.fromOptions(options);
        assertEquals(5 * 1024 * 1024, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_CHUNK_LENGTH / (5.0 * 1024 * 1024), params.minCompressRatio(), Double.MIN_VALUE);

        options.max_compressed_length = "badvalue";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromOptions(options))
                                                               .withMessage("Invalid 'max_compressed_length' value for the 'sstable_compressor' option.");
    }

    @Test
    public void maxCompressionLengthAndMinCompressRatioTest() {
        options.class_name = "snappy";
        options.min_compress_ratio = -1.0;
        options.max_compressed_length = "5Gib";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromOptions(options))
                                                               .withMessage("Can not specify both 'min_compress_ratio' and 'max_compressedlength' for the 'sstable_compressor' option.");
    }

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
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, LZ4Compressor.class);

        params = CompressionParams.fromOptions( null );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, LZ4Compressor.class);
    }

    @Test
    public void lz4Test() {
        options.class_name = SSTableCompressionOptions.CompressorType.lz4.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, LZ4Compressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, LZ4Compressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, LZ4Compressor.class);
        Map<String,String> map = params.asMap();
        assertEquals("0.5", map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void noneTest() {
        options.class_name = SSTableCompressionOptions.CompressorType.none.name();
        params = CompressionParams.fromOptions( options );
        // none is noever enabled.
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        // none does not set chunk length
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
        Map<String,String> map = params.asMap();
        assertNull( map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);

    }

    @Test
    public void noopTest() {
        options.class_name = SSTableCompressionOptions.CompressorType.noop.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, NoopCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        // noop does not set chunk length
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, NoopCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, NoopCompressor.class);
        Map<String,String> map = params.asMap();
        assertNull( map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void snappyTest() {
        options.class_name = SSTableCompressionOptions.CompressorType.snappy.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, SnappyCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, SnappyCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, SnappyCompressor.class);
        Map<String,String> map = params.asMap();
        assertEquals("0.5", map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void deflateTest() {
        options.class_name = SSTableCompressionOptions.CompressorType.deflate.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, DeflateCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, DeflateCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, DeflateCompressor.class);
        Map<String,String> map = params.asMap();
        assertNull( map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void zstdTest() {
        options.class_name = SSTableCompressionOptions.CompressorType.zstd.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, ZstdCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, ZstdCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, ZstdCompressor.class);
        Map<String,String> map = params.asMap();
        assertNull(map.get(CompressionParams.MIN_COMPRESS_RATIO));

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
    }

    @Test
    public void customTest()
    {
        options.class_name = TestCompressor.class.getName();
        params = CompressionParams.fromOptions(options);
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, TestCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions(options);
        assertParams(true, 5 * 1024, TestCompressor.class);

        options.enabled = false;
        params = CompressionParams.fromOptions(options);
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, null);
        options.enabled = true;

        options.class_name = "foo";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromOptions(options))
                                                               .withMessage("Could not create Compression for type org.apache.cassandra.io.compress.foo");
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
