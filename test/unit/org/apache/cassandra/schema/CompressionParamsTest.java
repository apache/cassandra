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
import java.util.HashMap;
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

public class CompressionParamsTest
{
    private SSTableCompressionOptions options;
    private CompressionParams params;

    @Before
    public void resetOptions() {
        options = new SSTableCompressionOptions();
    }

    @Test
    public void additionalParamsTest() {
        assertThat( options.parameters).isNull();
        params = CompressionParams.fromOptions(options);
        assertThat( params.getOtherOptions()).isNotNull();
        assertThat( params.getOtherOptions().isEmpty()).isTrue();

        options.parameters = new HashMap<>();
        params = CompressionParams.fromOptions(options);
        assertThat( params.getOtherOptions()).isNotNull();
        assertThat( params.getOtherOptions().isEmpty()).isTrue();

        options.parameters.put( "foo", "bar");
        params = CompressionParams.fromOptions(options);
        params = CompressionParams.fromOptions(options);
        assertThat( params.getOtherOptions()).isNotNull();
        assertThat( params.getOtherOptions().get("foo")).isEqualTo("bar");
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
        // pick a compressor that uses standard default options.
        options.class_name = "none";
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
        // pick a compressor that uses standard default options.
        options.class_name = "none";
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

    private void assertParams(boolean enabled, int chunkLength, int maxCompressedLength, double minCompressRatio, Class<?> compressor)
    {
        assertThat(params.isEnabled()).isEqualTo(enabled);
        assertThat(params.chunkLength()).isEqualTo(chunkLength);
        assertThat(params.maxCompressedLength()).isEqualTo(maxCompressedLength);
        assertThat(params.minCompressRatio()).isEqualTo(minCompressRatio);
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
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, CompressionParams.DEFAULT_CHUNK_LENGTH, CompressionParams.CompressorType.lz4.minRatio, LZ4Compressor.class);

        params = CompressionParams.fromOptions( null );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, LZ4Compressor.class);
    }

    @Test
    public void lz4Test() {
        options.class_name = CompressionParams.CompressorType.lz4.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, CompressionParams.DEFAULT_CHUNK_LENGTH, CompressionParams.CompressorType.lz4.minRatio, LZ4Compressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, 5 * 1024, CompressionParams.CompressorType.lz4.minRatio, LZ4Compressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5 * 1024, 10240, 0.5, LZ4Compressor.class);

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, 5 * 1024, 10240, 0.5, null);
    }

    @Test
    public void noneTest() {
        options.class_name = CompressionParams.CompressorType.none.name();
        params = CompressionParams.fromOptions( options );
        // none is never enabled.
        assertParams(false, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.CompressorType.none.minRatio, null);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        // none does not set chunk length
        assertParams(false, 5 * 1024, Integer.MAX_VALUE, CompressionParams.CompressorType.none.minRatio, null);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(false, 5*1024, 10240, 0.5, null);

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, 5*1024, 10240, 0.5, null);

    }

    @Test
    public void noopTest() {
        options.class_name = CompressionParams.CompressorType.noop.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.CompressorType.noop.minRatio, NoopCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, Integer.MAX_VALUE, CompressionParams.CompressorType.noop.minRatio, NoopCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, 10240, 0.5, NoopCompressor.class);

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, 5*1024, 10240, 0.5, null);
    }

    @Test
    public void snappyTest() {
        options.class_name = CompressionParams.CompressorType.snappy.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, (int)Math.ceil(CompressionParams.DEFAULT_CHUNK_LENGTH/ CompressionParams.CompressorType.snappy.minRatio), CompressionParams.CompressorType.snappy.minRatio, SnappyCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, (int)Math.ceil(5*1024/ CompressionParams.CompressorType.snappy.minRatio), CompressionParams.CompressorType.snappy.minRatio, SnappyCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, 10240, 0.5, SnappyCompressor.class);

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, 5*1024, 10240, 0.5, null);
    }

    @Test
    public void deflateTest() {
        options.class_name = CompressionParams.CompressorType.deflate.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.CompressorType.deflate.minRatio, DeflateCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, Integer.MAX_VALUE, CompressionParams.CompressorType.deflate.minRatio, DeflateCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, 10240, 0.5, DeflateCompressor.class);

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, 5*1024, 10240, 0.5, null);
    }

    @Test
    public void zstdTest() {
        options.class_name = CompressionParams.CompressorType.zstd.name();
        params = CompressionParams.fromOptions( options );
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.CompressorType.zstd.minRatio, ZstdCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, Integer.MAX_VALUE, CompressionParams.CompressorType.zstd.minRatio, ZstdCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, 10240, 0.5, ZstdCompressor.class);

        options.enabled = false;
        params = CompressionParams.fromOptions( options );
        assertParams(false, 5*1024, 10240, 0.5, null);
    }

    @Test
    public void customTest()
    {
        options.class_name = TestCompressor.class.getName();
        params = CompressionParams.fromOptions(options);
        assertParams(true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, TestCompressor.class);

        options.chunk_length = "5MiB";
        params = CompressionParams.fromOptions(options);
        assertParams(true, 5 * 1024, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, TestCompressor.class);

        options.min_compress_ratio=0.5;
        params = CompressionParams.fromOptions( options );
        assertParams(true, 5*1024, 10240, 0.5, TestCompressor.class);

        options.enabled = false;
        params = CompressionParams.fromOptions(options);
        assertParams(false, 5*1024, 10240, 0.5, null);

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
