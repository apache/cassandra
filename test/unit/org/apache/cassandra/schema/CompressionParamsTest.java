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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import org.apache.cassandra.config.ParameterizedClass;
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
    //private ParameterizedClass options;
    //private CompressionParams params;


    private static  ParameterizedClass emptyParameterizedClass() {
        return new ParameterizedClass(null, new HashMap<>());
    }

    @Test
    public void additionalParamsTest() {
        // no map
        ParameterizedClass options = new ParameterizedClass();
        CompressionParams params = CompressionParams.fromParameterizedClass(options);
        assertThat( params.getOtherOptions()).isNotNull();
        assertThat( params.getOtherOptions().isEmpty()).isTrue();

        options = emptyParameterizedClass();
        params = CompressionParams.fromParameterizedClass(options);
        assertThat( params.getOtherOptions()).isNotNull();
        assertThat( params.getOtherOptions().isEmpty()).isTrue();

        options.parameters.put( "foo", "bar");
        params = CompressionParams.fromParameterizedClass(options);
        params = CompressionParams.fromParameterizedClass(options);
        assertThat( params.getOtherOptions()).isNotNull();
        assertThat( params.getOtherOptions().get("foo")).isEqualTo("bar");
    }

    // Tests chunklength settings for both Options and Map.
    private static <T> void chunkLengthTest(BiConsumer<String,String> put, Consumer<String> remove, Function<T,CompressionParams> func, T instance)
    {
        // CHUNK_LENGTH

        // test empty string
        put.accept(CompressionParams.CHUNK_LENGTH, "");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length' value");

        // text zero string
        put.accept(CompressionParams.CHUNK_LENGTH, "0MiB");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length' value");


        // test properly formated value
        put.accept(CompressionParams.CHUNK_LENGTH, "1MiB");
        CompressionParams params = func.apply(instance);
        assertEquals(1024, params.chunkLength());

        // test bad string
        put.accept(CompressionParams.CHUNK_LENGTH, "badvalue");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length' value");

        // test not power of 2
        put.accept(CompressionParams.CHUNK_LENGTH, "3MiB");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length' value")
                                                               .withMessageContaining("Must be a power of 2");

        remove.accept(CompressionParams.CHUNK_LENGTH);


        // CHUNK_LENGTH_IN_KB
        // same tests as above

        put.accept(CompressionParams.CHUNK_LENGTH_IN_KB, "");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value");

        put.accept(CompressionParams.CHUNK_LENGTH_IN_KB, "0");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value");

        put.accept(CompressionParams.CHUNK_LENGTH_IN_KB, "1");
        params = func.apply(instance);
        assertEquals(1024, params.chunkLength());

        put.accept(CompressionParams.CHUNK_LENGTH_IN_KB, "badvalue");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value");

        put.accept(CompressionParams.CHUNK_LENGTH_IN_KB, "3");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value")
                                                               .withMessageContaining("Must be a power of 2");

        // test negative value
        put.accept(CompressionParams.CHUNK_LENGTH_IN_KB, "-1");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value")
                                                               .withMessageContaining("May not be <= 0");

        remove.accept(CompressionParams.CHUNK_LENGTH_IN_KB);


        // CHUNK_LENGTH_KB
        // same stests as above

        put.accept(CompressionParams.CHUNK_LENGTH_KB, "");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_kb' value");

        put.accept(CompressionParams.CHUNK_LENGTH_KB, "0");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_kb' value");

        put.accept(CompressionParams.CHUNK_LENGTH_KB, "1");
        params = func.apply(instance);
        assertEquals(1024, params.chunkLength());

        put.accept(CompressionParams.CHUNK_LENGTH_KB, "badvalue");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_kb' value");

        put.accept(CompressionParams.CHUNK_LENGTH_KB, "3");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_kb' value")
                                                               .withMessageContaining("Must be a power of 2");

        // test negative value
        put.accept(CompressionParams.CHUNK_LENGTH_KB, "-1");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_kb' value")
                                                               .withMessageContaining("May not be <= 0");

        remove.accept(CompressionParams.CHUNK_LENGTH_KB);

        // TEST COMBINATIONS
        put.accept(CompressionParams.CHUNK_LENGTH, "3MiB");
        put.accept(CompressionParams.CHUNK_LENGTH_IN_KB, "2");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessage(CompressionParams.TOO_MANY_CHUNK_LENGTH);

        remove.accept(CompressionParams.CHUNK_LENGTH);
        put.accept(CompressionParams.CHUNK_LENGTH_KB, "2");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessage(CompressionParams.TOO_MANY_CHUNK_LENGTH);

        remove.accept(CompressionParams.CHUNK_LENGTH_IN_KB);
        put.accept(CompressionParams.CHUNK_LENGTH, "3MiB");
        put.accept(CompressionParams.CHUNK_LENGTH_KB, "2");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessage(CompressionParams.TOO_MANY_CHUNK_LENGTH);
    }

    @Test
    public void chunkLengthTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        chunkLengthTest( options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        Map<String,String> map = new HashMap<String,String>();
        map.put( CompressionParams.CLASS, "lz4");
        Consumer<String> remove = (s) -> map.remove(s);
        chunkLengthTest( map::put,remove, CompressionParams::fromMap, map );
    }

    @Test
    public void multipleClassNameTest() {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = "none";
        options.parameters.put( CompressionParams.SSTABLE_COMPRESSION, "none");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass( options ))
        .withMessageContaining("The 'sstable_compression' option must not be used");

        Map<String,String> map = new HashMap<>();
        map.put( CompressionParams.CLASS, "none" );
        map.put( CompressionParams.SSTABLE_COMPRESSION, "none" );
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromMap( map ))
                                                               .withMessageContaining("The 'sstable_compression' option must not be used");

    }

    private static <T> void minCompressRatioTest(BiConsumer<String,String> put,  Function<T,CompressionParams> func, T instance)
    {

        CompressionParams params = func.apply(instance);
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());

        put.accept( CompressionParams.MIN_COMPRESS_RATIO, "0.0" ); //CompressionParams.DEFAULT_MIN_COMPRESS_RATIO
        params =func.apply(instance);
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());

        put.accept( CompressionParams.MIN_COMPRESS_RATIO, "0.3");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining( "Invalid 'min_compress_ratio' value")
                                                               .withMessageContaining("Can either be 0 or greater than or equal to 1");

        put.accept( CompressionParams.MIN_COMPRESS_RATIO, "1.3");
        params = func.apply(instance);
        assertEquals(1.3, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals( (int) Math.ceil(CompressionParams.DEFAULT_CHUNK_LENGTH / 1.3), params.maxCompressedLength());

        put.accept( CompressionParams.MIN_COMPRESS_RATIO,  "-1.0");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining( "Invalid 'min_compress_ratio' value")
                                                               .withMessageContaining("Can either be 0 or greater than or equal to 1");
    }

    @Test
    public void minCompressRatioTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        minCompressRatioTest( options.parameters::put, CompressionParams::fromParameterizedClass, options );

        Map<String,String> map = new HashMap<String,String>();
        map.put( CompressionParams.CLASS, "lz4");
        minCompressRatioTest( map::put, CompressionParams::fromMap, map );
    }

    private static <T> void maxCompressedLengthTest(BiConsumer<String,String> put,  Function<T,CompressionParams> func, T instance)
    {
        CompressionParams params = func.apply(instance);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);

        put.accept( CompressionParams.MAX_COMPRESSED_LENGTH,"");
        params = func.apply(instance);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);

        put.accept( CompressionParams.MAX_COMPRESSED_LENGTH,"4MiB");
        params = func.apply(instance);
        assertEquals(4*1024, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_CHUNK_LENGTH / (4.0 * 1024), params.minCompressRatio(), Double.MIN_VALUE);

        put.accept( CompressionParams.MAX_COMPRESSED_LENGTH,"badvalue");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'max_compressed_length' value")
                                                               .withMessageContaining("Invalid data storage");
    }

    @Test
    public void maxCompressedLengthTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        maxCompressedLengthTest(options.parameters::put, CompressionParams::fromParameterizedClass, options);

        Map<String, String> map = new HashMap<String, String>();
        map.put(CompressionParams.CLASS, "lz4");
        maxCompressedLengthTest(map::put, CompressionParams::fromMap, map);
    }

    @Test
    public void maxCompressionLengthAndMinCompressRatioTest() {
        ParameterizedClass options = emptyParameterizedClass();
        options.parameters.put( CompressionParams.MIN_COMPRESS_RATIO, "1.0");
        options.parameters.put( CompressionParams.MAX_COMPRESSED_LENGTH, "4Gib");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass(options))
                                                               .withMessage("Can not specify both 'min_compress_ratio' and 'max_compressed_length' for the compressor parameters.");

        Map<String,String> map = new HashMap<>();
        map.put( CompressionParams.CLASS, "lz4");
        map.put( CompressionParams.MIN_COMPRESS_RATIO, "1.0");
        map.put( CompressionParams.MAX_COMPRESSED_LENGTH, "4Gib");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromMap(map))
                                                               .withMessage("Can not specify both 'min_compress_ratio' and 'max_compressed_length' for the compressor parameters.");

    }

    private static void assertParams(CompressionParams params, boolean enabled, int chunkLength, int maxCompressedLength, double minCompressRatio, Class<?> compressor)
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
        CompressionParams params = CompressionParams.fromParameterizedClass( emptyParameterizedClass() );
        assertParams(params,true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, LZ4Compressor.class);

        params = CompressionParams.fromParameterizedClass( null );
        assertParams(params, true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, LZ4Compressor.class);

        params = CompressionParams.fromMap(Collections.EMPTY_MAP );
        assertParams(params,true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, LZ4Compressor.class);
    }

    private static <T> void compressorTest(Class<?> clazz, BiConsumer<String,String> put, Consumer<String> remove, Function<T,CompressionParams> func, T instance)
    {
        CompressionParams params = func.apply(instance);
        assertParams(params, true, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, clazz);

        put.accept( CompressionParams.CHUNK_LENGTH,"4MiB");
        params = func.apply(instance);
        assertParams(params, true, 4*1024, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, clazz);

        put.accept( CompressionParams.MAX_COMPRESSED_LENGTH,"2MiB");
        params = func.apply(instance);
        assertParams(params, true, 4*1024, 2*1024, 2.0, clazz);

        put.accept( CompressionParams.MAX_COMPRESSED_LENGTH,"2097151KiB");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() ->func.apply(instance))
        .withMessageContaining("Invalid 'max_compressed_length' value for the 'compression' option: Must be less than or equal to chunk length");
        assertParams(params, true, 4*1024, 2*1024, 2.0, clazz);

        remove.accept(CompressionParams.MAX_COMPRESSED_LENGTH);
        put.accept( CompressionParams.MIN_COMPRESS_RATIO,"1.5");
        params = func.apply(instance);
        assertParams(params, true, 4*1024, 2731, 1.5, clazz);

        put.accept( CompressionParams.ENABLED,"false");
        params = func.apply(instance);
        assertParams(params, false, 4*1024, 2731, 1.5, null);
    }


    @Test
    public void constructorTest() {
        Map<String,String> map = new HashMap<>();



        // chunk length < 0
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> new CompressionParams( TestCompressor.class.getName(), map,  -1, 0.0))
        .withMessage("Invalid 'chunk_length' value for the 'compression' option.  May not be <= 0: -1");

        // chunk length = 0
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> new CompressionParams( TestCompressor.class.getName(), map,  -1, 0.0))
                                                               .withMessage("Invalid 'chunk_length' value for the 'compression' option.  May not be <= 0: -1");

        // min compress ratio < 0
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> new CompressionParams( TestCompressor.class.getName(), map,  CompressionParams.DEFAULT_CHUNK_LENGTH, -1.0))
                                                               .withMessageContaining("Invalid 'min_compress_ratio' value for the 'compression' option.  Can either be 0 or greater than or equal to 1");

        // 0 < min compress ratio < 1
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> new CompressionParams( TestCompressor.class.getName(), map,  CompressionParams.DEFAULT_CHUNK_LENGTH, 0.5))
                                                               .withMessageContaining("Invalid 'min_compress_ratio' value for the 'compression' option.  Can either be 0 or greater than or equal to 1");

        // max compressed length > chunk length
        int len = 1 << 30;
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> new CompressionParams( TestCompressor.class.getName(),  len, len+1, map ))
                                                               .withMessageContaining("Invalid 'max_compressed_length' value for the 'compression' option: Must be less than or equal to chunk length");

        // max compressed length < 0
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> new CompressionParams( TestCompressor.class.getName(),  CompressionParams.DEFAULT_CHUNK_LENGTH, -1, map ))
                                                               .withMessageContaining("Invalid 'max_compressed_length' value for the 'compression' option.  May not be less than zero: -1");
    }

    @Test
    public void lz4Test() {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.lz4.name();
        compressorTest(LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = "LZ4Compressor";
        compressorTest(LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = LZ4Compressor.class.getName();
        compressorTest(LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );
    }

    @Test
    public void noneTest() {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.none.name();
        CompressionParams params = CompressionParams.fromParameterizedClass( options );
        // none is never enabled.
        assertParams(params, false, CompressionParams.DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, null);

        options.parameters.put( CompressionParams.CHUNK_LENGTH,"4MiB");
        params = CompressionParams.fromParameterizedClass( options );
        // none does not set chunk length
        assertParams(params, false, 4*1024, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, null);

        options.parameters.put( CompressionParams.MIN_COMPRESS_RATIO,"1.5");
        params = CompressionParams.fromParameterizedClass( options );
        assertParams(params, false, 4*1024, 2731, 1.5, null);

        options.parameters.put( CompressionParams.ENABLED,"false");
        params = CompressionParams.fromParameterizedClass( options );
        assertParams(params, false, 4*1024, 2731, 1.5, null);

    }

    @Test
    public void noopTest() {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.noop.name();
        compressorTest(NoopCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = "NoopCompressor";
        compressorTest(NoopCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = NoopCompressor.class.getName();
        compressorTest(NoopCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );
    }

    @Test
    public void snappyTest() {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.snappy.name();
        compressorTest(SnappyCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = "SnappyCompressor";
        compressorTest(SnappyCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = SnappyCompressor.class.getName();
        compressorTest(SnappyCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

    }

    @Test
    public void deflateTest() {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.deflate.name();
        compressorTest(DeflateCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = "DeflateCompressor";
        compressorTest(DeflateCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = DeflateCompressor.class.getName();
        compressorTest(DeflateCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );
    }

    @Test
    public void zstdTest() {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.zstd.name();
        compressorTest(ZstdCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = "ZstdCompressor";
        compressorTest(ZstdCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = ZstdCompressor.class.getName();
        compressorTest(ZstdCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );
    }

    @Test
    public void customTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = TestCompressor.class.getName();
        compressorTest(TestCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options );

        options.parameters.clear();
        options.class_name = "foo";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass(options))
                                                               .withMessage("Could not create Compression for type org.apache.cassandra.io.compress.foo");

        options.class_name = "foo";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass(options))
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
