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

import static java.lang.String.format;
import static org.apache.cassandra.schema.CompressionParams.CHUNK_LENGTH_IN_KB;
import static org.apache.cassandra.schema.CompressionParams.CLASS;
import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.apache.cassandra.schema.CompressionParams.MAX_COMPRESSED_LENGTH;
import static org.apache.cassandra.schema.CompressionParams.MIN_COMPRESS_RATIO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompressionParamsTest
{
    private static ParameterizedClass emptyParameterizedClass()
    {
        return new ParameterizedClass(null, new HashMap<>());
    }

    @Test
    public void additionalParamsTest()
    {
        // no map
        ParameterizedClass options = new ParameterizedClass();
        CompressionParams params = CompressionParams.fromParameterizedClass(options);
        assertThat(params.getOtherOptions()).isNotNull();
        assertThat(params.getOtherOptions().isEmpty()).isTrue();

        options = emptyParameterizedClass();
        params = CompressionParams.fromParameterizedClass(options);
        assertThat(params.getOtherOptions()).isNotNull();
        assertThat(params.getOtherOptions().isEmpty()).isTrue();

        options.class_name = TestCompressor.class.getName();
        options.parameters.put("foo", "bar");
        params = CompressionParams.fromParameterizedClass(options);
        assertThat(params.getOtherOptions()).isNotNull();
        assertThat(params.getOtherOptions().get("foo")).isEqualTo("bar");
    }

    // Tests chunklength settings for both Options and Map.
    private static <T> void chunkLengthTest(BiConsumer<String, String> put, Consumer<String> remove, Function<T, CompressionParams> func, T instance)
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
        assertEquals(1024 * 1024, params.chunkLength());

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

        put.accept(CHUNK_LENGTH_IN_KB, "");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value");

        put.accept(CHUNK_LENGTH_IN_KB, "0");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value");

        put.accept(CHUNK_LENGTH_IN_KB, "1");
        params = func.apply(instance);
        assertEquals(1024, params.chunkLength());

        put.accept(CHUNK_LENGTH_IN_KB, "badvalue");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value");

        put.accept(CHUNK_LENGTH_IN_KB, "3");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value")
                                                               .withMessageContaining("Must be a power of 2");

        // test negative value
        put.accept(CHUNK_LENGTH_IN_KB, "-1");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'chunk_length_in_kb' value")
                                                               .withMessageContaining("May not be <= 0");

        remove.accept(CHUNK_LENGTH_IN_KB);


        // TEST COMBINATIONS
        put.accept(CompressionParams.CHUNK_LENGTH, "3MiB");
        put.accept(CHUNK_LENGTH_IN_KB, "2");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessage(CompressionParams.TOO_MANY_CHUNK_LENGTH);
    }

    @Test
    public void chunkLengthTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        chunkLengthTest(options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        Map<String, String> map = new HashMap<String, String>();
        map.put(CLASS, "lz4");
        Consumer<String> remove = map::remove;
        chunkLengthTest(map::put, remove, CompressionParams::fromMap, map);
    }

    private static <T> void minCompressRatioTest(BiConsumer<String, String> put, Function<T, CompressionParams> func, T instance)
    {

        CompressionParams params = func.apply(instance);
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());

        put.accept(MIN_COMPRESS_RATIO, "0.0"); //CompressionParams.DEFAULT_MIN_COMPRESS_RATIO
        params = func.apply(instance);
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());

        put.accept(MIN_COMPRESS_RATIO, "0.3");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'min_compress_ratio' value")
                                                               .withMessageContaining("Can either be 0 or greater than or equal to 1");

        put.accept(MIN_COMPRESS_RATIO, "1.3");
        params = func.apply(instance);
        assertEquals(1.3, params.minCompressRatio(), Double.MIN_VALUE);
        assertEquals((int) Math.ceil(DEFAULT_CHUNK_LENGTH / 1.3), params.maxCompressedLength());

        put.accept(MIN_COMPRESS_RATIO, "-1.0");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'min_compress_ratio' value")
                                                               .withMessageContaining("Can either be 0 or greater than or equal to 1");
    }

    @Test
    public void minCompressRatioTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        minCompressRatioTest(options.parameters::put, CompressionParams::fromParameterizedClass, options);

        Map<String, String> map = new HashMap<>();
        map.put(CLASS, "lz4");
        minCompressRatioTest(map::put, CompressionParams::fromMap, map);
    }

    private static <T> void maxCompressedLengthTest(BiConsumer<String, String> put, Function<T, CompressionParams> func, T instance)
    {
        CompressionParams params = func.apply(instance);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);

        put.accept(CompressionParams.MAX_COMPRESSED_LENGTH, "");
        params = func.apply(instance);
        assertEquals(Integer.MAX_VALUE, params.maxCompressedLength());
        assertEquals(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, params.minCompressRatio(), Double.MIN_VALUE);

        put.accept(CompressionParams.MAX_COMPRESSED_LENGTH, "4KiB");
        params = func.apply(instance);
        assertEquals(4 * 1024, params.maxCompressedLength());
        assertEquals(DEFAULT_CHUNK_LENGTH / (4.0 * 1024), params.minCompressRatio(), Double.MIN_VALUE);

        put.accept(CompressionParams.MAX_COMPRESSED_LENGTH, "badvalue");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'max_compressed_length' value")
                                                               .withMessageContaining("Invalid data storage");
    }

    @Test
    public void maxCompressedLengthTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        maxCompressedLengthTest(options.parameters::put, CompressionParams::fromParameterizedClass, options);

        Map<String, String> map = new HashMap<>();
        map.put(CLASS, "lz4");
        maxCompressedLengthTest(map::put, CompressionParams::fromMap, map);
    }

    @Test
    public void maxCompressionLengthAndMinCompressRatioTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.parameters.put(MIN_COMPRESS_RATIO, "1.0");
        options.parameters.put(CompressionParams.MAX_COMPRESSED_LENGTH, "4Gib");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass(options))
                                                               .withMessage("Can not specify both 'min_compress_ratio' and 'max_compressed_length' for the compressor parameters.");

        Map<String, String> map = new HashMap<>();
        map.put(CLASS, "lz4");
        map.put(MIN_COMPRESS_RATIO, "1.0");
        map.put(CompressionParams.MAX_COMPRESSED_LENGTH, "4Gib");
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
            assertThat(params.getSstableCompressor()).isInstanceOf(compressor);
        else
            assertThat(params.getSstableCompressor()).isNull();
    }


    @Test
    public void defaultTest()
    {
        CompressionParams params = CompressionParams.fromParameterizedClass(emptyParameterizedClass());
        assertParams(params, true, DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, LZ4Compressor.class);
        roundTripMapTest(params);

        params = CompressionParams.fromParameterizedClass(null);
        assertParams(params, true, DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, LZ4Compressor.class);
        roundTripMapTest(params);

        params = CompressionParams.fromMap(Collections.emptyMap());
        assertParams(params, true, DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, LZ4Compressor.class);
        roundTripMapTest(params);
    }

    private static <T> void paramsTest(boolean enabled, Class<?> clazz, BiConsumer<String, String> put, Consumer<String> remove, Function<T, CompressionParams> func, T instance)
    {
        int expectedChunkLength = 4 * 1024 * 1024;
        int expectedCompressedLength = 2 * 1024 * 1024;
        int expectedRatioCompressedLength = 2796203;

        CompressionParams params = func.apply(instance);
        assertParams(params, enabled, DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, clazz);
        roundTripMapTest(params);

        put.accept(CompressionParams.CHUNK_LENGTH, "4MiB");
        params = func.apply(instance);
        assertParams(params, enabled, expectedChunkLength, Integer.MAX_VALUE, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO, clazz);
        roundTripMapTest(params);

        put.accept(CompressionParams.MAX_COMPRESSED_LENGTH, "2MiB");
        params = func.apply(instance);
        assertParams(params, enabled, expectedChunkLength, expectedCompressedLength, 2.0, clazz);
        roundTripMapTest(params);

        put.accept(CompressionParams.MAX_COMPRESSED_LENGTH, "2097151KiB");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> func.apply(instance))
                                                               .withMessageContaining("Invalid 'max_compressed_length' value for the 'compression' option: Must be less than or equal to chunk length");
        assertParams(params, enabled, expectedChunkLength, expectedCompressedLength, 2.0, clazz);
        roundTripMapTest(params);

        remove.accept(CompressionParams.MAX_COMPRESSED_LENGTH);
        put.accept(MIN_COMPRESS_RATIO, "1.5");
        params = func.apply(instance);
        assertParams(params, enabled, expectedChunkLength, expectedRatioCompressedLength, 1.5, clazz);
        roundTripMapTest(params);

        put.accept(CompressionParams.ENABLED, "false");
        params = func.apply(instance);
        assertParams(params, false, expectedChunkLength, expectedRatioCompressedLength, 1.5, null);
        roundTripMapTest(params);
    }

    @Test
    public void fromMapTest()
    {
        // chunk length < 0
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromMap(new HashMap<String, String>()
                                                               {{
                                                                   put(CLASS, TestCompressor.class.getName());
                                                                   put(CHUNK_LENGTH_IN_KB, "-1");
                                                                   put(MIN_COMPRESS_RATIO, "0");
                                                               }}))
                                                               .withMessage(format("Invalid '%s' value for the 'compression' option.  May not be <= 0: -1", CHUNK_LENGTH_IN_KB));

        // chunk length = 0
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromMap(new HashMap<String, String>()
                                                               {{
                                                                   put(CLASS, TestCompressor.class.getName());
                                                                   put(CHUNK_LENGTH_IN_KB, "0");
                                                                   put(MIN_COMPRESS_RATIO, "0");
                                                               }}))
                                                               .withMessage(format("Invalid '%s' value for the 'compression' option.  May not be <= 0: 0", CHUNK_LENGTH_IN_KB));

        // min compress ratio < 0
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromMap(new HashMap<String, String>()
                                                               {{
                                                                   put(CLASS, TestCompressor.class.getName());
                                                                   put(CHUNK_LENGTH_IN_KB, Integer.toString(DEFAULT_CHUNK_LENGTH));
                                                                   put(MIN_COMPRESS_RATIO, "-1.0");
                                                               }}))
                                                               .withMessageContaining(format("Invalid '%s' value for the 'compression' option.  Can either be 0 or greater than or equal to 1", MIN_COMPRESS_RATIO));

        // 0 < min compress ratio < 1
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromMap(new HashMap<String, String>()
                                                               {{
                                                                   put(CLASS, TestCompressor.class.getName());
                                                                   put(CHUNK_LENGTH_IN_KB, Integer.toString(DEFAULT_CHUNK_LENGTH));
                                                                   put(MIN_COMPRESS_RATIO, "0.5");
                                                               }}))
                                                               .withMessageContaining(format("Invalid '%s' value for the 'compression' option.  Can either be 0 or greater than or equal to 1", MIN_COMPRESS_RATIO));

        // max compressed length > chunk length
        int len = 1 << 5;
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromMap(new HashMap<String, String>()
                                                               {{
                                                                   put(CLASS, TestCompressor.class.getName());
                                                                   put(CHUNK_LENGTH_IN_KB, Integer.toString(len));
                                                                   put(MAX_COMPRESSED_LENGTH, Integer.toString(len + 1) + "KiB");
                                                               }}))
                                                               .withMessageContaining(format("Invalid '%s' value for the 'compression' option: Must be less than or equal to chunk length", MAX_COMPRESSED_LENGTH));
    }

    private static void roundTripMapTest(CompressionParams params)
    {
        Map<String, String> map = params.asMap();

        CompressionParams other = CompressionParams.fromMap(map);
        if (params.isEnabled())
        {
            assertTrue(other.isEnabled());
            assertThat(other.getOtherOptions()).isEqualTo(params.getOtherOptions());
            assertThat(other.maxCompressedLength()).isEqualTo(params.maxCompressedLength());
            assertThat(other.minCompressRatio()).isEqualTo(params.minCompressRatio());
            assertThat(other.chunkLength()).isEqualTo(params.chunkLength());
            assertThat(other.getCrcCheckChance()).isEqualTo(params.getCrcCheckChance());
            assertThat(other.klass()).isEqualTo(params.klass());
        }
        else
        {
            assertThat(map.size()).isEqualTo(1);
            assertThat(map.get(CompressionParams.ENABLED)).isEqualTo("false");
            assertFalse(other.isEnabled());
            assertTrue(other.getOtherOptions().isEmpty());
            assertThat(other.maxCompressedLength()).isEqualTo(Integer.MAX_VALUE);
            assertThat(other.minCompressRatio()).isEqualTo(CompressionParams.DEFAULT_MIN_COMPRESS_RATIO);
            assertThat(other.chunkLength()).isEqualTo(DEFAULT_CHUNK_LENGTH);
            assertThat(other.getCrcCheckChance()).isEqualTo(1.0);
            assertThat(other.getSstableCompressor()).isNull();
        }
    }

    @Test
    public void lz4Test()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.lz4.name();
        paramsTest(true, LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = LZ4Compressor.class.getSimpleName();
        paramsTest(true, LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = LZ4Compressor.class.getName();
        paramsTest(true, LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = LZ4Compressor.class.getName();
        options.parameters.put(LZ4Compressor.LZ4_COMPRESSOR_TYPE, LZ4Compressor.LZ4_FAST_COMPRESSOR);
        paramsTest(true, LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        Map<String, String> map = new HashMap<>();
        map.put(CLASS, CompressionParams.CompressorType.lz4.name());
        paramsTest(true, LZ4Compressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, LZ4Compressor.class.getName());
        paramsTest(true, LZ4Compressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, LZ4Compressor.class.getName());
        paramsTest(true, LZ4Compressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, LZ4Compressor.class.getName());
        map.put(LZ4Compressor.LZ4_COMPRESSOR_TYPE, LZ4Compressor.LZ4_FAST_COMPRESSOR);
        paramsTest(true, LZ4Compressor.class, map::put, map::remove, CompressionParams::fromMap, map);
    }

    @Test
    public void noneTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.none.name();
        paramsTest(false, null, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = CompressionParams.CompressorType.none.name();
        options.parameters.put("foo", "bar");
        paramsTest(false, null, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = null; // constructs the default class
        paramsTest(true, LZ4Compressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        Map<String, String> map = new HashMap<>();
        map.put(CompressionParams.ENABLED, "false");
        paramsTest(false, null, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CompressionParams.ENABLED, "false");
        map.put(CLASS, "dummy");
        paramsTest(false, null, map::put, map::remove, CompressionParams::fromMap, map);
    }

    @Test
    public void noopTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.noop.name();
        paramsTest(true, NoopCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = "NoopCompressor";
        paramsTest(true, NoopCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = NoopCompressor.class.getName();
        paramsTest(true, NoopCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = NoopCompressor.class.getName();
        options.parameters.put("foo", "bar");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> paramsTest(true, NoopCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options))
                                                               .withMessageContaining("Unknown compression options: ([foo])");

        Map<String, String> map = new HashMap<>();
        map.put(CLASS, CompressionParams.CompressorType.noop.name());
        paramsTest(true, NoopCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, "NoopCompressor");
        paramsTest(true, NoopCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, NoopCompressor.class.getName());
        paramsTest(true, NoopCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, NoopCompressor.class.getName());
        map.put("foo", "bor");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> paramsTest(true, NoopCompressor.class, map::put, map::remove, CompressionParams::fromMap, map))
                                                               .withMessageContaining("Unknown compression options: ([foo])");
    }

    @Test
    public void snappyTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.snappy.name();
        paramsTest(true, SnappyCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = SnappyCompressor.class.getSimpleName();
        paramsTest(true, SnappyCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = SnappyCompressor.class.getName();
        paramsTest(true, SnappyCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = SnappyCompressor.class.getName();
        options.parameters.put("foo", "bar");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> paramsTest(true, SnappyCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options))
                                                               .withMessageContaining("Unknown compression options: ([foo])");

        Map<String, String> map = new HashMap<>();
        map.put(CLASS, CompressionParams.CompressorType.snappy.name());
        paramsTest(true, SnappyCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, SnappyCompressor.class.getName());
        paramsTest(true, SnappyCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, SnappyCompressor.class.getName());
        paramsTest(true, SnappyCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, SnappyCompressor.class.getName());
        map.put("foo", "bor");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> paramsTest(true, SnappyCompressor.class, map::put, map::remove, CompressionParams::fromMap, map))
                                                               .withMessageContaining("Unknown compression options: ([foo])");
    }

    @Test
    public void deflateTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.deflate.name();
        paramsTest(true, DeflateCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = DeflateCompressor.class.getSimpleName();
        paramsTest(true, DeflateCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = DeflateCompressor.class.getName();
        paramsTest(true, DeflateCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = DeflateCompressor.class.getName();
        options.parameters.put("foo", "bar");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> paramsTest(true, DeflateCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options))
                                                               .withMessageContaining("Unknown compression options: ([foo])");


        Map<String, String> map = new HashMap<>();
        map.put(CLASS, CompressionParams.CompressorType.deflate.name());
        paramsTest(true, DeflateCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, DeflateCompressor.class.getSimpleName());
        paramsTest(true, DeflateCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, DeflateCompressor.class.getName());
        paramsTest(true, DeflateCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, DeflateCompressor.class.getName());
        map.put("foo", "bor");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> paramsTest(true, DeflateCompressor.class, map::put, map::remove, CompressionParams::fromMap, map))
                                                               .withMessageContaining("Unknown compression options: ([foo])");
    }

    @Test
    public void zstdTest()
    {
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = CompressionParams.CompressorType.zstd.name();
        paramsTest(true, ZstdCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = ZstdCompressor.class.getSimpleName();
        paramsTest(true, ZstdCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = ZstdCompressor.class.getName();
        paramsTest(true, ZstdCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = ZstdCompressor.class.getName();
        options.parameters.put(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(ZstdCompressor.FAST_COMPRESSION_LEVEL));
        paramsTest(true, ZstdCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        Map<String, String> map = new HashMap<>();
        map.put(CLASS, CompressionParams.CompressorType.zstd.name());
        paramsTest(true, ZstdCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, ZstdCompressor.class.getSimpleName());
        paramsTest(true, ZstdCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, ZstdCompressor.class.getName());
        paramsTest(true, ZstdCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, ZstdCompressor.class.getName());
        map.put(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(ZstdCompressor.FAST_COMPRESSION_LEVEL));
        paramsTest(true, ZstdCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);
    }

    @Test
    public void customTest()
    {
        // test with options
        ParameterizedClass options = emptyParameterizedClass();
        options.class_name = TestCompressor.class.getName();
        paramsTest(true, TestCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        options.parameters.clear();
        options.class_name = TestCompressor.class.getName();
        options.parameters.put("foo", "bar");
        paramsTest(true, TestCompressor.class, options.parameters::put, options.parameters::remove, CompressionParams::fromParameterizedClass, options);

        // test with map
        Map<String, String> map = new HashMap<>();
        map.put(CLASS, TestCompressor.class.getName());
        paramsTest(true, TestCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        map.clear();
        map.put(CLASS, TestCompressor.class.getName());
        map.put("foo", "bar");
        paramsTest(true, TestCompressor.class, map::put, map::remove, CompressionParams::fromMap, map);

        // test invalid class name
        options.parameters.clear();
        options.class_name = "foo";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass(options))
                                                               .withMessage("Could not create Compression for type org.apache.cassandra.io.compress.foo");
        map.clear();
        options.class_name = "foo";
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass(options))
                                                               .withMessage("Could not create Compression for type org.apache.cassandra.io.compress.foo");
    }

    @Test
    public void fromParameterizedClassTest()
    {
        Map<String, String> params = new HashMap<>();
        params.put("enabled", "true");
        params.put("chunk_length", "16KiB");
        params.put("min_compress_ratio", "0.0");
        params.put("max_comrpessed_length", "16KiB");
        params.put("class_specific_parameter", "value");
        ParameterizedClass parameterizedClass = new ParameterizedClass("lz4", params);
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> CompressionParams.fromParameterizedClass(parameterizedClass))
                                                               .withMessageContaining("Unknown compression options:")
                                                               .withMessageContaining("class_specific_parameter")
                                                               .withMessageContaining("max_comrpessed_length");

        params.remove("class_specific_parameter");
        params.remove("max_comrpessed_length");
        CompressionParams cp = CompressionParams.fromParameterizedClass(parameterizedClass);
        assertNotNull(cp);
    }

    @Test
    public void testMap()
    {
        Map<String, String> params = new HashMap<>();
        params.put(CompressionParams.ENABLED, "true");
        params.put(CompressionParams.CHUNK_LENGTH, "16KiB");
        params.put(MIN_COMPRESS_RATIO, "0.0");
        ParameterizedClass parameterizedClass = new ParameterizedClass("lz4", params);
        Map<String, String> map = CompressionParams.fromParameterizedClass(parameterizedClass).asMap();
        assertEquals("16", map.remove(CHUNK_LENGTH_IN_KB));
        assertEquals(CompressionParams.CompressorType.lz4.className, map.remove(CLASS));
        assertTrue(map.isEmpty());

        params.put(MIN_COMPRESS_RATIO, "1.5");
        parameterizedClass = new ParameterizedClass("lz4", params);
        map = CompressionParams.fromParameterizedClass(parameterizedClass).asMap();
        assertEquals("16", map.remove(CHUNK_LENGTH_IN_KB));
        assertEquals(CompressionParams.CompressorType.lz4.className, map.remove(CLASS));
        assertEquals("1.5", map.remove(MIN_COMPRESS_RATIO));
        assertTrue(map.isEmpty());

        params.put(LZ4Compressor.LZ4_COMPRESSOR_TYPE, LZ4Compressor.LZ4_FAST_COMPRESSOR);
        parameterizedClass = new ParameterizedClass("lz4", params);
        map = CompressionParams.fromParameterizedClass(parameterizedClass).asMap();
        assertEquals("16", map.remove(CHUNK_LENGTH_IN_KB));
        assertEquals(CompressionParams.CompressorType.lz4.className, map.remove(CLASS));
        assertEquals("1.5", map.remove(MIN_COMPRESS_RATIO));
        assertEquals(LZ4Compressor.LZ4_FAST_COMPRESSOR, map.remove(LZ4Compressor.LZ4_COMPRESSOR_TYPE));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testChunkSizeCalculation()
    {
        Map<String, String> params = new HashMap<>();
        params.put(CompressionParams.CHUNK_LENGTH, "16KiB");
        ParameterizedClass parameterizedClass = new ParameterizedClass("lz4", params);
        Map<String, String> map = CompressionParams.fromParameterizedClass(parameterizedClass).asMap();

        Map<String, String> params2 = new HashMap<>();
        params2.put(CHUNK_LENGTH_IN_KB, "16");
        ParameterizedClass parameterizedClass2 = new ParameterizedClass("lz4", params2);
        Map<String, String> map2 = CompressionParams.fromParameterizedClass(parameterizedClass2).asMap();

        assertEquals(map.get(CHUNK_LENGTH_IN_KB), map2.get(CHUNK_LENGTH_IN_KB));
    }

    public static class TestCompressor implements ICompressor
    {
        Map<String, String> options;

        static public TestCompressor create(Map<String, String> options)
        {
            return new TestCompressor(options);
        }

        private TestCompressor(Map<String, String> options)
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
