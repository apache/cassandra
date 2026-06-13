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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.io.compress.AbstractCompressionProvider;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.SnappyCompressor;

/**
 * Shared test compression provider and compressor tests.
 * Provides reusable providers and compressors used across
 * {@code CompressorRegistryTest}, {@code StartupChecksTest}, and any future tests
 * that exercise the compression provider interface.
 */
public final class CompressionProviderHelper
{
    private CompressionProviderHelper() {}

    // -------------------------------------------------------------------------
    // Providers
    // -------------------------------------------------------------------------

    // Test compression provider for testing various edge cases around initialization, health checks, and
    // compressor creation.
    public static class TestCompressionProvider extends AbstractCompressionProvider
    {
        // ParameterizedClass.parameters are used to pass different test scenarios .
        // resolveProvider() only strips FAIL_ON_MISSING_PROVIDER, so get to init().
        public static final String FAIL_INIT             = "fail_init";
        public static final String FAIL_HEALTH           = "fail_health";
        public static final String FAIL_HEALTH_EXCEPTION = "fail_health_exception";
        public static final String FAIL_CREATE           = "fail_create";
        public static final String FAIL_SERIALIZED_AS    = "fail_serialized_as";

        private boolean flag(String key)
        {
            return Boolean.parseBoolean(getParameters().getOrDefault(key, "false"));
        }

        @Override
        public void init(Map<String, String> parameters)
        {
            super.init(parameters);          //use this to store and provide test variants
            if (flag(FAIL_INIT))
                throw new RuntimeException("init failed: something went wrong with startup");
        }

        @Override
        public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options) throws IllegalStateException
        {
            if (flag(FAIL_CREATE))
            {
                throw new IllegalStateException("compressor instantiation failed");
            }
            return flag(FAIL_SERIALIZED_AS)
                ? PlainTestCompressor.create(options)
                : SerializedTestCompressor.create(options);
        }

        @Override
        public boolean isHealthy()
        {
            if (flag(FAIL_HEALTH_EXCEPTION))
                throw new RuntimeException("health check failed");
            if (flag(FAIL_HEALTH)) return false;
            return true;
        }
    }

    public static class CompatibleSnappyProvider extends AbstractCompressionProvider
    {
        @Override
        public boolean isHealthy()
        {
            return true;
        }

        @Override
        public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options)
        {
            return new MySnappyCompressor();
        }
    }

    public static class IncompatibleSnappyProvider extends AbstractCompressionProvider
    {
        @Override
        public boolean isHealthy()
        {
            return true;
        }

        @Override
        public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options)
        {
            return new IncompatibleSnappyCompressor();
        }
    }

    // -------------------------------------------------------------------------
    // Compressors
    // -------------------------------------------------------------------------

    abstract static class CustomTestCompressor implements ICompressor
    {
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

    // This test compressor does not override serializedAs(). serialized call should return PlainTestCompressor.class
    public static class PlainTestCompressor extends CustomTestCompressor
    {
        public static PlainTestCompressor create(Map<String, String> options)
        {
            return new PlainTestCompressor();
        }
    }

    // This test compressor overrides serializedAs(), it is compatible with SnappyCompressor
    public static class SerializedTestCompressor extends CustomTestCompressor
    {
        public static SerializedTestCompressor create(Map<String, String> options)
        {
            return new SerializedTestCompressor();
        }

        @Override
        public Class<? extends ICompressor> serializedAs()
        {
            return SnappyCompressor.class;
        }
    }

   // This test compressor overrides serializedAs(), this will create a compressor compatible with SnappyCompressor
    public static class MySnappyCompressor extends SnappyCompressor
    {
        public static MySnappyCompressor create(Map<String, String> options)
        {
            return new MySnappyCompressor();
        }

        @Override
        public Class<? extends ICompressor> serializedAs()
        {
            return SnappyCompressor.class;
        }
    }

    // This compressor also is compatible with Snappy but will produce data which is not compatible
    public static class IncompatibleSnappyCompressor extends SnappyCompressor
    {
        @Override
        public Class<? extends ICompressor> serializedAs()
        {
            return SnappyCompressor.class;
        }
        @Override
        public void compress(ByteBuffer src, ByteBuffer dest)
        {
            // Write raw uncompressed bytes instead of a valid Snappy stream.
            // Snappy will not be be able to decompress
            // This is what the smoke test's step 1 cross-check is designed to catch.
            dest.put(src);
        }
    }
}
