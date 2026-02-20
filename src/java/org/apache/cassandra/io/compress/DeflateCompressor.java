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
package org.apache.cassandra.io.compress;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.schema.CompressionParams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.google.common.collect.ImmutableSet;

public class DeflateCompressor implements ICompressor
{
    // Compression level constants for Deflate (zlib)
    public static final int NO_COMPRESSION = 0;
    public static final int BEST_SPEED = 1;
    public static final int BEST_COMPRESSION = 9;
    public static final int DEFAULT_COMPRESSION_LEVEL = Deflater.DEFAULT_COMPRESSION; // -1 (equivalent to level 6)

    public static final String COMPRESSION_LEVEL_OPTION_NAME = "compression_level";

    // Cache of compressor instances by compression level
    private static final ConcurrentHashMap<Integer, DeflateCompressor> instances = new ConcurrentHashMap<>();

    // Legacy singleton instance for backward compatibility (uses default compression level)
    public static final DeflateCompressor instance = new DeflateCompressor(DEFAULT_COMPRESSION_LEVEL);

    private static final FastThreadLocal<byte[]> threadLocalScratchBuffer = new FastThreadLocal<byte[]>()
    {
        @Override
        protected byte[] initialValue()
        {
            return new byte[CompressionParams.DEFAULT_CHUNK_LENGTH];
        }
    };

    public static byte[] getThreadLocalScratchBuffer()
    {
        return threadLocalScratchBuffer.get();
    }

    private final int compressionLevel;
    private final FastThreadLocal<Deflater> deflater;
    private final FastThreadLocal<Inflater> inflater;
    private final Set<Uses> recommendedUses;

    /**
     * Create a Deflate compressor with the given options
     * Invoked by {@link org.apache.cassandra.schema.CompressionParams#createCompressor} via reflection
     *
     * @param compressionOptions compression options
     * @return DeflateCompressor
     */
    public static DeflateCompressor create(Map<String, String> compressionOptions)
    {
        int level = getOrDefaultCompressionLevel(compressionOptions);
        validateCompressionLevel(level);
        return getOrCreate(level);
    }

    /**
     * Get a cached instance or create a new one
     *
     * @param level compression level
     * @return cached or new DeflateCompressor instance
     */
    public static DeflateCompressor getOrCreate(int level)
    {
        return instances.computeIfAbsent(level, DeflateCompressor::new);
    }

    /**
     * Private constructor with compression level
     *
     * @param compressionLevel the compression level to use (0-9, or -1 for default)
     */
    private DeflateCompressor(int compressionLevel)
    {
        this.compressionLevel = compressionLevel;
        deflater = new FastThreadLocal<Deflater>()
        {
            @Override
            protected Deflater initialValue()
            {
                return new Deflater(DeflateCompressor.this.compressionLevel);
            }
        };
        inflater = new FastThreadLocal<Inflater>()
        {
            @Override
            protected Inflater initialValue()
            {
                return new Inflater();
            }
        };
        recommendedUses = ImmutableSet.of(Uses.GENERAL);
    }

    /**
     * Get the compression level for this compressor
     *
     * @return compression level
     */
    public int compressionLevel()
    {
        return compressionLevel;
    }

    /**
     * Validate the compression level
     *
     * @param level compression level to validate
     * @throws IllegalArgumentException if level is invalid
     */
    public static void validateCompressionLevel(int level)
    {
        if (level != DEFAULT_COMPRESSION_LEVEL && (level < NO_COMPRESSION || level > BEST_COMPRESSION))
        {
            throw new IllegalArgumentException(String.format("%s=%d is invalid. Must be -1 (default) or 0-9",
                                                            COMPRESSION_LEVEL_OPTION_NAME, level));
        }
    }

    /**
     * Get the supplied compression level from options; otherwise, use the default
     *
     * @param options compression options
     * @return compression level
     */
    public static int getOrDefaultCompressionLevel(Map<String, String> options)
    {
        if (options == null || !options.containsKey(COMPRESSION_LEVEL_OPTION_NAME))
            return DEFAULT_COMPRESSION_LEVEL;

        String val = options.get(COMPRESSION_LEVEL_OPTION_NAME);
        try
        {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException(String.format("Invalid value for %s: %s",
                                                            COMPRESSION_LEVEL_OPTION_NAME, val), e);
        }
    }

    public Set<String> supportedOptions()
    {
        return new HashSet<>(Arrays.asList(COMPRESSION_LEVEL_OPTION_NAME));
    }

    public int initialCompressedBufferLength(int sourceLen)
    {
        // Taken from zlib deflateBound(). See http://www.zlib.net/zlib_tech.html.
        return sourceLen + (sourceLen >> 12) + (sourceLen >> 14) + (sourceLen >> 25) + 13;
    }

    public void compress(ByteBuffer input, ByteBuffer output)
    {
        if (input.hasArray() && output.hasArray())
        {
            int length = compressArray(input.array(), input.arrayOffset() + input.position(), input.remaining(),
                                       output.array(), output.arrayOffset() + output.position(), output.remaining());
            input.position(input.limit());
            output.position(output.position() + length);
        }
        else
            compressBuffer(input, output);
    }

    public int compressArray(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        Deflater def = deflater.get();
        def.reset();
        def.setInput(input, inputOffset, inputLength);
        def.finish();
        if (def.needsInput())
            return 0;

        int len = def.deflate(output, outputOffset, maxOutputLength);
        assert def.finished();
        return len;
    }

    public void compressBuffer(ByteBuffer input, ByteBuffer output)
    {
        Deflater def = deflater.get();
        def.reset();

        byte[] buffer = getThreadLocalScratchBuffer();
        // Use half the buffer for input, half for output.
        int chunkLen = buffer.length / 2;
        while (input.remaining() > chunkLen)
        {
            input.get(buffer, 0, chunkLen);
            def.setInput(buffer, 0, chunkLen);
            while (!def.needsInput())
            {
                int len = def.deflate(buffer, chunkLen, chunkLen);
                output.put(buffer, chunkLen, len);
            }
        }
        int inputLength = input.remaining();
        input.get(buffer, 0, inputLength);
        def.setInput(buffer, 0, inputLength);
        def.finish();
        while (!def.finished())
        {
            int len = def.deflate(buffer, chunkLen, chunkLen);
            output.put(buffer, chunkLen, len);
        }
    }


    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (input.hasArray() && output.hasArray())
        {
            int length = uncompress(input.array(), input.arrayOffset() + input.position(), input.remaining(),
                                    output.array(), output.arrayOffset() + output.position(), output.remaining());
            input.position(input.limit());
            output.position(output.position() + length);
        }
        else
            uncompressBuffer(input, output);
    }

    public void uncompressBuffer(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Inflater inf = inflater.get();
            inf.reset();

            byte[] buffer = getThreadLocalScratchBuffer();
            // Use half the buffer for input, half for output.
            int chunkLen = buffer.length / 2;
            while (input.remaining() > chunkLen)
            {
                input.get(buffer, 0, chunkLen);
                inf.setInput(buffer, 0, chunkLen);
                while (!inf.needsInput())
                {
                    int len = inf.inflate(buffer, chunkLen, chunkLen);
                    output.put(buffer, chunkLen, len);
                }
            }
            int inputLength = input.remaining();
            input.get(buffer, 0, inputLength);
            inf.setInput(buffer, 0, inputLength);
            while (!inf.needsInput())
            {
                int len = inf.inflate(buffer, chunkLen, chunkLen);
                output.put(buffer, chunkLen, len);
            }
        }
        catch (DataFormatException e)
        {
            throw new IOException(e);
        }
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        return uncompress(input, inputOffset, inputLength, output, outputOffset, output.length - outputOffset);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength) throws IOException
    {
        Inflater inf = inflater.get();
        inf.reset();
        inf.setInput(input, inputOffset, inputLength);
        if (inf.needsInput())
            return 0;

        // We assume output is big enough
        try
        {
            return inf.inflate(output, outputOffset, maxOutputLength);
        }
        catch (DataFormatException e)
        {
            throw new IOException(e);
        }
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public BufferType preferredBufferType()
    {
        // Prefer array-backed buffers.
        return BufferType.ON_HEAP;
    }

    @Override
    public Set<Uses> recommendedUses()
    {
        return recommendedUses;
    }
}
