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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;

/**
 * ZSTD Compressor
 */
public class ZstdCompressor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdCompressor.class);

    // These might change with the version of Zstd we're using
    public static final int FAST_COMPRESSION_LEVEL = Zstd.minCompressionLevel();
    public static final int BEST_COMPRESSION_LEVEL = Zstd.maxCompressionLevel();

    // Compressor Defaults
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;
    private static final boolean ENABLE_CHECKSUM_FLAG = true;

    @VisibleForTesting
    public static final String COMPRESSION_LEVEL_OPTION_NAME = "compression_level";

    private static final ConcurrentHashMap<Integer, ZstdCompressor> instances = new ConcurrentHashMap<>();

    private final int compressionLevel;
    private final Set<Uses> recommendedUses;

    /**
     * Create a Zstd compressor with the given options
     *
     * @param options
     * @return
     */
    public static ZstdCompressor create(Map<String, String> options)
    {
        int level = getOrDefaultCompressionLevel(options);

        if (!isValid(level))
            throw new IllegalArgumentException(String.format("%s=%d is invalid", COMPRESSION_LEVEL_OPTION_NAME, level));

        return getOrCreate(level);
    }

    /**
     * Private constructor
     *
     * @param compressionLevel
     */
    private ZstdCompressor(int compressionLevel)
    {
        this.compressionLevel = compressionLevel;
        this.recommendedUses = ImmutableSet.of(Uses.GENERAL);
        logger.trace("Creating Zstd Compressor with compression level={}", compressionLevel);
    }

    /**
     * Get a cached instance or return a new one
     *
     * @param level
     * @return
     */
    public static ZstdCompressor getOrCreate(int level)
    {
        return instances.computeIfAbsent(level, l -> new ZstdCompressor(level));
    }

    /**
     * Get initial compressed buffer length
     *
     * @param chunkLength
     * @return
     */
    @Override
    public int initialCompressedBufferLength(int chunkLength)
    {
        return (int) Zstd.compressBound(chunkLength);
    }

    /**
     * Decompress data using arrays
     *
     * @param input
     * @param inputOffset
     * @param inputLength
     * @param output
     * @param outputOffset
     * @return
     * @throws IOException
     */
    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    throws IOException
    {
        long dsz = Zstd.decompressByteArray(output, outputOffset, output.length - outputOffset,
                                            input, inputOffset, inputLength);

        if (Zstd.isError(dsz))
            throw new IOException(String.format("Decompression failed due to %s", Zstd.getErrorName(dsz)));

        return (int) dsz;
    }

    /**
     * Decompress data via ByteBuffers
     *
     * @param input
     * @param output
     * @throws IOException
     */
    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Zstd.decompress(output, input);
        } catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }
    }

    /**
     * Compress using ByteBuffers
     *
     * @param input
     * @param output
     * @throws IOException
     */
    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Zstd.compress(output, input, compressionLevel, ENABLE_CHECKSUM_FLAG);
        } catch (Exception e)
        {
            throw new IOException("Compression failed", e);
        }
    }

    /**
     * Check if the given compression level is valid. This can be a negative value as well.
     *
     * @param level
     * @return
     */
    private static boolean isValid(int level)
    {
        return (level >= FAST_COMPRESSION_LEVEL && level <= BEST_COMPRESSION_LEVEL);
    }

    /**
     * Parse the compression options
     *
     * @param options
     * @return
     */
    private static int getOrDefaultCompressionLevel(Map<String, String> options)
    {
        if (options == null)
            return DEFAULT_COMPRESSION_LEVEL;

        String val = options.get(COMPRESSION_LEVEL_OPTION_NAME);

        if (val == null)
            return DEFAULT_COMPRESSION_LEVEL;

        return Integer.valueOf(val);
    }

    /**
     * Return the preferred BufferType
     *
     * @return
     */
    @Override
    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    /**
     * Check whether the given BufferType is supported
     *
     * @param bufferType
     * @return
     */
    @Override
    public boolean supports(BufferType bufferType)
    {
        return bufferType == BufferType.OFF_HEAP;
    }

    /**
     * Lists the supported options by this compressor
     *
     * @return
     */
    @Override
    public Set<String> supportedOptions()
    {
        return new HashSet<>(Collections.singletonList(COMPRESSION_LEVEL_OPTION_NAME));
    }


    @VisibleForTesting
    public int getCompressionLevel()
    {
        return compressionLevel;
    }

    @Override
    public Set<Uses> recommendedUses()
    {
        return recommendedUses;
    }
}
