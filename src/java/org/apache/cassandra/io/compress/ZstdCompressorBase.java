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
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;

public abstract class ZstdCompressorBase implements ICompressor
{
    // These might change with the version of Zstd we're using
    public static final int FAST_COMPRESSION_LEVEL = Zstd.minCompressionLevel();

    // Compressor Defaults
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;
    public static final boolean ENABLE_CHECKSUM_FLAG = true;

    // Compressor option names
    public static final String COMPRESSION_LEVEL_OPTION_NAME = "compression_level";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final int compressionLevel;
    private final Set<ICompressor.Uses> recommendedUses;
    private final Set<String> supportedOptions;

    protected ZstdCompressorBase(int compressionLevel, Set<String> supportedOptions)
    {
        this.compressionLevel = compressionLevel;
        this.supportedOptions = Collections.unmodifiableSet(supportedOptions);
        this.recommendedUses = Set.of(ICompressor.Uses.GENERAL);
        logger.trace("Creating Zstd Compressor with compression level={}", compressionLevel);
    }

    @Override
    public int initialCompressedBufferLength(int chunkLength)
    {
        return (int) Zstd.compressBound(chunkLength);
    }

    @Override
    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    @Override
    public boolean supports(BufferType bufferType)
    {
        return bufferType == BufferType.OFF_HEAP;
    }

    @Override
    public Set<Uses> recommendedUses()
    {
        return recommendedUses;
    }

    @VisibleForTesting
    public int compressionLevel()
    {
        return compressionLevel;
    }

    @Override
    public Set<String> supportedOptions()
    {
        return supportedOptions;
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
        long dsz;
        try
        {
            dsz = Zstd.decompressByteArray(output, outputOffset, output.length - outputOffset,
                                           input, inputOffset, inputLength);
        }
        catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }

        if (Zstd.isError(dsz))
            throw new IOException("Decompression failed due to " + Zstd.getErrorName(dsz));

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
            Zstd.compress(output, input, compressionLevel(), ENABLE_CHECKSUM_FLAG);
        } catch (Exception e)
        {
            throw new IOException("Compression failed", e);
        }
    }

    /**
     * Check if the given compression level is valid. This can be a negative value as well.
     *
     * @param level compression level
     */
    public static void validateCompressionLevel(int level, int bestCompressionLevel)
    {
        if (level < FAST_COMPRESSION_LEVEL || level > bestCompressionLevel)
        {
            throw new IllegalArgumentException(String.format("%s=%d is invalid", COMPRESSION_LEVEL_OPTION_NAME, level));
        }
    }

    /**
     * Get the supplied compression level; otherwise, use the default
     *
     * @param options compression options
     * @return compression level
     */
    public static int getOrDefaultCompressionLevel(Map<String, String> options)
    {
        if (options == null)
            return DEFAULT_COMPRESSION_LEVEL;

        String val = options.get(COMPRESSION_LEVEL_OPTION_NAME);

        if (val == null)
            return DEFAULT_COMPRESSION_LEVEL;

        return Integer.parseInt(val);
    }
}
