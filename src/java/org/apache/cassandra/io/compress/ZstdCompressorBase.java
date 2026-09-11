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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZstdCompressorBase implements ICompressor
{
    // These might change with the version of Zstd we're using
    public static final int FAST_COMPRESSION_LEVEL = Zstd.minCompressionLevel();
    public static final int BEST_COMPRESSION_LEVEL = Zstd.maxCompressionLevel();

    // Compressor Defaults
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;
    public static final boolean ENABLE_CHECKSUM_FLAG = true;

    // Compressor option names
    public static final String COMPRESSION_LEVEL_OPTION_NAME = "compression_level";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final int compressionLevel;
    private final Set<ICompressor.Uses> recommendedUses;
    private final Set<String> supportedOptions;

    // Reusable native (de)compression contexts, pooled and borrowed per chunk.
    //
    // The static Zstd.compress / Zstd.decompress / Zstd.decompressByteArray helpers each allocate and free a fresh
    // native ZSTD_CCtx / ZSTD_DCtx on EVERY call - i.e. once per chunk. At small chunk sizes that fixed per-chunk
    // cost dominates, and it scales with chunk count rather than with data size. We keep a pool of contexts
    // instead, each configured once, and reuse them across chunks.
    private final Queue<ZstdCompressCtx> compressCtxPool = new ConcurrentLinkedQueue<>();
    private final Queue<ZstdDecompressCtx> decompressCtxPool = new ConcurrentLinkedQueue<>();

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
        ZstdDecompressCtx ctx = acquireDecompressCtx();
        boolean ok = false;
        try
        {
            int dsz = ctx.decompressByteArray(output, outputOffset, output.length - outputOffset,
                                              input, inputOffset, inputLength);
            if (Zstd.isError(dsz))
                throw new IOException("Decompression failed due to " + Zstd.getErrorName(dsz));
            ok = true;
            return dsz;
        }
        catch (IOException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }
        finally
        {
            // success -> return to pool; failure -> close, so a context that errored is never reused
            if (ok)
                decompressCtxPool.offer(ctx);
            else
                ctx.close();
        }
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
        ZstdDecompressCtx ctx = acquireDecompressCtx();
        boolean ok = false;
        try
        {
            // Zstd compressors expect only direct bytebuffer. See preferredBufferType and supports.
            int decompressedSize = ctx.decompressDirectByteBuffer(output, output.position(), output.limit() - output.position(),
                                                                  input, input.position(), input.limit() - input.position());
            output.position(output.position() + decompressedSize);
            input.position(input.limit());
            ok = true;
        }
        catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }
        finally
        {
            if (ok)
                decompressCtxPool.offer(ctx);
            else
                ctx.close();
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
        ZstdCompressCtx ctx = acquireCompressCtx();
        boolean ok = false;
        try
        {
            // Zstd compressors expect only direct bytebuffer. See preferredBufferType and supports.
            int compressedSize = ctx.compressDirectByteBuffer(output, output.position(), output.limit() - output.position(),
                                                              input, input.position(), input.limit() - input.position());
            output.position(output.position() + compressedSize);
            input.position(input.limit());
            ok = true;
        }
        catch (Exception e)
        {
            throw new IOException("Compression failed", e);
        }
        finally
        {
            if (ok)
                compressCtxPool.offer(ctx);
            else
                ctx.close();
        }
    }

    // ---- pooled native context management (see the compressCtxPool field comment) ----

    private ZstdCompressCtx acquireCompressCtx()
    {
        ZstdCompressCtx ctx = compressCtxPool.poll();
        if (ctx == null)
        {
            // Level and checksum are set once here, matching what the static Zstd.compress helper did per call, so
            // the frames written are identical to those written before contexts were pooled.
            ctx = new ZstdCompressCtx().setLevel(compressionLevel()).setChecksum(ENABLE_CHECKSUM_FLAG);
        }
        return ctx;
    }

    private ZstdDecompressCtx acquireDecompressCtx()
    {
        ZstdDecompressCtx ctx = decompressCtxPool.poll();
        return ctx == null ? new ZstdDecompressCtx() : ctx;
    }

    @VisibleForTesting
    int pooledCompressContexts()
    {
        return compressCtxPool.size();
    }

    @VisibleForTesting
    int pooledDecompressContexts()
    {
        return decompressCtxPool.size();
    }

    /**
     * Check if the given compression level is valid. This can be a negative value as well.
     *
     * @param level compression level
     */
    public static void validateCompressionLevel(int level)
    {
        if (level < FAST_COMPRESSION_LEVEL || level > BEST_COMPRESSION_LEVEL)
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
