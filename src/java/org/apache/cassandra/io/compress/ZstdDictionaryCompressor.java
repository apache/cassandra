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
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.CompressionDictionaryTrainingConfig;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.utils.concurrent.Ref;

public class ZstdDictionaryCompressor extends ZstdCompressorBase implements ICompressor, IDictionaryCompressor<ZstdCompressionDictionary>
{
    private static final ConcurrentHashMap<Integer, ZstdDictionaryCompressor> instancesPerLevel = new ConcurrentHashMap<>();
    private static final Cache<ZstdCompressionDictionary, ZstdDictionaryCompressor> instancePerDict =
    Caffeine.newBuilder()
            .maximumSize(DatabaseDescriptor.getCompressionDictionaryCacheSize())
            .expireAfterAccess(Duration.ofSeconds(DatabaseDescriptor.getCompressionDictionaryCacheExpireSeconds()))
            .removalListener((ZstdCompressionDictionary dictionary,
                              ZstdDictionaryCompressor compressor,
                              RemovalCause cause) -> {
                if (compressor != null)
                {
                    // Close pooled native (de)compression contexts BEFORE dropping the dictionary reference they
                    // were loaded with, so no context outlives the dictionary's native memory.
                    compressor.releaseNativeContexts();
                    // Release dictionary reference when compressor is evicted from cache
                    if (compressor.dictionaryRef != null)
                        compressor.dictionaryRef.release();
                }
            })
            .executor(ImmediateExecutor.INSTANCE)
            .build();

    // dictionary and its ref are null, when they are absent.
    // In this case, the compressor falls back to be the same as ZstdCompressor
    @Nullable
    private final ZstdCompressionDictionary dictionary;
    @Nullable
    private final Ref<ZstdCompressionDictionary> dictionaryRef;

    @Nullable
    private final ZstdDictCompress zstdDictCompress;
    @Nullable
    private final ZstdDictDecompress zstdDictDecompress;

    // Reusable native (de)compression contexts, pooled and borrowed per chunk.
    //
    // The static Zstd.compressDirectByteBufferFastDict / decompressDirectByteBufferFastDict helpers allocate and
    // free a fresh native ZSTD_CCtx / ZSTD_DCtx on EVERY call — i.e. once per chunk. At small chunk sizes that
    // fixed per-chunk cost dominates: profiling a 4KiB-chunk dictionary flush showed ~6.5% of node CPU in
    // ZSTD_createCCtx alone (plus the allocator/scheduler churn it drives), scaling with chunk count. We instead
    // keep a pool of contexts, each with the (immutable) dictionary loaded once, and reuse them across chunks.
    // A context is borrowed for a single (de)compress call, since ZstdCompressCtx / ZstdDecompressCtx are NOT
    // thread-safe and compress()/uncompress() may run on many threads concurrently. Pooled contexts are closed
    // when this compressor is evicted from its cache (releaseNativeContexts()).
    private final Queue<ZstdCompressCtx> compressCtxPool = new ConcurrentLinkedQueue<>();
    private final Queue<ZstdDecompressCtx> decompressCtxPool = new ConcurrentLinkedQueue<>();
    private volatile boolean released = false;

    /**
     * Create a ZstdDictionaryCompressor with the given options
     * Invoked by {@link org.apache.cassandra.schema.CompressionParams#createCompressor(ParameterizedClass)} via reflection
     *
     * @param options compression options
     * @return ZstdDictionaryCompressor
     */
    public static ZstdDictionaryCompressor create(Map<String, String> options)
    {
        int level = getOrDefaultCompressionLevel(options);
        validateCompressionLevel(level);
        // pass it through to validate
        CompressionDictionaryTrainingConfig.getMaxDictionarySize(options);
        CompressionDictionaryTrainingConfig.getMaxTotalSampleSize(options);
        CompressionDictionaryTrainingConfig.getMinTrainingFrequency(options);
        return getOrCreate(level, null);
    }

    // Constructor used to create the compressor for reading the sstable; the compression level is not relevant
    public static ZstdDictionaryCompressor create(ZstdCompressionDictionary dictionary)
    {
        return getOrCreate(DEFAULT_COMPRESSION_LEVEL, dictionary);
    }

    private static ZstdDictionaryCompressor getOrCreate(int level, ZstdCompressionDictionary dictionary)
    {
        if (dictionary == null)
        {
            return instancesPerLevel.computeIfAbsent(level, ZstdDictionaryCompressor::new);
        }

        return instancePerDict.get(dictionary, dict -> {
            // Get a reference to the dictionary when creating new compressor
            Ref<ZstdCompressionDictionary> ref = dict.tryRef();
            if (ref == null)
            {
                throw new IllegalStateException("Dictionary is released");
            }
            return new ZstdDictionaryCompressor(level, dictionary, ref);
        });
    }

    private ZstdDictionaryCompressor(int level)
    {
        this(level, null, null);
    }

    private ZstdDictionaryCompressor(int level, ZstdCompressionDictionary dictionary, Ref<ZstdCompressionDictionary> dictionaryRef)
    {
        super(level, Set.of(COMPRESSION_LEVEL_OPTION_NAME,
                            TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME,
                            TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME,
                            TRAINING_MIN_FREQUENCY_PARAMETER_NAME));
        this.dictionary = dictionary;
        this.dictionaryRef = dictionaryRef;

        zstdDictCompress = dictionary == null ? null : dictionary.dictionaryForCompression(level);
        zstdDictDecompress = dictionary == null ? null: dictionary.dictionaryForDecompression();
    }

    @Override
    public ZstdDictionaryCompressor getOrCopyWithDictionary(ZstdCompressionDictionary compressionDictionary)
    {
        return getOrCreate(compressionLevel(), compressionDictionary);
    }

    @Override
    public Kind acceptableDictionaryKind()
    {
        return Kind.ZSTD;
    }

    // ---- pooled native context management (see the compressCtxPool field comment) ----

    private ZstdCompressCtx acquireCompressCtx()
    {
        ZstdCompressCtx ctx = compressCtxPool.poll();
        if (ctx == null)
        {
            // The compression level is carried by the precomputed CDict, so loadDict is all that is required.
            ctx = new ZstdCompressCtx().loadDict(zstdDictCompress);
        }
        return ctx;
    }

    private void releaseCompressCtx(ZstdCompressCtx ctx)
    {
        compressCtxPool.offer(ctx);
        if (released) // evicted concurrently — ensure the just-returned context is not leaked
            drainCompressPool();
    }

    private ZstdDecompressCtx acquireDecompressCtx()
    {
        ZstdDecompressCtx ctx = decompressCtxPool.poll();
        if (ctx == null)
            ctx = new ZstdDecompressCtx().loadDict(zstdDictDecompress);
        return ctx;
    }

    private void releaseDecompressCtx(ZstdDecompressCtx ctx)
    {
        decompressCtxPool.offer(ctx);
        if (released)
            drainDecompressPool();
    }

    // Close every pooled context. Called on cache eviction; a context still borrowed by an in-flight call is
    // closed when it is returned, because the borrower re-checks 'released' in release*Ctx().
    private void releaseNativeContexts()
    {
        released = true;
        drainCompressPool();
        drainDecompressPool();
    }

    private void drainCompressPool()
    {
        ZstdCompressCtx ctx;
        while ((ctx = compressCtxPool.poll()) != null)
            ctx.close();
    }

    private void drainDecompressPool()
    {
        ZstdDecompressCtx ctx;
        while ((ctx = decompressCtxPool.poll()) != null)
            ctx.close();
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        // fallback to non-dict zstd compressor
        if (dictionary == null)
        {
            return super.uncompress(input, inputOffset, inputLength, output, outputOffset);
        }

        ZstdDecompressCtx ctx = acquireDecompressCtx();
        boolean ok = false;
        try
        {
            // Reuse a pooled context (dictionary loaded once) rather than the static one-shot, which allocates a
            // fresh ZSTD_DCtx per call. See the compressCtxPool field comment.
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
                releaseDecompressCtx(ctx);
            else
                ctx.close();
        }
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (dictionary == null)
        {
            super.uncompress(input, output);
            return;
        }

        ZstdDecompressCtx ctx = acquireDecompressCtx();
        boolean ok = false;
        try
        {
            // Zstd compressors expect only direct bytebuffer. See ZstdCompressorBase.preferredBufferType and supports.
            // The context carries the dictionary (loaded once) and is reused across chunks — see the pool comment.
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
            // success -> return to pool; failure -> close, so a context that errored is never reused
            if (ok)
                releaseDecompressCtx(ctx);
            else
                ctx.close();
        }
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (dictionary == null)
        {
            super.compress(input, output);
            return;
        }

        ZstdCompressCtx ctx = acquireCompressCtx();
        boolean ok = false;
        try
        {
            // Zstd compressors expect only direct bytebuffer. See ZstdCompressorBase.preferredBufferType and supports.
            // The context carries the dictionary (loaded once) and is reused across chunks — see the pool comment.
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
            // success -> return to pool; failure -> close, so a context that errored is never reused
            if (ok)
                releaseCompressCtx(ctx);
            else
                ctx.close();
        }
    }

    @VisibleForTesting
    ZstdCompressionDictionary dictionary()
    {
        return dictionary;
    }

    @VisibleForTesting
    public static void invalidateCache()
    {
        instancePerDict.invalidateAll();
        instancePerDict.cleanUp();
    }
}
