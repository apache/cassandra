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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.luben.zstd.Zstd;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.utils.concurrent.Ref;

import javax.annotation.Nullable;

public class ZstdDictionaryCompressor extends ZstdCompressorBase implements IDictionaryCompressor<ZstdCompressionDictionary>
{
    private static final ConcurrentHashMap<Integer, ZstdDictionaryCompressor> instancesPerLevel = new ConcurrentHashMap<>();
    private static final Cache<ZstdCompressionDictionary, ZstdDictionaryCompressor> instancePerDict =
    Caffeine.newBuilder()
            .maximumSize(DatabaseDescriptor.getCompressionDictionaryCacheSize())
            .expireAfterAccess(Duration.ofSeconds(DatabaseDescriptor.getCompressionDictionaryCacheExpireSeconds()))
            .removalListener((ZstdCompressionDictionary dictionary,
                              ZstdDictionaryCompressor compressor,
                              RemovalCause cause) -> {
                // Release dictionary reference when compressor is evicted from cache
                if (compressor != null && compressor.dictionaryRef != null)
                {
                    compressor.dictionaryRef.release();
                }
            })
            .build();

    // dictioanry and its ref are null, when they are absent.
    // In this case, the compressor falls back to be the same as ZstdCompressor
    @Nullable
    private final ZstdCompressionDictionary dictionary;
    @Nullable
    private final Ref<ZstdCompressionDictionary> dictionaryRef;

    /**
     * Create a ZstdDictionaryCompressor with the given options
     * Invoked by {@link org.apache.cassandra.schema.CompressionParams#createCompressor} via reflection
     *
     * @param options compression options
     * @return ZstdDictionaryCompressor
     */
    public static ZstdDictionaryCompressor create(Map<String, String> options)
    {
        int level = getOrDefaultCompressionLevel(options);
        validateCompressionLevel(level);
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
            Ref<ZstdCompressionDictionary> ref = dict != null ? dict.tryRef() : null;
            if (ref == null && dict != null)
            {
                // Dictionary is being closed, cannot create compressor
                throw new IllegalStateException("Dictionary is being closed");
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
        super(level, Set.of(COMPRESSION_LEVEL_OPTION_NAME));
        this.dictionary = dictionary;
        this.dictionaryRef = dictionaryRef;
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

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        // fallback to non-dict zstd compressor
        if (dictionary == null)
        {
            return super.uncompress(input, inputOffset, inputLength, output, outputOffset);
        }

        int dsz;
        try
        {
            dsz = (int) Zstd.decompressFastDict(output, outputOffset,
                                                input, inputOffset, inputLength,
                                                dictionary.dictionaryForDecompression());
        }
        catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }

        if (Zstd.isError(dsz))
            throw new IOException("Decompression failed due to " + Zstd.getErrorName(dsz));

        return dsz;
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (dictionary == null)
        {
            super.uncompress(input, output);
            return;
        }

        try
        {
            // Zstd compressors expect only direct bytebuffer. See ZstdCompressorBase.preferredBufferType and supports
            int decompressedSize = (int) Zstd.decompressDirectByteBufferFastDict(output, output.position(), output.limit() - output.position(),
                                                                                 input, input.position(), input.limit() - input.position(),
                                                                                 dictionary.dictionaryForDecompression());
            output.position(output.position() + decompressedSize);
            input.position(input.limit());
        }
        catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
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

        try
        {
            // Zstd compressors expect only direct bytebuffer. See ZstdCompressorBase.preferredBufferType and supports
            int compressedSize = (int) Zstd.compressDirectByteBufferFastDict(output, output.position(), output.limit() - output.position(),
                                                                             input, input.position(), input.limit() - input.position(),
                                                                             dictionary.dictionaryForCompression(compressionLevel()));
            output.position(output.position() + compressedSize);
            input.position(input.limit());
        }
        catch (Exception e)
        {
            throw new IOException("Compression failed", e);
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
    }
}
