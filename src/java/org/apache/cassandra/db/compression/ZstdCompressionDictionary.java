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

package org.apache.cassandra.db.compression;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.ZstdCompressorBase;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;

public class ZstdCompressionDictionary implements CompressionDictionary, SelfRefCounted<ZstdCompressionDictionary>
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdCompressionDictionary.class);

    private final DictId dictId;
    private final byte[] rawDictionary;
    private final int checksum;
    // One ZstdDictDecompress and multiple ZstdDictCompress (per level) can be derived from the same raw dictionary content
    private final ConcurrentHashMap<Integer, ZstdDictCompress> zstdDictCompressPerLevel = new ConcurrentHashMap<>();
    private final AtomicReference<ZstdDictDecompress> dictDecompress = new AtomicReference<>();
    private volatile Ref<ZstdCompressionDictionary> selfRef;
    private final Instant createdAt;

    @VisibleForTesting
    public ZstdCompressionDictionary(DictId dictId, byte[] rawDictionary)
    {
        this(dictId,
             rawDictionary,
             CompressionDictionary.calculateChecksum((byte) dictId.kind.ordinal(), dictId.id, rawDictionary),
             FBUtilities.now());
    }

    public ZstdCompressionDictionary(DictId dictId, byte[] rawDictionary, int checksum, Instant createdAt)
    {
        this.dictId = dictId;
        this.rawDictionary = rawDictionary;
        this.checksum = checksum;
        this.selfRef = null;
        this.createdAt = createdAt;
    }

    @Override
    public DictId dictId()
    {
        return dictId;
    }

    @Override
    public Kind kind()
    {
        return Kind.ZSTD;
    }

    @Override
    public byte[] rawDictionary()
    {
        return rawDictionary;
    }

    @Override
    public int checksum()
    {
        return checksum;
    }

    @Override
    public Instant createdAt()
    {
        return createdAt;
    }

    @Override
    public int estimatedOccupiedMemoryBytes()
    {
        int occupied = rawDictionary.length;
        occupied += dictDecompress.get() != null ? rawDictionary.length : 0;
        occupied += zstdDictCompressPerLevel.size() * rawDictionary.length;

        return occupied;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ZstdCompressionDictionary)) return false;
        ZstdCompressionDictionary that = (ZstdCompressionDictionary) o;
        return Objects.equals(dictId, that.dictId);
    }

    @Override
    public int hashCode()
    {
        return dictId.hashCode();
    }

    /**
     * Get a pre-processed compression tables that is optimized for compression.
     * It is derived/computed from dictionary bytes.
     * The internal data structure is different from the tables for decompression.
     * <br>
     * IMPORTANT: Caller MUST hold a valid reference (via tryRef/ref) to this dictionary.
     * The reference counting mechanism ensures tidy() cannot run while references exist,
     * making synchronization unnecessary. This method is safe to call concurrently as long
     * as each caller holds a reference.
     * <br>
     * @param compressionLevel compression level to create the compression table
     * @return ZstdDictCompress for the specified compression level
     * @throws IllegalStateException if called without holding a valid reference
     */
    public ZstdDictCompress dictionaryForCompression(int compressionLevel)
    {
        ensureNotReleased();
        ZstdCompressorBase.validateCompressionLevel(compressionLevel);

        // Fast path: check if already exists to avoid locking the bin
        ZstdDictCompress existing = zstdDictCompressPerLevel.get(compressionLevel);
        if (existing != null)
            return existing;

        // A little slow path: create new dictionary for this compression level
        // No additional synchronization needed - reference counting prevents tidy() while in use
        return zstdDictCompressPerLevel.computeIfAbsent(compressionLevel, level ->
            new ZstdDictCompress(rawDictionary, level));
    }

    /**
     * Get a pre-processed decompression tables that is optimized for decompression.
     * It is derived/computed from dictionary bytes.
     * The internal data structure is different from the tables for compression.
     * <br>
     * IMPORTANT: Caller MUST hold a valid reference (via tryRef/ref) to this dictionary.
     * The reference counting mechanism ensures tidy() cannot run while references exist,
     * making synchronization unnecessary. This method is safe to call concurrently as long
     * as each caller holds a reference.
     * <br>
     * Thread-safe: Multiple threads can safely call this method concurrently.
     * The decompression dictionary will be created exactly once on first access.
     *
     * @return ZstdDictDecompress for decompression operations
     * @throws IllegalStateException if called without holding a valid reference
     */
    public ZstdDictDecompress dictionaryForDecompression()
    {
        ensureNotReleased();
        // Fast path: if already initialized, return immediately
        ZstdDictDecompress result = dictDecompress.get();
        if (result != null)
            return result;

        // Slow path: need to initialize with proper double-checked locking
        // Reference counting guarantees tidy() won't run during this operation
        synchronized (this)
        {
            result = dictDecompress.get();
            if (result == null)
            {
                result = new ZstdDictDecompress(rawDictionary);
                dictDecompress.set(result);
            }
            return result;
        }
    }

    @Override
    public Ref<ZstdCompressionDictionary> tryRef()
    {
        return initRefLazily().tryRef();
    }

    @Override
    public Ref<ZstdCompressionDictionary> selfRef()
    {
        return selfRef;
    }

    @Override
    public Ref<ZstdCompressionDictionary> ref()
    {
        return initRefLazily().ref();
    }

    @Override
    public Ref<ZstdCompressionDictionary> initRefLazily()
    {
        if (selfRef == null)
        {
            synchronized (this)
            {
                if (selfRef == null)
                {
                    selfRef = new Ref<>(this, new Tidy(zstdDictCompressPerLevel, dictDecompress));
                }
            }
        }
        return selfRef;
    }

    private void ensureNotReleased()
    {
        if (selfRef == null)
            throw new IllegalStateException("Dictionary ref is not initialized. " +
                                            "Call initRefLazily() or tryRef() first: " + dictId);

        if (selfRef.globalCount() <= 0)
            throw new IllegalStateException("Dictionary has been released: " + dictId);
    }

    /**
     * Tidy implementation for cleaning up native Zstd resources.
     *
     * This class holds direct references to the resources that need cleanup,
     * avoiding a circular reference pattern where Tidy would hold a reference
     * to the parent dictionary object.
     */
    private static class Tidy implements RefCounted.Tidy
    {
        private final ConcurrentHashMap<Integer, ZstdDictCompress> zstdDictCompressPerLevel;
        private final AtomicReference<ZstdDictDecompress> dictDecompress;

        Tidy(ConcurrentHashMap<Integer, ZstdDictCompress> zstdDictCompressPerLevel,
             AtomicReference<ZstdDictDecompress> dictDecompress)
        {
            this.zstdDictCompressPerLevel = zstdDictCompressPerLevel;
            this.dictDecompress = dictDecompress;
        }

        /**
         * Clean up native resources when reference count reaches zero.
         *
         * IMPORTANT: This method is called exactly once when the last reference is released.
         * Reference counting guarantees that no other thread can be executing
         * dictionaryForCompression/Decompression when this runs, because:
         * 1. Those methods require holding a valid reference
         * 2. This only runs when refcount goes from 0 to -1
         * 3. Once refcount is negative, tryRef() returns null, preventing new references
         *
         * Therefore, no synchronization is needed - we have exclusive access to clean up.
         */
        @Override
        public void tidy()
        {
            // Close all compression dictionaries
            // No synchronization needed - reference counting ensures exclusive access
            for (ZstdDictCompress compressDict : zstdDictCompressPerLevel.values())
            {
                try
                {
                    compressDict.close();
                }
                catch (Exception e)
                {
                    // Log but don't fail - continue closing other resources
                    logger.warn("Failed to close ZstdDictCompress", e);
                }
            }
            zstdDictCompressPerLevel.clear();

            // Close decompression dictionary
            ZstdDictDecompress decompressDict = dictDecompress.get();
            if (decompressDict != null)
            {
                try
                {
                    decompressDict.close();
                }
                catch (Exception e)
                {
                    logger.warn("Failed to close ZstdDictDecompress", e);
                }
                dictDecompress.set(null);
            }
        }

        @Override
        public String name()
        {
            return ZstdCompressionDictionary.class.getSimpleName();
        }
    }
}
