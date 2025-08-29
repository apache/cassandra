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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import org.apache.cassandra.io.compress.ZstdCompressorBase;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;

public class ZstdCompressionDictionary implements CompressionDictionary, SelfRefCounted<ZstdCompressionDictionary>
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdCompressionDictionary.class);

    private final DictId dictId;
    private final byte[] rawDictionary;
    // One ZstdDictDecompress and multiple ZstdDictCompress (per level) can be derived from the same raw dictionary content
    private final ConcurrentHashMap<Integer, ZstdDictCompress> zstdDictCompressPerLevel = new ConcurrentHashMap<>();
    private volatile ZstdDictDecompress dictDecompress;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Ref<ZstdCompressionDictionary> selfRef;

    public ZstdCompressionDictionary(DictId dictId, byte[] rawDictionary)
    {
        this.dictId = dictId;
        this.rawDictionary = rawDictionary;
        this.selfRef = new Ref<>(this, new Tidy(zstdDictCompressPerLevel, dictDecompress));
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
     *
     * @param compressionLevel compression level to create the compression table
     * @return ZstdDictCompress
     */
    public ZstdDictCompress dictionaryForCompression(int compressionLevel)
    {
        if (closed.get())
            throw new IllegalStateException("Dictionary has been closed. " + dictId);

        ZstdCompressorBase.validateCompressionLevel(compressionLevel);

        return zstdDictCompressPerLevel.computeIfAbsent(compressionLevel, level -> {
            if (closed.get())
                throw new IllegalStateException("Dictionary has been closed");
            return new ZstdDictCompress(rawDictionary, level);
        });
    }

    /**
     * Get a pre-processed decompression tables that is optimized for decompression.
     * It is derived/computed from dictionary bytes.
     * The internal data structure is different from the tables for compression.
     *
     * @return ZstdDictDecompress
     */
    public ZstdDictDecompress dictionaryForDecompression()
    {
        if (closed.get())
            throw new IllegalStateException("Dictionary has been closed");

        ZstdDictDecompress result = dictDecompress;
        if (result != null)
            return result;

        synchronized (this)
        {
            if (closed.get())
                throw new IllegalStateException("Dictionary has been closed");

            result = dictDecompress;
            if (result == null)
            {
                result = new ZstdDictDecompress(rawDictionary);
                dictDecompress = result;
            }
            return result;
        }
    }

    @Override
    public Ref<ZstdCompressionDictionary> tryRef()
    {
        return selfRef.tryRef();
    }

    @Override
    public Ref<ZstdCompressionDictionary> selfRef()
    {
        return selfRef;
    }

    @Override
    public Ref<ZstdCompressionDictionary> ref()
    {
        return selfRef.ref();
    }

    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true))
        {
            selfRef.release();
        }
    }

    private static class Tidy implements RefCounted.Tidy
    {
        private final ConcurrentHashMap<Integer, ZstdDictCompress> zstdDictCompressPerLevel;
        private volatile ZstdDictDecompress dictDecompress;

        Tidy(ConcurrentHashMap<Integer, ZstdDictCompress> zstdDictCompressPerLevel, ZstdDictDecompress dictDecompress)
        {
            this.zstdDictCompressPerLevel = zstdDictCompressPerLevel;
            this.dictDecompress = dictDecompress;
        }

        @Override
        public void tidy()
        {
            // Close all compression dictionaries
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
            ZstdDictDecompress decompressDict = dictDecompress;
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
                dictDecompress = null;
            }
        }

        @Override
        public String name()
        {
            return ZstdCompressionDictionary.class.getSimpleName();
        }
    }
}
