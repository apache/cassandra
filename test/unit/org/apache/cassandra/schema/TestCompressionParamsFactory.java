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

import java.util.Collections;


/**
 * Contains simple constructors for various Compression implementations.
 * They are a little inconsistent in their choice of parameters -- this is done on purpose to test out various compression parameter combinations.
 */
public class TestCompressionParamsFactory
{

    /**
     * Creates Snappy CompressionParams with default chunk length and compression ratio of 1.1
     * @return CompressionParams for a SnappyCompressor with Default chunk length and compression ratio of 1.1
     */
    public static CompressionParams snappy()
    {
        return snappy(CompressionParams.DEFAULT_CHUNK_LENGTH);
    }

    /**
     * Creates Snappy CompressionParams with specified chunk length and compression ratio of 1.1
     * @param chunkLength the chunklength.
     * @return CompressionParams for a SnappyCompressor with specified chunk length and compression ratio of 1.1
     */
    public static CompressionParams snappy(int chunkLength)
    {
        return snappy(chunkLength,  1.1);
    }

    /**
     * Creates Snappy CompressionParams
     * @param chunkLength the chunklength.
     * @param minCompressRatio the minimum compress ratio
     * @return CompressionParams for a SnappyCompressor with specified chunk length and compression ratio
     */
    public static CompressionParams snappy(int chunkLength, double minCompressRatio)
    {
        return new CompressionParams("SnappyCompressor", Collections.emptyMap(), chunkLength, minCompressRatio);
    }

    /**
     * Creates Deflate CompressionParams
     * @return CompressionParams for a DeflateCompressor with default chunk length
     */
    public static CompressionParams deflate()
    {
        return deflate(CompressionParams.DEFAULT_CHUNK_LENGTH);
    }

    /**
     * Creates Deflate CompressionParams with the specified chunk length
     * @param chunkLength the chunk length
     * @return CompressionParams for a DeflateCompressor with specified chunk length.
     */
    public static CompressionParams deflate(int chunkLength)
    {
        return new CompressionParams("DeflateCompressor", Collections.emptyMap(), chunkLength, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO);
    }

    /**
     * Creates LZ4 CompressionParams with the default chunk length
     * @return CompressionParams for a Lz4Compressor with default chunk length.
     */
    public static CompressionParams lz4()
    {
        return lz4(CompressionParams.DEFAULT_CHUNK_LENGTH);
    }

    /**
     * Creates LZ4 CompressionParams with the specified chunk length
     * and a max compressed length of the same size.
     * @param chunkLength  the chunk and max compressed length.
     * @return CompressionParams for a Lz4Compressor with specified chunk length.
     */
    public static CompressionParams lz4(int chunkLength)
    {
        return lz4(chunkLength, chunkLength);
    }

    /**
     * Creates LZ4 CompressionParams with the specified chunk length and max compressed length
     * @param chunkLength the chunnk length
     * @param maxCompressedLength the max compressed size
     * @return CompressionParams for a Lz4Compressor.
     */
    public static CompressionParams lz4(int chunkLength, int maxCompressedLength)
    {
        return new CompressionParams("LZ4Compressor", chunkLength, maxCompressedLength, Collections.emptyMap());
    }

    /**
     * Creates Zstd CompressionParams with default chunk length
     * @return CompressionParams for a ZstdCompressor with default chunk length.
     */
    public static CompressionParams zstd()
    {
        return zstd(CompressionParams.DEFAULT_CHUNK_LENGTH);
    }

    /**
     * Creates Zstd CompressionParams with specified chunk length
     * @param chunkLength the chunk length
     * @return CompressionParams for a ZstdCompressor with specified chunk length.
     */
    public static CompressionParams zstd(Integer chunkLength)
    {
        return new CompressionParams("ZstdCompressor", Collections.emptyMap(), chunkLength, CompressionParams.DEFAULT_MIN_COMPRESS_RATIO);
    }

    /**
     * Creates Noop CompressionParams with default chunk length
     * @return CompressionParams for a NoopCompressor with default chunk length.
     */
    public static CompressionParams noop()
    {
        return new CompressionParams("NoopCompressor",  Collections.emptyMap(), CompressionParams.DEFAULT_CHUNK_LENGTH,  CompressionParams.DEFAULT_MIN_COMPRESS_RATIO);
    }

}
