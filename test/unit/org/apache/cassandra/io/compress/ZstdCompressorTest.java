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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.github.luben.zstd.Zstd;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Zstd Compressor specific tests. General compressor tests are in {@link CompressorTest}
 */
public class ZstdCompressorTest
{
    @Test
    public void emptyConfigurationUsesDefaultCompressionLevel()
    {
        ZstdCompressor compressor = ZstdCompressor.create(Collections.emptyMap());
        assertEquals(ZstdCompressor.DEFAULT_COMPRESSION_LEVEL, compressor.compressionLevel());
    }

    @Test(expected = IllegalArgumentException.class)
    public void badCompressionLevelParamThrowsExceptionMin()
    {
        ZstdCompressor.create(ImmutableMap.of(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(Zstd.minCompressionLevel() - 1)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void badCompressionLevelParamThrowsExceptionMax()
    {
        ZstdCompressor.create(ImmutableMap.of(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(Zstd.maxCompressionLevel() + 1)));
    }

    /**
     * Compression and decompression borrow a pooled native context instead of allocating one per chunk, so a run of
     * chunks must leave exactly one context in each pool - created once, returned after every call.
     */
    @Test
    public void contextsAreReusedAcrossChunks() throws Exception
    {
        // a level of its own, so the shared per-level instance is not one another test has already exercised
        ZstdCompressor compressor = ZstdCompressor.getOrCreate(7);
        assertEquals("no context is allocated before the first chunk", 0, compressor.pooledCompressContexts());
        assertEquals(0, compressor.pooledDecompressContexts());

        for (int i = 0; i < 16; i++)
            roundTrip(compressor, chunk(4096, i));

        assertEquals("one compression context, reused across all chunks", 1, compressor.pooledCompressContexts());
        assertEquals("one decompression context, reused across all chunks", 1, compressor.pooledDecompressContexts());
    }

    /**
     * Pooling must not change the bytes written: a frame produced through a pooled context has to stay readable by
     * the one-shot API this compressor used previously, which is what on-disk data was written with.
     */
    @Test
    public void framesRemainReadableByTheOneShotApi() throws Exception
    {
        ZstdCompressor compressor = ZstdCompressor.getOrCreate(8);
        byte[] data = chunk(8192, 42);

        ByteBuffer input = direct(data);
        ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(data.length));
        compressor.compress(input, compressed);
        compressed.flip();

        ByteBuffer output = ByteBuffer.allocateDirect(data.length);
        Zstd.decompress(output, compressed);
        output.flip();

        byte[] result = new byte[output.remaining()];
        output.get(result);
        assertArrayEquals(data, result);
    }

    /** The byte[] decompression path is pooled too, and must round-trip what the ByteBuffer path produced. */
    @Test
    public void byteArrayDecompressionRoundTrips() throws Exception
    {
        ZstdCompressor compressor = ZstdCompressor.getOrCreate(9);
        byte[] data = chunk(2048, 7);

        ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(data.length));
        compressor.compress(direct(data), compressed);
        compressed.flip();

        byte[] compressedBytes = new byte[compressed.remaining()];
        compressed.get(compressedBytes);

        byte[] output = new byte[data.length];
        int size = compressor.uncompress(compressedBytes, 0, compressedBytes.length, output, 0);

        assertEquals(data.length, size);
        assertArrayEquals(data, output);
    }

    /**
     * Contexts are not thread safe, so each call borrows one exclusively. Concurrent callers must all round-trip
     * correctly, and the pool must not grow beyond the number of threads that were ever in flight at once.
     */
    @Test
    public void concurrentCallersDoNotShareAContext() throws Exception
    {
        ZstdCompressor compressor = ZstdCompressor.getOrCreate(10);
        int threads = 8;
        int chunksPerThread = 40;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try
        {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int t = 0; t < threads; t++)
            {
                int seed = t;
                tasks.add(() -> {
                    for (int i = 0; i < chunksPerThread; i++)
                        roundTrip(compressor, chunk(4096, seed * 1000 + i));
                    return null;
                });
            }

            for (Future<Void> future : executor.invokeAll(tasks))
                future.get(2, TimeUnit.MINUTES); // surfaces any corruption or native failure
        }
        finally
        {
            executor.shutdownNow();
        }

        assertTrue("the pool must not exceed the peak concurrency, was " + compressor.pooledCompressContexts(),
                   compressor.pooledCompressContexts() <= threads);
        assertTrue(compressor.pooledDecompressContexts() <= threads);
    }

    private static void roundTrip(ZstdCompressor compressor, byte[] data) throws Exception
    {
        ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(data.length));
        compressor.compress(direct(data), compressed);
        compressed.flip();

        ByteBuffer output = ByteBuffer.allocateDirect(data.length);
        compressor.uncompress(compressed, output);
        output.flip();

        byte[] result = new byte[output.remaining()];
        output.get(result);
        assertArrayEquals(data, result);
    }

    private static ByteBuffer direct(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    /** Compressible but not trivial content, so the frame is a real one rather than a degenerate case. */
    private static byte[] chunk(int size, int seed)
    {
        Random random = new Random(seed);
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte) ('a' + random.nextInt(8));
        return data;
    }
}
