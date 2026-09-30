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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDictTrainer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;
import org.apache.cassandra.utils.concurrent.Ref;

// Compares: (a) raw pool acquire/release of a ZstdCompressCtx (no outer Ref at all), (b) the same pool
// acquire/release wrapped in a tryRef()/release() pair (what the current fix does per chunk), and
// (c) the full compress()+uncompress() round trip through ZstdDictionaryCompressor at a small chunk size,
// to see what fraction of the real call the Ref pair actually costs.
public class BenchmarkRefOverhead
{
    public static void main(String[] args) throws Exception
    {
        DatabaseDescriptor.daemonInitialization();

        byte[] sample = new byte[4096];
        for (int i = 0; i < sample.length; i++)
            sample[i] = (byte) (i % 251);

        int sampleSize = 100 * 1024;
        int dictSize = 6 * 1024;
        ZstdDictTrainer trainer = new ZstdDictTrainer(sampleSize, dictSize, 3);
        for (int i = 0; i < 200; i++)
            trainer.addSample(sample);
        byte[] dictBytes = trainer.trainSamples();

        ZstdCompressionDictionary dictionary = new ZstdCompressionDictionary(new DictId(Kind.ZSTD, 1), dictBytes);
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(dictionary);
        int level = compressor.compressionLevel();

        int warmup = 200_000;
        int iters = 2_000_000;

        // (a) raw pool acquire/release, no Ref at all
        for (int i = 0; i < warmup; i++)
        {
            ZstdCompressCtx ctx = dictionary.acquireCompressCtx(level);
            dictionary.releaseCompressCtx(level, ctx);
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < iters; i++)
        {
            ZstdCompressCtx ctx = dictionary.acquireCompressCtx(level);
            dictionary.releaseCompressCtx(level, ctx);
        }
        long poolOnlyNanos = System.nanoTime() - t0;

        // (b) same, but bracketed with a tryRef()/release() pair, matching the current fix's per-chunk shape
        for (int i = 0; i < warmup; i++)
        {
            Ref<ZstdCompressionDictionary> ref = dictionary.tryRef();
            ZstdCompressCtx ctx = dictionary.acquireCompressCtx(level);
            dictionary.releaseCompressCtx(level, ctx);
            ref.release();
        }
        t0 = System.nanoTime();
        for (int i = 0; i < iters; i++)
        {
            Ref<ZstdCompressionDictionary> ref = dictionary.tryRef();
            ZstdCompressCtx ctx = dictionary.acquireCompressCtx(level);
            dictionary.releaseCompressCtx(level, ctx);
            ref.release();
        }
        long poolPlusRefNanos = System.nanoTime() - t0;

        System.out.printf("pool acquire/release only:            %.1f ns/op%n", poolOnlyNanos / (double) iters);
        System.out.printf("pool acquire/release + tryRef/release: %.1f ns/op%n", poolPlusRefNanos / (double) iters);
        System.out.printf("Ref pair overhead (compress-side only): %.1f ns/op%n%n", (poolPlusRefNanos - poolOnlyNanos) / (double) iters);

        // (c) full compress()+uncompress() round trip, via the real compressor
        //     (already includes its own internal tryRef()/release() pair per call)
        for (int chunkSize : new int[] {4096, 16384, 65536})
        {
            ByteBuffer input = ByteBuffer.allocateDirect(chunkSize);
            input.put(sample, 0, Math.min(chunkSize, sample.length));
            if (chunkSize > sample.length)
            {
                byte[] filler = new byte[chunkSize - sample.length];
                input.put(filler);
            }
            ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(chunkSize));
            ByteBuffer decompressed = ByteBuffer.allocateDirect(chunkSize);

            for (int i = 0; i < warmup; i++)
            {
                input.clear(); input.limit(chunkSize);
                compressed.clear();
                decompressed.clear();
                compressor.compress(input, compressed);
                compressed.flip();
                compressor.uncompress(compressed, decompressed);
            }
            t0 = System.nanoTime();
            for (int i = 0; i < iters; i++)
            {
                input.clear(); input.limit(chunkSize);
                compressed.clear();
                decompressed.clear();
                compressor.compress(input, compressed);
                compressed.flip();
                compressor.uncompress(compressed, decompressed);
            }
            long fullRoundTripNanos = System.nanoTime() - t0;

            System.out.printf("chunk=%6d KiB: full round trip %.1f ns/op, Ref overhead %.1f%% of it%n",
                              chunkSize / 1024, fullRoundTripNanos / (double) iters,
                              100.0 * 2 * (poolPlusRefNanos - poolOnlyNanos) / (double) fullRoundTripNanos);
        }
    }
}
