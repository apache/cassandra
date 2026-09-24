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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.github.luben.zstd.ZstdDictTrainer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;

// Runs compress()+uncompress() continuously via the real public API and reports per-second latency buckets
// (mean/p99/max), instead of one flat average over the whole run. A flat average over millions of ops would
// dilute a rare, periodic stall to invisibility; per-second buckets over a long run make such a stall visible
// as a spike. Note that Ref's STRONG_LEAK_DETECTOR is a debug option (cassandra.test.debug_ref_count) and does
// not fire in production or in this bench; the always-on background thread is Ref's "Reference-Reaper".
//
// Usage: BenchmarkRefOverhead [durationSeconds] [chunkSizeBytes]
// Defaults: 25 minutes, 4096 B chunks (smallest/highest-churn case exercised elsewhere in this bench).
public class BenchmarkRefOverhead
{
    public static void main(String[] args) throws Exception
    {
        DatabaseDescriptor.daemonInitialization();

        long durationSeconds = args.length > 0 ? Long.parseLong(args[0]) : TimeUnit.MINUTES.toSeconds(25);
        int chunkSize = args.length > 1 ? Integer.parseInt(args[1]) : 4096;

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

        ByteBuffer input = ByteBuffer.allocateDirect(chunkSize);
        input.put(sample, 0, Math.min(chunkSize, sample.length));
        if (chunkSize > sample.length)
            input.put(new byte[chunkSize - sample.length]);
        ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(chunkSize));
        ByteBuffer decompressed = ByteBuffer.allocateDirect(chunkSize);

        // Warm up the JIT before recording anything, so early-run deopt/compilation noise isn't mistaken for
        // a reaper-related spike.
        long warmupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < warmupDeadline)
        {
            input.clear(); input.limit(chunkSize);
            compressed.clear();
            decompressed.clear();
            compressor.compress(input, compressed);
            compressed.flip();
            compressor.uncompress(compressed, decompressed);
        }

        System.out.println("Warmup done. Recording for " + durationSeconds + "s at chunk=" + chunkSize + "B.");
        System.out.println("Watch for periodic spikes in max_ns; correlate them with GC logs to attribute them.");
        System.out.println("elapsed_s,ops,mean_ns,p99_ns,max_ns");

        long windowNanos = TimeUnit.SECONDS.toNanos(1);
        long runStart = System.nanoTime();
        long windowStart = runStart;
        long windowCount = 0;
        long windowSumNanos = 0;
        long windowMaxNanos = 0;
        long[] windowSamples = new long[2_000_000];
        int windowSampleCount = 0;

        while (System.nanoTime() - runStart < TimeUnit.SECONDS.toNanos(durationSeconds))
        {
            input.clear(); input.limit(chunkSize);
            compressed.clear();
            decompressed.clear();

            long t0 = System.nanoTime();
            compressor.compress(input, compressed);
            compressed.flip();
            compressor.uncompress(compressed, decompressed);
            long elapsed = System.nanoTime() - t0;

            windowCount++;
            windowSumNanos += elapsed;
            if (elapsed > windowMaxNanos)
                windowMaxNanos = elapsed;
            if (windowSampleCount < windowSamples.length)
                windowSamples[windowSampleCount++] = elapsed;

            long now = System.nanoTime();
            if (now - windowStart >= windowNanos)
            {
                long p99 = approxP99(windowSamples, windowSampleCount);
                System.out.printf("%.0f,%d,%.1f,%d,%d%n",
                                  (now - runStart) / 1e9, windowCount,
                                  windowSumNanos / (double) windowCount, p99, windowMaxNanos);
                windowStart = now;
                windowCount = 0;
                windowSumNanos = 0;
                windowMaxNanos = 0;
                windowSampleCount = 0;
            }
        }
    }

    private static long approxP99(long[] samples, int count)
    {
        if (count == 0)
            return 0;
        // Sort in place - the caller resets windowSampleCount right after this, so mutating the live prefix is
        // safe, and it avoids an extra multi-MB array allocation every window (large enough to be a G1
        // "humongous" allocation, which is its own source of periodic GC pauses unrelated to anything measured).
        Arrays.sort(samples, 0, count);
        return samples[(int) Math.min(count - 1, Math.floor(count * 0.99))];
    }
}
