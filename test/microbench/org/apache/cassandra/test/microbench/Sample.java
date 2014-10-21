/**
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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.openjdk.jmh.annotations.*;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class Sample
{
    @Param({"65536"})
    private int pageSize;

    @Param({"1024"})
    private int uniquePages;

    @Param({"0.1"})
    private double randomRatio;

    @Param({"4..16"})
    private String randomRunLength;

    @Param({"4..128"})
    private String duplicateLookback;

    private byte[][] lz4Bytes;
    private byte[][] snappyBytes;
    private byte[][] rawBytes;

    private LZ4FastDecompressor lz4Decompressor = LZ4Factory.fastestInstance().fastDecompressor();

    private LZ4Compressor lz4Compressor = LZ4Factory.fastestInstance().fastCompressor();

    @State(Scope.Thread)
    public static class ThreadState
    {
        byte[] bytes;
    }

    @Setup
    public void setup() throws IOException
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int[] randomRunLength = range(this.randomRunLength);
        int[] duplicateLookback = range(this.duplicateLookback);
        rawBytes = new byte[uniquePages][pageSize];
        lz4Bytes = new byte[uniquePages][];
        snappyBytes = new byte[uniquePages][];
        byte[][] runs = new byte[duplicateLookback[1] - duplicateLookback[0]][];
        for (int i = 0 ; i < rawBytes.length ; i++)
        {
            byte[] trg = rawBytes[0];
            int runCount = 0;
            int byteCount = 0;
            while (byteCount < trg.length)
            {
                byte[] nextRun;
                if (runCount == 0 || random.nextDouble() < this.randomRatio)
                {
                    nextRun = new byte[random.nextInt(randomRunLength[0], randomRunLength[1])];
                    random.nextBytes(nextRun );
                    runs[runCount % runs.length] = nextRun;
                    runCount++;
                }
                else
                {
                    int index = runCount < duplicateLookback[1]
                            ? random.nextInt(runCount)
                            : (runCount - random.nextInt(duplicateLookback[0], duplicateLookback[1]));
                    nextRun = runs[index % runs.length];
                }
                System.arraycopy(nextRun, 0, trg, byteCount, Math.min(nextRun.length, trg.length - byteCount));
                byteCount += nextRun.length;
            }
            lz4Bytes[i] = lz4Compressor.compress(trg);
            snappyBytes[i] = Snappy.compress(trg);
        }
    }

    static int[] range(String spec)
    {
        String[] split = spec.split("\\.\\.");
        return new int[] { Integer.parseInt(split[0]), Integer.parseInt(split[1]) };
    }

    @Benchmark
    public void lz4(ThreadState state)
    {
        if (state.bytes == null)
            state.bytes = new byte[this.pageSize];
        byte[] in = lz4Bytes[ThreadLocalRandom.current().nextInt(lz4Bytes.length)];
        lz4Decompressor.decompress(in, state.bytes);
    }

    @Benchmark
    public void snappy(ThreadState state) throws IOException
    {
        byte[] in = snappyBytes[ThreadLocalRandom.current().nextInt(snappyBytes.length)];
        state.bytes = Snappy.uncompress(in);
    }
}
