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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx1G")
public class ZstdDictionaryCompressorThroughputBench extends ZstdDictionaryCompressorBenchBase
{
    @Benchmark
    public void compressionThroughput(Blackhole bh) throws IOException
    {
        inputBuffer.rewind();
        compressedBuffer.clear();

        compressor.compress(inputBuffer, compressedBuffer);
        bh.consume(compressedBuffer.position());
    }

    @Benchmark
    public void decompressionThroughput(Blackhole bh) throws IOException
    {
        // First compress the data
        inputBuffer.rewind();
        compressedBuffer.clear();
        compressor.compress(inputBuffer, compressedBuffer);

        // Then decompress it
        compressedBuffer.flip();
        decompressedBuffer.clear();
        compressor.uncompress(compressedBuffer, decompressedBuffer);

        bh.consume(decompressedBuffer.position());
    }
}
