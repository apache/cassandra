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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import org.apache.cassandra.utils.ChecksumType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.xerial.snappy.PureJavaCrc32C;

import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class ChecksumBench
{
    private static final Random random = new Random(12345678);

    // intentionally not on power-of-2 values
    @Param({ "31", "131", "517", "2041" })
    private int bufferSize;

    private byte[] array;

    @Setup
    public void setup() throws NoSuchAlgorithmException
    {
        array = new byte[bufferSize];
        random.nextBytes(array);
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public byte[] benchCrc32()
    {
        return Longs.toByteArray(ChecksumType.CRC32.of(array, 0, array.length));
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
            "-XX:+UnlockDiagnosticVMOptions", "-XX:-UseCRC32Intrinsics",
    })
    public byte[] benchCrc32NoIntrinsic()
    {
        return Longs.toByteArray(ChecksumType.CRC32.of(array, 0, array.length));
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public byte[] benchHasherCrc32c()
    {
        Hasher crc32cHasher = Hashing.crc32c().newHasher();
        crc32cHasher.putBytes(array);
        return crc32cHasher.hash().asBytes();
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public byte[] benchPureJavaCrc32c()
    {
        PureJavaCrc32C pureJavaCrc32C = new PureJavaCrc32C();
        pureJavaCrc32C.update(array, 0, array.length);
        return Longs.toByteArray(pureJavaCrc32C.getValue());
    }

    // Below benchmarks are commented because CRC32C is unavailable in Java 8.
//    @Benchmark
//    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
//            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
//    })
//    public byte[] benchCrc32c()
//    {
//        CRC32C crc32C = new CRC32C();
//        crc32C.update(array);
//        return Longs.toByteArray(crc32C.getValue());
//    }
//
//    @Benchmark
//    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
//            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
//            "-XX:+UnlockDiagnosticVMOptions", "-XX:-UseCRC32CIntrinsics",
//    })
//    public byte[] benchCrc32cNoIntrinsic()
//    {
//        CRC32C crc32C = new CRC32C();
//        crc32C.update(array);
//        return Longs.toByteArray(crc32C.getValue());
//    }
}
