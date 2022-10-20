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
package org.apache.cassandra.test.microbench.tries;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1) // no concurrent writes
@State(Scope.Benchmark)
public class InMemoryTrieWriteBench
{
    @Param({"ON_HEAP", "OFF_HEAP"})
    BufferType bufferType = BufferType.OFF_HEAP;

    @Param({"1000", "100000", "10000000"})
    int count = 1000;

    @Param({"8"})
    int keyLength = 8;

    final static InMemoryTrie.UpsertTransformer<Byte, Byte> resolver = (x, y) -> y;

    // Set this to true to print the trie sizes after insertions for sanity checking.
    // This might affect the timings, do not commit with this set to true.
    final static boolean PRINT_SIZES = false;

    @Benchmark
    public void putSequential(Blackhole bh) throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<Byte> trie = new InMemoryTrie(bufferType);
        ByteBuffer buf = ByteBuffer.allocate(keyLength);

        for (long current = 0; current < count; ++current)
        {
            long l = current;
            buf.putLong(keyLength - 8, l);
            trie.putRecursive(ByteComparable.fixedLength(buf), Byte.valueOf((byte) (l >> 56)), resolver);
        }
        if (PRINT_SIZES)
            System.out.println(trie.valuesCount());
        bh.consume(trie);
    }

    @Benchmark
    public void putRandom(Blackhole bh) throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<Byte> trie = new InMemoryTrie(bufferType);
        Random rand = new Random(1);
        byte[] buf = new byte[keyLength];

        for (long current = 0; current < count; ++current)
        {
            rand.nextBytes(buf);
            trie.putRecursive(ByteComparable.fixedLength(buf), Byte.valueOf(buf[0]), resolver);
        }
        if (PRINT_SIZES)
            System.out.println(trie.valuesCount());
        bh.consume(trie);
    }

    @Benchmark
    public void applySequential(Blackhole bh) throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<Byte> trie = new InMemoryTrie(bufferType);
        ByteBuffer buf = ByteBuffer.allocate(keyLength);

        for (long current = 0; current < count; ++current)
        {
            long l = current;
            buf.putLong(keyLength - 8, l);
            trie.putSingleton(ByteComparable.fixedLength(buf), Byte.valueOf((byte) (l >> 56)), resolver);
        }
        if (PRINT_SIZES)
            System.out.println(trie.valuesCount());
        bh.consume(trie);
    }

    @Benchmark
    public void applyRandom(Blackhole bh) throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<Byte> trie = new InMemoryTrie(bufferType);
        Random rand = new Random(1);
        byte[] buf = new byte[keyLength];

        for (long current = 0; current < count; ++current)
        {
            rand.nextBytes(buf);
            trie.putSingleton(ByteComparable.fixedLength(buf), Byte.valueOf(buf[0]), resolver);
        }
        if (PRINT_SIZES)
            System.out.println(trie.valuesCount());
        bh.consume(trie);
    }
}
