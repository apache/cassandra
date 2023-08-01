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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1) // no concurrent writes
@State(Scope.Benchmark)
public class InMemoryTrieUnionBench
{
    @Param({"ON_HEAP", "OFF_HEAP"})
    BufferType bufferType = BufferType.OFF_HEAP;

    @Param({"1000", "100000", "10000000"})
    int count = 1000;

    @Param({"2", "3", "8"})
    int sources = 2;

    @Param({"false", "true"})
    boolean sequential = true;

    final static InMemoryTrie.UpsertTransformer<Byte, Byte> resolver = (x, y) -> y;

    Trie<Byte> trie;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        List<InMemoryTrie<Byte>> tries = new ArrayList<>(sources);
        System.out.format("Putting %,d among %d tries\n", count, sources);
        Random rand = new Random(1);
        if (sequential)
        {
            long sz = 65536 / sources;
            for (int i = 0; i < sources; ++i)
                tries.add(new InMemoryTrie<>(bufferType));

            for (long current = 0; current < count; ++current)
            {
                long l = rand.nextLong();
                InMemoryTrie<Byte> tt = tries.get(Math.min((int) (((l >> 48) + 32768) / sz), sources - 1));
                tt.putRecursive(ByteComparable.of(l), (byte) (l >> 56), resolver);
            }

        }
        else
        {
            long current = 0;
            for (int i = 0; i < sources; ++i)
            {
                InMemoryTrie<Byte> trie = new InMemoryTrie(bufferType);
                int currMax = this.count * (i + 1) / sources;

                for (; current < currMax; ++current)
                {
                    long l = rand.nextLong();
                    trie.putRecursive(ByteComparable.of(l), (byte) (l >> 56), resolver);
                }
                tries.add(trie);
            }
        }

        for (InMemoryTrie<Byte> trie : tries)
        {
            System.out.format("Trie size on heap %,d off heap %,d\n",
                              trie.sizeOnHeap(), trie.sizeOffHeap());
        }
        trie = Trie.mergeDistinct(tries);

        System.out.format("Actual count %,d\n", Iterables.size(trie.values()));
    }

    @Benchmark
    public int iterateValues()
    {
        int sum = 0;
        for (byte b : trie.values())
            sum += b;
        return sum;
    }

    @Benchmark
    public int iterateValuesUnordered()
    {
        int sum = 0;
        for (byte b : trie.valuesUnordered())
            sum += b;
        return sum;
    }

    @Benchmark
    public int iterateEntries()
    {
        int sum = 0;
        for (Map.Entry<ByteComparable, Byte> en : trie.entrySet())
            sum += en.getValue();
        return sum;
    }

    @Benchmark
    public int iterateValuesLimited()
    {
        Iterable<Byte> values = trie.subtrie(ByteComparable.of(0L),
                                             true,
                                             ByteComparable.of(Long.MAX_VALUE / 2),         // 1/4 of all
                                             false)
                                    .values();
        int sum = 0;
        for (byte b : values)
            sum += b;
        return sum;
    }
}
