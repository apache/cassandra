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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesWalker;
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
public class InMemoryTrieReadBench
{
    @Param({"ON_HEAP", "OFF_HEAP"})
    BufferType bufferType = BufferType.OFF_HEAP;

    @Param({"1000", "100000", "10000000"})
    int count = 1000;

    final static InMemoryTrie.UpsertTransformer<Byte, Byte> resolver = (x, y) -> y;

    InMemoryTrie<Byte> trie;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        trie = new InMemoryTrie<>(bufferType);
        Random rand = new Random(1);

        System.out.format("Putting %,d\n", count);
        for (long current = 0; current < count; ++current)
        {
            long l = rand.nextLong();
            trie.putRecursive(ByteComparable.of(l), Byte.valueOf((byte) (l >> 56)), resolver);
        }
        System.out.format("Trie size on heap %,d off heap %,d\n",
                          trie.sizeOnHeap(), trie.sizeOffHeap());
        System.out.format("per entry on heap %.2f off heap %.2f\n",
                          trie.sizeOnHeap() * 1.0 / count, trie.sizeOffHeap() * 1.0 / count);
    }

    @Benchmark
    public void getRandom()
    {
        Random rand = new Random(1);

        for (long current = 0; current < count; ++current)
        {
            long l = rand.nextLong();
            Byte res = trie.get(ByteComparable.of(l));
            if (res.byteValue() != l >> 56)
                throw new AssertionError();
        }
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
    public int consumeValues()
    {
        class Counter implements Trie.ValueConsumer<Byte>
        {
            int sum = 0;

            @Override
            public void accept(Byte aByte)
            {
                sum += aByte;
            }
        }
        Counter counter = new Counter();
        trie.forEachValue(counter);
        return counter.sum;
    }

    @Benchmark
    public int consumeEntries()
    {
        class Counter implements BiConsumer<ByteComparable, Byte>
        {
            int sum = 0;

            @Override
            public void accept(ByteComparable byteComparable, Byte aByte)
            {
                sum += aByte;
            }
        }
        Counter counter = new Counter();
        trie.forEachEntry(counter);
        return counter.sum;
    }

    @Benchmark
    public int processEntries()
    {
        class Counter extends TrieEntriesWalker<Byte, Void>
        {
            int sum = 0;

            @Override
            protected void content(Byte content, byte[] bytes, int byteLength)
            {
                sum += content;
            }

            @Override
            public Void complete()
            {
                return null;
            }
        }
        Counter counter = new Counter();
        trie.process(counter);
        return counter.sum;
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
