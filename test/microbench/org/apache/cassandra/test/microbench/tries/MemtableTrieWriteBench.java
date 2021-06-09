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

import org.apache.cassandra.db.tries.MemtableTrie;
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
public class MemtableTrieWriteBench
{
    @Param({"ON_HEAP", "OFF_HEAP"})
    BufferType bufferType = BufferType.OFF_HEAP;

    @Param({"1000", "100000", "10000000"})
    int count = 1000;

    @Param({"8"})
    int keyLength = 8;

    final static MemtableTrie.UpsertTransformer<Byte, Byte> resolver = (x, y) -> y;

    @Benchmark
    public void putSequential() throws MemtableTrie.SpaceExhaustedException
    {
        MemtableTrie<Byte> trie = new MemtableTrie(bufferType);
        ByteBuffer buf = ByteBuffer.allocate(keyLength);

        for (long current = 0; current < count; ++current)
        {
            long l = current;
            buf.putLong(keyLength - 8, l);
            trie.putRecursive(ByteComparable.fixedLength(buf), Byte.valueOf((byte) (l >> 56)), resolver);
        }
    }

    @Benchmark
    public void putRandom() throws MemtableTrie.SpaceExhaustedException
    {
        MemtableTrie<Byte> trie = new MemtableTrie(bufferType);
        Random rand = new Random(1);
        byte[] buf = new byte[keyLength];

        for (long current = 0; current < count; ++current)
        {
            rand.nextBytes(buf);
            trie.putRecursive(ByteComparable.fixedLength(buf), buf[0], resolver);
        }
    }

    @Benchmark
    public void applySequential() throws MemtableTrie.SpaceExhaustedException
    {
        MemtableTrie<Byte> trie = new MemtableTrie(bufferType);
        ByteBuffer buf = ByteBuffer.allocate(keyLength);

        for (long current = 0; current < count; ++current)
        {
            long l = current;
            buf.putLong(keyLength - 8, l);
            trie.putSingleton(ByteComparable.fixedLength(buf), Byte.valueOf((byte) (l >> 56)), resolver);
        }
    }

    @Benchmark
    public void applyRandom() throws MemtableTrie.SpaceExhaustedException
    {
        MemtableTrie<Byte> trie = new MemtableTrie(bufferType);
        Random rand = new Random(1);
        byte[] buf = new byte[keyLength];

        for (long current = 0; current < count; ++current)
        {
            rand.nextBytes(buf);
            trie.putSingleton(ByteComparable.fixedLength(buf), Byte.valueOf(buf[0]), resolver);
        }
    }
}
