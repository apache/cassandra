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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.github.jamm.MemoryMeter;
import org.github.jamm.MemoryMeter.Guess;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1,jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1) // no concurrent writes
@State(Scope.Benchmark)
public class ComparisonReadBench
{
    // Note: To see a printout of the usage for each object, add .printVisitedTree() here (most useful with smaller number of
    // partitions).
    static MemoryMeter meter = MemoryMeter.builder()
                                          .withGuessing(Guess.INSTRUMENTATION_AND_SPECIFICATION, Guess.UNSAFE)
                                          .build();

    @Param({"ON_HEAP"})
    BufferType bufferType = BufferType.OFF_HEAP;

    @Param({"1000", "100000", "10000000"})
    int count = 1000;

    @Param({"TREE_MAP", "CSLM", "TRIE"})
    MapOption map = MapOption.TRIE;

    @Param({"LONG"})
    TypeOption type = TypeOption.LONG;

    final static InMemoryTrie.UpsertTransformer<Byte, Byte> resolver = (x, y) -> y;

    Access<?> access;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        switch (map)
        {
            case TREE_MAP:
                access = new NavigableMapAccess(new TreeMap(type.type.comparator()), type.type);
                break;
            case CSLM:
                access = new NavigableMapAccess(new ConcurrentSkipListMap(type.type.comparator()), type.type);
                break;
            case TRIE:
                access = new TrieAccess(type.type);
                break;
        }
        Random rand = new Random(1);

        System.out.format("Putting %,d\n", count);
        long time = System.currentTimeMillis();
        for (long current = 0; current < count; ++current)
        {
            long l = rand.nextLong();
            access.put(l, Byte.valueOf((byte) (l >> 56)));
        }
        time = System.currentTimeMillis() - time;
        System.out.format("Took %.3f seconds\n", time * 0.001);
        access.printSize();
    }

    interface Type<T>
    {
        T fromLong(long l);
        T fromByteComparable(ByteComparable bc);
        ByteComparable longToByteComparable(long l);
        Comparator<T> comparator();
    }

    public enum TypeOption
    {
        LONG(new LongType()),
        BIGINT(new BigIntType()),
        DECIMAL(new BigDecimalType()),
        STRING_BASE16(new StringType(16)),
        STRING_BASE10(new StringType(10)),
        STRING_BASE4(new StringType(4)),
        ARRAY(new ArrayType(1)),
        ARRAY_REP3(new ArrayType(3)),
        ARRAY_REP7(new ArrayType(7));

        final Type<?> type;

        TypeOption(Type<?> type)
        {
            this.type = type;
        }
    }

    static class LongType implements Type<Long>
    {
        public Long fromLong(long l)
        {
            return l;
        }

        public Long fromByteComparable(ByteComparable bc)
        {
            return ByteSourceInverse.getSignedLong(bc.asComparableBytes(ByteComparable.Version.OSS50));
        }

        public ByteComparable longToByteComparable(long l)
        {
            return ByteComparable.of(l);
        }

        public Comparator<Long> comparator()
        {
            return Comparator.naturalOrder();
        }
    }

    static class BigIntType implements Type<BigInteger>
    {
        public BigInteger fromLong(long l)
        {
            return BigInteger.valueOf(l);
        }

        public BigInteger fromByteComparable(ByteComparable bc)
        {
            return IntegerType.instance.compose(IntegerType.instance.fromComparableBytes(ByteSource.peekable(bc.asComparableBytes(ByteComparable.Version.OSS50)),
                                                                                         ByteComparable.Version.OSS50));
        }

        public ByteComparable longToByteComparable(long l)
        {
            return v -> IntegerType.instance.asComparableBytes(IntegerType.instance.decompose(fromLong(l)), v);
        }

        public Comparator<BigInteger> comparator()
        {
            return Comparator.naturalOrder();
        }
    }

    static class BigDecimalType implements Type<BigDecimal>
    {
        public BigDecimal fromLong(long l)
        {
            return BigDecimal.valueOf(l);
        }

        public BigDecimal fromByteComparable(ByteComparable bc)
        {
            return DecimalType.instance.compose(DecimalType.instance.fromComparableBytes(ByteSource.peekable(bc.asComparableBytes(ByteComparable.Version.OSS50)),
                                                                                         ByteComparable.Version.OSS50));
        }

        public ByteComparable longToByteComparable(long l)
        {
            return v -> DecimalType.instance.asComparableBytes(DecimalType.instance.decompose(fromLong(l)), v);
        }

        public Comparator<BigDecimal> comparator()
        {
            return Comparator.naturalOrder();
        }
    }

    static class StringType implements Type<String>
    {
        final int base;

        StringType(int base)
        {
            this.base = base;
        }

        public String fromLong(long l)
        {
            return Long.toString(l, base);
        }

        public String fromByteComparable(ByteComparable bc)
        {
            return new String(ByteSourceInverse.readBytes(bc.asComparableBytes(ByteComparable.Version.OSS50)), StandardCharsets.UTF_8);
        }

        public ByteComparable longToByteComparable(long l)
        {
            return ByteComparable.fixedLength(fromLong(l).getBytes(StandardCharsets.UTF_8));
        }

        public Comparator<String> comparator()
        {
            return Comparator.naturalOrder();
        }
    }

    static class ArrayType implements Type<byte[]>
    {
        final int reps;

        ArrayType(int reps)
        {
            this.reps = reps;
        }

        public byte[] fromLong(long l)
        {
            byte[] value = new byte[8 * reps];
            for (int i = 0; i < 8; ++i)
            {
                for (int j = 0; j < reps; ++j)
                    value[i * reps + j] = (byte)(l >> (56 - i * 8));
            }
            return value;
        }

        public byte[] fromByteComparable(ByteComparable bc)
        {
            return ByteSourceInverse.readBytes(bc.asComparableBytes(ByteComparable.Version.OSS50));
        }

        public ByteComparable longToByteComparable(long l)
        {
            return ByteComparable.fixedLength(fromLong(l));
        }

        public Comparator<byte[]> comparator()
        {
            return ByteArrayUtil::compareUnsigned;
        }
    }

    interface Access<T>
    {
        void put(long v, byte b);
        byte get(long v);
        Iterable<Byte> values();
        Iterable<Byte> valuesSlice(long left, boolean includeLeft, long right, boolean includeRight);
        Iterable<Map.Entry<T, Byte>> entrySet();
        void consumeValues(Consumer<Byte> consumer);
        void consumeEntries(BiConsumer<T, Byte> consumer);
        void printSize();
    }

    public enum MapOption
    {
        TREE_MAP,
        CSLM,
        TRIE
    }

    class TrieAccess<T> implements Access<T>
    {
        final InMemoryTrie<Byte> trie;
        final Type<T> type;

        TrieAccess(Type<T> type)
        {
            this.type = type;
            trie = new InMemoryTrie<>(bufferType);
        }

        public void put(long v, byte b)
        {
            try
            {
                trie.putRecursive(type.longToByteComparable(v), b, resolver);
            }
            catch (InMemoryTrie.SpaceExhaustedException e)
            {
                throw Throwables.propagate(e);
            }
        }

        public byte get(long v)
        {
            return trie.get(type.longToByteComparable(v));
        }

        public Iterable<Byte> values()
        {
            return trie.values();
        }

        public Iterable<Byte> valuesSlice(long left, boolean includeLeft, long right, boolean includeRight)
        {
            return trie.subtrie(type.longToByteComparable(left), includeLeft, type.longToByteComparable(right), includeRight)
                       .values();
        }

        public Iterable<Map.Entry<T, Byte>> entrySet()
        {
            return Iterables.transform(trie.entrySet(),
                    en -> new AbstractMap.SimpleEntry<>(type.fromByteComparable(en.getKey()),
                                                        en.getValue()));
        }

        public void consumeValues(Consumer<Byte> consumer)
        {
            trie.forEachValue(consumer::accept);
        }

        public void consumeEntries(BiConsumer<T, Byte> consumer)
        {
            trie.forEachEntry((key, value) -> consumer.accept(type.fromByteComparable(key), value));
        }

        public void printSize()
        {
            long deepsize = meter.measureDeep(trie);
            System.out.format("Trie size on heap %,d off heap %,d deep size %,d\n",
                              trie.sizeOnHeap(), trie.sizeOffHeap(), deepsize);
            System.out.format("per entry on heap %.2f off heap %.2f deep size %.2f\n",
                              trie.sizeOnHeap() * 1.0 / count, trie.sizeOffHeap() * 1.0 / count, deepsize * 1.0 / count);
        }
    }

    class NavigableMapAccess<T> implements Access<T>
    {
        final NavigableMap<T, Byte> navigableMap;
        final Type<T> type;

        NavigableMapAccess(NavigableMap<T, Byte> navigableMap, Type<T> type)
        {
            this.navigableMap = navigableMap;
            this.type = type;
        }

        public void put(long v, byte b)
        {
            navigableMap.put(type.fromLong(v), b);
        }

        public byte get(long v)
        {
            return navigableMap.get(type.fromLong(v));
        }

        public Iterable<Byte> values()
        {
            return navigableMap.values();
        }

        public Iterable<Byte> valuesSlice(long left, boolean includeLeft, long right, boolean includeRight)
        {
            return navigableMap.subMap(type.fromLong(left), includeLeft, type.fromLong(right), includeRight)
                               .values();
        }

        public Iterable<Map.Entry<T, Byte>> entrySet()
        {
            return navigableMap.entrySet();
        }

        public void consumeValues(Consumer<Byte> consumer)
        {
            navigableMap.values().forEach(consumer);
        }

        public void consumeEntries(BiConsumer<T, Byte> consumer)
        {
            navigableMap.forEach(consumer);
        }

        public void printSize()
        {
            long size = meter.measureDeep(navigableMap);
            System.out.format(map + " size on heap %,d\n", size);
            System.out.format("per entry on heap %.2f\n", size * 1.0 / count);
        }
    }

    @Benchmark
    public void getRandom()
    {
        Random rand = new Random(1);

        for (long current = 0; current < count; ++current)
        {
            long l = rand.nextLong();
            Byte res = access.get(l);
            if (res.byteValue() != l >> 56)
                throw new AssertionError();
        }
    }

    @Benchmark
    public int iterateValues()
    {
        int sum = 0;
        for (byte b : access.values())
            sum += b;
        return sum;
    }

    @Benchmark
    public int consumeValues()
    {
        class Counter implements Consumer<Byte>
        {
            int sum = 0;

            @Override
            public void accept(Byte aByte)
            {
                sum += aByte;
            }
        }
        Counter counter = new Counter();
        access.consumeValues(counter);
        return counter.sum;
    }

    @Benchmark
    public int consumeEntries()
    {
        class Counter<T> implements BiConsumer<T, Byte>
        {
            int sum = 0;

            @Override
            public void accept(T key, Byte aByte)
            {
                sum += aByte;
            }
        }
        Counter counter = new Counter();
        access.consumeEntries(counter);
        return counter.sum;
    }

    @Benchmark
    public int iterateEntries()
    {
        int sum = 0;
        for (Map.Entry<?, Byte> en : access.entrySet())
            sum += en.getValue();
        return sum;
    }


    @Benchmark
    public int getByIterateValueSlice()
    {
        Random rand = new Random(1);
        int sum = 0;
        for (int i = 0; i < count; ++i)
        {
            long v = rand.nextLong();
            Iterable<Byte> values = access.valuesSlice(v, true, v, true);
            for (byte b : values)
                sum += b;
        }
        return sum;
    }

    @Benchmark
    public int iterateValuesLimited()
    {
        int sum = 0;
        Iterable<Byte> values = access.valuesSlice(0L, false, Long.MAX_VALUE / 2, true); // 1/4
        for (byte b : values)
            sum += b;
        return sum;
    }
}
