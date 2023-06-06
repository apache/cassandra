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
package org.apache.cassandra.utils.bytecomparable;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;

import static org.apache.cassandra.utils.bytecomparable.ByteSourceComparisonTest.decomposeForTuple;
import static org.junit.Assert.assertEquals;

/**
 * Tests that the result of forward + backward ByteSource translation is the same as the original.
 */
public class ByteSourceConversionTest extends ByteSourceTestBase
{
    private final static Logger logger = LoggerFactory.getLogger(ByteSourceConversionTest.class);
    public static final Version VERSION = Version.OSS50;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testStringsAscii()
    {
        testType(AsciiType.instance, Arrays.stream(testStrings)
                                           .filter(s -> s.equals(new String(s.getBytes(StandardCharsets.US_ASCII),
                                                                            StandardCharsets.US_ASCII)))
                                           .toArray());
    }

    @Test
    public void testStringsUTF8()
    {
        testType(UTF8Type.instance, testStrings);
        testDirect(x -> ByteSource.of(x, VERSION), ByteSourceInverse::getString, testStrings);
    }

    @Test
    public void testBooleans()
    {
        testType(BooleanType.instance, testBools);
    }

    @Test
    public void testInts()
    {
        testType(Int32Type.instance, testInts);
        testDirect(ByteSource::of, ByteSourceInverse::getSignedInt, testInts);
    }

    @Test
    public void randomTestInts()
    {
        Random rand = new Random();
        for (int i=0; i<10000; ++i)
        {
            int i1 = rand.nextInt();
            assertConvertsSame(Int32Type.instance, i1);
        }

    }

    @Test
    public void testLongs()
    {
        testType(LongType.instance, testLongs);
        testDirect(ByteSource::of, ByteSourceInverse::getSignedLong, testLongs);
    }

    @Test
    public void testShorts()
    {
        testType(ShortType.instance, testShorts);
    }

    @Test
    public void testBytes()
    {
        testType(ByteType.instance, testBytes);
    }

    @Test
    public void testDoubles()
    {
        testType(DoubleType.instance, testDoubles);
    }

    @Test
    public void testFloats()
    {
        testType(FloatType.instance, testFloats);
    }

    @Test
    public void testBigInts()
    {
        testType(IntegerType.instance, testBigInts);
    }

    @Test
    public void testBigDecimals()
    {
        testTypeBuffers(DecimalType.instance, testBigDecimals);
    }

    @Test
    public void testUUIDs()
    {
        testType(UUIDType.instance, testUUIDs);
    }

    @Test
    public void testTimeUUIDs()
    {
        testType(TimeUUIDType.instance, Arrays.stream(testUUIDs)
                                              .filter(x -> x == null || x.version() == 1)
                                              .map(x -> x != null ? TimeUUID.fromUuid(x) : null)
                                              .toArray());
    }

    @Test
    public void testLexicalUUIDs()
    {
        testType(LexicalUUIDType.instance, testUUIDs);
    }

    @Test
    public void testSimpleDate()
    {
        testType(SimpleDateType.instance, Arrays.stream(testInts).filter(x -> x != null).toArray());
    }

    @Test
    public void testTimeType()
    {
        testType(TimeType.instance, Arrays.stream(testLongs).filter(x -> x != null && x >= 0 && x <= 24L * 60 * 60 * 1000 * 1000 * 1000).toArray());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDateType()
    {
        testType(DateType.instance, testDates);
    }

    @Test
    public void testTimestampType()
    {
        testType(TimestampType.instance, testDates);
    }

    @Test
    public void testBytesType()
    {
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < testValues.length; ++i)
            for (Object o : testValues[i])
                values.add(testTypes[i].decompose(o));

        testType(BytesType.instance, values);
    }

    @Test
    public void testInetAddressType() throws UnknownHostException
    {
        testType(InetAddressType.instance, testInets);
    }

    @Test
    public void testEmptyType()
    {
        testType(EmptyType.instance, new Void[] { null });
    }

    @Test
    public void testPatitionerDefinedOrder()
    {
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < testValues.length; ++i)
            for (Object o : testValues[i])
                values.add(testTypes[i].decompose(o));

        testBuffers(new PartitionerDefinedOrder(Murmur3Partitioner.instance), values);
        testBuffers(new PartitionerDefinedOrder(RandomPartitioner.instance), values);
        testBuffers(new PartitionerDefinedOrder(ByteOrderedPartitioner.instance), values);
    }

    @Test
    public void testPatitionerOrder()
    {
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < testValues.length; ++i)
            for (Object o : testValues[i])
                values.add(testTypes[i].decompose(o));

        testDecoratedKeys(Murmur3Partitioner.instance, values);
        testDecoratedKeys(RandomPartitioner.instance, values);
        testDecoratedKeys(ByteOrderedPartitioner.instance, values);
    }

    @Test
    public void testLocalPatitionerOrder()
    {
        for (int i = 0; i < testValues.length; ++i)
        {
            final AbstractType testType = testTypes[i];
            testDecoratedKeys(new LocalPartitioner(testType), Lists.transform(Arrays.asList(testValues[i]),
                                                                                            v -> testType.decompose(v)));
        }
    }

    interface PairTester
    {
        void test(AbstractType t1, AbstractType t2, Object o1, Object o2);
    }

    void testCombinationSampling(Random rand, PairTester tester)
    {
        for (int i=0;i<testTypes.length;++i)
            for (int j=0;j<testTypes.length;++j)
            {
                Object[] tv1 = new Object[3];
                Object[] tv2 = new Object[3];
                for (int t=0; t<tv1.length; ++t)
                {
                    tv1[t] = testValues[i][rand.nextInt(testValues[i].length)];
                    tv2[t] = testValues[j][rand.nextInt(testValues[j].length)];
                }

                for (Object o1 : tv1)
                    for (Object o2 : tv2)

                {
                    tester.test(testTypes[i], testTypes[j], o1, o2);
                }
            }
    }

    @Test
    public void testCombinations()
    {
        Random rand = new Random(0);
        testCombinationSampling(rand, this::assertClusteringPairConvertsSame);
    }

    @Test
    public void testNullsInClustering()
    {
        ByteBuffer[][] inputs = new ByteBuffer[][]
                                {
                                new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, "a"),
                                                  decomposeAndRandomPad(Int32Type.instance, 0)},
                                new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, "a"),
                                                  decomposeAndRandomPad(Int32Type.instance, null)},
                                new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, "a"),
                                                  null},
                                new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, ""),
                                                  decomposeAndRandomPad(Int32Type.instance, 0)},
                                new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, ""),
                                                  decomposeAndRandomPad(Int32Type.instance, null)},
                                new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, ""),
                                                  null},
                                new ByteBuffer[] {null,
                                                  decomposeAndRandomPad(Int32Type.instance, 0)},
                                new ByteBuffer[] {null,
                                                  decomposeAndRandomPad(Int32Type.instance, null)},
                                new ByteBuffer[] {null,
                                                  null},
                                };
        for (ByteBuffer[] input : inputs)
        {
            assertClusteringPairConvertsSame(ByteBufferAccessor.instance,
                                             UTF8Type.instance,
                                             Int32Type.instance,
                                             input[0],
                                             input[1],
                                             (t, v) -> (ByteBuffer) v);
        }
    }

    @Test
    public void testEmptyClustering()
    {
        ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;
        ClusteringComparator comp = new ClusteringComparator();
        EnumSet<ClusteringPrefix.Kind> skippedKinds = EnumSet.of(ClusteringPrefix.Kind.SSTABLE_LOWER_BOUND, ClusteringPrefix.Kind.SSTABLE_UPPER_BOUND);
        for (ClusteringPrefix.Kind kind : EnumSet.complementOf(skippedKinds))
        {
            if (kind.isBoundary())
                continue;

            ClusteringPrefix<ByteBuffer> empty = ByteSourceComparisonTest.makeBound(kind);
            ClusteringPrefix<ByteBuffer> converted = getClusteringPrefix(accessor, kind, comp, comp.asByteComparable(empty));
            assertEquals(empty, converted);
        }
    }

    void assertClusteringPairConvertsSame(AbstractType t1, AbstractType t2, Object o1, Object o2)
    {
        for (ValueAccessor<?> accessor : ValueAccessors.ACCESSORS)
            assertClusteringPairConvertsSame(accessor, t1, t2, o1, o2, AbstractType::decompose);
    }

    <V> void assertClusteringPairConvertsSame(ValueAccessor<V> accessor,
                                              AbstractType<?> t1, AbstractType<?> t2,
                                              Object o1, Object o2,
                                              BiFunction<AbstractType, Object, ByteBuffer> decompose)
    {
        boolean checkEquals = t1 != DecimalType.instance && t2 != DecimalType.instance;
        EnumSet<ClusteringPrefix.Kind> skippedKinds = EnumSet.of(ClusteringPrefix.Kind.SSTABLE_LOWER_BOUND, ClusteringPrefix.Kind.SSTABLE_UPPER_BOUND);
        for (ClusteringPrefix.Kind k1 : EnumSet.complementOf(skippedKinds))
            {
                ClusteringComparator comp = new ClusteringComparator(t1, t2);
                V[] b = accessor.createArray(2);
                b[0] = accessor.valueOf(decompose.apply(t1, o1));
                b[1] = accessor.valueOf(decompose.apply(t2, o2));
                ClusteringPrefix<V> c = ByteSourceComparisonTest.makeBound(accessor.factory(), k1, b);
                final ByteComparable bsc = comp.asByteComparable(c);
                logger.info("Clustering {} bytesource {}", c.clusteringString(comp.subtypes()), bsc.byteComparableAsString(VERSION));
                ClusteringPrefix<V> converted = getClusteringPrefix(accessor, k1, comp, bsc);
                assertEquals(String.format("Failed compare(%s, converted %s ByteSource %s) == 0\ntype %s",
                                           safeStr(c.clusteringString(comp.subtypes())),
                                           safeStr(converted.clusteringString(comp.subtypes())),
                                           bsc.byteComparableAsString(VERSION),
                                           comp),
                             0, comp.compare(c, converted));
                if (checkEquals)
                    assertEquals(String.format("Failed equals %s, got %s ByteSource %s\ntype %s",
                                               safeStr(c.clusteringString(comp.subtypes())),
                                               safeStr(converted.clusteringString(comp.subtypes())),
                                               bsc.byteComparableAsString(VERSION),
                                               comp),
                                 c, converted);

                ClusteringComparator compR = new ClusteringComparator(ReversedType.getInstance(t1), ReversedType.getInstance(t2));
                final ByteComparable bsrc = compR.asByteComparable(c);
                converted = getClusteringPrefix(accessor, k1, compR, bsrc);
                assertEquals(String.format("Failed reverse compare(%s, converted %s ByteSource %s) == 0\ntype %s",
                                           safeStr(c.clusteringString(compR.subtypes())),
                                           safeStr(converted.clusteringString(compR.subtypes())),
                                           bsrc.byteComparableAsString(VERSION),
                                           compR),
                             0, compR.compare(c, converted));
                if (checkEquals)
                    assertEquals(String.format("Failed reverse equals %s, got %s ByteSource %s\ntype %s",
                                               safeStr(c.clusteringString(compR.subtypes())),
                                               safeStr(converted.clusteringString(compR.subtypes())),
                                               bsrc.byteComparableAsString(VERSION),
                                               compR),
                                 c, converted);
            }
    }

    private static <V> ClusteringPrefix<V> getClusteringPrefix(ValueAccessor<V> accessor,
                                                               ClusteringPrefix.Kind k1,
                                                               ClusteringComparator comp,
                                                               ByteComparable bsc)
    {
        switch (k1)
        {
        case STATIC_CLUSTERING:
        case CLUSTERING:
            return comp.clusteringFromByteComparable(accessor, bsc);
        case EXCL_END_BOUND:
        case INCL_END_BOUND:
            return comp.boundFromByteComparable(accessor, bsc, true);
        case INCL_START_BOUND:
        case EXCL_START_BOUND:
            return comp.boundFromByteComparable(accessor, bsc, false);
        case EXCL_END_INCL_START_BOUNDARY:
        case INCL_END_EXCL_START_BOUNDARY:
            return comp.boundaryFromByteComparable(accessor, bsc);
        default:
            throw new AssertionError();
        }
    }

    private static ByteSource.Peekable source(ByteComparable bsc)
    {
        if (bsc == null)
            return null;
        return ByteSource.peekable(bsc.asComparableBytes(VERSION));
    }

    @Test
    public void testTupleType()
    {
        Random rand = ThreadLocalRandom.current();
        testCombinationSampling(rand, this::assertTupleConvertsSame);
    }

    @Test
    public void testTupleTypeNonFull()
    {
        TupleType tt = new TupleType(ImmutableList.of(UTF8Type.instance, Int32Type.instance));
        List<ByteBuffer> tests = ImmutableList.of
            (
            TupleType.buildValue(ByteBufferAccessor.instance,
                                 decomposeAndRandomPad(UTF8Type.instance, ""),
                                 decomposeAndRandomPad(Int32Type.instance, 0)),
            // Note: a decomposed null (e.g. decomposeAndRandomPad(Int32Type.instance, null)) should not reach a tuple
            TupleType.buildValue(ByteBufferAccessor.instance,
                                 decomposeAndRandomPad(UTF8Type.instance, ""),
                                 null),
            TupleType.buildValue(ByteBufferAccessor.instance,
                                 null,
                                 decomposeAndRandomPad(Int32Type.instance, 0)),
            TupleType.buildValue(ByteBufferAccessor.instance, decomposeAndRandomPad(UTF8Type.instance, "")),
            TupleType.buildValue(ByteBufferAccessor.instance, (ByteBuffer) null),
            TupleType.buildValue(ByteBufferAccessor.instance)
            );
        testBuffers(tt, tests);
    }

    void assertTupleConvertsSame(AbstractType t1, AbstractType t2, Object o1, Object o2)
    {
        TupleType tt = new TupleType(ImmutableList.of(t1, t2));
        ByteBuffer b1 = TupleType.buildValue(ByteBufferAccessor.instance,
                                             decomposeForTuple(t1, o1),
                                             decomposeForTuple(t2, o2));
        assertConvertsSameBuffers(tt, b1);
    }

    @Test
    public void testCompositeType()
    {
        Random rand = new Random(0);
        testCombinationSampling(rand, this::assertCompositeConvertsSame);
    }

    @Test
    public void testCompositeTypeNonFull()
    {
        CompositeType tt = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance);
        List<ByteBuffer> tests = ImmutableList.of
            (
            CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(UTF8Type.instance, ""), decomposeAndRandomPad(Int32Type.instance, 0)),
            CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(UTF8Type.instance, ""), decomposeAndRandomPad(Int32Type.instance, null)),
            CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(UTF8Type.instance, "")),
            CompositeType.build(ByteBufferAccessor.instance),
            CompositeType.build(ByteBufferAccessor.instance, true, decomposeAndRandomPad(UTF8Type.instance, "")),
            CompositeType.build(ByteBufferAccessor.instance,true)
            );
        for (ByteBuffer b : tests)
            tt.validate(b);
        testBuffers(tt, tests);
    }

    void assertCompositeConvertsSame(AbstractType t1, AbstractType t2, Object o1, Object o2)
    {
        CompositeType tt = CompositeType.getInstance(t1, t2);
        ByteBuffer b1 = CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(t1, o1), decomposeAndRandomPad(t2, o2));
        assertConvertsSameBuffers(tt, b1);
    }

    @Test
    public void testDynamicComposite()
    {
        DynamicCompositeType tt = DynamicCompositeType.getInstance(DynamicCompositeTypeTest.aliases);
        UUID[] uuids = DynamicCompositeTypeTest.uuids;
        List<ByteBuffer> tests = ImmutableList.of
            (
            DynamicCompositeTypeTest.createDynamicCompositeKey("test1", null, -1, false, true),
            DynamicCompositeTypeTest.createDynamicCompositeKey("test1", uuids[0], 24, false, true),
            DynamicCompositeTypeTest.createDynamicCompositeKey("test1", uuids[0], 42, false, true),
            DynamicCompositeTypeTest.createDynamicCompositeKey("test2", uuids[0], -1, false, true),
            DynamicCompositeTypeTest.createDynamicCompositeKey("test2", uuids[1], 42, false, true)
            );
        for (ByteBuffer b : tests)
            tt.validate(b);
        testBuffers(tt, tests);
    }

    @Test
    public void testListTypeString()
    {
        testCollection(ListType.getInstance(UTF8Type.instance, true), testStrings, () -> new ArrayList<>(), new Random());
    }

    @Test
    public void testListTypeLong()
    {
        testCollection(ListType.getInstance(LongType.instance, true), testLongs, () -> new ArrayList<>(), new Random());
    }

    @Test
    public void testSetTypeString()
    {
        testCollection(SetType.getInstance(UTF8Type.instance, true), testStrings, () -> new HashSet<>(), new Random());
    }

    @Test
    public void testSetTypeLong()
    {
        testCollection(SetType.getInstance(LongType.instance, true), testLongs, () -> new HashSet<>(), new Random());
    }

    <T, CT extends Collection<T>> void testCollection(CollectionType<CT> tt, T[] values, Supplier<CT> gen, Random rand)
    {
        int cnt = 0;
        List<CT> tests = new ArrayList<>();
        tests.add(gen.get());
        for (int c = 1; c <= 3; ++c)
            for (int j = 0; j < 5; ++j)
            {
                CT l = gen.get();
                for (int i = 0; i < c; ++i)
                {
                    T value = values[cnt++ % values.length];
                    if (value != null)
                        l.add(value);
                }

                tests.add(l);
            }
        testType(tt, tests);
    }

    @Test
    public void testMapTypeStringLong()
    {
        testMap(MapType.getInstance(UTF8Type.instance, LongType.instance, true), testStrings, testLongs, () -> new HashMap<>(), new Random());
    }

    @Test
    public void testMapTypeStringLongTree()
    {
        testMap(MapType.getInstance(UTF8Type.instance, LongType.instance, true), testStrings, testLongs, () -> new TreeMap<>(), new Random());
    }

    <K, V, M extends Map<K, V>> void testMap(MapType<K, V> tt, K[] keys, V[] values, Supplier<M> gen, Random rand)
    {
        List<M> tests = new ArrayList<>();
        tests.add(gen.get());
        for (int c = 1; c <= 3; ++c)
            for (int j = 0; j < 5; ++j)
            {
                M l = gen.get();
                for (int i = 0; i < c; ++i)
                {
                    V value = values[rand.nextInt(values.length)];
                    if (value != null)
                        l.put(keys[rand.nextInt(keys.length)], value);
                }

                tests.add(l);
            }
        testType(tt, tests);
    }

    /*
     * Convert type to a comparable.
     */
    private ByteComparable typeToComparable(AbstractType<?> type, ByteBuffer value)
    {
        return new ByteComparable()
        {
            @Override
            public ByteSource asComparableBytes(Version v)
            {
                return type.asComparableBytes(value, v);
            }

            @Override
            public String toString()
            {
                return type.getString(value);
            }
        };
    }

    public <T> void testType(AbstractType<T> type, Object[] values)
    {
        testType(type, Iterables.transform(Arrays.asList(values), x -> (T) x));
    }

    public <T> void testType(AbstractType<? super T> type, Iterable<T> values)
    {
        for (T i : values) {
            ByteBuffer b = decomposeAndRandomPad(type, i);
            logger.info("Value {} ({}) bytes {} ByteSource {}",
                              safeStr(i),
                              safeStr(type.getSerializer().toCQLLiteral(b)),
                              safeStr(ByteBufferUtil.bytesToHex(b)),
                              typeToComparable(type, b).byteComparableAsString(VERSION));
            assertConvertsSame(type, i);
        }
        if (!type.isReversed())
            testType(ReversedType.getInstance(type), values);
    }

    public <T> void testTypeBuffers(AbstractType<T> type, Object[] values)
    {
        testTypeBuffers(type, Lists.transform(Arrays.asList(values), x -> (T) x));
    }

    public <T> void testTypeBuffers(AbstractType<T> type, List<T> values)
    {
        // Main difference with above is that we use type.compare instead of checking equals
        testBuffers(type, Lists.transform(values, value -> decomposeAndRandomPad(type, value)));

    }
    public void testBuffers(AbstractType<?> type, List<ByteBuffer> values)
    {
        try
        {
            for (ByteBuffer b : values) {
                logger.info("Value {} bytes {} ByteSource {}",
                            safeStr(type.getSerializer().toCQLLiteral(b)),
                            safeStr(ByteBufferUtil.bytesToHex(b)),
                            typeToComparable(type, b).byteComparableAsString(VERSION));
            }
        }
        catch (UnsupportedOperationException e)
        {
            // Continue without listing values.
        }

        for (ByteBuffer i : values)
            assertConvertsSameBuffers(type, i);
    }

    void assertConvertsSameBuffers(AbstractType<?> type, ByteBuffer b1)
    {
        final ByteComparable bs1 = typeToComparable(type, b1);

        ByteBuffer actual = type.fromComparableBytes(source(bs1), VERSION);
        assertEquals(String.format("Failed compare(%s, converted %s (bytesource %s))",
                                   ByteBufferUtil.bytesToHex(b1),
                                   ByteBufferUtil.bytesToHex(actual),
                                   bs1.byteComparableAsString(VERSION)),
                     0,
                     type.compare(b1, actual));
    }

    public void testDecoratedKeys(IPartitioner type, List<ByteBuffer> values)
    {
        for (ByteBuffer i : values)
            assertConvertsSameDecoratedKeys(type, i);
    }

    void assertConvertsSameDecoratedKeys(IPartitioner type, ByteBuffer b1)
    {
        DecoratedKey k1 = type.decorateKey(b1);
        DecoratedKey actual = BufferDecoratedKey.fromByteComparable(k1, VERSION, type);

        assertEquals(String.format("Failed compare(%s[%s bs %s], %s[%s bs %s])\npartitioner %s",
                                   k1,
                                   ByteBufferUtil.bytesToHex(b1),
                                   k1.byteComparableAsString(VERSION),
                                   actual,
                                   ByteBufferUtil.bytesToHex(actual.getKey()),
                                   actual.byteComparableAsString(VERSION),
                                   type),
                     0,
                     k1.compareTo(actual));
        assertEquals(String.format("Failed equals(%s[%s bs %s], %s[%s bs %s])\npartitioner %s",
                                   k1,
                                   ByteBufferUtil.bytesToHex(b1),
                                   k1.byteComparableAsString(VERSION),
                                   actual,
                                   ByteBufferUtil.bytesToHex(actual.getKey()),
                                   actual.byteComparableAsString(VERSION),
                                   type),
                     k1,
                     actual);
    }

    static Object safeStr(Object i)
    {
        if (i == null)
            return null;
        if (i instanceof ByteBuffer)
        {
            ByteBuffer buf = (ByteBuffer) i;
            i = ByteBufferUtil.bytesToHex(buf);
        }
        String s = i.toString();
        if (s.length() > 100)
            s = s.substring(0, 100) + "...";
        return s.replaceAll("\0", "<0>");
    }

    public <T> void testDirect(Function<T, ByteSource> convertor, Function<ByteSource.Peekable, T> inverse, T[] values)
    {
        for (T i : values) {
            if (i == null)
                continue;

            logger.info("Value {} ByteSource {}\n",
                              safeStr(i),
                              convertor.apply(i));

        }
        for (T i : values)
            if (i != null)
                assertConvertsSame(convertor, inverse, i);
    }

    <T> void assertConvertsSame(Function<T, ByteSource> convertor, Function<ByteSource.Peekable, T> inverse, T v1)
    {
        ByteComparable b1 = v -> convertor.apply(v1);
        T actual = inverse.apply(source(b1));
        assertEquals(String.format("ByteSource %s", b1.byteComparableAsString(VERSION)), v1, actual);
    }

    <T> void assertConvertsSame(AbstractType<T> type, T v1)
    {
        ByteBuffer b1 = decomposeAndRandomPad(type, v1);
        final ByteComparable bc1 = typeToComparable(type, b1);
        ByteBuffer convertedBuffer = type.fromComparableBytes(source(bc1), VERSION);
        T actual = type.compose(convertedBuffer);

        assertEquals(String.format("Failed equals %s(%s bs %s), got %s",
                                   safeStr(v1),
                                   ByteBufferUtil.bytesToHex(b1),
                                   safeStr(bc1.byteComparableAsString(VERSION)),
                                   safeStr(actual)),
                     v1,
                     actual);
    }

    <T> ByteBuffer decomposeAndRandomPad(AbstractType<T> type, T v)
    {
        ByteBuffer b = type.decompose(v);
        Random rand = new Random(0);
        int padBefore = rand.nextInt(16);
        int padAfter = rand.nextInt(16);
        int paddedCapacity = b.remaining() + padBefore + padAfter;
        ByteBuffer padded = allocateBuffer(paddedCapacity);
        rand.ints(padBefore).forEach(x -> padded.put((byte) x));
        padded.put(b.duplicate());
        rand.ints(padAfter).forEach(x -> padded.put((byte) x));
        padded.clear().limit(padded.capacity() - padAfter).position(padBefore);
        return padded;
    }

    protected ByteBuffer allocateBuffer(int paddedCapacity)
    {
        return ByteBuffer.allocate(paddedCapacity);
    }
}