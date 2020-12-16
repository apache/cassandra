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

import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
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
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;

import static org.junit.Assert.assertEquals;

/**
 * Tests forward conversion to ByteSource/ByteComparable and that the result compares correctly.
 */
public class ByteSourceComparisonTest extends ByteSourceTestBase
{
    private final static Logger logger = LoggerFactory.getLogger(ByteSourceComparisonTest.class);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testStringsAscii()
    {
        testType(AsciiType.instance, testStrings);
    }

    @Test
    public void testStringsUTF8()
    {
        testType(UTF8Type.instance, testStrings);
        testDirect(x -> ByteSource.of(x, Version.OSS41), Ordering.<String>natural()::compare, testStrings);
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
        testDirect(x -> ByteSource.of(x), Integer::compare, testInts);
    }

    @Test
    public void randomTestInts()
    {
        Random rand = new Random();
        for (int i=0; i<10000; ++i)
        {
            int i1 = rand.nextInt();
            int i2 = rand.nextInt();
            assertComparesSame(Int32Type.instance, i1, i2);
        }

    }

    @Test
    public void testLongs()
    {
        testType(LongType.instance, testLongs);
        testDirect(x -> ByteSource.of(x), Long::compare, testLongs);
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
        testType(DecimalType.instance, testBigDecimals);
    }

    @Test
    public void testBigDecimalInCombination()
    {
        BigDecimal b1 = new BigDecimal("123456.78901201");
        BigDecimal b2 = new BigDecimal("123456.789012");
        Boolean b = false;

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = b1.negate();
        b2 = b2.negate();

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = new BigDecimal("-123456.78901289");
        b2 = new BigDecimal("-123456.789012");

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = new BigDecimal("1");
        b2 = new BigDecimal("1.1");

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);

        b1 = b1.negate();
        b2 = b2.negate();

        assertClusteringPairComparesSame(DecimalType.instance, BooleanType.instance, b1, b, b2, b);
        assertClusteringPairComparesSame(BooleanType.instance, DecimalType.instance, b, b1, b, b2);
    }

    @Test
    public void testUUIDs()
    {
        testType(UUIDType.instance, testUUIDs);
    }

    @Test
    public void testTimeUUIDs()
    {
        testType(TimeUUIDType.instance, Arrays.stream(testUUIDs).filter(x -> x == null || x.version() == 1).toArray());
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

        testType(BytesType.instance, values.toArray());
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
        void test(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4);
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
                        for (Object o3 : tv1)
                            for (Object o4 : tv2)

                {
                    tester.test(testTypes[i], testTypes[j], o1, o2, o3, o4);
                }
            }
    }

    @Test
    public void testCombinations()
    {
        Random rand = new Random(0);
        testCombinationSampling(rand, this::assertClusteringPairComparesSame);
    }

    void assertClusteringPairComparesSame(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4)
    {
        for (Version v : Version.values())
            for (ClusteringPrefix.Kind k1 : ClusteringPrefix.Kind.values())
                for (ClusteringPrefix.Kind k2 : ClusteringPrefix.Kind.values())
                {
                    ClusteringComparator comp = new ClusteringComparator(t1, t2);
                    ByteBuffer[] b = new ByteBuffer[2];
                    ByteBuffer[] d = new ByteBuffer[2];
                    b[0] = t1.decompose(o1);
                    b[1] = t2.decompose(o2);
                    d[0] = t1.decompose(o3);
                    d[1] = t2.decompose(o4);
                    ClusteringPrefix<ByteBuffer> c = makeBound(k1, b);
                    ClusteringPrefix<ByteBuffer> e = makeBound(k2, d);
                    final ByteComparable bsc = comp.asByteComparable(c);
                    final ByteComparable bse = comp.asByteComparable(e);
                    int expected = Integer.signum(comp.compare(c, e));
                    assertEquals(String.format("Failed comparing %s and %s, %s vs %s version %s",
                                               safeStr(c.clusteringString(comp.subtypes())),
                                               safeStr(e.clusteringString(comp.subtypes())), bsc, bse, v),
                                 expected, Integer.signum(ByteComparable.compare(bsc, bse, v)));
                    maybeCheck41Properties(expected, bsc, bse, v);
                    maybeAssertNotPrefix(bsc, bse, v);

                    ClusteringComparator compR = new ClusteringComparator(ReversedType.getInstance(t1), ReversedType.getInstance(t2));
                    final ByteComparable bsrc = compR.asByteComparable(c);
                    final ByteComparable bsre = compR.asByteComparable(e);
                    int expectedR = Integer.signum(compR.compare(c, e));
                    assertEquals(String.format("Failed comparing reversed %s and %s, %s vs %s version %s",
                                               safeStr(c.clusteringString(comp.subtypes())),
                                               safeStr(e.clusteringString(comp.subtypes())), bsrc, bsre, v),
                                 expectedR, Integer.signum(ByteComparable.compare(bsrc, bsre, v)));
                    maybeCheck41Properties(expectedR, bsrc, bsre, v);
                    maybeAssertNotPrefix(bsrc, bsre, v);
                }
    }

    static ClusteringPrefix<ByteBuffer> makeBound(ClusteringPrefix.Kind k1, ByteBuffer[] b)
    {
        return makeBound(ByteBufferAccessor.instance.factory(), k1, b);
    }

    static <V> ClusteringPrefix<V> makeBound(ValueAccessor.ObjectFactory<V> factory, ClusteringPrefix.Kind k1, V[] b)
    {
        switch (k1)
        {
        case INCL_END_EXCL_START_BOUNDARY:
        case EXCL_END_INCL_START_BOUNDARY:
            return factory.boundary(k1, b);

        case INCL_END_BOUND:
        case EXCL_END_BOUND:
        case INCL_START_BOUND:
        case EXCL_START_BOUND:
            return factory.bound(k1, b);

        case CLUSTERING:
            return factory.clustering(b);

        case STATIC_CLUSTERING:
            return factory.staticClustering();

        default:
            throw new AssertionError();
        }
    }

    @Test
    public void testTupleType()
    {
        Random rand = ThreadLocalRandom.current();
        testCombinationSampling(rand, this::assertTupleComparesSame);
    }

    @Test
    public void testTupleTypeNonFull()
    {
        TupleType tt = new TupleType(ImmutableList.of(UTF8Type.instance, Int32Type.instance));
        List<ByteBuffer> tests = ImmutableList.of
            (
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, ""),
                                                                                decomposeAndRandomPad(Int32Type.instance, 0)}),
            // Note: a decomposed null (e.g. decomposeAndRandomPad(Int32Type.instance, null)) should not reach a tuple
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, ""),
                                                                                null}),
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {null,
                                                                                decomposeAndRandomPad(Int32Type.instance, 0)}),
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeAndRandomPad(UTF8Type.instance, "")}),
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {null}),
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[0])
            );
        testBuffers(tt, tests);
    }

    void assertTupleComparesSame(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4)
    {
        TupleType tt = new TupleType(ImmutableList.of(t1, t2));
        ByteBuffer b1 = TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeForTuple(t1, o1),
                                                                                            decomposeForTuple(t2, o2)});
        ByteBuffer b2 = TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeForTuple(t1, o3),
                                                                                            decomposeForTuple(t2, o4)});
        assertComparesSameBuffers(tt, b1, b2);
    }

    static ByteBuffer decomposeForTuple(AbstractType t, Object o)
    {
        return o != null ? t.decompose(o) : null;
    }

    @Test
    public void testCompositeType()
    {
        Random rand = new Random(0);
        testCombinationSampling(rand, this::assertCompositeComparesSame);
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

    void assertCompositeComparesSame(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4)
    {
        CompositeType tt = CompositeType.getInstance(t1, t2);
        ByteBuffer b1 = CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(t1, o1), decomposeAndRandomPad(t2, o2));
        ByteBuffer b2 = CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(t1, o3), decomposeAndRandomPad(t2, o4));
        assertComparesSameBuffers(tt, b1, b2);
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
                    l.add(values[cnt++ % values.length]);

                tests.add(l);
            }
        testType(tt, tests.toArray());
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

    @Test
    public void testDecoratedKeyPrefixesVOSS41()
    {
        // This should pass with the OSS 4.1 encoding
        testDecoratedKeyPrefixes(Version.OSS41);
    }

    @Test
    public void testDecoratedKeyPrefixesVLegacy()
    {
        // ... and fail with the legacy encoding
        try
        {
            testDecoratedKeyPrefixes(Version.LEGACY);
        }
        catch (AssertionError e)
        {
            // Correct path, test failing.
            return;
        }
        Assert.fail("Test expected to fail.");
    }

    @Test
    public void testFixedLengthWithOffset()
    {
        byte[] bytes = new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        ByteSource source = ByteSource.fixedLength(bytes, 0, 1);
        assertEquals(1, source.next());
        assertEquals(ByteSource.END_OF_STREAM, source.next());

        source = ByteSource.fixedLength(bytes, 4, 5);
        assertEquals(5, source.next());
        assertEquals(6, source.next());
        assertEquals(7, source.next());
        assertEquals(8, source.next());
        assertEquals(9, source.next());
        assertEquals(ByteSource.END_OF_STREAM, source.next());

        ByteSource.fixedLength(bytes, 9, 0);
        assertEquals(ByteSource.END_OF_STREAM, source.next());
    }

    @Test
    public void testFixedLengthNegativeLength()
    {
        byte[] bytes = new byte[]{ 1, 2, 3 };

        expectedException.expect(IllegalArgumentException.class);
        ByteSource.fixedLength(bytes, 0, -1);
    }

    @Test
    public void testFixedLengthNegativeOffset()
    {
        byte[] bytes = new byte[]{ 1, 2, 3 };

        expectedException.expect(IllegalArgumentException.class);
        ByteSource.fixedLength(bytes, -1, 1);
    }

    @Test
    public void testFixedLengthOutOfBounds()
    {
        byte[] bytes = new byte[]{ 1, 2, 3 };

        expectedException.expect(IllegalArgumentException.class);
        ByteSource.fixedLength(bytes, 0, 4);
    }

    @Test
    public void testFixedOffsetOutOfBounds()
    {
        byte[] bytes = new byte[]{ 1, 2, 3 };

        expectedException.expect(IllegalArgumentException.class);
        ByteSource.fixedLength(bytes, 4, 1);
    }

    public void testDecoratedKeyPrefixes(Version version)
    {
        testDecoratedKeyPrefixes("012345678BCDE\0", "", version);
        testDecoratedKeyPrefixes("012345678ABCDE\0", "ABC", version);
        testDecoratedKeyPrefixes("0123456789ABCDE\0", "\0AB", version);
        testDecoratedKeyPrefixes("0123456789ABCDEF\0", "\0", version);

        testDecoratedKeyPrefixes("0123456789ABCDEF0", "ABC", version);
        testDecoratedKeyPrefixes("0123456789ABCDEF", "", version);
        testDecoratedKeyPrefixes("0123456789ABCDE", "", version);
        testDecoratedKeyPrefixes("0123456789ABCD", "\0AB", version);
        testDecoratedKeyPrefixes("0123456789ABC", "\0", version);

    }

    public void testDecoratedKeyPrefixes(String key, String append, Version version)
    {
        logger.info("Testing {} + {}", safeStr(key), safeStr(append));
        IPartitioner partitioner = Murmur3Partitioner.instance;
        ByteBuffer original = ByteBufferUtil.bytes(key);
        ByteBuffer collision = Util.generateMurmurCollision(original, append.getBytes(StandardCharsets.UTF_8));

        long[] hash = new long[2];
        MurmurHash.hash3_x64_128(original, 0, original.limit(), 0, hash);
        logger.info(String.format("Original hash  %016x,%016x", hash[0], hash[1]));
        MurmurHash.hash3_x64_128(collision, 0, collision.limit(), 0, hash);
        logger.info(String.format("Collision hash %016x,%016x", hash[0], hash[1]));

        DecoratedKey kk1 = partitioner.decorateKey(original);
        DecoratedKey kk2 = partitioner.decorateKey(collision);
        logger.info("{}\n{}\n{}\n{}", kk1, kk2, kk1.byteComparableAsString(version), kk2.byteComparableAsString(version));

        final ByteSource s1 = kk1.asComparableBytes(version);
        final ByteSource s2 = kk2.asComparableBytes(version);
        logger.info("{}\n{}", s1, s2);

        // Check that the representations compare correctly
        Assert.assertEquals(Long.signum(kk1.compareTo(kk2)), ByteComparable.compare(kk1, kk2, version));
        // s1 must not be a prefix of s2
        assertNotPrefix(s1, s2);
    }

    private void assertNotPrefix(ByteSource s1, ByteSource s2)
    {
        int c1, c2;
        do
        {
            c1 = s1.next();
            c2 = s2.next();
        }
        while (c1 == c2 && c1 != ByteSource.END_OF_STREAM);

        // Equal is ok
        if (c1 == c2)
            return;

        Assert.assertNotEquals("ByteComparable is a prefix of other", ByteSource.END_OF_STREAM, c1);
        Assert.assertNotEquals("ByteComparable is a prefix of other", ByteSource.END_OF_STREAM, c2);
    }

    private int compare(ByteSource s1, ByteSource s2)
    {
        int c1, c2;
        do
        {
            c1 = s1.next();
            c2 = s2.next();
        }
        while (c1 == c2 && c1 != ByteSource.END_OF_STREAM);

        return Integer.compare(c1, c2);
    }

    private void maybeAssertNotPrefix(ByteComparable s1, ByteComparable s2, Version version)
    {
        if (version == Version.OSS41)
            assertNotPrefix(s1.asComparableBytes(version), s2.asComparableBytes(version));
    }

    private void maybeCheck41Properties(int expectedComparison, ByteComparable s1, ByteComparable s2, Version version)
    {
        if (version != Version.OSS41)
            return;

        if (s1 == null || s2 == null || 0 == expectedComparison)
            return;
        int b1 = ThreadLocalRandom.current().nextInt(ByteSource.MIN_SEPARATOR, ByteSource.MAX_SEPARATOR + 1);
        int b2 = ThreadLocalRandom.current().nextInt(ByteSource.MIN_SEPARATOR, ByteSource.MAX_SEPARATOR + 1);
        assertEquals(String.format("Comparison failed for %s(%s + %02x) and %s(%s + %02x)", s1, s1.byteComparableAsString(version), b1, s2, s2.byteComparableAsString(version), b2),
                expectedComparison, Integer.signum(compare(ByteSource.withTerminator(b1, s1.asComparableBytes(version)), ByteSource.withTerminator(b2, s2.asComparableBytes(version)))));
        assertNotPrefix(ByteSource.withTerminator(b1, s1.asComparableBytes(version)), ByteSource.withTerminator(b2, s2.asComparableBytes(version)));
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
                    l.put(keys[rand.nextInt(keys.length)], values[rand.nextInt(values.length)]);

                tests.add(l);
            }
        testType(tt, tests.toArray());
    }

    /*
     * Convert type to a comparable.
     */
    private ByteComparable typeToComparable(AbstractType type, ByteBuffer value)
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

    public void testType(AbstractType type, Object[] values)
    {
        for (Object i : values) {
            ByteBuffer b = decomposeAndRandomPad(type, i);
            logger.info("Value {} ({}) bytes {} ByteSource {}",
                              safeStr(i),
                              safeStr(type.getSerializer().toCQLLiteral(b)),
                              safeStr(ByteBufferUtil.bytesToHex(b)),
                              typeToComparable(type, b).byteComparableAsString(Version.OSS41));
        }
        for (Object i : values)
            for (Object j : values)
                assertComparesSame(type, i, j);
        if (!type.isReversed())
            testType(ReversedType.getInstance(type), values);
    }

    public void testBuffers(AbstractType type, List<ByteBuffer> values)
    {
        try
        {
            for (ByteBuffer b : values) {
                logger.info("Value {} bytes {} ByteSource {}",
                            safeStr(type.getSerializer().toCQLLiteral(b)),
                            safeStr(ByteBufferUtil.bytesToHex(b)),
                            typeToComparable(type, b).byteComparableAsString(Version.OSS41));
            }
        }
        catch (UnsupportedOperationException e)
        {
            // Continue without listing values.
        }

        for (ByteBuffer i : values)
            for (ByteBuffer j : values)
                assertComparesSameBuffers(type, i, j);
    }

    void assertComparesSameBuffers(AbstractType type, ByteBuffer b1, ByteBuffer b2)
    {
        int expected = Integer.signum(type.compare(b1, b2));
        final ByteComparable bs1 = typeToComparable(type, b1);
        final ByteComparable bs2 = typeToComparable(type, b2);

        for (Version version : Version.values())
        {
            int actual = Integer.signum(ByteComparable.compare(bs1, bs2, version));
            assertEquals(String.format("Failed comparing %s(%s) and %s(%s)", ByteBufferUtil.bytesToHex(b1), bs1.byteComparableAsString(version), ByteBufferUtil.bytesToHex(b2), bs2.byteComparableAsString(version)),
                         expected,
                         actual);
            maybeCheck41Properties(expected, bs1, bs2, version);
        }
    }

    public void testDecoratedKeys(IPartitioner type, List<ByteBuffer> values)
    {
        for (ByteBuffer i : values)
            for (ByteBuffer j : values)
                assertComparesSameDecoratedKeys(type, i, j);
    }

    void assertComparesSameDecoratedKeys(IPartitioner type, ByteBuffer b1, ByteBuffer b2)
    {
        DecoratedKey k1 = type.decorateKey(b1);
        DecoratedKey k2 = type.decorateKey(b2);
        int expected = Integer.signum(k1.compareTo(k2));

        for (Version version : Version.values())
        {
            int actual = Integer.signum(ByteComparable.compare(k1, k2, version));
            assertEquals(String.format("Failed comparing %s[%s](%s) and %s[%s](%s)\npartitioner %s version %s",
                                       ByteBufferUtil.bytesToHex(b1),
                                       k1,
                                       k1.byteComparableAsString(version),
                                       ByteBufferUtil.bytesToHex(b2),
                                       k2,
                                       k2.byteComparableAsString(version),
                                       type,
                                       version),
                         expected,
                         actual);
            maybeAssertNotPrefix(k1, k2, version);
        }
    }

    static Object safeStr(Object i)
    {
        if (i == null)
            return null;
        String s = i.toString();
        if (s.length() > 100)
            s = s.substring(0, 100) + "...";
        return s.replaceAll("\0", "<0>");
    }

    public <T> void testDirect(Function<T, ByteSource> convertor, BiFunction<T, T, Integer> comparator, T[] values)
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
                for (T j : values)
                    if (j != null)
                        assertComparesSame(convertor, comparator, i, j);
    }

    <T> void assertComparesSame(Function<T, ByteSource> convertor, BiFunction<T, T, Integer> comparator, T v1, T v2)
    {
        ByteComparable b1 = v -> convertor.apply(v1);
        ByteComparable b2 = v -> convertor.apply(v2);
        int expected = Integer.signum(comparator.apply(v1, v2));
        int actual = Integer.signum(ByteComparable.compare(b1, b2, null));  // version ignored above
        assertEquals(String.format("Failed comparing %s and %s", v1, v2), expected, actual);
    }

    void assertComparesSame(AbstractType type, Object v1, Object v2)
    {
        ByteBuffer b1 = decomposeAndRandomPad(type, v1);
        ByteBuffer b2 = decomposeAndRandomPad(type, v2);
        int expected = Integer.signum(type.compare(b1, b2));
        final ByteComparable bc1 = typeToComparable(type, b1);
        final ByteComparable bc2 = typeToComparable(type, b2);

        for (Version version : Version.values())
        {
            int actual = Integer.signum(ByteComparable.compare(bc1, bc2, version));
            if (expected != actual)
            {
                if (type.isReversed())
                {
                    // This can happen for reverse of nulls and prefixes. Check that it's ok within multi-component
                    ClusteringComparator cc = new ClusteringComparator(type);
                    ByteComparable c1 = cc.asByteComparable(Clustering.make(b1));
                    ByteComparable c2 = cc.asByteComparable(Clustering.make(b2));
                    int actualcc = Integer.signum(ByteComparable.compare(c1, c2, version));
                    if (actualcc == expected)
                        return;
                    assertEquals(String.format("Failed comparing reversed %s(%s, %s) and %s(%s, %s) direct (%d) and as clustering", safeStr(v1), ByteBufferUtil.bytesToHex(b1), c1, safeStr(v2), ByteBufferUtil.bytesToHex(b2), c2, actual), expected, actualcc);
                }
                else
                    assertEquals(String.format("Failed comparing %s(%s) and %s(%s)", safeStr(v1), ByteBufferUtil.bytesToHex(b1), safeStr(v2), ByteBufferUtil.bytesToHex(b2)), expected, actual);
            }
            maybeCheck41Properties(expected, bc1, bc2, version);
        }
    }

    ByteBuffer decomposeAndRandomPad(AbstractType type, Object v)
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
