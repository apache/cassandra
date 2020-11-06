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
package org.apache.cassandra.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.DynamicCompositeTypeTest;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.utils.ByteComparable.Version;

import static org.junit.Assert.assertEquals;

public class ByteSourceTest
{
    private final static Logger logger = LoggerFactory.getLogger(ByteSourceTest.class);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    String[] testStrings = new String[] { "", "\0", "\0\0", "\001", "A\0\0B", "A\0B\0", "0", "0\0", "00", "1", "\377" };
    Integer[] testInts = new Integer[] { null, Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -256, -255, -128, -127, -1, 0, 1, 127, 128, 255, 256, Integer.MAX_VALUE - 1, Integer.MAX_VALUE };
    Byte[] testBytes = new Byte[] { -128, -127, -1, 0, 1, 127 };
    Short[] testShorts = new Short[] { Short.MIN_VALUE, Short.MIN_VALUE + 1, -256, -255, -128, -127, -1, 0, 1, 127, 128, 255, 256, Short.MAX_VALUE - 1, Short.MAX_VALUE };
    Long[] testLongs = new Long[] { null, Long.MIN_VALUE, Long.MIN_VALUE + 1, Integer.MIN_VALUE - 1L, -256L, -255L, -128L, -127L, -1L, 0L, 1L, 127L, 128L, 255L, 256L, Integer.MAX_VALUE + 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE };
    Double[] testDoubles = new Double[] { null, Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -1e+200, -1e3, -1e0, -1e-3, -1e-200, -Double.MIN_VALUE, -0.0, 0.0, Double.MIN_VALUE, 1e-200, 1e-3, 1e0, 1e3, 1e+200, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN };
    Float[] testFloats = new Float[] { null, Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, -1e+30f, -1e3f, -1e0f, -1e-3f, -1e-30f, -Float.MIN_VALUE, -0.0f, 0.0f, Float.MIN_VALUE, 1e-30f, 1e-3f, 1e0f, 1e3f, 1e+30f, Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN };
    Boolean[] testBools = new Boolean[] { null, false, true };
    UUID[] testUUIDs = new UUID[] { null, UUIDGen.getTimeUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                                    UUIDGen.getTimeUUID(123, 234), UUIDGen.getTimeUUID(123, 234), UUIDGen.getTimeUUID(123),
                                    UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8"),
                                    UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                    UUID.fromString("e902893a-9d22-3c7e-a7b8-d6e313b71d9f"),
                                    UUID.fromString("74738ff5-5367-5958-9aee-98fffdcd1876"),
                                    UUID.fromString("52df1bb0-6a2f-11e6-b6e4-a6dea7a01b67"),
                                    UUID.fromString("52df1bb0-6a2f-11e6-362d-aff2143498ea"),
                                    UUID.fromString("52df1bb0-6a2f-11e6-b62d-aff2143498ea")};
    // Instant.MIN/MAX fail Date.from.
    Date[] testDates = new Date[] { null,
                                    Date.from(Instant.ofEpochSecond(Integer.MIN_VALUE)),
                                    Date.from(Instant.ofEpochSecond(Short.MIN_VALUE)),
                                    Date.from(Instant.ofEpochMilli(-2000)),
                                    Date.from(Instant.EPOCH),
                                    Date.from(Instant.ofEpochMilli(2000)),
                                    Date.from(Instant.ofEpochSecond(Integer.MAX_VALUE)),
                                    Date.from(Instant.now()) };
    BigInteger[] testBigInts;

    {
        Set<BigInteger> bigs = new TreeSet<>();
        for (Long l : testLongs)
            if (l != null)
                bigs.add(BigInteger.valueOf(l));
        for (int i = 0; i < 11; ++i)
        {
            bigs.add(BigInteger.valueOf(i));
            bigs.add(BigInteger.valueOf(-i));

            bigs.add(BigInteger.valueOf((1L << 4 * i) - 1));
            bigs.add(BigInteger.valueOf((1L << 4 * i)));
            bigs.add(BigInteger.valueOf(-(1L << 4 * i) - 1));
            bigs.add(BigInteger.valueOf(-(1L << 4 * i)));
            String p = exp10(i);
            bigs.add(new BigInteger(p));
            bigs.add(new BigInteger("-" + p));
            p = exp10(1 << i);
            bigs.add(new BigInteger(p));
            bigs.add(new BigInteger("-" + p));

            BigInteger base = BigInteger.ONE.shiftLeft(512 * i);
            bigs.add(base);
            bigs.add(base.add(BigInteger.ONE));
            bigs.add(base.subtract(BigInteger.ONE));
            base = base.negate();
            bigs.add(base);
            bigs.add(base.add(BigInteger.ONE));
            bigs.add(base.subtract(BigInteger.ONE));
        }
        testBigInts = bigs.toArray(new BigInteger[0]);
    }
    BigDecimal[] testBigDecimals;
    {
        String vals = "0, 1, 1.1, 21, 98.9, 99, 99.9, 100, 100.1, 101, 331, 0.4, 0.07, 0.0700, 0.005, " +
                      "6e4, 7e200, 6e-300, 8.1e2000, 8.1e-2000, 9e2000, " +
                      "123456789012.34567890e-1000, 123456.78901234, 1234.56789012e2, " +
                      "1.0000, 0.01e2, 100e-2, 00, 0.000, 0E-18, 0E+18";
        List<BigDecimal> decs = new ArrayList<>();
        for (String s : vals.split(", "))
        {
            decs.add(new BigDecimal(s));
            decs.add(new BigDecimal("-" + s));
        }
        testBigDecimals = decs.toArray(new BigDecimal[0]);
    }

    static String exp10(int pow)
    {
        StringBuilder builder = new StringBuilder();
        builder.append('1');
        for (int i=0; i<pow; ++i)
            builder.append('0');
        return builder.toString();
    }

    Object[][] testValues = new Object[][] { testStrings, testInts, testBools, testDoubles, testBigInts, testBigDecimals };
    AbstractType[] testTypes = new AbstractType[] {
                               AsciiType.instance,
                               Int32Type.instance,
                               BooleanType.instance,
                               DoubleType.instance,
                               IntegerType.instance,
                               DecimalType.instance };

    @Test
    public void testStringsAscii()
    {
        testType(AsciiType.instance, testStrings);
    }

    @Test
    public void testStringsUTF8()
    {
        testType(UTF8Type.instance, testStrings);
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
        InetAddress[] testInets = new InetAddress[] { null,
                                                      InetAddress.getLocalHost(),
                                                      InetAddress.getLoopbackAddress(),
                                                      InetAddress.getByName("192.168.0.1"),
                                                      InetAddress.getByName("fe80::428d:5cff:fe53:1dc9"),
                                                      InetAddress.getByName("2001:610:3:200a:192:87:36:2"),
                                                      InetAddress.getByName("10.0.0.1"),
                                                      InetAddress.getByName("0a00:0001::"),
                                                      InetAddress.getByName("::10.0.0.1") };
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

    ClusteringPrefix.Kind[] kinds = new ClusteringPrefix.Kind[] {
    ClusteringPrefix.Kind.INCL_START_BOUND,
    ClusteringPrefix.Kind.CLUSTERING,
    ClusteringPrefix.Kind.EXCL_START_BOUND,
    };

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
            for (ClusteringPrefix.Kind k1 : kinds)
                for (ClusteringPrefix.Kind k2 : kinds)
                {
                    ClusteringComparator comp = new ClusteringComparator(t1, t2);
                    ByteBuffer[] b = new ByteBuffer[2];
                    ByteBuffer[] d = new ByteBuffer[2];
                    b[0] = t1.decompose(o1);
                    b[1] = t2.decompose(o2);
                    d[0] = t1.decompose(o3);
                    d[1] = t2.decompose(o4);
                    ClusteringPrefix<ByteBuffer> c = ByteBufferAccessor.instance.factory().bound(k1, b);
                    ClusteringPrefix<ByteBuffer> e = ByteBufferAccessor.instance.factory().bound(k2, d);
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

    @Test
    public void testTupleType()
    {
        Random rand = ThreadLocalRandom.current();
        testCombinationSampling(rand, this::assertTupleComparesSame);
    }

    @Test
    public void testTupleTypeNonFull()
    {
        TupleType tt = new TupleType(ImmutableList.of(AsciiType.instance, Int32Type.instance));
        List<ByteBuffer> tests = ImmutableList.of
            (
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeAndRandomPad(AsciiType.instance, ""),
                                                                                decomposeAndRandomPad(Int32Type.instance, 0)}),
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeAndRandomPad(AsciiType.instance, ""),
                                                                                decomposeAndRandomPad(Int32Type.instance, null)}),
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {decomposeAndRandomPad(AsciiType.instance, "")}),
            TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[0])
            );
        testBuffers(tt, tests);
    }

    void assertTupleComparesSame(AbstractType t1, AbstractType t2, Object o1, Object o2, Object o3, Object o4)
    {
        TupleType tt = new TupleType(ImmutableList.of(t1, t2));
        ByteBuffer b1 = TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {t1.decompose(o1), t2.decompose(o2)});
        ByteBuffer b2 = TupleType.buildValue(ByteBufferAccessor.instance, new ByteBuffer[] {t1.decompose(o3), t2.decompose(o4)});
        assertComparesSame(tt, b1, b2);
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
        CompositeType tt = CompositeType.getInstance(AsciiType.instance, Int32Type.instance);
        List<ByteBuffer> tests = ImmutableList.of
            (
            CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(AsciiType.instance, ""), decomposeAndRandomPad(Int32Type.instance, 0)),
            CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(AsciiType.instance, ""), decomposeAndRandomPad(Int32Type.instance, null)),
            CompositeType.build(ByteBufferAccessor.instance, decomposeAndRandomPad(AsciiType.instance, "")),
            CompositeType.build(ByteBufferAccessor.instance),
            CompositeType.build(ByteBufferAccessor.instance, true, decomposeAndRandomPad(AsciiType.instance, "")),
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
        assertComparesSame(tt, b1, b2);
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
        testCollection(ListType.getInstance(AsciiType.instance, true), testStrings, () -> new ArrayList<>(), new Random());
    }

    @Test
    public void testListTypeLong()
    {
        testCollection(ListType.getInstance(LongType.instance, true), testLongs, () -> new ArrayList<>(), new Random());
    }

    @Test
    public void testSetTypeString()
    {
        testCollection(SetType.getInstance(AsciiType.instance, true), testStrings, () -> new HashSet<>(), new Random());
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
        testMap(MapType.getInstance(AsciiType.instance, LongType.instance, true), testStrings, testLongs, () -> new HashMap<>(), new Random());
    }

    @Test
    public void testMapTypeStringLongTree()
    {
        testMap(MapType.getInstance(AsciiType.instance, LongType.instance, true), testStrings, testLongs, () -> new TreeMap<>(), new Random());
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
            for (Object i : values) {
                ByteBuffer b = decomposeAndRandomPad(type, i);
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

    private Object safeStr(Object i)
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
        padded.put(b);
        rand.ints(padAfter).forEach(x -> padded.put((byte) x));
        padded.clear().limit(padded.capacity() - padAfter).position(padBefore);
        return padded;
    }

    protected ByteBuffer allocateBuffer(int paddedCapacity)
    {
        return ByteBuffer.allocate(paddedCapacity);
    }
}
