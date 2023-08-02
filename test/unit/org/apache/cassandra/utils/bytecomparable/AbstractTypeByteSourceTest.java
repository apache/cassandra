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
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
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
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LengthPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDGen;

@RunWith(Parameterized.class)
public class AbstractTypeByteSourceTest
{
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()";

    @Parameterized.Parameters(name = "version={0}")
    public static Iterable<ByteComparable.Version> versions()
    {
        return ImmutableList.of(ByteComparable.Version.OSS50);
    }

    private final ByteComparable.Version version;

    public AbstractTypeByteSourceTest(ByteComparable.Version version)
    {
        this.version = version;
    }

    private <T> void testValuesForType(AbstractType<T> type, T... values)
    {
        testValuesForType(type, Arrays.asList(values));
    }

    private <T> void testValuesForType(AbstractType<T> type, List<T> values)
    {
        for (T initial : values)
            decodeAndAssertEquals(type, initial);
        if (IntegerType.instance.equals(type))
            // IntegerType tests go through A LOT of values, so short of randomly picking up to, let's say 1000
            // values to combine with, we'd rather skip the comparison tests for them.
            return;
        for (int i = 0; i < values.size(); ++i)
        {
            for (int j = i + 1; j < values.size(); ++j)
            {
                ByteBuffer left = type.decompose(values.get(i));
                ByteBuffer right = type.decompose(values.get(j));
                int compareBuffers = Integer.signum(type.compare(left, right));
                ByteSource leftSource = type.asComparableBytes(left.duplicate(), version);
                ByteSource rightSource = type.asComparableBytes(right.duplicate(), version);
                int compareBytes = Integer.signum(ByteComparable.compare(v -> leftSource, v -> rightSource, version));
                Assert.assertEquals(compareBuffers, compareBytes);
            }
        }
    }

    private <T> void testValuesForType(AbstractType<T> type, Stream<T> values)
    {
        values.forEach(initial -> decodeAndAssertEquals(type, initial));
    }

    private <T> void decodeAndAssertEquals(AbstractType<T> type, T initial)
    {
        ByteBuffer initialBuffer = type.decompose(initial);
        // Assert that fromComparableBytes decodes correctly.
        ByteSource.Peekable peekableBytes = ByteSource.peekable(type.asComparableBytes(initialBuffer, version));
        ByteBuffer decodedBuffer = type.fromComparableBytes(peekableBytes, version);
        Assert.assertEquals("For " + ByteSourceComparisonTest.safeStr(initial),
                            ByteBufferUtil.bytesToHex(initialBuffer),
                            ByteBufferUtil.bytesToHex(decodedBuffer));
        // Assert that the value composed from fromComparableBytes is the correct one.
        peekableBytes = ByteSource.peekable(type.asComparableBytes(initialBuffer, version));
        T decoded = type.compose(type.fromComparableBytes(peekableBytes, version));
        Assert.assertEquals(initial, decoded);
    }

    private static String newRandomAlphanumeric(Random prng, int length)
    {
        StringBuilder random = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            random.append(ALPHABET.charAt(prng.nextInt(ALPHABET.length())));
        return random.toString();
    }

    @Test
    public void testAsciiType()
    {
        String[] asciiStrings = new String[]
        {
                "",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
                "!@#$%^&*()",
        };
        testValuesForType(AsciiType.instance, asciiStrings);

        Random prng = new Random();
        Stream<String> asciiStream = Stream.generate(() -> newRandomAlphanumeric(prng, 10)).limit(1000);
        testValuesForType(AsciiType.instance, asciiStream);
    }

    @Test
    public void testBooleanType()
    {
        testValuesForType(BooleanType.instance, Boolean.TRUE, Boolean.FALSE, null);
    }

    @Test
    public void testBytesType()
    {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        Random prng = new Random();
        byte[] byteArray;
        int[] arrayLengths = new int[] {1, 10, 100, 1000};
        for (int length : arrayLengths)
        {
            byteArray = new byte[length];
            for (int i = 0; i < 1000; ++i)
            {
                prng.nextBytes(byteArray);
                byteBuffers.add(ByteBuffer.wrap(byteArray));
            }
        }
        testValuesForType(BytesType.instance, byteBuffers.toArray(new ByteBuffer[0]));
    }

    @Test
    public void testByteType()
    {
        testValuesForType(ByteType.instance, new Byte[] { null });

        Stream<Byte> allBytes = IntStream.range(Byte.MIN_VALUE, Byte.MAX_VALUE + 1)
                                         .mapToObj(value -> (byte) value);
        testValuesForType(ByteType.instance, allBytes);
    }

    @Test
    public void testCompositeType()
    {
        CompositeType compType = CompositeType.getInstance(UTF8Type.instance, TimeUUIDType.instance, IntegerType.instance);
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        Random prng = new Random();
        // Test with complete CompositeType rows
        for (int i = 0; i < 1000; ++i)
        {
            String randomString = newRandomAlphanumeric(prng, 10);
            TimeUUID randomUuid = TimeUUID.Generator.nextTimeUUID();
            BigInteger randomVarint = BigInteger.probablePrime(80, prng);
            byteBuffers.add(compType.decompose(randomString, randomUuid, randomVarint));
        }
        // Test with incomplete CompositeType rows, where only the first element is present
        ByteBuffer[] incompleteComposite = new ByteBuffer[1];
        incompleteComposite[0] = UTF8Type.instance.decompose(newRandomAlphanumeric(prng, 10));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, true, incompleteComposite));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, false, incompleteComposite));
        // ...and the last end-of-component byte is not 0.
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, true, incompleteComposite, (byte) 1));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, false, incompleteComposite, (byte) 1));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, true, incompleteComposite, (byte) -1));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, false, incompleteComposite, (byte) -1));
        // Test with incomplete CompositeType rows, where only the last element is not present
        incompleteComposite = new ByteBuffer[2];
        incompleteComposite[0] = UTF8Type.instance.decompose(newRandomAlphanumeric(prng, 10));
        incompleteComposite[1] = TimeUUIDType.instance.decompose(TimeUUID.Generator.nextTimeUUID());
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, true, incompleteComposite));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, false, incompleteComposite));
        // ...and the last end-of-component byte is not 0.
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, true, incompleteComposite, (byte) 1));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, false, incompleteComposite, (byte) 1));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, true, incompleteComposite, (byte) -1));
        byteBuffers.add(CompositeType.build(ByteBufferAccessor.instance, false, incompleteComposite, (byte) -1));

        testValuesForType(compType, byteBuffers.toArray(new ByteBuffer[0]));
    }

    @Test
    public void testDateType()
    {
        Stream<Date> dates = Stream.of(null,
                                       new Date(Long.MIN_VALUE),
                                       new Date(Long.MAX_VALUE),
                                       new Date());
        testValuesForType(DateType.instance, dates);

        dates = new Random().longs(1000).mapToObj(Date::new);
        testValuesForType(DateType.instance, dates);
    }

    @Test
    public void testDecimalType()
    {
        // We won't be using testValuesForType for DecimalType (i.e. we won't also be comparing the initial and decoded
        // ByteBuffer values). That's because the same BigDecimal value can be represented with a couple of different,
        // even if equivalent pairs of <mantissa, scale> (e.g. 0.1 is 1 * e-1, as well as 10 * e-2, as well as...).
        // And in practice it's easier to just convert to BigDecimals and then compare, instead of trying to manually
        // decode and convert to canonical representations, which then to compare. For example of generating canonical
        // decimals in the first place, see testReversedType().
        Consumer<BigDecimal> bigDecimalConsumer = initial ->
        {
            ByteSource byteSource = DecimalType.instance.asComparableBytes(DecimalType.instance.decompose(initial), version);
            BigDecimal decoded = DecimalType.instance.compose(DecimalType.instance.fromComparableBytes(ByteSource.peekable(byteSource), version));
            if (initial == null)
                Assert.assertNull(decoded);
            else
                Assert.assertEquals(0, initial.compareTo(decoded));
        };
        // Test some interesting predefined BigDecimal values.
        Stream.of(null,
                  BigDecimal.ZERO,
                  BigDecimal.ONE,
                  BigDecimal.ONE.add(BigDecimal.ONE),
                  BigDecimal.TEN,
                  BigDecimal.valueOf(0.0000000000000000000000000000000001),
                  BigDecimal.valueOf(-0.0000000000000000000000000000000001),
                  BigDecimal.valueOf(0.0000000000000001234567891011121314),
                  BigDecimal.valueOf(-0.0000000000000001234567891011121314),
                  BigDecimal.valueOf(12345678910111213.141516171819202122),
                  BigDecimal.valueOf(-12345678910111213.141516171819202122),
                  new BigDecimal(BigInteger.TEN, Integer.MIN_VALUE),
                  new BigDecimal(BigInteger.TEN.negate(), Integer.MIN_VALUE),
                  new BigDecimal(BigInteger.TEN, Integer.MAX_VALUE),
                  new BigDecimal(BigInteger.TEN.negate(), Integer.MAX_VALUE),
                  new BigDecimal(BigInteger.TEN.pow(1000), Integer.MIN_VALUE),
                  new BigDecimal(BigInteger.TEN.pow(1000).negate(), Integer.MIN_VALUE),
                  new BigDecimal(BigInteger.TEN.pow(1000), Integer.MAX_VALUE),
                  new BigDecimal(BigInteger.TEN.pow(1000).negate(), Integer.MAX_VALUE))
              .forEach(bigDecimalConsumer);
        // Test BigDecimals created from random double values with predefined range modifiers.
        double[] bounds = {
                Double.MIN_VALUE,
                -1_000_000_000.0,
                -100_000.0,
                -1.0,
                1.0,
                100_000.0,
                1_000_000_000.0,
                Double.MAX_VALUE};
        for (double bound : bounds)
        {
            new Random().doubles(1000)
                        .mapToObj(initial -> BigDecimal.valueOf(initial * bound))
                        .forEach(bigDecimalConsumer);
        }
    }

    @Test
    public void testDoubleType()
    {
        Stream<Double> doubles = Stream.of(null,
                                           Double.NaN,
                                           Double.POSITIVE_INFINITY,
                                           Double.NEGATIVE_INFINITY,
                                           Double.MAX_VALUE,
                                           Double.MIN_VALUE,
                                           +0.0,
                                           -0.0,
                                           +1.0,
                                           -1.0,
                                           +12345678910.111213141516,
                                           -12345678910.111213141516);
        testValuesForType(DoubleType.instance, doubles);

        doubles = new Random().doubles(1000).boxed();
        testValuesForType(DoubleType.instance, doubles);
    }

    @Test
    public void testDurationType()
    {
        Random prng = new Random();
        Stream<Duration> posDurations = Stream.generate(() ->
                                                        {
                                                            int months = prng.nextInt(12) + 1;
                                                            int days = prng.nextInt(28) + 1;
                                                            long nanos = (Math.abs(prng.nextLong() % 86_400_000_000_000L)) + 1;
                                                            return Duration.newInstance(months, days, nanos);
                                                        })
                                              .limit(1000);
        testValuesForType(DurationType.instance, posDurations);
        Stream<Duration> negDurations = Stream.generate(() ->
                                                        {
                                                            int months = prng.nextInt(12) + 1;
                                                            int days = prng.nextInt(28) + 1;
                                                            long nanos = (Math.abs(prng.nextLong() % 86_400_000_000_000L)) + 1;
                                                            return Duration.newInstance(-months, -days, -nanos);
                                                        })
                                              .limit(1000);
        testValuesForType(DurationType.instance, negDurations);
    }

    @Test
    public void testDynamicCompositeType()
    {
        DynamicCompositeType dynamicCompType = DynamicCompositeType.getInstance(new HashMap<>());
        ImmutableList<String> allTypes = ImmutableList.of("org.apache.cassandra.db.marshal.BytesType",
                                                          "org.apache.cassandra.db.marshal.TimeUUIDType",
                                                          "org.apache.cassandra.db.marshal.IntegerType");
        List<ByteBuffer> allValues = new ArrayList<>();
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        Random prng = new Random();
        for (int i = 0; i < 10; ++i)
        {
            String randomString = newRandomAlphanumeric(prng, 10);
            allValues.add(ByteBufferUtil.bytes(randomString));
            UUID randomUuid = TimeUUID.Generator.nextTimeAsUUID();
            allValues.add(ByteBuffer.wrap(UUIDGen.decompose(randomUuid)));
            byte randomByte = (byte) prng.nextInt();
            allValues.add(ByteBuffer.allocate(1).put(randomByte));

            // Three-component key with aliased and non-aliased types and end-of-component byte varying (0, 1, -1).
            byteBuffers.add(DynamicCompositeType.build(allTypes, allValues));
            byteBuffers.add(createStringUuidVarintDynamicCompositeKey(randomString, randomUuid, randomByte, (byte) 1));
            byteBuffers.add(createStringUuidVarintDynamicCompositeKey(randomString, randomUuid, randomByte, (byte) -1));

            // Two-component key with aliased and non-aliased types and end-of-component byte varying (0, 1, -1).
            byteBuffers.add(DynamicCompositeType.build(allTypes.subList(0, 2), allValues.subList(0, 2)));
            byteBuffers.add(createStringUuidVarintDynamicCompositeKey(randomString, randomUuid, -1, (byte) 1));
            byteBuffers.add(createStringUuidVarintDynamicCompositeKey(randomString, randomUuid, -1, (byte) -1));

            // One-component key with aliased and non-aliased type and end-of-component byte varying (0, 1, -1).
            byteBuffers.add(DynamicCompositeType.build(allTypes.subList(0, 1), allValues.subList(0, 1)));
            byteBuffers.add(createStringUuidVarintDynamicCompositeKey(randomString, null, -1, (byte) 1));
            byteBuffers.add(createStringUuidVarintDynamicCompositeKey(randomString, null, -1, (byte) -1));

            allValues.clear();
        }
        testValuesForType(dynamicCompType, byteBuffers.toArray(new ByteBuffer[0]));
    }

    // Similar to DynamicCompositeTypeTest.createDynamicCompositeKey(string, uuid, i, true, false), but not using any
    // aliased types, in order to do an exact comparison of the unmarshalled DynamicCompositeType payload with the
    // input one. If aliased types are used, due to DynamicCompositeType.build(List<String>, List<ByteBuffer>)
    // always including the full type info in the newly constructed payload, an exact comparison won't work.
    private static ByteBuffer createStringUuidVarintDynamicCompositeKey(String string, UUID uuid, int i, byte lastEocByte)
    {
        // 1. Calculate how many bytes do we need for a key of this DynamicCompositeType
        String bytesType = "org.apache.cassandra.db.marshal.BytesType";
        String timeUuidType = "org.apache.cassandra.db.marshal.TimeUUIDType";
        String varintType = "org.apache.cassandra.db.marshal.IntegerType";
        ByteBuffer bytes = ByteBufferUtil.bytes(string);
        int totalSize = 0;
        // Take into account the string component data (BytesType is aliased)
        totalSize += 2 + bytesType.length() + 2 + bytes.remaining() + 1;
        if (uuid != null)
        {
            // Take into account the UUID component data (TimeUUIDType is aliased)
            totalSize += 2 + timeUuidType.length() + 2 + 16 + 1;
            if (i != -1)
            {
                // Take into account the varint component data (IntegerType is _not_ aliased).
                // Notice that we account for a single byte of varint data, so we'll downcast the int payload
                // to byte and use only that as the actual varint payload.
                totalSize += 2 + varintType.length() + 2 + 1 + 1;
            }
        }

        // 2. Allocate a buffer with that many bytes
        ByteBuffer bb = ByteBuffer.allocate(totalSize);

        // 3. Write the key data for each component in the allocated buffer
        bb.putShort((short) bytesType.length());
        bb.put(ByteBufferUtil.bytes(bytesType));
        bb.putShort((short) bytes.remaining());
        bb.put(bytes);
        // Make the end-of-component byte 1 if requested and the time-UUID component is null.
        bb.put(uuid == null ? lastEocByte : (byte) 0);
        if (uuid != null)
        {
            bb.putShort((short) timeUuidType.length());
            bb.put(ByteBufferUtil.bytes(timeUuidType));
            bb.putShort((short) 16);
            bb.put(UUIDGen.decompose(uuid));
            // Set the end-of-component byte if requested and the varint component is null.
            bb.put(i == -1 ? lastEocByte : (byte) 0);
            if (i != -1)
            {
                bb.putShort((short) varintType.length());
                bb.put(ByteBufferUtil.bytes(varintType));
                bb.putShort((short) 1);
                bb.put((byte) i);
                bb.put(lastEocByte);
            }
        }
        bb.rewind();
        return bb;
    }

    @Test
    public void testFloatType()
    {
        Stream<Float> floats = Stream.of(null,
                                         Float.NaN,
                                         Float.POSITIVE_INFINITY,
                                         Float.NEGATIVE_INFINITY,
                                         Float.MAX_VALUE,
                                         Float.MIN_VALUE,
                                         +0.0F,
                                         -0.0F,
                                         +1.0F,
                                         -1.0F,
                                         +123456.7891011F,
                                         -123456.7891011F);
        testValuesForType(FloatType.instance, floats);

        floats = new Random().ints(1000).mapToObj(Float::intBitsToFloat);
        testValuesForType(FloatType.instance, floats);
    }

    @Test
    public void testInetAddressType() throws UnknownHostException
    {
        Stream<InetAddress> inetAddresses = Stream.of(null,
                                                      InetAddress.getLocalHost(),
                                                      InetAddress.getLoopbackAddress(),
                                                      InetAddress.getByName("0.0.0.0"),
                                                      InetAddress.getByName("10.0.0.1"),
                                                      InetAddress.getByName("172.16.1.1"),
                                                      InetAddress.getByName("192.168.2.2"),
                                                      InetAddress.getByName("224.3.3.3"),
                                                      InetAddress.getByName("255.255.255.255"),
                                                      InetAddress.getByName("0000:0000:0000:0000:0000:0000:0000:0000"),
                                                      InetAddress.getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
                                                      InetAddress.getByName("fe80:1:23:456:7890:1:23:456"));
        testValuesForType(InetAddressType.instance, inetAddresses);

        Random prng = new Random();
        byte[] ipv4Bytes = new byte[4];
        byte[] ipv6Bytes = new byte[16];
        InetAddress[] addresses = new InetAddress[2000];
        for (int i = 0; i < addresses.length / 2; ++i)
        {
            prng.nextBytes(ipv4Bytes);
            addresses[2 * i] = InetAddress.getByAddress(ipv4Bytes);
            addresses[2 * i + 1] = InetAddress.getByAddress(ipv6Bytes);
        }
        testValuesForType(InetAddressType.instance, addresses);

    }

    @Test
    public void testInt32Type()
    {
        Stream<Integer> ints = Stream.of(null,
                                         Integer.MIN_VALUE,
                                         Integer.MIN_VALUE + 1,
                                         -256, -255, -128, -127, -1,
                                         0,
                                         1, 127, 128, 255, 256,
                                         Integer.MAX_VALUE - 1,
                                         Integer.MAX_VALUE);
        testValuesForType(Int32Type.instance, ints);

        ints = new Random().ints(1000).boxed();
        testValuesForType(Int32Type.instance, ints);
    }

    @Test
    public void testIntegerType()
    {
        Stream<BigInteger> varints = IntStream.range(-1000000, 1000000).mapToObj(BigInteger::valueOf);
        testValuesForType(IntegerType.instance, varints);

        varints = Stream.of(null,
                            BigInteger.valueOf(12345678910111213L),
                            BigInteger.valueOf(12345678910111213L).negate(),
                            BigInteger.valueOf(Long.MAX_VALUE),
                            BigInteger.valueOf(Long.MAX_VALUE).negate(),
                            BigInteger.valueOf(Long.MAX_VALUE - 1).multiply(BigInteger.valueOf(Long.MAX_VALUE - 1)),
                            BigInteger.valueOf(Long.MAX_VALUE - 1).multiply(BigInteger.valueOf(Long.MAX_VALUE - 1)).negate());
        testValuesForType(IntegerType.instance, varints);

        List<BigInteger> varintList = new ArrayList<>();
        for (int i = 0; i < 10000; ++i)
        {
            BigInteger initial = BigInteger.ONE.shiftLeft(i);
            varintList.add(initial);
            BigInteger plusOne = initial.add(BigInteger.ONE);
            varintList.add(plusOne);
            varintList.add(plusOne.negate());
            BigInteger minusOne = initial.subtract(BigInteger.ONE);
            varintList.add(minusOne);
            varintList.add(minusOne.negate());
        }
        testValuesForType(IntegerType.instance, varintList.toArray(new BigInteger[0]));
    }

    @Test
    public void testUuidTypes()
    {
        Random prng = new Random();
        UUID[] testUuids = new UUID[3001];
        for (int i = 0; i < testUuids.length / 3; ++i)
        {
            testUuids[3 * i] = UUID.randomUUID();
            testUuids[3 * i + 1] = TimeUUID.Generator.nextTimeAsUUID();
            testUuids[3 * i + 2] = TimeUUID.atUnixMicrosWithLsbAsUUID(prng.nextLong(), prng.nextLong());
        }
        testUuids[testUuids.length - 1] = null;
        testValuesForType(UUIDType.instance, testUuids);
        testValuesForType(LexicalUUIDType.instance, testUuids);
        testValuesForType(TimeUUIDType.instance, Arrays.stream(testUuids)
                                                       .filter(u -> u == null || u.version() == 1)
                                                       .map(u -> u != null ? TimeUUID.fromUuid(u) : null));
    }

    private static <E, C extends Collection<E>> List<C> newRandomElementCollections(Supplier<? extends C> collectionProducer,
                                                                                    Supplier<? extends E> elementProducer,
                                                                                    int numCollections,
                                                                                    int numElementsInCollection)
    {
        List<C> result = new ArrayList<>();
        for (int i = 0; i < numCollections; ++i)
        {
            C coll = collectionProducer.get();
            for (int j = 0; j < numElementsInCollection; ++j)
            {
                coll.add(elementProducer.get());
            }
            result.add(coll);
        }
        return result;
    }

    @Test
    public void testListType()
    {
        // Test lists with element components not having known/computable length (e.g. strings).
        Random prng = new Random();
        List<List<String>> stringLists = newRandomElementCollections(ArrayList::new,
                                                                     () -> newRandomAlphanumeric(prng, 10),
                                                                     100,
                                                                     100);
        testValuesForType(ListType.getInstance(UTF8Type.instance, false), stringLists);
        testValuesForType(ListType.getInstance(UTF8Type.instance, true), stringLists);
        // Test lists with element components with known/computable length (e.g. 128-bit UUIDs).
        List<List<UUID>> uuidLists = newRandomElementCollections(ArrayList::new,
                                                                 UUID::randomUUID,
                                                                 100,
                                                                 100);
        testValuesForType(ListType.getInstance(UUIDType.instance, false), uuidLists);
        testValuesForType(ListType.getInstance(UUIDType.instance, true), uuidLists);
    }

    @Test
    public void testLongType()
    {
        Stream<Long> longs = Stream.of(null,
                                       Long.MIN_VALUE,
                                       Long.MIN_VALUE + 1,
                                       (long) Integer.MIN_VALUE - 1,
                                       -256L, -255L, -128L, -127L, -1L,
                                       0L,
                                       1L, 127L, 128L, 255L, 256L,
                                       (long) Integer.MAX_VALUE + 1,
                                       Long.MAX_VALUE - 1,
                                       Long.MAX_VALUE);
        testValuesForType(LongType.instance, longs);

        longs = new Random().longs(1000).boxed();
        testValuesForType(LongType.instance, longs);
    }

    private static <K, V> List<Map<K, V>> newRandomEntryMaps(Supplier<? extends K> keyProducer,
                                                             Supplier<? extends V> valueProducer,
                                                             int numMaps,
                                                             int numEntries)
    {
        List<Map<K, V>> result = new ArrayList<>();
        for (int i = 0; i < numMaps; ++i)
        {
            Map<K, V> map = new HashMap<>();
            for (int j = 0; j < numEntries; ++j)
            {
                K key = keyProducer.get();
                V value = valueProducer.get();
                map.put(key, value);
            }
            result.add(map);
        }
        return result;
    }

    @Test
    public void testMapType()
    {
        Random prng = new Random();
        List<Map<String, UUID>> stringToUuidMaps = newRandomEntryMaps(() -> newRandomAlphanumeric(prng, 10),
                                                                      UUID::randomUUID,
                                                                      100,
                                                                      100);
        testValuesForType(MapType.getInstance(UTF8Type.instance, UUIDType.instance, false), stringToUuidMaps);
        testValuesForType(MapType.getInstance(UTF8Type.instance, UUIDType.instance, true), stringToUuidMaps);

        List<Map<UUID, String>> uuidToStringMaps = newRandomEntryMaps(UUID::randomUUID,
                                                                      () -> newRandomAlphanumeric(prng, 10),
                                                                      100,
                                                                      100);
        testValuesForType(MapType.getInstance(UUIDType.instance, UTF8Type.instance, false), uuidToStringMaps);
        testValuesForType(MapType.getInstance(UUIDType.instance, UTF8Type.instance, true), uuidToStringMaps);
    }

    @Test
    public void testPartitionerDefinedOrder()
    {
        Random prng = new Random();
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        byteBuffers.add(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        for (int i = 0; i < 1000; ++i)
        {
            String randomString = newRandomAlphanumeric(prng, 10);
            byteBuffers.add(UTF8Type.instance.decompose(randomString));
            int randomInt = prng.nextInt();
            byteBuffers.add(Int32Type.instance.decompose(randomInt));
            double randomDouble = prng.nextDouble();
            byteBuffers.add(DoubleType.instance.decompose(randomDouble));
            BigInteger randomishVarint = BigInteger.probablePrime(100, prng);
            byteBuffers.add(IntegerType.instance.decompose(randomishVarint));
            BigDecimal randomishDecimal = BigDecimal.valueOf(prng.nextLong(), prng.nextInt(100) - 50);
            byteBuffers.add(DecimalType.instance.decompose(randomishDecimal));
        }

        byte[] bytes = new byte[100];
        prng.nextBytes(bytes);
        ByteBuffer exhausted = ByteBuffer.wrap(bytes);
        ByteBufferUtil.readBytes(exhausted, 100);

        List<IPartitioner> partitioners = Arrays.asList(
                Murmur3Partitioner.instance,
                RandomPartitioner.instance,
                LengthPartitioner.instance
                // NOTE LocalPartitioner, OrderPreservingPartitioner, and ByteOrderedPartitioner don't need a dedicated
                // PartitionerDefinedOrder.
                //   1) LocalPartitioner uses its inner AbstractType
                //   2) OrderPreservingPartitioner uses UTF8Type
                //   3) ByteOrderedPartitioner uses BytesType
        );
        for (IPartitioner partitioner : partitioners)
        {
            AbstractType<?> partitionOrdering = partitioner.partitionOrdering(null);
            Assert.assertTrue(partitionOrdering instanceof PartitionerDefinedOrder);
            for (ByteBuffer input : byteBuffers)
            {
                ByteSource byteSource = partitionOrdering.asComparableBytes(input, version);
                ByteBuffer output = partitionOrdering.fromComparableBytes(ByteSource.peekable(byteSource), version);
                Assert.assertEquals("For partitioner " + partitioner.getClass().getSimpleName(),
                                    ByteBufferUtil.bytesToHex(input),
                                    ByteBufferUtil.bytesToHex(output));
            }
            ByteSource byteSource = partitionOrdering.asComparableBytes(exhausted, version);
            ByteBuffer output = partitionOrdering.fromComparableBytes(ByteSource.peekable(byteSource), version);
            Assert.assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, output);
        }
    }

    @Test
    public void testReversedType()
    {
        // Test how ReversedType handles null ByteSource.Peekable - here the choice of base type is important, as
        // the base type should also be able to handle null ByteSource.Peekable.
        ReversedType<BigInteger> reversedVarintType = ReversedType.getInstance(IntegerType.instance);
        ByteBuffer decodedNull = reversedVarintType.fromComparableBytes(null, ByteComparable.Version.OSS50);
        Assert.assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, decodedNull);

        // Test how ReversedType handles random data with some common and important base types.
        Map<AbstractType<?>, BiFunction<Random, Integer, ByteBuffer>> bufferGeneratorByType = new HashMap<>();
        bufferGeneratorByType.put(UTF8Type.instance, (prng, length) -> UTF8Type.instance.decompose(newRandomAlphanumeric(prng, length)));
        bufferGeneratorByType.put(BytesType.instance, (prng, length) ->
        {
            byte[] randomBytes = new byte[length];
            prng.nextBytes(randomBytes);
            return ByteBuffer.wrap(randomBytes);
        });
        bufferGeneratorByType.put(IntegerType.instance, (prng, length) ->
        {
            BigInteger randomVarint = BigInteger.valueOf(prng.nextLong());
            for (int i = 1; i < length / 8; ++i)
                randomVarint = randomVarint.multiply(BigInteger.valueOf(prng.nextLong()));
            return IntegerType.instance.decompose(randomVarint);
        });
        bufferGeneratorByType.put(DecimalType.instance, (prng, length) ->
        {
            BigInteger randomMantissa = BigInteger.valueOf(prng.nextLong());
            for (int i = 1; i < length / 8; ++i)
                randomMantissa = randomMantissa.multiply(BigInteger.valueOf(prng.nextLong()));
            // Remove all trailing zeros from the mantissa and use an even scale, in order to have a "canonically
            // represented" (in the context of DecimalType's encoding) decimal, i.e. one which wouldn't be re-scaled to
            // conform with the "compacted mantissa between 0 and 1, scale as a power of 100" rule.
            while (randomMantissa.remainder(BigInteger.TEN).equals(BigInteger.ZERO))
                randomMantissa = randomMantissa.divide(BigInteger.TEN);
            int randomScale = prng.nextInt() & -2;
            BigDecimal randomDecimal = new BigDecimal(randomMantissa, randomScale);
            return DecimalType.instance.decompose(randomDecimal);
        });
        Random prng = new Random();
        for (Map.Entry<AbstractType<?>, BiFunction<Random, Integer, ByteBuffer>> entry : bufferGeneratorByType.entrySet())
        {
            ReversedType<?> reversedType = ReversedType.getInstance(entry.getKey());
            for (int length = 32; length <= 512; length *= 4)
            {
                for (int i = 0; i < 100; ++i)
                {
                    ByteBuffer initial = entry.getValue().apply(prng, length);
                    ByteSource.Peekable reversedPeekable = ByteSource.peekable(reversedType.asComparableBytes(initial, ByteComparable.Version.OSS50));
                    ByteBuffer decoded = reversedType.fromComparableBytes(reversedPeekable, ByteComparable.Version.OSS50);
                    Assert.assertEquals(initial, decoded);
                }
            }
        }
    }

    @Test
    public void testSetType()
    {
        // Test sets with element components not having known/computable length (e.g. strings).
        Random prng = new Random();
        List<Set<String>> stringSets = newRandomElementCollections(HashSet::new,
                                                                   () -> newRandomAlphanumeric(prng, 10),
                                                                   100,
                                                                   100);
        testValuesForType(SetType.getInstance(UTF8Type.instance, false), stringSets);
        testValuesForType(SetType.getInstance(UTF8Type.instance, true), stringSets);
        // Test sets with element components with known/computable length (e.g. 128-bit UUIDs).
        List<Set<UUID>> uuidSets = newRandomElementCollections(HashSet::new,
                                                               UUID::randomUUID,
                                                               100,
                                                               100);
        testValuesForType(SetType.getInstance(UUIDType.instance, false), uuidSets);
        testValuesForType(SetType.getInstance(UUIDType.instance, true), uuidSets);
    }

    @Test
    public void testShortType()
    {
        testValuesForType(ShortType.instance, new Short[] { null });

        Stream<Short> allShorts = IntStream.range(Short.MIN_VALUE, Short.MAX_VALUE + 1)
                                           .mapToObj(value -> (short) value);
        testValuesForType(ShortType.instance, allShorts);
    }

    @Test
    public void testSimpleDateType()
    {
        testValuesForType(SimpleDateType.instance, new Integer[] { null });

        testValuesForType(SimpleDateType.instance, new Random().ints(1000).boxed());

        // Test by manually creating and manually interpreting simple dates from random millis.
        new Random().ints(1000).forEach(initialMillis ->
                                         {
                                             initialMillis = Math.abs(initialMillis);
                                             Integer initialDays = SimpleDateSerializer.timeInMillisToDay(initialMillis);
                                             ByteBuffer simpleDateBuffer = SimpleDateType.instance.fromTimeInMillis(initialMillis);
                                             ByteSource byteSource = SimpleDateType.instance.asComparableBytes(simpleDateBuffer, version);
                                             Integer decodedDays = SimpleDateType.instance.compose(SimpleDateType.instance.fromComparableBytes(ByteSource.peekable(byteSource), version));
                                             Assert.assertEquals(initialDays, decodedDays);
                                         });

        // Test by manually creating and manually interpreting simple dates from strings.
        String[] simpleDateStrings = new String[]
                                             {
                                                     "1970-01-01",
                                                     "1970-01-02",
                                                     "1969-12-31",
                                                     "-0001-01-02",
                                                     "-5877521-01-02",
                                                     "2014-01-01",
                                                     "+5881580-01-10",
                                                     "1920-12-01",
                                                     "1582-10-19"
                                             };
        for (String simpleDate : simpleDateStrings)
        {
            ByteBuffer simpleDataBuffer = SimpleDateType.instance.fromString(simpleDate);
            ByteSource byteSource = SimpleDateType.instance.asComparableBytes(simpleDataBuffer, version);
            Integer decodedDays = SimpleDateType.instance.compose(SimpleDateType.instance.fromComparableBytes(ByteSource.peekable(byteSource), version));
            String decodedDate = SimpleDateSerializer.instance.toString(decodedDays);
            Assert.assertEquals(simpleDate, decodedDate);
        }
    }

    @Test
    public void testTimestampType()
    {
        Date[] dates = new Date[]
                               {
                                       null,
                                       new Date(),
                                       new Date(0L),
                                       new Date(-1L),
                                       new Date(Long.MAX_VALUE),
                                       new Date(Long.MIN_VALUE)
                               };
        testValuesForType(TimestampType.instance, dates);
        testValuesForType(TimestampType.instance, new Random().longs(1000).mapToObj(Date::new));
    }

    @Test
    public void testTimeType()
    {
        testValuesForType(TimeType.instance, new Long[] { null });

        testValuesForType(TimeType.instance, new Random().longs(1000).boxed());
    }

    @Test
    public void testTupleType()
    {
        TupleType tt = new TupleType(Arrays.asList(UTF8Type.instance,
                                                   DecimalType.instance,
                                                   IntegerType.instance,
                                                   BytesType.instance));
        Random prng = new Random();
        List<ByteBuffer> tuplesData = new ArrayList<>();
        String[] utf8Values = new String[]
                                      {
                                              "a",
                                              "©",
                                              newRandomAlphanumeric(prng, 10),
                                              newRandomAlphanumeric(prng, 100)
                                      };
        BigDecimal[] decimalValues = new BigDecimal[]
                                             {
                                                     null,
                                                     BigDecimal.ZERO,
                                                     BigDecimal.ONE,
                                                     BigDecimal.valueOf(1234567891011121314L, 50),
                                                     BigDecimal.valueOf(1234567891011121314L, 50).negate()
                                             };
        BigInteger[] varintValues = new BigInteger[]
                                            {
                                                    null,
                                                    BigInteger.ZERO,
                                                    BigInteger.TEN.pow(1000),
                                                    BigInteger.TEN.pow(1000).negate()
                                            };
        byte[] oneByte = new byte[1];
        byte[] tenBytes = new byte[10];
        byte[] hundredBytes = new byte[100];
        byte[] thousandBytes = new byte[1000];
        prng.nextBytes(oneByte);
        prng.nextBytes(tenBytes);
        prng.nextBytes(hundredBytes);
        prng.nextBytes(thousandBytes);
        byte[][] bytesValues = new byte[][]
                                       {
                                               new byte[0],
                                               oneByte,
                                               tenBytes,
                                               hundredBytes,
                                               thousandBytes
                                       };
        for (String utf8 : utf8Values)
        {
            for (BigDecimal decimal : decimalValues)
            {
                for (BigInteger varint : varintValues)
                {
                    for (byte[] bytes : bytesValues)
                    {
                        ByteBuffer tupleData = TupleType.buildValue(UTF8Type.instance.decompose(utf8),
                                                                    decimal != null ? DecimalType.instance.decompose(decimal) : null,
                                                                    varint != null ? IntegerType.instance.decompose(varint) : null,
                                                                    // We could also use the wrapped bytes directly
                                                                    BytesType.instance.decompose(ByteBuffer.wrap(bytes)));
                        tuplesData.add(tupleData);
                    }
                }
            }
        }
        testValuesForType(tt, tuplesData.toArray(new ByteBuffer[0]));
    }

    @Test
    public void testUtf8Type()
    {
        Random prng = new Random();
        testValuesForType(UTF8Type.instance, Stream.generate(() -> newRandomAlphanumeric(prng, 100)).limit(1000));
    }

    @Test
    public void testTypeWithByteOrderedComparison()
    {
        Random prng = new Random();
        byte[] singleByte = new byte[] { (byte) prng.nextInt() };
        byte[] tenBytes = new byte[10];
        prng.nextBytes(tenBytes);
        byte[] hundredBytes = new byte[100];
        prng.nextBytes(hundredBytes);
        byte[] thousandBytes = new byte[1000];
        prng.nextBytes(thousandBytes);
        // No null here, as the default asComparableBytes(ByteBuffer, Version) implementation (and more specifically
        // the ByteSource.of(ByteBuffer, Version) encoding) would throw then.
        testValuesForType(ByteOrderedType.instance, Stream.of(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                              ByteBuffer.wrap(singleByte),
                                                              ByteBuffer.wrap(tenBytes),
                                                              ByteBuffer.wrap(hundredBytes),
                                                              ByteBuffer.wrap(thousandBytes)));
    }

    private static class ByteOrderedType extends AbstractType<ByteBuffer>
    {
        public static final ByteOrderedType instance = new ByteOrderedType();

        private ByteOrderedType()
        {
            super(ComparisonType.BYTE_ORDER);
        }

        @Override
        public ByteBuffer fromString(String source) throws MarshalException
        {
            return null;
        }

        @Override
        public Term fromJSONObject(Object parsed) throws MarshalException
        {
            return null;
        }

        @Override
        public TypeSerializer<ByteBuffer> getSerializer()
        {
            return ByteOrderedSerializer.instance;
        }

        static class ByteOrderedSerializer extends TypeSerializer<ByteBuffer>
        {

            static final ByteOrderedSerializer instance = new ByteOrderedSerializer();

            @Override
            public ByteBuffer serialize(ByteBuffer value)
            {
                return value != null ? value.duplicate() : null;
            }

            @Override
            public <V> ByteBuffer deserialize(V bytes, ValueAccessor<V> accessor)
            {
                return accessor.toBuffer(bytes);
            }

            @Override
            public <V> void validate(V bytes, ValueAccessor<V> accessor) throws MarshalException
            {

            }

            @Override
            public String toString(ByteBuffer value)
            {
                return ByteBufferUtil.bytesToHex(value);
            }

            @Override
            public Class<ByteBuffer> getType()
            {
                return ByteBuffer.class;
            }
        }
    }
}
