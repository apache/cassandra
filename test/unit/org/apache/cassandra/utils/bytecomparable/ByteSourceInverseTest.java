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

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.memory.MemoryUtil;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.stream.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
public class ByteSourceInverseTest
{
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()";

    @Parameterized.Parameters(name = "version={0}")
    public static Iterable<ByteComparable.Version> versions()
    {
        return ImmutableList.of(ByteComparable.Version.OSS50);
    }

    private final ByteComparable.Version version;

    public ByteSourceInverseTest(ByteComparable.Version version)
    {
        this.version = version;
    }

    @Test
    public void testGetSignedInt()
    {
        IntConsumer intConsumer = initial ->
        {
            ByteSource byteSource = ByteSource.of(initial);
            int decoded = ByteSourceInverse.getSignedInt(byteSource);
            Assert.assertEquals(initial, decoded);
        };

        IntStream.of(Integer.MIN_VALUE, Integer.MIN_VALUE + 1,
                     -256, -255, -128, -127, -1, 0, 1, 127, 128, 255, 256,
                     Integer.MAX_VALUE - 1, Integer.MAX_VALUE)
                 .forEach(intConsumer);
        new Random().ints(1000)
                    .forEach(intConsumer);
    }

    @Test
    public void testNextInt()
    {
        // The high and low 32 bits of this long differ only in the first and last bit (in the high 32 bits they are
        // both 0s instead of 1s). The first bit difference will be negated by the bit flipping when writing down a
        // fixed length signed number, so the only remaining difference will be in the last bit.
        int hi = 0b0001_0010_0011_0100_0101_0110_0111_1000;
        int lo = hi | 1 | 1 << 31;
        long l1 = Integer.toUnsignedLong(hi) << 32 | Integer.toUnsignedLong(lo);

        ByteSource byteSource = ByteSource.of(l1);
        int i1 = ByteSourceInverse.getSignedInt(byteSource);
        int i2 = ByteSourceInverse.getSignedInt(byteSource);
        Assert.assertEquals(i1 + 1, i2);

        try
        {
            ByteSourceInverse.getSignedInt(byteSource);
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            // Expected.
        }

        byteSource = ByteSource.of(l1);
        int iFirst = ByteSourceInverse.getSignedInt(byteSource);
        Assert.assertEquals(i1, iFirst);
        int iNext = ByteSourceInverse.getSignedInt(byteSource);
        Assert.assertEquals(i2, iNext);
    }

    @Test
    public void testGetSignedLong()
    {
        LongConsumer longConsumer = initial ->
        {
            ByteSource byteSource = ByteSource.of(initial);
            long decoded = ByteSourceInverse.getSignedLong(byteSource);
            Assert.assertEquals(initial, decoded);
        };

        LongStream.of(Long.MIN_VALUE, Long.MIN_VALUE + 1, Integer.MIN_VALUE - 1L,
                      -256L, -255L, -128L, -127L, -1L, 0L, 1L, 127L, 128L, 255L, 256L,
                      Integer.MAX_VALUE + 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE)
                  .forEach(longConsumer);
        new Random().longs(1000)
                    .forEach(longConsumer);
    }

    @Test
    public void testGetSignedByte()
    {
        Consumer<Byte> byteConsumer = boxedByte ->
        {
            byte initial = boxedByte;
            ByteBuffer byteBuffer = ByteType.instance.decompose(initial);
            ByteSource byteSource = ByteType.instance.asComparableBytes(byteBuffer, version);
            byte decoded = ByteSourceInverse.getSignedByte(byteSource);
            Assert.assertEquals(initial, decoded);
        };

        IntStream.range(Byte.MIN_VALUE, Byte.MAX_VALUE + 1)
                 .forEach(byteInteger -> byteConsumer.accept((byte) byteInteger));
    }

    @Test
    public void testGetSignedShort()
    {
        Consumer<Short> shortConsumer = boxedShort ->
        {
            short initial = boxedShort;
            ByteBuffer shortBuffer = ShortType.instance.decompose(initial);
            ByteSource byteSource = ShortType.instance.asComparableBytes(shortBuffer, version);
            short decoded = ByteSourceInverse.getSignedShort(byteSource);
            Assert.assertEquals(initial, decoded);
        };

        IntStream.range(Short.MIN_VALUE, Short.MAX_VALUE + 1)
                 .forEach(shortInteger -> shortConsumer.accept((short) shortInteger));
    }

    @Test
    public void testBadByteSourceForFixedLengthNumbers()
    {
        byte[] bytes = new byte[8];
        new Random().nextBytes(bytes);
        for (Map.Entry<String, Integer> entries : ImmutableMap.of("getSignedInt", 4,
                  "getSignedLong", 8,
                  "getSignedByte", 1,
                  "getSignedShort", 2).entrySet())
        {
            String methodName = entries.getKey();
            int length = entries.getValue();
            try
            {
                Method fixedLengthNumberMethod = ByteSourceInverse.class.getMethod(methodName, ByteSource.class);
                ArrayList<ByteSource> sources = new ArrayList<>();
                sources.add(null);
                sources.add(ByteSource.EMPTY);
                for (int i = 0; i < length; ++i)
                    sources.add(ByteSource.fixedLength(bytes, 0, i));
                // Note: not testing invalid bytes (e.g. using the construction below) as they signify a programming
                // error (throwing AssertionError) rather than something that could happen due to e.g. bad files.
                //      ByteSource.withTerminatorLegacy(257, ByteSource.fixedLength(bytes, 0, length - 1));
                for (ByteSource badSource : sources)
                {
                    try
                    {
                        fixedLengthNumberMethod.invoke(ByteSourceInverse.class, badSource);
                        Assert.fail("Expected exception not thrown");
                    }
                    catch (Throwable maybe)
                    {
                        maybe = Throwables.unwrapped(maybe);
                        final String message = "Unexpected throwable " + maybe + " with cause " + maybe.getCause();
                        if (badSource == null)
                            Assert.assertTrue(message,
                                              maybe instanceof NullPointerException);
                        else
                            Assert.assertTrue(message,
                                              maybe instanceof IllegalArgumentException);
                    }
                }
            }
            catch (NoSuchMethodException e)
            {
                Assert.fail("Expected ByteSourceInverse to have method called " + methodName
                            + " with a single parameter of type ByteSource");
            }
        }
    }

    @Test
    public void testBadByteSourceForVariableLengthNumbers()
    {
        for (long value : Arrays.asList(0L, 1L << 6, 1L << 13, 1L << 20, 1L << 27, 1L << 34, 1L << 41, 1L << 48, 1L << 55))
        {
            Assert.assertEquals(value, ByteSourceInverse.getVariableLengthInteger(ByteSource.variableLengthInteger(value)));

            ArrayList<ByteSource> sources = new ArrayList<>();
            sources.add(null);
            sources.add(ByteSource.EMPTY);
            int length = ByteComparable.length(version -> ByteSource.variableLengthInteger(value), ByteComparable.Version.OSS50);
            for (int i = 0; i < length; ++i)
                sources.add(ByteSource.cut(ByteSource.variableLengthInteger(value), i));

            for (ByteSource badSource : sources)
            {
                try
                {
                    ByteSourceInverse.getVariableLengthInteger(badSource);
                    Assert.fail("Expected exception not thrown");
                }
                catch (Throwable maybe)
                {
                    maybe = Throwables.unwrapped(maybe);
                    final String message = "Unexpected throwable " + maybe + " with cause " + maybe.getCause();
                    if (badSource == null)
                        Assert.assertTrue(message,
                                          maybe instanceof NullPointerException);
                    else
                        Assert.assertTrue(message,
                                          maybe instanceof IllegalArgumentException);
                }
            }
        }
    }

    @Test
    public void testGetString()
    {
        Consumer<String> stringConsumer = initial ->
        {
            ByteSource.Peekable byteSource = initial == null ? null : ByteSource.peekable(ByteSource.of(initial, version));
            String decoded = ByteSourceInverse.getString(byteSource);
            Assert.assertEquals(initial, decoded);
        };

        Stream.of(null, "© 2018 DataStax", "", "\n", "\0", "\0\0", "\001", "0", "0\0", "00", "1")
              .forEach(stringConsumer);

        Random prng = new Random();
        int stringLength = 10;
        String random;
        for (int i = 0; i < 1000; ++i)
        {
            random = newRandomAlphanumeric(prng, stringLength);
            stringConsumer.accept(random);
        }
    }

    private static String newRandomAlphanumeric(Random prng, int length)
    {
        StringBuilder random = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            random.append(ALPHABET.charAt(prng.nextInt(ALPHABET.length())));
        return random.toString();
    }

    @Test
    public void testGetByteBuffer()
    {
        for (Consumer<byte[]> byteArrayConsumer : Arrays.<Consumer<byte[]>>asList(initialBytes ->
            {
                ByteSource.Peekable byteSource = ByteSource.peekable(ByteSource.of(ByteBuffer.wrap(initialBytes), version));
                byte[] decodedBytes = ByteSourceInverse.getUnescapedBytes(byteSource);
                Assert.assertArrayEquals(initialBytes, decodedBytes);
            },
            initialBytes ->
            {
                ByteSource.Peekable byteSource = ByteSource.peekable(ByteSource.of(initialBytes, version));
                byte[] decodedBytes = ByteSourceInverse.getUnescapedBytes(byteSource);
                Assert.assertArrayEquals(initialBytes, decodedBytes);
            },
            initialBytes ->
            {
                long address = MemoryUtil.allocate(initialBytes.length);
                try
                {
                    MemoryUtil.setBytes(address, initialBytes, 0, initialBytes.length);
                    ByteSource.Peekable byteSource = ByteSource.peekable(ByteSource.ofMemory(address, initialBytes.length, version));
                    byte[] decodedBytes = ByteSourceInverse.getUnescapedBytes(byteSource);
                    Assert.assertArrayEquals(initialBytes, decodedBytes);
                }
                finally
                {
                    MemoryUtil.free(address);
                }
            }
            ))
        {
            for (byte[] tricky : Arrays.asList(
            // ESCAPE - leading, in the middle, trailing
            new byte[]{ 0, 2, 3, 4, 5 }, new byte[]{ 1, 2, 0, 4, 5 }, new byte[]{ 1, 2, 3, 4, 0 },
            // END_OF_STREAM/ESCAPED_0_DONE - leading, in the middle, trailing
            new byte[]{ -1, 2, 3, 4, 5 }, new byte[]{ 1, 2, -1, 4, 5 }, new byte[]{ 1, 2, 3, 4, -1 },
            // ESCAPED_0_CONT - leading, in the middle, trailing
            new byte[]{ -2, 2, 3, 4, 5 }, new byte[]{ 1, 2, -2, 4, 5 }, new byte[]{ 1, 2, 3, 4, -2 },
            // ESCAPE + ESCAPED_0_DONE - leading, in the middle, trailing
            new byte[]{ 0, -1, 3, 4, 5 }, new byte[]{ 1, 0, -1, 4, 5 }, new byte[]{ 1, 2, 3, 0, -1 },
            // ESCAPE + ESCAPED_0_CONT + ESCAPED_0_DONE - leading, in the middle, trailing
            new byte[]{ 0, -2, -1, 4, 5 }, new byte[]{ 1, 0, -2, -1, 5 }, new byte[]{ 1, 2, 0, -2, -1 }))
            {
                byteArrayConsumer.accept(tricky);
            }

            byte[] bytes = new byte[1000];
            Random prng = new Random();
            for (int i = 0; i < 1000; ++i)
            {
                prng.nextBytes(bytes);
                byteArrayConsumer.accept(bytes);
            }

            int stringLength = 10;
            String random;
            for (int i = 0; i < 1000; ++i)
            {
                random = newRandomAlphanumeric(prng, stringLength);
                byteArrayConsumer.accept(random.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    public void testReadBytes()
    {
        Map<Class<?>, Function<Object, ByteSource>> generatorPerType = new HashMap<>();
        List<Object> originalValues = new ArrayList<>();
        Random prng = new Random();

        generatorPerType.put(String.class, s ->
        {
            String string = (String) s;
            return ByteSource.of(string, version);
        });
        for (int i = 0; i < 100; ++i)
            originalValues.add(newRandomAlphanumeric(prng, 10));

        generatorPerType.put(Integer.class, i ->
        {
            Integer integer = (Integer) i;
            return ByteSource.of(integer);
        });
        for (int i = 0; i < 100; ++i)
            originalValues.add(prng.nextInt());

        generatorPerType.put(Long.class, l ->
        {
            Long looong = (Long) l;
            return ByteSource.of(looong);
        });
        for (int i = 0; i < 100; ++i)
            originalValues.add(prng.nextLong());

        generatorPerType.put(UUID.class, u ->
        {
            UUID uuid = (UUID) u;
            ByteBuffer uuidBuffer = UUIDType.instance.decompose(uuid);
            return UUIDType.instance.asComparableBytes(uuidBuffer, version);
        });
        for (int i = 0; i < 100; ++i)
            originalValues.add(UUID.randomUUID());

        for (Object value : originalValues)
        {
            Class<?> type = value.getClass();
            Function<Object, ByteSource> generator = generatorPerType.get(type);
            ByteSource originalSource = generator.apply(value);
            ByteSource originalSourceCopy = generator.apply(value);
            byte[] bytes = ByteSourceInverse.readBytes(originalSource);
            // The best way to test the read bytes seems to be to assert that just directly using them as a
            // ByteSource (using ByteSource.fixedLength(byte[])) they compare as equal to another ByteSource obtained
            // from the same original value.
            int compare = ByteComparable.compare(v -> originalSourceCopy, v -> ByteSource.fixedLength(bytes), version);
            Assert.assertEquals(0, compare);
        }
    }
}
