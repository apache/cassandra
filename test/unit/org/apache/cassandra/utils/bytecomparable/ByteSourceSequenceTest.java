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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class ByteSourceSequenceTest
{

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()";

    @Parameterized.Parameters(name = "version={0}")
    public static Iterable<ByteComparable.Version> versions()
    {
        return ImmutableList.of(ByteComparable.Version.OSS50);
    }

    private final ByteComparable.Version version;

    public ByteSourceSequenceTest(ByteComparable.Version version)
    {
        this.version = version;
    }

    @Test
    public void testNullsSequence()
    {
        ByteSource.Peekable comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                null, null, null
        ));
        expectNextComponentNull(comparableBytes);
        expectNextComponentNull(comparableBytes);
        expectNextComponentNull(comparableBytes);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());
    }

    @Test
    public void testNullsAndUnknownLengthsSequence()
    {
        ByteSource.Peekable comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                null, ByteSource.of("b", version), ByteSource.of("c", version)
        ));
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "b");
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "c");
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of("a", version), null, ByteSource.of("c", version)
        ));
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "a");
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "c");
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of("a", version), ByteSource.of("b", version), null
        ));
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "a");
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "b");
        expectNextComponentNull(comparableBytes);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of("a", version), null, null
        ));
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "a");
        expectNextComponentNull(comparableBytes);
        expectNextComponentNull(comparableBytes);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                null, null, ByteSource.of("c", version)
        ));
        expectNextComponentNull(comparableBytes);
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getString, "c");
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());
    }

    private static void expectNextComponentNull(ByteSource.Peekable comparableBytes)
    {
        // We expect null-signifying separator, followed by a null ByteSource component
        ByteSource.Peekable next = ByteSourceInverse.nextComponentSource(comparableBytes);
        assertNull(next);
    }

    private static <T> void expectNextComponentValue(ByteSource.Peekable comparableBytes,
                                                     Function<ByteSource.Peekable, T> decoder,
                                                     T expected)
    {
        // We expect a regular separator, followed by a ByteSource component corresponding to the expected value
        ByteSource.Peekable next = ByteSourceInverse.nextComponentSource(comparableBytes);
        assertNotNull(next);
        T decoded = decoder.apply(next);
        assertEquals(expected, decoded);
    }

    @Test
    public void testNullsAndKnownLengthsSequence()
    {
        int intValue = 42;
        BigInteger varintValue = BigInteger.valueOf(2018L);
        ByteSource.Peekable comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                null, ByteSource.of(intValue), varintToByteSource(varintValue)
        ));
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getSignedInt, intValue);
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of(intValue), null, varintToByteSource(varintValue)
        ));
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getSignedInt, intValue);
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of(intValue), varintToByteSource(varintValue), null
        ));
        expectNextComponentValue(comparableBytes, ByteSourceInverse::getSignedInt, intValue);
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        expectNextComponentNull(comparableBytes);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                null, null, varintToByteSource(varintValue)
        ));
        expectNextComponentNull(comparableBytes);
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                null, varintToByteSource(varintValue), null
        ));
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        expectNextComponentNull(comparableBytes);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                varintToByteSource(varintValue), null, null
        ));
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        expectNextComponentNull(comparableBytes);
        expectNextComponentNull(comparableBytes);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        Boolean boolValue = new Random().nextBoolean();
        ByteSource boolSource = BooleanType.instance.asComparableBytes(BooleanType.instance.decompose(boolValue), version);
        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                varintToByteSource(varintValue), boolSource, null
        ));
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        expectNextComponentValue(comparableBytes, BooleanType.instance, boolValue);
        expectNextComponentNull(comparableBytes);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());

        boolSource = BooleanType.instance.asComparableBytes(BooleanType.instance.decompose(boolValue), version);
        comparableBytes = ByteSource.peekable(ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                varintToByteSource(varintValue), null, boolSource
        ));
        expectNextComponentValue(comparableBytes, VARINT, varintValue);
        expectNextComponentNull(comparableBytes);
        expectNextComponentValue(comparableBytes, BooleanType.instance, boolValue);
        assertEquals(ByteSource.TERMINATOR, comparableBytes.next());
    }

    @Test
    public void testOptionalSignedFixedLengthTypesSequence()
    {
        Random prng = new Random();
        String randomString = newRandomAlphanumeric(prng, 10);
        byte randomByte = (byte) prng.nextInt();
        short randomShort = (short) prng.nextInt();
        int randomInt = prng.nextInt();
        long randomLong = prng.nextLong();
        BigInteger randomVarint = BigInteger.probablePrime(80, prng);

        Map<AbstractType<?>, ByteBuffer> valuesByType = new HashMap<AbstractType<?>, ByteBuffer>()
        {{
            put(ByteType.instance, ByteType.instance.decompose(randomByte));
            put(ShortType.instance, ShortType.instance.decompose(randomShort));
            put(SimpleDateType.instance, SimpleDateType.instance.decompose(randomInt));
            put(TimeType.instance, TimeType.instance.decompose(randomLong));
        }};

        for (Map.Entry<AbstractType<?>, ByteBuffer> entry : valuesByType.entrySet())
        {
            AbstractType<?> type = entry.getKey();
            ByteBuffer value = entry.getValue();

            ByteSource byteSource = type.asComparableBytes(value, version);
            ByteSource.Peekable sequence = ByteSource.peekable(ByteSource.withTerminator(
                    ByteSource.TERMINATOR,
                    ByteSource.of(randomString, version), byteSource, varintToByteSource(randomVarint)
            ));
            expectNextComponentValue(sequence, ByteSourceInverse::getString, randomString);
            expectNextComponentValue(sequence, type, value);
            expectNextComponentValue(sequence, VARINT, randomVarint);
            assertEquals(ByteSource.TERMINATOR, sequence.next());

            byteSource = type.asComparableBytes(type.decompose(null), version);
            sequence = ByteSource.peekable(ByteSource.withTerminator(
                    ByteSource.TERMINATOR,
                    ByteSource.of(randomString, version), byteSource, varintToByteSource(randomVarint)
            ));
            expectNextComponentValue(sequence, ByteSourceInverse::getString, randomString);
            expectNextComponentNull(sequence);
            expectNextComponentValue(sequence, VARINT, randomVarint);
            assertEquals(ByteSource.TERMINATOR, sequence.next());
        }
    }

    private ByteSource varintToByteSource(BigInteger value)
    {
        ByteBuffer valueByteBuffer = VARINT.decompose(value);
        return VARINT.asComparableBytes(valueByteBuffer, version);
    }

    private static final UTF8Type UTF8 = UTF8Type.instance;
    private static final DecimalType DECIMAL = DecimalType.instance;
    private static final IntegerType VARINT = IntegerType.instance;

    // A regular comparator using the natural ordering for all types.
    private static final ClusteringComparator COMP = new ClusteringComparator(Arrays.asList(
            UTF8,
            DECIMAL,
            VARINT
    ));
    // A comparator that reverses the ordering for the first unknown length type
    private static final ClusteringComparator COMP_REVERSED_UNKNOWN_LENGTH = new ClusteringComparator(Arrays.asList(
            ReversedType.getInstance(UTF8),
            DECIMAL,
            VARINT
    ));
    // A comparator that reverses the ordering for the second unknown length type
    private static final ClusteringComparator COMP_REVERSED_UNKNOWN_LENGTH_2 = new ClusteringComparator(Arrays.asList(
            UTF8,
            ReversedType.getInstance(DECIMAL),
            VARINT
    ));
    // A comparator that reverses the ordering for the sole known/computable length type
    private static final ClusteringComparator COMP_REVERSED_KNOWN_LENGTH = new ClusteringComparator(Arrays.asList(
            UTF8,
            DECIMAL,
            ReversedType.getInstance(VARINT)
    ));
    // A comparator that reverses the ordering for all types
    private static final ClusteringComparator COMP_ALL_REVERSED = new ClusteringComparator(Arrays.asList(
            ReversedType.getInstance(UTF8),
            ReversedType.getInstance(DECIMAL),
            ReversedType.getInstance(VARINT)
    ));

    @Test
    public void testClusteringPrefixBoundNormalAndReversed()
    {
        String stringValue = "Lorem ipsum dolor sit amet";
        BigDecimal decimalValue = BigDecimal.valueOf(123456789, 20);
        BigInteger varintValue = BigInteger.valueOf(2018L);

        // Create some non-null clustering key values that will be encoded and decoded to byte-ordered representation
        // with different types of clustering comparators (and in other tests with different types of prefixes).
        ByteBuffer[] clusteringKeyValues = new ByteBuffer[] {
                UTF8.decompose(stringValue),
                DECIMAL.decompose(decimalValue),
                VARINT.decompose(varintValue)
        };

        for (ClusteringPrefix.Kind prefixKind : ClusteringPrefix.Kind.values())
        {
            if (prefixKind.isBoundary())
                continue;

            ClusteringPrefix prefix = BufferClusteringBound.create(prefixKind, clusteringKeyValues);
            // Use the regular comparator.
            ByteSource.Peekable comparableBytes = ByteSource.peekable(COMP.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, UTF8, stringValue);
            expectNextComponentValue(comparableBytes, DECIMAL, decimalValue);
            expectNextComponentValue(comparableBytes, VARINT, varintValue);

            prefix = BufferClusteringBound.create(prefixKind, clusteringKeyValues);
            // Use the comparator reversing the ordering for the first unknown length type.
            comparableBytes = ByteSource.peekable(COMP_REVERSED_UNKNOWN_LENGTH.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, ReversedType.getInstance(UTF8), stringValue);
            expectNextComponentValue(comparableBytes, DECIMAL, decimalValue);
            expectNextComponentValue(comparableBytes, VARINT, varintValue);

            prefix = BufferClusteringBound.create(prefixKind, clusteringKeyValues);
            // Use the comparator reversing the ordering for the second unknown length type.
            comparableBytes = ByteSource.peekable(COMP_REVERSED_UNKNOWN_LENGTH_2.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, UTF8, stringValue);
            expectNextComponentValue(comparableBytes, ReversedType.getInstance(DECIMAL), decimalValue);
            expectNextComponentValue(comparableBytes, VARINT, varintValue);

            prefix = BufferClusteringBound.create(prefixKind, clusteringKeyValues);
            // Use the comparator reversing the ordering for the known/computable length type.
            comparableBytes = ByteSource.peekable(COMP_REVERSED_KNOWN_LENGTH.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, UTF8, stringValue);
            expectNextComponentValue(comparableBytes, DECIMAL, decimalValue);
            expectNextComponentValue(comparableBytes, ReversedType.getInstance(VARINT), varintValue);

            prefix = BufferClusteringBound.create(prefixKind, clusteringKeyValues);
            // Use the all-reversing comparator.
            comparableBytes = ByteSource.peekable(COMP_ALL_REVERSED.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, ReversedType.getInstance(UTF8), stringValue);
            expectNextComponentValue(comparableBytes, ReversedType.getInstance(DECIMAL), decimalValue);
            expectNextComponentValue(comparableBytes, ReversedType.getInstance(VARINT), varintValue);
        }
    }

    @Test
    public void testClusteringPrefixBoundNulls()
    {
        String stringValue = "Lorem ipsum dolor sit amet";
        BigDecimal decimalValue = BigDecimal.valueOf(123456789, 20);
        BigInteger varintValue = BigInteger.valueOf(2018L);

        // Create clustering key values where the component for an unknown length type is null.
        ByteBuffer[] unknownLengthNull = new ByteBuffer[] {
                UTF8.decompose(stringValue),
                DECIMAL.decompose(null),
                VARINT.decompose(varintValue)
        };
        // Create clustering key values where the component for a known/computable length type is null.
        ByteBuffer[] knownLengthNull = new ByteBuffer[] {
                UTF8.decompose(stringValue),
                DECIMAL.decompose(decimalValue),
                VARINT.decompose(null)
        };

        for (ClusteringPrefix.Kind prefixKind : ClusteringPrefix.Kind.values())
        {
            if (prefixKind.isBoundary())
                continue;

            // Test the decoding of a null component of a non-reversed unknown length type.
            ClusteringPrefix prefix = BufferClusteringBound.create(prefixKind, unknownLengthNull);
            ByteSource.Peekable comparableBytes = ByteSource.peekable(COMP.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, UTF8, stringValue);
            expectNextComponentNull(comparableBytes);
            expectNextComponentValue(comparableBytes, VARINT, varintValue);
            // Test the decoding of a null component of a reversed unknown length type.
            prefix = BufferClusteringBound.create(prefixKind, unknownLengthNull);
            comparableBytes = ByteSource.peekable(COMP_REVERSED_UNKNOWN_LENGTH_2.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, UTF8, stringValue);
            expectNextComponentNull(comparableBytes);
            expectNextComponentValue(comparableBytes, VARINT, varintValue);

            // Test the decoding of a null component of a non-reversed known/computable length type.
            prefix = BufferClusteringBound.create(prefixKind, knownLengthNull);
            comparableBytes = ByteSource.peekable(COMP.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, UTF8, stringValue);
            expectNextComponentValue(comparableBytes, DECIMAL, decimalValue);
            expectNextComponentNull(comparableBytes);
            // Test the decoding of a null component of a reversed known/computable length type.
            prefix = BufferClusteringBound.create(prefixKind, knownLengthNull);
            comparableBytes = ByteSource.peekable(COMP_REVERSED_KNOWN_LENGTH.asByteComparable(prefix).asComparableBytes(version));
            expectNextComponentValue(comparableBytes, UTF8, stringValue);
            expectNextComponentValue(comparableBytes, DECIMAL, decimalValue);
            expectNextComponentNull(comparableBytes);
        }
    }

    private <T> void expectNextComponentValue(ByteSource.Peekable comparableBytes,
                                              AbstractType<T> type,
                                              T expected)
    {
        // We expect a regular separator, followed by a ByteSource component corresponding to the expected value
        ByteSource.Peekable next = ByteSourceInverse.nextComponentSource(comparableBytes);
        T decoded = type.compose(type.fromComparableBytes(next, version));
        assertEquals(expected, decoded);
    }

    private void expectNextComponentValue(ByteSource.Peekable comparableBytes,
                                          AbstractType<?> type,
                                          ByteBuffer expected)
    {
        // We expect a regular separator, followed by a ByteSource component corresponding to the expected value
        ByteSource.Peekable next = ByteSourceInverse.nextComponentSource(comparableBytes);
        assertEquals(expected, type.fromComparableBytes(next, version));
    }

    @Test
    public void testGetBoundFromPrefixTerminator()
    {
        String stringValue = "Lorem ipsum dolor sit amet";
        BigDecimal decimalValue = BigDecimal.valueOf(123456789, 20);
        BigInteger varintValue = BigInteger.valueOf(2018L);

        ByteBuffer[] clusteringKeyValues = new ByteBuffer[] {
                UTF8.decompose(stringValue),
                DECIMAL.decompose(decimalValue),
                VARINT.decompose(varintValue)
        };
        ByteBuffer[] nullValueBeforeTerminator = new ByteBuffer[] {
                UTF8.decompose(stringValue),
                DECIMAL.decompose(decimalValue),
                VARINT.decompose(null)
        };

        for (ClusteringPrefix.Kind prefixKind : ClusteringPrefix.Kind.values())
        {
            // NOTE dimitar.dimitrov I assume there's a sensible explanation why does STATIC_CLUSTERING use a custom
            // terminator that's not one of the common separator values, but I haven't spent enough time to get it.
            if (prefixKind.isBoundary())
                continue;

            // Test that the read terminator value is exactly the encoded value of this prefix' bound.
            ClusteringPrefix prefix = BufferClusteringBound.create(prefixKind, clusteringKeyValues);
            ByteSource.Peekable comparableBytes = ByteSource.peekable(COMP.asByteComparable(prefix).asComparableBytes(version));
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            ByteSourceInverse.getString(comparableBytes);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            DECIMAL.fromComparableBytes(comparableBytes, version);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            VARINT.fromComparableBytes(comparableBytes, version);
            // Expect the last separator (i.e. the terminator) to be the one specified by the prefix kind.
            assertEquals(prefixKind.asByteComparableValue(version), comparableBytes.next());

            // Test that the read terminator value is exactly the encoded value of this prefix' bound, when the
            // terminator is preceded by a null value.
            prefix = BufferClusteringBound.create(prefixKind, nullValueBeforeTerminator);
            comparableBytes = ByteSource.peekable(COMP.asByteComparable(prefix).asComparableBytes(version));
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            ByteSourceInverse.getString(comparableBytes);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            DECIMAL.fromComparableBytes(comparableBytes, version);
            // Expect null-signifying separator here.
            assertEquals(ByteSource.NEXT_COMPONENT_EMPTY, comparableBytes.next());
            // No varint to read
            // Expect the last separator (i.e. the terminator) to be the one specified by the prefix kind.
            assertEquals(prefixKind.asByteComparableValue(version), comparableBytes.next());

            // Test that the read terminator value is exactly the encoded value of this prefix' bound, when the
            // terminator is preceded by a reversed null value.
            prefix = BufferClusteringBound.create(prefixKind, nullValueBeforeTerminator);
            // That's the comparator that will reverse the ordering of the type of the last value in the prefix (the
            // one before the terminator). In other tests we're more interested in the fact that values of this type
            // have known/computable length, which is why we've named it so...
            comparableBytes = ByteSource.peekable(COMP_REVERSED_KNOWN_LENGTH.asByteComparable(prefix).asComparableBytes(version));
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            ByteSourceInverse.getString(comparableBytes);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            DECIMAL.fromComparableBytes(comparableBytes, version);
            // Expect reversed null-signifying separator here.
            assertEquals(ByteSource.NEXT_COMPONENT_EMPTY_REVERSED, comparableBytes.next());
            // No varint to read
            // Expect the last separator (i.e. the terminator) to be the one specified by the prefix kind.
            assertEquals(prefixKind.asByteComparableValue(version), comparableBytes.next());
        }
    }

    @Test
    public void testReversedTypesInClusteringKey()
    {
        String stringValue = "Lorem ipsum dolor sit amet";
        BigDecimal decimalValue = BigDecimal.valueOf(123456789, 20);

        AbstractType<String> reversedStringType = ReversedType.getInstance(UTF8);
        AbstractType<BigDecimal> reversedDecimalType = ReversedType.getInstance(DECIMAL);

        final ClusteringComparator comparator = new ClusteringComparator(Arrays.asList(
                // unknown length type
                UTF8,
                // known length type
                DECIMAL,
                // reversed unknown length type
                reversedStringType,
                // reversed known length type
                reversedDecimalType
        ));
        ByteBuffer[] clusteringKeyValues = new ByteBuffer[] {
                UTF8.decompose(stringValue),
                DECIMAL.decompose(decimalValue),
                UTF8.decompose(stringValue),
                DECIMAL.decompose(decimalValue)
        };

        final ClusteringComparator comparator2 = new ClusteringComparator(Arrays.asList(
                // known length type
                DECIMAL,
                // unknown length type
                UTF8,
                // reversed known length type
                reversedDecimalType,
                // reversed unknown length type
                reversedStringType
        ));
        ByteBuffer[] clusteringKeyValues2 = new ByteBuffer[] {
                DECIMAL.decompose(decimalValue),
                UTF8.decompose(stringValue),
                DECIMAL.decompose(decimalValue),
                UTF8.decompose(stringValue)
        };

        for (ClusteringPrefix.Kind prefixKind : ClusteringPrefix.Kind.values())
        {
            if (prefixKind.isBoundary())
                continue;

            ClusteringPrefix prefix = BufferClusteringBound.create(prefixKind, clusteringKeyValues);
            ByteSource.Peekable comparableBytes = ByteSource.peekable(comparator.asByteComparable(prefix).asComparableBytes(version));

            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            assertEquals(getComponentValue(UTF8, comparableBytes), stringValue);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            assertEquals(getComponentValue(DECIMAL, comparableBytes), decimalValue);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            assertEquals(getComponentValue(reversedStringType, comparableBytes), stringValue);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
            assertEquals(getComponentValue(reversedDecimalType, comparableBytes), decimalValue);

            assertEquals(prefixKind.asByteComparableValue(version), comparableBytes.next());
            assertEquals(ByteSource.END_OF_STREAM, comparableBytes.next());

            ClusteringPrefix prefix2 = BufferClusteringBound.create(prefixKind, clusteringKeyValues2);
            ByteSource.Peekable comparableBytes2 = ByteSource.peekable(comparator2.asByteComparable(prefix2).asComparableBytes(version));

            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes2.next());
            assertEquals(getComponentValue(DECIMAL, comparableBytes2), decimalValue);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes2.next());
            assertEquals(getComponentValue(UTF8, comparableBytes2), stringValue);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes2.next());
            assertEquals(getComponentValue(reversedDecimalType, comparableBytes2), decimalValue);
            assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes2.next());
            assertEquals(getComponentValue(reversedStringType, comparableBytes2), stringValue);

            assertEquals(prefixKind.asByteComparableValue(version), comparableBytes2.next());
            assertEquals(ByteSource.END_OF_STREAM, comparableBytes2.next());
        }
    }

    private <T extends AbstractType<E>, E> E getComponentValue(T type, ByteSource.Peekable comparableBytes)
    {
        return type.compose(type.fromComparableBytes(comparableBytes, version));
    }

    @Test
    public void testReadingNestedSequence_Simple()
    {
        String padding1 = "A string";
        String padding2 = "Another string";

        BigInteger varint1 = BigInteger.valueOf(0b10000000);
        BigInteger varint2 = BigInteger.valueOf(1 >> 30);
        BigInteger varint3 = BigInteger.valueOf(0x10000000L);
        BigInteger varint4 = BigInteger.valueOf(Long.MAX_VALUE);

        String string1 = "Testing byte sources";
        String string2 = "is neither easy nor fun;";
        String string3 = "But do it we must.";
        String string4 = "— DataStax, 2018";

        MapType<BigInteger, String> varintStringMapType = MapType.getInstance(VARINT, UTF8, false);
        Map<BigInteger, String> varintStringMap = new TreeMap<>();
        varintStringMap.put(varint1, string1);
        varintStringMap.put(varint2, string2);
        varintStringMap.put(varint3, string3);
        varintStringMap.put(varint4, string4);

        ByteSource sequence = ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of(padding1, version),
                varintStringMapType.asComparableBytes(varintStringMapType.decompose(varintStringMap), version),
                ByteSource.of(padding2, version)
        );
        ByteSource.Peekable comparableBytes = ByteSource.peekable(sequence);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding1);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(varintStringMapType, comparableBytes), varintStringMap);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding2);
        sequence = ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                varintStringMapType.asComparableBytes(varintStringMapType.decompose(varintStringMap), version),
                ByteSource.of(padding1, version),
                ByteSource.of(padding2, version)
        );
        comparableBytes = ByteSource.peekable(sequence);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(varintStringMapType, comparableBytes), varintStringMap);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding1);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding2);
        sequence = ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of(padding1, version),
                ByteSource.of(padding2, version),
                varintStringMapType.asComparableBytes(varintStringMapType.decompose(varintStringMap), version)
        );
        comparableBytes = ByteSource.peekable(sequence);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding1);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding2);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(varintStringMapType, comparableBytes), varintStringMap);

        MapType<String, BigInteger> stringVarintMapType = MapType.getInstance(UTF8, VARINT, false);
        Map<String, BigInteger> stringVarintMap = new HashMap<>();
        stringVarintMap.put(string1, varint1);
        stringVarintMap.put(string2, varint2);
        stringVarintMap.put(string3, varint3);
        stringVarintMap.put(string4, varint4);

        sequence = ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of(padding1, version),
                stringVarintMapType.asComparableBytes(stringVarintMapType.decompose(stringVarintMap), version),
                ByteSource.of(padding2, version)
        );
        comparableBytes = ByteSource.peekable(sequence);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding1);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(stringVarintMapType, comparableBytes), stringVarintMap);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding2);

        MapType<String, String> stringStringMapType = MapType.getInstance(UTF8, UTF8, false);
        Map<String, String> stringStringMap = new HashMap<>();
        stringStringMap.put(string1, string4);
        stringStringMap.put(string2, string3);
        stringStringMap.put(string3, string2);
        stringStringMap.put(string4, string1);

        sequence = ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of(padding1, version),
                stringStringMapType.asComparableBytes(stringStringMapType.decompose(stringStringMap), version),
                ByteSource.of(padding2, version)
        );
        comparableBytes = ByteSource.peekable(sequence);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding1);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(stringStringMapType, comparableBytes), stringStringMap);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding2);

        MapType<BigInteger, BigInteger> varintVarintMapType = MapType.getInstance(VARINT, VARINT, false);
        Map<BigInteger, BigInteger> varintVarintMap = new HashMap<>();
        varintVarintMap.put(varint1, varint4);
        varintVarintMap.put(varint2, varint3);
        varintVarintMap.put(varint3, varint2);
        varintVarintMap.put(varint4, varint1);

        sequence = ByteSource.withTerminator(
                ByteSource.TERMINATOR,
                ByteSource.of(padding1, version),
                varintVarintMapType.asComparableBytes(varintVarintMapType.decompose(varintVarintMap), version),
                ByteSource.of(padding2, version)
        );
        comparableBytes = ByteSource.peekable(sequence);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding1);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(varintVarintMapType, comparableBytes), varintVarintMap);
        assertEquals(ByteSource.NEXT_COMPONENT, comparableBytes.next());
        assertEquals(getComponentValue(UTF8, comparableBytes), padding2);
    }

    @Test
    public void testReadingNestedSequence_DecoratedKey()
    {
        Random prng = new Random();

        MapType<String, BigDecimal> stringDecimalMapType = MapType.getInstance(UTF8, DECIMAL, false);
        Map<String, BigDecimal> stringDecimalMap = new HashMap<>();
        for (int i = 0; i < 4; ++i)
            stringDecimalMap.put(newRandomAlphanumeric(prng, 10), BigDecimal.valueOf(prng.nextDouble()));
        ByteBuffer key = stringDecimalMapType.decompose(stringDecimalMap);
        testDecodingKeyWithLocalPartitionerForType(key, stringDecimalMapType);

        MapType<BigDecimal, String> decimalStringMapType = MapType.getInstance(DECIMAL, UTF8, false);
        Map<BigDecimal, String> decimalStringMap = new HashMap<>();
        for (int i = 0; i < 4; ++i)
            decimalStringMap.put(BigDecimal.valueOf(prng.nextDouble()), newRandomAlphanumeric(prng, 10));
        key = decimalStringMapType.decompose(decimalStringMap);
        testDecodingKeyWithLocalPartitionerForType(key, decimalStringMapType);

        if (version != ByteComparable.Version.LEGACY)
        {
            CompositeType stringDecimalCompType = CompositeType.getInstance(UTF8, DECIMAL);
            key = stringDecimalCompType.decompose(newRandomAlphanumeric(prng, 10), BigDecimal.valueOf(prng.nextDouble()));
            testDecodingKeyWithLocalPartitionerForType(key, stringDecimalCompType);

            CompositeType decimalStringCompType = CompositeType.getInstance(DECIMAL, UTF8);
            key = decimalStringCompType.decompose(BigDecimal.valueOf(prng.nextDouble()), newRandomAlphanumeric(prng, 10));
            testDecodingKeyWithLocalPartitionerForType(key, decimalStringCompType);

            DynamicCompositeType dynamicCompType = DynamicCompositeType.getInstance(DynamicCompositeTypeTest.aliases);
            key = DynamicCompositeTypeTest.createDynamicCompositeKey(
                    newRandomAlphanumeric(prng, 10), TimeUUID.Generator.nextTimeAsUUID(), 42, true, false);
            testDecodingKeyWithLocalPartitionerForType(key, dynamicCompType);

            key = DynamicCompositeTypeTest.createDynamicCompositeKey(
            newRandomAlphanumeric(prng, 10), TimeUUID.Generator.nextTimeAsUUID(), 42, true, true);
            testDecodingKeyWithLocalPartitionerForType(key, dynamicCompType);
        }
    }

    private static String newRandomAlphanumeric(Random prng, int length)
    {
        StringBuilder random = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            random.append(ALPHABET.charAt(prng.nextInt(ALPHABET.length())));
        return random.toString();
    }

    private <T> void testDecodingKeyWithLocalPartitionerForType(ByteBuffer key, AbstractType<T> type)
    {
        IPartitioner partitioner = new LocalPartitioner(type);
        CachedHashDecoratedKey initial = (CachedHashDecoratedKey) partitioner.decorateKey(key);
        BufferDecoratedKey base = BufferDecoratedKey.fromByteComparable(initial, version, partitioner);
        CachedHashDecoratedKey decoded = new CachedHashDecoratedKey(base.getToken(), base.getKey());
        Assert.assertEquals(initial, decoded);
    }
}
