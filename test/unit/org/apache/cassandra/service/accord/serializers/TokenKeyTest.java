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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.LazyToString;
import accord.utils.ReflectionUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.api.TokenKey.serializer;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.apache.cassandra.utils.CassandraGenerators.partitioners;
import static org.apache.cassandra.utils.CassandraGenerators.token;

public class TokenKeyTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Before
    public void before()
    {
        // AccordRoutingKey$TokenKey reaches into DD to get partitioner, so need to set that up...
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void beforeIsTokenSentinel()
    {
        qt().forAll(simpleTokenKey()).check(tokenKey -> {
            var t = tokenKey.before();
            Assertions.assertThat(t.isTokenSentinel()).isTrue();
            Assertions.assertThat(t.isTableSentinel()).isEqualTo(tokenKey.isTableSentinel());
            Assertions.assertThat(t.isMin()).isEqualTo(tokenKey.isMin());
            Assertions.assertThat(t.isMax()).isEqualTo(tokenKey.isMax());
            Assertions.assertThat(t.isBefore()).isTrue();
            Assertions.assertThat(t.isAfter()).isFalse();

            Assertions.assertThatThrownBy(() -> t.before());
            Assertions.assertThatThrownBy(() -> t.after());

            Assertions.assertThat(tokenKey.compareTo(t)).isGreaterThan(0);
            Assertions.assertThat(t.compareTo(tokenKey)).isLessThan(0);
        });
    }

    @Test
    public void afterIsTokenSentinel()
    {
        qt().forAll(simpleTokenKey()).check(tokenKey -> {
            var t = tokenKey.after();
            Assertions.assertThat(t.isTokenSentinel()).isTrue();
            Assertions.assertThat(t.isTableSentinel()).isEqualTo(tokenKey.isTableSentinel());
            Assertions.assertThat(t.isMin()).isEqualTo(tokenKey.isMin());
            Assertions.assertThat(t.isMax()).isEqualTo(tokenKey.isMax());
            Assertions.assertThat(t.isBefore()).isFalse();
            Assertions.assertThat(t.isAfter()).isTrue();

            Assertions.assertThat(tokenKey.compareTo(t)).isLessThan(0);
            Assertions.assertThat(t.compareTo(tokenKey)).isGreaterThan(0);
        });
    }

    @Test
    public void serdeSimple()
    {
        Gen<TokenKey> tokenKeyGen = AccordGenerators.allowBeforeAndAfter(simpleTokenKey());
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(tokenKeyGen).check(expected -> {
            DatabaseDescriptor.setPartitionerUnsafe(expected.token().getPartitioner());
            Serializers.testSerde(output, serializer, expected);
            testSerdePrefix(output, serializer, expected);
        });
    }

    @Test
    public void serde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(tokenKeyWithBeforeAndAfterGen())
            .check(key -> {
                IPartitioner partitioner = key.token().getPartitioner();
                DatabaseDescriptor.setPartitionerUnsafe(partitioner);

                Serializers.testSerde(output, serializer, key);
                Assertions.assertThat(serializer.deserializeAndConsume(serializer.serialize(key), partitioner)).isEqualTo(key);
                {
                    TokenKey roundTrip = serializer.deserializeWithPrefixAndImpliedLength(key.prefix(), serializer.serializeWithoutPrefixOrLength(key), partitioner);
                    Assertions.assertThat(roundTrip).isEqualTo(key);
                }
                {
                    TokenKey roundTrip = serializer.deserializeWithPrefixAndImpliedLength(key.prefix(), serializer.serializeWithoutPrefixOrLength(key), ByteBufferAccessor.instance, 0, partitioner);
                    Assertions.assertThat(roundTrip).isEqualTo(key);
                }
                {
                    TokenKey roundTrip = serializer.deserializeWithPrefix(key.prefix(), serializer.serializedSizeWithoutPrefix(key), serializer.serializeWithoutPrefixOrLength(key), partitioner);
                    Assertions.assertThat(roundTrip).isEqualTo(key);
                }
                {
                    TokenKey roundTrip = serializer.deserializeWithPrefix(key.prefix(), serializer.serializedSizeWithoutPrefix(key), serializer.serializeWithoutPrefixOrLength(key), ByteBufferAccessor.instance, 0, partitioner);
                    Assertions.assertThat(roundTrip).isEqualTo(key);
                }
                output.clear();
                serializer.serialize(key, output);
                try (DataInputBuffer in = new DataInputBuffer(output.toByteArray()))
                {
                    serializer.skip(in, partitioner);
                    Invariants.require(0 == in.available());
                }
            });
    }

    @Test
    public void compare()
    {
        qt().forAll(tokenKeyGen())
            .check(key -> {
                ByteBuffer keyBytes = serializer.serialize(key);
                for (TokenKey test : mutateAfter(key))
                {
                    ByteBuffer testBytes = serializer.serialize(test);
                    Invariants.require(test.compareTo(key) > 0);
                    Invariants.require(ByteBufferUtil.compareUnsigned(testBytes, keyBytes) > 0);
                }
                for (TokenKey test : mutateBefore(key))
                {
                    ByteBuffer testBytes = serializer.serialize(test);
                    Invariants.require(test.compareTo(key) < 0);
                    Invariants.require(ByteBufferUtil.compareUnsigned(testBytes, keyBytes) < 0);
                }
            });
    }

    @Test
    public void serdeNoTable()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(tokenKeyGen()).check(key -> {
            AccordGenerators.maybeUpdatePartitioner(key);
            Serializers.testSerde(output, TokenKey.noTableSerializer, key, key.table());
        });
    }

    private static Gen<TokenKey> simpleTokenKey()
    {
        return AccordGenerators.partitioner().flatMap(p -> AccordGenerators.routingKeysGen(p));
    }

    private static void testSerdePrefix(DataOutputBuffer output, TokenKey.Serializer serializer, TokenKey input) throws IOException
    {
        output.clear();
        Object expected = input.prefix();
        long expectedSize = serializer.serializedSizeOfPrefix(expected);
        serializer.serializePrefix(expected, output);
        Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
        DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        Object read = serializer.deserializePrefix(in);
        Assertions.assertThat(read)
                  .describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, expected).toString()))
                  .isEqualTo(expected);
    }

    private static Gen<TokenKey> tokenKeyGen()
    {
        return fromQT(partitioners()).filter(IPartitioner::accordSupported)
               .flatMap(partitioner -> routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(token(partitioner)), partitioner));
    }

    private static Gen<TokenKey> tokenKeyWithBeforeAndAfterGen()
    {
        return AccordGenerators.allowBeforeAndAfter(tokenKeyGen());
    }

    private List<TokenKey> mutateAfter(TokenKey mutate)
    {
        List<TokenKey> results = new ArrayList<>();
        if (!mutate.isTableSentinel())
        {
            Token token = mutate.token();
            if (token instanceof ByteOrderedPartitioner.BytesToken)
            {
                byte[] bytes = (byte[]) token.getTokenValue();
                bytes = bytes.clone();
                for (int i = 0 ; i < bytes.length ; ++i)
                {
                    if ((bytes[i] & 0xff) != 0xff)
                    {
                        ++bytes[i];
                        add(results, mutate.withToken(new ByteOrderedPartitioner.BytesToken(bytes.clone())));
                        --bytes[i];
                    }
                }
                add(results, mutate.withToken(new ByteOrderedPartitioner.BytesToken(Arrays.copyOf(bytes, bytes.length + 1))));
            }
            else if (token instanceof LongToken)
            {
                long value = token.getLongValue();
                if (value < Long.MAX_VALUE)
                    add(results, mutate.withToken(new LongToken(value + 1)));
                for (long v = 2L; v >= 0 ; v <<= 1)
                {
                    if ((value & v) == 0)
                        add(results, mutate.withToken(new LongToken(value | v)));
                }
                if (value >= 0)
                {
                    long higher = value;
                    while ((higher <<= 8) > value)
                        add(results, mutate.withToken(new LongToken(higher)));
                }
                else
                {
                    for (int i = 1 ; i < 8 ; ++i)
                        add(results, mutate.withToken(new LongToken(value >> (i * 8))));
                }
            }
            else if (token instanceof BigIntegerToken)
            {
                BigInteger value = (BigInteger) token.getTokenValue();
                if (value.compareTo(RandomPartitioner.MAXIMUM) < 0)
                    add(results, mutate.withToken(new BigIntegerToken(value.add(BigInteger.ONE))));
                for (long v = 1L; v >= 0 ; v <<= 1)
                {
                    BigInteger i = BigInteger.valueOf(v);
                    if (value.and(i).equals(BigInteger.ZERO))
                        add(results, mutate.withToken(new BigIntegerToken(value.or(i))));
                }
                BigInteger higher = value;
                while ((higher = higher.shiftLeft(8)).compareTo(RandomPartitioner.MAXIMUM) <= 0)
                    add(results, mutate.withToken(new BigIntegerToken(higher)));
            }
            else throw new UnsupportedOperationException();
        }
        TableId tableId = mutate.table();
        if (tableId.msb() != Long.MAX_VALUE)
            add(results, mutate.withTable(TableId.fromRaw(tableId.msb() + 1, tableId.lsb())));
        if (tableId.lsb() != Long.MAX_VALUE)
            add(results, mutate.withTable(TableId.fromRaw(tableId.msb(), tableId.lsb() + 1)));
        return results;
    }

    private List<TokenKey> mutateBefore(TokenKey mutate)
    {
        List<TokenKey> results = new ArrayList<>();
        if (!mutate.isTableSentinel())
        {
            Token token = mutate.token();
            if (token instanceof ByteOrderedPartitioner.BytesToken)
            {
                byte[] bytes = (byte[]) token.getTokenValue();
                bytes = bytes.clone();
                for (int i = 0 ; i < bytes.length ; ++i)
                {
                    add(results, mutate.withToken(new ByteOrderedPartitioner.BytesToken(Arrays.copyOf(bytes, i))));
                    if ((bytes[i] & 0xff) != 0)
                    {
                        --bytes[i];
                        add(results, mutate.withToken(new ByteOrderedPartitioner.BytesToken(bytes.clone())));
                        ++bytes[i];
                    }
                }
            }
            else if (token instanceof LongToken)
            {
                long value = token.getLongValue();
                if (value > Long.MIN_VALUE)
                    add(results, mutate.withToken(new LongToken(value - 1)));
                for (long v = 2L; v >= 0 ; v <<= 1)
                {
                    if ((value & v) != 0)
                        add(results, mutate.withToken(new LongToken(value & ~v)));
                }
                if (value >= 0)
                {
                    for (int i = 1 ; i < 8 ; ++i)
                        add(results, mutate.withToken(new LongToken(value >>> (i * 8))));
                }
                else
                {
                    for (int i = 0 ; i < 7 ; ++i)
                    {
                        long next = value & (-1L << (i * 8));
                        if (next != value)
                            add(results, mutate.withToken(new LongToken(next)));
                    }
                }
            }
            else if (token instanceof BigIntegerToken)
            {
                BigInteger value = (BigInteger) token.getTokenValue();
                if (value.compareTo(RandomPartitioner.MINIMUM.getTokenValue()) > 0)
                    add(results, mutate.withToken(new BigIntegerToken(value.subtract(BigInteger.ONE))));
                for (long v = 1L; v >= 0 ; v <<= 1)
                {
                    BigInteger i = BigInteger.valueOf(v);
                    if (!value.and(i).equals(BigInteger.ZERO))
                        add(results, mutate.withToken(new BigIntegerToken(value.andNot(i))));
                }
                for (int i = 1 ; i < 8 ; ++i)
                    add(results, mutate.withToken(new BigIntegerToken(value.shiftRight(i * 16))));
            }
            else throw new UnsupportedOperationException();
        }
        TableId tableId = mutate.table();
        if (tableId.msb() != Long.MIN_VALUE)
            add(results, mutate.withTable(TableId.fromRaw(tableId.msb() - 1, tableId.lsb())));
        if (tableId.lsb() != Long.MIN_VALUE)
            add(results, mutate.withTable(TableId.fromRaw(tableId.msb(), tableId.lsb() -1)));
        return results;
    }

    private static void add(List<TokenKey> to, TokenKey vary)
    {
        to.add(vary);
        if (!vary.isTokenSentinel())
        {
            to.add(vary.before());
            to.add(vary.after());
        }
    }

    private static Gen<TokenKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<Token> tokenGen, IPartitioner partitioner)
    {
        Gen<TokenKey> result = AccordGenerators.routingKeyGen(tableIdGen, Gens.enums().all(AccordGenerators.RoutingKeyKind.class), tokenGen, partitioner);
        if (!(partitioner instanceof ByteOrderedPartitioner))
            return result;
        return result.map((rs, k) -> {
            byte[] bytes = (byte[]) k.token().getTokenValue();
            if (bytes.length >= 3)
            {
                while (rs.nextFloat() < 0.25f)
                {
                    int i = rs.nextInt(bytes.length - 2);
                    bytes[i] = 0;
                    bytes[i + 1] = (byte) rs.nextInt(0, TokenKey.Serializer.ESCAPE_BYTE);
                }
            }

            return k;
        });
    }

}