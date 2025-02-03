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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
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
        // AccordRoutingKey$TokenKey reaches into DD to get partitioner, so need to set that up...
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void serde()
    {
        qt().forAll(fromQT(partitioners().assuming(IPartitioner::accordSupported)).flatMap(partitioner -> routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(token(partitioner)), partitioner)))
            .check(key -> {
                IPartitioner partitioner = key.token().getPartitioner();
                {
                    ByteBuffer buffer = serializer.serialize(key);
                    TokenKey roundTrip = serializer.deserialize(buffer, partitioner);
                    TokenKey roundTrip2 = serializer.deserializeAndConsume(buffer, partitioner);
                    Assertions.assertThat(roundTrip).isEqualTo(key);
                    Assertions.assertThat(roundTrip2).isEqualTo(key);
                }
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
                try (DataOutputBuffer buf = new DataOutputBuffer())
                {
                    serializer.serialize(key, buf, 0);
                    byte[] bytes = buf.toByteArray();
                    Assertions.assertThat(bytes.length).isEqualTo(serializer.serializedSize(key, 0));
                    try (DataInputBuffer in = new DataInputBuffer(bytes))
                    {
                        TokenKey roundTrip = serializer.deserialize(in, 0, partitioner);
                        Assertions.assertThat(roundTrip).isEqualTo(key);
                        Invariants.require(0 == in.available());
                    }
                    try (DataInputBuffer in = new DataInputBuffer(bytes))
                    {
                        serializer.skip(in, 0, partitioner);
                        Invariants.require(0 == in.available());
                    }
                }
            });
    }

    @Test
    public void compare()
    {
        qt().forAll(fromQT(partitioners().assuming(IPartitioner::accordSupported)).flatMap(partitioner -> routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(token(partitioner)), partitioner)))
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
        to.add(vary.before());
        to.add(vary.after());
    }

    private static Gen<TokenKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<Token> tokenGen, IPartitioner partitioner)
    {
        Gen<TokenKey> result = AccordGenerators.routingKeyGen(tableIdGen, Gens.enums().all(TokenKey.RoutingKeyKind.class), tokenGen, partitioner);
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