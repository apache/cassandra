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
package org.apache.cassandra.db;

import java.util.Arrays;
import java.util.Random;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.Constraint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class ColumnFamilyStoreMBeanTest
{
    @BeforeClass
    public static void setup()
    {
        // can't use client due to the fact thread pools startup and fail due to config issues
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testToTokenRangesMurmur3Partitioner()
    {
        testToTokenRanges(Murmur3Partitioner.instance);
    }

    @Test
    public void testToTokenRangesRandomPartitioner()
    {
        testToTokenRanges(RandomPartitioner.instance);
    }

    @Test
    public void testToTokenRangesOrderPreservingPartitioner()
    {
        testToTokenRanges(OrderPreservingPartitioner.instance);
    }

    @Test
    public void testToTokenRangesByteOrderedPartitioner()
    {
        testToTokenRanges(ByteOrderedPartitioner.instance);
    }

    @Test
    public void testInvalidateTokenRangesFormat()
    {
        ColumnFamilyStore store = Mockito.mock(ColumnFamilyStore.class);
        Mockito.doCallRealMethod().when(store).forceCompactionForTokenRanges(Mockito.any());
        IPartitioner previous = DatabaseDescriptor.getPartitioner();
        try
        {
            DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);

            for (String s : Arrays.asList("testing", "t1:", ":t2", "spaces should not have an impact"))
                Assertions.assertThatThrownBy(() -> store.forceCompactionForTokenRanges(s))
                          .hasMessageStartingWith(String.format("Unable to parse token range %s;", s));
        }
        finally
        {
            DatabaseDescriptor.setPartitionerUnsafe(previous);
        }

    }

    private static void testToTokenRanges(IPartitioner partitioner)
    {
        Token.TokenFactory tokenFactory = partitioner.getTokenFactory();
        Gen<Token> tokenGen = tokenGen(partitioner);
        qt().forAll(tokenGen, tokenGen)
            .checkAssert((left, right) ->
                         assertThat(ColumnFamilyStore.toTokenRanges(partitioner, toString(tokenFactory, left, right)))
                         .isEqualTo(ImmutableSet.of(new Range<>(left, right))));
    }

    private static String toString(Token.TokenFactory tokenFactory, Token left, Token right)
    {
        return tokenFactory.toString(left) + ColumnFamilyStore.TOKEN_DELIMITER + tokenFactory.toString(right);
    }

    private static Gen<Token> tokenGen(IPartitioner partitioner)
    {
        // Random and RandomSource can not share the same seed, but there is a workaround...
        // use RandomSource to generate a seed!
        return rs -> partitioner.getRandomToken(new Random(rs.next(Constraint.none())));
    }
}
