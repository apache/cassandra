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

package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.cql3.functions.types.utils.Bytes;
import org.apache.cassandra.harry.checker.TestHelper;

public class RandomPartitionerTest extends PartitionerTestCase
{
    public void initPartitioner()
    {
        partitioner = RandomPartitioner.instance;
    }

    protected boolean shouldStopRecursion(Token left, Token right)
    {
        return left.size(right) < Math.scalb(1, -112);
    }

    @Test
    public void testSplit()
    {
        assertSplit(tok("a"), tok("b"), 16);
        assertSplit(tok("a"), tok("bbb"), 16);
    }

    @Test
    public void testSplitWrapping()
    {
        assertSplit(tok("b"), tok("a"), 16);
        assertSplit(tok("bbb"), tok("a"), 16);
    }

    @Test
    public void testSplitExceedMaximumCase()
    {
        RandomPartitioner.BigIntegerToken left = new RandomPartitioner.BigIntegerToken(RandomPartitioner.MAXIMUM.subtract(BigInteger.valueOf(10)));

        assertSplit(left, tok("a"), 16);
    }

    @Test
    public void testIncrement()
    {
        TestHelper.withRandom((rng) -> {
            for (int i = 0; i < 10_000; i++)
            {
                BigInteger bi = BigInteger.valueOf(Math.abs(rng.next()));
                byte[] bytes = bi.toByteArray();
                byte[] copy = Arrays.copyOf(bytes, bytes.length);

                BigInteger incremented = new BigInteger(RandomPartitioner.increment(bytes));
                BigInteger expected = bi.add(BigInteger.valueOf(1));
                if (!expected.equals(incremented))
                {
                    throw new IllegalArgumentException(String.format("\nBefore increment: %s" +
                                                                     "\n After increment: %s," +
                                                                     "\n%s != %s",
                                                                     Bytes.toHexString(copy),
                                                                     Bytes.toHexString(bytes),
                                                                     expected,
                                                                     incremented));
                }
            }
        });
    }

    @Test
    public void testDecrement()
    {
        TestHelper.withRandom((rng) -> {
            for (int i = 0; i < 10_000; i++)
            {
                BigInteger bi = BigInteger.valueOf(Math.abs(rng.next() + 1));
                byte[] bytes = bi.toByteArray();
                byte[] copy = Arrays.copyOf(bytes, bytes.length);

                RandomPartitioner.decrement(bytes);
                BigInteger incremented = new BigInteger(bytes);
                BigInteger expected = bi.add(BigInteger.valueOf(-1));
                if (!expected.equals(incremented))
                {
                    throw new IllegalArgumentException(String.format("\nBefore increment: %s" +
                                                                     "\n After increment: %s," +
                                                                     "\n%s != %s",
                                                                     Bytes.toHexString(copy),
                                                                     Bytes.toHexString(bytes),
                                                                     expected,
                                                                     incremented));
                }
            }
        });
    }
}
