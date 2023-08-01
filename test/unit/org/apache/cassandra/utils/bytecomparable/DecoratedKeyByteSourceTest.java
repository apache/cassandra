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

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

@RunWith(Parameterized.class)
public class DecoratedKeyByteSourceTest
{
    private static final int NUM_ITERATIONS = 100;
    private static final int RANDOM_BYTES_LENGTH = 100;

    @Parameterized.Parameters(name = "version={0}")
    public static Iterable<ByteComparable.Version> versions()
    {
        return ImmutableList.of(ByteComparable.Version.OSS50);
    }

    private final ByteComparable.Version version;

    public DecoratedKeyByteSourceTest(ByteComparable.Version version)
    {
        this.version = version;
    }

    @Test
    public void testDecodeBufferDecoratedKey()
    {
        for (int i = 0; i < NUM_ITERATIONS; ++i)
        {
            BufferDecoratedKey initialBuffer =
                    (BufferDecoratedKey) ByteOrderedPartitioner.instance.decorateKey(newRandomBytesBuffer());
            BufferDecoratedKey decodedBuffer = BufferDecoratedKey.fromByteComparable(
                    initialBuffer, version, ByteOrderedPartitioner.instance);
            Assert.assertEquals(initialBuffer, decodedBuffer);
        }
    }

    @Test
    public void testDecodeKeyBytes()
    {
        for (int i = 0; i < NUM_ITERATIONS; ++i)
        {
            BufferDecoratedKey initialBuffer =
                    (BufferDecoratedKey) ByteOrderedPartitioner.instance.decorateKey(newRandomBytesBuffer());
            ByteSource.Peekable src = ByteSource.peekable(initialBuffer.asComparableBytes(version));
            byte[] keyBytes = DecoratedKey.keyFromByteSource(src, version, ByteOrderedPartitioner.instance);
            Assert.assertEquals(ByteSource.END_OF_STREAM, src.next());
            Assert.assertArrayEquals(initialBuffer.getKey().array(), keyBytes);
        }
    }

    private static ByteBuffer newRandomBytesBuffer()
    {
        byte[] randomBytes = new byte[RANDOM_BYTES_LENGTH];
        new Random().nextBytes(randomBytes);
        return ByteBuffer.wrap(randomBytes);
    }
}
