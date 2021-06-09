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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeDecoratedKey extends DecoratedKey
{
    final long peer;

    public NativeDecoratedKey(Token token, NativeAllocator allocator, OpOrder.Group writeOp, ByteBuffer key)
    {
        super(token);
        assert key != null;
        assert key.order() == ByteOrder.BIG_ENDIAN;

        int size = key.remaining();
        this.peer = allocator.allocate(4 + size, writeOp);
        MemoryUtil.setInt(peer, size);
        MemoryUtil.setBytes(peer + 4, key);
    }

    public NativeDecoratedKey(Token token, NativeAllocator allocator, OpOrder.Group writeOp, byte[] keyBytes)
    {
        super(token);
        assert keyBytes != null;

        int size = keyBytes.length;
        this.peer = allocator.allocate(4 + size, writeOp);
        MemoryUtil.setInt(peer, size);
        MemoryUtil.setBytes(peer + 4, keyBytes, 0, size);
    }

    @Inline
    int length()
    {
        return MemoryUtil.getInt(peer);
    }

    @Inline
    long address()
    {
        return this.peer + 4;
    }

    @Override
    public ByteBuffer getKey()
    {
        return MemoryUtil.getByteBuffer(peer + 4, MemoryUtil.getInt(peer), ByteOrder.BIG_ENDIAN);
    }

    @Override
    public int getKeyLength()
    {
        return MemoryUtil.getInt(peer);
    }

    @Override
    protected ByteSource keyComparableBytes(Version version)
    {
        return ByteSource.of(address(), length(), version);
    }

    /**
     * A factory method that translates the given byte-comparable representation to a {@link NativeDecoratedKey}
     * instance. If the given byte comparable doesn't represent the encoding of a native decorated key, anything from a
     * wide variety of throwables may be thrown (e.g. {@link AssertionError}, {@link IndexOutOfBoundsException},
     * {@link IllegalStateException}, etc.).
     *
     * @param byteComparable A byte-comparable representation (presumably of a {@link NativeDecoratedKey} instance).
     * @param version The encoding version used for the given byte comparable.
     * @param partitioner The partitioner of the encoded decorated key. Needed in order to correctly decode the token
     *                    bytes of the key.
     * @param allocator The native allocator needed to copy the key contents to off-heap memory.
     *
     * @return A new {@link NativeDecoratedKey} instance, corresponding to the given byte-comparable representation. If
     * we were to call {@link #asComparableBytes(Version)} on the returned object, we should get a {@link ByteSource}
     * equal to the one of the input byte comparable.
     */
    public static NativeDecoratedKey fromByteComparable(ByteComparable byteComparable,
                                                        ByteComparable.Version version,
                                                        IPartitioner partitioner,
                                                        NativeAllocator allocator,
                                                        OpOrder.Group opGroup)
    {
        return DecoratedKey.fromByteComparable(byteComparable,
                                               version,
                                               partitioner,
                                               (token, keyBytes) -> new NativeDecoratedKey(token, allocator, opGroup, keyBytes));
    }
}
