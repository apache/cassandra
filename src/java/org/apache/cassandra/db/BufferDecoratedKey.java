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

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class BufferDecoratedKey extends DecoratedKey
{
    private final ByteBuffer key;

    public BufferDecoratedKey(Token token, ByteBuffer key)
    {
        super(token);
        assert key != null;
        this.key = key;
    }

    public ByteBuffer getKey()
    {
        return key;
    }

    @Override
    public int getKeyLength()
    {
        return key.remaining();
    }

    /**
     * A factory method that translates the given byte-comparable representation to a {@link BufferDecoratedKey}
     * instance. If the given byte comparable doesn't represent the encoding of a buffer decorated key, anything from a
     * wide variety of throwables may be thrown (e.g. {@link AssertionError}, {@link IndexOutOfBoundsException},
     * {@link IllegalStateException}, etc.).
     *
     * @param byteComparable A byte-comparable representation (presumably of a {@link BufferDecoratedKey} instance).
     * @param version The encoding version used for the given byte comparable.
     * @param partitioner The partitioner of the encoded decorated key. Needed in order to correctly decode the token
     *                    bytes of the key.
     * @return A new {@link BufferDecoratedKey} instance, corresponding to the given byte-comparable representation. If
     * we were to call {@link #asComparableBytes(Version)} on the returned object, we should get a {@link ByteSource}
     * equal to the one of the input byte comparable.
     */
    public static BufferDecoratedKey fromByteComparable(ByteComparable byteComparable,
                                                        Version version,
                                                        IPartitioner partitioner)
    {
        return DecoratedKey.fromByteComparable(byteComparable,
                                               version,
                                               partitioner,
                                               (token, keyBytes) -> new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes)));
    }
}
