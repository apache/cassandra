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
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import accord.messages.SimpleReply;
import accord.utils.Invariants;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SmallEnumSerializer<E extends Enum<E>> implements IVersionedSerializer<E>
{
    public static final SmallEnumSerializer<SimpleReply> simpleReply = new SmallEnumSerializer<>(SimpleReply.class);

    // TODO: should use something other than ordinal for ser/deser
    final E[] values;

    public SmallEnumSerializer(Class<E> clazz)
    {
        this.values = clazz.getEnumConstants();
        Invariants.checkArgument(values.length < 255); // allow an extra 1 for nullable variant to ensure consistency
    }

    public E forOrdinal(int ordinal)
    {
        return values[ordinal];
    }

    @Override
    public void serialize(E t, DataOutputPlus out, int version) throws IOException
    {
        out.write(t.ordinal());
    }

    @Override
    public E deserialize(DataInputPlus in, int version) throws IOException
    {
        return values[in.readByte()];
    }

    public ByteBuffer serialize(E e)
    {
        ByteBuffer out = ByteBuffer.allocate(1);
        out.put((byte)e.ordinal());
        out.flip();
        return out;
    }

    @Override
    public long serializedSize(E t, int version)
    {
        return 1;
    }

    public static class NullableSmallEnumSerializer<E extends Enum<E>> implements IVersionedSerializer<E>
    {
        // TODO: should use something other than ordinal for ser/deser
        final E[] values;

        public NullableSmallEnumSerializer(SmallEnumSerializer<E> wrap)
        {
            this.values = wrap.values;
        }

        public E forOrdinal(int ordinal)
        {
            return values[ordinal];
        }

        @Override
        public void serialize(@Nullable E t, DataOutputPlus out, int version) throws IOException
        {
            out.write(t == null ? 0 : 1 + t.ordinal());
        }

        @Override
        public E deserialize(DataInputPlus in, int version) throws IOException
        {
            int ordinal = in.readByte();
            return ordinal == 0 ? null : values[ordinal - 1];
        }

        public ByteBuffer serialize(E e)
        {
            ByteBuffer out = ByteBuffer.allocate(1);
            out.put((byte)(e == null ? 0 : (1 + e.ordinal())));
            out.flip();
            return out;
        }

        @Override
        public long serializedSize(E t, int version)
        {
            return 1;
        }
    }
}
