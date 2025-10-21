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
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

import javax.annotation.Nullable;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

public abstract class EncodeAsVInt32<T> implements UnversionedSerializer<T>
{
    /**
     * Creates a serializer that uses vint to store the encoded value.
     *
     * Negative ints cause undefined behavior and are unsafe to use; this logic is only safe for 0 and posotive values
     */
    public static <T> EncodeAsVInt32<T> withNulls(ToIntFunction<? super T> encode, IntFunction<? extends T> decode)
    {
        return new WithNulls<>(encode, decode);
    }

    public static <T> EncodeAsVInt32<T> withoutNulls(ToIntFunction<? super T> encode, IntFunction<? extends T> decode)
    {
        return new WithoutNulls<>(encode, decode);
    }

    public static <E extends Enum<?>> EncodeAsVInt32<E> of(Class<E> clazz)
    {
        E[] values = clazz.getEnumConstants();
        return withNulls(Enum::ordinal, i -> values[i]);
    }

    static class WithNulls<T> extends EncodeAsVInt32<T>
    {
        private WithNulls(ToIntFunction<? super T> encode, IntFunction<? extends T> decode)
        {
            super(encode, decode);
        }

        @Override
        int encode(@Nullable T t)
        {
            return t == null ? 0 : (1 + encode.applyAsInt(t));
        }

        @Override
        T decode(long i)
        {
            return i == 0 ? null : decode.apply(Ints.checkedCast(i - 1));
        }
    }

    static class WithoutNulls<T> extends EncodeAsVInt32<T>
    {
        private WithoutNulls(ToIntFunction<? super T> encode, IntFunction<? extends T> decode)
        {
            super(encode, decode);
        }

        @Override
        int encode(@Nullable T t)
        {
            return encode.applyAsInt(t);
        }

        @Override
        T decode(long i)
        {
            return decode.apply(Ints.checkedCast(i));
        }
    }

    final ToIntFunction<? super T> encode;
    final IntFunction<? extends T> decode;

    abstract int encode(T t);
    abstract T decode(long i);

    private EncodeAsVInt32(ToIntFunction<? super T> encode, IntFunction<? extends T> decode)
    {
        this.encode = encode;
        this.decode = decode;
    }

    @Override
    public void serialize(T t, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(encode(t));
    }

    @Override
    public T deserialize(DataInputPlus in) throws IOException
    {
        // we read a long to ensure we are correct even if the underlying conversion may return -1
        return decode(in.readUnsignedVInt());
    }

    @Override
    public long serializedSize(T t)
    {
        return VIntCoding.computeUnsignedVIntSize(encode(t));
    }
}
