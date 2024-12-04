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

package org.apache.cassandra.service.accord.txn;

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import accord.utils.Invariants;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Item that is serialized by default
 */
@NotThreadSafe
public abstract class AbstractSerialized<T>
{
    private static final long EMPTY = ObjectSizes.measure(new AbstractSerialized<ByteBuffer>(null, null) {
        @Override
        protected IVersionedSerializer<ByteBuffer> serializer()
        {
            throw new AssertionError();
        }
    });
    public final Version version;
    private @Nullable final ByteBuffer bytes;
    private transient @Nullable T memoized = null;

    public AbstractSerialized(@Nullable ByteBuffer bytes, Version version)
    {
        this.version = version;
        this.bytes = bytes;
    }

    public AbstractSerialized(T value)
    {
        this.version = Version.LATEST;
        this.bytes = serializer().serializeUnchecked(Invariants.nonNull(value), version);
        this.memoized = value;
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY + ByteBufferUtil.estimatedSizeOnHeap(bytes);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || !(o instanceof AbstractSerialized)) return false;

        AbstractSerialized<?> that = (AbstractSerialized<?>) o;

        return Objects.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode()
    {
        return bytes != null ? bytes.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return Objects.toString(get());
    }

    protected abstract IVersionedSerializer<T> serializer();

    protected boolean isNull()
    {
        return bytes == null;
    }

    @Nullable
    protected T get()
    {
        T result = memoized;
        if (result == null && bytes != null)
            memoized = result = serializer().deserializeUnchecked(bytes, version);
        return result;
    }

    public void unmemoize()
    {
        memoized = null;
    }

    @Nullable
    protected ByteBuffer unsafeBytes()
    {
        return bytes;
    }

    @Nonnull
    protected ByteBuffer bytes(Version target)
    {
        Invariants.nonNull(bytes);
        if (version == target)
            return bytes;
        return serializer().serializeUnchecked(get(), target);
    }

    public static <T> AbstractSerialized<T> of(IVersionedSerializer<T> serializer, T value)
    {
        return new AbstractSerialized<T>(value)
        {
            @Override
            protected IVersionedSerializer<T> serializer()
            {
                return serializer;
            }
        };
    }

    public static <T> AbstractSerialized<T> fromBytes(IVersionedSerializer<T> serializer, ByteBuffer bytes, Version version)
    {
        return new AbstractSerialized<T>(bytes, version)
        {
            @Override
            protected IVersionedSerializer<T> serializer()
            {
                return serializer;
            }
        };
    }
}
