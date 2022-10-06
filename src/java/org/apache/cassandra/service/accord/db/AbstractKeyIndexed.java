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

package org.apache.cassandra.service.accord.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Contains a map of objects serialized to byte buffers
 * @param <T>
 */
public abstract class AbstractKeyIndexed<T>
{
    final NavigableMap<PartitionKey, ByteBuffer> serialized;

    abstract void serialize(T t, DataOutputPlus out, int version) throws IOException;
    abstract T deserialize(DataInputPlus in, int version) throws IOException;
    abstract long serializedSize(T t, int version);
    abstract long emptySizeOnHeap();

    protected ByteBuffer serialize(T item)
    {
        int version = MessagingService.current_version;
        long size = serializedSize(item, version) + TypeSizes.INT_SIZE;
        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeInt(version);
            serialize(item, out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected T deserialize(ByteBuffer bytes)
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            int version = in.readInt();
            return deserialize(in, version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKeyIndexed<?> that = (AbstractKeyIndexed<?>) o;
        return serialized.equals(that.serialized);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(serialized);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + serialized.entrySet().stream()
                         .map(e -> e.getKey() + "=" + deserialize(e.getValue()))
                         .collect(Collectors.joining(", ", "{", "}"));
    }

    public AbstractKeyIndexed(List<T> items, Function<T, PartitionKey> keyFunction)
    {
        this.serialized = new TreeMap<>();
        for (int i=0, mi=items.size(); i<mi; i++)
        {
            T item = items.get(i);
            PartitionKey key = keyFunction.apply(item);
            // TODO: support multiple reads/writes per key
            Preconditions.checkArgument(!this.serialized.containsKey(key));
            this.serialized.put(key, serialize(item));
        }
    }

    public AbstractKeyIndexed(NavigableMap<PartitionKey, ByteBuffer> serialized)
    {
        this.serialized = serialized;
    }

    public T getDeserialized(PartitionKey key)
    {
        ByteBuffer bytes = serialized.get(key);
        if (bytes == null)
            return null;
        return deserialize(bytes);
    }

    public long estimatedSizeOnHeap()
    {
        long size = emptySizeOnHeap();
        for (Map.Entry<PartitionKey, ByteBuffer> entry : serialized.entrySet())
        {
            size += entry.getKey().estimatedSizeOnHeap();
            size += ByteBufferUtil.EMPTY_SIZE_ON_HEAP + ByteBufferAccessor.instance.size(entry.getValue());
        }
        return size;
    }

    static class Serializer<V, S extends AbstractKeyIndexed<V>> implements IVersionedSerializer<S>
    {
        private final Function<NavigableMap<PartitionKey, ByteBuffer>, S> factory;

        Serializer(Function<NavigableMap<PartitionKey, ByteBuffer>, S> factory)
        {
            this.factory = factory;
        }

        @Override
        public void serialize(S items, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(items.serialized.size());
            for (Map.Entry<PartitionKey, ByteBuffer> entry : items.serialized.entrySet())
            {
                PartitionKey.serializer.serialize(entry.getKey(), out, version);
                ByteBufferUtil.writeWithVIntLength(entry.getValue(), out);
            }
        }

        @Override
        public S deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = (int) in.readUnsignedVInt();
            NavigableMap<PartitionKey, ByteBuffer> items = new TreeMap<>();
            for (int i=0; i<size; i++)
                items.put(PartitionKey.serializer.deserialize(in, version), ByteBufferUtil.readWithVIntLength(in));
            return factory.apply(items);
        }

        @Override
        public long serializedSize(S items, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(items.serialized.size());
            for (Map.Entry<PartitionKey, ByteBuffer> entry : items.serialized.entrySet())
            {
                size += PartitionKey.serializer.serializedSize(entry.getKey());
                size += ByteBufferUtil.serializedSizeWithVIntLength(entry.getValue());
            }
            return size;
        }
    }
}
