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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordObjectSizes;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Contains a map of objects serialized to byte buffers
 * @param <T>
 */
public abstract class AbstractKeyIndexed<T>
{
    final Keys keys;
    final ByteBuffer[] serialized;

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

    protected static <T> ByteBuffer serialize(T item, IVersionedSerializer<T> serializer)
    {
        int version = MessagingService.current_version;
        long size = serializer.serializedSize(item, version) + TypeSizes.INT_SIZE;
        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeInt(version);
            serializer.serialize(item, out, version);
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
        return Arrays.equals(serialized, that.serialized);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(serialized);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + IntStream.range(0, keys.size())
                                                     .mapToObj(i -> keys.get(i) + "=" + deserialize(serialized[i]))
                                                     .collect(Collectors.joining(", ", "{", "}"));
    }

    protected AbstractKeyIndexed(Keys keys, ByteBuffer[] serialized)
    {
        this.keys = keys;
        this.serialized = serialized;
    }

    public AbstractKeyIndexed(List<T> items, Function<T, PartitionKey> keyFunction)
    {
        Key[] keys = new Key[items.size()];
        for (int i=0, mi=items.size(); i<mi; i++)
            keys[i] = keyFunction.apply(items.get(i));
        this.keys = Keys.of(keys);
        this.serialized = new ByteBuffer[items.size()];
        for (int i=0, mi=items.size(); i<mi; i++)
            serialized[this.keys.indexOf(keyFunction.apply(items.get(i)))] = serialize(items.get(i));
    }

    protected <V> V slice(KeyRanges ranges, BiFunction<Keys, ByteBuffer[], V> constructor)
    {
        // TODO: Routables patch permits us to do this more efficiently
        Keys keys = this.keys.slice(ranges);
        ByteBuffer[] serialized = new ByteBuffer[keys.size()];
        int j = 0;
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            j = this.keys.findNext(keys.get(i), j);
            serialized[i] = this.serialized[j++];
        }
        return constructor.apply(keys, serialized);
    }

    public <V> V merge(AbstractKeyIndexed<?> that, BiFunction<Keys, ByteBuffer[], V> constructor)
    {
        // TODO: special method for linear merging keyed and non-keyed lists simultaneously
        Keys keys = this.keys.union(that.keys);
        ByteBuffer[] serialized = new ByteBuffer[keys.size()];
        int i = 0, j = 0, o = 0;
        while (i < this.keys.size() && j < that.keys.size())
        {
            int c = this.keys.get(i).compareTo(that.keys.get(j));
            if (c < 0) serialized[o++] = this.serialized[i++];
            else if (c > 0) serialized[o++] = that.serialized[j++];
            else { serialized[o++] = this.serialized[i++]; j++; }
        }
        while (i < this.keys.size())
            serialized[o++] = this.serialized[i++];
        while (j < that.keys.size())
            serialized[o++] = that.serialized[j++];
        return constructor.apply(keys, serialized);
    }

    public T getDeserialized(PartitionKey key)
    {
        int i = keys.indexOf(key);
        if (i < 0) return null;
        return deserialize(serialized[i]);
    }

    public long estimatedSizeOnHeap()
    {
        long size = emptySizeOnHeap() + AccordObjectSizes.keys(keys);
        for (ByteBuffer buffer : serialized) size += ByteBufferUtil.estimatedSizeOnHeap(buffer);
        return size;
    }

    static class Serializer<V, S extends AbstractKeyIndexed<V>> implements IVersionedSerializer<S>
    {
        private final BiFunction<Keys, ByteBuffer[], S> factory;

        Serializer(BiFunction<Keys, ByteBuffer[], S> factory)
        {
            this.factory = factory;
        }

        @Override
        public void serialize(S items, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(items.serialized.length);
            for (Key key : items.keys) PartitionKey.serializer.serialize((PartitionKey) key, out, version);
            for (ByteBuffer buffer : items.serialized) ByteBufferUtil.writeWithVIntLength(buffer, out);
        }

        @Override
        public S deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = (int) in.readUnsignedVInt();
            Key[] keys = new Key[size];
            for (int i=0; i<size; i++)
                keys[i] = PartitionKey.serializer.deserialize(in, version);

            ByteBuffer[] serialized = new ByteBuffer[size];
            for (int i=0; i<size; i++)
                serialized[i] = ByteBufferUtil.readWithVIntLength(in);
            return factory.apply(Keys.ofSorted(keys), serialized);
        }

        @Override
        public long serializedSize(S items, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(items.serialized.length);
            for (Key key : items.keys) size += PartitionKey.serializer.serializedSize((PartitionKey) key, version);
            for (ByteBuffer buffer : items.serialized) size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);
            return size;
        }
    }
}
