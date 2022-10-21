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

package org.apache.cassandra.service.accord.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.Routable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class PartitionKey extends AccordRoutableKey implements Key
{
    private static final long EMPTY_SIZE;

    static
    {
        DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        EMPTY_SIZE = ObjectSizes.measureDeep(new PartitionKey(null, key));
    }

    final DecoratedKey key;

    public PartitionKey(TableId tableId, DecoratedKey key)
    {
        super(tableId);
        this.key = key;
    }

    public static PartitionKey of(Key key)
    {
        return (PartitionKey) key;
    }

    public static PartitionKey of(Partition partition)
    {
        return new PartitionKey(partition.metadata().id, partition.partitionKey());
    }

    public static PartitionKey of(SinglePartitionReadCommand command)
    {
        return new PartitionKey(command.metadata().id, command.partitionKey());
    }

    @Override
    public Token token()
    {
        return partitionKey().getToken();
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    @Override
    public RoutingKey toUnseekable()
    {
        return new TokenKey(tableId(), token());
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + ByteBufferAccessor.instance.size(partitionKey().getKey());
    }

    @Override
    public String toString()
    {
        return "PartitionKey{" +
               "tableId=" + tableId() +
               ", key=" + partitionKey() +
               '}';
    }

    // TODO: callers to this method are not correctly handling ranges
    public static PartitionKey toPartitionKey(Routable routable)
    {
        return (PartitionKey) routable;
    }

    public static final Serializer serializer = new Serializer();
    public static class Serializer implements IVersionedSerializer<PartitionKey>
    {
        // TODO: add vint to value accessor and use vints
        private Serializer() {}

        @Override
        public void serialize(PartitionKey key, DataOutputPlus out, int version) throws IOException
        {
            key.tableId().serialize(out);
            ByteBufferUtil.writeWithShortLength(key.partitionKey().getKey(), out);
        }

        public <V> int serialize(PartitionKey key, V dst, ValueAccessor<V> accessor, int offset)
        {
            int position = offset;
            position += key.tableId().serialize(dst, accessor, position);
            ByteBuffer bytes = key.partitionKey().getKey();
            int numBytes = ByteBufferAccessor.instance.size(bytes);
            Preconditions.checkState(numBytes <= Short.MAX_VALUE);
            position += accessor.putShort(dst, position, (short) numBytes);
            position += accessor.copyByteBufferTo(bytes, 0, dst, position, numBytes);
            return position - offset;

        }

        @Override
        public PartitionKey deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(tableId);
            DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
            return new PartitionKey(tableId, key);
        }

        public <V> PartitionKey deserialize(V src, ValueAccessor<V> accessor, int offset) throws IOException
        {
            TableId tableId = TableId.deserialize(src, accessor, offset);
            offset += TableId.serializedSize();
            TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
            int numBytes = accessor.getShort(src, offset);
            offset += TypeSizes.SHORT_SIZE;
            ByteBuffer bytes = ByteBuffer.allocate(numBytes);
            accessor.copyTo(src, offset, bytes, ByteBufferAccessor.instance, 0, numBytes);
            DecoratedKey key = metadata.partitioner.decorateKey(bytes);
            return new PartitionKey(tableId, key);
        }

        @Override
        public long serializedSize(PartitionKey key, int version)
        {
            return serializedSize(key);
        }

        public long serializedSize(PartitionKey key)
        {
            return key.tableId().serializedSize() + ByteBufferUtil.serializedSizeWithShortLength(key.partitionKey().getKey());
        }
    }
}
