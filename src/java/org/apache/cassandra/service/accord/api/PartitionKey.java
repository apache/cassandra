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

import accord.api.Key;
import accord.primitives.Range;
import accord.utils.Invariants;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

// final in part because we refer to its class directly in AccordRoutableKey.compareTo
public final class PartitionKey extends AccordRoutableKey implements Key
{
    private static final long EMPTY_SIZE;

    static
    {
        DecoratedKey key = getPartitioner().decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER);
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

    public static PartitionKey of(PartitionUpdate update)
    {
        return new PartitionKey(update.metadata().id, update.partitionKey());
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
    public org.apache.cassandra.service.accord.api.TokenKey toUnseekable()
    {
        return new org.apache.cassandra.service.accord.api.TokenKey(table, token());
    }

    @Override
    public Range asRange()
    {
        return TokenRange.create(TokenKey.before(table, key.getToken()), new TokenKey(table, key.getToken()));
    }

    @Override
    byte sentinel()
    {
        return org.apache.cassandra.service.accord.api.TokenKey.NORMAL_SENTINEL;
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + ByteBufferAccessor.instance.size(partitionKey().getKey());
    }

    @Override
    public String suffix()
    {
        return partitionKey().toString();
    }

    public static final Serializer serializer = new Serializer();
    public static class Serializer implements AccordKeySerializer<PartitionKey>
    {
        private Serializer() {}

        @Override
        public void serialize(PartitionKey key, DataOutputPlus out) throws IOException
        {
            key.table().serializeCompact(out);
            ByteBufferUtil.writeWithVIntLength(key.partitionKey().getKey(), out);
        }

        public <V> int serialize(PartitionKey key, V dst, ValueAccessor<V> accessor, int offset)
        {
            int position = offset;
            position += key.table().serializeCompact(dst, accessor, position);
            ByteBuffer bytes = key.partitionKey().getKey();
            Invariants.require(key.partitionKey().getPartitioner() == getPartitioner());
            int numBytes = bytes.remaining();
            position += accessor.putUnsignedVInt32(dst, position, numBytes);
            position += accessor.copyByteBufferTo(bytes, 0, dst, position, numBytes);
            return position - offset;

        }

        @Override
        public void skip(DataInputPlus in) throws IOException
        {
            TableId.skipCompact(in);
            ByteBufferUtil.skipWithVIntLength(in);
        }

        @Override
        public PartitionKey deserialize(DataInputPlus in) throws IOException
        {
            TableId tableId = TableId.deserializeCompact(in);
            DecoratedKey key = getPartitioner().decorateKey(ByteBufferUtil.readWithVIntLength(in));
            return new PartitionKey(tableId, key);
        }

        public <V> PartitionKey deserialize(V src, ValueAccessor<V> accessor, int offset) throws IOException
        {
            TableId tableId = TableId.deserializeCompact(src, accessor, offset);
            offset += tableId.serializedCompactSize();
            int numBytes = accessor.getUnsignedVInt32(src, offset);
            offset += VIntCoding.readLengthOfVInt(src, accessor, offset);
            ByteBuffer bytes = ByteBuffer.allocate(numBytes);
            accessor.copyTo(src, offset, bytes, ByteBufferAccessor.instance, 0, numBytes);
            DecoratedKey key = getPartitioner().decorateKey(bytes);
            return new PartitionKey(tableId, key);
        }

        @Override
        public long serializedSize(PartitionKey key)
        {
            return key.table().serializedCompactSize() + ByteBufferUtil.serializedSizeWithVIntLength(key.partitionKey().getKey());
        }
    }
}
