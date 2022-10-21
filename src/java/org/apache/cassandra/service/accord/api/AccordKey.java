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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public interface AccordKey extends Key
{
    enum Kind
    {
        TOKEN, PARTITION, SENTINEL;

        public boolean supportsRanges()
        {
            switch (this)
            {
                case SENTINEL:
                case TOKEN:
                    return true;
                default:
                    return false;
            }
        }
    };

    TableId tableId();
    PartitionPosition partitionKey();
    Kind kind();

    static AccordKey of(Key key)
    {
        return (AccordKey) key;
    }

    static PartitionKey of(Partition partition)
    {
        return new PartitionKey(partition.metadata().id, partition.partitionKey());
    }

    static PartitionKey of(SinglePartitionReadCommand command)
    {
        return new PartitionKey(command.metadata().id, command.partitionKey());
    }

    static int compare(AccordKey left, AccordKey right)
    {
        int cmp = left.tableId().compareTo(right.tableId());
        if (cmp != 0)
            return cmp;

        if (left instanceof SentinelKey || right instanceof SentinelKey)
        {
            int leftInt = left instanceof SentinelKey ? ((SentinelKey) left).asInt() : 0;
            int rightInt = right instanceof SentinelKey ? ((SentinelKey) right).asInt() : 0;
            return Integer.compare(leftInt, rightInt);
        }

        return left.partitionKey().compareTo(right.partitionKey());
    }

    static int compareKeys(Key left, Key right)
    {
        return compare((AccordKey) left, (AccordKey) right);
    }

    default int compareTo(AccordKey that)
    {
        return compare(this, that);
    }

    @Override
    default int routingHash()
    {
        return partitionKey().getToken().tokenHash();
    }

    class SentinelKey implements AccordKey
    {
        private final TableId tableId;
        private final boolean isMin;

        private SentinelKey(TableId tableId, boolean isMin)
        {
            this.tableId = tableId;
            this.isMin = isMin;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SentinelKey that = (SentinelKey) o;
            return isMin == that.isMin && tableId.equals(that.tableId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, isMin);
        }

        @Override
        public int compareTo(Key that)
        {
            return compare(this, (AccordKey) that);
        }

        @Override
        public Kind kind()
        {
            return Kind.SENTINEL;
        }

        public static SentinelKey min(TableId tableId)
        {
            return new SentinelKey(tableId, true);
        }

        public static SentinelKey max(TableId tableId)
        {
            return new SentinelKey(tableId, false);
        }

        @Override
        public TableId tableId()
        {
            return tableId;
        }

        @Override
        public PartitionPosition partitionKey()
        {
            throw new UnsupportedOperationException();
        }

        int asInt()
        {
            return isMin ? -1 : 1;
        }

        @Override
        public String toString()
        {
            return "SentinelKey{" +
                   "tableId=" + tableId +
                   ", key=" + (isMin ? "min": "max") +
                   '}';
        }

        public static final IVersionedSerializer<SentinelKey> serializer = new IVersionedSerializer<SentinelKey>()
        {
            @Override
            public void serialize(SentinelKey key, DataOutputPlus out, int version) throws IOException
            {
                out.writeBoolean(key.isMin);
                key.tableId().serialize(out);
            }

            @Override
            public SentinelKey deserialize(DataInputPlus in, int version) throws IOException
            {
                boolean isMin = in.readBoolean();
                TableId tableId = TableId.deserialize(in);
                return new SentinelKey(tableId, isMin);
            }

            @Override
            public long serializedSize(SentinelKey key, int version)
            {
                return TypeSizes.BOOL_SIZE + TableId.serializedSize();
            }
        };
    }

    abstract class AbstractKey<T extends PartitionPosition> implements AccordKey
    {
        private final TableId tableId;
        private final T key;

        public AbstractKey(TableId tableId, T key)
        {
            this.tableId = tableId;
            this.key = key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AbstractKey<?> that = (AbstractKey<?>) o;
            return tableId.equals(that.tableId) && key.equals(that.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, key);
        }

        @Override
        public int compareTo(Key that)
        {
            return compare(this, (AccordKey) that);
        }

        @Override
        public TableId tableId()
        {
            return tableId;
        }

        @Override
        public T partitionKey()
        {
            return key;
        }
    }

    class PartitionKey extends AbstractKey<DecoratedKey>
    {
        private static final long EMPTY_SIZE;
        static
        {
            DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            EMPTY_SIZE = ObjectSizes.measureDeep(new PartitionKey(null, key));
        }

        public PartitionKey(TableId tableId, DecoratedKey key)
        {
            super(tableId, key);
        }

        @Override
        public Kind kind()
        {
            return Kind.PARTITION;
        }

        @Override
        public String toString()
        {
            return "PartitionKey{" +
                   "tableId=" + tableId() +
                   ", key=" + partitionKey() +
                   '}';
        }

        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + ByteBufferAccessor.instance.size(partitionKey().getKey());
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
                TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
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

    class TokenKey extends AbstractKey<Token.KeyBound>
    {
        public TokenKey(TableId tableId, Token.KeyBound key)
        {
            super(tableId, key);
        }

        public static TokenKey min(TableId tableId, Token token)
        {
            return new TokenKey(tableId, token.minKeyBound());
        }

        public static TokenKey max(TableId tableId, Token token)
        {
            return new TokenKey(tableId, token.maxKeyBound());
        }

        @Override
        public Kind kind()
        {
            return Kind.TOKEN;
        }

        @Override
        public String toString()
        {
            return "TokenKey{" +
                   "tableId=" + tableId() +
                   ", key=" + partitionKey() +
                   '}';
        }

        public static final IVersionedSerializer<TokenKey> serializer = new IVersionedSerializer<TokenKey>()
        {
            @Override
            public void serialize(TokenKey key, DataOutputPlus out, int version) throws IOException
            {
                key.tableId().serialize(out);
                Token.KeyBound bound = key.partitionKey();
                out.writeBoolean(bound.isMinimumBound);
                Token.serializer.serialize(bound.getToken(), out, version);
            }

            @Override
            public TokenKey deserialize(DataInputPlus in, int version) throws IOException
            {
                TableId tableId = TableId.deserialize(in);
                TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
                boolean isMinimumBound = in.readBoolean();
                Token token = Token.serializer.deserialize(in, metadata.partitioner, version);
                return new TokenKey(tableId, isMinimumBound ? token.minKeyBound() : token.maxKeyBound());
            }

            @Override
            public long serializedSize(TokenKey key, int version)
            {
                Token.KeyBound bound = key.partitionKey();
                return key.tableId().serializedSize()
                     + TypeSizes.sizeof(bound.isMinimumBound)
                     + Token.serializer.serializedSize(bound.getToken(), version);
            }
        };
    }

    IVersionedSerializer<AccordKey> serializer = new IVersionedSerializer<AccordKey>()
    {
        @Override
        public void serialize(AccordKey key, DataOutputPlus out, int version) throws IOException
        {
            out.write(key.kind().ordinal());
            switch (key.kind())
            {
                case TOKEN:
                    TokenKey.serializer.serialize((TokenKey) key, out, version);
                    break;
                case PARTITION:
                    PartitionKey.serializer.serialize((PartitionKey) key, out, version);
                    break;
                case SENTINEL:
                    SentinelKey.serializer.serialize((SentinelKey) key, out, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }

        }

        @Override
        public AccordKey deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            switch (kind)
            {
                case TOKEN:
                    return TokenKey.serializer.deserialize(in, version);
                case PARTITION:
                    return PartitionKey.serializer.deserialize(in, version);
                case SENTINEL:
                    return SentinelKey.serializer.deserialize(in, version);
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public long serializedSize(AccordKey key, int version)
        {
            long size = TypeSizes.BYTE_SIZE; // kind ordinal
            switch (key.kind())
            {
                case TOKEN:
                    size += TokenKey.serializer.serializedSize((TokenKey) key, version);
                    break;
                case PARTITION:
                    size += PartitionKey.serializer.serializedSize((PartitionKey) key, version);
                    break;
                case SENTINEL:
                    size += SentinelKey.serializer.serializedSize((SentinelKey) key, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            return size;
        }
    };
}
