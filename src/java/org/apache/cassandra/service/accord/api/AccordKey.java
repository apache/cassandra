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
import java.util.Objects;

import accord.api.Key;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface AccordKey extends Key<AccordKey>
{
    TableId tableId();
    PartitionPosition partitionKey();

    static int compare(AccordKey left, AccordKey right)
    {
        int cmp = left.tableId().compareTo(right.tableId());
        return cmp == 0 ? left.partitionKey().compareTo(right.partitionKey()) : cmp;
    }

    default int compareTo(AccordKey that)
    {
        return compare(this, that);
    }

    static abstract class AbstractKey<T extends PartitionPosition> implements AccordKey
    {
        private final TableId tableId;
        private final T key;

        public AbstractKey(TableId tableId, T key)
        {
            this.tableId = tableId;
            this.key = key;
        }

        @Override
        public TableId tableId()
        {
            return tableId;
        }

        @Override
        public PartitionPosition partitionKey()
        {
            return key;
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
    }

    public static class PartitionKey extends AbstractKey<DecoratedKey>
    {
        public PartitionKey(TableId tableId, DecoratedKey key)
        {
            super(tableId, key);
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return (DecoratedKey) super.partitionKey();
        }

        @Override
        public String toString()
        {
            return "PartitionKey{" +
                   "tableId=" + tableId() +
                   ", key=" + partitionKey() +
                   '}';
        }

        public static final IVersionedSerializer<PartitionKey> serializer = new IVersionedSerializer<PartitionKey>()
        {
            @Override
            public void serialize(PartitionKey key, DataOutputPlus out, int version) throws IOException
            {
                key.tableId().serialize(out);
                ByteBufferUtil.writeWithVIntLength(key.partitionKey().getKey(), out);
            }

            @Override
            public PartitionKey deserialize(DataInputPlus in, int version) throws IOException
            {
                TableId tableId = TableId.deserialize(in);
                TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
                DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
                return new PartitionKey(tableId, key);
            }

            @Override
            public long serializedSize(PartitionKey key, int version)
            {
                return key.tableId().serializedSize() + ByteBufferUtil.serializedSizeWithVIntLength(key.partitionKey().getKey());
            }
        };
    }

    public static class TokenKey extends AbstractKey<Token.KeyBound>
    {
        public TokenKey(TableId tableId, Token.KeyBound key)
        {
            super(tableId, key);
        }

        @Override
        public Token.KeyBound partitionKey()
        {
            return (Token.KeyBound) super.partitionKey();
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
}
