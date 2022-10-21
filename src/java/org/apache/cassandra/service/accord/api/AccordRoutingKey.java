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
import accord.api.RoutingKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class AccordRoutingKey extends AccordRoutableKey implements RoutingKey
{
    enum RoutingKeyKind
    {
        TOKEN, SENTINEL;
    }

    protected AccordRoutingKey(TableId tableId)
    {
        super(tableId);
    }

    public abstract RoutingKeyKind kindOfRoutingKey();
    public abstract long estimatedSizeOnHeap();

    public static AccordRoutingKey of(Key key)
    {
        return (AccordRoutingKey) key;
    }

    public static class SentinelKey extends AccordRoutingKey
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SentinelKey(null, true));

        private final boolean isMin;

        private SentinelKey(TableId tableId, boolean isMin)
        {
            super(tableId);
            this.isMin = isMin;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, isMin);
        }

        @Override
        public RoutingKeyKind kindOfRoutingKey()
        {
            return RoutingKeyKind.SENTINEL;
        }

        @Override
        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE;
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
        public Token token()
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

    public static class TokenKey extends AccordRoutingKey
    {
        private static final long EMPTY_SIZE;

        static
        {
            Token key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER).getToken();
            EMPTY_SIZE = ObjectSizes.measureDeep(new TokenKey(null, key));
        }

        final Token token;
        public TokenKey(TableId tableId, Token token)
        {
            super(tableId);
            this.token = token;
        }

        @Override
        public Token token()
        {
            return token;
        }

        @Override
        public RoutingKeyKind kindOfRoutingKey()
        {
            return RoutingKeyKind.TOKEN;
        }

        @Override
        public String toString()
        {
            return "TokenKey{" +
                   "tableId=" + tableId() +
                   ", key=" + token() +
                   '}';
        }

        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + token().getHeapSize();
        }

        public static final Serializer serializer = new Serializer();
        public static class Serializer implements IVersionedSerializer<TokenKey>
        {
            private Serializer() {}

            @Override
            public void serialize(TokenKey key, DataOutputPlus out, int version) throws IOException
            {
                key.tableId().serialize(out);
                Token.compactSerializer.serialize(key.token, out, version);
            }

            @Override
            public TokenKey deserialize(DataInputPlus in, int version) throws IOException
            {
                TableId tableId = TableId.deserialize(in);
                TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
                Token token = Token.compactSerializer.deserialize(in, metadata.partitioner, version);
                return new TokenKey(tableId, token);
            }

            @Override
            public long serializedSize(TokenKey key, int version)
            {
                return TableId.serializedSize() + Token.compactSerializer.serializedSize(key.token(), version);
            }
        }
    }

    public static final IVersionedSerializer<AccordRoutingKey> serializer = new IVersionedSerializer<AccordRoutingKey>()
    {
        final RoutingKeyKind[] kinds = RoutingKeyKind.values();
        @Override
        public void serialize(AccordRoutingKey key, DataOutputPlus out, int version) throws IOException
        {
            out.write(key.kindOfRoutingKey().ordinal());
            switch (key.kindOfRoutingKey())
            {
                case TOKEN:
                    TokenKey.serializer.serialize((TokenKey) key, out, version);
                    break;
                case SENTINEL:
                    SentinelKey.serializer.serialize((SentinelKey) key, out, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public AccordRoutingKey deserialize(DataInputPlus in, int version) throws IOException
        {
            RoutingKeyKind kind = kinds[in.readByte()];
            switch (kind)
            {
                case TOKEN:
                    return TokenKey.serializer.deserialize(in, version);
                case SENTINEL:
                    return SentinelKey.serializer.deserialize(in, version);
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public long serializedSize(AccordRoutingKey key, int version)
        {
            long size = TypeSizes.BYTE_SIZE; // kind ordinal
            switch (key.kindOfRoutingKey())
            {
                case TOKEN:
                    size += TokenKey.serializer.serializedSize((TokenKey) key, version);
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
