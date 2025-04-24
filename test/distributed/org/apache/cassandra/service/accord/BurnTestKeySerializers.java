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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import accord.api.Key;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Update;
import accord.api.Write;
import accord.impl.PrefixedIntHashKey;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListResult;
import accord.impl.list.ListUpdate;
import accord.impl.list.ListWrite;
import accord.local.Node;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.AccordRoutableKey;
import org.apache.cassandra.service.accord.api.AccordRoutableKey.AccordSearchableKeySerializer;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.CastingSerializer;

@SuppressWarnings("unchecked")
public class BurnTestKeySerializers
{
    private BurnTestKeySerializers() {}

    public static final AccordRoutableKey.AccordKeySerializer<Key> key =
    (AccordRoutableKey.AccordKeySerializer<Key>)
    (AccordSearchableKeySerializer<?>)
    new AccordSearchableKeySerializer<PrefixedIntHashKey>()
    {
        @Override
        public void serialize(PrefixedIntHashKey t, DataOutputPlus out) throws IOException
        {
            assert t instanceof PrefixedIntHashKey.Key;
            out.writeInt(t.prefix);
            out.writeInt(t.key);
            out.writeInt(t.hash);
        }

        @Override
        public PrefixedIntHashKey deserialize(DataInputPlus in) throws IOException
        {
            int prefix = in.readInt();
            int key = in.readInt();
            int hash = in.readInt();
            return PrefixedIntHashKey.key(prefix, key, hash);
        }

        @Override
        public long serializedSize(PrefixedIntHashKey t)
        {
            return 3 * Integer.BYTES;
        }

        @Override
        public void skip(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(3 * Integer.BYTES);
        }

        @Override
        public int fixedKeyLengthForPrefix(Object prefix)
        {
            return 8;
        }

        @Override
        public int serializedSizeOfPrefix(Object prefix)
        {
            return 4;
        }

        @Override
        public int serializedSizeWithoutPrefix(PrefixedIntHashKey key)
        {
            return 8;
        }

        @Override
        public void serializePrefix(Object prefix, DataOutputPlus out) throws IOException
        {
            out.writeInt((Integer) prefix);
        }

        @Override
        public void serializeWithoutPrefixOrLength(PrefixedIntHashKey key, DataOutputPlus out) throws IOException
        {
            out.writeInt(key.hash);
            out.writeInt(key.key);
        }

        @Override
        public Object deserializePrefix(DataInputPlus in) throws IOException
        {
            return in.readInt();
        }

        @Override
        public PrefixedIntHashKey deserializeWithPrefix(Object prefix, int length, DataInputPlus in) throws IOException
        {
            int key = in.readInt();
            int hash = in.readInt();
            return PrefixedIntHashKey.key((Integer)prefix, key, hash);
        }
    };

    public static final AccordSearchableKeySerializer<RoutingKey> routingKey =
    (AccordSearchableKeySerializer<RoutingKey>)
    (AccordSearchableKeySerializer<?>)
    new AccordSearchableKeySerializer<PrefixedIntHashKey.Hash>()
    {
        public void serialize(PrefixedIntHashKey.Hash t, DataOutputPlus out) throws IOException
        {
            out.writeInt(t.prefix);
            out.writeInt(t.hash);
        }

        public PrefixedIntHashKey.Hash deserialize(DataInputPlus in) throws IOException
        {
            int prefix = in.readInt();
            int hash = in.readInt();
            return new PrefixedIntHashKey.Hash(prefix, hash);
        }

        public long serializedSize(PrefixedIntHashKey.Hash t)
        {
            return 2 * Integer.BYTES;
        }

        public void skip(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(2 * Integer.BYTES);
        }

        @Override
        public int fixedKeyLengthForPrefix(Object prefix)
        {
            return 4;
        }

        @Override
        public int serializedSizeOfPrefix(Object prefix)
        {
            return 4;
        }

        @Override
        public int serializedSizeWithoutPrefix(PrefixedIntHashKey.Hash key)
        {
            return 4;
        }

        @Override
        public void serializePrefix(Object prefix, DataOutputPlus out) throws IOException
        {
            out.writeInt((Integer) prefix);
        }

        @Override
        public void serializeWithoutPrefixOrLength(PrefixedIntHashKey.Hash key, DataOutputPlus out) throws IOException
        {
            out.writeInt(key.hash);
        }

        @Override
        public Object deserializePrefix(DataInputPlus in) throws IOException
        {
            return in.readInt();
        }

        @Override
        public PrefixedIntHashKey.Hash deserializeWithPrefix(Object prefix, int length, DataInputPlus in) throws IOException
        {
            int hash = in.readInt();
            return PrefixedIntHashKey.forHash((Integer)prefix, hash);
        }
    };

    public static final UnversionedSerializer<Range> range =
    (UnversionedSerializer<Range>)
    (UnversionedSerializer<?>)
    new UnversionedSerializer<PrefixedIntHashKey.Range>()
    {
        @Override
        public void serialize(PrefixedIntHashKey.Range t, DataOutputPlus out) throws IOException
        {
            routingKey.serialize(t.start(), out);
            routingKey.serialize(t.end(), out);
        }

        @Override
        public PrefixedIntHashKey.Range deserialize(DataInputPlus in) throws IOException
        {
            RoutingKey start = routingKey.deserialize(in);
            RoutingKey end = routingKey.deserialize(in);
            return PrefixedIntHashKey.range((PrefixedIntHashKey.PrefixedIntRoutingKey) start, (PrefixedIntHashKey.PrefixedIntRoutingKey) end);
        }

        @Override
        public long serializedSize(PrefixedIntHashKey.Range t)
        {
            throw new RuntimeException("not implemented");
        }
    };

    public static final ParameterisedVersionedSerializer<Read, TableMetadatasAndKeys, Version> read = (ParameterisedVersionedSerializer) new ParameterisedVersionedSerializer<ListRead, TableMetadatasAndKeys, Version>()
    {
        @Override
        public void serialize(ListRead t, TableMetadatasAndKeys seekables, DataOutputPlus out, Version version) throws IOException
        {
            out.writeBoolean(t.isEphemeralRead);
            KeySerializers.seekables.serialize(t.userReadKeys, out);
            KeySerializers.seekables.serialize(t.keys, out);
        }

        @Override
        public ListRead deserialize(TableMetadatasAndKeys seekables, DataInputPlus in, Version version) throws IOException
        {
            boolean isEphemeralRead = in.readBoolean();
            Seekables<?, ?> userReadKeys = KeySerializers.seekables.deserialize(in);
            Seekables<?, ?> keys = KeySerializers.seekables.deserialize(in);
            return new ListRead(Function.identity(), isEphemeralRead, userReadKeys, keys);
        }

        @Override
        public long serializedSize(ListRead t, TableMetadatasAndKeys seekables, Version version)
        {
            throw new RuntimeException("not implemented");
        }
    };

    public static final UnversionedSerializer<Query> query = CastingSerializer.create(ListQuery.class, new UnversionedSerializer<>()
    {
        public void serialize(ListQuery t, DataOutputPlus out) throws IOException
        {
            if (t == null)
            {
                out.writeByte(0);
                return;
            }
            out.writeByte(1);
            TopologySerializers.nodeId.serialize(t.client, out);
            out.writeLong(t.requestId);
            out.writeBoolean(t.isEphemeralRead);
        }

        public ListQuery deserialize(DataInputPlus in) throws IOException
        {
            switch (in.readByte())
            {
                case 0:
                    return null;
                case 1:
                    break;
                default:
                    throw new AssertionError();
            }

            Node.Id client = TopologySerializers.nodeId.deserialize(in);
            long requestId = in.readLong();
            boolean isEphemeralRead = in.readBoolean();
            return new ListQuery(client, requestId, isEphemeralRead);
        }

        public long serializedSize(ListQuery t)
        {
            throw new RuntimeException("not implemented");
        }
    });

    public static final ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> update = (ParameterisedVersionedSerializer) new ParameterisedVersionedSerializer<ListUpdate, TableMetadatasAndKeys, Version>()
    {
        public void serialize(ListUpdate t, TableMetadatasAndKeys seekables, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.size());
            for (Map.Entry<Key, Integer> e : t.entrySet())
            {
                KeySerializers.key.serialize(e.getKey(), out);
                out.writeInt(e.getValue());
            }
        }

        public ListUpdate deserialize(TableMetadatasAndKeys seekables, DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            ListUpdate listUpdate = new ListUpdate(Function.identity());
            for (int i = 0; i < size; i++)
            {
                Key k = KeySerializers.key.deserialize(in);
                int v = in.readInt();
                listUpdate.put(k, v);
            }
            return listUpdate;
        }

        public long serializedSize(ListUpdate t, TableMetadatasAndKeys seekables, Version version)
        {
            throw new RuntimeException("not implemented");
        }
    };

    public static final ParameterisedVersionedSerializer<Write, Seekables, Version> write = (ParameterisedVersionedSerializer) new ParameterisedVersionedSerializer<ListWrite, Seekables, Version>()
    {
        public void serialize(ListWrite t, Seekables seekables, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.size());
            for (Map.Entry<Key, int[]> e : t.entrySet())
            {
                KeySerializers.key.serialize(e.getKey(), out);
                out.writeInt(e.getValue().length);
                for (int v : e.getValue())
                    out.writeInt(v);
            }
        }

        public ListWrite deserialize(Seekables seekables, DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            ListWrite write = new ListWrite(Function.identity());
            for (int i = 0; i < size; i++)
            {
                Key k = KeySerializers.key.deserialize(in);
                int len = in.readInt();
                int[] vals = new int[len];
                for (int j = 0; j < len; j++)
                    vals[j] = in.readInt();
                write.put(k, vals);
            }
            return write;
        }

        public long serializedSize(ListWrite t, Seekables seekables, Version version)
        {
            throw new RuntimeException("not implemented");
        }
    };

    public static final UnversionedSerializer<TableMetadatasAndKeys> tablesAndKeys = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(TableMetadatasAndKeys t, DataOutputPlus out) throws IOException
        {
        }

        @Override
        public TableMetadatasAndKeys deserialize(DataInputPlus in) throws IOException
        {
            return null;
        }

        @Override
        public long serializedSize(TableMetadatasAndKeys t)
        {
            return 0;
        }
    };

    public static final UnversionedSerializer<Result> result = CastingSerializer.create(ListResult.class, new UnversionedSerializer<>()
    {
        public void serialize(ListResult t, DataOutputPlus out) throws IOException
        {
            TopologySerializers.nodeId.serialize(t.client, out);
            out.writeLong(t.requestId);
            CommandSerializers.txnId.serialize(t.txnId, out);

            KeySerializers.seekables.serialize(t.readKeys, out);
            KeySerializers.keys.serialize(t.responseKeys, out);

            out.writeInt(t.read.length);
            for (int[] ints : t.read)
            {
                out.writeInt(ints.length);
                for (int i : ints)
                    out.writeInt(i);
            }

            out.writeInt(t.update == null ? 0 : 1);
            if (t.update != null)
                update.serialize(t.update, null, out, Version.LATEST);

            out.writeInt(t.status.ordinal());
        }

        public ListResult deserialize(DataInputPlus in) throws IOException
        {
            Node.Id client = TopologySerializers.nodeId.deserialize(in);
            long requestId = in.readLong();
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Seekables<?, ?> readKeys = KeySerializers.seekables.deserialize(in);
            Keys responseKeys = KeySerializers.keys.deserialize(in);
            int[][] read = new int[in.readInt()][];
            for (int i = 0; i < read.length; i++)
            {
                int[] v = new int[in.readInt()];
                for (int j = 0; j < v.length; j++)
                {
                    v[j] = in.readInt();
                }
                read[i] = v;
            }
            ListUpdate upd = null;
            if (in.readInt() != 0)
                upd = (ListUpdate) update.deserialize(null, in, Version.LATEST);
            ListResult.Status status = ListResult.Status.values()[in.readInt()];
            return new ListResult(status, client, requestId, txnId, readKeys, responseKeys, read, upd);
        }

        public long serializedSize(ListResult t)
        {
            throw new RuntimeException("not implemented");
        }
    });
}