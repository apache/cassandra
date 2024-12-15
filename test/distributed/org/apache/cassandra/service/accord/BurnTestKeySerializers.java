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
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.AccordRoutableKey;
import org.apache.cassandra.service.accord.api.AccordRoutableKey.AccordKeySerializer;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.utils.CastingSerializer;

@SuppressWarnings("unchecked")
public class BurnTestKeySerializers
{
    private BurnTestKeySerializers() {}

    public static final AccordRoutableKey.AccordKeySerializer<Key> key =
    (AccordRoutableKey.AccordKeySerializer<Key>)
    (AccordRoutableKey.AccordKeySerializer<?>)
    new AccordRoutableKey.AccordKeySerializer<PrefixedIntHashKey>()
    {
        public void serialize(PrefixedIntHashKey t, DataOutputPlus out, int version) throws IOException
        {
            assert t instanceof PrefixedIntHashKey.Key;
            out.writeInt(t.prefix);
            out.writeInt(t.key);
            out.writeInt(t.hash);
        }

        public PrefixedIntHashKey deserialize(DataInputPlus in, int version) throws IOException
        {
            int prefix = in.readInt();
            int key = in.readInt();
            int hash = in.readInt();
            return PrefixedIntHashKey.key(prefix, key, hash);
        }

        public long serializedSize(PrefixedIntHashKey t, int version)
        {
            return 3 * Integer.BYTES;
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            in.skipBytesFully(3 * Integer.BYTES);
        }
    };

    public static final IVersionedSerializer<RoutingKey> routingKey =
    (AccordKeySerializer<RoutingKey>)
    (AccordKeySerializer<?>)
    new AccordRoutableKey.AccordKeySerializer<PrefixedIntHashKey.Hash>()
    {
        public void serialize(PrefixedIntHashKey.Hash t, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(t.prefix);
            out.writeInt(t.hash);
        }

        public PrefixedIntHashKey.Hash deserialize(DataInputPlus in, int version) throws IOException
        {
            int prefix = in.readInt();
            int hash = in.readInt();
            return new PrefixedIntHashKey.Hash(prefix, hash);
        }

        public long serializedSize(PrefixedIntHashKey.Hash t, int version)
        {
            return 2 * Integer.BYTES;
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            in.skipBytesFully(2 * Integer.BYTES);
        }
    };

    public static final IVersionedSerializer<Range> range =
    (IVersionedSerializer<Range>)
    (IVersionedSerializer<?>)
    new IVersionedSerializer<PrefixedIntHashKey.Range>()
    {
        @Override
        public void serialize(PrefixedIntHashKey.Range t, DataOutputPlus out, int version) throws IOException
        {
            routingKey.serialize(t.start(), out, version);
            routingKey.serialize(t.end(), out, version);
        }

        @Override
        public PrefixedIntHashKey.Range deserialize(DataInputPlus in, int version) throws IOException
        {
            RoutingKey start = routingKey.deserialize(in, version);
            RoutingKey end = routingKey.deserialize(in, version);
            return PrefixedIntHashKey.range((PrefixedIntHashKey.PrefixedIntRoutingKey) start, (PrefixedIntHashKey.PrefixedIntRoutingKey) end);
        }

        @Override
        public long serializedSize(PrefixedIntHashKey.Range t, int version)
        {
            throw new RuntimeException("not implemented");
        }
    };

    public static final IVersionedSerializer<Read> read = new CastingSerializer<>(ListRead.class, new IVersionedSerializer<>()
    {
        public void serialize(ListRead t, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(t.isEphemeralRead);
            KeySerializers.seekables.serialize(t.userReadKeys, out, version);
            KeySerializers.seekables.serialize(t.keys, out, version);
        }

        public ListRead deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isEphemeralRead = in.readBoolean();
            Seekables<?, ?> userReadKeys = KeySerializers.seekables.deserialize(in, version);
            Seekables<?, ?> keys = KeySerializers.seekables.deserialize(in, version);
            return new ListRead(Function.identity(), isEphemeralRead, userReadKeys, keys);
        }

        public long serializedSize(ListRead t, int version)
        {
            throw new RuntimeException("not implemented");
        }
    });

    public static final IVersionedSerializer<Query> query = new CastingSerializer<>(ListQuery.class, new IVersionedSerializer<>()
    {
        public void serialize(ListQuery t, DataOutputPlus out, int version) throws IOException
        {
            if (t == null)
            {
                out.writeByte(0);
                return;
            }
            out.writeByte(1);
            TopologySerializers.NodeIdSerializer.serialize(t.client, out);
            out.writeLong(t.requestId);
            out.writeBoolean(t.isEphemeralRead);
        }

        public ListQuery deserialize(DataInputPlus in, int version) throws IOException
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

            Node.Id client = TopologySerializers.NodeIdSerializer.deserialize(in);
            long requestId = in.readLong();
            boolean isEphemeralRead = in.readBoolean();
            return new ListQuery(client, requestId, isEphemeralRead);
        }

        public long serializedSize(ListQuery t, int version)
        {
            throw new RuntimeException("not implemented");
        }
    });

    public static final IVersionedSerializer<Update> update = new CastingSerializer<>(ListUpdate.class, new IVersionedSerializer<>()
    {
        public void serialize(ListUpdate t, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(t.size());
            for (Map.Entry<Key, Integer> e : t.entrySet())
            {
                KeySerializers.key.serialize(e.getKey(), out, version);
                out.writeInt(e.getValue());
            }
        }

        public ListUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            ListUpdate listUpdate = new ListUpdate(Function.identity());
            for (int i = 0; i < size; i++)
            {
                Key k = KeySerializers.key.deserialize(in, version);
                int v = in.readInt();
                listUpdate.put(k, v);
            }
            return listUpdate;
        }

        public long serializedSize(ListUpdate t, int version)
        {
            throw new RuntimeException("not implemented");
        }
    });

    public static final IVersionedSerializer<Write> write = new CastingSerializer<>(ListWrite.class, new IVersionedSerializer<>()
    {
        public void serialize(ListWrite t, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(t.size());
            for (Map.Entry<Key, int[]> e : t.entrySet())
            {
                KeySerializers.key.serialize(e.getKey(), out, version);
                out.writeInt(e.getValue().length);
                for (int v : e.getValue())
                    out.writeInt(v);
            }
        }

        public ListWrite deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            ListWrite write = new ListWrite(Function.identity());
            for (int i = 0; i < size; i++)
            {
                Key k = KeySerializers.key.deserialize(in, version);
                int len = in.readInt();
                int[] vals = new int[len];
                for (int j = 0; j < len; j++)
                    vals[j] = in.readInt();
                write.put(k, vals);
            }
            return write;
        }

        public long serializedSize(ListWrite t, int version)
        {
            throw new RuntimeException("not implemented");
        }
    });

    public static final IVersionedSerializer<Result> result = new CastingSerializer<>(ListResult.class, new IVersionedSerializer<>()
    {
        public void serialize(ListResult t, DataOutputPlus out, int version) throws IOException
        {
            TopologySerializers.NodeIdSerializer.serialize(t.client, out);
            out.writeLong(t.requestId);
            CommandSerializers.txnId.serialize(t.txnId, out, version);

            KeySerializers.seekables.serialize(t.readKeys, out, version);
            KeySerializers.keys.serialize(t.responseKeys, out, version);

            out.writeInt(t.read.length);
            for (int[] ints : t.read)
            {
                out.writeInt(ints.length);
                for (int i : ints)
                    out.writeInt(i);
            }

            out.writeInt(t.update == null ? 0 : 1);
            if (t.update != null)
                update.serialize(t.update, out, version);

            out.writeInt(t.status.ordinal());
        }

        public ListResult deserialize(DataInputPlus in, int version) throws IOException
        {
            Node.Id client = TopologySerializers.NodeIdSerializer.deserialize(in);
            long requestId = in.readLong();
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Seekables<?, ?> readKeys = KeySerializers.seekables.deserialize(in, version);
            Keys responseKeys = KeySerializers.keys.deserialize(in, version);
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
            ListUpdate update = null;
            if (in.readInt() != 0)
                update = (ListUpdate) BurnTestKeySerializers.update.deserialize(in, version);
            ListResult.Status status = ListResult.Status.values()[in.readInt()];
            return new ListResult(status, client, requestId, txnId, readKeys, responseKeys, read, update);
        }

        public long serializedSize(ListResult t, int version)
        {
            throw new RuntimeException("not implemented");
        }
    });
}