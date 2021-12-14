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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.Map;

import accord.local.Node;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordQuery;
import org.apache.cassandra.service.accord.db.AccordRead;
import org.apache.cassandra.service.accord.db.AccordUpdate;
import org.apache.cassandra.service.accord.db.AccordWrite;

public class CommandSerializers
{
    private CommandSerializers() {}

    public static final IVersionedSerializer<TxnId> txnId = new TimestampSerializer<>(TxnId::new);
    public static final IVersionedSerializer<Timestamp> timestamp = new TimestampSerializer<>(Timestamp::new);
    public static final IVersionedSerializer<Ballot> ballot = new TimestampSerializer<>(Ballot::new);

    private static class TimestampSerializer<T extends Timestamp> implements IVersionedSerializer<T>
    {
        interface Factory<T extends Timestamp>
        {
            T create(long epoch, long real, int logical, Node.Id node);
        }

        private final TimestampSerializer.Factory<T> factory;

        TimestampSerializer(TimestampSerializer.Factory<T> factory)
        {
            this.factory = factory;
        }

        @Override
        public void serialize(T ts, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(ts.epoch);
            out.writeLong(ts.real);
            out.writeInt(ts.logical);
            TopologySerializers.nodeId.serialize(ts.node, out, version);
        }

        @Override
        public T deserialize(DataInputPlus in, int version) throws IOException
        {
            return factory.create(in.readLong(),
                                  in.readLong(),
                                  in.readInt(),
                                  TopologySerializers.nodeId.deserialize(in, version));
        }

        @Override
        public long serializedSize(T ts, int version)
        {
            return TypeSizes.sizeof(ts.epoch)
                   + TypeSizes.sizeof(ts.real)
                   + TypeSizes.sizeof(ts.logical)
                   + TopologySerializers.nodeId.serializedSize(ts.node, version);
        }
    }

    public static final IVersionedSerializer<Txn> txn = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Txn txn, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.keys.serialize(txn.keys, out, version);
            AccordRead.serializer.serialize((AccordRead) txn.read, out, version);
            AccordQuery.serializer.serialize((AccordQuery) txn.query, out, version);
            out.writeBoolean(txn.update != null);
            if (txn.update != null)
                AccordUpdate.serializer.serialize((AccordUpdate) txn.update, out, version);

        }

        @Override
        public Txn deserialize(DataInputPlus in, int version) throws IOException
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);
            AccordRead read = AccordRead.serializer.deserialize(in, version);
            AccordQuery query = AccordQuery.serializer.deserialize(in, version);
            if (in.readBoolean())
                return new Txn(keys, read, query, AccordUpdate.serializer.deserialize(in, version));
            else
                return new Txn(keys, read, query);
        }

        @Override
        public long serializedSize(Txn txn, int version)
        {
            long size = KeySerializers.keys.serializedSize(txn.keys, version);
            size += AccordRead.serializer.serializedSize((AccordRead) txn.read, version);
            size += AccordQuery.serializer.serializedSize((AccordQuery) txn.query, version);
            size += TypeSizes.sizeof(txn.update != null);
            if (txn.update != null)
                size += AccordUpdate.serializer.serializedSize((AccordUpdate) txn.update, version);
            return size;
        }
    };

    public static final IVersionedSerializer<Status> status = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Status status, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(status.ordinal());

        }

        @Override
        public Status deserialize(DataInputPlus in, int version) throws IOException
        {
            return Status.values()[in.readInt()];
        }

        @Override
        public long serializedSize(Status status, int version)
        {
            return TypeSizes.sizeof(status.ordinal());
        }
    };

    public static final IVersionedSerializer<Dependencies> deps = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Dependencies deps, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(deps.size());
            for (Map.Entry<TxnId, Txn> entry : deps)
            {
                txnId.serialize(entry.getKey(), out, version);
                txn.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public Dependencies deserialize(DataInputPlus in, int version) throws IOException
        {
            Dependencies deps = new Dependencies();
            int size = in.readInt();
            for (int i=0; i<size; i++)
                deps.add(txnId.deserialize(in, version), txn.deserialize(in, version));
            return deps;
        }

        @Override
        public long serializedSize(Dependencies deps, int version)
        {
            long size = TypeSizes.sizeof(deps.size());
            for (Map.Entry<TxnId, Txn> entry : deps)
            {
                size += txnId.serializedSize(entry.getKey(), version);
                size += txn.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };

    public static final IVersionedSerializer<Writes> writes = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Writes writes, DataOutputPlus out, int version) throws IOException
        {
            timestamp.serialize(writes.executeAt, out, version);
            KeySerializers.keys.serialize(writes.keys, out, version);
            AccordWrite.serializer.serialize((AccordWrite) writes.write, out, version);
        }

        @Override
        public Writes deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Writes(timestamp.deserialize(in, version),
                              KeySerializers.keys.deserialize(in, version),
                              AccordWrite.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(Writes writes, int version)
        {
            long size = timestamp.serializedSize(writes.executeAt, version);
            size += KeySerializers.keys.serializedSize(writes.keys, version);
            size += AccordWrite.serializer.serializedSize((AccordWrite) writes.write, version);
            return size;
        }
    };
}
