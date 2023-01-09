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

import com.google.common.primitives.Ints;

import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;

import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIds;
import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIdsCount;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIds;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIdsCount;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public abstract class DepsSerializer<D extends Deps> implements IVersionedSerializer<D>
{
    public static final DepsSerializer<Deps> deps = new DepsSerializer<Deps>()
    {
        @Override
        Deps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in, int version)
        {
            return new Deps(keyDeps, rangeDeps);
        }
    };

    public static final DepsSerializer<PartialDeps> partialDeps = new DepsSerializer<PartialDeps>()
    {
        @Override
        PartialDeps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in, int version) throws IOException
        {
            Ranges covering = KeySerializers.ranges.deserialize(in, version);
            return new PartialDeps(covering, keyDeps, rangeDeps);
        }

        @Override
        public void serialize(PartialDeps partialDeps, DataOutputPlus out, int version) throws IOException
        {
            super.serialize(partialDeps, out, version);
            KeySerializers.ranges.serialize(partialDeps.covering, out, version);
        }

        @Override
        public long serializedSize(PartialDeps partialDeps, int version)
        {
            return super.serializedSize(partialDeps, version)
                 + KeySerializers.ranges.serializedSize(partialDeps.covering, version);
        }
    };

    @Override
    public void serialize(D deps, DataOutputPlus out, int version) throws IOException
    {
        KeyDeps keyDeps = deps.keyDeps;
        {
            KeySerializers.keys.serialize(keyDeps.keys(), out, version);

            int txnIdCount = keyDeps.txnIdCount();
            out.writeUnsignedVInt32(txnIdCount);
            for (int i = 0; i < txnIdCount; i++)
                CommandSerializers.txnId.serialize(keyDeps.txnId(i), out, version);

            int keysToTxnIdsCount = keysToTxnIdsCount(keyDeps);
            out.writeUnsignedVInt32(keysToTxnIdsCount);
            for (int i = 0; i < keysToTxnIdsCount; i++)
                out.writeUnsignedVInt32(keysToTxnIds(keyDeps, i));
        }

        RangeDeps rangeDeps = deps.rangeDeps;
        {
            int rangeCount = rangeDeps.rangeCount();
            out.writeUnsignedVInt32(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                TokenRange.serializer.serialize((TokenRange) rangeDeps.range(i), out, version);

            int txnIdCount = rangeDeps.txnIdCount();
            out.writeUnsignedVInt32(txnIdCount);
            for (int i = 0; i < txnIdCount; i++)
                CommandSerializers.txnId.serialize(rangeDeps.txnId(i), out, version);

            int rangesToTxnIdsCount = rangesToTxnIdsCount(rangeDeps);
            out.writeUnsignedVInt32(rangesToTxnIdsCount);
            for (int i = 0; i < rangesToTxnIdsCount; i++)
                out.writeUnsignedVInt32(rangesToTxnIds(rangeDeps, i));
        }
    }

    @Override
    public D deserialize(DataInputPlus in, int version) throws IOException
    {
        KeyDeps keyDeps;
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);

            int txnIdCount = in.readUnsignedVInt32();
            TxnId[] txnIds = new TxnId[txnIdCount];
            for (int i = 0; i < txnIdCount; i++)
                txnIds[i] = CommandSerializers.txnId.deserialize(in, version);

            int keysToTxnIdsCount = in.readUnsignedVInt32();
            int[] keysToTxnIds = new int[keysToTxnIdsCount];
            for (int i = 0; i < keysToTxnIdsCount; i++)
                keysToTxnIds[i] = in.readUnsignedVInt32();

            keyDeps = KeyDeps.SerializerSupport.create(keys, txnIds, keysToTxnIds);
        }

        RangeDeps rangeDeps;
        {
            int rangeCount = Ints.checkedCast(in.readUnsignedVInt32());
            Range[] ranges = new Range[rangeCount];
            for (int i = 0; i < rangeCount; i++)
                ranges[i] = TokenRange.serializer.deserialize(in, version);

            int txnIdCount = in.readUnsignedVInt32();
            TxnId[] txnIds = new TxnId[txnIdCount];
            for (int i = 0; i < txnIdCount; i++)
                txnIds[i] = CommandSerializers.txnId.deserialize(in, version);

            int rangesToTxnIdsCount = in.readUnsignedVInt32();
            int[] rangesToTxnIds = new int[rangesToTxnIdsCount];
            for (int i = 0; i < rangesToTxnIdsCount; i++)
                rangesToTxnIds[i] = in.readUnsignedVInt32();

            rangeDeps = RangeDeps.SerializerSupport.create(ranges, txnIds, rangesToTxnIds);
        }

        return deserialize(keyDeps, rangeDeps, in, version);
    }

    abstract D deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in, int version) throws IOException;

    @Override
    public long serializedSize(D deps, int version)
    {
        long size = 0L;

        KeyDeps keyDeps = deps.keyDeps;
        {
            size += KeySerializers.keys.serializedSize(keyDeps.keys(), version);

            int txnIdCount = keyDeps.txnIdCount();
            size += sizeofUnsignedVInt(txnIdCount);
            for (int i = 0; i < txnIdCount; i++)
                size += CommandSerializers.txnId.serializedSize(keyDeps.txnId(i), version);

            int keysToTxnIdsCount = keysToTxnIdsCount(keyDeps);
            size += sizeofUnsignedVInt(keysToTxnIdsCount);
            for (int i = 0; i < keysToTxnIdsCount; i++)
                size += sizeofUnsignedVInt(keysToTxnIds(keyDeps, i));
        }

        RangeDeps rangeDeps = deps.rangeDeps;
        {
            int rangeCount = rangeDeps.rangeCount();
            size += sizeofUnsignedVInt(rangeCount);
            for (int i = 0 ; i < rangeCount ; ++i)
                size += TokenRange.serializer.serializedSize((TokenRange) rangeDeps.range(i), version);

            int txnIdCount = rangeDeps.txnIdCount();
            size += sizeofUnsignedVInt(txnIdCount);
            for (int i = 0; i < txnIdCount; i++)
                size += CommandSerializers.txnId.serializedSize(rangeDeps.txnId(i), version);

            int rangesToTxnIdsCount = rangesToTxnIdsCount(rangeDeps);
            size += sizeofUnsignedVInt(rangesToTxnIdsCount);
            for (int i = 0; i < rangesToTxnIdsCount; i++)
                size += sizeofUnsignedVInt(rangesToTxnIds(rangeDeps, i));
        }

        return size;
    }
}
