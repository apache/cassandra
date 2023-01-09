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

import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;

import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIds;
import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIdsCount;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIds;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIdsCount;

public abstract class DepsSerializer<D extends Deps> implements IVersionedSerializer<D>
{
    public static final DepsSerializer<Deps> deps = new DepsSerializer<Deps>()
    {
        @Override
        Deps deserialize(Keys keys, TxnId[] keyTxnIds, int[] keysToTxnIds,
                         Range[] ranges, TxnId[] rangeTxnIds, int[] rangesToTxnIds,
                         DataInputPlus in, int version)
        {
            return new Deps(KeyDeps.SerializerSupport.create(keys, keyTxnIds, keysToTxnIds),
                            RangeDeps.SerializerSupport.create(ranges, rangeTxnIds, rangesToTxnIds));
        }
    };

    public static final DepsSerializer<PartialDeps> partialDeps = new DepsSerializer<PartialDeps>()
    {
        @Override
        PartialDeps deserialize(Keys keys, TxnId[] keyTxnIds, int[] keysToTxnIds,
                         Range[] ranges, TxnId[] rangeTxnIds, int[] rangesToTxnIds,
                         DataInputPlus in, int version) throws IOException
        {
            Ranges covering = KeySerializers.ranges.deserialize(in, version);
            return new PartialDeps(covering, KeyDeps.SerializerSupport.create(keys, keyTxnIds, keysToTxnIds),
                            RangeDeps.SerializerSupport.create(ranges, rangeTxnIds, rangesToTxnIds));
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
            return KeySerializers.ranges.serializedSize(partialDeps.covering, version)
                   + super.serializedSize(partialDeps, version);
        }
    };

    @Override
    public void serialize(D deps, DataOutputPlus out, int version) throws IOException
    {
        Keys keys = deps.keyDeps.keys();
        KeySerializers.keys.serialize(keys, out, version);

        {
            int txnIdCount = deps.keyDeps.txnIdCount();
            out.writeUnsignedVInt(txnIdCount);
            for (int i = 0; i < txnIdCount; i++)
                CommandSerializers.txnId.serialize(deps.keyDeps.txnId(i), out, version);

            int keysToTxnIdsCount = keysToTxnIdsCount(deps.keyDeps);
            out.writeUnsignedVInt(keysToTxnIdsCount);
            for (int i = 0; i < keysToTxnIdsCount; i++)
                out.writeUnsignedVInt(keysToTxnIds(deps.keyDeps, i));
        }

        {
            int rangeCount = deps.rangeDeps.rangeCount();
            out.writeUnsignedVInt(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                TokenRange.serializer.serialize((TokenRange) deps.rangeDeps.range(i), out, version);

            int txnIdCount = deps.rangeDeps.txnIdCount();
            out.writeUnsignedVInt(txnIdCount);
            for (int i = 0; i < txnIdCount; i++)
                CommandSerializers.txnId.serialize(deps.rangeDeps.txnId(i), out, version);

            int rangesToTxnIdsCount = rangesToTxnIdsCount(deps.rangeDeps);
            out.writeUnsignedVInt(rangesToTxnIdsCount);
            for (int i = 0; i < rangesToTxnIdsCount; i++)
                out.writeUnsignedVInt(rangesToTxnIds(deps.rangeDeps, i));
        }
    }

    @Override
    public D deserialize(DataInputPlus in, int version) throws IOException
    {
        Keys keys = KeySerializers.keys.deserialize(in, version);
        TxnId[] keyTxnIds = new TxnId[(int) in.readUnsignedVInt()];
        for (int i = 0; i < keyTxnIds.length; i++)
            keyTxnIds[i] = CommandSerializers.txnId.deserialize(in, version);
        int[] keysToTxnIds = new int[(int) in.readUnsignedVInt()];
        for (int i = 0; i < keysToTxnIds.length; i++)
            keysToTxnIds[i] = (int) in.readUnsignedVInt();

        Range[] ranges = new Range[(int) in.readUnsignedVInt()];
        for (int i = 0; i < ranges.length; i++)
            ranges[i] = TokenRange.serializer.deserialize(in, version);
        TxnId[] rangeTxnIds = new TxnId[(int) in.readUnsignedVInt()];
        for (int i = 0; i < rangeTxnIds.length; i++)
            rangeTxnIds[i] = CommandSerializers.txnId.deserialize(in, version);

        int[] rangeToTxnIds = new int[(int) in.readUnsignedVInt()];
        for (int i = 0; i < rangeToTxnIds.length; i++)
            rangeToTxnIds[i] = (int) in.readUnsignedVInt();

        return deserialize(keys, keyTxnIds, keysToTxnIds, ranges, rangeTxnIds, rangeToTxnIds, in, version);
    }

    abstract D deserialize(Keys keys, TxnId[] keyTxnIds, int[] keysToTxnIds,
                           Range[] ranges, TxnId[] rangeTxnIds, int[] rangesToTxnIds,
                           DataInputPlus in, int version) throws IOException;

    @Override
    public long serializedSize(D deps, int version)
    {
        long size = KeySerializers.keys.serializedSize(deps.keyDeps.keys(), version);

        {
            int txnIdCount = deps.keyDeps.txnIdCount();
            size += TypeSizes.sizeofUnsignedVInt(txnIdCount);
            for (int i = 0; i < txnIdCount; i++)
                size += CommandSerializers.txnId.serializedSize(deps.keyDeps.txnId(i), version);

            int keyToTxnIdCount = keysToTxnIdsCount(deps.keyDeps);
            size += TypeSizes.sizeofUnsignedVInt(keyToTxnIdCount);
            for (int i = 0; i < keyToTxnIdCount; i++)
                size += TypeSizes.sizeofUnsignedVInt(keysToTxnIds(deps.keyDeps, i));
        }

        {
            int rangeCount = deps.rangeDeps.rangeCount();
            size += TypeSizes.sizeofUnsignedVInt(rangeCount);
            for (int i = 0 ; i < rangeCount ; ++i)
                size += TokenRange.serializer.serializedSize((TokenRange) deps.rangeDeps.range(i), version);

            int txnIdCount = deps.rangeDeps.txnIdCount();
            size += TypeSizes.sizeofUnsignedVInt(txnIdCount);
            for (int i = 0; i<txnIdCount; i++)
                size += CommandSerializers.txnId.serializedSize(deps.rangeDeps.txnId(i), version);

            int rangesToTxnIdsCount = rangesToTxnIdsCount(deps.rangeDeps);
            size += TypeSizes.sizeofUnsignedVInt(rangesToTxnIdsCount);
            for (int i = 0; i < rangesToTxnIdsCount; i++)
                size += TypeSizes.sizeofUnsignedVInt(rangesToTxnIds(deps.rangeDeps, i));
        }
        return size;
    }
}
