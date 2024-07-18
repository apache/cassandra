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
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.NullableSerializer;

import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIds;
import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIdsCount;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIds;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIdsCount;
import static accord.primitives.Routable.Domain.Key;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public abstract class DepsSerializer<D extends Deps> extends IVersionedWithKeysSerializer.AbstractWithKeysSerializer implements IVersionedWithKeysSerializer<Seekables<?, ?>, D>
{
    public static final DepsSerializer<Deps> deps = new DepsSerializer<>()
    {
        @Override
        Deps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, KeyDeps directKeyDeps, DataInputPlus in, int version)
        {
            return new Deps(keyDeps, rangeDeps, directKeyDeps);
        }
    };
    public static final IVersionedSerializer<Deps> nullableDeps = NullableSerializer.wrap(deps);

    public static final DepsSerializer<PartialDeps> partialDeps = new DepsSerializer<>()
    {
        @Override
        PartialDeps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, KeyDeps directKeyDeps, DataInputPlus in, int version) throws IOException
        {
            Ranges covering = KeySerializers.ranges.deserialize(in, version);
            return new PartialDeps(covering, keyDeps, rangeDeps, directKeyDeps);
        }

        @Override
        public void serialize(PartialDeps partialDeps, DataOutputPlus out, int version) throws IOException
        {
            super.serialize(partialDeps, out, version);
            KeySerializers.ranges.serialize(partialDeps.covering, out, version);
        }

        @Override
        public void serialize(Seekables<?, ?> superset, PartialDeps partialDeps, DataOutputPlus out, int version) throws IOException
        {
            super.serialize(superset, partialDeps, out, version);
            KeySerializers.ranges.serialize(partialDeps.covering, out, version);
        }

        @Override
        public long serializedSize(PartialDeps partialDeps, int version)
        {
            return super.serializedSize(partialDeps, version)
                 + KeySerializers.ranges.serializedSize(partialDeps.covering, version);
        }

        @Override
        public long serializedSize(Seekables<?, ?> keys, PartialDeps partialDeps, int version)
        {
            return super.serializedSize(keys, partialDeps, version)
                 + KeySerializers.ranges.serializedSize(partialDeps.covering, version);
        }
    };

    public static final IVersionedSerializer<PartialDeps> nullablePartialDeps = NullableSerializer.wrap(partialDeps);

    abstract D deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, KeyDeps directKeyDeps, DataInputPlus in, int version) throws IOException;

    @Override
    public void serialize(D deps, DataOutputPlus out, int version) throws IOException
    {
        KeySerializers.keys.serialize(deps.keyDeps.keys(), out, version);
        serializeWithoutKeys(deps, out, version);
    }

    @Override
    public void serialize(Seekables<?, ?> superset, D deps, DataOutputPlus out, int version) throws IOException
    {
        if (superset.domain() == Key) serializeSubset(deps.keyDeps.keys(), superset, out);
        else KeySerializers.keys.serialize(deps.keyDeps.keys(), out, version);
        serializeWithoutKeys(deps, out, version);
    }

    @Override
    public D deserialize(DataInputPlus in, int version) throws IOException
    {
        Keys keys = KeySerializers.keys.deserialize(in, version);
        return deserializeWithoutKeys(keys, in, version);
    }

    @Override
    public D deserialize(Seekables<?, ?> superset, DataInputPlus in, int version) throws IOException
    {
        Keys keys;
        if (superset.domain() == Key) keys = (Keys)deserializeSubset(superset, in);
        else keys = KeySerializers.keys.deserialize(in, version);
        return deserializeWithoutKeys(keys, in, version);
    }

    @Override
    public long serializedSize(D deps, int version)
    {
        long size = KeySerializers.keys.serializedSize(deps.keyDeps.keys(), version);
        size += serializedSizeWithoutKeys(deps, version);
        return size;
    }

    @Override
    public long serializedSize(Seekables<?, ?> keys, D deps, int version)
    {
        long size;
        if (keys.domain() == Key) size = serializedSubsetSize(deps.keyDeps.keys(), keys);
        else size = KeySerializers.keys.serializedSize(deps.keyDeps.keys(), version);
        size += serializedSizeWithoutKeys(deps, version);
        return size;
    }

    private void serializeWithoutKeys(D deps, DataOutputPlus out, int version) throws IOException
    {
        serializeKeyDepsWithoutKeys(deps.keyDeps, out, version);

        {
            RangeDeps rangeDeps = deps.rangeDeps;
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

        {
            Keys keys = deps.directKeyDeps.keys();
            boolean isSubset = isSubset(keys, deps.keyDeps.keys());
            out.writeBoolean(isSubset);
            if (isSubset) serializeSubset(keys, deps.keyDeps.keys(), out);
            else KeySerializers.keys.serialize(keys, out, version);

            serializeKeyDepsWithoutKeys(deps.directKeyDeps, out, version);
        }
    }

    private void serializeKeyDepsWithoutKeys(KeyDeps keyDeps, DataOutputPlus out, int version) throws IOException
    {
        int txnIdCount = keyDeps.txnIdCount();
        out.writeUnsignedVInt32(txnIdCount);
        for (int i = 0; i < txnIdCount; i++)
            CommandSerializers.txnId.serialize(keyDeps.txnId(i), out, version);

        int keysToTxnIdsCount = keysToTxnIdsCount(keyDeps);
        out.writeUnsignedVInt32(keysToTxnIdsCount);
        for (int i = 0; i < keysToTxnIdsCount; i++)
            out.writeUnsignedVInt32(keysToTxnIds(keyDeps, i));
    }

    private D deserializeWithoutKeys(Keys keys, DataInputPlus in, int version) throws IOException
    {
        KeyDeps keyDeps = deserializeKeyDeps(keys, in, version);

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

        KeyDeps directKeyDeps;
        {
            boolean isSubset = in.readBoolean();
            Keys directKeys = isSubset ? (Keys)deserializeSubset(keys, in) : KeySerializers.keys.deserialize(in, version);
            directKeyDeps = deserializeKeyDeps(directKeys, in, version);
        }

        return deserialize(keyDeps, rangeDeps, directKeyDeps, in, version);
    }

    private static KeyDeps deserializeKeyDeps(Keys keys, DataInputPlus in, int version) throws IOException
    {
        int txnIdCount = in.readUnsignedVInt32();
        TxnId[] txnIds = new TxnId[txnIdCount];
        for (int i = 0; i < txnIdCount; i++)
            txnIds[i] = CommandSerializers.txnId.deserialize(in, version);

        int keysToTxnIdsCount = in.readUnsignedVInt32();
        int[] keysToTxnIds = new int[keysToTxnIdsCount];
        for (int i = 0; i < keysToTxnIdsCount; i++)
            keysToTxnIds[i] = in.readUnsignedVInt32();

        return KeyDeps.SerializerSupport.create(keys, txnIds, keysToTxnIds);
    }

    private long serializedSizeWithoutKeys(D deps, int version)
    {
        long size = serializedSizeOfKeyDepsWithoutKeys(deps.keyDeps, version);

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

        {
            boolean isSubset = isSubset(deps.directKeyDeps.keys(), deps.keyDeps.keys());
            size += 1;
            size += isSubset ? serializedSubsetSize(deps.directKeyDeps.keys(), deps.keyDeps.keys()) : KeySerializers.keys.serializedSize(deps.directKeyDeps.keys(), version);
            size += serializedSizeOfKeyDepsWithoutKeys(deps.directKeyDeps, version);
        }
        return size;
    }

    private static long serializedSizeOfKeyDepsWithoutKeys(KeyDeps keyDeps, int version)
    {
        int txnIdCount = keyDeps.txnIdCount();
        long size = sizeofUnsignedVInt(txnIdCount);
        for (int i = 0; i < txnIdCount; i++)
            size += CommandSerializers.txnId.serializedSize(keyDeps.txnId(i), version);

        int keysToTxnIdsCount = keysToTxnIdsCount(keyDeps);
        size += sizeofUnsignedVInt(keysToTxnIdsCount);
        for (int i = 0; i < keysToTxnIdsCount; i++)
            size += sizeofUnsignedVInt(keysToTxnIds(keyDeps, i));
        return size;
    }

    private static boolean isSubset(Keys test, Keys superset)
    {
        return test.foldl(superset, (k, p, v, i) -> v + 1, 0, 0, 0) == test.size();
    }

}
