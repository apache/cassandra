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
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.primitives.Deps.SerializerSupport.keyToTxnId;
import static accord.primitives.Deps.SerializerSupport.keyToTxnIdCount;

public abstract class DepsSerializer<D extends Deps> implements IVersionedSerializer<D>
{
    public static final DepsSerializer<Deps> deps = new DepsSerializer<Deps>()
    {
        @Override
        Deps deserialize(Keys keys, TxnId[] txnIds, int[] keyToTxnIds, DataInputPlus in, int version)
        {
            return Deps.SerializerSupport.create(keys, txnIds, keyToTxnIds);
        }
    };

    public static final DepsSerializer<PartialDeps> partialDeps = new DepsSerializer<PartialDeps>()
    {
        @Override
        PartialDeps deserialize(Keys keys, TxnId[] txnIds, int[] keyToTxnIds, DataInputPlus in, int version) throws IOException
        {
            KeyRanges covering = KeySerializers.ranges.deserialize(in, version);
            return PartialDeps.SerializerSupport.create(covering, keys, txnIds, keyToTxnIds);
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
        Keys keys = deps.keys();
        KeySerializers.keys.serialize(keys, out, version);

        int txnIdCount = deps.txnIdCount();
        out.writeUnsignedVInt(txnIdCount);
        for (int i=0; i<txnIdCount; i++)
            CommandSerializers.txnId.serialize(deps.txnId(i), out, version);

        int keyToTxnIdCount = keyToTxnIdCount(deps);
        out.writeUnsignedVInt(keyToTxnIdCount);
        for (int i=0; i<keyToTxnIdCount; i++)
            out.writeUnsignedVInt(keyToTxnId(deps, i));
    }

    @Override
    public D deserialize(DataInputPlus in, int version) throws IOException
    {
        Keys keys = KeySerializers.keys.deserialize(in, version);
        TxnId[] txnIds = new TxnId[(int) in.readUnsignedVInt()];
        for (int i=0; i<txnIds.length; i++)
            txnIds[i] = CommandSerializers.txnId.deserialize(in, version);
        int[] keyToTxnIds = new int[(int) in.readUnsignedVInt()];
        for (int i=0; i<keyToTxnIds.length; i++)
            keyToTxnIds[i] = (int) in.readUnsignedVInt();
        return deserialize(keys, txnIds, keyToTxnIds, in, version);
    }

    abstract D deserialize(Keys keys, TxnId[] txnIds, int[] keyToTxnIds, DataInputPlus in, int version) throws IOException;

    @Override
    public long serializedSize(D deps, int version)
    {
        Keys keys = deps.keys();
        long size = KeySerializers.keys.serializedSize(keys, version);

        int txnIdCount = deps.txnIdCount();
        size += TypeSizes.sizeofUnsignedVInt(txnIdCount);
        for (int i=0; i<txnIdCount; i++)
            size += CommandSerializers.txnId.serializedSize(deps.txnId(i), version);

        int keyToTxnIdCount = keyToTxnIdCount(deps);
        size += TypeSizes.sizeofUnsignedVInt(keyToTxnIdCount);
        for (int i=0; i<keyToTxnIdCount; i++)
            size += TypeSizes.sizeofUnsignedVInt(keyToTxnId(deps, i));
        return size;
    }
}
