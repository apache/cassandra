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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class TxnMultiUpdate implements Update, IMeasurableMemory
{
    private final Keys keys;
    public final TxnUpdate[] updates;

    public TxnMultiUpdate(TxnUpdate[] updates)
    {
        this.updates = updates;
        Keys keys = Keys.EMPTY;
        for (int i = 0; i < updates.length; i++) // TODO perf
            keys = keys.with(updates[i].keys());

        this.keys = keys;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public Write apply(Timestamp executeAt, @Nullable Data data)
    {
        for (int i = 0; i < updates.length; i++)
        {
            TxnUpdate update = updates[i];
            if (update.checkCondition(data))
            {
                TxnData txnData = data != null ? ((TxnData) data) : new TxnData();
                txnData.setSelectedBranch(i);
                return update.apply(executeAt, txnData); // apply the first matching branch and stop
            }
        }

        return TxnWrite.EMPTY;
    }

    @Override
    public Update slice(Ranges ranges)
    {
        TxnUpdate[] slice = new TxnUpdate[updates.length];
        for (int i = 0; i < updates.length; i++)
            slice[i] = (TxnUpdate) updates[i].slice(ranges);
        return new TxnMultiUpdate(slice);
    }

    @Override
    public Update merge(Update other)
    {
        TxnMultiUpdate that = (TxnMultiUpdate) other;
        TxnUpdate[] merged = new TxnUpdate[updates.length];
        for (int i = 0; i < updates.length; i++)
            merged[i] = updates[i].merge(that.updates[i]);
        return new TxnMultiUpdate(merged);
    }

    public final static IVersionedSerializer<TxnMultiUpdate> serializer = new IVersionedSerializer<TxnMultiUpdate>()
    {
        @Override
        public void serialize(TxnMultiUpdate t, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(t.updates.length);
            for (int i = 0; i < t.updates.length; i++)
                TxnUpdate.serializer.serialize(t.updates[i], out, version);
        }

        @Override
        public TxnMultiUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            int length = in.readInt();
            TxnUpdate[] updates = new TxnUpdate[length];
            for (int i = 0; i < length; i++)
                updates[i] = TxnUpdate.serializer.deserialize(in, version);
            return new TxnMultiUpdate(updates);
        }

        @Override
        public long serializedSize(TxnMultiUpdate t, int version)
        {
            long size = TypeSizes.INT_SIZE;
            for (int i = 0; i < t.updates.length; i++)
                size += TxnUpdate.serializer.serializedSize(t.updates[i], version);
            return size;
        }
    };

    @Override
    public long unsharedHeapSize()
    {
        return 0; // TODO
    }
}
