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
import java.nio.ByteBuffer;

import accord.local.Command;
import accord.local.Command.WaitingOn;
import accord.primitives.Keys;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ImmutableBitSet;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import accord.utils.SortedArrays.SortedArrayList;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class WaitingOnSerializer
{
    public static void serialize(TxnId txnId, WaitingOn waitingOn, DataOutputPlus out) throws IOException
    {
        if (txnId.kind().awaitsOnlyDeps())
        {
            Timestamp executeAtLeast = waitingOn.executeAtLeast();
            out.writeBoolean(executeAtLeast != null);
            if (executeAtLeast != null)
                CommandSerializers.timestamp.serialize(executeAtLeast, out);
        }
        int keyCount = waitingOn.keys.size();
        int txnIdCount = waitingOn.txnIds.size();
        int waitingOnLength = (txnIdCount + keyCount + 63) / 64;
        serialize(waitingOnLength, waitingOn.waitingOn, out);
        if (txnId.domain() == Routable.Domain.Range)
        {
            int appliedOrInvalidatedLength = (txnIdCount + 63) / 64;
            serialize(appliedOrInvalidatedLength, waitingOn.appliedOrInvalidated, out);
        }
    }

    public static WaitingOn deserialize(TxnId txnId, Keys keys, SortedArrayList<TxnId> txnIds, DataInputPlus in) throws IOException
    {
        Timestamp executeAtLeast = null;
        if (txnId.kind().awaitsOnlyDeps())
        {
            if (in.readBoolean())
                executeAtLeast = CommandSerializers.timestamp.deserialize(in);
        }
        int waitingOnLength = (txnIds.size() + keys.size() + 63) / 64;
        ImmutableBitSet waitingOn = deserialize(waitingOnLength, in);
        ImmutableBitSet appliedOrInvalidated = null;
        if (txnId.domain() == Routable.Domain.Range)
        {
            int appliedOrInvalidatedLength = (txnIds.size() + 63) / 64;
            appliedOrInvalidated = deserialize(appliedOrInvalidatedLength, in);
        }

        WaitingOn result = new WaitingOn(keys, txnIds, waitingOn, appliedOrInvalidated);
        if (executeAtLeast != null)
            result = new Command.WaitingOnWithExecuteAt(result, executeAtLeast);
        return result;
    }

    public static long serializedSize(TxnId txnId, WaitingOn waitingOn)
    {
        int keyCount = waitingOn.keys.size();
        int txnIdCount = waitingOn.txnIds.size();
        int waitingOnLength = (txnIdCount + keyCount + 63) / 64;
        long size = serializedSize(waitingOnLength, waitingOn.waitingOn);
        if (txnId.kind().awaitsOnlyDeps())
        {
            Timestamp executeAtLeast = waitingOn.executeAtLeast();
            size += 1;
            if (executeAtLeast != null)
                size += CommandSerializers.timestamp.serializedSize();
        }
        if (waitingOn.appliedOrInvalidated == null)
            return size;

        int appliedOrInvalidatedLength = (txnIdCount + 63) / 64;
        return size + serializedSize(appliedOrInvalidatedLength, waitingOn.appliedOrInvalidated);
    }

    private static void serialize(int length, SimpleBitSet write, DataOutputPlus out) throws IOException
    {
        long[] bits = SimpleBitSet.SerializationSupport.getArray(write);
        Invariants.checkState(length == bits.length);
        for (long v : bits)
            out.writeLong(v);
    }

    private static ImmutableBitSet deserialize(int length, DataInputPlus in) throws IOException
    {
        long[] bits = new long[length];
        for (int i = 0 ; i < length ; ++i)
            bits[i] = in.readLong();
        return ImmutableBitSet.SerializationSupport.construct(bits);
    }

    public static long serializedSize(int length, SimpleBitSet write)
    {
        long[] bits = SimpleBitSet.SerializationSupport.getArray(write);
        Invariants.checkState(length == bits.length);
        return (long) TypeSizes.LONG_SIZE * length;
    }

    public static ByteBuffer serialize(TxnId txnId, WaitingOn waitingOn) throws IOException
    {
        int keyCount = waitingOn.keys.size();
        int txnIdCount = waitingOn.txnIds.size();
        int waitingOnLength = (txnIdCount + keyCount + 63) / 64;
        int appliedOrInvalidatedLength = 0;
        if (txnId.domain() == Routable.Domain.Range)
            appliedOrInvalidatedLength = (txnIdCount + 63) / 64;

        int size = TypeSizes.LONG_SIZE * (waitingOnLength + appliedOrInvalidatedLength);
        Timestamp executeAtLeast = null;
        if (txnId.kind().awaitsOnlyDeps())
        {
            executeAtLeast = waitingOn.executeAtLeast();
            size += 1;
            if (executeAtLeast != null)
                size += CommandSerializers.timestamp.serializedSize();
        }

        ByteBuffer out = ByteBuffer.allocate(size);
        if (txnId.kind().awaitsOnlyDeps())
        {
            out.put((byte)(executeAtLeast != null ? 1 : 0));
            if (executeAtLeast != null)
                CommandSerializers.timestamp.serialize(executeAtLeast, out);
        }

        serialize(waitingOnLength, waitingOn.waitingOn, out);
        if (appliedOrInvalidatedLength > 0)
            serialize(appliedOrInvalidatedLength, waitingOn.appliedOrInvalidated, out);
        return out.flip();
    }

    private static void serialize(int length, SimpleBitSet write, ByteBuffer out)
    {
        long[] bits = SimpleBitSet.SerializationSupport.getArray(write);
        Invariants.checkState(length == bits.length);
        for (int i = 0; i < length; i++)
            out.putLong(bits[i]);
    }

    public static WaitingOn deserialize(TxnId txnId, Keys keys, SortedArrayList<TxnId> txnIds, ByteBuffer in) throws IOException
    {
        int position = in.position();
        Timestamp executeAtLeast = null;
        if (txnId.kind().awaitsOnlyDeps())
        {
            if (in.get(position++) != 0)
            {
                executeAtLeast = CommandSerializers.timestamp.deserialize(in, position);
                position += CommandSerializers.timestamp.serializedSize();
            }
        }

        int waitingOnLength = (txnIds.size() + keys.size() + 63) / 64;
        ImmutableBitSet waitingOn = deserialize(position, waitingOnLength, in);
        ImmutableBitSet appliedOrInvalidated = null;
        if (txnId.domain() == Routable.Domain.Range)
        {
            position += waitingOnLength*8;
            int appliedOrInvalidatedLength = (txnIds.size() + 63) / 64;
            appliedOrInvalidated = deserialize(position, appliedOrInvalidatedLength, in);
        }

        WaitingOn result = new WaitingOn(keys, txnIds, waitingOn, appliedOrInvalidated);
        if (executeAtLeast != null)
            result = new Command.WaitingOnWithExecuteAt(result, executeAtLeast);
        return result;
    }

    private static ImmutableBitSet deserialize(int position, int length, ByteBuffer in)
    {
        long[] bits = new long[length];
        for (int i = 0 ; i < length ; ++i)
        {
            bits[i] = in.getLong(position);
            position += 8;
        }
        return ImmutableBitSet.SerializationSupport.construct(bits);
    }
}
