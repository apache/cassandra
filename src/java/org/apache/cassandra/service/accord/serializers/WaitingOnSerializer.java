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
import accord.primitives.KeyDeps;
import accord.primitives.RangeDeps;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ImmutableBitSet;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.vint.VIntCoding;

public class WaitingOnSerializer
{
    public static long serializedSize(TxnId txnId, WaitingOn waitingOn)
    {
        Invariants.checkState(txnId.is(Routable.Domain.Key) == (waitingOn.appliedOrInvalidated == null));
        int keyCount = waitingOn.keys.size();
        int txnIdCount = waitingOn.txnIdCount();
        int waitingOnLength = (txnIdCount + keyCount + 63) / 64;
        long size = 1;
        if (waitingOn.executeAtLeast() != null)
            size += CommandSerializers.timestamp.serializedSize();
        size += serializedSize(waitingOnLength, waitingOn.waitingOn);
        size += TypeSizes.sizeofUnsignedVInt(keyCount);
        size += TypeSizes.sizeofUnsignedVInt(txnIdCount);
        if (waitingOn.appliedOrInvalidated == null)
            return size;

        int appliedOrInvalidatedLength = (txnIdCount + 63) / 64;
        return size + serializedSize(appliedOrInvalidatedLength, waitingOn.appliedOrInvalidated);
    }

    public static long serializedSize(int length, SimpleBitSet write)
    {
        long[] bits = SimpleBitSet.SerializationSupport.getArray(write);
        Invariants.checkState(length == bits.length, "Expected length %d != %d", length, bits.length);
        return (long) TypeSizes.LONG_SIZE * length;
    }

    public static ByteBuffer serialize(TxnId txnId, WaitingOn waitingOn) throws IOException
    {
        Invariants.checkState(txnId.is(Routable.Domain.Key) == (waitingOn.appliedOrInvalidated == null));
        int keyCount = waitingOn.keys.size();
        int txnIdCount = waitingOn.txnIdCount();
        int waitingOnLength = (txnIdCount + keyCount + 63) / 64;
        int appliedOrInvalidatedLength = 0;
        if (waitingOn.appliedOrInvalidated != null)
            appliedOrInvalidatedLength = (txnIdCount + 63) / 64;

        Timestamp executeAtLeast = waitingOn.executeAtLeast();
        ByteBuffer out = ByteBuffer.allocate(1 + (executeAtLeast == null ? 0 : CommandSerializers.timestamp.serializedSize())
                                             + TypeSizes.sizeofUnsignedVInt(keyCount) + TypeSizes.sizeofUnsignedVInt(txnIdCount)
                                             + TypeSizes.LONG_SIZE * (waitingOnLength + appliedOrInvalidatedLength));

        if (executeAtLeast == null) out.put((byte)0);
        else
        {
            out.put((byte) 1);
            CommandSerializers.timestamp.serialize(executeAtLeast, out);
        }
        VIntCoding.writeUnsignedVInt32(keyCount, out);
        VIntCoding.writeUnsignedVInt32(txnIdCount, out);
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

    public static WaitingOn deserialize(TxnId txnId, RoutingKeys keys, RangeDeps directRangeDeps, KeyDeps directKeyDeps, ByteBuffer in) throws IOException
    {
        int txnIdCount = directRangeDeps.txnIdCount() + directKeyDeps.txnIdCount();
        int waitingOnLength = (txnIdCount + keys.size() + 63) / 64;
        int position = in.position();
        int flags = in.get(position++);
        Timestamp executesAtLeast = null;
        if ((flags & 1) != 0)
        {
            executesAtLeast = CommandSerializers.timestamp.deserialize(in, position);
            position += CommandSerializers.timestamp.serializedSize();
        }
        int a = VIntCoding.readUnsignedVInt32(in, position);
        position += TypeSizes.sizeofUnsignedVInt(a);
        int b = VIntCoding.readUnsignedVInt32(in, position);
        position += TypeSizes.sizeofUnsignedVInt(b);
        ImmutableBitSet waitingOn = deserialize(position, waitingOnLength, in);
        ImmutableBitSet appliedOrInvalidated = null;
        if (txnId.domain() == Routable.Domain.Range)
        {
            position += waitingOnLength*8;
            int appliedOrInvalidatedLength = (txnIdCount + 63) / 64;
            appliedOrInvalidated = deserialize(position, appliedOrInvalidatedLength, in);
        }

        WaitingOn result = new WaitingOn(keys, directRangeDeps, directKeyDeps, waitingOn, appliedOrInvalidated);
        if (executesAtLeast != null)
            result = new Command.WaitingOnWithExecuteAt(result, executesAtLeast);
        return result;
    }

    private static ImmutableBitSet deserialize(int position, int length, ByteBuffer in)
    {
        long[] bits = new long[length];
        for (int i = 0 ; i < length ; ++i)
        {
            bits[i] = in.getLong(position);
            position += Long.BYTES;
        }
        return ImmutableBitSet.SerializationSupport.construct(bits);
    }
}
