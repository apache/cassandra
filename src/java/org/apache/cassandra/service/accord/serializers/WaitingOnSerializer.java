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

import accord.impl.CommandChange.WaitingOnBitSets;
import accord.local.Command;
import accord.local.Command.WaitingOn;
import accord.primitives.PartialDeps;
import accord.primitives.RangeDeps;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ImmutableBitSet;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;

public class WaitingOnSerializer
{
    public static void serializeBitSetsOnly(TxnId txnId, WaitingOn waitingOn, DataOutputPlus out) throws IOException
    {
        Invariants.require(txnId.is(Key) == (waitingOn.appliedOrInvalidated == null));
        int keyCount = waitingOn.keys.size();
        int txnIdCount = waitingOn.txnIdCount();
        int waitingOnLength = (txnIdCount + keyCount + 63) / 64;
        out.writeUnsignedVInt32(waitingOnLength);
        serialize(waitingOnLength, waitingOn.waitingOn, out);

        if (txnId.is(Range))
        {
            int appliedOrInvalidatedLength = (txnIdCount + 63) / 64;
            out.writeUnsignedVInt32(waitingOnLength - appliedOrInvalidatedLength);
            serialize(appliedOrInvalidatedLength, waitingOn.appliedOrInvalidated, out);
        }
    }

    public static final class WaitingOnBitSetsAndLength extends WaitingOnBitSets
    {
        final int waitingOnLength, appliedOrInvalidatedLength;

        public WaitingOnBitSetsAndLength(ImmutableBitSet waitingOn, ImmutableBitSet appliedOrInvalidated, int waitingOnLength, int appliedOrInvalidatedLength)
        {
            super(waitingOn, appliedOrInvalidated);
            this.waitingOnLength = waitingOnLength;
            this.appliedOrInvalidatedLength = appliedOrInvalidatedLength;
        }

        public WaitingOn construct(PartialDeps deps, Timestamp executeAtLeast, long uniqueHlc)
        {
            Invariants.nonNull(deps);
            RoutingKeys keys = deps.keyDeps.keys();
            RangeDeps directRangeDeps = deps.rangeDeps;
            int txnIdCount = directRangeDeps.txnIdCount();
            Invariants.require(waitingOn.size()/64 == (txnIdCount + keys.size() + 63) / 64);
            Invariants.require(appliedOrInvalidated == null || (appliedOrInvalidated.size()/64 == (txnIdCount + 63)/64));

            WaitingOn result = new WaitingOn(keys, directRangeDeps, waitingOn, appliedOrInvalidated);
            if (executeAtLeast != null) return new Command.WaitingOnWithExecuteAt(result, executeAtLeast);
            else if (uniqueHlc != 0) return new Command.WaitingOnWithMinUniqueHlc(result, uniqueHlc);
            return result;
        }

        public void reserialize(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(waitingOnLength);
            serialize(waitingOnLength, waitingOn, out);
            if (appliedOrInvalidated != null)
            {
                out.writeUnsignedVInt32(waitingOnLength - appliedOrInvalidatedLength);
                serialize(appliedOrInvalidatedLength, appliedOrInvalidated, out);
            }
        }
    }

    public static WaitingOnBitSets deserializeBitSets(TxnId txnId, DataInputPlus in) throws IOException
    {
        ImmutableBitSet waitingOn, appliedOrInvalidated = null;
        int waitingOnLength, appliedOrInvalidatedLength = 0;
        waitingOnLength = in.readUnsignedVInt32();
        waitingOn = deserialize(waitingOnLength, in);
        if (txnId.is(Range))
        {
            appliedOrInvalidatedLength = waitingOnLength - in.readUnsignedVInt32();
            appliedOrInvalidated = deserialize(appliedOrInvalidatedLength, in);
        }

        return new WaitingOnBitSetsAndLength(waitingOn, appliedOrInvalidated, waitingOnLength, appliedOrInvalidatedLength);
    }

    public static void skip(TxnId txnId, DataInputPlus in) throws IOException
    {
        int waitingOnLength = in.readUnsignedVInt32();
        in.skipBytesFully(waitingOnLength * 8);
        if (txnId.is(Range))
        {
            int delta = in.readUnsignedVInt32();
            in.skipBytesFully((waitingOnLength - delta) * 8);
        }
    }

    private static void serialize(int length, SimpleBitSet write, DataOutputPlus out) throws IOException
    {
        long[] bits = SimpleBitSet.SerializationSupport.getArray(write);
        Invariants.require(length == bits.length);
        for (int i = 0; i < length; i++)
            out.writeLong(bits[i]);
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
        Invariants.require(length == bits.length, "Expected length %d != %d", length, bits.length);
        return (long) TypeSizes.LONG_SIZE * length;
    }
}
