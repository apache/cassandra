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

import accord.local.Command.WaitingOn;
import accord.primitives.Deps;
import accord.utils.ImmutableBitSet;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class WaitingOnSerializer
{
    public static void serialize(WaitingOn waitingOn, DataOutputPlus out) throws IOException
    {
        // TODO (expected): use run length encoding; we know that at most 1/3rd of bits will be set between the three bitsets
        int length = (waitingOn.deps.txnIdCount() + 63) / 64;
        serialize(length, waitingOn.waitingOnCommit, out);
        serialize(length, waitingOn.waitingOnApply, out);
        serialize(length, waitingOn.appliedOrInvalidated, out);
    }

    public static WaitingOn deserialize(Deps deps, DataInputPlus in) throws IOException
    {
        int length = (deps.txnIdCount() + 63) / 64;
        ImmutableBitSet waitingOnCommit = deserialize(length, in);
        ImmutableBitSet waitingOnApply = deserialize(length, in);
        ImmutableBitSet appliedOrInvalidated = deserialize(length, in);
        return new WaitingOn(deps, waitingOnCommit, waitingOnApply, appliedOrInvalidated);
    }

    public static long serializedSize(WaitingOn waitingOn)
    {
        int length = (waitingOn.deps.txnIdCount() + 63) / 64;
        return serializedSize(length, waitingOn.waitingOnCommit)
               + serializedSize(length, waitingOn.waitingOnApply)
               + serializedSize(length, waitingOn.appliedOrInvalidated);
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

    public static ByteBuffer serialize(WaitingOn waitingOn) throws IOException
    {
        int length = (waitingOn.deps.txnIdCount() + 63) / 64;
        ByteBuffer out = ByteBuffer.allocate(TypeSizes.LONG_SIZE * length * 3);
        serialize(length, waitingOn.waitingOnCommit, out);
        serialize(length, waitingOn.waitingOnApply, out);
        serialize(length, waitingOn.appliedOrInvalidated, out);
        return (ByteBuffer) out.flip();
    }

    private static void serialize(int length, SimpleBitSet write, ByteBuffer out)
    {
        long[] bits = SimpleBitSet.SerializationSupport.getArray(write);
        Invariants.checkState(length == bits.length);
        for (int i = 0; i < length; i++)
            out.putLong(bits[i]);
    }

    public static WaitingOn deserialize(Deps deps, ByteBuffer in) throws IOException
    {
        int length = (deps.txnIdCount() + 63) / 64;
        ImmutableBitSet waitingOnCommit = deserialize(length, in);
        ImmutableBitSet waitingOnApply = deserialize(length, in);
        ImmutableBitSet appliedOrInvalidated = deserialize(length, in);
        return new WaitingOn(deps, waitingOnCommit, waitingOnApply, appliedOrInvalidated);
    }

    private static ImmutableBitSet deserialize(int length, ByteBuffer in)
    {
        long[] bits = new long[length];
        for (int i = 0 ; i < length ; ++i)
            bits[i] = in.getLong();
        return ImmutableBitSet.SerializationSupport.construct(bits);
    }
}
