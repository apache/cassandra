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

import accord.api.ProgressLog.BlockedUntil;
import accord.messages.Await;
import accord.messages.Await.AsyncAwaitComplete;
import accord.messages.Await.AwaitOk;
import accord.messages.RecoverAwait;
import accord.messages.RecoverAwait.RecoverAwaitOk;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

public class AwaitSerializers
{
    public static final UnversionedSerializer<Await> request = new RequestSerializer<>()
    {
        @Override
        public Await deserialize(TxnId txnId, Participants<?> scope, BlockedUntil blockedUntil, boolean notifyProgressLog, long minAwaitEpoch, long maxAwaitEpoch, int callbackId, DataInputPlus in)
        {
            return Await.SerializerSupport.create(txnId, scope, blockedUntil, notifyProgressLog, minAwaitEpoch, maxAwaitEpoch, callbackId);
        }
    };

    public static final UnversionedSerializer<RecoverAwait> recoverRequest = new RequestSerializer<>()
    {
        @Override
        public RecoverAwait deserialize(TxnId txnId, Participants<?> scope, BlockedUntil blockedUntil, boolean notifyProgressLog, long minAwaitEpoch, long maxAwaitEpoch, int callbackId, DataInputPlus in) throws IOException
        {
            TxnId recoverId = CommandSerializers.txnId.deserialize(in);
            return RecoverAwait.SerializerSupport.create(txnId, scope, blockedUntil, notifyProgressLog, minAwaitEpoch, maxAwaitEpoch, callbackId, recoverId);
        }

        @Override
        public void serialize(RecoverAwait await, DataOutputPlus out) throws IOException
        {
            super.serialize(await, out);
            CommandSerializers.txnId.serialize(await.recoverId, out);
        }

        @Override
        public long serializedSize(RecoverAwait await)
        {
            return super.serializedSize(await) + CommandSerializers.txnId.serializedSize(await.recoverId);
        }
    };

    static abstract class RequestSerializer<A extends Await> implements UnversionedSerializer<A>
    {
        abstract A deserialize(TxnId txnId, Participants<?> scope, BlockedUntil blockedUntil, boolean notifyProgressLog, long minAwaitEpoch, long maxAwaitEpoch, int callbackId, DataInputPlus in) throws IOException;

        @Override
        public void serialize(A await, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(await.txnId, out);
            KeySerializers.participants.serialize(await.scope, out);
            out.writeByte((await.blockedUntil.ordinal() << 1) | (await.notifyProgressLog ? 1 : 0));
            out.writeUnsignedVInt(await.maxAwaitEpoch - await.txnId.epoch());
            out.writeUnsignedVInt(await.maxAwaitEpoch - await.minAwaitEpoch);
            out.writeUnsignedVInt32(await.callbackId + 1);
            Invariants.require(await.callbackId >= -1);
        }

        @Override
        public A deserialize(DataInputPlus in) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            int blockedAndNotify = in.readByte();
            BlockedUntil blockedUntil = BlockedUntil.forOrdinal(blockedAndNotify >>> 1);
            boolean notifyProgressLog = (blockedAndNotify & 1) == 1;
            long maxAwaitEpoch = in.readUnsignedVInt() + txnId.epoch();
            long minAwaitEpoch = maxAwaitEpoch - in.readUnsignedVInt();
            int callbackId = in.readUnsignedVInt32() - 1;
            Invariants.require(callbackId >= -1);
            return deserialize(txnId, scope, blockedUntil, notifyProgressLog, minAwaitEpoch, maxAwaitEpoch, callbackId, in);
        }

        @Override
        public long serializedSize(A await)
        {
            return CommandSerializers.txnId.serializedSize(await.txnId)
                   + KeySerializers.participants.serializedSize(await.scope)
                   + TypeSizes.BYTE_SIZE
                   + VIntCoding.computeUnsignedVIntSize(await.maxAwaitEpoch - await.txnId.epoch())
                   + VIntCoding.computeUnsignedVIntSize(await.maxAwaitEpoch - await.minAwaitEpoch)
                   + VIntCoding.computeUnsignedVIntSize(await.callbackId + 1);
        }
    }

    public static final UnversionedSerializer<AwaitOk> syncReply = EncodeAsVInt32.of(AwaitOk.class);
    public static final UnversionedSerializer<RecoverAwaitOk> recoverReply = EncodeAsVInt32.of(RecoverAwaitOk.class);

    public static final UnversionedSerializer<AsyncAwaitComplete> asyncReply = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(AsyncAwaitComplete ok, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(ok.txnId, out);
            KeySerializers.route.serialize(ok.route, out);
            out.writeByte(ok.newStatus.ordinal());
            out.writeUnsignedVInt32(ok.callbackId);
        }

        @Override
        public AsyncAwaitComplete deserialize(DataInputPlus in) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Route<?> scope = KeySerializers.route.deserialize(in);
            SaveStatus newStatus = SaveStatus.forOrdinal(in.readByte());
            int callbackId = in.readUnsignedVInt32();
            return new AsyncAwaitComplete(txnId, scope, newStatus, callbackId);
        }

        @Override
        public long serializedSize(AsyncAwaitComplete ok)
        {
            return CommandSerializers.txnId.serializedSize(ok.txnId)
                   + KeySerializers.route.serializedSize(ok.route)
                   + TypeSizes.BYTE_SIZE
                   + VIntCoding.computeVIntSize(ok.callbackId);
        }
    };
}
