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
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

public class AwaitSerializer
{
    public static final IVersionedSerializer<Await> request = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Await await, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(await.txnId, out, version);
            KeySerializers.participants.serialize(await.scope, out, version);
            out.writeByte(await.blockedUntil.ordinal());
            out.writeUnsignedVInt(await.maxAwaitEpoch - await.txnId.epoch());
            out.writeUnsignedVInt(await.maxAwaitEpoch - await.minAwaitEpoch);
            out.writeUnsignedVInt32(await.callbackId + 1);
            Invariants.checkState(await.callbackId >= -1);
        }

        @Override
        public Await deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> scope = KeySerializers.participants.deserialize(in, version);
            BlockedUntil blockedUntil = BlockedUntil.forOrdinal(in.readByte());
            long maxAwaitEpoch = in.readUnsignedVInt() + txnId.epoch();
            long minAwaitEpoch = maxAwaitEpoch - in.readUnsignedVInt();
            int callbackId = in.readUnsignedVInt32() - 1;
            Invariants.checkState(callbackId >= -1);
            return Await.SerializerSupport.create(txnId, scope, blockedUntil, minAwaitEpoch, maxAwaitEpoch, callbackId);
        }

        @Override
        public long serializedSize(Await await, int version)
        {
            return CommandSerializers.txnId.serializedSize(await.txnId, version)
                   + KeySerializers.participants.serializedSize(await.scope, version)
                   + TypeSizes.BYTE_SIZE
                   + VIntCoding.computeUnsignedVIntSize(await.maxAwaitEpoch - await.txnId.epoch())
                   + VIntCoding.computeUnsignedVIntSize(await.maxAwaitEpoch - await.minAwaitEpoch)
                   + VIntCoding.computeUnsignedVIntSize(await.callbackId + 1);
        }
    };

    public static final IVersionedSerializer<AwaitOk> syncReply = new EnumSerializer<>(AwaitOk.class);

    public static final IVersionedSerializer<AsyncAwaitComplete> asyncReply = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AsyncAwaitComplete ok, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(ok.txnId, out, version);
            KeySerializers.route.serialize(ok.route, out, version);
            out.writeByte(ok.newStatus.ordinal());
            out.writeUnsignedVInt32(ok.callbackId);
        }

        @Override
        public AsyncAwaitComplete deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Route<?> scope = KeySerializers.route.deserialize(in, version);
            SaveStatus newStatus = SaveStatus.forOrdinal(in.readByte());
            int callbackId = in.readUnsignedVInt32();
            return new AsyncAwaitComplete(txnId, scope, newStatus, callbackId);
        }

        @Override
        public long serializedSize(AsyncAwaitComplete ok, int version)
        {
            return CommandSerializers.txnId.serializedSize(ok.txnId, version)
                   + KeySerializers.route.serializedSize(ok.route, version)
                   + TypeSizes.BYTE_SIZE
                   + VIntCoding.computeVIntSize(ok.callbackId);
        }
    };
}
