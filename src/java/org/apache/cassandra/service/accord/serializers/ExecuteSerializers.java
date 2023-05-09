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

import accord.messages.ReadData;
import accord.messages.WaitForDependenciesThenApply;
import accord.messages.WhenReadyToExecute;
import accord.messages.WhenReadyToExecute.ExecuteNack;
import accord.messages.WhenReadyToExecute.ExecuteOk;
import accord.messages.WhenReadyToExecute.ExecuteReply;
import accord.messages.WhenReadyToExecute.ExecuteType;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnUnresolvedData;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class ExecuteSerializers
{
    public static final IVersionedSerializer<WhenReadyToExecute> toExecuteSerializer = new IVersionedSerializer<WhenReadyToExecute>()
    {
        @Override
        public void serialize(WhenReadyToExecute t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.kind().val);
            serializerFor(t).serialize(t, out, version);
        }

        @Override
        public WhenReadyToExecute deserialize(DataInputPlus in, int version) throws IOException
        {
            return serializerFor(ExecuteType.fromValue(in.readByte())).deserialize(in, version);
        }

        @Override
        public long serializedSize(WhenReadyToExecute t, int version)
        {
            return sizeof(t.kind().val) + serializerFor(t).serializedSize(t, version);
        }
    };

    public static final WaitAndApplySerializer waitAndApplySerializer = new WaitAndApplySerializer();

    private static class WaitAndApplySerializer implements ExecuteSerializer<WaitForDependenciesThenApply>
    {
        @Override
        public void serialize(WaitForDependenciesThenApply waitAndApply, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(waitAndApply.txnId, out, version);
            KeySerializers.partialRoute.serialize(waitAndApply.route, out, version);
            DepsSerializer.partialDeps.serialize(waitAndApply.deps, out, version);
            KeySerializers.seekables.serialize(waitAndApply.scope, out, version);
            CommandSerializers.writes.serialize(waitAndApply.writes, out, version);
            TxnResult.serializer.serialize((TxnResult) waitAndApply.result, out, version);
            out.writeBoolean(waitAndApply.notifyAgent);
        }

        @Override
        public WaitForDependenciesThenApply deserialize(DataInputPlus in, int version) throws IOException
        {
            return WaitForDependenciesThenApply.SerializerSupport.create(
                CommandSerializers.txnId.deserialize(in, version),
                KeySerializers.partialRoute.deserialize(in, version),
                DepsSerializer.partialDeps.deserialize(in, version),
                KeySerializers.seekables.deserialize(in, version),
                CommandSerializers.writes.deserialize(in, version),
                TxnResult.serializer.deserialize(in, version),
                in.readBoolean());
        }

        @Override
        public long serializedSize(WaitForDependenciesThenApply notifyAgent, int version)
        {
            return CommandSerializers.txnId.serializedSize(notifyAgent.txnId, version)
                + KeySerializers.partialRoute.serializedSize(notifyAgent.route, version)
                + DepsSerializer.partialDeps.serializedSize(notifyAgent.deps, version)
                + KeySerializers.seekables.serializedSize(notifyAgent.scope, version)
                + CommandSerializers.writes.serializedSize(notifyAgent.writes, version)
                + TxnResult.serializer.serializedSize((TxnData)notifyAgent.result, version)
                + sizeof(notifyAgent.notifyAgent);
        }
    }

    public static final ReadDataSerializer readDataSerializer = new ReadDataSerializer();

    private static class ReadDataSerializer implements ExecuteSerializer<ReadData>
    {
        @Override
        public void serialize(ReadData read, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out, version);
            KeySerializers.seekables.serialize(read.scope, out, version);
            out.writeUnsignedVInt(read.waitForEpoch());
            out.writeUnsignedVInt(read.executeAtEpoch - read.waitForEpoch());
            KeySerializers.nullableRoutingKeys.serialize(read.dataReadKeys, out, version);
            TxnRead.nullableSerializer.serialize((TxnRead)read.followupRead, out, version);
        }

        @Override
        public ReadData deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Seekables<?, ?> readScope = KeySerializers.seekables.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            long executeAtEpoch = in.readUnsignedVInt() + waitForEpoch;
            RoutingKeys dataReadKeys = KeySerializers.nullableRoutingKeys.deserialize(in, version);
            TxnRead followupRead = TxnRead.nullableSerializer.deserialize(in, version);
            return ReadData.SerializerSupport.create(txnId, readScope, executeAtEpoch, waitForEpoch, dataReadKeys, followupRead);
        }

        @Override
        public long serializedSize(ReadData read, int version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId, version)
                   + KeySerializers.seekables.serializedSize(read.scope, version)
                   + TypeSizes.sizeofUnsignedVInt(read.waitForEpoch())
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch - read.waitForEpoch())
                   + KeySerializers.nullableRoutingKeys.serializedSize(read.dataReadKeys, version)
                   + TxnRead.nullableSerializer.serializedSize((TxnRead)read.followupRead, version);
        }
    }

    // TODO is there a way to satisfy generics without an interface or explicit cast?
    private interface ExecuteSerializer<T extends WhenReadyToExecute> extends IVersionedSerializer<T>
    {
        void serialize(T bound, DataOutputPlus out, int version) throws IOException;
        T deserialize(DataInputPlus in, int version) throws IOException;
        long serializedSize(T condition, int version);
    }

    private static ExecuteSerializer serializerFor(WhenReadyToExecute toSerialize)
    {
        return serializerFor(toSerialize.kind());
    }

    private static ExecuteSerializer serializerFor(ExecuteType type)
    {
        switch (type)
        {
            case readData:
                return readDataSerializer;
            case waitAndApply:
                return waitAndApplySerializer;
            default:
                throw new IllegalStateException("Unsupported ExecuteType " + type);
        }
    }

    public static final IVersionedSerializer<ExecuteReply> reply = new IVersionedSerializer<ExecuteReply>()
    {
        // TODO (now): use something other than ordinal
        final ExecuteNack[] nacks = ExecuteNack.values();

        @Override
        public void serialize(ExecuteReply reply, DataOutputPlus out, int version) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(1 + ((ExecuteNack) reply).ordinal());
                return;
            }

            out.writeByte(0);
            ExecuteOk executeOk = (ExecuteOk) reply;
            TxnUnresolvedData.nullableSerializer.serialize(executeOk.unresolvedData, out, version);
        }

        @Override
        public ExecuteReply deserialize(DataInputPlus in, int version) throws IOException
        {
            int id = in.readByte();
            if (id != 0)
                return nacks[id - 1];

            return new ExecuteOk(TxnUnresolvedData.nullableSerializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(ExecuteReply reply, int version)
        {
            if (!reply.isOk())
                return TypeSizes.BYTE_SIZE;

            ExecuteOk executeOk = (ExecuteOk) reply;
            return TypeSizes.BYTE_SIZE + TxnUnresolvedData.nullableSerializer.serializedSize(executeOk.unresolvedData, version);
        }
    };
}
