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

import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.FullRoute;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TxnRequestSerializer.WithUnsyncedSerializer;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.cassandra.utils.NullableSerializer.*;

public class PreacceptSerializers
{
    private PreacceptSerializers() {}

    public static final IVersionedSerializer<PreAccept> request = new WithUnsyncedSerializer<PreAccept>()
    {
        @Override
        public void serializeBody(PreAccept msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.partialTxn.serialize(msg.partialTxn, out, version);
            serializeNullable(msg.route, out, version, KeySerializers.fullRoute);
            out.writeUnsignedVInt(msg.maxEpoch - msg.minEpoch);
        }

        @Override
        public PreAccept deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey) throws IOException
        {
            PartialTxn partialTxn = CommandSerializers.partialTxn.deserialize(in, version);
            @Nullable FullRoute<?> fullRoute = deserializeNullable(in, version, KeySerializers.fullRoute);
            long maxEpoch = in.readUnsignedVInt() + minEpoch;
            return PreAccept.SerializerSupport.create(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey,
                                                      maxEpoch, partialTxn, fullRoute);
        }

        @Override
        public long serializedBodySize(PreAccept msg, int version)
        {
            return CommandSerializers.partialTxn.serializedSize(msg.partialTxn, version)
                   + serializedNullableSize(msg.route, version, KeySerializers.fullRoute)
                   + TypeSizes.sizeofUnsignedVInt(msg.maxEpoch - msg.minEpoch);
        }
    };

    public static final IVersionedSerializer<PreAcceptReply> reply = new IVersionedSerializer<PreAcceptReply>()
    {
        @Override
        public void serialize(PreAcceptReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOk());
            if (!reply.isOk())
                return;

            PreAcceptOk preAcceptOk = (PreAcceptOk) reply;
            CommandSerializers.txnId.serialize(preAcceptOk.txnId, out, version);
            CommandSerializers.timestamp.serialize(preAcceptOk.witnessedAt, out, version);
            DepsSerializer.partialDeps.serialize(preAcceptOk.deps, out, version);
        }

        @Override
        public PreAcceptReply deserialize(DataInputPlus in, int version) throws IOException
        {
            if (!in.readBoolean())
                return PreAccept.PreAcceptNack.INSTANCE;

            return new PreAcceptOk(CommandSerializers.txnId.deserialize(in, version),
                                   CommandSerializers.timestamp.deserialize(in, version),
                                   DepsSerializer.partialDeps.deserialize(in, version));
        }

        @Override
        public long serializedSize(PreAcceptReply reply, int version)
        {
            long size = TypeSizes.sizeof(reply.isOk());
            if (!reply.isOk())
                return size;

            PreAcceptOk preAcceptOk = (PreAcceptOk) reply;
            size += CommandSerializers.txnId.serializedSize(preAcceptOk.txnId, version);
            size += CommandSerializers.timestamp.serializedSize(preAcceptOk.witnessedAt, version);
            size += DepsSerializer.partialDeps.serializedSize(preAcceptOk.deps, version);

            return size;
        }
    };
}
