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

import accord.api.Data;
import accord.impl.AbstractFetchCoordinator.FetchRequest;
import accord.impl.AbstractFetchCoordinator.FetchResponse;
import accord.local.Status;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordFetchCoordinator.StreamData;
import org.apache.cassandra.service.accord.AccordFetchCoordinator.StreamingTxn;
import org.apache.cassandra.utils.CastingSerializer;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class FetchSerializers
{
    public static final IVersionedSerializer<FetchRequest> request = new IVersionedSerializer<FetchRequest>()
    {
        @Override
        public void serialize(FetchRequest request, DataOutputPlus out, int version) throws IOException
        {
            Invariants.checkArgument(request.txnId.equals(TxnId.NONE));
            Invariants.checkArgument(request.waitForStatus == Status.Applied);
            Invariants.checkArgument(request.waitUntil.equals(Timestamp.MAX));

            out.writeUnsignedVInt(request.waitForEpoch());
            CommandSerializers.txnId.serialize((TxnId) request.executeReadAt, out, version);
            KeySerializers.ranges.serialize((Ranges) request.readScope, out, version);

            DepsSerializer.partialDeps.serialize(request.partialDeps, out, version);
            StreamingTxn.serializer.serialize(request.read, out, version);
        }

        @Override
        public FetchRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            return new FetchRequest(in.readUnsignedVInt(),
                                    CommandSerializers.txnId.deserialize(in, version),
                                    KeySerializers.ranges.deserialize(in, version),
                                    DepsSerializer.partialDeps.deserialize(in, version),
                                    StreamingTxn.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(FetchRequest request, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(request.waitForEpoch())
                   + CommandSerializers.txnId.serializedSize((TxnId) request.executeReadAt, version)
                   + KeySerializers.ranges.serializedSize((Ranges) request.readScope, version)
                   + DepsSerializer.partialDeps.serializedSize(request.partialDeps, version)
                   + StreamingTxn.serializer.serializedSize(request.read, version);
        }
    };

    public static final IVersionedSerializer<ReadReply> reply = new IVersionedSerializer<ReadReply>()
    {
        final ReadData.ReadNack[] nacks = ReadData.ReadNack.values();
        final IVersionedSerializer<Data> streamDataSerializer = new CastingSerializer<>(StreamData.class, StreamData.serializer);

        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, int version) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(1 + ((ReadData.ReadNack) reply).ordinal());
                return;
            }

            out.writeByte(0);
            FetchResponse response = (FetchResponse) reply;
            serializeNullable(response.unavailable, out, version, KeySerializers.ranges);
            serializeNullable(response.data, out, version, streamDataSerializer);
            CommandSerializers.timestamp.serialize(response.maxApplied, out, version);
        }

        @Override
        public ReadReply deserialize(DataInputPlus in, int version) throws IOException
        {
            int id = in.readByte();
            if (id != 0)
                return nacks[id - 1];

            return new FetchResponse(deserializeNullable(in, version, KeySerializers.ranges),
                                     deserializeNullable(in, version, streamDataSerializer),
                                     CommandSerializers.timestamp.deserialize(in, version));
        }

        @Override
        public long serializedSize(ReadReply reply, int version)
        {
            if (!reply.isOk())
                return TypeSizes.BYTE_SIZE;

            FetchResponse response = (FetchResponse) reply;
            return TypeSizes.BYTE_SIZE
                   + serializedNullableSize(response.unavailable, version, KeySerializers.ranges)
                   + serializedNullableSize(response.data, version, streamDataSerializer)
                   + CommandSerializers.timestamp.serializedSize(response.maxApplied, version);
        }
    };
}
