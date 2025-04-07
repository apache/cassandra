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
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Ranges;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordFetchCoordinator.AccordFetchRequest;
import org.apache.cassandra.service.accord.AccordFetchCoordinator.StreamData;
import org.apache.cassandra.service.accord.AccordFetchCoordinator.StreamingTxn;
import org.apache.cassandra.utils.CastingSerializer;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class FetchSerializers
{
    public static final IVersionedSerializer<FetchRequest> request = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(FetchRequest request, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt(request.executeAtEpoch);
            CommandSerializers.txnId.serialize(request.txnId, out);
            KeySerializers.ranges.serialize((Ranges) request.scope, out);
            DepsSerializers.partialDeps.serialize(request.partialDeps, out);
            StreamingTxn.serializer.serialize(request.read, out, version);
        }

        @Override
        public FetchRequest deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordFetchRequest(in.readUnsignedVInt(),
                                          CommandSerializers.txnId.deserialize(in),
                                          KeySerializers.ranges.deserialize(in),
                                          DepsSerializers.partialDeps.deserialize(in),
                                          StreamingTxn.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(FetchRequest request, Version version)
        {
            return TypeSizes.sizeofUnsignedVInt(request.executeAtEpoch)
                   + CommandSerializers.txnId.serializedSize(request.txnId)
                   + KeySerializers.ranges.serializedSize((Ranges) request.scope)
                   + DepsSerializers.partialDeps.serializedSize(request.partialDeps)
                   + StreamingTxn.serializer.serializedSize(request.read, version);
        }
    };

    public static final UnversionedSerializer<ReadReply> reply = new UnversionedSerializer<>()
    {
        final CommitOrReadNack[] nacks = CommitOrReadNack.values();
        final UnversionedSerializer<Data> streamDataSerializer = CastingSerializer.create(StreamData.class, StreamData.serializer);

        @Override
        public void serialize(ReadReply reply, DataOutputPlus out) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(1 + ((CommitOrReadNack) reply).ordinal());
                return;
            }

            out.writeByte(0);
            FetchResponse response = (FetchResponse) reply;
            serializeNullable(response.unavailable, out, KeySerializers.ranges);
            serializeNullable(response.data, out, streamDataSerializer);
            CommandSerializers.timestamp.serialize(response.safeToReadAfter, out);
        }

        @Override
        public ReadReply deserialize(DataInputPlus in) throws IOException
        {
            int id = in.readByte();
            if (id != 0)
                return nacks[id - 1];

            return new FetchResponse(deserializeNullable(in, KeySerializers.ranges),
                                     deserializeNullable(in, streamDataSerializer),
                                     CommandSerializers.timestamp.deserialize(in));
        }

        @Override
        public long serializedSize(ReadReply reply)
        {
            if (!reply.isOk())
                return TypeSizes.BYTE_SIZE;

            FetchResponse response = (FetchResponse) reply;
            return TypeSizes.BYTE_SIZE
                   + serializedNullableSize(response.unavailable, KeySerializers.ranges)
                   + serializedNullableSize(response.data, streamDataSerializer)
                   + CommandSerializers.timestamp.serializedSize(response.safeToReadAfter);
        }
    };

}
