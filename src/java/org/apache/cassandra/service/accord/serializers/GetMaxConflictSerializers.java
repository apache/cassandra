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

import accord.messages.GetMaxConflict;
import accord.messages.GetMaxConflict.GetMaxConflictOk;
import accord.primitives.PartialRoute;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class GetMaxConflictSerializers
{
    public static final IVersionedSerializer<GetMaxConflict> request = new TxnRequestSerializer.WithUnsyncedSerializer<GetMaxConflict>()
    {
        @Override
        public void serializeBody(GetMaxConflict msg, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.seekables.serialize(msg.keys, out, version);
            out.writeUnsignedVInt(msg.executionEpoch);
        }

        @Override
        public GetMaxConflict deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey) throws IOException
        {
            Seekables<?, ?> keys = KeySerializers.seekables.deserialize(in, version);
            long executionEpoch = in.readUnsignedVInt();
            return GetMaxConflict.SerializationSupport.create(scope, waitForEpoch, minEpoch, keys, executionEpoch);
        }

        @Override
        public long serializedBodySize(GetMaxConflict msg, int version)
        {
            return KeySerializers.seekables.serializedSize(msg.keys, version)
                   + TypeSizes.sizeofUnsignedVInt(msg.executionEpoch);
        }
    };

    public static final IVersionedSerializer<GetMaxConflictOk> reply = new IVersionedSerializer<GetMaxConflictOk>()
    {
        @Override
        public void serialize(GetMaxConflictOk reply, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.timestamp.serialize(reply.maxConflict, out, version);
            out.writeUnsignedVInt(reply.latestEpoch);
        }

        @Override
        public GetMaxConflictOk deserialize(DataInputPlus in, int version) throws IOException
        {
            Timestamp maxConflict = CommandSerializers.timestamp.deserialize(in, version);
            long latestEpoch = in.readUnsignedVInt();
            return new GetMaxConflictOk(maxConflict, latestEpoch);
        }

        @Override
        public long serializedSize(GetMaxConflictOk reply, int version)
        {
            return CommandSerializers.timestamp.serializedSize(reply.maxConflict, version)
                   + TypeSizes.sizeofUnsignedVInt(reply.latestEpoch);
        }
    };
}
