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
import accord.primitives.Route;
import accord.primitives.Timestamp;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class GetMaxConflictSerializers
{
    public static final UnversionedSerializer<GetMaxConflict> request = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(GetMaxConflict msg, DataOutputPlus out) throws IOException
        {
            KeySerializers.route.serialize(msg.scope, out);
            out.writeUnsignedVInt(msg.waitForEpoch);
            out.writeUnsignedVInt(msg.minEpoch);
            out.writeUnsignedVInt(msg.executionEpoch);
        }

        @Override
        public GetMaxConflict deserialize(DataInputPlus in) throws IOException
        {
            Route<?> scope = KeySerializers.route.deserialize(in);
            long waitForEpoch = in.readUnsignedVInt();
            long minEpoch = in.readUnsignedVInt();
            long executionEpoch = in.readUnsignedVInt();
            return GetMaxConflict.SerializationSupport.create(scope, waitForEpoch, minEpoch, executionEpoch);
        }

        @Override
        public long serializedSize(GetMaxConflict msg)
        {
            return KeySerializers.route.serializedSize(msg.scope())
                   + TypeSizes.sizeofUnsignedVInt(msg.waitForEpoch)
                   + TypeSizes.sizeofUnsignedVInt(msg.minEpoch)
                   + TypeSizes.sizeofUnsignedVInt(msg.executionEpoch);
        }
    };

    public static final UnversionedSerializer<GetMaxConflictOk> reply = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(GetMaxConflictOk reply, DataOutputPlus out) throws IOException
        {
            CommandSerializers.timestamp.serialize(reply.maxConflict, out);
            out.writeUnsignedVInt(reply.latestEpoch);
        }

        @Override
        public GetMaxConflictOk deserialize(DataInputPlus in) throws IOException
        {
            Timestamp maxConflict = CommandSerializers.timestamp.deserialize(in);
            long latestEpoch = in.readUnsignedVInt();
            return new GetMaxConflictOk(maxConflict, latestEpoch);
        }

        @Override
        public long serializedSize(GetMaxConflictOk reply)
        {
            return CommandSerializers.timestamp.serializedSize(reply.maxConflict)
                   + TypeSizes.sizeofUnsignedVInt(reply.latestEpoch);
        }
    };
}
