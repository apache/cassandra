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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.utils.UUIDSerializer;

public class PaxosCleanupResponse
{
    public final UUID session;
    public final boolean wasSuccessful;
    public final String message;

    public PaxosCleanupResponse(UUID session, boolean wasSuccessful, @Nullable String message)
    {
        this.session = session;
        this.wasSuccessful = wasSuccessful;
        this.message = message;
    }

    public static PaxosCleanupResponse success(UUID session)
    {
        return new PaxosCleanupResponse(session, true, null);
    }

    public static PaxosCleanupResponse failed(UUID session, String message)
    {
        return new PaxosCleanupResponse(session, false, message);
    }

    public static final IVerbHandler<PaxosCleanupResponse> verbHandler = (message) -> PaxosCleanupSession.finishSession(message.from(), message.payload);

    public static final IVersionedSerializer<PaxosCleanupResponse> serializer = new IVersionedSerializer<PaxosCleanupResponse>()
    {
        public void serialize(PaxosCleanupResponse finished, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(finished.session, out, version);
            out.writeBoolean(finished.wasSuccessful);
            out.writeBoolean(finished.message != null);
            if (finished.message != null)
                out.writeUTF(finished.message);
        }

        public PaxosCleanupResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID session = UUIDSerializer.serializer.deserialize(in, version);
            boolean success = in.readBoolean();
            String message = in.readBoolean() ? in.readUTF() : null;
            return new PaxosCleanupResponse(session, success, message);
        }

        public long serializedSize(PaxosCleanupResponse finished, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(finished.session, version);
            size += TypeSizes.sizeof(finished.wasSuccessful);
            size += TypeSizes.sizeof(finished.message != null);
            if (finished.message != null)
                size += TypeSizes.sizeof(finished.message);
            return size;
        }
    };
}
