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
package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Message to cleanup repair resources on replica nodes.
 *
 * @since 2.1.6
 */
public class CleanupMessage extends RepairMessage
{
    public final TimeUUID parentRepairSession;

    public CleanupMessage(TimeUUID parentRepairSession)
    {
        super(null);
        this.parentRepairSession = parentRepairSession;
    }

    @Override
    public TimeUUID parentRepairSession()
    {
        return parentRepairSession;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CleanupMessage))
            return false;
        CleanupMessage other = (CleanupMessage) o;
        return parentRepairSession.equals(other.parentRepairSession);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parentRepairSession);
    }

    public static final IVersionedSerializer<CleanupMessage> serializer = new IVersionedSerializer<CleanupMessage>()
    {
        public void serialize(CleanupMessage message, DataOutputPlus out, int version) throws IOException
        {
            message.parentRepairSession.serialize(out);
        }

        public CleanupMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID parentRepairSession = TimeUUID.deserialize(in);
            return new CleanupMessage(parentRepairSession);
        }

        public long serializedSize(CleanupMessage message, int version)
        {
            return TimeUUID.sizeInBytes();
        }
    };
}
