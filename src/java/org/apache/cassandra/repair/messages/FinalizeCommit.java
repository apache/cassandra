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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.TimeUUID;

public class FinalizeCommit extends RepairMessage
{
    public final TimeUUID sessionID;

    public FinalizeCommit(TimeUUID sessionID)
    {
        super(null);
        assert sessionID != null;
        this.sessionID = sessionID;
    }

    @Override
    public TimeUUID parentRepairSession()
    {
        return sessionID;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FinalizeCommit that = (FinalizeCommit) o;

        return sessionID.equals(that.sessionID);
    }

    public int hashCode()
    {
        return sessionID.hashCode();
    }

    public String toString()
    {
        return "FinalizeCommit{" +
               "sessionID=" + sessionID +
               '}';
    }

    public static final IVersionedSerializer<FinalizeCommit> serializer = new IVersionedSerializer<FinalizeCommit>()
    {
        public void serialize(FinalizeCommit msg, DataOutputPlus out, int version) throws IOException
        {
            msg.sessionID.serialize(out);
        }

        public FinalizeCommit deserialize(DataInputPlus in, int version) throws IOException
        {
            return new FinalizeCommit(TimeUUID.deserialize(in));
        }

        public long serializedSize(FinalizeCommit msg, int version)
        {
            return TimeUUID.sizeInBytes();
        }
    };
}
