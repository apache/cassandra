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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Notification from coordinator to replicas when a bulk data transfer fails, triggering cleanup of the pending
 * transfer state.
 * @see CoordinatedTransfer
 * @see PendingLocalTransfer
 */
public class TransferFailed
{
    final TimeUUID planId;

    public TransferFailed(TimeUUID planId)
    {
        this.planId = planId;
    }

    public static final IVersionedSerializer<TransferFailed> serializer = new IVersionedSerializer<TransferFailed>()
    {
        @Override
        public void serialize(TransferFailed t, DataOutputPlus out, int version) throws IOException
        {
            TimeUUID.Serializer.instance.serialize(t.planId, out, version);
        }

        @Override
        public TransferFailed deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID planId = TimeUUID.Serializer.instance.deserialize(in, version);
            return new TransferFailed(planId);
        }

        @Override
        public long serializedSize(TransferFailed t, int version)
        {
            return TimeUUID.Serializer.instance.serializedSize(t.planId, version);
        }
    };

    @Override
    public String toString()
    {
        return "TransferFailed{" +
               "planId=" + planId +
               '}';
    }
}