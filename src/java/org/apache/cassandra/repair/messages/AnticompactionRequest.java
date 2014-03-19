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

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

public class AnticompactionRequest extends RepairMessage
{
    public static MessageSerializer serializer = new AnticompactionRequestSerializer();
    public final UUID parentRepairSession;

    public AnticompactionRequest(UUID parentRepairSession)
    {
        super(Type.ANTICOMPACTION_REQUEST, null);
        this.parentRepairSession = parentRepairSession;
    }

    public static class AnticompactionRequestSerializer implements MessageSerializer<AnticompactionRequest>
    {
        public void serialize(AnticompactionRequest message, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out, version);
        }

        public AnticompactionRequest deserialize(DataInput in, int version) throws IOException
        {
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in, version);
            return new AnticompactionRequest(parentRepairSession);
        }

        public long serializedSize(AnticompactionRequest message, int version)
        {
            return UUIDSerializer.serializer.serializedSize(message.parentRepairSession, version);
        }
    }
}