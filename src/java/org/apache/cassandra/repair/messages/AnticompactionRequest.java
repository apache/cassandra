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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class AnticompactionRequest extends RepairMessage
{
    public static MessageSerializer serializer = new AnticompactionRequestSerializer();
    public final UUID parentRepairSession;
    /**
     * Successfully repaired ranges. Does not contain null.
     */
    public final Collection<Range<Token>> successfulRanges;

    public AnticompactionRequest(UUID parentRepairSession, Collection<Range<Token>> ranges)
    {
        super(Type.ANTICOMPACTION_REQUEST, null);
        this.parentRepairSession = parentRepairSession;
        this.successfulRanges = ranges;
    }

    public static class AnticompactionRequestSerializer implements MessageSerializer<AnticompactionRequest>
    {
        public void serialize(AnticompactionRequest message, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out, version);
            out.writeInt(message.successfulRanges.size());
            for (Range<Token> r : message.successfulRanges)
            {
                MessagingService.validatePartitioner(r);
                Range.tokenSerializer.serialize(r, out, version);
            }
        }

        public AnticompactionRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in, version);
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), version));
            return new AnticompactionRequest(parentRepairSession, ranges);
        }

        public long serializedSize(AnticompactionRequest message, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(message.parentRepairSession, version);
            for (Range<Token> r : message.successfulRanges)
                size += Range.tokenSerializer.serializedSize(r, version);
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "AnticompactionRequest{" +
                "parentRepairSession=" + parentRepairSession +
                "} " + super.toString();
    }
}
