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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;


public class PrepareMessage extends RepairMessage
{
    public final static MessageSerializer serializer = new PrepareMessageSerializer(false);
    public final static MessageSerializer globalSerializer = new PrepareMessageSerializer(true);
    public final List<UUID> cfIds;
    public final Collection<Range<Token>> ranges;

    public final UUID parentRepairSession;
    public final boolean isIncremental;

    public PrepareMessage(UUID parentRepairSession, List<UUID> cfIds, Collection<Range<Token>> ranges, boolean isIncremental, boolean isGlobal)
    {
        super(isGlobal ? Type.PREPARE_GLOBAL_MESSAGE : Type.PREPARE_MESSAGE, null);
        this.parentRepairSession = parentRepairSession;
        this.cfIds = cfIds;
        this.ranges = ranges;
        this.isIncremental = isIncremental;
    }

    public static class PrepareMessageSerializer implements MessageSerializer<PrepareMessage>
    {
        private final boolean isGlobal;

        public PrepareMessageSerializer(boolean global)
        {
            this.isGlobal = global;
        }

        public void serialize(PrepareMessage message, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(message.cfIds.size());
            for (UUID cfId : message.cfIds)
                UUIDSerializer.serializer.serialize(cfId, out, version);
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out, version);
            out.writeInt(message.ranges.size());
            for (Range<Token> r : message.ranges)
            {
                MessagingService.validatePartitioner(r);
                Range.tokenSerializer.serialize(r, out, version);
            }
            out.writeBoolean(message.isIncremental);
        }

        public PrepareMessage deserialize(DataInput in, int version) throws IOException
        {
            int cfIdCount = in.readInt();
            List<UUID> cfIds = new ArrayList<>(cfIdCount);
            for (int i = 0; i < cfIdCount; i++)
                cfIds.add(UUIDSerializer.serializer.deserialize(in, version));
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in, version);
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), version));
            boolean isIncremental = in.readBoolean();

            return new PrepareMessage(parentRepairSession, cfIds, ranges, isIncremental, isGlobal);
        }

        public long serializedSize(PrepareMessage message, int version)
        {
            long size;
            TypeSizes sizes = TypeSizes.NATIVE;
            size = sizes.sizeof(message.cfIds.size());
            for (UUID cfId : message.cfIds)
                size += UUIDSerializer.serializer.serializedSize(cfId, version);
            size += UUIDSerializer.serializer.serializedSize(message.parentRepairSession, version);
            size += sizes.sizeof(message.ranges.size());
            for (Range<Token> r : message.ranges)
                size += Range.tokenSerializer.serializedSize(r, version);
            size += sizes.sizeof(message.isIncremental);
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "PrepareMessage{" +
                "cfIds='" + cfIds + '\'' +
                ", ranges=" + ranges +
                ", parentRepairSession=" + parentRepairSession +
                ", isIncremental="+isIncremental +
                '}';
    }
}
