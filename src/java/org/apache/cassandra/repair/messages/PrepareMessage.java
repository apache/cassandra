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
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.TimeUUID;


public class PrepareMessage extends RepairMessage
{
    public final List<TableId> tableIds;
    public final Collection<Range<Token>> ranges;

    public final TimeUUID parentRepairSession;
    public final boolean isIncremental;
    public final long repairedAt;
    public final boolean isGlobal;
    public final PreviewKind previewKind;

    public PrepareMessage(TimeUUID parentRepairSession, List<TableId> tableIds, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal, PreviewKind previewKind)
    {
        super(null);
        this.parentRepairSession = parentRepairSession;
        this.tableIds = tableIds;
        this.ranges = ranges;
        this.isIncremental = isIncremental;
        this.repairedAt = repairedAt;
        this.isGlobal = isGlobal;
        this.previewKind = previewKind;
    }

    @Override
    public TimeUUID parentRepairSession()
    {
        return parentRepairSession;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PrepareMessage))
            return false;
        PrepareMessage other = (PrepareMessage) o;
        return parentRepairSession.equals(other.parentRepairSession) &&
               isIncremental == other.isIncremental &&
               isGlobal == other.isGlobal &&
               previewKind == other.previewKind &&
               repairedAt == other.repairedAt &&
               tableIds.equals(other.tableIds) &&
               ranges.equals(other.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parentRepairSession, isGlobal, previewKind, isIncremental, repairedAt, tableIds, ranges);
    }

    private static final String MIXED_MODE_ERROR = "Some nodes involved in repair are on an incompatible major version. " +
                                                   "Repair is not supported in mixed major version clusters.";

    public static final IVersionedSerializer<PrepareMessage> serializer = new IVersionedSerializer<PrepareMessage>()
    {
        public void serialize(PrepareMessage message, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(version == MessagingService.current_version, MIXED_MODE_ERROR);

            out.writeInt(message.tableIds.size());
            for (TableId tableId : message.tableIds)
                tableId.serialize(out);
            message.parentRepairSession.serialize(out);
            out.writeInt(message.ranges.size());
            for (Range<Token> r : message.ranges)
            {
                IPartitioner.validate(r);
                Range.tokenSerializer.serialize(r, out, version);
            }
            out.writeBoolean(message.isIncremental);
            out.writeLong(message.repairedAt);
            out.writeBoolean(message.isGlobal);
            out.writeInt(message.previewKind.getSerializationVal());
        }

        public PrepareMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            Preconditions.checkArgument(version == MessagingService.current_version, MIXED_MODE_ERROR);

            int tableIdCount = in.readInt();
            List<TableId> tableIds = new ArrayList<>(tableIdCount);
            for (int i = 0; i < tableIdCount; i++)
                tableIds.add(TableId.deserialize(in));
            TimeUUID parentRepairSession = TimeUUID.deserialize(in);
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(in, IPartitioner.global(), version));
            boolean isIncremental = in.readBoolean();
            long timestamp = in.readLong();
            boolean isGlobal = in.readBoolean();
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new PrepareMessage(parentRepairSession, tableIds, ranges, isIncremental, timestamp, isGlobal, previewKind);
        }

        public long serializedSize(PrepareMessage message, int version)
        {
            long size;
            size = TypeSizes.sizeof(message.tableIds.size());
            for (TableId tableId : message.tableIds)
                size += tableId.serializedSize();
            size += TimeUUID.sizeInBytes();
            size += TypeSizes.sizeof(message.ranges.size());
            for (Range<Token> r : message.ranges)
                size += Range.tokenSerializer.serializedSize(r, version);
            size += TypeSizes.sizeof(message.isIncremental);
            size += TypeSizes.sizeof(message.repairedAt);
            size += TypeSizes.sizeof(message.isGlobal);
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
        }
    };

    @Override
    public String toString()
    {
        return "PrepareMessage{" +
               "tableIds='" + tableIds + '\'' +
               ", ranges=" + ranges +
               ", parentRepairSession=" + parentRepairSession +
               ", isIncremental=" + isIncremental +
               ", timestamp=" + repairedAt +
               ", isGlobal=" + isGlobal +
               '}';
    }
}
