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
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.util.UUID;

import com.google.common.base.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

/**
 * StreamMessageHeader is appended before sending actual data to describe what it's sending.
 */
public class StreamMessageHeader
{
    public static FileMessageHeaderSerializer serializer = new FileMessageHeaderSerializer();

    public final TableId tableId;
    public UUID planId;
    // it tells us if the file was sent by a follower stream session
    public final boolean sendByFollower;
    public int sessionIndex;
    public final int sequenceNumber;
    public final long repairedAt;
    public final UUID pendingRepair;
    public final InetAddressAndPort sender;

    public StreamMessageHeader(TableId tableId,
                               InetAddressAndPort sender,
                               UUID planId,
                               boolean sendByFollower,
                               int sessionIndex,
                               int sequenceNumber,
                               long repairedAt,
                               UUID pendingRepair)
    {
        this.tableId = tableId;
        this.sender = sender;
        this.planId = planId;
        this.sendByFollower = sendByFollower;
        this.sessionIndex = sessionIndex;
        this.sequenceNumber = sequenceNumber;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Header (");
        sb.append("tableId: ").append(tableId);
        sb.append(", #").append(sequenceNumber);
        sb.append(", repairedAt: ").append(repairedAt);
        sb.append(", pendingRepair: ").append(pendingRepair);
        sb.append(", sendByFollower: ").append(sendByFollower);
        sb.append(')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamMessageHeader that = (StreamMessageHeader) o;
        return sendByFollower == that.sendByFollower &&
               sequenceNumber == that.sequenceNumber &&
               Objects.equal(tableId, that.tableId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableId, sendByFollower, sequenceNumber);
    }

    public void addSessionInfo(StreamSession session)
    {
        planId = session.planId();
        sessionIndex = session.sessionIndex();
    }

    public static class FileMessageHeaderSerializer
    {
        public void serialize(StreamMessageHeader header, DataOutputPlus out, int version) throws IOException
        {
            header.tableId.serialize(out);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
            inetAddressAndPortSerializer.serialize(header.sender, out, version);
            UUIDSerializer.serializer.serialize(header.planId, out, version);
            out.writeBoolean(header.sendByFollower);
            out.writeInt(header.sessionIndex);
            out.writeInt(header.sequenceNumber);
            out.writeLong(header.repairedAt);
            out.writeBoolean(header.pendingRepair != null);
            if (header.pendingRepair != null)
            {
                UUIDSerializer.serializer.serialize(header.pendingRepair, out, version);
            }
        }

        public StreamMessageHeader deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
            InetAddressAndPort sender = inetAddressAndPortSerializer.deserialize(in, version);
            UUID planId = UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            boolean sendByFollower = in.readBoolean();
            int sessionIndex = in.readInt();
            int sequenceNumber = in.readInt();
            long repairedAt = in.readLong();
            UUID pendingRepair = in.readBoolean() ? UUIDSerializer.serializer.deserialize(in, version) : null;

            return new StreamMessageHeader(tableId, sender, planId, sendByFollower, sessionIndex, sequenceNumber, repairedAt, pendingRepair);
        }

        public long serializedSize(StreamMessageHeader header, int version)
        {
            long size = header.tableId.serializedSize();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
            size += inetAddressAndPortSerializer.serializedSize(header.sender, version);
            size += UUIDSerializer.serializer.serializedSize(header.planId, version);
            size += TypeSizes.sizeof(header.sendByFollower);
            size += TypeSizes.sizeof(header.sessionIndex);
            size += TypeSizes.sizeof(header.sequenceNumber);
            size += TypeSizes.sizeof(header.repairedAt);
            size += TypeSizes.sizeof(header.pendingRepair != null);
            size += header.pendingRepair != null ? UUIDSerializer.serializer.serializedSize(header.pendingRepair, version) : 0;

            return size;
        }
    }
}
