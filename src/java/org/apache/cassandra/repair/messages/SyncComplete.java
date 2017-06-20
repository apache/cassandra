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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.SessionSummary;

/**
 *
 * @since 2.0
 */
public class SyncComplete extends RepairMessage
{
    public static final MessageSerializer serializer = new SyncCompleteSerializer();

    /** nodes that involved in this sync */
    public final NodePair nodes;
    /** true if sync success, false otherwise */
    public final boolean success;

    public final List<SessionSummary> summaries;

    public SyncComplete(RepairJobDesc desc, NodePair nodes, boolean success, List<SessionSummary> summaries)
    {
        super(Type.SYNC_COMPLETE, desc);
        this.nodes = nodes;
        this.success = success;
        this.summaries = summaries;
    }

    public SyncComplete(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, boolean success, List<SessionSummary> summaries)
    {
        super(Type.SYNC_COMPLETE, desc);
        this.summaries = summaries;
        this.nodes = new NodePair(endpoint1, endpoint2);
        this.success = success;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SyncComplete))
            return false;
        SyncComplete other = (SyncComplete)o;
        return messageType == other.messageType &&
               desc.equals(other.desc) &&
               success == other.success &&
               nodes.equals(other.nodes) &&
               summaries.equals(other.summaries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(messageType, desc, success, nodes, summaries);
    }

    private static class SyncCompleteSerializer implements MessageSerializer<SyncComplete>
    {
        public void serialize(SyncComplete message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            NodePair.serializer.serialize(message.nodes, out, version);
            out.writeBoolean(message.success);

            out.writeInt(message.summaries.size());
            for (SessionSummary summary: message.summaries)
            {
                SessionSummary.serializer.serialize(summary, out, version);
            }
        }

        public SyncComplete deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            NodePair nodes = NodePair.serializer.deserialize(in, version);
            boolean success = in.readBoolean();

            int numSummaries = in.readInt();
            List<SessionSummary> summaries = new ArrayList<>(numSummaries);
            for (int i=0; i<numSummaries; i++)
            {
                summaries.add(SessionSummary.serializer.deserialize(in, version));
            }

            return new SyncComplete(desc, nodes, success, summaries);
        }

        public long serializedSize(SyncComplete message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += NodePair.serializer.serializedSize(message.nodes, version);
            size += TypeSizes.sizeof(message.success);

            size += TypeSizes.sizeof(message.summaries.size());
            for (SessionSummary summary: message.summaries)
            {
                size += SessionSummary.serializer.serializedSize(summary, version);
            }

            return size;
        }
    }
}
