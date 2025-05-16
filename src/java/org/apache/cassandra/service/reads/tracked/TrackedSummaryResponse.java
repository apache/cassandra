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
package org.apache.cassandra.service.reads.tracked;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.replication.MutationSummary;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackedSummaryResponse
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedSummaryResponse.class);

    public final TrackedRead.Id readId;
    public final MutationSummary summary;
    public final int dataNode;
    public final int[] summaryNodes;

    public TrackedSummaryResponse(TrackedRead.Id readId, MutationSummary summary, int dataNode, int[] summaryNodes)
    {
        this.readId = readId;
        this.summary = summary;
        this.dataNode = dataNode;
        this.summaryNodes = summaryNodes;
    }

    public static final IVerbHandler<TrackedSummaryResponse> verbHandler = message ->
    {
        TrackedSummaryResponse response = message.payload;
        if (logger.isTraceEnabled())
            logger.trace("Received summary {} from {}, for {}", response.summary, message.from(), response.readId);
        ReadReconciliations.instance.acceptRemoteSummary(message.from(), message.payload);
    };

    public static final IVersionedSerializer<TrackedSummaryResponse> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TrackedSummaryResponse summary, DataOutputPlus out, int version) throws IOException
        {
            TrackedRead.Id.serializer.serialize(summary.readId, out, version);
            MutationSummary.serializer.serialize(summary.summary, out, version);
            out.writeInt(summary.dataNode);
            out.writeInt(summary.summaryNodes.length);
            for (int hostid : summary.summaryNodes)
                out.writeInt(hostid);
        }

        @Override
        public TrackedSummaryResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            TrackedRead.Id id = TrackedRead.Id.serializer.deserialize(in, version);
            MutationSummary summary = MutationSummary.serializer.deserialize(in, version);
            int dataNode = in.readInt();
            int[] summaryNodes = new int[in.readInt()];
            for (int i = 0; i < summaryNodes.length; i++)
                summaryNodes[i] = in.readInt();
            return new TrackedSummaryResponse(id, summary, dataNode, summaryNodes);
        }

        @Override
        public long serializedSize(TrackedSummaryResponse summary, int version)
        {
            return TrackedRead.Id.serializer.serializedSize(summary.readId, version) +
                   MutationSummary.serializer.serializedSize(summary.summary, version) +
                   TypeSizes.sizeof(summary.dataNode) +
                   TypeSizes.sizeof(summary.summaryNodes.length) +
                   TypeSizes.INT_SIZE * (long) summary.summaryNodes.length;
        }
    };
}
