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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IReadResponse;
import org.apache.cassandra.db.ReadKind;
import org.apache.cassandra.io.EmbeddedAsymmetricVersionedSerializer;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.utils.ArraySerializers;

import static org.apache.cassandra.db.ReadKind.TRACKED_SUMMARY;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public class TrackedSummaryResponse implements IReadResponse
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
        MutationTrackingService.ensureEnabled();
        TrackedSummaryResponse response = message.payload;
        if (logger.isTraceEnabled())
            logger.trace("Received summary {} from {}, for {}", response.summary, message.from(), response.readId);
        ReadReconciliations.instance.acceptRemoteSummary(message.from(), message.payload);
    };

    public static final UnversionedSerializer<TrackedSummaryResponse> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(TrackedSummaryResponse summary, DataOutputPlus out) throws IOException
        {
            TrackedRead.Id.serializer.serialize(summary.readId, out);
            MutationSummary.serializer.serialize(summary.summary, out);
            out.writeUnsignedVInt32(summary.dataNode);
            ArraySerializers.serializeVIntArray(summary.summaryNodes, out);
        }

        @Override
        public TrackedSummaryResponse deserialize(DataInputPlus in) throws IOException
        {
            TrackedRead.Id id = TrackedRead.Id.serializer.deserialize(in);
            MutationSummary summary = MutationSummary.serializer.deserialize(in);
            int dataNode = in.readUnsignedVInt32();
            int[] summaryNodes = ArraySerializers.deserializeVIntArray(in);
            return new TrackedSummaryResponse(id, summary, dataNode, summaryNodes);
        }

        @Override
        public long serializedSize(TrackedSummaryResponse summary)
        {
            long size = TrackedRead.Id.serializer.serializedSize(summary.readId);
            size += MutationSummary.serializer.serializedSize(summary.summary);
            size += sizeofUnsignedVInt(summary.dataNode);
            size += ArraySerializers.serializedVIntArraySize(summary.summaryNodes);
            return size;
        }
    };

    public static final IVersionedAsymmetricSerializer<TrackedSummaryResponse, TrackedSummaryResponse> embedded =
        EmbeddedAsymmetricVersionedSerializer.mtEmbedded(serializer);

    @Override
    public ReadKind kind()
    {
        return TRACKED_SUMMARY;
    }
}
