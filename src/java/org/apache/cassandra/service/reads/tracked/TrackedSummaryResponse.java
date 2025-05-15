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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;

import java.io.IOException;

public class TrackedSummaryResponse
{
    private final TrackedRead.Id readId;
    private final MutationSummary summary;

    public TrackedSummaryResponse(TrackedRead.Id readId, MutationSummary summary)
    {
        this.readId = readId;
        this.summary = summary;
    }

    public TrackedRead.Id readId()
    {
        return readId;
    }

    public MutationSummary summary()
    {
        return summary;
    }

    public static final IVerbHandler<TrackedSummaryResponse> verbHandler =
        message -> MutationTrackingService.instance.localReads().receiveSummary(message.from(), message.payload);

    public static final IVersionedSerializer<TrackedSummaryResponse> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TrackedSummaryResponse summary, DataOutputPlus out, int version) throws IOException
        {
            TrackedRead.Id.serializer.serialize(summary.readId, out, version);
            MutationSummary.serializer.serialize(summary.summary, out, version);
        }

        @Override
        public TrackedSummaryResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            TrackedRead.Id id = TrackedRead.Id.serializer.deserialize(in, version);
            MutationSummary summary = MutationSummary.serializer.deserialize(in, version);
            return new TrackedSummaryResponse(id, summary);
        }

        @Override
        public long serializedSize(TrackedSummaryResponse summary, int version)
        {
            return TrackedRead.Id.serializer.serializedSize(summary.readId, version) +
                   MutationSummary.serializer.serializedSize(summary.summary, version);
        }
    };
}
