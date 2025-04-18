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
import org.apache.cassandra.net.Message;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;

import java.io.IOException;

public class TrackedReadSummary
{
    private final long readId;
    private final MutationSummary summary;

    public TrackedReadSummary(long readId, MutationSummary summary)
    {
        this.readId = readId;
        this.summary = summary;
    }

    public long readId()
    {
        return readId;
    }

    public MutationSummary summary()
    {
        return summary;
    }

    public static final IVerbHandler<TrackedReadSummary> verbHandler = new IVerbHandler<TrackedReadSummary>()
    {
        @Override
        public void doVerb(Message<TrackedReadSummary> message) throws IOException
        {
            MutationTrackingService.instance.localReads().receiveSummary(message.from(), message.payload);
        }
    };

    public static final IVersionedSerializer<TrackedReadSummary> serializer = new IVersionedSerializer<TrackedReadSummary>()
    {
        @Override
        public void serialize(TrackedReadSummary summary, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(summary.readId);
            MutationSummary.serializer.serialize(summary.summary, out, version);
        }

        @Override
        public TrackedReadSummary deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TrackedReadSummary(in.readLong(),
                                          MutationSummary.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(TrackedReadSummary summary, int version)
        {
            return TypeSizes.LONG_SIZE + MutationSummary.serializer.serializedSize(summary.summary, version);
        }
    };
}
