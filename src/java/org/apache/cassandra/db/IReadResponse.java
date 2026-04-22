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
package org.apache.cassandra.db;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.tracked.TrackedDataResponse;
import org.apache.cassandra.service.reads.tracked.TrackedSummaryResponse;

import static org.apache.cassandra.db.ReadKind.UNTRACKED;

public interface IReadResponse
{
    ReadKind kind();

    IVersionedSerializer<IReadResponse> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(IReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            if (version >= MessagingService.VERSION_61)
                ReadKind.serializer.serialize(response.kind(), out);
            else
                Preconditions.checkArgument(response.kind() == UNTRACKED);

            switch (response.kind())
            {
                case UNTRACKED:
                    ReadResponse.serializer.serialize((ReadResponse) response, out, version);
                    break;
                case TRACKED_DATA:
                    TrackedDataResponse.embedded.serialize((TrackedDataResponse) response, out, version);
                    break;
                case TRACKED_SUMMARY:
                    TrackedSummaryResponse.embedded.serialize((TrackedSummaryResponse) response, out, version);
                    break;
                default:
                    throw new IllegalStateException("Unhandled kind: " + response.kind());
            }
        }

        @Override
        public IReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {

            ReadKind kind = version >= MessagingService.VERSION_61 ? ReadKind.serializer.deserialize(in) : UNTRACKED;
            switch (kind)
            {
                case UNTRACKED:
                    return ReadResponse.serializer.deserialize(in, version);
                case TRACKED_DATA:
                    return TrackedDataResponse.embedded.deserialize(in, version);
                case TRACKED_SUMMARY:
                    return TrackedSummaryResponse.embedded.deserialize(in, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + kind);
            }
        }

        @Override
        public long serializedSize(IReadResponse response, int version)
        {
            long size = 0;

            if (version >= MessagingService.VERSION_61)
                size += ReadKind.serializer.serializedSize(response.kind());
            else
                Preconditions.checkArgument(response.kind() == UNTRACKED);

            switch (response.kind())
            {
                case UNTRACKED:
                    return size + ReadResponse.serializer.serializedSize((ReadResponse) response, version);
                case TRACKED_DATA:
                    return size + TrackedDataResponse.embedded.serializedSize((TrackedDataResponse) response, version);
                case TRACKED_SUMMARY:
                    return size + TrackedSummaryResponse.embedded.serializedSize((TrackedSummaryResponse) response, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + response.kind());
            }
        }
    };
}
