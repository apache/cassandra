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

package org.apache.cassandra.service.paxos;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.utils.NullableSerializer;

/**
 * Response from a forwarded PaxosPrepareRefresh operation.
 * Each response carries a single target result (targetIndex + supersededBy),
 * or just the mutation ID if targetIndex is null.
 * Multiple non-final responses are sent incrementally as targets respond,
 * followed by a final response for the last target.
 */
public class PrepareRefreshForwardResponse
{
    public static final Serializer serializer = new Serializer();

    public final MutationId mutationId;
    @Nullable
    public final Integer targetIndex;
    @Nullable
    public final Ballot supersededBy;

    public PrepareRefreshForwardResponse(MutationId mutationId)
    {
        this.mutationId = mutationId;
        this.targetIndex = null;
        this.supersededBy = null;
    }

    public PrepareRefreshForwardResponse(MutationId mutationId, int targetIndex, @Nullable Ballot supersededBy)
    {
        this.mutationId = mutationId;
        this.targetIndex = targetIndex;
        this.supersededBy = supersededBy;
    }

    public static class Serializer implements IVersionedSerializer<PrepareRefreshForwardResponse>
    {
        private static final IVersionedSerializer<Ballot> NULLABLE_BALLOT_SERIALIZER = NullableSerializer.wrap(Ballot.Serializer.instance);

        @Override
        public void serialize(PrepareRefreshForwardResponse response, DataOutputPlus out, int version) throws IOException
        {
            MutationId.serializer.serialize(response.mutationId, out);
            boolean hasTarget = response.targetIndex != null;
            out.writeBoolean(hasTarget);
            if (hasTarget)
            {
                out.writeUnsignedVInt32(response.targetIndex);
                NULLABLE_BALLOT_SERIALIZER.serialize(response.supersededBy, out, version);
            }
        }

        @Override
        public PrepareRefreshForwardResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            MutationId mutationId = MutationId.serializer.deserialize(in);
            boolean hasTarget = in.readBoolean();
            if (!hasTarget)
                return new PrepareRefreshForwardResponse(mutationId);
            int targetIndex = in.readUnsignedVInt32();
            Ballot supersededBy = NULLABLE_BALLOT_SERIALIZER.deserialize(in, version);
            return new PrepareRefreshForwardResponse(mutationId, targetIndex, supersededBy);
        }

        @Override
        public long serializedSize(PrepareRefreshForwardResponse response, int version)
        {
            long size = MutationId.serializer.serializedSize(response.mutationId);
            size += 1; // hasTarget boolean
            if (response.targetIndex != null)
            {
                size += TypeSizes.sizeofUnsignedVInt(response.targetIndex);
                size += NULLABLE_BALLOT_SERIALIZER.serializedSize(response.supersededBy, version);
            }
            return size;
        }
    }
}
