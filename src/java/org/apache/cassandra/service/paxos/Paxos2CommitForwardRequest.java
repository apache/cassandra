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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.service.paxos.Commit.Agreed;

/**
 * Request to forward a Paxos V2 commit operation to a replica coordinator.
 * This is used when the original coordinator is not a replica but needs to
 * execute a Paxos commit for a tracked keyspace that requires MutationId generation.
 * 
 * Contains only the essential data needed by PaxosCommit instead of the full Participants object.
 */
public class Paxos2CommitForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final Agreed commit;
    public final ConsistencyLevel consistencyForConsensus;
    public final ConsistencyLevel consistencyForCommit;
    public final EndpointsForToken all;
    public final EndpointsForToken allLive;
    public final EndpointsForToken allDown;
    public final int required;
    public final boolean isUrgent;

    public Paxos2CommitForwardRequest(Agreed commit,
                                     ConsistencyLevel consistencyForConsensus,
                                     ConsistencyLevel consistencyForCommit,
                                     EndpointsForToken all,
                                     EndpointsForToken allLive,
                                     EndpointsForToken allDown,
                                     int required,
                                     boolean isUrgent)
    {
        this.commit = commit;
        this.consistencyForConsensus = consistencyForConsensus;
        this.consistencyForCommit = consistencyForCommit;
        this.all = all;
        this.allLive = allLive;
        this.allDown = allDown;
        this.required = required;
        this.isUrgent = isUrgent;
    }

    public static class Serializer implements IVersionedSerializer<Paxos2CommitForwardRequest>
    {
        @Override
        public void serialize(Paxos2CommitForwardRequest request, DataOutputPlus out, int version) throws IOException
        {
            Agreed.serializer.serialize(request.commit, out, version);
            out.writeByte(request.consistencyForConsensus.code);
            out.writeByte(request.consistencyForCommit.code);

            EndpointsForToken.serializer.serialize(request.all, out, version);
            EndpointsForToken.serializer.serialize(request.allLive, out, version);
            EndpointsForToken.serializer.serialize(request.allDown, out, version);

            out.writeUnsignedVInt32(request.required);
            out.writeBoolean(request.isUrgent);
        }

        @Override
        public Paxos2CommitForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Agreed commit = Agreed.serializer.deserialize(in, version);
            ConsistencyLevel consistencyForConsensus = ConsistencyLevel.fromCode(in.readUnsignedByte());
            ConsistencyLevel consistencyForCommit = ConsistencyLevel.fromCode(in.readUnsignedByte());

            IPartitioner partitioner = commit.metadata().partitioner;
            EndpointsForToken all = EndpointsForToken.serializer.deserialize(in, partitioner, version);
            EndpointsForToken allLive = EndpointsForToken.serializer.deserialize(in, partitioner, version);
            EndpointsForToken allDown = EndpointsForToken.serializer.deserialize(in, partitioner, version);

            int required = in.readUnsignedVInt32();
            boolean isUrgent = in.readBoolean();

            return new Paxos2CommitForwardRequest(commit, consistencyForConsensus, consistencyForCommit,
                                                  all, allLive, allDown, required, isUrgent);
        }

        @Override
        public long serializedSize(Paxos2CommitForwardRequest request, int version)
        {
            long size = Agreed.serializer.serializedSize(request.commit, version)
                        + 1  // consistencyForConsensus.code
                        + 1; // consistencyForCommit.code

            size += EndpointsForToken.serializer.serializedSize(request.all, version);
            size += EndpointsForToken.serializer.serializedSize(request.allLive, version);
            size += EndpointsForToken.serializer.serializedSize(request.allDown, version);

            size += TypeSizes.sizeofUnsignedVInt(request.required);
            size += TypeSizes.BOOL_SIZE; // isUrgent

            return size;
        }
    }
}