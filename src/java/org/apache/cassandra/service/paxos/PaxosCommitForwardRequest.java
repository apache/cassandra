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
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Request to forward a Paxos V1 commit operation to a replica coordinator.
 * This is used when the original coordinator is not a replica but needs to
 * execute a Paxos commit for a tracked keyspace that requires MutationId generation.
 *
 * When {@code respondAfterSend} is true, the handler will respond immediately after
 * sending the commits to replicas without waiting for application. This is used
 * for the sendCommit path where we need to ensure commits were dispatched before
 * continuing with a prepare operation.
 */
public class PaxosCommitForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final Commit proposal;
    public final ConsistencyLevel consistencyLevel;
    /**
     * If true, the handler will respond immediately after sending the commits
     * to replicas without waiting for application to complete.
     */
    public final boolean respondAfterSend;

    public PaxosCommitForwardRequest(Commit proposal, ConsistencyLevel consistencyLevel)
    {
        this(proposal, consistencyLevel, false);
    }

    public PaxosCommitForwardRequest(Commit proposal, ConsistencyLevel consistencyLevel, boolean respondAfterSend)
    {
        this.proposal = proposal;
        this.consistencyLevel = consistencyLevel;
        this.respondAfterSend = respondAfterSend;
    }

    public static class Serializer implements IVersionedSerializer<PaxosCommitForwardRequest>
    {
        @Override
        public void serialize(PaxosCommitForwardRequest request, DataOutputPlus out, int version) throws IOException
        {
            Commit.serializer.serialize(request.proposal, out, version);
            out.write(request.consistencyLevel.code);
            out.writeBoolean(request.respondAfterSend);
        }

        @Override
        public PaxosCommitForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Commit proposal = Commit.serializer.deserialize(in, version);
            ConsistencyLevel consistencyLevel = ConsistencyLevel.fromCode(in.readUnsignedByte());
            boolean respondAfterSend = in.readBoolean();
            return new PaxosCommitForwardRequest(proposal, consistencyLevel, respondAfterSend);
        }

        @Override
        public long serializedSize(PaxosCommitForwardRequest request, int version)
        {
            return Commit.serializer.serializedSize(request.proposal, version)
                   + 1  // consistencyLevel
                   + 1; // respondAfterSend
        }
    }
}
