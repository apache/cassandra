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

import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.ClientState;

/**
 * Request to forward a CAS operation to a replica coordinator for tracked keyspaces.
 * Contains the essential information needed to execute the CAS operation on the remote coordinator.
 */
public class CasForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final String keyspaceName;
    public final String cfName;
    public final DecoratedKey key;
    public final ConsistencyLevel consistencyForPaxos;
    public final ConsistencyLevel consistencyForCommit;
    public final long nowInSeconds;
    public final ClientState clientState;
    public final CQL3CasRequest casRequest;  // The actual CAS request to forward

    public CasForwardRequest(String keyspaceName,
                             String cfName,
                             DecoratedKey key,
                             ConsistencyLevel consistencyForPaxos,
                             ConsistencyLevel consistencyForCommit,
                             long nowInSeconds,
                             ClientState clientState,
                             CQL3CasRequest casRequest)
    {
        this.keyspaceName = keyspaceName;
        this.cfName = cfName;
        this.key = key;
        this.consistencyForPaxos = consistencyForPaxos;
        this.consistencyForCommit = consistencyForCommit;
        this.nowInSeconds = nowInSeconds;
        this.clientState = clientState;
        this.casRequest = casRequest;
    }

    public static class Serializer implements IVersionedSerializer<CasForwardRequest>
    {
        @Override
        public void serialize(CasForwardRequest forwardRequest, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(forwardRequest.keyspaceName);
            out.writeUTF(forwardRequest.cfName);
            DecoratedKey.serializer.serialize(forwardRequest.key, out, version);
            out.writeByte(forwardRequest.consistencyForPaxos.code);
            out.writeByte(forwardRequest.consistencyForCommit.code);
            out.writeUnsignedVInt(forwardRequest.nowInSeconds);
            serializeClientState(forwardRequest.clientState, out);
            CQL3CasRequest.serializer.serialize(forwardRequest.casRequest, out, version);
        }

        @Override
        public CasForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspaceName = in.readUTF();
            String cfName = in.readUTF();
            DecoratedKey key = (DecoratedKey) DecoratedKey.serializer.deserialize(in, version);
            ConsistencyLevel consistencyForPaxos = ConsistencyLevel.fromCode(in.readUnsignedByte());
            ConsistencyLevel consistencyForCommit = ConsistencyLevel.fromCode(in.readUnsignedByte());
            long nowInSeconds = in.readUnsignedVInt();
            ClientState clientState = deserializeClientState(in);
            CQL3CasRequest casRequest = CQL3CasRequest.serializer.deserialize(in, version);

            return new CasForwardRequest(keyspaceName, cfName, key, consistencyForPaxos, consistencyForCommit,
                                         nowInSeconds, clientState, casRequest);
        }

        @Override
        public long serializedSize(CasForwardRequest forwardRequest, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(forwardRequest.keyspaceName);
            size += TypeSizes.sizeof(forwardRequest.cfName);
            size += DecoratedKey.serializer.serializedSize(forwardRequest.key, version);
            size += 1; // consistencyForPaxos.code
            size += 1; // consistencyForCommit.code
            size += TypeSizes.sizeofUnsignedVInt(forwardRequest.nowInSeconds);
            size += serializedSizeClientState(forwardRequest.clientState);
            size += CQL3CasRequest.serializer.serializedSize(forwardRequest.casRequest, version);
            return size;
        }

        private static final int IS_SUPER = 0x01;
        private static final int IS_INTERNAL = 0x02;
        private static final int APPLY_GUARDRAILS = 0x04;

        private static void serializeClientState(ClientState state, DataOutputPlus out) throws IOException
        {
            int flags = (state.isSuper() ? IS_SUPER : 0)
                      | (state.isInternal ? IS_INTERNAL : 0)
                      | (state.applyGuardrails() ? APPLY_GUARDRAILS : 0)
                      ;
            out.write(flags);
        }

        private static ClientState deserializeClientState(DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedByte();
            boolean isSuper = (flags & IS_SUPER) != 0;
            boolean isInternal = (flags & IS_INTERNAL) != 0;
            boolean applyGuardrails = (flags & APPLY_GUARDRAILS) != 0;
            return ClientState.forForwardedCalls(isInternal, applyGuardrails, isSuper);
        }

        private static long serializedSizeClientState(ClientState state)
        {
            return 1;
        }
    }
}