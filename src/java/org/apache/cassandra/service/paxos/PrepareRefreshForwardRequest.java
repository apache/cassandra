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
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.utils.CollectionSerializers;

/**
 * Request to forward a PaxosPrepareRefresh operation to a full replica coordinator.
 * This is used when the original coordinator is not a full replica but needs to
 * execute a Paxos prepare refresh for a tracked keyspace that requires MutationId generation.
 *
 * The full replica coordinator will generate the mutation ID and send the refresh
 * to all target nodes with the same mutation ID.
 */
public class PrepareRefreshForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final Ballot promised;
    public final Committed commit;
    public final List<InetAddressAndPort> refreshTargets;
    public final boolean isUrgent;

    public PrepareRefreshForwardRequest(Ballot promised, Committed commit, List<InetAddressAndPort> refreshTargets, boolean isUrgent)
    {
        this.promised = promised;
        this.commit = commit;
        this.refreshTargets = refreshTargets;
        this.isUrgent = isUrgent;
    }

    public static class Serializer implements IVersionedSerializer<PrepareRefreshForwardRequest>
    {
        @Override
        public void serialize(PrepareRefreshForwardRequest request, DataOutputPlus out, int version) throws IOException
        {
            request.promised.serialize(out);
            Committed.serializer.serialize(request.commit, out, version);
            CollectionSerializers.serializeList(request.refreshTargets, out, version, InetAddressAndPort.Serializer.inetAddressAndPortSerializer);
            out.writeBoolean(request.isUrgent);
        }

        @Override
        public PrepareRefreshForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Ballot promised = Ballot.deserialize(in);
            Committed commit = Committed.serializer.deserialize(in, version);
            List<InetAddressAndPort> refreshTargets = CollectionSerializers.deserializeList(in, version, InetAddressAndPort.Serializer.inetAddressAndPortSerializer);
            boolean isUrgent = in.readBoolean();

            return new PrepareRefreshForwardRequest(promised, commit, refreshTargets, isUrgent);
        }

        @Override
        public long serializedSize(PrepareRefreshForwardRequest request, int version)
        {
            long size = Ballot.sizeInBytes();
            size += Committed.serializer.serializedSize(request.commit, version);
            size += CollectionSerializers.serializedListSize(request.refreshTargets, version, InetAddressAndPort.Serializer.inetAddressAndPortSerializer);
            size += TypeSizes.BOOL_SIZE; // isUrgent

            return size;
        }
    }
}
