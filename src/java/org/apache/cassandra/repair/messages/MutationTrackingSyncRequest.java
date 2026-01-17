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
package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;

import static org.apache.cassandra.utils.CollectionSerializers.nullableIntSetSerializer;

/**
 * Request sent from the mutation tracking repair coordinator to each participant to collect
 * their current witnessed offsets. This establishes a happens-before relationship: the
 * participant's response contains offsets captured after receiving this request, which is
 * sent after the repair starts.
 * <p>
 * The liveHostIds set tells the responder which hosts are participating in this repair,
 * so that the response only includes offsets witnessed by those hosts. This prevents the
 * coordinator from setting sync targets that include offsets only known to down nodes.
 */
public class MutationTrackingSyncRequest extends RepairMessage
{
    /** The set of host IDs participating in this repair. Null means all replicas. */
    public final Set<Integer> liveHostIds;

    public MutationTrackingSyncRequest(RepairJobDesc desc, Set<Integer> liveHostIds)
    {
        super(desc);
        this.liveHostIds = liveHostIds;
    }

    @Override
    public String toString()
    {
        return "MutationTrackingSyncRequest{" +
               "desc=" + desc +
               ", liveHostIds=" + liveHostIds +
               '}';
    }

    public static final IVersionedSerializer<MutationTrackingSyncRequest> serializer = new IVersionedSerializer<>()
    {
        public void serialize(MutationTrackingSyncRequest request, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(request.desc, out, version);
            nullableIntSetSerializer.serialize(request.liveHostIds, out);
        }

        public MutationTrackingSyncRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            Set<Integer> liveHostIds = nullableIntSetSerializer.deserialize(in);
            return new MutationTrackingSyncRequest(desc, liveHostIds);
        }

        public long serializedSize(MutationTrackingSyncRequest request, int version)
        {
            return RepairJobDesc.serializer.serializedSize(request.desc, version)
                   + nullableIntSetSerializer.serializedSize(request.liveHostIds);
        }
    };
}
