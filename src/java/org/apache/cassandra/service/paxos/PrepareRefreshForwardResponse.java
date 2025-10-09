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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.NullableSerializer;

/**
 * Response from a forwarded PaxosPrepareRefresh operation.
 * Contains the superseding ballot for each refresh target (null if confirmed).
 */
public class PrepareRefreshForwardResponse
{
    public static final Serializer serializer = new Serializer();

    /**
     * List of superseding ballots, one per refresh target.
     * Null entry means the promise was confirmed for that target.
     */
    public final List<Ballot> supersededBy;

    public PrepareRefreshForwardResponse(List<Ballot> supersededBy)
    {
        this.supersededBy = supersededBy;
    }

    public static class Serializer implements IVersionedSerializer<PrepareRefreshForwardResponse>
    {
        private static final IVersionedSerializer<Ballot> NULLABLE_BALLOT_SERIALIZER = NullableSerializer.wrap(Ballot.Serializer.instance);

        @Override
        public void serialize(PrepareRefreshForwardResponse response, DataOutputPlus out, int version) throws IOException
        {
            CollectionSerializers.serializeList(response.supersededBy, out, version, NULLABLE_BALLOT_SERIALIZER);
        }

        @Override
        public PrepareRefreshForwardResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            List<Ballot> supersededBy = CollectionSerializers.deserializeList(in, version, NULLABLE_BALLOT_SERIALIZER);
            return new PrepareRefreshForwardResponse(supersededBy);
        }

        @Override
        public long serializedSize(PrepareRefreshForwardResponse response, int version)
        {
            return CollectionSerializers.serializedListSize(response.supersededBy, version, NULLABLE_BALLOT_SERIALIZER);
        }
    }
}
