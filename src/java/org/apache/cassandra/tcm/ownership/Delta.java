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

package org.apache.cassandra.tcm.ownership;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.tcm.membership.EndpointLookup;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public interface Delta
{
    Serializer serializer = new Serializer();

    Delta merge(Delta other);
    Delta invert();
    boolean isEmpty();

    /**
     * Required since we still encode the placements with endpoints
     */
    EndpointDelta asEndpointDelta(EndpointLookup endpointLookup);
    RangesByEndpoint removals(EndpointLookup endpointLookup);
    RangesByEndpoint additions(EndpointLookup endpointLookup);
    Collection<Range<Token>> addedRanges();
    Collection<Range<Token>> removedRanges();
    Set<NodeId> allPeers(Function<InetAddressAndPort, NodeId> nodeIdLookup);

    class Serializer implements MetadataSerializer<Delta>
    {
        @Override
        public void serialize(Delta t, DataOutputPlus out, Version version) throws IOException
        {
            if (version.isBefore(Version.V9))
            {
                if (!(t instanceof EndpointDelta))
                    throw new IllegalStateException("Serialization version is before V9, can't serialize node id deltas");
                EndpointDelta.serializer.serialize((EndpointDelta)t, out, version);
            }
            else
            {
                // We might serialize EndpointDeltas even on V9 when we serve a catchup request and
                // read back the log - if there are Deltas at V8 in the log, they are reserialized to V9 when
                // sending them to the requesting peer.
                if (t instanceof NodeIdDelta)
                {
                    out.writeBoolean(true);
                    NodeIdDelta.serializer.serialize((NodeIdDelta)t, out, version);
                }
                else
                {
                    out.writeBoolean(false);
                    EndpointDelta.serializer.serialize((EndpointDelta)t, out, version);
                }
            }
        }

        @Override
        public Delta deserialize(DataInputPlus in, Version version) throws IOException
        {
            if (version.isBefore(Version.V9))
                return EndpointDelta.serializer.deserialize(in, version);

            if (in.readBoolean())
                return NodeIdDelta.serializer.deserialize(in, version);
            else
                return EndpointDelta.serializer.deserialize(in, version);
        }

        @Override
        public long serializedSize(Delta t, Version version)
        {
            if (version.isBefore(Version.V9))
            {
                if (!(t instanceof EndpointDelta))
                    throw new IllegalStateException("Serialization version is before V9, can't serialize node id deltas");
                return EndpointDelta.serializer.serializedSize((EndpointDelta)t, version);
            }
            else
            {
                long size = TypeSizes.BOOL_SIZE;
                if (t instanceof NodeIdDelta)
                    size += NodeIdDelta.serializer.serializedSize((NodeIdDelta)t, version);
                else
                    size += EndpointDelta.serializer.serializedSize((EndpointDelta)t, version);
                return size;
            }
        }
    }
}
