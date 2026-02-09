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
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.membership.EndpointLookup;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class ReplicaNode
{
    public static final Serializer serializer = new Serializer();
    public final NodeId nodeId;
    public final Range<Token> range;
    private final boolean full;

    public ReplicaNode(NodeId nodeId, Range<Token> range, boolean full)
    {
        this.nodeId = nodeId;
        this.range = range;
        this.full = full;
    }

    public Replica toReplica(EndpointLookup endpointLookup)
    {
       return new Replica(endpointLookup.endpoint(nodeId), range, full);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ReplicaNode)) return false;
        ReplicaNode that = (ReplicaNode) o;
        return full == that.full && Objects.equals(nodeId, that.nodeId) && Objects.equals(range, that.range);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeId, range, full);
    }

    @Override
    public String toString()
    {
        return "ReplicaNode{" +
               "nodeId=" + nodeId +
               ", range=" + range +
               ", full=" + full +
               '}';
    }

    public static class Serializer implements MetadataSerializer<ReplicaNode>
    {
        @Override
        public void serialize(ReplicaNode t, DataOutputPlus out, Version version) throws IOException
        {
            NodeId.serializer.serialize(t.nodeId, out, version);
            Range.serializer.serialize(t.range, out, version);
            out.writeBoolean(t.full);
        }

        @Override
        public ReplicaNode deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId replicaNode = NodeId.serializer.deserialize(in, version);
            Range<Token> range = Range.serializer.deserialize(in, version);
            boolean full = in.readBoolean();
            return new ReplicaNode(replicaNode, range, full);
        }

        @Override
        public long serializedSize(ReplicaNode t, Version version)
        {
            long size = NodeId.serializer.serializedSize(t.nodeId, version);
            size += Range.serializer.serializedSize(t.range, version);
            size += TypeSizes.sizeof(t.full);
            return size;
        }
    }
}
