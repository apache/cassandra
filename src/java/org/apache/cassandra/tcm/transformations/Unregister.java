/*
 * Licensed to the Apache Softwarea Foundation (ASF) under one
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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.EnumSet;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class Unregister implements Transformation
{
    public static final Serializer serializer = new Serializer();

    private final NodeId nodeId;
    private final EnumSet<NodeState> allowedNodeStartStates;

    public Unregister(NodeId nodeId, EnumSet<NodeState> allowedNodeStartStates)
    {
        this.nodeId = nodeId;
        this.allowedNodeStartStates = allowedNodeStartStates;
    }

    @Override
    public Kind kind()
    {
        return Kind.UNREGISTER;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        if (!prev.directory.peerIds().contains(nodeId))
            return new Rejected(INVALID, String.format("Can not unregister %s since it is not present in the directory.", nodeId));

        NodeState startState = prev.directory.peerState(nodeId);
        if (!allowedNodeStartStates.contains(startState))
            return new Transformation.Rejected(INVALID, "Can't unregister " + nodeId + " - node state is " + startState + " not " + allowedNodeStartStates);

        ClusterMetadata.Transformer next = prev.transformer().unregister(nodeId);

        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    /**
     * unsafe, only for test use
     */
    @VisibleForTesting
    public static void unregister(NodeId nodeId)
    {
        ClusterMetadataService.instance()
                              .commit(new Unregister(nodeId, EnumSet.allOf(NodeState.class)));
    }

    public String toString()
    {
        return "Unregister{" +
               ", nodeId=" + nodeId +
               '}';
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, Unregister>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof Unregister;
            Unregister register = (Unregister)t;
            if (version.isAtLeast(Version.V2))
            {
                out.writeUnsignedVInt32(register.allowedNodeStartStates.size());
                for (NodeState allowedState : register.allowedNodeStartStates)
                    out.writeUTF(allowedState.name());
            }
            NodeId.serializer.serialize(register.nodeId, out, version);
        }

        public Unregister deserialize(DataInputPlus in, Version version) throws IOException
        {
            EnumSet<NodeState> states = EnumSet.noneOf(NodeState.class);
            if (version.isAtLeast(Version.V2))
            {
                int startStateSize = in.readUnsignedVInt32();
                for (int i = 0; i < startStateSize; i++)
                    states.add(NodeState.valueOf(in.readUTF()));
            }
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            return new Unregister(nodeId, version.isAtLeast(Version.V2) ? states : EnumSet.allOf(NodeState.class));
        }

        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof Unregister;
            Unregister unregister = (Unregister) t;
            long size = 0;
            if (version.isAtLeast(Version.V2))
            {
                size += TypeSizes.sizeofUnsignedVInt(unregister.allowedNodeStartStates.size());
                for (NodeState state : unregister.allowedNodeStartStates)
                    size += TypeSizes.sizeof(state.name());
            }
            size += NodeId.serializer.serializedSize(unregister.nodeId, version);
            return size;
        }
    }
}
