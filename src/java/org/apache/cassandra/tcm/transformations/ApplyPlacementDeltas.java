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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;

public abstract class ApplyPlacementDeltas implements Transformation
{
    protected final NodeId nodeId;
    protected final PlacementDeltas delta;
    protected final LockedRanges.Key lockKey;
    protected final boolean unlock;

    ApplyPlacementDeltas(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey, boolean unlock)
    {
        this.nodeId = nodeId;
        this.delta = delta;
        this.lockKey = lockKey;
        this.unlock = unlock;
    }

    public abstract Kind kind();
    public abstract ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer);

    public PlacementDeltas inverseDelta()
    {
        return delta.invert();
    }

    public PlacementDeltas delta()
    {
        return delta;
    }

    @Override
    public final Result execute(ClusterMetadata prev)
    {
        ClusterMetadata.Transformer next = prev.transformer();

        if (!delta.isEmpty())
            next = next.with(delta.apply(prev.placements));

        next = transform(prev, next);

        if (unlock)
            next = next.with(prev.lockedRanges.unlock(lockKey));

        return success(next, prev.lockedRanges.locked.get(lockKey));
    }

    @Override
    public String toString()
    {
        return getClass().getName() +
               "{" +
               "id=" + nodeId +
               ", delta=" + delta +
               '}';
    }

    public NodeId nodeId()
    {
        return nodeId;
    }

    abstract static class SerializerBase<T extends ApplyPlacementDeltas> implements AsymmetricMetadataSerializer<Transformation, T>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ApplyPlacementDeltas change = (T) t;
            NodeId.serializer.serialize(change.nodeId, out, version);
            PlacementDeltas.serializer.serialize(change.delta, out, version);
            LockedRanges.Key.serializer.serialize(change.lockKey, out, version);
        }

        public T deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            PlacementDeltas delta = PlacementDeltas.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
            return construct(nodeId, delta, lockKey);
        }

        public long serializedSize(Transformation t, Version version)
        {
            ApplyPlacementDeltas change = (T) t;

            return NodeId.serializer.serializedSize(change.nodeId, version) +
                   PlacementDeltas.serializer.serializedSize(change.delta, version) +
                   LockedRanges.Key.serializer.serializedSize(change.lockKey, version);
        }

        abstract T construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey);
    }
}
