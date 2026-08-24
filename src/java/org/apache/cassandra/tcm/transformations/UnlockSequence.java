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

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class UnlockSequence implements Transformation
{
    public static final Serializer serializer = new Serializer();

    private final NodeId nodeId;
    private final LockedRanges.Key lockKey;

    public UnlockSequence(NodeId nodeId, LockedRanges.Key lockKey)
    {
        this.nodeId = nodeId;
        this.lockKey = lockKey;
    }

    static boolean isSupportedBy(ClusterMetadata metadata)
    {
        return metadata.directory.commonSerializationVersion.isAtLeast(Kind.UNLOCK_SEQUENCE.introducedIn);
    }

    @Override
    public Kind kind()
    {
        return Kind.UNLOCK_SEQUENCE;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        if (!prev.inProgressSequences.contains(nodeId))
            return new Rejected(ExceptionCode.INVALID, "Can't find an in-progress sequence for this operation");

        if (prev.inProgressSequences.get(nodeId).nextStep() != kind())
            return new Rejected(ExceptionCode.INVALID, String.format("Can't commit sequenced operations out of order. Expected %s, but got %s", prev.inProgressSequences.get(nodeId).nextStep(), kind()));

        ClusterMetadata.Transformer next =
        prev.transformer()
            .with(prev.inProgressSequences.without(nodeId))
            .with(prev.lockedRanges.unlock(lockKey));

        return Transformation.success(next, prev.lockedRanges.locked.get(lockKey));
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '{' + "id=" + nodeId + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof UnlockSequence)) return false;
        UnlockSequence that = (UnlockSequence) o;
        return this.nodeId.equals(that.nodeId) && this.lockKey.equals(that.lockKey);
    }

    @Override
    public int hashCode()
    {
        return nodeId.hashCode() + 31 * lockKey.hashCode();
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, UnlockSequence>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            UnlockSequence unlock = (UnlockSequence) t;
            NodeId.serializer.serialize(unlock.nodeId, out, version);
            LockedRanges.Key.serializer.serialize(unlock.lockKey, out, version);
        }

        @Override
        public UnlockSequence deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
            return new UnlockSequence(nodeId, lockKey);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            UnlockSequence unlock = (UnlockSequence) t;
            return NodeId.serializer.serializedSize(unlock.nodeId, version) +
                   LockedRanges.Key.serializer.serializedSize(unlock.lockKey, version);
        }
    }
}
