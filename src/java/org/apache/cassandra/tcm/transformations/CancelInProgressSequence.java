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
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class CancelInProgressSequence implements Transformation
{
    public static final Serializer serializer = new Serializer();

    private final NodeId nodeId;

    public CancelInProgressSequence(NodeId nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public Kind kind()
    {
        return Kind.CANCEL_SEQUENCE;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        InProgressSequence<?> sequence = prev.inProgressSequences.get(nodeId);
        if (null == sequence)
            return new Rejected(String.format("No in-progress sequence found for node id %s at epoch %s",
                                              nodeId, prev.epoch));

        // TODO unlock - requires IPS to have/provide locked ranges - or maybe this is just part of cancel?
        // Doesn't really need to remove the sequence here, could encapsulate that in cancel() - makes renaming
        // it more important b/c it's really a "recipe" or plan for reverting affects - maybe rename to
        // "revert(Applied)Effects"?
        ClusterMetadata.Transformer transformer = sequence.cancel(prev)
                                                          .with(prev.inProgressSequences.without(nodeId));
        return success(transformer, LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public String toString()
    {
        return "CancelInProgressSequence{" +
               "nodeId=" + nodeId +
               '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, CancelInProgressSequence>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof CancelInProgressSequence;
            CancelInProgressSequence cancel = (CancelInProgressSequence) t;
            NodeId.serializer.serialize(cancel.nodeId, out, version);
        }

        @Override
        public CancelInProgressSequence deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            return new CancelInProgressSequence(nodeId);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof CancelInProgressSequence;
            return NodeId.serializer.serializedSize(((CancelInProgressSequence)t).nodeId, version);
        }
    }
}
