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

package org.apache.cassandra.tcm.transformations.cms;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.locator.MetaStrategy.entireRange;

/**
 * This class along with AddToCMS, StartAddToCMS & RemoveFromCMS, contain a high degree of duplication with their intended
 * replacements ReconfigureCMS and AdvanceCMSReconfiguration. This shouldn't be a big problem as the intention is to
 * remove this superceded version asap.
 * @deprecated in favour of ReconfigureCMS
 */
@Deprecated(since = "CEP-21")
public class FinishAddToCMS extends BaseMembershipTransformation
{
    public static final AsymmetricMetadataSerializer<Transformation, FinishAddToCMS> serializer = new SerializerBase<FinishAddToCMS>()
    {
        public FinishAddToCMS createTransformation(InetAddressAndPort addr)
        {
            return new FinishAddToCMS(addr);
        }
    };

    public FinishAddToCMS(InetAddressAndPort addr)
    {
        super(addr);
    }

    @Override
    public Kind kind()
    {
        return Kind.FINISH_ADD_TO_CMS;
    }

    public Replica replicaForStreaming()
    {
        return replica;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        InProgressSequences sequences = prev.inProgressSequences;
        NodeId targetNode = prev.directory.peerId(replica.endpoint());
        MultiStepOperation<?> sequence = sequences.get(targetNode);

        if (sequence == null)
            return new Rejected(INVALID, "Can't execute finish join as cluster metadata does not hold join sequence for this node");

        if (!(sequence instanceof AddToCMS))
            return new Rejected(INVALID, "Can't execute finish join as cluster metadata contains a sequence of a different kind");

        ReplicationParams metaParams = ReplicationParams.meta(prev);
        InetAddressAndPort endpoint = prev.directory.endpoint(targetNode);
        Replica replica = new Replica(endpoint, entireRange, true);

        ClusterMetadata.Transformer transformer = prev.transformer();
        DataPlacement.Builder builder = prev.placements.get(metaParams)
                                                       .unbuild()
                                                       .withReadReplica(prev.nextEpoch(), replica);
        transformer = transformer.with(prev.placements.unbuild().with(metaParams, builder.build()).build())
                                 .with(prev.inProgressSequences.without(targetNode));
        return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
    }

    public String toString()
    {
        return "FinishAddMember{" +
               "endpoint=" + endpoint +
               ", replica=" + replica +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return super.equals(o);
    }
}