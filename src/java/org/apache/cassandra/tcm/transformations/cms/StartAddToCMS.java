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

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.locator.MetaStrategy.entireRange;

/**
 * This class along with AddToCMS, FinishAddToCMS & RemoveFromCMS, contain a high degree of duplication with their intended
 * replacements ReconfigureCMS and AdvanceCMSReconfiguration. This shouldn't be a big problem as the intention is to
 * remove this superceded version asap.
 * @deprecated in favour of ReconfigureCMS
 */
@Deprecated(since = "CEP-21")
public class StartAddToCMS extends BaseMembershipTransformation
{
    public static final AsymmetricMetadataSerializer<Transformation, StartAddToCMS> serializer = new SerializerBase<StartAddToCMS>()
    {
        public StartAddToCMS createTransformation(InetAddressAndPort addr)
        {
            return new StartAddToCMS(addr);
        }
    };

    public StartAddToCMS(InetAddressAndPort addr)
    {
        super(addr);
    }

    @Override
    public Kind kind()
    {
        return Kind.START_ADD_TO_CMS;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        NodeId nodeId = prev.directory.peerId(endpoint);
        MultiStepOperation<?> sequence = prev.inProgressSequences.get(nodeId);
        if (sequence != null)
            return new Rejected(INVALID, String.format("Cannot add node to CMS, since it already has an active in-progress sequence %s", sequence));
        if (prev.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance) != null)
            return new Rejected(INVALID, String.format("Cannot add node to CMS as a CMS reconfiguration is currently active"));

        Replica replica = new Replica(endpoint, entireRange, true);
        ReplicationParams metaParams = ReplicationParams.meta(prev);
        RangesByEndpoint readReplicas = prev.placements.get(metaParams).reads.byEndpoint();
        RangesByEndpoint writeReplicas = prev.placements.get(metaParams).writes.byEndpoint();

        if (readReplicas.containsKey(endpoint) || writeReplicas.containsKey(endpoint))
            return new Transformation.Rejected(INVALID, "Endpoint is already a member of CMS");

        ClusterMetadata.Transformer transformer = prev.transformer();
        DataPlacement.Builder builder = prev.placements.get(metaParams).unbuild()
                                                       .withWriteReplica(prev.nextEpoch(), replica);

        transformer.with(prev.placements.unbuild().with(metaParams, builder.build()).build());

        Set<InetAddressAndPort> streamCandidates = new HashSet<>();
        for (Replica r : prev.placements.get(metaParams).reads.byEndpoint().flattenValues())
        {
            if (!replica.equals(r))
                streamCandidates.add(r.endpoint());
        }

        AddToCMS joinSequence = new AddToCMS(prev.nextEpoch(), nodeId, streamCandidates, new FinishAddToCMS(endpoint));
        transformer = transformer.with(prev.inProgressSequences.with(nodeId, joinSequence));
        return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
    }

    @Override
    public String toString()
    {
        return "StartAddToCMS{" +
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
