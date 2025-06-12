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

import java.util.Set;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.tcm.CMSMembership;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

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

        CMSMembership cms = prev.cmsMembership;
        if (cms.joiningMembers().contains(nodeId) || cms.fullMembers().contains(nodeId))
            return new Transformation.Rejected(INVALID, "Endpoint is already a member of CMS");

        ClusterMetadata.Transformer transformer = prev.transformer().startJoiningCMS(nodeId);

        Set<InetAddressAndPort> streamCandidates = prev.fullCMSMembers();
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
