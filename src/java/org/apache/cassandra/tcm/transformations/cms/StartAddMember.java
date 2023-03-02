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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;

import static org.apache.cassandra.tcm.transformations.cms.EntireRange.affectedRanges;

public class StartAddMember extends BaseMembershipTransformation
{
    public static final AsymmetricMetadataSerializer<Transformation, StartAddMember> serializer = new SerializerBase<StartAddMember>()
    {
        public StartAddMember createTransformation(InetAddressAndPort addr)
        {
            return new StartAddMember(addr);
        }
    };

    public StartAddMember(InetAddressAndPort addr)
    {
        super(addr);
    }

    public Kind kind()
    {
        return Kind.START_ADD_TO_CMS;
    }

    public Result execute(ClusterMetadata prev)
    {
        RangesByEndpoint readReplicas = prev.placements.get(ReplicationParams.meta()).reads.byEndpoint();
        RangesByEndpoint writeReplicas = prev.placements.get(ReplicationParams.meta()).writes.byEndpoint();

        if (readReplicas.containsKey(endpoint) || writeReplicas.containsKey(endpoint))
            return new Rejected("Endpoint is already a member of the Cluster Metadata ownership set");

        ClusterMetadata.Transformer transformer = prev.transformer();
        DataPlacement.Builder builder = prev.placements.get(ReplicationParams.meta()).unbuild()
                                                       .withWriteReplica(replica);

        transformer.with(prev.placements.unbuild().with(ReplicationParams.meta(), builder.build()).build());

        List<InetAddressAndPort> streamCandidates = new ArrayList<>();
        for (Replica replica : prev.placements.get(ReplicationParams.meta()).reads.byEndpoint().flattenValues())
        {
            if (!this.replica.equals(replica))
                streamCandidates.add(replica.endpoint());
        }

        ProgressBarrier barrier = new ProgressBarrier(prev.nextEpoch(), affectedRanges.toPeers(prev.placements, prev.directory), false);
        AddToCMS joinSequence = new AddToCMS(barrier, streamCandidates, new FinishAddMember(endpoint));

        return success(transformer.with(prev.inProgressSequences.with(prev.directory.peerId(replica.endpoint()), joinSequence)),
                       affectedRanges);
    }
}
