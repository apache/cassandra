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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.accord.topology.FastPathStrategy;
import org.apache.cassandra.tcm.CMSMembership;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class CancelCMSReconfiguration implements Transformation
{
    public static final Serializer serializer = new Serializer();

    public static final CancelCMSReconfiguration instance = new CancelCMSReconfiguration();
    private CancelCMSReconfiguration()
    {
    }

    @Override
    public Kind kind()
    {
        return Kind.CANCEL_CMS_RECONFIGURATION;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        ReconfigureCMS reconfigureCMS = (ReconfigureCMS) prev.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance);
        if (reconfigureCMS == null)
            return new Rejected(ExceptionCode.INVALID, "Can not cancel reconfiguration since there does not seem to be any in-flight");

        ClusterMetadata.Transformer transformer = prev.transformer();
        // Reset any partially completed transition by removing the pending replica from the write group
        if (reconfigureCMS.next.activeTransition != null)
        {
            // see what the placements for the meta keyspace will be after cancelling the active join
            CMSMembership cms = prev.cmsMembership.cancelJoining(reconfigureCMS.next.activeTransition.nodeId);
            if (!cms.joiningMembers().isEmpty())
                return new Rejected(ExceptionCode.INVALID,
                                    String.format("Placements will be inconsistent if this transformation is applied:" +
                                                  "\nFull members %s\nJoining members: %s",
                                                  cms.fullMembers(),
                                                  cms.joiningMembers()));

            // if all is good, actually cancel the joining member
            transformer = transformer.cancelJoiningCMS(reconfigureCMS.next.activeTransition.nodeId);
            // Recalculate the replication params for the meta keyspace based on the actual placement as it will be
            ReplicationParams recalculated = getAccurateReplication(prev.directory, cms);

            // If they no longer match the replication params in schema, i.e. the transitions completed so far did not
            // bring the membership/placements into line with configuration, update schema to match what we actually have
            if (!recalculated.equals(ReplicationParams.meta(prev)))
            {
                KeyspaceMetadata keyspace = prev.schema.getKeyspaceMetadata(SchemaConstants.METADATA_KEYSPACE_NAME);
                KeyspaceMetadata newKeyspace = keyspace.withSwapped(new KeyspaceParams(keyspace.params.durableWrites,
                                                                                       recalculated,
                                                                                       FastPathStrategy.simple()));
                transformer = transformer.with(new DistributedSchema(prev.schema.getKeyspaces().withAddedOrUpdated(newKeyspace)));
            }
        }

        return Transformation.success(transformer.with(prev.inProgressSequences.without(ReconfigureCMS.SequenceKey.instance))
                                                 .with(prev.lockedRanges.unlock(reconfigureCMS.next.lockKey)),
                                      MetaStrategy.affectedRanges(prev));
    }

    private ReplicationParams getAccurateReplication(Directory directory, CMSMembership membership)
    {
        Map<String, Integer> replicasPerDc = new HashMap<>();
        membership.fullMembers().forEach(id -> {
            String dc = directory.location(id).datacenter;
            int count = replicasPerDc.getOrDefault(dc, 0);
            replicasPerDc.put(dc, ++count);
        });
        return ReplicationParams.ntsMeta(replicasPerDc);
    }

    @Override
    public String toString()
    {
        return "CancelCMSReconfiguration{}";
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, CancelCMSReconfiguration>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
        }

        public CancelCMSReconfiguration deserialize(DataInputPlus in, Version version) throws IOException
        {
            return instance;
        }

        public long serializedSize(Transformation t, Version version)
        {
            return 0;
        }
    }
}
