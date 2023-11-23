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

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.EntireRange;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.tcm.ownership.EntireRange.entireRange;

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

        ReplicationParams metaParams = ReplicationParams.meta(prev);
        ClusterMetadata.Transformer transformer = prev.transformer();
        if (reconfigureCMS.next.activeTransition != null)
        {
            InetAddressAndPort pendingEndpoint = prev.directory.endpoint(reconfigureCMS.next.activeTransition.nodeId);
            Replica pendingReplica = new Replica(pendingEndpoint, entireRange, true);
            DataPlacement.Builder builder = prev.placements.get(metaParams).unbuild()
                                                           .withoutWriteReplica(prev.nextEpoch(), pendingReplica);

            DataPlacement placement = builder.build();
            if (!placement.reads.equals(placement.writes))
                return new Rejected(ExceptionCode.INVALID, String.format("Placements will be inconsistent if this transformation is applied:\nReads %s\nWrites: %s",
                                                                         placement.reads,
                                                                         placement.writes));

            transformer = transformer.with(prev.placements.unbuild().with(metaParams, placement).build());
        }

        return Transformation.success(transformer.with(prev.inProgressSequences.without(ReconfigureCMS.SequenceKey.instance))
                                                 .with(prev.lockedRanges.unlock(reconfigureCMS.next.lockKey)),
                                      EntireRange.affectedRanges(prev));
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
