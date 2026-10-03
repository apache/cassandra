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

import accord.local.Node;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.AccordNodeInfo.Delta;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class AccordChangeNodeInfo implements Transformation
{
    private final Node.Id node;
    private final Delta delta;
    private final long updatedMillis;

    public AccordChangeNodeInfo(Node.Id node, Delta delta, long updatedMillis)
    {
        this.node = node;
        this.delta = delta;
        this.updatedMillis = updatedMillis;
    }

    public Kind kind()
    {
        return Kind.UPDATE_ACCORD_NODE_INFO;
    }

    @Override
    public boolean eligibleToCommit(ClusterMetadata metadata)
    {
        return AccordNodeInfos.supportsExtendedNodeInfo(metadata);
    }

    public Result execute(ClusterMetadata metadata)
    {
        // rejections are safe here, as they fire before we commit to the new epoch and prevent a no-op epoch being issued
        if (delta.hasNoEffectUpon(metadata.accordNodeInfos.getOrDefault(node)))
            return new Rejected(ExceptionCode.INVALID, String.format("%s already records %s", node, delta));

        try
        {
            return Transformation.success(metadata.transformer().withAccordNodeInfo(node, delta, updatedMillis), LockedRanges.AffectedRanges.EMPTY);
        }
        catch (InvalidRequestException e)
        {
            return new Rejected(ExceptionCode.INVALID, e.getMessage());
        }
    }

    @Override
    public String toString()
    {
        return "AccordChangeNodeInfo{" +
               "node=" + node +
               ", delta=" + delta +
               ", updatedMillis=" + updatedMillis +
               '}';
    }

    public static final AsymmetricMetadataSerializer<Transformation, AccordChangeNodeInfo> serializer = new AsymmetricMetadataSerializer<Transformation, AccordChangeNodeInfo>()
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AccordChangeNodeInfo update = (AccordChangeNodeInfo) t;
            TopologySerializers.nodeId.serialize(update.node, out);
            Delta.serializer.serialize(update.delta, out, version);
            out.writeUnsignedVInt(update.updatedMillis);
        }

        public AccordChangeNodeInfo deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordChangeNodeInfo(TopologySerializers.nodeId.deserialize(in),
                                            Delta.serializer.deserialize(in, version),
                                            in.readUnsignedVInt());
        }

        public long serializedSize(Transformation t, Version version)
        {
            AccordChangeNodeInfo update = (AccordChangeNodeInfo) t;
            return TopologySerializers.nodeId.serializedSize(update.node) +
                   Delta.serializer.serializedSize(update.delta, version) +
                   TypeSizes.sizeofUnsignedVInt(update.updatedMillis);
        }
    };
}
