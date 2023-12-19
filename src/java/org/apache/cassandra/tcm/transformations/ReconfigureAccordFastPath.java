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
import org.apache.cassandra.service.accord.AccordFastPath;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class ReconfigureAccordFastPath implements Transformation
{
    private final Node.Id node;
    private final AccordFastPath.Status status;
    private final long updateTimeMillis;
    private final long updateDelayMillis;

    public ReconfigureAccordFastPath(Node.Id node, AccordFastPath.Status status, long updateTimeMillis, long updateDelayMillis)
    {
        this.node = node;
        this.status = status;
        this.updateTimeMillis = updateTimeMillis;
        this.updateDelayMillis = updateDelayMillis;
    }

    public Kind kind()
    {
        return Kind.UPDATE_AVAILABILITY;
    }

    public Result execute(ClusterMetadata metadata)
    {
        try
        {
            return Transformation.success(metadata.transformer().withFastPathStatusSince(node, status, updateTimeMillis, updateDelayMillis), LockedRanges.AffectedRanges.EMPTY);
        }
        catch (InvalidRequestException e)
        {
            return new Rejected(ExceptionCode.INVALID, e.getMessage());
        }
    }

    public static final AsymmetricMetadataSerializer<Transformation, ReconfigureAccordFastPath> serializer = new AsymmetricMetadataSerializer<Transformation, ReconfigureAccordFastPath>()
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ReconfigureAccordFastPath update = (ReconfigureAccordFastPath) t;
            TopologySerializers.nodeId.serialize(update.node, out, version);
            AccordFastPath.Status.serializer.serialize(update.status, out, version);
            out.writeUnsignedVInt(update.updateTimeMillis);
            out.writeUnsignedVInt(update.updateDelayMillis);

        }

        public ReconfigureAccordFastPath deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new ReconfigureAccordFastPath(TopologySerializers.nodeId.deserialize(in, version),
                                          AccordFastPath.Status.serializer.deserialize(in, version),
                                          in.readUnsignedVInt(), in.readUnsignedVInt());
        }

        public long serializedSize(Transformation t, Version version)
        {
            ReconfigureAccordFastPath update = (ReconfigureAccordFastPath) t;
            return TopologySerializers.nodeId.serializedSize(update.node, version) +
                   AccordFastPath.Status.serializer.serializedSize(update.status, version) +
                   TypeSizes.sizeofUnsignedVInt(update.updateTimeMillis) +
                   TypeSizes.sizeofUnsignedVInt(update.updateDelayMillis);
        }
    };
}
