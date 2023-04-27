/*
 * Licensed to the Apache Softwarea Foundation (ASF) under one
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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class Unregister implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(Unregister.class);
    public static final Serializer serializer = new Serializer();

    private final NodeId nodeId;
    public Unregister(NodeId nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public Kind kind()
    {
        return Kind.UNREGISTER;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        if (!prev.directory.peerIds().contains(nodeId))
            return new Rejected(String.format("Can not unregsiter %s since it is not present in the directory.", nodeId));

        ClusterMetadata.Transformer next = prev.transformer()
                                           .unregister(nodeId);

        return success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    @VisibleForTesting
    public static void register(NodeId nodeId)
    {
        ClusterMetadataService.instance()
                              .commit(new Unregister(nodeId),
                                      (metadata) -> metadata.directory.peerIds().contains(nodeId),
                                      (metadata) -> metadata,
                                      (metadata, reason) -> {
                                          throw new IllegalStateException("Can't unregister node: " + reason);
                                      });
    }

    public String toString()
    {
        return "Unregister{" +
               ", nodeId=" + nodeId +
               '}';
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, Unregister>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof Unregister;
            Unregister register = (Unregister)t;
            NodeId.serializer.serialize(register.nodeId, out, version);
        }

        public Unregister deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            return new Unregister(nodeId);
        }

        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof Unregister;
            Unregister unregister = (Unregister) t;
            return NodeId.serializer.serializedSize(unregister.nodeId, version);
        }
    }
}
