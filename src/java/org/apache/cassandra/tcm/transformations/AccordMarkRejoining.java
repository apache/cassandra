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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordTopology;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CollectionSerializers;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class AccordMarkRejoining implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMarkRejoining.class);
    
    private final Set<NodeId> ids;

    public AccordMarkRejoining(Set<NodeId> ids)
    {
        this.ids = ids;
    }

    @Override
    public Kind kind()
    {
        return Kind.ACCORD_MARK_REJOINING;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        for (NodeId id : ids)
            if (!prev.directory.peerIds().contains(id))
                return new Rejected(INVALID, String.format("Can not unmark node %s as it is not present in the directory.", id));

        Set<Node.Id> accordIds = ids.stream().map(AccordTopology::tcmIdToAccord).collect(Collectors.toSet());

        for (Node.Id id : accordIds)
            if (!prev.accordStaleReplicas.contains(id))
                return new Rejected(INVALID, String.format("Can not unmark node %s as it is not stale.", id));

        logger.info("Unmarking " + ids + ". They will now participate in durability status coordination...");
        ClusterMetadata.Transformer next = prev.transformer().unmarkStaleReplicas(accordIds);
        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public String toString()
    {
        return "AccordMarkRejoining{ids=" + ids + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordMarkRejoining that = (AccordMarkRejoining) o;
        return Objects.equals(ids, that.ids);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ids);
    }

    public static final AsymmetricMetadataSerializer<Transformation, AccordMarkRejoining> serializer = new AsymmetricMetadataSerializer<>()
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof AccordMarkRejoining;
            AccordMarkRejoining mark = (AccordMarkRejoining) t;
            CollectionSerializers.serializeCollection(mark.ids, out, version, NodeId.serializer);
        }

        @Override
        public AccordMarkRejoining deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordMarkRejoining(CollectionSerializers.deserializeSet(in, version, NodeId.serializer));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof AccordMarkRejoining;
            AccordMarkRejoining mark = (AccordMarkRejoining) t;
            return CollectionSerializers.serializedCollectionSize(mark.ids, version, NodeId.serializer);
        }
    };
}
