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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.topology.AccordTopology;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CollectionSerializers;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class AccordMarkHardRemoved implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMarkHardRemoved.class);

    private final Set<NodeId> ids;
    private final boolean force;

    public AccordMarkHardRemoved(Set<NodeId> ids, boolean force)
    {
        this.ids = ids;
        this.force = force;
    }

    @Override
    public Kind kind()
    {
        return Kind.ACCORD_MARK_HARD_REMOVED;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        for (NodeId id : ids)
        {
            if (!prev.directory.peerIds().contains(id))
                continue;

            if (!force)
            {
                boolean removing = false;
                for (MultiStepOperation<?> operation : prev.inProgressSequences)
                {
                    switch (operation.kind())
                    {
                        case REPLACE:
                            removing |= id.equals(((BootstrapAndReplace) operation).finishReplace.replaced);
                            continue;
                        case REMOVE:
                        case LEAVE:
                            removing |= id.equals(((UnbootstrapAndLeave) operation).finishLeave.nodeId);
                    }
                }

                if (!removing)
                    return new Rejected(INVALID, String.format("Cannot mark node %s hard removed as it is still present in the directory.", id));
            }
        }

        SortedArrayList<Node.Id> hardRemoveIds = SortedArrayList.ofUnsorted(ids.stream().map(AccordTopology::tcmIdToAccord).toArray(Node.Id[]::new));

        for (Node.Id id : hardRemoveIds)
            if (prev.accordStaleReplicas.hardRemoved().contains(id))
                return new Rejected(INVALID, String.format("Cannot mark node %s hard removed as it already is.", id));

        logger.info("Marking " + ids + " hard removed. These nodes should be permanently offline and unable to respond to any messages at any future point.");
        ClusterMetadata.Transformer next = prev.transformer().markHardRemovedReplicas(hardRemoveIds);
        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public String toString()
    {
        return "AccordMarkHardRemoved{ids=" + ids + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordMarkHardRemoved that = (AccordMarkHardRemoved) o;
        return Objects.equals(ids, that.ids);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ids);
    }

    public static final AsymmetricMetadataSerializer<Transformation, AccordMarkHardRemoved> serializer = new AsymmetricMetadataSerializer<>()
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof AccordMarkHardRemoved;
            AccordMarkHardRemoved mark = (AccordMarkHardRemoved) t;
            CollectionSerializers.serializeCollection(mark.ids, out, version, NodeId.serializer);
            out.writeBoolean(mark.force);
        }

        @Override
        public AccordMarkHardRemoved deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordMarkHardRemoved(CollectionSerializers.deserializeSet(in, version, NodeId.serializer), in.readBoolean());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof AccordMarkHardRemoved;
            AccordMarkHardRemoved mark = (AccordMarkHardRemoved) t;
            return CollectionSerializers.serializedCollectionSize(mark.ids, version, NodeId.serializer) + TypeSizes.BOOL_SIZE;
        }
    };
}
