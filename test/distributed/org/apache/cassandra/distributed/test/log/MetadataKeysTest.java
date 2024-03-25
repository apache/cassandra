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

package org.apache.cassandra.distributed.test.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MetadataKey;
import org.apache.cassandra.tcm.MetadataKeys;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.Register;

public class MetadataKeysTest extends CMSTestBase
{
    static
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void metadataKeysChangeTest() throws Exception
    {
        Generator<Register> genRegister = new Generators.InetAddrAndPortGenerator()
                                  .zip(Generators.pick("datacenter1", "datacenter2"),
                                       Generators.pick("rack1", "rack2"),
                                       (InetAddressAndPort addr, String dc, String rack) -> {
                                           return new Register(new NodeAddresses(addr),
                                                               new Location(dc, rack),
                                                               NodeVersion.CURRENT);
                                       });
        EntropySource rng = new JdkRandomEntropySource(1l);

        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, new TokenPlacementModel.SimpleReplicationFactor(3)))
        {
            Register register = genRegister.generate(rng);
            checkDiff(sut.service, register);
            sut.service.commit(register);

            {
                PrepareJoin prepareJoin = prepareJoinGenerator(sut.service.metadata(), sut.service.placementProvider()).generate(rng);
                checkDiff(sut.service, prepareJoin);
                sut.service.commit(prepareJoin);

                BootstrapAndJoin bootstrapAndJoin = (BootstrapAndJoin) sut.service.metadata().inProgressSequences.get(prepareJoin.nodeId());
                checkDiff(sut.service, bootstrapAndJoin.startJoin);
                sut.service.commit(bootstrapAndJoin.startJoin);

                checkDiff(sut.service, bootstrapAndJoin.midJoin);
                sut.service.commit(bootstrapAndJoin.midJoin);

                checkDiff(sut.service, bootstrapAndJoin.finishJoin);
                sut.service.commit(bootstrapAndJoin.finishJoin);
            }

            {
                PrepareMove prepareMove = prepareMoveGenerator(sut.service.metadata(), sut.service.placementProvider()).generate(rng);
                checkDiff(sut.service, prepareMove);
                sut.service.commit(prepareMove);

                Move bootstrapAndMove = (Move) sut.service.metadata().inProgressSequences.get(prepareMove.nodeId());
                checkDiff(sut.service, bootstrapAndMove.startMove);
                sut.service.commit(bootstrapAndMove.startMove);

                checkDiff(sut.service, bootstrapAndMove.midMove);
                sut.service.commit(bootstrapAndMove.midMove);

                checkDiff(sut.service, bootstrapAndMove.finishMove);
                sut.service.commit(bootstrapAndMove.finishMove);
            }

            {
                PrepareLeave prepareLeave = prepareLeaveGenerator(sut.service.metadata(), sut.service.placementProvider()).generate(rng);
                checkDiff(sut.service, prepareLeave);
                sut.service.commit(prepareLeave);

                UnbootstrapAndLeave bootstrapAndLeave = (UnbootstrapAndLeave) sut.service.metadata().inProgressSequences.get(prepareLeave.nodeId());
                checkDiff(sut.service, bootstrapAndLeave.startLeave);
                sut.service.commit(bootstrapAndLeave.startLeave);

                checkDiff(sut.service, bootstrapAndLeave.midLeave);
                sut.service.commit(bootstrapAndLeave.midLeave);

                checkDiff(sut.service, bootstrapAndLeave.finishLeave);
                sut.service.commit(bootstrapAndLeave.finishLeave);
            }
        }
    }

    @Test
    public void metadataKeysExtensionTest() throws Exception
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, new TokenPlacementModel.SimpleReplicationFactor(3)))
        {
            Transformation transformation;
            transformation = new CustomTransformation.PokeInt(1);
            checkDiff(sut.service, transformation, Collections.singleton(CustomTransformation.PokeInt.METADATA_KEY));
            sut.service.commit(transformation);

            transformation = new CustomTransformation.PokeInt(2);
            checkDiff(sut.service, transformation, Collections.singleton(CustomTransformation.PokeInt.METADATA_KEY));
            sut.service.commit(transformation);

            transformation = CustomTransformation.ClearInt.instance;
            checkDiff(sut.service, transformation, Collections.singleton(CustomTransformation.PokeInt.METADATA_KEY));
            sut.service.commit(transformation);
        }
    }

    private static void checkDiff(ClusterMetadataService cms, Transformation transformation)
    {
        ClusterMetadata before = cms.metadata();
        Transformation.Result result = transformation.execute(before);
        ClusterMetadata after = result.success().metadata;
        Assert.assertEquals(result.success().affectedMetadata, MetadataKeys.diffKeys(before, after));
    }

    private static void checkDiff(ClusterMetadataService cms, Transformation transformation, Set<MetadataKey> expected)
    {
        ClusterMetadata before = cms.metadata();
        Transformation.Result result = transformation.execute(before);
        ClusterMetadata after = result.success().metadata;
        Assert.assertEquals(expected, result.success().affectedMetadata);
        Assert.assertEquals(result.success().affectedMetadata, MetadataKeys.diffKeys(before, after));
    }

    public Generator<PrepareJoin> prepareJoinGenerator(ClusterMetadata metadata, PlacementProvider placementProvider)
    {
        List<NodeId> pickFrom = new ArrayList<>();
        for (Map.Entry<NodeId, NodeState> e : metadata.directory.states.entrySet())
        {
            if (e.getValue().equals(NodeState.REGISTERED))
                pickFrom.add(e.getKey());
        }

        return Generators.pick(pickFrom).zip(new LongGenerator().map(Murmur3Partitioner.LongToken::new),
                                             (node, token) -> new PrepareJoin(node, Collections.singleton(token), placementProvider, true, false));
    }

    public Generator<PrepareLeave> prepareLeaveGenerator(ClusterMetadata metadata, PlacementProvider placementProvider)
    {
        List<NodeId> pickFrom = new ArrayList<>();
        for (Map.Entry<NodeId, NodeState> e : metadata.directory.states.entrySet())
        {
            if (e.getValue().equals(NodeState.JOINED))
                pickFrom.add(e.getKey());
        }

        return Generators.pick(pickFrom).map((node) -> new PrepareLeave(node, true, placementProvider, LeaveStreams.Kind.UNBOOTSTRAP));
    }

    public Generator<PrepareMove> prepareMoveGenerator(ClusterMetadata metadata, PlacementProvider placementProvider)
    {
        List<NodeId> pickFrom = new ArrayList<>();
        for (Map.Entry<NodeId, NodeState> e : metadata.directory.states.entrySet())
        {
            if (e.getValue().equals(NodeState.JOINED))
                pickFrom.add(e.getKey());
        }

        return Generators.pick(pickFrom).zip(new LongGenerator().map(Murmur3Partitioner.LongToken::new),
                                             (node, token) -> new PrepareMove(node, Collections.singleton(token), placementProvider, false));
    }

    private static final class LongGenerator implements Generator<Long>
    {

        @Override
        public Long generate(EntropySource rng)
        {
            return rng.next();
        }
    }
}
