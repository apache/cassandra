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

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareReplace;

import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_JOIN;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_LEAVE;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_MOVE;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_REPLACE;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_JOIN;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_LEAVE;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_MOVE;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_REPLACE;
import static org.apache.cassandra.tcm.Transformation.Kind.START_JOIN;
import static org.apache.cassandra.tcm.Transformation.Kind.START_LEAVE;
import static org.apache.cassandra.tcm.Transformation.Kind.START_MOVE;
import static org.apache.cassandra.tcm.Transformation.Kind.START_REPLACE;
import static org.apache.cassandra.tcm.membership.MembershipUtils.node;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.randomDeltas;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.randomTokens;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.ranges;

public class SequencesUtils
{
    public static LockedRanges lockedRanges(DataPlacements placements, Random random)
    {
        LockedRanges locked = LockedRanges.EMPTY;
        for (int i = 0; i < random.nextInt(10) + 1; i++)
            locked = locked.lock(LockedRanges.keyFor(epoch(random)), affectedRanges(placements, random));
        return locked;
    }

    public static LockedRanges.AffectedRanges affectedRanges(DataPlacements placements, Random random)
    {
        LockedRanges.AffectedRangesBuilder affected = LockedRanges.AffectedRanges.builder();
        placements.asMap().forEach((params, placement) -> {
            placement.reads.ranges.forEach((range) -> {
                if (random.nextDouble() >= 0.6)
                    affected.add(params, range);
            });
        });
        return affected.build();
    }

    public static Epoch epoch(Random random)
    {
        return Epoch.create(Math.abs(random.nextLong()));
    }

    public static BootstrapAndJoin bootstrapAndJoin(IPartitioner partitioner, Random random, Predicate<NodeId> alreadyInUse)
    {
        Transformation.Kind[] potentialNextStates = {START_JOIN, MID_JOIN, FINISH_JOIN};
        NodeId node = node(random);
        while (alreadyInUse.test(node))
            node = node(random);
        Epoch epoch = epoch(random);
        Set<Token> tokens = randomTokens(10, partitioner, random);
        List<Range<Token>> ranges = ranges(tokens, partitioner);
        PlacementDeltas deltas = randomDeltas(ranges, random);
        LockedRanges.Key key = LockedRanges.keyFor(epoch);
        return new BootstrapAndJoin(epoch,
                                    key,
                                    deltas,
                                    potentialNextStates[random.nextInt(3)],
                                    new PrepareJoin.StartJoin(node, deltas, key),
                                    new PrepareJoin.MidJoin(node, deltas, key),
                                    new PrepareJoin.FinishJoin(node, tokens, deltas, key),
                                    true,
                                    true);
    }

    public static BootstrapAndReplace bootstrapAndReplace(IPartitioner partitioner, Random random, Predicate<NodeId> alreadyInUse)
    {
        Transformation.Kind[] potentialNextStates = {START_REPLACE, MID_REPLACE, FINISH_REPLACE};
        NodeId replaced = node(random);
        NodeId replacement = node(random);

        while (alreadyInUse.test(replacement))
            replacement = node(random);

        Epoch epoch = epoch(random);
        Set<Token> tokens = randomTokens(10, partitioner, random);
        List<Range<Token>> ranges = ranges(tokens, partitioner);
        PlacementDeltas deltas = randomDeltas(ranges, random);
        LockedRanges.Key key = LockedRanges.keyFor(epoch);
        return new BootstrapAndReplace(epoch,
                                       key,
                                       tokens,
                                       potentialNextStates[random.nextInt(3)],
                                       new PrepareReplace.StartReplace(replaced, replacement, deltas, key),
                                       new PrepareReplace.MidReplace(replaced, replacement, deltas, key),
                                       new PrepareReplace.FinishReplace(replaced, replacement, deltas, key),
                                       true,
                                       true);
    }

    public static UnbootstrapAndLeave unbootstrapAndLeave(IPartitioner partitioner, Random random, Predicate<NodeId> alreadyInUse)
    {
        Transformation.Kind[] potentialNextStates = {START_LEAVE, MID_LEAVE, FINISH_LEAVE};
        NodeId node = node(random);
        while (alreadyInUse.test(node))
            node = node(random);
        Epoch epoch = epoch(random);
        Set<Token> tokens = randomTokens(10, partitioner, random);
        List<Range<Token>> ranges = ranges(tokens, partitioner);
        PlacementDeltas deltas = randomDeltas(ranges, random);
        LockedRanges.Key key = LockedRanges.keyFor(epoch);
        return new UnbootstrapAndLeave(epoch,
                                       key,
                                       potentialNextStates[random.nextInt(3)],
                                       new PrepareLeave.StartLeave(node, deltas, key),
                                       new PrepareLeave.MidLeave(node, deltas, key),
                                       new PrepareLeave.FinishLeave(node, deltas, key),
                                       new UnbootstrapStreams());
    }

    public static Move move(IPartitioner partitioner, Random random, Predicate<NodeId> alreadyInUse)
    {
        Transformation.Kind[] potentialNextStates = {START_MOVE, MID_MOVE, FINISH_MOVE};
        NodeId node = node(random);
        while (alreadyInUse.test(node))
            node = node(random);

        Epoch epoch = epoch(random);
        Set<Token> tokens = randomTokens(10, partitioner, random);
        List<Range<Token>> ranges = ranges(tokens, partitioner);
        PlacementDeltas deltas = randomDeltas(ranges, random);
        LockedRanges.Key key = LockedRanges.keyFor(epoch);
        return new Move(epoch,
                        key,
                        potentialNextStates[random.nextInt(3)],
                        tokens,
                        deltas,
                        new PrepareMove.StartMove(node, deltas, key),
                        new PrepareMove.MidMove(node, deltas, key),
                        new PrepareMove.FinishMove(node, tokens, deltas, key),
                        true);
    }

    public static Epoch epoch(int epoch)
    {
        return Epoch.create(epoch);
    }

    // Custom transforms to lock/unlock an arbitrary set of ranges to
    // avoid having to actually initiate some range movement
    public static class LockRanges implements Transformation, Serializable
    {
        public static final AsymmetricMetadataSerializer<Transformation, LockRanges> serializer = new AsymmetricMetadataSerializer<Transformation, LockRanges>()
        {
            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version){}
            @Override
            public LockRanges deserialize(DataInputPlus in, Version version) {return new LockRanges();}
            @Override
            public long serializedSize(Transformation t, Version version) {return 0;}
        };

        public static final String NAME = "TestLockRanges";

        // at the moment, the detail of the specific LockedRanges doesn't matter, transformations
        // which are rejected in the presence of locking are rejected whatever is actually locked
        private static final LockedRanges.AffectedRanges toLock =
            LockedRanges.AffectedRanges.singleton(ReplicationParams.simple(3),
                                                  new Range<>(Murmur3Partitioner.instance.getMinimumToken(),
                                                              Murmur3Partitioner.instance.getRandomToken()));

        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            LockedRanges newLocked = metadata.lockedRanges.lock(LockedRanges.keyFor(metadata.epoch), toLock);
            return Transformation.success(metadata.transformer().with(newLocked), toLock);
        }
    }

    public static class ClearLockedRanges implements Transformation, Serializable
    {
        public static final AsymmetricMetadataSerializer<Transformation, ClearLockedRanges> serializer = new AsymmetricMetadataSerializer<Transformation, ClearLockedRanges>()
        {
            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) {}
            @Override
            public ClearLockedRanges deserialize(DataInputPlus in, Version version) {return new ClearLockedRanges();}
            @Override
            public long serializedSize(Transformation t, Version version) {return 0;}
        };
        public static final String NAME = "TestClearLockedRanges";

        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            LockedRanges newLocked = LockedRanges.EMPTY;
            return Transformation.success(metadata.transformer().with(newLocked), LockedRanges.AffectedRanges.EMPTY);
        }
    }
}
