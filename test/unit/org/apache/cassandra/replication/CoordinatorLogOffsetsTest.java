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

package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import com.google.common.collect.Sets;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.AlterSchema;

import static accord.utils.Property.qt;

public class CoordinatorLogOffsetsTest
{
    private static final Gen<Long> LOG_ID_GEN = rs -> {
        int hostId = rs.nextInt(1, 4);
        int hostLogId = rs.nextInt(1, 11);
        return CoordinatorLogId.asLong(hostId, hostLogId);
    };

    private static final Gen<Long> SEQUENCE_ID_GEN = rs -> {
        int offset = rs.nextBiasedInt(1, 10_000, 1_000_000);
        return MutationId.sequenceId(offset, offset);
    };

    private static final Gen<MutationId> MUTATION_ID_GEN = rs -> new MutationId(LOG_ID_GEN.next(rs), SEQUENCE_ID_GEN.next(rs));

    private static final Gen<ImmutableCoordinatorLogOffsets> COORDINATOR_LOG_OFFSETS_GEN = rs -> {
        int numMutations = rs.nextBiasedInt(0, 10, 1000);
        ImmutableCoordinatorLogOffsets.Builder builder = new ImmutableCoordinatorLogOffsets.Builder(numMutations);
        for (int i = 0; i < numMutations; i++)
            builder.add(MUTATION_ID_GEN.next(rs));
        int numTransfers = rs.nextBiasedInt(0, 1, 10);
        for (int i = 0; i < numTransfers; i++)
            builder.addTransfer(MUTATION_ID_GEN.next(rs), new Bounds<>(new Murmur3Partitioner.LongToken(1), new Murmur3Partitioner.LongToken(2)));
        return builder.build();
    };

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void roundtripSerde()
    {
        qt()
        .forAll(COORDINATOR_LOG_OFFSETS_GEN)
        .check(originals -> {
            try (DataOutputBuffer outputBuffer = DataOutputBuffer.scratchBuffer.get())
            {
                ImmutableCoordinatorLogOffsets.serializer.serialize(originals, outputBuffer, Version.CURRENT);
                try (DataInputBuffer inputBuffer = new DataInputBuffer(outputBuffer.buffer(), true))
                {
                    ImmutableCoordinatorLogOffsets deserialized = ImmutableCoordinatorLogOffsets.serializer.deserialize(inputBuffer, Version.CURRENT);
                    CoordinatorLogOffsets.Mutations<Offsets.Immutable> mutations = deserialized.mutations();
                    Assertions.assertThat(Sets.newHashSet(mutations)).isEqualTo(Sets.newHashSet(originals.mutations().iterator()));
                    for (long logId : originals.mutations())
                        Assertions.assertThat(mutations.offsets(logId)).isEqualTo(originals.mutations().offsets(logId));
                    Assertions.assertThat(outputBuffer.getLength()).isEqualTo(ImmutableCoordinatorLogOffsets.serializer.serializedSize(originals, Version.CURRENT));
                }
            }
        });
    }

    @Test
    public void monotonicAdd()
    {
        monotonicAdd(() -> new NonBlockingCoordinatorLogOffsets.Concurrent(16));
        monotonicAdd(NonBlockingCoordinatorLogOffsets.Exclusive::new);
    }

    public void monotonicAdd(Supplier<MutableCoordinatorLogOffsets> ctor)
    {
        qt()
        .forAll(Gens.lists(MUTATION_ID_GEN).ofSizeBetween(3, 100))
        .check(ids -> {
            MutableCoordinatorLogOffsets logOffsets = ctor.get();
            for (MutationId id : ids)
            {
                Offsets originalOffsets = logOffsets.mutations().offsets(id.logId());
                boolean existed = originalOffsets.contains(id.offset());
                logOffsets.add(id);
                Offsets updatedOffsets = logOffsets.mutations().offsets(id.logId());
                if (existed)
                    Assertions.assertThat(updatedOffsets).hasSameSizeAs(originalOffsets);
                Assertions.assertThat(updatedOffsets.contains(id.offset())).isTrue();
            }
        });
    }

    @Test
    public void monotonicMerge()
    {
        monotonicMerge(() -> new NonBlockingCoordinatorLogOffsets.Concurrent(16));
        monotonicMerge(NonBlockingCoordinatorLogOffsets.Exclusive::new);
    }

    public void monotonicMerge(Supplier<MutableCoordinatorLogOffsets> ctor)
    {
        qt()
        .forAll(COORDINATOR_LOG_OFFSETS_GEN, COORDINATOR_LOG_OFFSETS_GEN)
        .check((left, right) -> {
            MutableCoordinatorLogOffsets merged = ctor.get();
            merged.addAll(left.mutations());
            merged.addAll(right.mutations());
            for (Long logId : merged.mutations())
            {
                Offsets leftOffsets = left.mutations().offsets(logId);
                Offsets rightOffsets = right.mutations().offsets(logId);
                Offsets mergedOffsets = Offsets.Immutable.copy(merged.mutations().offsets(logId));
                Assertions.assertThat(mergedOffsets).isEqualTo(Offsets.Immutable.union(leftOffsets, rightOffsets));
            }
        });
    }

    @Test
    public void builderEquivalentToMutable()
    {
        builderEquivalentToMutable(() -> new NonBlockingCoordinatorLogOffsets.Concurrent(16));
        builderEquivalentToMutable(NonBlockingCoordinatorLogOffsets.Exclusive::new);
    }

    public void builderEquivalentToMutable(Supplier<MutableCoordinatorLogOffsets> ctor)
    {
        qt()
        .forAll(Gens.lists(MUTATION_ID_GEN).ofSizeBetween(3, 100))
        .check(ids -> {
            MutableCoordinatorLogOffsets logOffsets = ctor.get();
            ImmutableCoordinatorLogOffsets.Builder builder = new ImmutableCoordinatorLogOffsets.Builder(ids.size());
            for (MutationId id : ids)
            {
                logOffsets.add(id);
                builder.add(id);
            }

            ImmutableCoordinatorLogOffsets fromBuilder = builder.build();
            Assertions.assertThat(fromBuilder.mutations()).hasSize(logOffsets.mutations().size());
            for (Long logId : logOffsets.mutations())
                Assertions.assertThat(fromBuilder.mutations().offsets(logId)).isEqualTo(Offsets.Immutable.copy(logOffsets.mutations().offsets(logId)));
        });
    }

    @Test
    public void mutableImplsEquivalent()
    {
        class Args
        {
            public final List<MutationId> ids;
            public final int contentions;

            public Args(List<MutationId> ids, int contentions)
            {
                this.ids = ids;
                this.contentions = contentions;
            }
        }

        Gen<Args> argsGen = rs -> {
            List<MutationId> ids = Gens.lists(MUTATION_ID_GEN).ofSizeBetween(3, 100).next(rs);
            int contentions = rs.nextInt(1, 16);
            return new Args(ids, contentions);
        };

        qt()
        .forAll(argsGen)
        .check(args -> {
            NonBlockingCoordinatorLogOffsets.Exclusive exclusive = new NonBlockingCoordinatorLogOffsets.Exclusive();
            NonBlockingCoordinatorLogOffsets.Concurrent concurrent = new NonBlockingCoordinatorLogOffsets.Concurrent(args.contentions);
            ExecutorService executor = Executors.newFixedThreadPool(args.contentions);
            List<Future<?>> concurrentUpdates = new ArrayList<>();

            for (MutationId id : args.ids)
            {
                concurrentUpdates.add(executor.submit(() -> concurrent.add(id)));
                exclusive.add(id);
            }
            for (Future<?> task : concurrentUpdates)
                task.get();

            Assertions.assertThatIterable(exclusive.mutations()).hasSameSizeAs(concurrent.mutations());
            for (Long logId : exclusive.mutations())
                Assertions.assertThat(exclusive.mutations().offsets(logId)).isEqualTo(concurrent.mutations().offsets(logId));
        });
    }

    @Test
    public void reconciledBounds() throws InterruptedException, ExecutionException {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        MutationJournal.start();

        String ks = "ks";
        String tbl = "tbl";

        TableMetadata tableMetadata = TableMetadata.builder(ks, tbl)
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addRegularColumn("v", Int32Type.instance)
                .build();

        InetAddressAndPort addr1 = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
        InetAddressAndPort addr2 = InetAddressAndPort.getByNameUnchecked("127.0.0.2");
        InetAddressAndPort addr3 = InetAddressAndPort.getByNameUnchecked("127.0.0.3");
        Location location = new Location("dc1", "rack1");

        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());

        // RF=3, all instances are replicas
        ClusterMetadataTestHelper.addEndpoint(addr1, new Murmur3Partitioner.LongToken(1), location);
        ClusterMetadataTestHelper.addEndpoint(addr2, new Murmur3Partitioner.LongToken(2), location);
        ClusterMetadataTestHelper.addEndpoint(addr3, new Murmur3Partitioner.LongToken(3), location);

        ClusterMetadataTestHelper.createKeyspace(ks, KeyspaceParams.simple(3, ReplicationType.tracked));
        ClusterMetadataTestHelper.commit(new AlterSchema(SchemaTransformations.addTable(tableMetadata, false)));

        CommitLog.instance.start();
        MutationTrackingService.start(metadata);

        // Eventually, will also run perturbations before checking isReconciled (like log truncation, durability, etc.)
        // to ensure that we don't prune data required to check what's been reconciled

        // Applied at all replicas
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 1, 1);
            MutationTrackingService.instance().startWriting(mutation);

            MutationTrackingService.instance().finishWriting(mutation);
            MutationTrackingService.instance().receivedWriteResponse(mutation.id(), addr2);
            MutationTrackingService.instance().receivedWriteResponse(mutation.id(), addr3);
            MutationTrackingService.instance().persistLogStateForTesting();

            ImmutableCoordinatorLogOffsets logOffsets = new ImmutableCoordinatorLogOffsets.Builder()
                    .add(mutation.id())
                    .build();
            Range<Token> range = getShardRange(mutation);
            List<? extends Offsets> offsets = Collections.singletonList(logOffsets.mutations().offsets(mutation.id().logId()));
            MutationTrackingService.instance().updateReplicatedOffsets(ks, range, offsets, true, addr2);
            MutationTrackingService.instance().updateReplicatedOffsets(ks, range, offsets, true, addr3);

            Assertions.assertThat(MutationTrackingService.instance().isDurablyReconciled(logOffsets)).isTrue();
        }

        // Applied locally but not on remote replicas
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 2, 2);
            MutationTrackingService.instance().startWriting(mutation);
            MutationTrackingService.instance().finishWriting(mutation);
            MutationTrackingService.instance().persistLogStateForTesting();

            ImmutableCoordinatorLogOffsets logOffsets = new ImmutableCoordinatorLogOffsets.Builder()
                    .add(mutation.id())
                    .build();
            Assertions.assertThat(MutationTrackingService.instance().isDurablyReconciled(logOffsets)).isFalse();
        }

        // Applied on remote replicas but not locally
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 3, 3);
            MutationTrackingService.instance().startWriting(mutation);

            MutationTrackingService.instance().receivedWriteResponse(mutation.id(), addr2);
            MutationTrackingService.instance().receivedWriteResponse(mutation.id(), addr3);
            MutationTrackingService.instance().persistLogStateForTesting();

            ImmutableCoordinatorLogOffsets logOffsets = new ImmutableCoordinatorLogOffsets.Builder()
                    .add(mutation.id())
                    .build();

            Range<Token> range = getShardRange(mutation);
            List<? extends Offsets> offsets = Collections.singletonList(logOffsets.mutations().offsets(mutation.id().logId()));
            MutationTrackingService.instance().updateReplicatedOffsets(ks, range, offsets, true, addr2);
            MutationTrackingService.instance().updateReplicatedOffsets(ks, range, offsets, true, addr3);

            Assertions.assertThat(MutationTrackingService.instance().isDurablyReconciled(logOffsets)).isFalse();
        }

        // If no replicas are aware of a log, it should be considered unreconciled out of caution
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 4, 4);
            MutationTrackingService.instance().startWriting(mutation);

            MutationTrackingService.instance().finishWriting(mutation);
            MutationTrackingService.instance().receivedWriteResponse(mutation.id(), addr2);
            MutationTrackingService.instance().receivedWriteResponse(mutation.id(), addr3);
            MutationTrackingService.instance().persistLogStateForTesting();

            MutationId fakeMutationId = new MutationId(CoordinatorLogId.asLong(111, 222), MutationId.sequenceId(333, 444));
            Assertions.assertThat(metadata.directory.version(new NodeId(fakeMutationId.hostId()))).isNull();

            Offsets.Immutable.Builder offsetsBuilder = new Offsets.Immutable.Builder(new CoordinatorLogId(fakeMutationId.logId()));
            offsetsBuilder.add(fakeMutationId.offset());

            ImmutableCoordinatorLogOffsets.Builder logOffsetsBuilder = new ImmutableCoordinatorLogOffsets.Builder();
            logOffsetsBuilder.add(fakeMutationId);
            Assertions.assertThatThrownBy(() -> MutationTrackingService.instance().isDurablyReconciled(logOffsetsBuilder.build()))
                    .hasSameClassAs(new IllegalStateException())
                    .hasMessageMatching("Could not find shard for logId \\d+");
        }

        MutationTrackingService.shutdown();
        CommitLog.instance.stopUnsafe(true);
    }

    private Range<Token> getShardRange(Mutation mutation)
    {
        Map<String, Range<Token>> ksRanges = new HashMap<>();
        MutationTrackingService.instance().forEachKeyspace(shards -> {
            Shard shard = shards.lookUp(mutation);
            ksRanges.put(shard.keyspace, shard.range);
        });
        return ksRanges.get(mutation.getKeyspaceName());
    }
}
