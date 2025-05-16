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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils;
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

import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import com.google.common.collect.Sets;

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
        int numIds = rs.nextBiasedInt(0, 10, 1000);
        ImmutableCoordinatorLogOffsets.Builder builder = new ImmutableCoordinatorLogOffsets.Builder(numIds);
        for (int i = 0; i < numIds; i++)
            builder.add(MUTATION_ID_GEN.next(rs));
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
        .check(offsets -> {
            try (DataOutputBuffer outputBuffer = DataOutputBuffer.scratchBuffer.get())
            {
                ImmutableCoordinatorLogOffsets.serializer.serialize(offsets, outputBuffer, MessagingService.current_version);
                byte[] bytes = outputBuffer.toByteArray();
                try (DataInputBuffer inputBuffer = new DataInputBuffer(bytes))
                {
                    ImmutableCoordinatorLogOffsets deserialized = ImmutableCoordinatorLogOffsets.serializer.deserialize(inputBuffer, MessagingService.current_version);
                    Assertions.assertThat(Sets.newHashSet(deserialized.iterator())).isEqualTo(Sets.newHashSet(offsets.iterator()));
                    for (long logId : offsets)
                        Assertions.assertThat(deserialized.offsets(logId)).isEqualTo(offsets.offsets(logId));
                    Assertions.assertThat(bytes.length).isEqualTo(ImmutableCoordinatorLogOffsets.serializer.serializedSize(offsets, MessagingService.current_version));
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
                Offsets originalOffsets = logOffsets.offsets(id.logId());
                boolean existed = originalOffsets.contains(id.offset());
                logOffsets.add(id);
                Offsets updatedOffsets = logOffsets.offsets(id.logId());
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
            merged.addAll(left);
            merged.addAll(right);
            for (Long logId : merged)
            {
                Offsets leftOffsets = left.offsets(logId);
                Offsets rightOffsets = right.offsets(logId);
                Offsets mergedOffsets = Offsets.Immutable.copy(merged.offsets(logId));
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
            Assertions.assertThat(fromBuilder).hasSize(logOffsets.size());
            for (Long logId : logOffsets)
                Assertions.assertThat(fromBuilder.offsets(logId)).isEqualTo(Offsets.Immutable.copy(logOffsets.offsets(logId)));
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

            Assertions.assertThatIterable(exclusive).hasSameSizeAs(concurrent);
            for (Long logId : exclusive)
                Assertions.assertThat(exclusive.offsets(logId)).isEqualTo(concurrent.offsets(logId));
        });
    }

    @Test
    public void reconciledBounds() throws InterruptedException, ExecutionException {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        MutationJournal.instance.start();

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

        MutationTrackingService.instance.start(metadata);

        // Eventually, will also run perturbations before checking isReconciled (like log truncation, durability, etc.)
        // to ensure that we don't prune data required to check what's been reconciled

        // Applied at all replicas
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 1, 1);
            MutationTrackingService.instance.startWriting(mutation);

            MutationTrackingService.instance.finishWriting(mutation);
            MutationTrackingService.instance.receivedWriteResponse(mutation.id(), addr2);
            MutationTrackingService.instance.receivedWriteResponse(mutation.id(), addr3);

            ImmutableCoordinatorLogOffsets logOffsets = new ImmutableCoordinatorLogOffsets.Builder()
                    .add(mutation.id())
                    .build();
            Assertions.assertThat(MutationTrackingService.instance.isDurablyReconciled(ks, logOffsets)).isTrue();
        }

        // Applied locally but not on remote replicas
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 2, 2);
            MutationTrackingService.instance.startWriting(mutation);
            MutationTrackingService.instance.finishWriting(mutation);

            ImmutableCoordinatorLogOffsets logOffsets = new ImmutableCoordinatorLogOffsets.Builder()
                    .add(mutation.id())
                    .build();
            Assertions.assertThat(MutationTrackingService.instance.isDurablyReconciled(ks, logOffsets)).isFalse();
        }

        // Applied on remote replicas but not locally
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 3, 3);
            MutationTrackingService.instance.startWriting(mutation);

            MutationTrackingService.instance.receivedWriteResponse(mutation.id(), addr2);
            MutationTrackingService.instance.receivedWriteResponse(mutation.id(), addr3);

            ImmutableCoordinatorLogOffsets logOffsets = new ImmutableCoordinatorLogOffsets.Builder()
                    .add(mutation.id())
                    .build();
            Assertions.assertThat(MutationTrackingService.instance.isDurablyReconciled(ks, logOffsets)).isFalse();
        }

        // If no replicas are aware of a log, it should be considered unreconciled out of caution
        {
            Mutation mutation = MutationTrackingUtils.createMutation(tableMetadata, 4, 4);
            MutationTrackingService.instance.startWriting(mutation);

            MutationTrackingService.instance.finishWriting(mutation);
            MutationTrackingService.instance.receivedWriteResponse(mutation.id(), addr2);
            MutationTrackingService.instance.receivedWriteResponse(mutation.id(), addr3);

            MutationId fakeMutationId = new MutationId(CoordinatorLogId.asLong(111, 222), MutationId.sequenceId(333, 444));
            Assertions.assertThat(metadata.directory.version(new NodeId(fakeMutationId.hostId()))).isNull();

            Offsets.Immutable.Builder offsetsBuilder = new Offsets.Immutable.Builder(new CoordinatorLogId(fakeMutationId.logId()));
            offsetsBuilder.add(fakeMutationId.offset());

            ImmutableCoordinatorLogOffsets.Builder logOffsetsBuilder = new ImmutableCoordinatorLogOffsets.Builder();
            logOffsetsBuilder.add(fakeMutationId);
            Assertions.assertThat(MutationTrackingService.instance.isDurablyReconciled(ks, logOffsetsBuilder.build())).isFalse();
        }

        MutationTrackingService.instance.shutdownBlocking();
    }
}