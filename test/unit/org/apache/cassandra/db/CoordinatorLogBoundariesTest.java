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

package org.apache.cassandra.db;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationId;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class CoordinatorLogBoundariesTest
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

    private static final Gen<CoordinatorLogBoundaries> COORDINATOR_LOG_BOUNDARIES_GEN = rs -> {
        MutableCoordinatorLogBoundaries boundaries = MutableCoordinatorLogBoundaries.create();
        int numIds = rs.nextBiasedInt(0, 10, 1000);
        for (int i = 0; i < numIds; i++)
            boundaries.add(MUTATION_ID_GEN.next(rs));
        return boundaries;
    };

    @Test
    public void roundtripSerde()
    {
        qt()
        .forAll(COORDINATOR_LOG_BOUNDARIES_GEN)
        .check(boundaries -> {
            try (DataOutputBuffer outputBuffer = DataOutputBuffer.scratchBuffer.get())
            {
                CoordinatorLogBoundaries.serializer.serialize(boundaries, outputBuffer, MessagingService.current_version);
                byte[] bytes = outputBuffer.toByteArray();
                try (DataInputBuffer inputBuffer = new DataInputBuffer(bytes))
                {
                    CoordinatorLogBoundaries deserialized = CoordinatorLogBoundaries.serializer.deserialize(inputBuffer, MessagingService.current_version);
                    Assertions.assertThat(boundaries).isEqualTo(deserialized);
                    Assertions.assertThat(bytes.length).isEqualTo(CoordinatorLogBoundaries.serializer.serializedSize(boundaries, MessagingService.current_version));
                }
            }
        });
    }

    @Test
    public void monotonicAdd()
    {
        qt()
        .forAll(Gens.lists(MUTATION_ID_GEN).ofSizeBetween(3, 100))
        .check(ids -> {
            CoordinatorLogBoundariesMap boundaries = new CoordinatorLogBoundariesMap();
            for (MutationId id : ids)
            {
                int originalOffset = boundaries.maxOffset(id.logId());
                boundaries.add(id);
                int updatedOffset = boundaries.maxOffset(id.logId());
                Assertions.assertThat(updatedOffset).isGreaterThanOrEqualTo(originalOffset);
                Assertions.assertThat(updatedOffset).isEqualTo(Math.max(originalOffset, id.offset()));
            }
        });
    }

    @Test
    public void monotonicMerge()
    {
        qt()
        .forAll(COORDINATOR_LOG_BOUNDARIES_GEN, COORDINATOR_LOG_BOUNDARIES_GEN)
        .check((left, right) -> {
            MutableCoordinatorLogBoundaries boundaries = MutableCoordinatorLogBoundaries.create();
            boundaries.addAll(left);
            boundaries.addAll(right);
            CoordinatorLogBoundaries merged = boundaries;
            for (Long logId : merged)
            {
                int leftOffset = left.maxOffset(logId);
                int rightOffset = right.maxOffset(logId);
                int mergedOffset = merged.maxOffset(logId);
                Assertions.assertThat(mergedOffset).isGreaterThanOrEqualTo(leftOffset);
                Assertions.assertThat(mergedOffset).isGreaterThanOrEqualTo(rightOffset);
                Assertions.assertThat(mergedOffset).isIn(leftOffset, rightOffset);
            }
        });
    }

    @Test
    public void builderEquivalentToMutable()
    {
        qt()
        .forAll(Gens.lists(MUTATION_ID_GEN).ofSizeBetween(3, 100))
        .check(ids -> {
            CoordinatorLogBoundariesMap boundaries = new CoordinatorLogBoundariesMap();
            CoordinatorLogBoundariesBuilder builder = new CoordinatorLogBoundariesBuilder();
            for (MutationId id : ids)
            {
                boundaries.add(id);
                builder.add(id);
            }

            CoordinatorLogBoundaries fromBuilder = builder.build();
            Assertions.assertThat(fromBuilder).hasSize(boundaries.size());
            for (Long logId : boundaries)
                Assertions.assertThat(fromBuilder.max(logId)).isEqualTo(boundaries.max(logId));
        });
    }
}