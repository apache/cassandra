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
package org.apache.cassandra.service.writes.thresholds;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.thresholds.ThresholdCounter;

import static org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot.create;
import static org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot.empty;
import static org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot.writeSizeWarnMessage;
import static org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot.writeTombstoneWarnMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class WriteWarningsSnapshotTest
{
    private static final InetAddressAndPort REPLICA1 = address(127, 0, 0, 1);
    private static final InetAddressAndPort REPLICA2 = address(127, 0, 0, 2);
    private static final InetAddressAndPort REPLICA3 = address(127, 0, 0, 3);

    @Test
    public void testEmptySnapshot()
    {
        WriteWarningsSnapshot snapshot = empty();
        assertThat(snapshot.isEmpty()).isTrue();
        assertThat(snapshot.writeSize).isEqualTo(ThresholdCounter.empty());
        assertThat(snapshot.writeTombstone).isEqualTo(ThresholdCounter.empty());
    }

    @Test
    public void testCreateWithEmptyCounters()
    {
        WriteWarningsSnapshot snapshot = create(ThresholdCounter.empty(), ThresholdCounter.empty());
        assertThat(snapshot).isEqualTo(empty());
        assertThat(snapshot.isEmpty()).isTrue();
    }

    @Test
    public void testCreateWithNonEmptyCounters()
    {
        ThresholdCounter sizeCounter = new ThresholdCounter(ImmutableSet.of(REPLICA1), 1024);
        ThresholdCounter tombstoneCounter = new ThresholdCounter(ImmutableSet.of(REPLICA2), 500);

        WriteWarningsSnapshot snapshot = create(sizeCounter, tombstoneCounter);

        assertThat(snapshot.isEmpty()).isFalse();
        assertThat(snapshot.writeSize).isEqualTo(sizeCounter);
        assertThat(snapshot.writeTombstone).isEqualTo(tombstoneCounter);
    }

    @Test
    public void testMergeEmptyWithEmpty()
    {
        WriteWarningsSnapshot result = empty().merge(empty());
        assertThat(result).isEqualTo(empty());
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testMergeEmptyWithNonEmpty()
    {
        ThresholdCounter sizeCounter = new ThresholdCounter(ImmutableSet.of(REPLICA1), 2048);
        WriteWarningsSnapshot nonEmpty = create(sizeCounter, ThresholdCounter.empty());

        WriteWarningsSnapshot result1 = empty().merge(nonEmpty);
        WriteWarningsSnapshot result2 = nonEmpty.merge(empty());

        assertThat(result1).isEqualTo(nonEmpty);
        assertThat(result2).isEqualTo(nonEmpty);
    }

    @Test
    public void testMergeWithNull()
    {
        ThresholdCounter sizeCounter = new ThresholdCounter(ImmutableSet.of(REPLICA1), 2048);
        WriteWarningsSnapshot snapshot = create(sizeCounter, ThresholdCounter.empty());

        WriteWarningsSnapshot result = snapshot.merge(null);

        assertThat(result).isEqualTo(snapshot);
    }

    @Test
    public void testMergeSelfWithSelf()
    {
        qt().forAll(all()).check(snapshot -> snapshot.merge(snapshot).equals(snapshot));
    }

    @Test
    public void testMergeNonOverlappingReplicas()
    {
        WriteWarningsSnapshot snapshot1 = create(
            new ThresholdCounter(ImmutableSet.of(REPLICA1), 1024),
            new ThresholdCounter(ImmutableSet.of(REPLICA1), 100)
        );

        WriteWarningsSnapshot snapshot2 = create(
            new ThresholdCounter(ImmutableSet.of(REPLICA2), 2048),
            new ThresholdCounter(ImmutableSet.of(REPLICA2), 200)
        );

        WriteWarningsSnapshot merged = snapshot1.merge(snapshot2);

        // Should combine replica sets and take max values
        assertThat(merged.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2));
        assertThat(merged.writeSize.maxValue).isEqualTo(2048); // max of 1024 and 2048
        assertThat(merged.writeTombstone.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2));
        assertThat(merged.writeTombstone.maxValue).isEqualTo(200); // max of 100 and 200
    }

    @Test
    public void testMergeOverlappingReplicas()
    {
        WriteWarningsSnapshot snapshot1 = create(
            new ThresholdCounter(ImmutableSet.of(REPLICA1, REPLICA2), 3000),
            ThresholdCounter.empty()
        );

        WriteWarningsSnapshot snapshot2 = create(
            new ThresholdCounter(ImmutableSet.of(REPLICA2, REPLICA3), 4000),
            ThresholdCounter.empty()
        );

        WriteWarningsSnapshot merged = snapshot1.merge(snapshot2);

        // Should combine all unique replicas and take max value
        assertThat(merged.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2, REPLICA3));
        assertThat(merged.writeSize.maxValue).isEqualTo(4000); // max of 3000 and 4000
    }

    @Test
    public void testMergeDifferentThresholdTypes()
    {
        WriteWarningsSnapshot snapshot1 = create(
            new ThresholdCounter(ImmutableSet.of(REPLICA1), 5000),
            ThresholdCounter.empty()
        );

        WriteWarningsSnapshot snapshot2 = create(
            ThresholdCounter.empty(),
            new ThresholdCounter(ImmutableSet.of(REPLICA2), 300)
        );

        WriteWarningsSnapshot merged = snapshot1.merge(snapshot2);

        assertThat(merged.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(merged.writeSize.maxValue).isEqualTo(5000);
        assertThat(merged.writeTombstone.instances).isEqualTo(ImmutableSet.of(REPLICA2));
        assertThat(merged.writeTombstone.maxValue).isEqualTo(300);
    }

    @Test
    public void testWriteSizeWarnMessage()
    {
        String message = writeSizeWarnMessage(3, 1048576);
        assertThat(message).isEqualTo("3 nodes detected write to large partition; estimated size is 1048576 bytes (see write_size_warn_threshold)");
    }

    @Test
    public void testWriteSizeWarnMessageSingleNode()
    {
        String message = writeSizeWarnMessage(1, 2048);
        assertThat(message).isEqualTo("1 nodes detected write to large partition; estimated size is 2048 bytes (see write_size_warn_threshold)");
    }

    @Test
    public void testWriteTombstoneWarnMessage()
    {
        String message = writeTombstoneWarnMessage(2, 500);
        assertThat(message).isEqualTo("2 nodes detected write to partition with many tombstones; estimated count is 500 (see write_tombstone_warn_threshold)");
    }

    @Test
    public void testWriteTombstoneWarnMessageSingleNode()
    {
        String message = writeTombstoneWarnMessage(1, 1000);
        assertThat(message).isEqualTo("1 nodes detected write to partition with many tombstones; estimated count is 1000 (see write_tombstone_warn_threshold)");
    }

    @Test
    public void testMergeCommutative()
    {
        qt().forAll(all(), all()).check((a, b) -> a.merge(b).equals(b.merge(a)));
    }

    @Test
    public void testMergeAssociative()
    {
        qt().forAll(all(), all(), all()).check((a, b, c) -> a.merge(b).merge(c).equals(a.merge(b.merge(c))));
    }

    private static InetAddressAndPort address(int a, int b, int c, int d)
    {
        try
        {
            InetAddress address = InetAddress.getByAddress(new byte[]{ (byte) a, (byte) b, (byte) c, (byte) d });
            return InetAddressAndPort.getByAddress(address);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static Gen<WriteWarningsSnapshot> all()
    {
        Gen<Boolean> isEmpty = SourceDSL.booleans().all();
        Gen<WriteWarningsSnapshot> nonEmpty = nonEmpty();
        Gen<WriteWarningsSnapshot> gen = rs ->
            isEmpty.generate(rs) ? empty() : nonEmpty.generate(rs);
        return gen.describedAs(WriteWarningsSnapshot::toString);
    }

    private static Gen<WriteWarningsSnapshot> nonEmpty()
    {
        Gen<ThresholdCounter> counter = counter();
        Gen<WriteWarningsSnapshot> gen = rs -> {
            ThresholdCounter writeSize = counter.generate(rs);
            ThresholdCounter writeTombstone = counter.generate(rs);
            return create(writeSize, writeTombstone);
        };
        return gen.assuming(snapshot -> !snapshot.isEmpty()).describedAs(WriteWarningsSnapshot::toString);
    }

    private static Gen<ThresholdCounter> counter()
    {
        Gen<Boolean> isEmpty = SourceDSL.booleans().all();
        Constraint maxValue = Constraint.between(1, Long.MAX_VALUE);
        Gen<ImmutableSet<InetAddressAndPort>> instances = SourceDSL.arbitrary()
                                                                   .pick(ImmutableSet.of(REPLICA1),
                                                                         ImmutableSet.of(REPLICA2),
                                                                         ImmutableSet.of(REPLICA3),
                                                                         ImmutableSet.of(REPLICA1, REPLICA2));
        Gen<ThresholdCounter> gen = rs ->
            isEmpty.generate(rs) ? ThresholdCounter.empty()
                                 : new ThresholdCounter(instances.generate(rs), rs.next(maxValue));
        return gen.describedAs(ThresholdCounter::toString);
    }
}
