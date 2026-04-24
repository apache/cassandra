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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import org.apache.cassandra.schema.TableId;

import static org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot.create;
import static org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot.writeSizeWarnMessage;
import static org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot.writeTombstoneWarnMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class WriteWarningsSnapshotTest
{
    private static final TableId TABLE1 = TableId.fromUUID(new UUID(0, 1));
    private static final TableId TABLE2 = TableId.fromUUID(new UUID(0, 2));
    private static final WriteWarningsSnapshot EMPTY_SNAPSHOT = create(WriteThresholdCounter.empty(), WriteThresholdCounter.empty());

    @Test
    public void testCreateWithNonEmptyCounters()
    {
        WriteThresholdCounter sizeCounter = WriteThresholdCounter.create(map(TABLE1, 1024L));
        WriteThresholdCounter tombstoneCounter = WriteThresholdCounter.create(map(TABLE2, 500L));
        WriteWarningsSnapshot snapshot = create(sizeCounter, tombstoneCounter);

        assertThat(snapshot.isEmpty()).isFalse();
        assertThat(snapshot.writeSize).isEqualTo(sizeCounter);
        assertThat(snapshot.writeTombstone).isEqualTo(tombstoneCounter);
    }

    @Test
    public void testMergeWithNull()
    {
        WriteThresholdCounter sizeCounter = WriteThresholdCounter.create(map(TABLE1, 2048L));
        WriteWarningsSnapshot snapshot = create(sizeCounter, WriteThresholdCounter.empty());
        WriteWarningsSnapshot result = snapshot.merge(null);

        assertThat(result).isEqualTo(snapshot);
    }

    @Test
    public void testMergeSelfWithSelf()
    {
        qt().forAll(all()).check(snapshot -> snapshot.merge(snapshot).equals(snapshot));
    }

    @Test
    public void testMergeNonOverlappingTables()
    {
        WriteWarningsSnapshot snapshot1 = create(
            WriteThresholdCounter.create(map(TABLE1, 1024L)),
            WriteThresholdCounter.create(map(TABLE1, 100L))
        );

        WriteWarningsSnapshot snapshot2 = create(
            WriteThresholdCounter.create(map(TABLE2, 2048L)),
            WriteThresholdCounter.create(map(TABLE2, 200L))
        );

        WriteWarningsSnapshot merged = snapshot1.merge(snapshot2);

        assertThat(merged.writeSize.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(merged.writeSize.tableValues).containsEntry(TABLE2, 2048L);
        assertThat(merged.writeTombstone.tableValues).containsEntry(TABLE1, 100L);
        assertThat(merged.writeTombstone.tableValues).containsEntry(TABLE2, 200L);
    }

    @Test
    public void testMergeOverlappingTablesTakesMax()
    {
        WriteWarningsSnapshot snapshot1 = create(
            WriteThresholdCounter.create(map(TABLE1, 3000L)),
            WriteThresholdCounter.empty()
        );

        WriteWarningsSnapshot snapshot2 = create(
            WriteThresholdCounter.create(map(TABLE1, 4000L)),
            WriteThresholdCounter.empty()
        );

        WriteWarningsSnapshot merged = snapshot1.merge(snapshot2);

        assertThat(merged.writeSize.tableValues).containsEntry(TABLE1, 4000L);
        assertThat(merged.writeSize.tableValues).hasSize(1);
    }

    @Test
    public void testMergeDifferentThresholdTypes()
    {
        WriteWarningsSnapshot snapshot1 = create(
            WriteThresholdCounter.create(map(TABLE1, 5000L)),
            WriteThresholdCounter.empty()
        );

        WriteWarningsSnapshot snapshot2 = create(
            WriteThresholdCounter.empty(),
            WriteThresholdCounter.create(map(TABLE2, 300L))
        );

        WriteWarningsSnapshot merged = snapshot1.merge(snapshot2);

        assertThat(merged.writeSize.tableValues).containsEntry(TABLE1, 5000L);
        assertThat(merged.writeTombstone.tableValues).containsEntry(TABLE2, 300L);
    }

    @Test
    public void testWriteSizeWarnMessage()
    {
        String message = writeSizeWarnMessage(1048576L);
        assertThat(message).isEqualTo("Write to large partition; estimated size is 1048576 bytes (see write_size_warn_threshold)");
    }

    @Test
    public void testWriteTombstoneWarnMessage()
    {
        String message = writeTombstoneWarnMessage(500L);
        assertThat(message).isEqualTo("Write to partition with many tombstones; estimated count is 500 (see write_tombstone_warn_threshold)");
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

    private static Gen<WriteWarningsSnapshot> all()
    {
        Gen<Boolean> isEmpty = SourceDSL.booleans().all();
        Gen<WriteWarningsSnapshot> nonEmpty = nonEmpty();
        Gen<WriteWarningsSnapshot> gen = rs -> isEmpty.generate(rs) ? EMPTY_SNAPSHOT : nonEmpty.generate(rs);
        return gen.describedAs(WriteWarningsSnapshot::toString);
    }

    private static Gen<WriteWarningsSnapshot> nonEmpty()
    {
        Gen<WriteThresholdCounter> counter = counter();
        Gen<WriteWarningsSnapshot> gen = rs ->
        {
            WriteThresholdCounter writeSize = counter.generate(rs);
            WriteThresholdCounter writeTombstone = counter.generate(rs);
            return create(writeSize, writeTombstone);
        };
        return gen.assuming(snapshot -> !snapshot.isEmpty()).describedAs(WriteWarningsSnapshot::toString);
    }

    private static Gen<WriteThresholdCounter> counter()
    {
        Constraint maxValue = Constraint.between(1, Long.MAX_VALUE);
        Gen<WriteThresholdCounter> gen = rs ->
        {
            if (rs.next(Constraint.between(0, 3)) == 0)
                return WriteThresholdCounter.empty();
            Map<TableId, Long> values = new HashMap<>();
            values.put(TABLE1, rs.next(maxValue));
            if (rs.next(Constraint.between(0, 1)) == 1)
                values.put(TABLE2, rs.next(maxValue));
            return WriteThresholdCounter.create(values);
        };
        return gen.describedAs(WriteThresholdCounter::toString);
    }

    private static Map<TableId, Long> map(TableId t1, long v1)
    {
        Map<TableId, Long> m = new HashMap<>();
        m.put(t1, v1);
        return m;
    }
}
