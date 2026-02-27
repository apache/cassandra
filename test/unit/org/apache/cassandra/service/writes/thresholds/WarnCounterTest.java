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

import org.apache.cassandra.schema.TableId;

import static org.assertj.core.api.Assertions.assertThat;

public class WarnCounterTest
{
    private static final TableId TABLE1 = TableId.fromUUID(new UUID(0, 1));
    private static final TableId TABLE2 = TableId.fromUUID(new UUID(0, 2));
    private static final TableId TABLE3 = TableId.fromUUID(new UUID(0, 3));

    @Test
    public void testAddWarningSingleTable()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(map(TABLE1, 1024L));

        WriteThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(snapshot.tableValues).hasSize(1);
    }

    @Test
    public void testAddWarningMultipleTables()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(map(TABLE1, 1024L, TABLE2, 2048L));

        WriteThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(snapshot.tableValues).containsEntry(TABLE2, 2048L);
    }

    @Test
    public void testMaxValueTrackedPerTable()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(map(TABLE1, 1024L));
        counter.addWarning(map(TABLE1, 2048L));
        counter.addWarning(map(TABLE1, 512L));

        WriteThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.tableValues).containsEntry(TABLE1, 2048L);
    }

    @Test
    public void testMaxValueIndependentPerTable()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(map(TABLE1, 1000L, TABLE2, 500L));
        counter.addWarning(map(TABLE1, 500L, TABLE2, 1000L));

        WriteThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.tableValues).containsEntry(TABLE1, 1000L);
        assertThat(snapshot.tableValues).containsEntry(TABLE2, 1000L);
    }

    @Test
    public void testNewTableAddedOnSubsequentCall()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(map(TABLE1, 1024L));
        counter.addWarning(map(TABLE2, 2048L));

        WriteThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(snapshot.tableValues).containsEntry(TABLE2, 2048L);
    }

    @Test
    public void testSnapshotIsImmutable()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(map(TABLE1, 1024L));

        WriteThresholdCounter snapshot1 = counter.snapshot();

        counter.addWarning(map(TABLE1, 2048L));
        counter.addWarning(map(TABLE2, 512L));

        WriteThresholdCounter snapshot2 = counter.snapshot();

        // snapshot1 should not be affected by subsequent addWarning calls
        assertThat(snapshot1.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(snapshot1.tableValues).doesNotContainKey(TABLE2);

        assertThat(snapshot2.tableValues).containsEntry(TABLE1, 2048L);
        assertThat(snapshot2.tableValues).containsEntry(TABLE2, 512L);
    }

    @Test
    public void testEmptyCounter()
    {
        WarnCounter counter = new WarnCounter();

        WriteThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.isEmpty()).isTrue();
        assertThat(snapshot.tableValues).isEmpty();
    }

    @Test
    public void testEmptyMapDoesNothing()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(new HashMap<>());

        assertThat(counter.snapshot().isEmpty()).isTrue();
    }

    private static Map<TableId, Long> map(TableId t1, long v1)
    {
        Map<TableId, Long> m = new HashMap<>();
        m.put(t1, v1);
        return m;
    }

    private static Map<TableId, Long> map(TableId t1, long v1, TableId t2, long v2)
    {
        Map<TableId, Long> m = new HashMap<>();
        m.put(t1, v1);
        m.put(t2, v2);
        return m;
    }
}
