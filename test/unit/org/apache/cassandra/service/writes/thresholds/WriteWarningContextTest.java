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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.TableId;

import static org.assertj.core.api.Assertions.assertThat;

public class WriteWarningContextTest
{
    private static final TableId TABLE1 = TableId.fromUUID(new UUID(0, 1));
    private static final TableId TABLE2 = TableId.fromUUID(new UUID(0, 2));

    @Test
    public void testIsSupported()
    {
        assertThat(WriteWarningContext.isSupported(EnumSet.of(ParamType.WRITE_SIZE_WARN))).isTrue();
        assertThat(WriteWarningContext.isSupported(EnumSet.of(ParamType.WRITE_TOMBSTONE_WARN))).isTrue();
        assertThat(WriteWarningContext.isSupported(EnumSet.of(
            ParamType.WRITE_SIZE_WARN,
            ParamType.WRITE_TOMBSTONE_WARN
        ))).isTrue();

        // Read threshold params are not supported
        assertThat(WriteWarningContext.isSupported(EnumSet.of(
            ParamType.TOMBSTONE_WARNING,
            ParamType.TOMBSTONE_FAIL
        ))).isFalse();

        assertThat(WriteWarningContext.isSupported(EnumSet.noneOf(ParamType.class))).isFalse();
    }

    @Test
    public void testUpdateCountersWithSingleCall()
    {
        WriteWarningContext context = new WriteWarningContext();

        Map<ParamType, Object> params = new HashMap<>();
        params.put(ParamType.WRITE_SIZE_WARN, tableMap(TABLE1, 1024L));
        params.put(ParamType.WRITE_TOMBSTONE_WARN, tableMap(TABLE1, 500L));

        context.updateCounters(params);

        WriteWarningsSnapshot snapshot = context.snapshot();
        assertThat(snapshot.writeSize.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(snapshot.writeTombstone.tableValues).containsEntry(TABLE1, 500L);
    }

    @Test
    public void testUpdateCountersFromMultipleCalls()
    {
        WriteWarningContext context = new WriteWarningContext();

        // First call reports TABLE1 with lower values
        Map<ParamType, Object> params1 = new HashMap<>();
        params1.put(ParamType.WRITE_SIZE_WARN, tableMap(TABLE1, 1024L));
        params1.put(ParamType.WRITE_TOMBSTONE_WARN, tableMap(TABLE1, 500L));
        context.updateCounters(params1);

        // Second call reports TABLE1 with higher values and TABLE2
        Map<ParamType, Object> params2 = new HashMap<>();
        params2.put(ParamType.WRITE_SIZE_WARN, tableMap(TABLE1, 2048L));
        params2.put(ParamType.WRITE_TOMBSTONE_WARN, tableMap(TABLE2, 1000L));
        context.updateCounters(params2);

        WriteWarningsSnapshot snapshot = context.snapshot();

        // Max values per table should be taken
        assertThat(snapshot.writeSize.tableValues).containsEntry(TABLE1, 2048L);
        assertThat(snapshot.writeTombstone.tableValues).containsEntry(TABLE1, 500L);
        assertThat(snapshot.writeTombstone.tableValues).containsEntry(TABLE2, 1000L);
    }

    @Test
    public void testUpdateCountersWithPartialWarnings()
    {
        WriteWarningContext context = new WriteWarningContext();

        // First call only reports size warning
        Map<ParamType, Object> params1 = new HashMap<>();
        params1.put(ParamType.WRITE_SIZE_WARN, tableMap(TABLE1, 1024L));
        context.updateCounters(params1);

        // Second call only reports tombstone warning
        Map<ParamType, Object> params2 = new HashMap<>();
        params2.put(ParamType.WRITE_TOMBSTONE_WARN, tableMap(TABLE2, 500L));
        context.updateCounters(params2);

        WriteWarningsSnapshot snapshot = context.snapshot();

        assertThat(snapshot.writeSize.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(snapshot.writeTombstone.tableValues).containsEntry(TABLE2, 500L);
    }

    @Test
    public void testUpdateCountersWithEmptyParams()
    {
        WriteWarningContext context = new WriteWarningContext();

        context.updateCounters(new HashMap<>());

        assertThat(context.snapshot().isEmpty()).isTrue();
    }

    @Test
    public void testUpdateCountersWithUnsupportedParams()
    {
        WriteWarningContext context = new WriteWarningContext();

        Map<ParamType, Object> params = new HashMap<>();
        params.put(ParamType.TOMBSTONE_WARNING, 100);
        params.put(ParamType.TOMBSTONE_FAIL, 200);
        context.updateCounters(params);

        assertThat(context.snapshot().isEmpty()).isTrue();
    }

    @Test
    public void testUpdateCountersMaxValueTrackedPerTable()
    {
        WriteWarningContext context = new WriteWarningContext();

        context.updateCounters(paramsSize(tableMap(TABLE1, 1024L)));
        context.updateCounters(paramsSize(tableMap(TABLE1, 2048L)));
        context.updateCounters(paramsSize(tableMap(TABLE1, 512L)));

        WriteWarningsSnapshot snapshot = context.snapshot();
        assertThat(snapshot.writeSize.tableValues).containsEntry(TABLE1, 2048L);
    }

    @Test
    public void testSnapshotIsImmutable()
    {
        WriteWarningContext context = new WriteWarningContext();

        context.updateCounters(paramsSize(tableMap(TABLE1, 1024L)));
        WriteWarningsSnapshot snapshot1 = context.snapshot();

        context.updateCounters(paramsSize(tableMap(TABLE1, 2048L)));
        context.updateCounters(paramsSize(tableMap(TABLE2, 512L)));
        WriteWarningsSnapshot snapshot2 = context.snapshot();

        // First snapshot should not be affected by subsequent calls
        assertThat(snapshot1.writeSize.tableValues).containsEntry(TABLE1, 1024L);
        assertThat(snapshot1.writeSize.tableValues).doesNotContainKey(TABLE2);

        assertThat(snapshot2.writeSize.tableValues).containsEntry(TABLE1, 2048L);
        assertThat(snapshot2.writeSize.tableValues).containsEntry(TABLE2, 512L);
    }

    @Test
    public void testSizeAndTombstoneTrackedIndependently()
    {
        WriteWarningContext context = new WriteWarningContext();

        Map<ParamType, Object> params = new HashMap<>();
        params.put(ParamType.WRITE_SIZE_WARN, tableMap(TABLE1, Long.MAX_VALUE));
        params.put(ParamType.WRITE_TOMBSTONE_WARN, tableMap(TABLE2, 500L));

        context.updateCounters(params);

        WriteWarningsSnapshot snapshot = context.snapshot();
        assertThat(snapshot.writeSize.tableValues).containsEntry(TABLE1, Long.MAX_VALUE);
        assertThat(snapshot.writeTombstone.tableValues).containsEntry(TABLE2, 500L);
        assertThat(snapshot.writeSize.tableValues).doesNotContainKey(TABLE2);
        assertThat(snapshot.writeTombstone.tableValues).doesNotContainKey(TABLE1);
    }

    private static Map<TableId, Long> tableMap(TableId tableId, long value)
    {
        Map<TableId, Long> m = new HashMap<>();
        m.put(tableId, value);
        return m;
    }

    private static Map<ParamType, Object> paramsSize(Map<TableId, Long> sizeMap)
    {
        Map<ParamType, Object> params = new HashMap<>();
        params.put(ParamType.WRITE_SIZE_WARN, sizeMap);
        return params;
    }
}