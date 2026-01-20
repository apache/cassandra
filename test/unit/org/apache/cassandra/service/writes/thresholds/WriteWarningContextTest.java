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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ParamType;

import static org.assertj.core.api.Assertions.assertThat;

public class WriteWarningContextTest
{
    private static final InetAddressAndPort REPLICA1 = address(127, 0, 0, 1);
    private static final InetAddressAndPort REPLICA2 = address(127, 0, 0, 2);
    private static final InetAddressAndPort REPLICA3 = address(127, 0, 0, 3);

    @Test
    public void testIsSupported()
    {
        // Test with write size warning
        assertThat(WriteWarningContext.isSupported(EnumSet.of(ParamType.WRITE_SIZE_WARN))).isTrue();

        // Test with write tombstone warning
        assertThat(WriteWarningContext.isSupported(EnumSet.of(ParamType.WRITE_TOMBSTONE_WARN))).isTrue();

        // Test with both
        assertThat(WriteWarningContext.isSupported(EnumSet.of(
            ParamType.WRITE_SIZE_WARN,
            ParamType.WRITE_TOMBSTONE_WARN
        ))).isTrue();

        // Test with unsupported param types (read threshold params)
        assertThat(WriteWarningContext.isSupported(EnumSet.of(
            ParamType.TOMBSTONE_WARNING,
            ParamType.TOMBSTONE_FAIL
        ))).isFalse();

        // Test with empty set
        assertThat(WriteWarningContext.isSupported(EnumSet.noneOf(ParamType.class))).isFalse();
    }

    @Test
    public void testUpdateCountersWithSingleReplica()
    {
        WriteWarningContext context = new WriteWarningContext();

        Map<ParamType, Object> params = new HashMap<>();
        params.put(ParamType.WRITE_SIZE_WARN, 1024L);
        params.put(ParamType.WRITE_TOMBSTONE_WARN, 500);

        context.updateCounters(params, REPLICA1);

        WriteWarningsSnapshot snapshot = context.snapshot();
        assertThat(snapshot.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot.writeSize.maxValue).isEqualTo(1024L);
        assertThat(snapshot.writeTombstone.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot.writeTombstone.maxValue).isEqualTo(500L);
    }

    @Test
    public void testUpdateCountersWithMultipleReplicas()
    {
        WriteWarningContext context = new WriteWarningContext();

        // First replica reports warnings
        Map<ParamType, Object> params1 = new HashMap<>();
        params1.put(ParamType.WRITE_SIZE_WARN, 1024L);
        params1.put(ParamType.WRITE_TOMBSTONE_WARN, 500);
        context.updateCounters(params1, REPLICA1);

        // Second replica reports higher values
        Map<ParamType, Object> params2 = new HashMap<>();
        params2.put(ParamType.WRITE_SIZE_WARN, 2048L);
        params2.put(ParamType.WRITE_TOMBSTONE_WARN, 1000);
        context.updateCounters(params2, REPLICA2);

        // Third replica reports lower values
        Map<ParamType, Object> params3 = new HashMap<>();
        params3.put(ParamType.WRITE_SIZE_WARN, 512L);
        params3.put(ParamType.WRITE_TOMBSTONE_WARN, 300);
        context.updateCounters(params3, REPLICA3);

        WriteWarningsSnapshot snapshot = context.snapshot();

        // All replicas should be tracked
        assertThat(snapshot.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2, REPLICA3));
        assertThat(snapshot.writeTombstone.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2, REPLICA3));

        // Max values should be taken
        assertThat(snapshot.writeSize.maxValue).isEqualTo(2048L);
        assertThat(snapshot.writeTombstone.maxValue).isEqualTo(1000L);
    }

    @Test
    public void testUpdateCountersWithPartialWarnings()
    {
        WriteWarningContext context = new WriteWarningContext();

        // First replica only reports size warning
        Map<ParamType, Object> params1 = new HashMap<>();
        params1.put(ParamType.WRITE_SIZE_WARN, 1024L);
        context.updateCounters(params1, REPLICA1);

        // Second replica only reports tombstone warning
        Map<ParamType, Object> params2 = new HashMap<>();
        params2.put(ParamType.WRITE_TOMBSTONE_WARN, 500);
        context.updateCounters(params2, REPLICA2);

        WriteWarningsSnapshot snapshot = context.snapshot();

        assertThat(snapshot.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot.writeSize.maxValue).isEqualTo(1024L);
        assertThat(snapshot.writeTombstone.instances).isEqualTo(ImmutableSet.of(REPLICA2));
        assertThat(snapshot.writeTombstone.maxValue).isEqualTo(500L);
    }

    @Test
    public void testUpdateCountersWithEmptyParams()
    {
        WriteWarningContext context = new WriteWarningContext();

        Map<ParamType, Object> params = new HashMap<>();
        context.updateCounters(params, REPLICA1);

        WriteWarningsSnapshot snapshot = context.snapshot();
        assertThat(snapshot.isEmpty()).isTrue();
    }

    @Test
    public void testUpdateCountersWithUnsupportedParams()
    {
        WriteWarningContext context = new WriteWarningContext();

        // Params with unsupported types (read threshold params) should be ignored
        Map<ParamType, Object> params = new HashMap<>();
        params.put(ParamType.TOMBSTONE_WARNING, 100);
        params.put(ParamType.TOMBSTONE_FAIL, 200);
        context.updateCounters(params, REPLICA1);

        WriteWarningsSnapshot snapshot = context.snapshot();
        assertThat(snapshot.isEmpty()).isTrue();
    }

    @Test
    public void testUpdateCountersWithSameReplicaMultipleTimes()
    {
        WriteWarningContext context = new WriteWarningContext();

        // Same replica reports warnings multiple times
        Map<ParamType, Object> params1 = new HashMap<>();
        params1.put(ParamType.WRITE_SIZE_WARN, 1024L);
        context.updateCounters(params1, REPLICA1);

        Map<ParamType, Object> params2 = new HashMap<>();
        params2.put(ParamType.WRITE_SIZE_WARN, 2048L);
        context.updateCounters(params2, REPLICA1);

        WriteWarningsSnapshot snapshot = context.snapshot();

        // Replica should only be counted once
        assertThat(snapshot.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        // Max value should be taken
        assertThat(snapshot.writeSize.maxValue).isEqualTo(2048L);
    }

    @Test
    public void testSnapshotIsImmutable()
    {
        WriteWarningContext context = new WriteWarningContext();

        Map<ParamType, Object> params = new HashMap<>();
        params.put(ParamType.WRITE_SIZE_WARN, 1024L);
        context.updateCounters(params, REPLICA1);

        WriteWarningsSnapshot snapshot1 = context.snapshot();

        // Add more warnings
        Map<ParamType, Object> params2 = new HashMap<>();
        params2.put(ParamType.WRITE_SIZE_WARN, 2048L);
        context.updateCounters(params2, REPLICA2);

        WriteWarningsSnapshot snapshot2 = context.snapshot();

        // First snapshot should not be affected
        assertThat(snapshot1.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot1.writeSize.maxValue).isEqualTo(1024L);

        // Second snapshot should have both
        assertThat(snapshot2.writeSize.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2));
        assertThat(snapshot2.writeSize.maxValue).isEqualTo(2048L);
    }

    @Test
    public void testUpdateCountersWithLongAndIntegerValues()
    {
        WriteWarningContext context = new WriteWarningContext();

        Map<ParamType, Object> params = new HashMap<>();
        // Size is always Long
        params.put(ParamType.WRITE_SIZE_WARN, Long.MAX_VALUE);
        // Tombstone can be Integer
        params.put(ParamType.WRITE_TOMBSTONE_WARN, Integer.MAX_VALUE);

        context.updateCounters(params, REPLICA1);

        WriteWarningsSnapshot snapshot = context.snapshot();
        assertThat(snapshot.writeSize.maxValue).isEqualTo(Long.MAX_VALUE);
        assertThat(snapshot.writeTombstone.maxValue).isEqualTo(Integer.MAX_VALUE);
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
}