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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.thresholds.ThresholdCounter;

import static org.assertj.core.api.Assertions.assertThat;

public class WarnCounterTest
{
    private static final InetAddressAndPort REPLICA1 = address(127, 0, 0, 1);
    private static final InetAddressAndPort REPLICA2 = address(127, 0, 0, 2);
    private static final InetAddressAndPort REPLICA3 = address(127, 0, 0, 3);

    @Test
    public void testAddWarningSingleReplica()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(REPLICA1, 1024);

        ThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot.maxValue).isEqualTo(1024);
    }

    @Test
    public void testAddWarningMultipleReplicas()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(REPLICA1, 1024);
        counter.addWarning(REPLICA2, 2048);
        counter.addWarning(REPLICA3, 512);

        ThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2, REPLICA3));
        assertThat(snapshot.maxValue).isEqualTo(2048); // Max value
    }

    @Test
    public void testAddWarningSameReplicaMultipleTimes()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(REPLICA1, 1024);
        counter.addWarning(REPLICA1, 2048);
        counter.addWarning(REPLICA1, 512);

        ThresholdCounter snapshot = counter.snapshot();
        // Replica should only be added once
        assertThat(snapshot.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        // Max value should be taken
        assertThat(snapshot.maxValue).isEqualTo(2048);
    }

    @Test
    public void testMaxValueTracking()
    {
        WarnCounter counter = new WarnCounter();

        // Add in increasing order
        counter.addWarning(REPLICA1, 100);
        assertThat(counter.snapshot().maxValue).isEqualTo(100);

        counter.addWarning(REPLICA2, 500);
        assertThat(counter.snapshot().maxValue).isEqualTo(500);

        counter.addWarning(REPLICA3, 1000);
        assertThat(counter.snapshot().maxValue).isEqualTo(1000);

        // Add smaller value, should not change max
        counter.addWarning(REPLICA1, 200);
        assertThat(counter.snapshot().maxValue).isEqualTo(1000);
    }

    @Test
    public void testMaxValueTrackingDecreasingOrder()
    {
        WarnCounter counter = new WarnCounter();

        // Add in decreasing order
        counter.addWarning(REPLICA1, 1000);
        assertThat(counter.snapshot().maxValue).isEqualTo(1000);

        counter.addWarning(REPLICA2, 500);
        assertThat(counter.snapshot().maxValue).isEqualTo(1000);

        counter.addWarning(REPLICA3, 100);
        assertThat(counter.snapshot().maxValue).isEqualTo(1000);
    }

    @Test
    public void testMaxValueWithZero()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(REPLICA1, 0);

        ThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot.maxValue).isEqualTo(0);
    }

    @Test
    public void testMaxValueWithLargeLong()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(REPLICA1, Long.MAX_VALUE);
        counter.addWarning(REPLICA2, Long.MAX_VALUE - 1);

        ThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.maxValue).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testSnapshotIsImmutable()
    {
        WarnCounter counter = new WarnCounter();
        counter.addWarning(REPLICA1, 1024);

        ThresholdCounter snapshot1 = counter.snapshot();
        assertThat(snapshot1.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot1.maxValue).isEqualTo(1024);

        // Add more warnings
        counter.addWarning(REPLICA2, 2048);

        ThresholdCounter snapshot2 = counter.snapshot();

        // First snapshot should not be affected
        assertThat(snapshot1.instances).isEqualTo(ImmutableSet.of(REPLICA1));
        assertThat(snapshot1.maxValue).isEqualTo(1024);

        // Second snapshot should have both
        assertThat(snapshot2.instances).isEqualTo(ImmutableSet.of(REPLICA1, REPLICA2));
        assertThat(snapshot2.maxValue).isEqualTo(2048);
    }

    @Test
    public void testEmptyCounter()
    {
        WarnCounter counter = new WarnCounter();

        ThresholdCounter snapshot = counter.snapshot();
        assertThat(snapshot.instances).isEmpty();
        assertThat(snapshot.maxValue).isEqualTo(0); // Default value
    }

    @Test
    public void testConcurrentModification()
    {
        WarnCounter counter = new WarnCounter();

        // Simulate concurrent warnings from multiple replicas
        counter.addWarning(REPLICA1, 1000);
        counter.addWarning(REPLICA2, 2000);
        counter.addWarning(REPLICA3, 1500);

        // Get snapshot while adding more
        ThresholdCounter snapshot1 = counter.snapshot();
        counter.addWarning(REPLICA1, 3000);
        ThresholdCounter snapshot2 = counter.snapshot();

        // Both snapshots should be valid
        assertThat(snapshot1.instances).hasSize(3);
        assertThat(snapshot2.instances).hasSize(3);
        assertThat(snapshot2.maxValue).isEqualTo(3000);
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