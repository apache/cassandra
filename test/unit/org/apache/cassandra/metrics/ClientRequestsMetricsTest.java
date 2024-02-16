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

package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class ClientRequestsMetricsTest
{
    private static ClientRequestsMetrics c1;
    private static ClientRequestsMetrics c2;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();

        c1 = new ClientRequestsMetrics("tenant1");
        c2 = new ClientRequestsMetrics("tenant2");
    }

    @AfterClass
    public static void teardown()
    {
        releaseAll(c1);
        releaseAll(c2);
    }

    protected static void releaseAll(ClientRequestsMetrics ccrm)
    {
        ccrm.readMetrics.release();
        ccrm.rangeMetrics.release();
        ccrm.writeMetrics.release();
        ccrm.casWriteMetrics.release();
        ccrm.casReadMetrics.release();
        ccrm.viewWriteMetrics.release();
        for (ConsistencyLevel level : ConsistencyLevel.values())
        {
            ccrm.readMetricsForLevel(level).release();
            ccrm.writeMetricsForLevel(level).release();
        }
    }

    @Test
    public void testDefaultMetrics()
    {
        ClientRequestsMetricsProvider defaultMetricsProvider = ClientRequestsMetricsProvider.instance;
        assertThat(defaultMetricsProvider).isInstanceOf(ClientRequestsMetricsProvider.DefaultClientRequestsMetricsProvider.class);

        ClientRequestsMetrics defaultMetrics = defaultMetricsProvider.metrics("");
        assertThat(defaultMetrics).isInstanceOf(ClientRequestsMetrics.class);
    }

    @Test
    public void testReadMetrics()
    {
        // update tenant1 read metrics, tenant2 metrics remain 0
        updateClientRequestMetrics(c1.readMetrics);
        verifyClientRequestMetrics(c1.readMetrics, 1);
        verifyClientRequestMetrics(c2.readMetrics, 0);

        // update tenant2 read metrics, tenant1 metrics remain 1
        updateClientRequestMetrics(c2.readMetrics);
        verifyClientRequestMetrics(c1.readMetrics, 1);
        verifyClientRequestMetrics(c2.readMetrics, 1);
    }

    @Test
    public void testRangeMetrics()
    {
        // update tenant1 range metrics, tenant2 metrics remain 0
        updateClientRangeRequestMetrics(c1.rangeMetrics);
        verifyClientRangeRequestMetrics(c1.rangeMetrics, 1);
        verifyClientRangeRequestMetrics(c2.rangeMetrics, 0);

        // update tenant2 range metrics, tenant1 metrics remain 1
        updateClientRangeRequestMetrics(c2.rangeMetrics);
        verifyClientRangeRequestMetrics(c1.rangeMetrics, 1);
        verifyClientRangeRequestMetrics(c2.rangeMetrics, 1);
    }

    @Test
    public void testWriteMetrics()
    {
        // update tenant1 write metrics, tenant2 metrics remain 0
        updateClientWriteRequestMetrics(c1.writeMetrics);
        verifyClientWriteRequestMetrics(c1.writeMetrics, 1);
        verifyClientWriteRequestMetrics(c2.writeMetrics, 0);

        // update tenant2 write metrics, tenant1 metrics remain 1
        updateClientWriteRequestMetrics(c2.writeMetrics);
        verifyClientWriteRequestMetrics(c1.writeMetrics, 1);
        verifyClientWriteRequestMetrics(c2.writeMetrics, 1);
    }

    @Test
    public void testCasReadMetrics()
    {
        // update tenant1 cas metrics, tenant2 metrics remain 0
        updateCASClientRequestMetrics(c1.casReadMetrics);
        verifyCASClientRequestMetrics(c1.casReadMetrics, 1);
        verifyCASClientRequestMetrics(c2.casReadMetrics, 0);

        // update tenant2 cas metrics, tenant1 metrics remain 1
        updateCASClientRequestMetrics(c2.casReadMetrics);
        verifyCASClientRequestMetrics(c1.casReadMetrics, 1);
        verifyCASClientRequestMetrics(c2.casReadMetrics, 1);
    }

    @Test
    public void testViewWriteMetrics()
    {
        // update tenant1 view metrics, tenant2 metrics remain 0
        updateViewWriteMetrics(c1.viewWriteMetrics);
        verifyViewWriteMetrics(c1.viewWriteMetrics, 1);
        verifyViewWriteMetrics(c2.viewWriteMetrics, 0);

        // update tenant2 view metrics, tenant1 metrics remain 1
        updateViewWriteMetrics(c2.viewWriteMetrics);
        verifyViewWriteMetrics(c1.viewWriteMetrics, 1);
        verifyViewWriteMetrics(c2.viewWriteMetrics, 1);
    }

    @Test
    public void testCasWriteMetrics()
    {
        // update tenant1 cas write metrics, tenant2 metrics remain 0
        updateCASClientWriteRequestMetrics(c1.casWriteMetrics);
        verifyCASClientWriteRequestMetrics(c1.casWriteMetrics, 1);
        verifyCASClientWriteRequestMetrics(c2.casWriteMetrics, 0);

        // update tenant2 cas write metrics, tenant1 metrics remain 1
        updateCASClientWriteRequestMetrics(c2.casWriteMetrics);
        verifyCASClientWriteRequestMetrics(c1.casWriteMetrics, 1);
        verifyCASClientWriteRequestMetrics(c2.casWriteMetrics, 1);
    }

    @Test
    public void testReadMetricsMap()
    {
        for (ConsistencyLevel level : ConsistencyLevel.values())
        {
            // update tenant1 view metrics, tenant2 metrics remain 0
            updateClientRequestMetrics(c1.readMetricsForLevel(level));
            verifyClientRequestMetrics(c1.readMetricsForLevel(level), 1);
            verifyClientRequestMetrics(c2.readMetricsForLevel(level), 0);

            // update tenant2 view metrics, tenant1 metrics remain 1
            updateClientRequestMetrics(c2.readMetricsForLevel(level));
            verifyClientRequestMetrics(c1.readMetricsForLevel(level), 1);
            verifyClientRequestMetrics(c2.readMetricsForLevel(level), 1);
        }
    }

    @Test
    public void testWriteMetricsMap()
    {
        for (ConsistencyLevel level : ConsistencyLevel.values())
        {
            // update tenant1 view metrics, tenant2 metrics remain 0
            updateClientWriteRequestMetrics(c1.writeMetricsForLevel(level));
            verifyClientWriteRequestMetrics(c1.writeMetricsForLevel(level), 1);
            verifyClientWriteRequestMetrics(c2.writeMetricsForLevel(level), 0);

            // update tenant2 view metrics, tenant1 metrics remain 1
            updateClientWriteRequestMetrics(c2.writeMetricsForLevel(level));
            verifyClientWriteRequestMetrics(c1.writeMetricsForLevel(level), 1);
            verifyClientWriteRequestMetrics(c2.writeMetricsForLevel(level), 1);
        }
    }

    private void updateViewWriteMetrics(ViewWriteMetrics metrics)
    {
        metrics.viewWriteLatency.update(1, TimeUnit.MILLISECONDS);
        metrics.viewReplicasSuccess.inc();
        metrics.viewReplicasAttempted.inc(2);
        updateClientRequestMetrics(metrics);
    }

    private void updateCASClientWriteRequestMetrics(CASClientWriteRequestMetrics metrics)
    {
        metrics.overMaxPendingThreshold.inc();
        metrics.conditionNotMet.inc();
        metrics.mutationSize.update(1);
        updateCASClientRequestMetrics(metrics);
    }

    private void updateCASClientRequestMetrics(CASClientRequestMetrics metrics)
    {
        metrics.unfinishedCommit.inc();
        metrics.contention.update(1);
        updateClientRequestMetrics(metrics);
    }

    private void updateClientRangeRequestMetrics(ClientRangeRequestMetrics metrics)
    {
        metrics.roundTrips.update(1);
        updateClientRequestMetrics(metrics);
    }

    private void updateClientWriteRequestMetrics(ClientWriteRequestMetrics metrics)
    {
        metrics.mutationSize.update(1);
        updateClientRequestMetrics(metrics);
    }

    private void updateClientRequestMetrics(ClientRequestMetrics metrics)
    {
        metrics.timeouts.mark();
        metrics.failures.mark();
        metrics.unavailables.mark();
        updateLatencyMetrics(metrics.executionTimeMetrics);
        updateLatencyMetrics(metrics.serviceTimeMetrics);
    }

    private void updateLatencyMetrics(LatencyMetrics metrics)
    {
        metrics.latency.update(1, TimeUnit.MILLISECONDS);
        metrics.totalLatency.inc();
    }

    private void verifyViewWriteMetrics(ViewWriteMetrics metrics, int value)
    {
        assertEquals(value == 0 ? 0 : value + 1, metrics.viewReplicasAttempted.getCount());
        assertEquals(value, metrics.viewReplicasSuccess.getCount());
        assertEquals(value == 0 ? 0 : 1, metrics.viewPendingMutations.getValue().intValue());
        assertEquals(value, metrics.viewWriteLatency.getCount());
        verifyClientRequestMetrics(metrics, value);
    }

    private void verifyCASClientWriteRequestMetrics(CASClientWriteRequestMetrics metrics, int value)
    {
        assertEquals(value, metrics.mutationSize.getCount());
        assertEquals(value, metrics.conditionNotMet.getCount());
        assertEquals(value, metrics.overMaxPendingThreshold.getCount());
        verifyCASClientRequestMetrics(metrics, value);
    }

    private void verifyCASClientRequestMetrics(CASClientRequestMetrics metrics, int value)
    {
        assertEquals(value, metrics.unfinishedCommit.getCount());
        assertEquals(value, metrics.contention.getCount());
        verifyClientRequestMetrics(metrics, value);
    }

    private void verifyClientRangeRequestMetrics(ClientRangeRequestMetrics metrics, int value)
    {
        assertEquals(value, metrics.roundTrips.getCount());
        verifyClientRequestMetrics(metrics, value);
    }

    private void verifyClientWriteRequestMetrics(ClientWriteRequestMetrics metrics, int value)
    {
        assertEquals(value, metrics.mutationSize.getCount());
        verifyClientRequestMetrics(metrics, value);
    }

    private void verifyClientRequestMetrics(ClientRequestMetrics metrics, int value)
    {
        assertEquals(value, metrics.timeouts.getCount());
        assertEquals(value, metrics.failures.getCount());
        assertEquals(value, metrics.unavailables.getCount());
        verifyLatencyMetrics(metrics.executionTimeMetrics, value);
        verifyLatencyMetrics(metrics.serviceTimeMetrics, value);
    }

    private void verifyLatencyMetrics(LatencyMetrics metrics, int value)
    {
        assertEquals(value, metrics.latency.getCount());
        assertEquals(value, metrics.totalLatency.getCount());
    }
}
