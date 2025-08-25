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

package org.apache.cassandra.service.throttler.dynamic;

import com.codahale.metrics.Counter;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.service.RateLimiterService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetrics;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetricsManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class CassandraResourceUtilizationTest extends CQLTester
{
    private static final String ALL_SYSTEM_KEYSPACES_TABLES_AND_PINGLESS_TABLES = "^system.*\\..+|^pingless\\..+";
    private static final String KEYSPACE_THROTTLE = "ks_throttle";
    private static final String TABLE = "tbl";
    private static final Collection SINGLETON_TABLE = Collections.singleton(TABLE);

    private static final long VERY_LARGE_THREADPOOL_PENDING_TASKS = Long.MAX_VALUE;

    private static final AtomicBoolean hasDoneSetup = new AtomicBoolean(false);

    private static TableMetadata cfm;

    public CassandraResourceUtilizationTest()
    {
        requireNetwork();
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        cfm = TableMetadata.builder(KEYSPACE_THROTTLE, TABLE)
                           .addPartitionKeyColumn("k", UTF8Type.instance)
                           .addStaticColumn("s", UTF8Type.instance)
                           .addClusteringColumn("i", IntegerType.instance)
                           .addRegularColumn("v", UTF8Type.instance)
                           .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE_THROTTLE, KeyspaceParams.simple(1), cfm);
        cfm = Schema.instance.getTableMetadata(KEYSPACE_THROTTLE, TABLE);
    }

    @Test
    public void testTrafficType()
    {
        KeyspaceMetrics ksMetrics = Keyspace.open(KEYSPACE_THROTTLE).metric;
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);

        // RangeCoordRead
        Assert.assertFalse(TrafficType.RangeCoordRead.isWrite());
        Assert.assertTrue(TrafficType.RangeCoordRead.isRangeRead());
        Assert.assertTrue(TrafficType.RangeCoordRead.isCoordTraffic());

        // NonRangeCoordRead
        Assert.assertFalse(TrafficType.SinglePartitionCoordRead.isWrite());
        Assert.assertFalse(TrafficType.SinglePartitionCoordRead.isRangeRead());
        Assert.assertTrue(TrafficType.SinglePartitionCoordRead.isCoordTraffic());

        // RangeReplicaRead
        Assert.assertFalse(TrafficType.RangeReplicaRead.isWrite());
        Assert.assertTrue(TrafficType.RangeReplicaRead.isRangeRead());
        Assert.assertFalse(TrafficType.RangeReplicaRead.isCoordTraffic());

        // NonRangeReplicaRead
        Assert.assertFalse(TrafficType.SinglePartitionReplicaRead.isWrite());
        Assert.assertFalse(TrafficType.SinglePartitionReplicaRead.isRangeRead());
        Assert.assertFalse(TrafficType.SinglePartitionReplicaRead.isCoordTraffic());

        // CoordWrite
        Assert.assertTrue(TrafficType.CoordWrite.isWrite());
        Assert.assertFalse(TrafficType.CoordWrite.isRangeRead());
        Assert.assertTrue(TrafficType.CoordWrite.isCoordTraffic());

        // ReplicaWrite
        Assert.assertTrue(TrafficType.ReplicaWrite.isWrite());
        Assert.assertFalse(TrafficType.ReplicaWrite.isRangeRead());
        Assert.assertFalse(TrafficType.ReplicaWrite.isCoordTraffic());

        for (TrafficType t : TrafficType.values())
        {
            // verify TrafficType.getCulpritKeyspaceCache
            if (t.isWrite())
            {
                Assert.assertSame(t.getCulpritKeyspaceCache(), CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces);
            } else
            {
                if (t.isRangeRead())
                {
                    Assert.assertSame(t.getCulpritKeyspaceCache(), CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces);
                } else {
                    Assert.assertSame(t.getCulpritKeyspaceCache(), CulpritTrafficChecker.readAggressiveThrottlingKeyspaces);
                }
            }

            // verify TrafficType.CulpritLatencyMetricsSupplier
            if (t.isWrite())
            {
                Assert.assertSame(t.getCulpritLatencyMetricsSupplier().getLatencyMetrics(ksMetrics), ksMetrics.writeLatency);
            } else
            {
                if (t.isRangeRead())
                {
                    Assert.assertSame(t.getCulpritLatencyMetricsSupplier().getLatencyMetrics(ksMetrics), ksMetrics.rangeLatency);
                } else {
                    Assert.assertSame(t.getCulpritLatencyMetricsSupplier().getLatencyMetrics(ksMetrics), ksMetrics.readLatency);
                }
            }

            // verify TrafficType.CulpritKeyspaceAddedCounterSupplier
            if (t.isWrite())
            {
                Assert.assertSame(t.getCulpritKeyspaceAddedCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.addKSForWriteThrottling);
            } else
            {
                if (t.isRangeRead())
                {
                    Assert.assertSame(t.getCulpritKeyspaceAddedCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.addKSForRangeThrottling);
                } else {
                    Assert.assertSame(t.getCulpritKeyspaceAddedCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.addKSForReadThrottling);
                }
            }

            // verify TrafficType.RequestsTrendingUpwardCounterSupplier
            if (t.isWrite())
            {
                Assert.assertSame(t.getRequestsTrendingUpwardCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.writeRequestsTrendingUpward);
            } else
            {
                if (t.isRangeRead())
                {
                    Assert.assertSame(t.getRequestsTrendingUpwardCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.rangeRequestsTrendingUpward);
                } else {
                    Assert.assertSame(t.getRequestsTrendingUpwardCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.readRequestsTrendingUpward);
                }
            }

            // verify TrafficType.LatencyTrendingUpwardCounterSupplier
            if (t.isWrite())
            {
                Assert.assertSame(t.getLatencyTrendingUpwardCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.writeLatencyTrendingUpward);
            } else
            {
                if (t.isRangeRead())
                {
                    Assert.assertSame(t.getLatencyTrendingUpwardCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.rangeLatencyTrendingUpward);
                } else {
                    Assert.assertSame(t.getLatencyTrendingUpwardCounterSupplier().getCounter(ksThrottlingMetrics), ksThrottlingMetrics.readLatencyTrendingUpward);
                }
            }
        }
    }

    @Test
    public void testCpuUtil1()
    {
        ResourcesStats resourcesStats = getResourceStats();

        long cpuUtil1Cur = resourcesStats.getCpuUtil1Cur();
        long cpuUtil1OneMinute = resourcesStats.getCpuUtil1OneMinute();
        long cpuUtil1FiveMinute = resourcesStats.getCpuUtil1FiveMinute();
        long cpuUtil1FifteenMinute = resourcesStats.getCpuUtil1FifteenMinute();
        Assert.assertTrue("Current: " + cpuUtil1Cur, cpuUtil1Cur >= 0 && cpuUtil1Cur <= 100);
        Assert.assertTrue("OneMinute: " + cpuUtil1OneMinute, cpuUtil1OneMinute >= 0 && cpuUtil1OneMinute <= 100);
        Assert.assertTrue("FiveMinute: " + cpuUtil1FiveMinute, cpuUtil1FiveMinute >= 0 && cpuUtil1FiveMinute <= 100);
        Assert.assertTrue("FifteenMinute: " + cpuUtil1FifteenMinute, cpuUtil1FifteenMinute >= 0 && cpuUtil1FifteenMinute <= 100);

        // test out of range setCpuUtil1
        long count1 = resourcesStats.cpuUtil1Meter.getCount();
        resourcesStats.setCpuUtil1(-1);
        Assert.assertEquals(count1, resourcesStats.cpuUtil1Meter.getCount());
        resourcesStats.setCpuUtil1(101);
        Assert.assertEquals(count1, resourcesStats.cpuUtil1Meter.getCount());
    }

    @Test
    public void testCpuUtil2()
    {
        ResourcesStats resourcesStats = getResourceStats();

        long cpuUtil2Cur = resourcesStats.getCpuUtil2Cur();
        long cpuUtil2OneMinute = resourcesStats.getCpuUtil2OneMinute();
        long cpuUtil2FiveMinute = resourcesStats.getCpuUtil2FiveMinute();
        long cpuUtil2FifteenMinute = resourcesStats.getCpuUtil2FifteenMinute();
        Assert.assertTrue("Current: " + cpuUtil2Cur, cpuUtil2Cur >= 0 && cpuUtil2Cur <= 100);
        Assert.assertTrue("OneMinute: " + cpuUtil2OneMinute, cpuUtil2OneMinute >= 0 && cpuUtil2OneMinute <= 100);
        Assert.assertTrue("FiveMinute: " + cpuUtil2FiveMinute, cpuUtil2FiveMinute >= 0 && cpuUtil2FiveMinute <= 100);
        Assert.assertTrue("FifteenMinute: " + cpuUtil2FifteenMinute, cpuUtil2FifteenMinute >= 0 && cpuUtil2FifteenMinute <= 100);

        // test out of range setCpuUtil2
        long count1 = resourcesStats.cpuUtil2Meter.getCount();
        resourcesStats.setCpuUtil2(-1);
        Assert.assertEquals(count1, resourcesStats.cpuUtil2Meter.getCount());
        resourcesStats.setCpuUtil2(101);
        Assert.assertEquals(count1, resourcesStats.cpuUtil2Meter.getCount());
    }

    @Test
    public void testPendingReads()
    {
        ResourcesStats resourcesStats = getResourceStats();

        int pendingReadsCur = resourcesStats.getPendingReadsCur();
        long pendingReadsOneMinute = resourcesStats.getPendingReadsOneMinute();
        long pendingReadsFiveMinute = resourcesStats.getPendingReadsFiveMinute();
        long pendingReadsFifteenMinute = resourcesStats.getPendingReadsFifteenMinute();
        Assert.assertTrue("Current: " + pendingReadsCur, pendingReadsCur >= 0);
        Assert.assertTrue("OneMinute: " + pendingReadsOneMinute, pendingReadsOneMinute >= 0);
        Assert.assertTrue("FiveMinute: " + pendingReadsFiveMinute, pendingReadsFiveMinute >= 0);
        Assert.assertTrue("FifteenMinute: " + pendingReadsFifteenMinute, pendingReadsFifteenMinute >= 0);
    }

    @Test
    public void testPendingMutations()
    {
        ResourcesStats resourcesStats = getResourceStats();

        int pendingMutationsCur = resourcesStats.getPendingMutationsCur();
        long pendingMutationsOneMinute = resourcesStats.getPendingMutationsOneMinute();
        long pendingMutationsFiveMinute = resourcesStats.getPendingMutationsFiveMinute();
        long pendingMutationsFifteenMinute = resourcesStats.getPendingMutationsFifteenMinute();
        Assert.assertTrue("Current: " + pendingMutationsCur, pendingMutationsCur >= 0);
        Assert.assertTrue("OneMinute: " + pendingMutationsOneMinute, pendingMutationsOneMinute >= 0);
        Assert.assertTrue("FiveMinute: " + pendingMutationsFiveMinute, pendingMutationsFiveMinute >= 0);
        Assert.assertTrue("FifteenMinute: " + pendingMutationsFifteenMinute, pendingMutationsFifteenMinute >= 0);
    }

    @Test
    public void throttlingOptionIsSingleton()
    {
        Assert.assertTrue(CassandraResourceUtilization.instance.throttlingOptions ==
                RateLimiterService.instance.getThrottlingOptions());
        Assert.assertTrue(CassandraResourceUtilization.instance.throttlingOptions ==
                DatabaseDescriptor.getThrottlingOptions());
    }

    @Test
    public void testNoThrottling()
    {
        long originalCpuThresholdOneMinute = CassandraResourceUtilization.instance.throttlingOptions.getCpuThresholdOneMinute();
        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(99);

        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.fetchCurrentHealth();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());

        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(originalCpuThresholdOneMinute);
    }

    @Test
    public void testNoThrottlingOnlyWithPendingReads()
    {
        long originalCpuThresholdOneMinute = CassandraResourceUtilization.instance.throttlingOptions.getCpuThresholdOneMinute();
        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(99);

        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setPendingReads(100);
        cassandraResourceUtilization.checkSignals();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());

        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(originalCpuThresholdOneMinute);
    }

    @Test
    public void testNoThrottlingOnlyWithPendingMutations()
    {
        long originalCpuThresholdOneMinute = CassandraResourceUtilization.instance.throttlingOptions.getCpuThresholdOneMinute();
        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(99);

        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setPendingMutations(100);
        cassandraResourceUtilization.checkSignals();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());

        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(originalCpuThresholdOneMinute);
    }

    @Test
    public void testYesThrottlingDueToCpuSignal()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());

        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());
    }

    @Test
    public void testYesThrottlingDueToThreadpoolSignal()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;

        // make sure no CPU signal
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(99);

        // before reduce the threedpool singal threshold
        cassandraResourceUtilization.checkSignals();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        // reduce the threshold for threedpool singal, for reads
        cassandraResourceUtilization.throttlingOptions.setThreadpoolThresholdReads(0);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        // set back the threshold for threedpool singal
        cassandraResourceUtilization.throttlingOptions.setThreadpoolThresholdReads(VERY_LARGE_THREADPOOL_PENDING_TASKS);
        cassandraResourceUtilization.checkSignals();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        // reduce the threshold for threedpool singal, for writes
        cassandraResourceUtilization.throttlingOptions.setThreadpoolThresholdWrites(0);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        // set back the threshold for threedpool singal
        cassandraResourceUtilization.throttlingOptions.setThreadpoolThresholdWrites(VERY_LARGE_THREADPOOL_PENDING_TASKS);
        cassandraResourceUtilization.checkSignals();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        // reduce the threshold for threedpool singal, for native transport
        cassandraResourceUtilization.throttlingOptions.setThreadpoolThresholdNativeTransport(0);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        // set back the threshold for threedpool singal
        cassandraResourceUtilization.throttlingOptions.setThreadpoolThresholdNativeTransport(VERY_LARGE_THREADPOOL_PENDING_TASKS);
        cassandraResourceUtilization.checkSignals();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
    }

    @Test
    public void testResetThrottlingNoop()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        // when we invoke "adjustThrottling", then it should be a noop as  reset the throttling since "oldestThrottlingIndicatorTimeInMS = 0" as
        // well as lastThrottlingIndicatorTimeInMS=0
        cassandraResourceUtilization.adjustThrottling();

        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());
    }

    @Test
    public void testResetThrottlingTrackOldThrottlingIndicatorTime()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        long lastThrottlingIndicatorTime = System.currentTimeMillis();
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;

        // when we invoke "adjustThrottling", then it should override "oldestThrottlingIndicatorTimeInMS" value to
        // lastThrottlingIndicatorTimeInMS
        cassandraResourceUtilization.adjustThrottling();

        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());
    }

    @Test
    public void testResetThrottlingIncreaseThrottlingAndThenReset()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        long lastThrottlingIndicatorTime = System.currentTimeMillis();
        long oldestThrottlingIndicatorTimeInMS = System.currentTimeMillis() - SECONDS.toMillis(cassandraResourceUtilization.throttlingOptions.getMoreAggressiveThrottlingAfterInSec() + 1);
        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = oldestThrottlingIndicatorTimeInMS;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(oldestThrottlingIndicatorTimeInMS, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.put("test_keyspace", true);

        // this should linearly increase the throttling
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1 * 2, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(1, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1 + 0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());

        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = oldestThrottlingIndicatorTimeInMS;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1 + 0.1 + 0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);


        lastThrottlingIndicatorTime = System.currentTimeMillis() - SECONDS.toMillis(cassandraResourceUtilization.throttlingOptions.getResetAfterNoThrottlingSeenInSec() + 1);
        oldestThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = oldestThrottlingIndicatorTimeInMS;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(oldestThrottlingIndicatorTimeInMS, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);

        // this should reset the throttling
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(2, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());
    }

    @Test
    public void testDoNotIncreaseThrottlingPercentageIfNoIndicatorUpdateSinceLastCheckpoint()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);

        long lastThrottlingIndicatorTime = System.currentTimeMillis();
        long oldestThrottlingIndicatorTimeInMS = System.currentTimeMillis() - SECONDS.toMillis(cassandraResourceUtilization.throttlingOptions.getMoreAggressiveThrottlingAfterInSec() + 1);
        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = oldestThrottlingIndicatorTimeInMS;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;

        // this should increase the throttling
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0.2, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);

        // this should not increase the throttling
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.2, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);

        // this should not increase the throttling
        lastThrottlingIndicatorTime = System.currentTimeMillis() - SECONDS.toMillis(cassandraResourceUtilization.throttlingOptions.getMoreAggressiveThrottlingAfterInSec() + 1);;
        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = lastThrottlingIndicatorTime;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0.2, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
    }

    @Test
    public void testValidateCpuThrottlingCalculation()
    {
        long originalCpuThresholdOneMinute = CassandraResourceUtilization.instance.throttlingOptions.getCpuThresholdOneMinute();
        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(99);

        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        cassandraResourceUtilization.fetchCurrentHealth();

        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        cassandraResourceUtilization.fetchCurrentHealth();

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(2, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());

        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(originalCpuThresholdOneMinute);
    }

    @Test
    public void testThrottleUserTrafficWhenSetupIsIncomplete()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.isSetupComplete = false;

        Map<String, String[]> tables = new HashMap<>();
        tables.put("system", new String[]{"local"});
        tables.put("system_auth", new String[]{"roles"});
        tables.put(KEYSPACE_THROTTLE, new String[]{TABLE, "non_existing_table"});
        tables.put("non_existing_ks", new String[]{"tb1", "tb2"});

        for (Map.Entry<String, String[]> entry : tables.entrySet())
        {
            String keyspace = entry.getKey();
            String[] tablesInKeyspace = entry.getValue();
            for (String table : tablesInKeyspace)
            {
                for (TrafficType trafficType : TrafficType.values())
                {
                    Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(keyspace,
                            Collections.singleton(table), trafficType));
                }
            }
        }
    }

    @Test
    public void testIgnoreTablesFilterWithKeyspaceCreateAndDrop()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        ThrottlingOptions throttlingOptions = cassandraResourceUtilization.throttlingOptions;
        throttlingOptions.setIgnoreTablesRegex(ALL_SYSTEM_KEYSPACES_TABLES_AND_PINGLESS_TABLES + "|^cass_ru_test_keyspace.test_table");
        cassandraResourceUtilization.syncIgnoreTablesFilter();

        String newKeyspace = "cass_ru_test_keyspace";
        String newTable = "test_table";
        Assert.assertFalse(cassandraResourceUtilization.ignoreTablesFilter.matches(newKeyspace, newTable));

        TableMetadata tableMetadata = TableMetadata.builder(newKeyspace, newTable)
                .addPartitionKeyColumn("k", UTF8Type.instance)
                .addStaticColumn("s", UTF8Type.instance)
                .addClusteringColumn("i", IntegerType.instance)
                .addRegularColumn("v", UTF8Type.instance)
                .build();

        // create keyspace
        SchemaLoader.createKeyspace(newKeyspace, KeyspaceParams.simple(1), tableMetadata);
        Assert.assertTrue(cassandraResourceUtilization.ignoreTablesFilter.matches(newKeyspace, newTable));

        // drop keyspace
        schemaChange(String.format("DROP KEYSPACE %s", newKeyspace));
        Assert.assertFalse(cassandraResourceUtilization.ignoreTablesFilter.matches(newKeyspace, newTable));
    }

    @Test
    public void testSkipSystemKS()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.isSetupComplete = true;

        KeyspaceThrottlingMetrics systemAuthMetrics = KeyspaceThrottlingMetricsManager.getMetrics("system_auth");
        Assert.assertEquals(0, systemAuthMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic("system_auth", Collections.singleton("roles"), TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(1, systemAuthMetrics.skipKSThrottling.getCount());

        KeyspaceThrottlingMetrics systemMetrics = KeyspaceThrottlingMetricsManager.getMetrics("system_traces");
        Assert.assertEquals(0, systemMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic("system_traces", Collections.singleton("events"), TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(1, systemMetrics.skipKSThrottling.getCount());

        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertEquals(0, userKSMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(0, userKSMetrics.skipKSThrottling.getCount());
    }

    @Test
    public void testSkipUserKeyspace()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;

        cassandraResourceUtilization.throttlingOptions.setIgnoreTablesRegex(ALL_SYSTEM_KEYSPACES_TABLES_AND_PINGLESS_TABLES +
                "|" + KEYSPACE_THROTTLE + "\\." + TABLE);
        cassandraResourceUtilization.syncIgnoreTablesFilter();
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertEquals(0, userKSMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(1, userKSMetrics.skipKSThrottling.getCount());
    }

    @Test
    public void testHardBlockSinglePartitionCoordReads()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.throttlingOptions.setHardBlockSinglePartitionCoordReadsTablesRegex(KEYSPACE_THROTTLE + "\\." + TABLE);
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.syncHardBlockSinglePartitionCoordReadsTablesFilter();

        Collection<String> tables = new LinkedList<>();
        tables.add("other_table1");
        tables.add("other_table2");
        Assert.assertEquals(0, userKSMetrics.hardBlockSinglePartitionCoordReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(0, userKSMetrics.hardBlockSinglePartitionCoordReads.getCount());
        tables.add(TABLE);
        Assert.assertEquals(0, userKSMetrics.hardBlockSinglePartitionCoordReads.getCount());
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockSinglePartitionCoordReads.getCount());

        // should not block with regex being empty
        cassandraResourceUtilization.throttlingOptions.setHardBlockSinglePartitionCoordReadsTablesRegex("");
        cassandraResourceUtilization.syncHardBlockSinglePartitionCoordReadsTablesFilter();
        Assert.assertEquals(1, userKSMetrics.hardBlockSinglePartitionCoordReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockSinglePartitionCoordReads.getCount());
    }

    @Test
    public void testHardBlockSinglePartitionReplicaReads()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.throttlingOptions.setHardBlockSinglePartitionReplicaReadsTablesRegex(KEYSPACE_THROTTLE + "\\." + TABLE);
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.syncHardBlockSinglePartitionReplicaReadsTablesFilter();

        Collection<String> tables = new LinkedList<>();
        tables.add("other_table1");
        tables.add("other_table2");
        Assert.assertEquals(0, userKSMetrics.hardBlockSinglePartitionReplicaReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.SinglePartitionReplicaRead));
        Assert.assertEquals(0, userKSMetrics.hardBlockSinglePartitionReplicaReads.getCount());
        tables.add(TABLE);
        Assert.assertEquals(0, userKSMetrics.hardBlockSinglePartitionReplicaReads.getCount());
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.SinglePartitionReplicaRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockSinglePartitionReplicaReads.getCount());

        // should not block with regex being empty
        cassandraResourceUtilization.throttlingOptions.setHardBlockSinglePartitionReplicaReadsTablesRegex("");
        cassandraResourceUtilization.syncHardBlockSinglePartitionReplicaReadsTablesFilter();
        Assert.assertEquals(1, userKSMetrics.hardBlockSinglePartitionReplicaReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.SinglePartitionReplicaRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockSinglePartitionReplicaReads.getCount());
    }

    @Test
    public void testHardBlockRangeCoordReads()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.throttlingOptions.setHardBlockRangeCoordReadsTablesRegex(KEYSPACE_THROTTLE + "\\." + TABLE);
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.syncHardBlockRangeCoordReadsTablesFilter();

        Collection<String> tables = new LinkedList<>();
        tables.add("other_table1");
        tables.add("other_table2");
        Assert.assertEquals(0, userKSMetrics.hardBlockRangeCoordReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.RangeCoordRead));
        Assert.assertEquals(0, userKSMetrics.hardBlockRangeCoordReads.getCount());
        tables.add(TABLE);
        Assert.assertEquals(0, userKSMetrics.hardBlockRangeCoordReads.getCount());
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.RangeCoordRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockRangeCoordReads.getCount());

        // should not block with regex being empty
        cassandraResourceUtilization.throttlingOptions.setHardBlockRangeCoordReadsTablesRegex("");
        cassandraResourceUtilization.syncHardBlockRangeCoordReadsTablesFilter();
        Assert.assertEquals(1, userKSMetrics.hardBlockRangeCoordReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.RangeCoordRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockRangeCoordReads.getCount());
    }

    @Test
    public void testHardBlockRangeReplicaReads()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.throttlingOptions.setHardBlockRangeReplicaReadsTablesRegex(KEYSPACE_THROTTLE + "\\." + TABLE);
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.syncHardBlockRangeReplicaReadsTablesFilter();

        Collection<String> tables = new LinkedList<>();
        tables.add("other_table1");
        tables.add("other_table2");
        Assert.assertEquals(0, userKSMetrics.hardBlockRangeReplicaReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.RangeReplicaRead));
        Assert.assertEquals(0, userKSMetrics.hardBlockRangeReplicaReads.getCount());
        tables.add(TABLE);
        Assert.assertEquals(0, userKSMetrics.hardBlockRangeReplicaReads.getCount());
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.RangeReplicaRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockRangeReplicaReads.getCount());

        // should not block with regex being empty
        cassandraResourceUtilization.throttlingOptions.setHardBlockRangeReplicaReadsTablesRegex("");
        cassandraResourceUtilization.syncHardBlockRangeReplicaReadsTablesFilter();
        Assert.assertEquals(1, userKSMetrics.hardBlockRangeReplicaReads.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.RangeReplicaRead));
        Assert.assertEquals(1, userKSMetrics.hardBlockRangeReplicaReads.getCount());
    }

    @Test
    public void testHardBlockCoordWrites()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.throttlingOptions.setHardBlockCoordWritesTablesRegex(KEYSPACE_THROTTLE + "\\." + TABLE);
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.syncHardBlockCoordWritesTablesFilter();

        Collection<String> tables = new LinkedList<>();
        tables.add("other_table1");
        tables.add("other_table2");
        Assert.assertEquals(0, userKSMetrics.hardBlockCoordWrites.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.CoordWrite));
        Assert.assertEquals(0, userKSMetrics.hardBlockCoordWrites.getCount());
        tables.add(TABLE);
        Assert.assertEquals(0, userKSMetrics.hardBlockCoordWrites.getCount());
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.CoordWrite));
        Assert.assertEquals(1, userKSMetrics.hardBlockCoordWrites.getCount());

        // should not block with regex being empty
        cassandraResourceUtilization.throttlingOptions.setHardBlockCoordWritesTablesRegex("");
        cassandraResourceUtilization.syncHardBlockCoordWritesTablesFilter();
        Assert.assertEquals(1, userKSMetrics.hardBlockCoordWrites.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.CoordWrite));
        Assert.assertEquals(1, userKSMetrics.hardBlockCoordWrites.getCount());
    }

    @Test
    public void testHardBlockReplicaWrites()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.throttlingOptions.setHardBlockReplicaWritesTablesRegex(KEYSPACE_THROTTLE + "\\." + TABLE);
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.syncHardBlockReplicaWritesTablesFilter();

        Collection<String> tables = new LinkedList<>();
        tables.add("other_table1");
        tables.add("other_table2");
        Assert.assertEquals(0, userKSMetrics.hardBlockReplicaWrites.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.ReplicaWrite));
        Assert.assertEquals(0, userKSMetrics.hardBlockReplicaWrites.getCount());
        tables.add(TABLE);
        Assert.assertEquals(0, userKSMetrics.hardBlockReplicaWrites.getCount());
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.ReplicaWrite));
        Assert.assertEquals(1, userKSMetrics.hardBlockReplicaWrites.getCount());

        // should not block with regex being empty
        cassandraResourceUtilization.throttlingOptions.setHardBlockReplicaWritesTablesRegex("");
        cassandraResourceUtilization.syncHardBlockReplicaWritesTablesFilter();
        Assert.assertEquals(1, userKSMetrics.hardBlockReplicaWrites.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, tables, TrafficType.ReplicaWrite));
        Assert.assertEquals(1, userKSMetrics.hardBlockReplicaWrites.getCount());
    }

    @Test
    public void testTableFilterWithSchemaChanges()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        ThrottlingOptions throttlingOptions = cassandraResourceUtilization.throttlingOptions;

        String newKeyspace = "cass_ru_test_keyspace";
        String newTable = "test_table";
        String regex = ".*\\.test_table"; // any keyspace, test_table table

        // Before adding table && Before regex change, should not block
        assertAllHardBlockerFilters(newKeyspace, newTable, false);

        // After regex change && before adding table, should not block
        throttlingOptions.setHardBlockSinglePartitionCoordReadsTablesRegex(regex);
        cassandraResourceUtilization.syncHardBlockSinglePartitionCoordReadsTablesFilter();

        throttlingOptions.setHardBlockSinglePartitionReplicaReadsTablesRegex(regex);
        cassandraResourceUtilization.syncHardBlockSinglePartitionReplicaReadsTablesFilter();

        throttlingOptions.setHardBlockRangeCoordReadsTablesRegex(regex);
        cassandraResourceUtilization.syncHardBlockRangeCoordReadsTablesFilter();

        throttlingOptions.setHardBlockRangeReplicaReadsTablesRegex(regex);
        cassandraResourceUtilization.syncHardBlockRangeReplicaReadsTablesFilter();

        throttlingOptions.setHardBlockCoordWritesTablesRegex(regex);
        cassandraResourceUtilization.syncHardBlockCoordWritesTablesFilter();

        throttlingOptions.setHardBlockReplicaWritesTablesRegex(regex);
        cassandraResourceUtilization.syncHardBlockReplicaWritesTablesFilter();
        assertAllHardBlockerFilters(newKeyspace, newTable, false);

        // After regex change && after adding table, should block
        TableMetadata tableMetadata = TableMetadata.builder(newKeyspace, newTable)
                .addPartitionKeyColumn("k", UTF8Type.instance)
                .addStaticColumn("s", UTF8Type.instance)
                .addClusteringColumn("i", IntegerType.instance)
                .addRegularColumn("v", UTF8Type.instance)
                .build();
        SchemaLoader.createKeyspace(newKeyspace, KeyspaceParams.simple(1), tableMetadata);
        assertAllHardBlockerFilters(newKeyspace, newTable, true);

        // After regex change && after dropping table, should not block
        schemaChange(String.format("DROP KEYSPACE %s", newKeyspace));
        assertAllHardBlockerFilters(newKeyspace, newTable, false);
    }

    @Test
    public void testThrottleUserTraffic()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.currentThrottlingPercentage = 1.0;
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.readLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForRangeThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxReadThrottling.getCount()); //inc
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.size());

        cassandraResourceUtilization.currentThrottlingPercentage = 0.0;
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.readLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForRangeThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noReadThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(0, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.size());

        // throttle single partition read traffic (current throttling is at 0.9, and 0.1 will be added, so it will be full throttling)
        cassandraResourceUtilization.currentThrottlingPercentage = 0.9;
        CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.put(KEYSPACE_THROTTLE, true);
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.SinglePartitionCoordRead));
        Assert.assertEquals(1, userKSMetrics.aggressiveThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.readLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForRangeThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.maxReadThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(1, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.size());

        // do not throttle range read traffic just because readAggressiveThrottlingKeyspaces is set
        cassandraResourceUtilization.currentThrottlingPercentage = 0.0;
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.RangeCoordRead));
        Assert.assertEquals(1, userKSMetrics.aggressiveThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.readLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForRangeThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.noReadThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(1, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.size());

        // throttle range read traffic (current throttling is at 0.9, and 0.1 will be added, so it will be full throttling)
        cassandraResourceUtilization.currentThrottlingPercentage = 0.9;
        CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.put(KEYSPACE_THROTTLE, true);
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.RangeCoordRead));
        Assert.assertEquals(2, userKSMetrics.aggressiveThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.readLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForRangeThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(3, userKSMetrics.maxReadThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(1, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(1, CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.size());

        // do not throttle write traffic if both readAggressiveThrottlingKeyspaces and rangeReadAggressiveThrottlingKeyspaces is set
        cassandraResourceUtilization.currentThrottlingPercentage = 0.0;
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.CoordWrite));
        Assert.assertEquals(2, userKSMetrics.aggressiveThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.readLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForRangeThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noWriteThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(3, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(1, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(1, CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(0, CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.size());

        // throttle write traffic if mutationAggressiveThrottlingKeyspaces is set
        // (current throttling is at 0.9, and 0.1 will be added, so it will be full throttling)
        cassandraResourceUtilization.currentThrottlingPercentage = 0.9;
        CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.put(KEYSPACE_THROTTLE, true);
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, SINGLETON_TABLE, TrafficType.CoordWrite));
        Assert.assertEquals(3, userKSMetrics.aggressiveThrottling.getCount()); // inc
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.readLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.rangeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeLatencyTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForRangeThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(3, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxWriteThrottling.getCount()); // inc
        Assert.assertEquals(1, CulpritTrafficChecker.readAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(1, CulpritTrafficChecker.rangeReadAggressiveThrottlingKeyspaces.size());
        Assert.assertEquals(1, CulpritTrafficChecker.mutationAggressiveThrottlingKeyspaces.size()); // inc
    }

    @Test
    public void testThrottleIfOnlyReadTrafficIsSaturated()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        cassandraResourceUtilization.throttlingOptions.setPendingReadsThresholdOneMinute(0);
        cassandraResourceUtilization.throttlingOptions.setPendingMutationsThresholdOneMinute(10);
        cassandraResourceUtilization.throttlingOptions.setPendingNativeTransportThresholdOneMinute(10);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
    }

    @Test
    public void testThrottleIfOnlyWriteTrafficIsSaturated()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        cassandraResourceUtilization.throttlingOptions.setPendingReadsThresholdOneMinute(10);
        cassandraResourceUtilization.throttlingOptions.setPendingMutationsThresholdOneMinute(0);
        cassandraResourceUtilization.throttlingOptions.setPendingNativeTransportThresholdOneMinute(10);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
    }

    @Test
    public void testThrottleIfOnlyNativeTransportIsSaturated()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        cassandraResourceUtilization.throttlingOptions.setPendingReadsThresholdOneMinute(10);
        cassandraResourceUtilization.throttlingOptions.setPendingMutationsThresholdOneMinute(10);
        cassandraResourceUtilization.throttlingOptions.setPendingNativeTransportThresholdOneMinute(0);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
    }

    @Test
    public void testUpwardFunction()
    {
        Assert.assertTrue(CulpritTrafficChecker.isTrendingUpward(30, 20.0, 10.0));
        Assert.assertFalse(CulpritTrafficChecker.isTrendingUpward(20.0, 10.0, 30.0));
        Assert.assertFalse(CulpritTrafficChecker.isTrendingUpward(20.0, 40.0, 30.0));
    }

    @Test
    public void testNoSpikeInRequestRate()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        KeyspaceMetrics keyspaceMetrics = Keyspace.open(KEYSPACE_THROTTLE).metric;
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertFalse(CulpritTrafficChecker.spikeInRequestRate(KEYSPACE_THROTTLE, TrafficType.SinglePartitionCoordRead, keyspaceMetrics, ksThrottlingMetrics, cassandraResourceUtilization.throttlingOptions));
        Assert.assertFalse(CulpritTrafficChecker.spikeInRequestRate(KEYSPACE_THROTTLE, TrafficType.CoordWrite, keyspaceMetrics, ksThrottlingMetrics, cassandraResourceUtilization.throttlingOptions));
    }

    @Test
    public void testNoSpikeInLatency()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        KeyspaceMetrics keyspaceMetrics = Keyspace.open(KEYSPACE_THROTTLE).metric;
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertFalse(CulpritTrafficChecker.spikeInLatency(KEYSPACE_THROTTLE, TrafficType.SinglePartitionCoordRead, keyspaceMetrics, ksThrottlingMetrics, cassandraResourceUtilization.throttlingOptions));
        Assert.assertFalse(CulpritTrafficChecker.spikeInLatency(KEYSPACE_THROTTLE, TrafficType.CoordWrite, keyspaceMetrics, ksThrottlingMetrics, cassandraResourceUtilization.throttlingOptions));
    }

    @Test
    public void testCassandraIsNotNormal()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);


        StorageService.instance.setOperationMode(StorageService.Mode.LEAVING);
        cassandraResourceUtilization.fetchCurrentHealth();
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        StorageService.instance.setOperationMode(StorageService.Mode.NORMAL);
        cassandraResourceUtilization.fetchCurrentHealth();
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());


        StorageService.instance.setOperationMode(StorageService.Mode.JOINING);
        cassandraResourceUtilization.fetchCurrentHealth();
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        StorageService.instance.setOperationMode(StorageService.Mode.NORMAL);
        cassandraResourceUtilization.fetchCurrentHealth();
        Assert.assertEquals(2, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
    }

    @Test(expected = OverloadedException.class)
    public void testThrowOverloadedException()
    {
        throw CassandraResourceUtilization.buildOverloadeExceptionDuetoRateLimiter();
    }

    @Test
    public void testDifferentiateOverloadedException()
    {
        Assert.assertTrue(CassandraResourceUtilization.isExceptionDuetoRateLimiter(CassandraResourceUtilization.buildOverloadeExceptionDuetoRateLimiter()));
        Assert.assertFalse(CassandraResourceUtilization.isExceptionDuetoRateLimiter(new OverloadedException("Something else")));
    }

    @Test
    public void testHealthThreadpool() throws InterruptedException
    {
        ThrottlingOptions options = CassandraResourceUtilization.instance.throttlingOptions;
        options.setCpuThresholdOneMinute(99);
        options.setHealthCheckFreqInSec(1);
        options.setHealthCheckInitDelayInSec(0);

        // activate the health check thread pool by setting the init time lower
        CassandraResourceUtilization lowInitTime = CassandraResourceUtilization.instance;
        lowInitTime.startHealthCheckThread();
        Thread.sleep(5000);
        Assert.assertEquals(0, lowInitTime.throttlingMetrics.needsThrottling.getCount());
        // this metric is incremented as a result of a health check already happening by the thread pool
        Assert.assertTrue(lowInitTime.throttlingMetrics.doesNotNeedThrottling.getCount() > 0);
        lowInitTime.reportThread.shutdown();
        Assert.assertTrue(lowInitTime.reportThread.awaitTermination(10, SECONDS));
        Assert.assertTrue(lowInitTime.reportThread.isShutdown());

        lowInitTime.throttlingMetrics.doesNotNeedThrottling.dec(lowInitTime.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, lowInitTime.throttlingMetrics.doesNotNeedThrottling.getCount());

        // deactivate the health check thread pool by setting the init time pretty high
        options.setHealthCheckInitDelayInSec(24 * 3600);
        CassandraResourceUtilization highInitTime = CassandraResourceUtilization.instance;
        highInitTime.reportThread = executorFactory().scheduled(false, "CassandraResourceUtilization", Thread.MAX_PRIORITY);
        highInitTime.startHealthCheckThread();

        Thread.sleep(10000);
        Assert.assertEquals(0, highInitTime.throttlingMetrics.needsThrottling.getCount());
        // this metric is not incremented as the health check thread has not yet commenced
        Assert.assertEquals(0, lowInitTime.throttlingMetrics.doesNotNeedThrottling.getCount());
        highInitTime.reportThread.shutdown();
        Assert.assertTrue(highInitTime.reportThread.awaitTermination(10, SECONDS));
        Assert.assertTrue(highInitTime.reportThread.isShutdown());
    }

    @Test
    public void testEpochtimeConversion()
    {
        Assert.assertEquals("1970-01-01 00:00:00 UTC", CassandraResourceUtilization.convertEpochTimeToUTC(0));
        Assert.assertEquals("2023-10-06 20:39:22 UTC", CassandraResourceUtilization.convertEpochTimeToUTC(1696624762983L));
    }

    @Test(expected = OverloadedException.class)
    public void testThrottlingMutationReplicationTrafficEnabled()
    {
        CassandraResourceUtilization cassandraResourceUtilization = forceThrottling();
        cassandraResourceUtilization.throttlingOptions.setThrottleReadReplicaTraffic(false);
        try
        {
            Keyspace ks = Keyspace.open(KEYSPACE_THROTTLE);
            ColumnFamilyStore cf = ks.getColumnFamilyStore(TABLE);
            Mutation mutation = new RowUpdateBuilder(cf.metadata(), FBUtilities.timestampMicros(), ByteBufferUtil.bytes("1"))
            .clustering(ByteBufferUtil.bytes("2"))
            .build();

            ks.applyFuture(mutation, true, true).get();
        }
        catch (ExecutionException e)
        {
            Assert.assertEquals(0,  cassandraResourceUtilization.throttlingMetrics.disableReadReplicaTrafficThrottling.getCount());
            Assert.assertEquals(0,  cassandraResourceUtilization.throttlingMetrics.disableMutationReplicaTrafficThrottling.getCount());
            OverloadedException e1 = (OverloadedException) e.getCause();
            Assert.assertEquals("from dynamic throttler: 127.0.0.1", e1.getMessage());
            throw e1;
        }
        catch (InterruptedException e)
        {
            Assert.assertFalse("Unexpected exception: " + e.getMessage(), false);
        }
    }

    @Test
    public void testThrottlingMutationReplicationTrafficDisabled()
    {
        CassandraResourceUtilization cassandraResourceUtilization = forceThrottling();
        cassandraResourceUtilization.throttlingOptions.setThrottleReadReplicaTraffic(true);
        cassandraResourceUtilization.throttlingOptions.setThrottleMutationReplicaTraffic(false);
        Keyspace ks = Keyspace.open(KEYSPACE_THROTTLE);
        ColumnFamilyStore cf = ks.getColumnFamilyStore(TABLE);
        Mutation mutation = new RowUpdateBuilder(cf.metadata(), FBUtilities.timestampMicros(), ByteBufferUtil.bytes("1"))
        .clustering(ByteBufferUtil.bytes("2"))
        .build();

        try
        {
            ks.applyFuture(mutation, true, true).get();
            Assert.assertEquals(0,  cassandraResourceUtilization.throttlingMetrics.disableReadReplicaTrafficThrottling.getCount());
            Assert.assertEquals(1,  cassandraResourceUtilization.throttlingMetrics.disableMutationReplicaTrafficThrottling.getCount());
        }
        catch (InterruptedException | ExecutionException e)
        {
            Assert.assertFalse("Unexpected exception: " + e.getMessage(), false);
        }
    }

    @Test(expected = OverloadedException.class)
    public void testThrottlingReadReplicationTrafficEnabled()
    {
        CassandraResourceUtilization cassandraResourceUtilization = forceThrottling();
        cassandraResourceUtilization.throttlingOptions.setThrottleMutationReplicaTraffic(false);
        try
        {
            ReadCommand cmd = new AbstractReadCommandBuilder.PartitionRangeBuilder(Keyspace.open(KEYSPACE_THROTTLE).getColumnFamilyStore(TABLE)).build();
            cmd.executeLocally(cmd.executionController());
        }
        catch (OverloadedException e)
        {
            Assert.assertEquals(0,  cassandraResourceUtilization.throttlingMetrics.disableReadReplicaTrafficThrottling.getCount());
            Assert.assertEquals(0,  cassandraResourceUtilization.throttlingMetrics.disableMutationReplicaTrafficThrottling.getCount());
            Assert.assertEquals("from dynamic throttler: 127.0.0.1", e.getMessage());
            throw e;
        }
    }

    @Test
    public void testThrottlingReadReplicationTrafficDisabled()
    {
        CassandraResourceUtilization cassandraResourceUtilization = forceThrottling();
        cassandraResourceUtilization.throttlingOptions.setThrottleReadReplicaTraffic(false);
        cassandraResourceUtilization.throttlingOptions.setThrottleMutationReplicaTraffic(true);
        ReadCommand cmd = new AbstractReadCommandBuilder.PartitionRangeBuilder(Keyspace.open(KEYSPACE_THROTTLE).getColumnFamilyStore(TABLE)).build();
        cmd.executeLocally(cmd.executionController());
        Assert.assertEquals(1,  cassandraResourceUtilization.throttlingMetrics.disableReadReplicaTrafficThrottling.getCount());
        Assert.assertEquals(0,  cassandraResourceUtilization.throttlingMetrics.disableMutationReplicaTrafficThrottling.getCount());
    }

    private CassandraResourceUtilization forceThrottling()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.currentThrottlingPercentage = 1.0;
        return cassandraResourceUtilization;
    }

    private ResourcesStats getResourceStats()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.fetchCurrentHealth();
        return cassandraResourceUtilization.resourcesStats;
    }

    @Before
    public void resetThrottling()
    {
        ensureSetupOnlyOnce();

        // we need to make sure rate limiter is enabled for each unit test method
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        resetThrottlingOption(cassandraResourceUtilization.throttlingOptions);
        cassandraResourceUtilization.syncAllFilters();
        cassandraResourceUtilization.resetThrottlingParams();
        cassandraResourceUtilization.isSetupComplete = true;
    }

    private void ensureSetupOnlyOnce()
    {
        // In the parent CQLTester, our setup() method will not be hit because by default throttlingOptions.enabled is false.
        // Therefore, we need to ensure it is called once and only once, before any test method is executed.
        if (hasDoneSetup.compareAndSet(false, true)) // ensure we only do setup once
        {
            CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
            Assert.assertFalse(cassandraResourceUtilization.isSetupComplete);
            cassandraResourceUtilization.setup(false);
            Assert.assertTrue(cassandraResourceUtilization.isSetupComplete);

            // ensure all the system keyspaces exist in the cache of ignore_tables filter after
            // cassandraResourceUtilization.setup() is called.
            // For simplicity, we check one table of each system keyspace.
            String[][] systemKeyspaceTables = new String[][]
            {
                new String[]{"system","peers"},
                new String[]{"system_schema","keyspaces"},
                new String[]{"system_traces","events"},
                new String[]{"system_auth","role_members"},
                new String[]{"system_distributed","parent_repair_history"},
            };
            for (String[] keyspaceAndTable : systemKeyspaceTables)
            {
                assert cassandraResourceUtilization.ignoreTablesFilter.matches(keyspaceAndTable[0], keyspaceAndTable[1]);
            }
        }
    }

    @Before
    public void resetMetrics()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        resetCounter(cassandraResourceUtilization.throttlingMetrics.needsThrottling);
        resetCounter(cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling);
        resetCounter(cassandraResourceUtilization.throttlingMetrics.resetThrottling);
        resetCounter(cassandraResourceUtilization.throttlingMetrics.increaseThrottling);
        resetCounter(cassandraResourceUtilization.throttlingMetrics.disableThrottling);
        resetCounter(cassandraResourceUtilization.throttlingMetrics.disableReadReplicaTrafficThrottling);
        resetCounter(cassandraResourceUtilization.throttlingMetrics.disableMutationReplicaTrafficThrottling);

        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        resetCounter(userKSMetrics.addKSForReadThrottling);
        resetCounter(userKSMetrics.addKSForRangeThrottling);
        resetCounter(userKSMetrics.addKSForWriteThrottling);
        resetCounter(userKSMetrics.skipKSThrottling);
        resetCounter(userKSMetrics.readRequestsTrendingUpward);
        resetCounter(userKSMetrics.rangeRequestsTrendingUpward);
        resetCounter(userKSMetrics.writeRequestsTrendingUpward);
        resetCounter(userKSMetrics.readLatencyTrendingUpward);
        resetCounter(userKSMetrics.rangeLatencyTrendingUpward);
        resetCounter(userKSMetrics.writeLatencyTrendingUpward);
        resetCounter(userKSMetrics.minReadThrottling);
        resetCounter(userKSMetrics.minWriteThrottling);
        resetCounter(userKSMetrics.maxReadThrottling);
        resetCounter(userKSMetrics.maxWriteThrottling);
        resetCounter(userKSMetrics.noReadThrottling);
        resetCounter(userKSMetrics.noWriteThrottling);
        resetCounter(userKSMetrics.aggressiveThrottling);
        resetCounter(userKSMetrics.hardBlockSinglePartitionCoordReads);
        resetCounter(userKSMetrics.hardBlockSinglePartitionReplicaReads);
        resetCounter(userKSMetrics.hardBlockRangeCoordReads);
        resetCounter(userKSMetrics.hardBlockRangeReplicaReads);
        resetCounter(userKSMetrics.hardBlockCoordWrites);
        resetCounter(userKSMetrics.hardBlockReplicaWrites);
    }

    private void assertAllHardBlockerFilters(String keyspace, String table, boolean assertTrue)
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Collection tableSingleton = Collections.singleton(table);

        if (assertTrue)
        {
            for (TrafficType trafficType : TrafficType.values())
            {
                Assert.assertTrue(trafficType.getHardBlockTablesFilter().matches(keyspace, table));
                Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(keyspace, tableSingleton, trafficType));
            }
        } else
        {
            for (TrafficType trafficType : TrafficType.values())
            {
                Assert.assertFalse(trafficType.getHardBlockTablesFilter().matches(keyspace, table));
            }
        }
    }

    private void resetCounter(Counter counter)
    {
        counter.dec(counter.getCount());
    }

    private void resetThrottlingOption(ThrottlingOptions throttlingOptions) {
        throttlingOptions.enabled = true;

        // for checking CPU signals
        throttlingOptions.cpu_threshold_one_minute = 80;
        throttlingOptions.pending_reads_threshold_one_minute = 0;
        throttlingOptions.pending_mutations_threshold_one_minute = 0;
        throttlingOptions.pending_native_transport_threshold_one_minute = 0;

        // for checking threadpool signals
        throttlingOptions.threadpool_threshold_reads = VERY_LARGE_THREADPOOL_PENDING_TASKS;
        throttlingOptions.threadpool_threshold_writes = VERY_LARGE_THREADPOOL_PENDING_TASKS;
        throttlingOptions.threadpool_threshold_native_transport = VERY_LARGE_THREADPOOL_PENDING_TASKS;

        throttlingOptions.percentage_of_traffic_to_throttling = 0.1;
        throttlingOptions.more_aggressive_throttling_after_in_sec = 1 * 60; // 1 minutes
        throttlingOptions.reset_after_no_throttling_seen_in_sec = 15 * 60; // 15 minutes
        throttlingOptions.aggressive_throttling_qps_ratio = 4;
        throttlingOptions.aggressive_throttling_latency_ratio = 4;
        throttlingOptions.ignore_tables_regex = "^system.*\\..+|^pingless\\..+";
        throttlingOptions.health_check_init_delay_in_sec = 60;
        throttlingOptions.health_check_freq_in_sec = 1;
        throttlingOptions.throttle_read_replica_traffic = true;
        throttlingOptions.throttle_mutation_replica_traffic = true;
        throttlingOptions.hard_block_single_partition_coord_reads_tables_regex = "";
        throttlingOptions.hard_block_single_partition_replica_reads_tables_regex = "";
        throttlingOptions.hard_block_range_coord_reads_tables_regex = "";
        throttlingOptions.hard_block_range_replica_reads_tables_regex = "";
        throttlingOptions.hard_block_coord_writes_tables_regex = "";
        throttlingOptions.hard_block_replica_writes_tables_regex = "";
    }
}
