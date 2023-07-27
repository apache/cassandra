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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetrics;
import org.apache.cassandra.service.throttler.dynamic.metrics.ThrottlingMetrics;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.AutoRepair;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetricsManager;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CassandraResourceUtilizationTest extends CQLTester
{
    private static final String KEYSPACE_THROTTLE = "ks_throttle";
    private static final String TABLE = "tbl";
    private DebuggableScheduledThreadPoolExecutor repairExecutor = new DebuggableScheduledThreadPoolExecutor(
    8, new NamedThreadFactory("AutoRepair-Repair-Unit-Test"));

    private static TableMetadata cfm;
    ColumnFamilyStore cfs;

    public CassandraResourceUtilizationTest()
    {
        requireNetwork();
        AutoRepair.instance.setup();
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
    }

    @Test
    public void testNrThrottling1()
    {
        ResourcesStats resourcesStats = getResourceStats();

        long nrThrottled1Cur = resourcesStats.getNrThrottled1Cur();
        long nrThrottled1OneMinute = resourcesStats.getNrThrottled1OneMinute();
        long nrThrottled1FiveMinute = resourcesStats.getNrThrottled1FiveMinute();
        long nrThrottled1FifteenMinute = resourcesStats.getNrThrottled1FifteenMinute();
        Assert.assertTrue("Current: " + nrThrottled1Cur, nrThrottled1Cur >= 0);
        Assert.assertTrue("OneMinute: " + nrThrottled1OneMinute, nrThrottled1OneMinute >= 0);
        Assert.assertTrue("FiveMinute: " + nrThrottled1FiveMinute, nrThrottled1FiveMinute >= 0);
        Assert.assertTrue("FifteenMinute: " + nrThrottled1FifteenMinute, nrThrottled1FifteenMinute >= 0);
    }

    @Test
    public void testNrThrottling2()
    {
        ResourcesStats resourcesStats = getResourceStats();

        long nrThrottled2Cur = resourcesStats.getNrThrottled2Cur();
        long nrThrottled2OneMinute = resourcesStats.getNrThrottled2OneMinute();
        long nrThrottled2FiveMinute = resourcesStats.getNrThrottled2FiveMinute();
        long nrThrottled2FifteenMinute = resourcesStats.getNrThrottled2FifteenMinute();
        Assert.assertTrue("Current: " + nrThrottled2Cur, nrThrottled2Cur >= 0);
        Assert.assertTrue("OneMinute: " + nrThrottled2OneMinute, nrThrottled2OneMinute >= 0);
        Assert.assertTrue("FiveMinute: " + nrThrottled2FiveMinute, nrThrottled2FiveMinute >= 0);
        Assert.assertTrue("FifteenMinute: " + nrThrottled2FifteenMinute, nrThrottled2FifteenMinute >= 0);
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
    public void testNoThrottling()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
        cassandraResourceUtilization.fetchCurrentHealth();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testNoThrottlingOnlyWithCpuUtil1()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(100);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testNoThrottlingOnlyWithCpuUtil2()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil2(100);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testNoThrottlingOnlyWithNRThrottled1()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setNrThrottled1(100);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testNoThrottlingOnlyWithNRThrottled2()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setNrThrottled2(100);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testNoThrottlingOnlyWithPendingReads()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setPendingReads(100);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testNoThrottlingOnlyWithPendingMutations()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setPendingMutations(100);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testYesThrottling()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());

        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.cpu_threshold_one_minute = 0;
        cassandraResourceUtilization.throttlingOptions.nr_throttling_threshold_one_minute = 0;
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.cpu_threshold_cur+1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.cpu_threshold_cur+1);
        resourcesStats.setNrThrottled1(cassandraResourceUtilization.throttlingOptions.nr_throttling_threshold_cur+1);
        resourcesStats.setNrThrottled2(cassandraResourceUtilization.throttlingOptions.nr_throttling_threshold_cur+1);
        resourcesStats.setPendingReads(cassandraResourceUtilization.throttlingOptions.pending_reads_threshold_cur+1);
        resourcesStats.setPendingMutations(cassandraResourceUtilization.throttlingOptions.pending_mutations_threshold_cur + 1);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testResetThrottlingNoop()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        // when we invoke "adjustThrottling", then it should be a noop as  reset the throttling since "oldestThrottlingIndicatorTimeInMS = 0" as
        // well as lastThrottlingIndicatorTimeInMS=0
        cassandraResourceUtilization.adjustThrottling();

        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testResetThrottlingTrackOldThrottlingIndicatorTime()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        long lastThrottlingIndicatorTime = System.currentTimeMillis();
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;

        // when we invoke "adjustThrottling", then it should override "oldestThrottlingIndicatorTimeInMS" value to
        // lastThrottlingIndicatorTimeInMS
        cassandraResourceUtilization.adjustThrottling();

        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testResetThrottlingDoubleThrottlingAndThenReset()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        long lastThrottlingIndicatorTime = System.currentTimeMillis();
        long oldestThrottlingIndicatorTimeInMS = System.currentTimeMillis() - SECONDS.toMillis(cassandraResourceUtilization.throttlingOptions.more_aggressive_throttling_after_in_sec + 1);
        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = oldestThrottlingIndicatorTimeInMS;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(oldestThrottlingIndicatorTimeInMS, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        cassandraResourceUtilization.aggressiveThorttlingDatastores.add("test_keyspace");

        // this should double the throttling
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1 * 2, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(1, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());

        lastThrottlingIndicatorTime = System.currentTimeMillis() - SECONDS.toMillis(cassandraResourceUtilization.throttlingOptions.reset_after_no_throttling_seen_in_sec + 1);
        oldestThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = oldestThrottlingIndicatorTimeInMS;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(oldestThrottlingIndicatorTimeInMS, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);

        // this should reset the throttling
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testValidateCpuThrottlingCalculation()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);

        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        NativeResourceUtilization nativeResourceUtilization = (NativeResourceUtilization) cassandraResourceUtilization.resourceUtilzation;
        nativeResourceUtilization.cpuStatFilePath = getClass().getClassLoader().getResource(NativeResourceUtilizationTest.TEST_CPU_STAT_FILE_PATH).getFile();
        cassandraResourceUtilization.fetchCurrentHealth();

        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingPercentageCur, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        Assert.assertEquals(5468, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertEquals(0, cassandraResourceUtilization.resourcesStats.getNrThrottled1Cur());
        Assert.assertEquals(0, cassandraResourceUtilization.resourcesStats.getNrThrottled2Cur());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        cassandraResourceUtilization.nrThrottled1Prev = 5460;
        cassandraResourceUtilization.fetchCurrentHealth();
        Assert.assertEquals(5468, cassandraResourceUtilization.nrThrottled1Prev);
        Assert.assertEquals(-1, cassandraResourceUtilization.nrThrottled2Prev);
        Assert.assertEquals(8, cassandraResourceUtilization.resourcesStats.getNrThrottled1Cur());
        Assert.assertEquals(0, cassandraResourceUtilization.resourcesStats.getNrThrottled2Cur());

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(2, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doubleThrottling.getCount());
    }

    @Test
    public void testSkipSystemKS()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);

        KeyspaceThrottlingMetrics systemAuthMetrics = KeyspaceThrottlingMetricsManager.getMetrics("system_auth");
        Assert.assertEquals(0, systemAuthMetrics.skipSystemKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic("system_auth", true));
        Assert.assertEquals(1, systemAuthMetrics.skipSystemKSThrottling.getCount());


        KeyspaceThrottlingMetrics systemMetrics = KeyspaceThrottlingMetricsManager.getMetrics("system");
        Assert.assertEquals(0, systemMetrics.skipSystemKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic("system", true));
        Assert.assertEquals(1, systemMetrics.skipSystemKSThrottling.getCount());

        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertEquals(0, userKSMetrics.skipSystemKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true));
        Assert.assertEquals(0, userKSMetrics.skipSystemKSThrottling.getCount());
    }

    @Test
    public void testThrottleUserTraffic()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.cpu_threshold_one_minute = 0;
        cassandraResourceUtilization.throttlingOptions.nr_throttling_threshold_one_minute = 0;
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.cpu_threshold_cur+1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.cpu_threshold_cur+1);
        resourcesStats.setNrThrottled1(cassandraResourceUtilization.throttlingOptions.nr_throttling_threshold_cur+1);
        resourcesStats.setNrThrottled2(cassandraResourceUtilization.throttlingOptions.nr_throttling_threshold_cur+1);
        resourcesStats.setPendingReads(cassandraResourceUtilization.throttlingOptions.pending_reads_threshold_cur+1);
        resourcesStats.setPendingMutations(cassandraResourceUtilization.throttlingOptions.pending_mutations_threshold_cur + 1);
        cassandraResourceUtilization.shouldThrottle();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.throttlingOptions.percentage_of_traffice_to_throttling = 1.0;
        cassandraResourceUtilization.throttlingPercentageCur = 1.0;
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true));
        Assert.assertEquals(0, userKSMetrics.requestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());


        cassandraResourceUtilization.throttlingOptions.percentage_of_traffice_to_throttling = 0.0;
        cassandraResourceUtilization.throttlingPercentageCur = 0.0;
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true));
        Assert.assertEquals(0, userKSMetrics.requestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());

        cassandraResourceUtilization.throttlingOptions.percentage_of_traffice_to_throttling = 0.0;
        cassandraResourceUtilization.throttlingPercentageCur = 0.0;
        cassandraResourceUtilization.aggressiveThorttlingDatastores.add(KEYSPACE_THROTTLE);
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true));
        Assert.assertEquals(0, userKSMetrics.requestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.maxThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.aggressiveThorttlingDatastores.size());
    }

    @Test
    public void testUpwardFunction()
    {
        Assert.assertTrue(CassandraResourceUtilization.isTrendingUpward(30, 20.0, 10.0));
        Assert.assertFalse(CassandraResourceUtilization.isTrendingUpward(20.0, 10.0, 30.0));
        Assert.assertFalse(CassandraResourceUtilization.isTrendingUpward(20.0, 40.0, 30.0));
    }

    @Test
    public void testNoSpikeInRequestRate()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        KeyspaceMetrics keyspaceMetrics = Keyspace.open(KEYSPACE_THROTTLE).metric;
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertFalse(cassandraResourceUtilization.spikeInRequestRate(KEYSPACE_THROTTLE, keyspaceMetrics, true, ksThrottlingMetrics));
        Assert.assertFalse(cassandraResourceUtilization.spikeInRequestRate(KEYSPACE_THROTTLE, keyspaceMetrics, false, ksThrottlingMetrics));
    }

    @Test
    public void testNoSpikeInLatency()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        KeyspaceMetrics keyspaceMetrics = Keyspace.open(KEYSPACE_THROTTLE).metric;
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertFalse(cassandraResourceUtilization.spikeInLatency(KEYSPACE_THROTTLE, keyspaceMetrics, true, ksThrottlingMetrics));
        Assert.assertFalse(cassandraResourceUtilization.spikeInLatency(KEYSPACE_THROTTLE, keyspaceMetrics, false, ksThrottlingMetrics));
    }

    private ResourcesStats getResourceStats()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup(false);
        cassandraResourceUtilization.fetchCurrentHealth();
        return cassandraResourceUtilization.resourcesStats;
    }

    @Before
    public void deregisterMetrics()
    {
        Metrics.remove(ThrottlingMetrics.factory.createMetricName("NeedsThrottling"));
        Metrics.remove(ThrottlingMetrics.factory.createMetricName("DoesNotNeedsThrottling"));
        Metrics.remove(ThrottlingMetrics.factory.createMetricName("ResetThrottling"));
        Metrics.remove(ThrottlingMetrics.factory.createMetricName("DoubleThrottling"));
        Metrics.remove(ThrottlingMetrics.factory.createMetricName("DisableThrottling"));
        KeyspaceThrottlingMetricsManager.throttlingMetrics.clear();

        Metrics.remove(ResourcesStats.factory.createMetricName("CpuUtil1"));
        Metrics.remove(ResourcesStats.factory.createMetricName("CpuUtil2"));
        Metrics.remove(ResourcesStats.factory.createMetricName("NRThrottled1"));
        Metrics.remove(ResourcesStats.factory.createMetricName("NRThrottled2"));
        Metrics.remove(ResourcesStats.factory.createMetricName("PendingReads"));
        Metrics.remove(ResourcesStats.factory.createMetricName("PendingMutations"));
        Metrics.remove(ResourcesStats.factory.createMetricName("CpuUtil1Current"));
        Metrics.remove(ResourcesStats.factory.createMetricName("CpuUtil2Current"));
        Metrics.remove(ResourcesStats.factory.createMetricName("NRThrottled1Current"));
        Metrics.remove(ResourcesStats.factory.createMetricName("NRThrottled2Current"));
        Metrics.remove(ResourcesStats.factory.createMetricName("PendingReadsCurrent"));
        Metrics.remove(ResourcesStats.factory.createMetricName("PendingMutationsCurrent"));
    }
}
