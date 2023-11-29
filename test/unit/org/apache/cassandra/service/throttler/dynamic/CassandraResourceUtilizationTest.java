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

import org.apache.cassandra.config.DatabaseDescriptor;

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
import org.apache.cassandra.service.RateLimiterService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetrics;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.AutoRepair;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.throttler.dynamic.metrics.KeyspaceThrottlingMetricsManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class CassandraResourceUtilizationTest extends CQLTester
{
    private static final String KEYSPACE_THROTTLE = "ks_throttle";
    private static final String TABLE = "tbl";

    private static TableMetadata cfm;

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
        CassandraResourceUtilization.instance.throttlingOptions.setEnabled(true);
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
        long originalCpuThresholdOneMinute = CassandraResourceUtilization.instance.throttlingOptions.getCpuThresholdOneMinute();
        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(99);

        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);
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
        cassandraResourceUtilization.setup(false);
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
        cassandraResourceUtilization.setup(false);
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
    public void testYesThrottling()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());

        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setPendingReads(cassandraResourceUtilization.throttlingOptions.getPendingReadsThresholdCur() + 1);
        resourcesStats.setPendingMutations(cassandraResourceUtilization.throttlingOptions.getPendingMutationsThresholdCur() + 1);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(1, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        Assert.assertEquals(0.1, cassandraResourceUtilization.throttlingMetrics.currentThrottlingPercentage.getValue());
    }

    @Test
    public void testResetThrottlingNoop()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        // when we invoke "adjustThrottling", then it should be a noop as  reset the throttling since "oldestThrottlingIndicatorTimeInMS = 0" as
        // well as lastThrottlingIndicatorTimeInMS=0
        cassandraResourceUtilization.adjustThrottling();

        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
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
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        long lastThrottlingIndicatorTime = System.currentTimeMillis();
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;

        // when we invoke "adjustThrottling", then it should override "oldestThrottlingIndicatorTimeInMS" value to
        // lastThrottlingIndicatorTimeInMS
        cassandraResourceUtilization.adjustThrottling();

        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
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
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        long lastThrottlingIndicatorTime = System.currentTimeMillis();
        long oldestThrottlingIndicatorTimeInMS = System.currentTimeMillis() - SECONDS.toMillis(cassandraResourceUtilization.throttlingOptions.getMoreAggressiveThrottlingAfterInSec() + 1);
        cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS = oldestThrottlingIndicatorTimeInMS;
        cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS = lastThrottlingIndicatorTime;
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(oldestThrottlingIndicatorTimeInMS, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.put("test_keyspace", true);

        // this should linearly increase the throttling
        cassandraResourceUtilization.adjustThrottling();
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(lastThrottlingIndicatorTime, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1 * 2, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(1, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
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
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
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
        cassandraResourceUtilization.setup(false);
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
        cassandraResourceUtilization.setup(false);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);

        cassandraResourceUtilization.fetchCurrentHealth();

        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingCheckPointTimeInMS);
        Assert.assertEquals(0, cassandraResourceUtilization.lastThrottlingIndicatorTimeInMS);
        Assert.assertEquals(0.1, cassandraResourceUtilization.currentThrottlingPercentage, 0.0);
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
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
    public void testIgnoreKeyspaceFromDatabaseDescriptor()
    {
        ThrottlingOptions originalThrottlingOptions = RateLimiterService.instance.getThrottlingOptions();

        ThrottlingOptions simulatedFromDbDescriptor = new ThrottlingOptions();
        simulatedFromDbDescriptor.ignore_keyspaces = "some_keyspace"; // change ignore_keyspaces without changing ignoreKeyspacesPattern
        RateLimiterService.instance.setThrottlingOptions(simulatedFromDbDescriptor);
        Assert.assertEquals(Pattern.compile(RateLimiterService.instance.getThrottlingOptions().ignore_keyspaces).toString(),
                            RateLimiterService.instance.getThrottlingOptions().getIgnoreKeyspacesPattern().toString());

        RateLimiterService.instance.setThrottlingOptions(originalThrottlingOptions);
    }

    @Test
    public void testSkipSystemKS()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);

        KeyspaceThrottlingMetrics systemAuthMetrics = KeyspaceThrottlingMetricsManager.getMetrics("system_auth");
        Assert.assertEquals(0, systemAuthMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic("system_auth", true, false));
        Assert.assertEquals(1, systemAuthMetrics.skipKSThrottling.getCount());

        KeyspaceThrottlingMetrics systemMetrics = KeyspaceThrottlingMetricsManager.getMetrics("system_traces");
        Assert.assertEquals(0, systemMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic("system_traces", true, false));
        Assert.assertEquals(1, systemMetrics.skipKSThrottling.getCount());

        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertEquals(0, userKSMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true, false));
        Assert.assertEquals(0, userKSMetrics.skipKSThrottling.getCount());
    }

    @Test
    public void testSkipUserKeyspace()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);
        cassandraResourceUtilization.throttlingOptions.setIgnoreKeyspaces("system.*|pingless|" + KEYSPACE_THROTTLE);
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertEquals(0, userKSMetrics.skipKSThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true, false));
        Assert.assertEquals(1, userKSMetrics.skipKSThrottling.getCount());
    }

    @Test
    public void testThrottleUserTraffic()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setPendingReads(cassandraResourceUtilization.throttlingOptions.getPendingReadsThresholdCur() + 1);
        resourcesStats.setPendingMutations(cassandraResourceUtilization.throttlingOptions.getPendingMutationsThresholdCur() + 1);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);

        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        cassandraResourceUtilization.currentThrottlingPercentage = 1.0;
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true, false));
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());


        cassandraResourceUtilization.currentThrottlingPercentage = 0.0;
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true, false));
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(0, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());

        // throttle read traffic (current throttling is at 0.9, and 0.1 will be added, so it will be full throttling)
        cassandraResourceUtilization.currentThrottlingPercentage = 0.9;
        cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.put(KEYSPACE_THROTTLE, true);
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, true, false));
        Assert.assertEquals(1, userKSMetrics.aggressiveThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());

        // do not throttle write traffic if readAggressiveThorttlingKeyspaces is set
        cassandraResourceUtilization.currentThrottlingPercentage = 0.0;
        ;
        cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.put(KEYSPACE_THROTTLE, true);
        Assert.assertFalse(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, false, false));
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());

        // throttle write traffic if mutationAggressiveThorttlingKeyspaces is set
        // (current throttling is at 0.9, and 0.1 will be added, so it will be full throttling)
        cassandraResourceUtilization.currentThrottlingPercentage = 0.9;
        cassandraResourceUtilization.mutationAggressiveThorttlingKeyspaces.put(KEYSPACE_THROTTLE, true);
        Assert.assertTrue(cassandraResourceUtilization.throttleUserTraffic(KEYSPACE_THROTTLE, false, false));
        Assert.assertEquals(0, userKSMetrics.readRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.writeRequestsTrendingUpward.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.addKSForWriteThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noReadThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.noWriteThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minReadThrottling.getCount());
        Assert.assertEquals(0, userKSMetrics.minWriteThrottling.getCount());
        Assert.assertEquals(2, userKSMetrics.maxReadThrottling.getCount());
        Assert.assertEquals(1, userKSMetrics.maxWriteThrottling.getCount());
        Assert.assertEquals(1, cassandraResourceUtilization.readAggressiveThorttlingKeyspaces.size());
    }

    @Test
    public void testThrottleIfOnlyReadTrafficIsSaturated()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        cassandraResourceUtilization.throttlingOptions.setPendingReadsThresholdCur(10);
        cassandraResourceUtilization.throttlingOptions.setPendingMutationsThresholdCur(10);
        cassandraResourceUtilization.throttlingOptions.setPendingNativeTransportThresholdCur(10);
        resourcesStats.setPendingReads(cassandraResourceUtilization.throttlingOptions.getPendingReadsThresholdCur() + 1);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
    }

    @Test
    public void testThrottleIfOnlyWriteTrafficIsSaturated()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        cassandraResourceUtilization.throttlingOptions.setPendingReadsThresholdCur(10);
        cassandraResourceUtilization.throttlingOptions.setPendingMutationsThresholdCur(10);
        cassandraResourceUtilization.throttlingOptions.setPendingNativeTransportThresholdCur(10);
        resourcesStats.setPendingMutations(cassandraResourceUtilization.throttlingOptions.getPendingMutationsThresholdCur() + 1);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
    }

    @Test
    public void testThrottleIfOnlyNativeTransportIsSaturated()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        cassandraResourceUtilization.throttlingOptions.setPendingReadsThresholdCur(10);
        cassandraResourceUtilization.throttlingOptions.setPendingMutationsThresholdCur(10);
        cassandraResourceUtilization.throttlingOptions.setPendingNativeTransportThresholdCur(10);
        resourcesStats.setPendingNativeTransport(cassandraResourceUtilization.throttlingOptions.getPendingNativeTransportThresholdCur() + 1);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
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
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);
        KeyspaceMetrics keyspaceMetrics = Keyspace.open(KEYSPACE_THROTTLE).metric;
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertFalse(cassandraResourceUtilization.spikeInRequestRate(KEYSPACE_THROTTLE, keyspaceMetrics, true, ksThrottlingMetrics));
        Assert.assertFalse(cassandraResourceUtilization.spikeInRequestRate(KEYSPACE_THROTTLE, keyspaceMetrics, false, ksThrottlingMetrics));
    }

    @Test
    public void testNoSpikeInLatency()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);
        KeyspaceMetrics keyspaceMetrics = Keyspace.open(KEYSPACE_THROTTLE).metric;
        KeyspaceThrottlingMetrics ksThrottlingMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        Assert.assertFalse(cassandraResourceUtilization.spikeInLatency(KEYSPACE_THROTTLE, keyspaceMetrics, true, ksThrottlingMetrics));
        Assert.assertFalse(cassandraResourceUtilization.spikeInLatency(KEYSPACE_THROTTLE, keyspaceMetrics, false, ksThrottlingMetrics));
    }

    @Test
    public void testCassandraIsNotNormal()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.setup(false);


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
        long originalCpuThresholdOneMinute = CassandraResourceUtilization.instance.throttlingOptions.getCpuThresholdOneMinute();
        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(99);

        // activate the health check thread pool by setting the init time lower
        CassandraResourceUtilization lowInitTime = CassandraResourceUtilization.instance;
        lowInitTime.throttlingOptions.setHealthCheckFreqInSec(1);
        lowInitTime.throttlingOptions.setHealthCheckInitDelayInSec(0);
        lowInitTime.setup(true);
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
        CassandraResourceUtilization highInitTime = CassandraResourceUtilization.instance;
        highInitTime.throttlingOptions.setHealthCheckInitDelayInSec(24 * 3600);
        highInitTime.reportThread = executorFactory().scheduled(false, "CassandraResourceUtilization", Thread.MAX_PRIORITY);
        highInitTime.setup(true);

        Thread.sleep(10000);
        Assert.assertEquals(0, highInitTime.throttlingMetrics.needsThrottling.getCount());
        // this metric is not incremented as the health check thread has not yet commenced
        Assert.assertEquals(0, lowInitTime.throttlingMetrics.doesNotNeedThrottling.getCount());
        highInitTime.reportThread.shutdown();
        Assert.assertTrue(highInitTime.reportThread.awaitTermination(10, SECONDS));
        Assert.assertTrue(highInitTime.reportThread.isShutdown());

        CassandraResourceUtilization.instance.throttlingOptions.setCpuThresholdOneMinute(originalCpuThresholdOneMinute);
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
        cassandraResourceUtilization.setup(false);

        Assert.assertEquals(0, cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        Assert.assertFalse(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.throttlingOptions.setCpuThresholdOneMinute(0);
        ResourcesStats resourcesStats = cassandraResourceUtilization.resourcesStats;
        resourcesStats.setCpuUtil1(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setCpuUtil2(cassandraResourceUtilization.throttlingOptions.getCpuThresholdCur() + 1);
        resourcesStats.setPendingReads(cassandraResourceUtilization.throttlingOptions.getPendingReadsThresholdCur() + 1);
        resourcesStats.setPendingMutations(cassandraResourceUtilization.throttlingOptions.getPendingMutationsThresholdCur() + 1);
        cassandraResourceUtilization.checkSignals();
        Assert.assertTrue(cassandraResourceUtilization.shouldThrottle);
        cassandraResourceUtilization.currentThrottlingPercentage = 1.0;
        return cassandraResourceUtilization;
    }

    private ResourcesStats getResourceStats()
    {
        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.setup(false);
        cassandraResourceUtilization.fetchCurrentHealth();
        return cassandraResourceUtilization.resourcesStats;
    }

    @Before
    public void resetMetrics()
    {
        // we need to make sure rate limiter is enabled for each unit test method
        ThrottlingOptions options = new ThrottlingOptions();
        options.setEnabled(true);
        options.setIgnoreKeyspaces("system.*|pingless");
        DatabaseDescriptor.setThrottlingOptions(options);

        CassandraResourceUtilization cassandraResourceUtilization = CassandraResourceUtilization.instance;
        cassandraResourceUtilization.throttlingMetrics.needsThrottling.dec(cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.dec(cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.resetThrottling.dec(cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.increaseThrottling.dec(cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.disableThrottling.dec(cassandraResourceUtilization.throttlingMetrics.disableThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.disableReadReplicaTrafficThrottling.dec(cassandraResourceUtilization.throttlingMetrics.disableReadReplicaTrafficThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.disableMutationReplicaTrafficThrottling.dec(cassandraResourceUtilization.throttlingMetrics.disableMutationReplicaTrafficThrottling.getCount());

        cassandraResourceUtilization.resetThrottlingParams();

        cassandraResourceUtilization.throttlingMetrics.needsThrottling.dec(cassandraResourceUtilization.throttlingMetrics.needsThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.dec(cassandraResourceUtilization.throttlingMetrics.doesNotNeedThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.resetThrottling.dec(cassandraResourceUtilization.throttlingMetrics.resetThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.increaseThrottling.dec(cassandraResourceUtilization.throttlingMetrics.increaseThrottling.getCount());
        cassandraResourceUtilization.throttlingMetrics.disableThrottling.dec(cassandraResourceUtilization.throttlingMetrics.disableThrottling.getCount());

        RateLimiterService.instance.setThrottlingOptions(DatabaseDescriptor.getThrottlingOptions());
        cassandraResourceUtilization.throttlingOptions = RateLimiterService.instance.getThrottlingOptions();
        cassandraResourceUtilization.currentThrottlingPercentage = cassandraResourceUtilization.throttlingOptions.getPercentageOfTrafficToThrottling();


        KeyspaceThrottlingMetrics userKSMetrics = KeyspaceThrottlingMetricsManager.getMetrics(KEYSPACE_THROTTLE);
        userKSMetrics.addKSForReadThrottling.dec(userKSMetrics.addKSForReadThrottling.getCount());
        userKSMetrics.addKSForWriteThrottling.dec(userKSMetrics.addKSForWriteThrottling.getCount());
        userKSMetrics.skipKSThrottling.dec(userKSMetrics.skipKSThrottling.getCount());
        userKSMetrics.readRequestsTrendingUpward.dec(userKSMetrics.readRequestsTrendingUpward.getCount());
        userKSMetrics.writeRequestsTrendingUpward.dec(userKSMetrics.writeRequestsTrendingUpward.getCount());
        userKSMetrics.readLatencyTrendingUpward.dec(userKSMetrics.readLatencyTrendingUpward.getCount());
        userKSMetrics.writeLatencyTrendingUpward.dec(userKSMetrics.writeLatencyTrendingUpward.getCount());
        userKSMetrics.minReadThrottling.dec(userKSMetrics.minReadThrottling.getCount());
        userKSMetrics.minWriteThrottling.dec(userKSMetrics.minWriteThrottling.getCount());
        userKSMetrics.maxReadThrottling.dec(userKSMetrics.maxReadThrottling.getCount());
        userKSMetrics.maxWriteThrottling.dec(userKSMetrics.maxWriteThrottling.getCount());
        userKSMetrics.noReadThrottling.dec(userKSMetrics.noReadThrottling.getCount());
        userKSMetrics.noWriteThrottling.dec(userKSMetrics.noWriteThrottling.getCount());
        userKSMetrics.aggressiveThrottling.dec(userKSMetrics.aggressiveThrottling.getCount());
    }

    // we need to make sure rate limiter is disabled for the one-node-cluster test
    @BeforeClass
    public static void setUp()
    {
        ThrottlingOptions options = new ThrottlingOptions();
        options.setEnabled(false);
        DatabaseDescriptor.setThrottlingOptions(options);
    }
}
