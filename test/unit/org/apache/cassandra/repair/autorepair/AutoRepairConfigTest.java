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

package org.apache.cassandra.repair.autorepair;

import java.util.HashSet;
import java.util.Map;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.Options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.AutoRepairConfig}
 */
@RunWith(Parameterized.class)
public class AutoRepairConfigTest extends CQLTester
{
    private AutoRepairConfig config;

    private final Set<String> testSet = ImmutableSet.of("dc1");

    @Parameterized.Parameter
    public AutoRepairConfig.RepairType repairType;

    @Parameterized.Parameters
    public static Object[] repairTypes()
    {
        return AutoRepairConfig.RepairType.values();
    }

    @Before
    public void setUp()
    {
        config = new AutoRepairConfig(true);
        AutoRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
    }

    @Test
    public void autoRepairConfigDefaultsAreNotNull()
    {
        AutoRepairConfig config = new AutoRepairConfig();
        assertNotNull(config.global_settings);
    }

    @Test
    public void autoRepairConfigRepairTypesAreNotNull()
    {
        AutoRepairConfig config = new AutoRepairConfig();
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            assertNotNull(config.getOptions(repairType));
        }
    }

    @Test
    public void testIsAutoRepairEnabledReturnsTrueWhenRepairIsEnabled()
    {
        config.global_settings.enabled = true;

        assertTrue(config.isAutoRepairEnabled(repairType));
    }

    @Test
    public void testRepairMinDuration()
    {
        config = new AutoRepairConfig(false);

        config.setRepairTaskMinDuration("3s");
        assertEquals(3L, config.getRepairTaskMinDuration().toSeconds());
    }

    @Test
    public void testIsAutoRepairEnabledReturnsTrueWhenRepairIsDisabledGlobally()
    {
        config = new AutoRepairConfig(false);
        config.global_settings.enabled = true;
        assertFalse(config.isAutoRepairEnabled(repairType));
    }

    @Test
    public void testIsAutoRepairEnabledReturnsTrueWhenRepairIsDisabledForRepairType()
    {
        config.global_settings.enabled = true;
        config.setAutoRepairEnabled(repairType, false);
        assertFalse(config.getOptions(repairType).enabled);
    }

    @Test
    public void testSetAutoRepairEnabledNoMVOrCDC()
    {
        DatabaseDescriptor.setCDCEnabled(false);
        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        config.setAutoRepairEnabled(repairType, true);

        assertTrue(config.getOptions(repairType).enabled);
    }

    @Test
    public void testSetRepairByKeyspace()
    {
        // Should default to true.
        assertTrue(config.getRepairByKeyspace(repairType));

        config.setRepairByKeyspace(repairType, false);

        assertFalse(config.getOptions(repairType).repair_by_keyspace);
    }

    @Test
    public void testGetRepairByKeyspace()
    {
        config.global_settings.repair_by_keyspace = true;

        boolean result = config.getRepairByKeyspace(repairType);

        assertTrue(result);
    }

    @Test
    public void testSetRepairThreads()
    {
        config.setRepairThreads(repairType, 5);

        assertEquals(5, config.getOptions(repairType).number_of_repair_threads.intValue());
    }

    @Test
    public void testGetRepairThreads()
    {
        config.global_settings.number_of_repair_threads = 5;

        int result = config.getRepairThreads(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testGetRepairMinFrequencyInHours()
    {
        config.global_settings.min_repair_interval = new DurationSpec.IntSecondsBound("5s");

        DurationSpec.IntSecondsBound result = config.getRepairMinInterval(repairType);

        assertEquals(5, result.toSeconds());
    }

    @Test
    public void testSetRepairMinFrequencyInHours()
    {
        config.setRepairMinInterval(repairType, "5s");

        assertEquals(5, config.getOptions(repairType).min_repair_interval.toSeconds());
    }

    @Test
    public void testGetAutoRepairHistoryClearDeleteHostsBufferInSec()
    {
        config.history_clear_delete_hosts_buffer_interval = new DurationSpec.IntSecondsBound("5s");

        int result = config.getAutoRepairHistoryClearDeleteHostsBufferInterval().toSeconds();

        assertEquals(5, result);
    }

    @Test
    public void testSetAutoRepairHistoryClearDeleteHostsBufferInSec()
    {
        config.setAutoRepairHistoryClearDeleteHostsBufferInterval("5s");

        assertEquals(new DurationSpec.IntSecondsBound("5s"), config.history_clear_delete_hosts_buffer_interval);
    }

    @Test
    public void testGetRepairSSTableCountHigherThreshold()
    {
        config.global_settings.sstable_upper_threshold = 5;

        int result = config.getRepairSSTableCountHigherThreshold(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetRepairSSTableCountHigherThreshold()
    {
        config.setRepairSSTableCountHigherThreshold(repairType, 5);

        assertEquals(5, config.getOptions(repairType).sstable_upper_threshold.intValue());
    }

    @Test
    public void testGetAutoRepairTableMaxRepairTimeInSec()
    {
        config.global_settings.table_max_repair_time = new DurationSpec.IntSecondsBound("5s");

        DurationSpec.IntSecondsBound result = config.getAutoRepairTableMaxRepairTime(repairType);

        assertEquals(5, result.toSeconds());
    }

    @Test
    public void testSetAutoRepairTableMaxRepairTimeInSec()
    {
        config.setAutoRepairTableMaxRepairTime(repairType, "5s");

    assertEquals(5, config.getOptions(repairType).table_max_repair_time.toSeconds());
}

    @Test
    public void testGetIgnoreDCs()
    {
        config.global_settings.ignore_dcs = testSet;

        Set<String> result = config.getIgnoreDCs(repairType);

        assertEquals(testSet, result);
    }

    @Test
    public void testSetIgnoreDCs()
    {
        config.setIgnoreDCs(repairType, testSet);

        assertEquals(config.getOptions(repairType).ignore_dcs, testSet);
    }

    @Test
    public void testGetRepairPrimaryTokenRangeOnly()
    {
        config.global_settings.repair_primary_token_range_only = true;

        boolean result = config.getRepairPrimaryTokenRangeOnly(repairType);

        assertTrue(result);
    }

    @Test
    public void testSetRepairPrimaryTokenRangeOnly()
    {
        config.setRepairPrimaryTokenRangeOnly(repairType, true);

        assertTrue(config.getOptions(repairType).repair_primary_token_range_only);
    }

    @Test
    public void testGetParallelRepairPercentageInGroup()
    {
        config.global_settings.parallel_repair_percentage = 5;

        int result = config.getParallelRepairPercentage(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetParallelRepairPercentageInGroup()
    {
        config.setParallelRepairPercentage(repairType, 5);

        assertEquals(5, config.getOptions(repairType).parallel_repair_percentage.intValue());
    }

    @Test
    public void testGetParallelRepairCountInGroup()
    {
        config.global_settings.parallel_repair_count = 5;

        int result = config.getParallelRepairCount(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetParallelRepairCountInGroup()
    {
        config.setParallelRepairCount(repairType, 5);

        assertEquals(5, config.getOptions(repairType).parallel_repair_count.intValue());
    }

    @Test
    public void testGetAllowParallelReplicaRepair()
    {
        // should default to false
        assertFalse(config.global_settings.allow_parallel_replica_repair);
        assertFalse(config.getAllowParallelReplicaRepair(repairType));

        // setting global to true should also cause repair type config to inherit.
        config.global_settings.allow_parallel_replica_repair = true;
        assertTrue(config.getAllowParallelReplicaRepair(repairType));

    }

    @Test
    public void testSetAllowParallelReplicaRepair()
    {
        // should default to false
        assertFalse(config.getAllowParallelReplicaRepair(repairType));

        // setting explicitly for repair type should update it
        config.setAllowParallelReplicaRepair(repairType, true);
        assertTrue(config.getAllowParallelReplicaRepair(repairType));
    }

    @Test
    public void testGetAllowParallelReplicaRepairAcrossSchedules()
    {
        // should default to true
        assertTrue(config.global_settings.allow_parallel_replica_repair_across_schedules);
        assertTrue(config.getAllowParallelReplicaRepairAcrossSchedules(repairType));

        // setting global to true should also cause repair type config to inherit.
        config.global_settings.allow_parallel_replica_repair_across_schedules = false;
        assertFalse(config.getAllowParallelReplicaRepairAcrossSchedules(repairType));

    }

    @Test
    public void testSetAllowParallelReplicaRepairAcrossSchedules()
    {
        // should default to true
        assertTrue(config.getAllowParallelReplicaRepairAcrossSchedules(repairType));

        // setting explicitly for repair type should update it
        config.setAllowParallelReplicaRepairAcrossSchedules(repairType, false);
        assertFalse(config.getAllowParallelReplicaRepairAcrossSchedules(repairType));
    }

    @Test
    public void testGetMaterializedViewRepairEnabled()
    {
        config.global_settings.materialized_view_repair_enabled = true;

        boolean result = config.getMaterializedViewRepairEnabled(repairType);

        assertTrue(result);
    }

    @Test
    public void testSetMVRepairEnabled()
    {
        config.setMaterializedViewRepairEnabled(repairType, true);

        assertTrue(config.getOptions(repairType).materialized_view_repair_enabled);
    }

    @Test
    public void testSetForceRepairNewNode()
    {
        config.setForceRepairNewNode(repairType, true);

        assertTrue(config.getOptions(repairType).force_repair_new_node);
    }

    @Test
    public void testGetForceRepairNewNode()
    {
        config.global_settings.force_repair_new_node = true;

        boolean result = config.getForceRepairNewNode(repairType);

        assertTrue(result);
    }

    @Test
    public void testIsAutoRepairSchedulingEnabledDefault()
    {
        config = new AutoRepairConfig();

        boolean result = config.isAutoRepairSchedulingEnabled();

        assertFalse(result);
    }

    @Test
    public void testIsAutoRepairSchedulingEnabledTrue()
    {
        boolean result = config.isAutoRepairSchedulingEnabled();

        assertTrue(result);
    }

    @Test
    public void testGetDefaultOptionsMVRepairIsEnabledByDefault()
    {
        Options defaultOptions = Options.getDefaultOptions();

        assertFalse(defaultOptions.materialized_view_repair_enabled);
    }

    @Test
    public void testGetDefaultOptionsTokenRangeSplitter()
    {
        Options defaultOptions = Options.getDefaultOptions();

        ParameterizedClass expectedDefault = new ParameterizedClass(RepairTokenRangeSplitter.class.getName(), Collections.emptyMap());

        assertEquals(expectedDefault, defaultOptions.token_range_splitter);
        assertEquals(RepairTokenRangeSplitter.class.getName(), AutoRepairConfig.newAutoRepairTokenRangeSplitter(repairType, defaultOptions.token_range_splitter).getClass().getName());
    }

    @Test(expected = ConfigurationException.class)
    public void testInvalidTokenRangeSplitter()
    {
        AutoRepairConfig.newAutoRepairTokenRangeSplitter(repairType, new ParameterizedClass("invalid-class", Collections.emptyMap()));
    }

    @Test
    public void testSetInitialSchedulerDelay()
    {
        config.setInitialSchedulerDelay(repairType, "5s");

        assertEquals(5, config.getOptions(repairType).initial_scheduler_delay.toSeconds());
    }

    @Test
    public void testGetInitialSchedulerDelay()
    {
        config.global_settings.initial_scheduler_delay = new DurationSpec.IntSecondsBound("5s");

        int result = config.getInitialSchedulerDelay(repairType).toSeconds();

        assertEquals(5, result);
    }

    @Test
    public void testSetRepairSessionTimeout()
    {
        config.setRepairSessionTimeout(repairType, "1h");

        assertEquals(3600, config.getOptions(repairType).repair_session_timeout.toSeconds());
    }

    @Test
    public void testDefaultOptions()
    {
        Map<AutoRepairConfig.RepairType, Options> defaultOptions = Options.getDefaultOptionsMap();
        Options options = defaultOptions.get(repairType);
        assertFalse(options.enabled);
        assertTrue(options.repair_by_keyspace);
        assertEquals(Integer.valueOf(1), options.number_of_repair_threads);
        assertEquals(Integer.valueOf(3), options.parallel_repair_count);
        assertEquals(Integer.valueOf(3), options.parallel_repair_percentage);
        assertEquals(Integer.valueOf(50000), options.sstable_upper_threshold);
        assertEquals(new HashSet<>(), options.ignore_dcs);
        assertTrue(options.repair_primary_token_range_only);
        assertFalse(options.force_repair_new_node);
        assertEquals(new DurationSpec.IntSecondsBound("6h"), options.table_max_repair_time);
        assertFalse(options.materialized_view_repair_enabled);
        assertEquals(new ParameterizedClass(RepairTokenRangeSplitter.class.getName(), Collections.emptyMap()), options.token_range_splitter);
        assertEquals(new DurationSpec.IntSecondsBound("5m"), options.initial_scheduler_delay);
        assertEquals(new DurationSpec.IntSecondsBound("3h"), options.repair_session_timeout);
        assertEquals(new DurationSpec.IntSecondsBound("24h"), options.min_repair_interval);
    }

    @Test
    public void testGlobalOptions()
    {
        AutoRepairConfig config = new AutoRepairConfig();
        assertFalse(config.global_settings.enabled);
        assertTrue(config.global_settings.repair_by_keyspace);
        assertEquals(Integer.valueOf(1), config.global_settings.number_of_repair_threads);
        assertEquals(Integer.valueOf(3), config.global_settings.parallel_repair_count);
        assertEquals(Integer.valueOf(3), config.global_settings.parallel_repair_percentage);
        assertEquals(Integer.valueOf(50000), config.global_settings.sstable_upper_threshold);
        assertEquals(new HashSet<>(), config.global_settings.ignore_dcs);
        assertTrue(config.global_settings.repair_primary_token_range_only);
        assertFalse(config.global_settings.force_repair_new_node);
        assertEquals(new DurationSpec.IntSecondsBound("6h"), config.global_settings.table_max_repair_time);
        assertFalse(config.global_settings.materialized_view_repair_enabled);
        assertEquals(new ParameterizedClass(RepairTokenRangeSplitter.class.getName(), Collections.emptyMap()), config.global_settings.token_range_splitter);
        assertEquals(new DurationSpec.IntSecondsBound("5m"), config.global_settings.initial_scheduler_delay);
        assertEquals(new DurationSpec.IntSecondsBound("3h"), config.global_settings.repair_session_timeout);
        assertEquals(new DurationSpec.IntSecondsBound("24h"), config.global_settings.min_repair_interval);
    }
}
