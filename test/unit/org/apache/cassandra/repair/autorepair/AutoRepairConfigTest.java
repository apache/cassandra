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

import java.util.EnumMap;
import java.util.Objects;
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
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class AutoRepairConfigTest extends CQLTester
{
    private AutoRepairConfig config;

    private Set<String> testSet = ImmutableSet.of("dc1");

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
        config.repair_type_overrides = null;
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
            assertNotNull(config.repair_type_overrides.get(repairType));
        }
    }

    @Test
    public void testIsAutoRepairEnabledReturnsTrueWhenRepairIsEnabled()
    {
        config.global_settings.enabled = true;

        assertTrue(config.isAutoRepairEnabled(repairType));
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
        config.repair_type_overrides = new EnumMap<>(AutoRepairConfig.RepairType.class);
        config.repair_type_overrides.put(repairType, new Options());
        config.repair_type_overrides.get(repairType).enabled = false;
        assertFalse(config.isAutoRepairEnabled(repairType));
    }

    @Test
    public void testSetAutoRepairEnabledNoMVOrCDC()
    {
        DatabaseDescriptor.setCDCEnabled(false);
        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        config.setAutoRepairEnabled(repairType, true);

        assertTrue(config.repair_type_overrides.get(repairType).enabled);
    }

    @Test
    public void testSetRepairByKeyspace()
    {
        config.setRepairByKeyspace(repairType, true);

        assertTrue(config.repair_type_overrides.get(repairType).repair_by_keyspace);
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

        assert config.repair_type_overrides.get(repairType).number_of_repair_threads == 5;
    }

    @Test
    public void testGetRepairThreads()
    {
        config.global_settings.number_of_repair_threads = 5;

        int result = config.getRepairThreads(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testGetRepairSubRangeNum()
    {
        config.global_settings.number_of_subranges = 5;

        int result = config.getRepairSubRangeNum(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetRepairSubRangeNum()
    {
        config.setRepairSubRangeNum(repairType, 5);

        assert config.repair_type_overrides.get(repairType).number_of_subranges == 5;
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

        assert config.repair_type_overrides.get(repairType).min_repair_interval.toSeconds() == 5;
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

        assert Objects.equals(config.history_clear_delete_hosts_buffer_interval, new DurationSpec.IntSecondsBound("5s"));
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

        assert config.repair_type_overrides.get(repairType).sstable_upper_threshold == 5;
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

        assert config.repair_type_overrides.get(repairType).table_max_repair_time.toSeconds() == 5;
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

        assertEquals(config.repair_type_overrides.get(repairType).ignore_dcs, testSet);
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

        assertTrue(config.repair_type_overrides.get(repairType).repair_primary_token_range_only);
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

        assert config.repair_type_overrides.get(repairType).parallel_repair_percentage == 5;
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

        assert config.repair_type_overrides.get(repairType).parallel_repair_count == 5;
    }

    @Test
    public void testGetMVRepairEnabled()
    {
        config.global_settings.mv_repair_enabled = true;

        boolean result = config.getMVRepairEnabled(repairType);

        assertTrue(result);
    }

    @Test
    public void testSetMVRepairEnabled()
    {
        config.setMVRepairEnabled(repairType, true);

        assertTrue(config.repair_type_overrides.get(repairType).mv_repair_enabled);
    }

    @Test
    public void testSetForceRepairNewNode()
    {
        config.setForceRepairNewNode(repairType, true);

        assertTrue(config.repair_type_overrides.get(repairType).force_repair_new_node);
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

        assertFalse(defaultOptions.mv_repair_enabled);
    }

    @Test
    public void testGetDefaultOptionsTokenRangeSplitter()
    {
        Options defaultOptions = Options.getDefaultOptions();

        ParameterizedClass expectedDefault = new ParameterizedClass(DefaultAutoRepairTokenSplitter.class.getName(), Collections.emptyMap());

        assertEquals(expectedDefault, defaultOptions.token_range_splitter);
        assertEquals(DefaultAutoRepairTokenSplitter.class.getName(), FBUtilities.newAutoRepairTokenRangeSplitter(defaultOptions.token_range_splitter).getClass().getName());
    }

    @Test(expected = ConfigurationException.class)
    public void testInvalidTokenRangeSplitter()
    {
        FBUtilities.newAutoRepairTokenRangeSplitter(new ParameterizedClass("invalid-class", Collections.emptyMap()));
    }

    @Test
    public void testSetInitialSchedulerDelay()
    {
        config.setInitialSchedulerDelay(repairType, "5s");

        assert config.repair_type_overrides.get(repairType).initial_scheduler_delay.toSeconds() == 5;
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

        assert config.repair_type_overrides.get(repairType).repair_session_timeout.toSeconds() == 3600;
    }

}
