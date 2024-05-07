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

package org.apache.cassandra.repair;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.AutoRepairConfig.Options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    public void testSetAutoRepairEnabledWithMV()
    {
        DatabaseDescriptor.setCDCEnabled(false);
        DatabaseDescriptor.setMaterializedViewsEnabled(true);

        try
        {
            config.setAutoRepairEnabled(repairType, true);

            if (repairType == AutoRepairConfig.RepairType.incremental)
            {
                assertFalse(config.repair_type_overrides.get(repairType).enabled); // IR should not be allowed with MV
                assertNotEquals(AutoRepairConfig.RepairType.incremental, repairType); // should receive exception
            }
            else
            {
                assertTrue(config.repair_type_overrides.get(repairType).enabled);
            }
        }
        catch (ConfigurationException e)
        {
            // should throw only if repairType is incremental
            assertEquals(AutoRepairConfig.RepairType.incremental, repairType);
        }
    }

    @Test
    public void testSetAutoRepairEnabledWithCDC()
    {
        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setMaterializedViewsEnabled(false);

        try
        {
            config.setAutoRepairEnabled(repairType, true);


            if (repairType == AutoRepairConfig.RepairType.incremental)
            {
                assertFalse(config.repair_type_overrides.get(repairType).enabled); // IR should not be allowed with CDC
                assertNotEquals(AutoRepairConfig.RepairType.incremental, repairType); // should receive exception
            }
            else
            {
                assertTrue(config.repair_type_overrides.get(repairType).enabled);
            }
        }
        catch (ConfigurationException e)
        {
            // should throw only if repairType is incremental
            assertEquals(AutoRepairConfig.RepairType.incremental, repairType);
        }
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
        config.global_settings.min_repair_interval_in_hours = 5;

        int result = config.getRepairMinIntervalInHours(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetRepairMinFrequencyInHours()
    {
        config.setRepairMinIntervalInHours(repairType, 5);

        assert config.repair_type_overrides.get(repairType).min_repair_interval_in_hours == 5;
    }

    @Test
    public void testGetAutoRepairHistoryClearDeleteHostsBufferInSec()
    {
        config.history_clear_delete_hosts_buffer_in_sec = 5;

        int result = config.getAutoRepairHistoryClearDeleteHostsBufferInSec();

        assertEquals(5, result);
    }

    @Test
    public void testSetAutoRepairHistoryClearDeleteHostsBufferInSec()
    {
        config.setAutoRepairHistoryClearDeleteHostsBufferInSec(5);

        assert config.history_clear_delete_hosts_buffer_in_sec == 5;
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
    public void testGetRepairIgnoreKeyspaces()
    {
        config.global_settings.ignore_keyspaces = "keyspace1";

        String result = config.getRepairIgnoreKeyspaces(repairType);

        assertEquals("keyspace1", result);
    }

    @Test
    public void testSetRepairIgnoreKeyspaces()
    {
        config.setRepairIgnoreKeyspaces(repairType, "keyspace1");

        assertEquals(config.repair_type_overrides.get(repairType).ignore_keyspaces, "keyspace1");
    }

    @Test
    public void testGetRepairOnlyKeyspaces()
    {
        config.global_settings.repair_only_keyspaces = "keyspace1";

        String result = config.getRepairOnlyKeyspaces(repairType);

        assertEquals("keyspace1", result);
    }

    @Test
    public void testSetRepairOnlyKeyspaces()
    {
        config.setRepairOnlyKeyspaces(repairType, "keyspace1");

        assertEquals(config.repair_type_overrides.get(repairType).repair_only_keyspaces, "keyspace1");
    }

    @Test
    public void testGetAutoRepairTableMaxRepairTimeInSec()
    {
        config.global_settings.table_max_repair_time_in_sec = (long) 5;

        long result = config.getAutoRepairTableMaxRepairTimeInSec(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetAutoRepairTableMaxRepairTimeInSec()
    {
        config.setAutoRepairTableMaxRepairTimeInSec(repairType, 5);

        assert config.repair_type_overrides.get(repairType).table_max_repair_time_in_sec == 5;
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
    public void testGetDCGroups()
    {
        config.global_settings.repair_dc_groups = testSet;

        Set<String> result = config.getDCGroups(repairType);

        assertEquals(testSet, result);
    }

    @Test
    public void testSetDCGroups()
    {
        config.setDCGroups(repairType, testSet);

        assertEquals(config.repair_type_overrides.get(repairType).repair_dc_groups, testSet);
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
        config.global_settings.parallel_repair_percentage_in_group = 5;

        int result = config.getParallelRepairPercentageInGroup(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetParallelRepairPercentageInGroup()
    {
        config.setParallelRepairPercentageInGroup(repairType, 5);

        assert config.repair_type_overrides.get(repairType).parallel_repair_percentage_in_group == 5;
    }

    @Test
    public void testGetParallelRepairCountInGroup()
    {
        config.global_settings.parallel_repair_count_in_group = 5;

        int result = config.getParallelRepairCountInGroup(repairType);

        assertEquals(5, result);
    }

    @Test
    public void testSetParallelRepairCountInGroup()
    {
        config.setParallelRepairCountInGroup(repairType, 5);

        assert config.repair_type_overrides.get(repairType).parallel_repair_count_in_group == 5;
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
    public void testApplyOverridesUsesDefaultOptions()
    {
        config.repair_type_overrides.put(repairType, null);
        config.global_settings = null;
        Options.defaultOptions.ignore_keyspaces = "testtest";

        assertEquals("testtest", config.applyOverrides(repairType, opt -> opt.ignore_keyspaces));

        Options.defaultOptions.ignore_keyspaces = Options.getDefaultOptions().ignore_keyspaces;
    }

    @Test
    public void testApplyOverridesUsesGlobalSettings()
    {
        config.repair_type_overrides.put(repairType, null);
        config.global_settings.ignore_keyspaces = "testtest";
        Options.defaultOptions.ignore_keyspaces = null;

        assertEquals("testtest", config.applyOverrides(repairType, opt -> opt.ignore_keyspaces));

        Options.defaultOptions.ignore_keyspaces = Options.getDefaultOptions().ignore_keyspaces;
    }

    @Test
    public void testApplyOverridesUsesRepairTypeOptions()
    {
        Options repairTypeOverride = new Options();
        repairTypeOverride.ignore_keyspaces = "testtest";
        config.repair_type_overrides.put(repairType, repairTypeOverride);
        config.global_settings = null;
        Options.defaultOptions.ignore_keyspaces = null;

        assertEquals("testtest", config.applyOverrides(repairType, opt -> opt.ignore_keyspaces));

        Options.defaultOptions.ignore_keyspaces = Options.getDefaultOptions().ignore_keyspaces;
    }

    @Test
    public void testGetDefaultOptionsMVRepairIsEnabledByDefault()
    {
        Options defaultOptions = Options.getDefaultOptions();

        assertTrue(defaultOptions.mv_repair_enabled);
    }
}
