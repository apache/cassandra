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

package org.apache.cassandra.service;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link org.apache.cassandra.service.AutoRepairService}
 */
public class AutoRepairServiceBasicTest extends CQLTester
{
    private static AutoRepairService autoRepairService;
    private static AutoRepairConfig config;

    @Before
    public void setUp()
    {
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        DatabaseDescriptor.setCDCEnabled(false);
        config = new AutoRepairConfig();
        autoRepairService = new AutoRepairService();
        autoRepairService.config = config;
    }

    @Test
    public void testSetup()
    {
        AutoRepairService.instance.config = null;

        AutoRepairService.setup();

        assertEquals(DatabaseDescriptor.getAutoRepairConfig(), AutoRepairService.instance.config);
    }

    @Test
    public void testGetAutoRepairConfigReturnsConfig()
    {
        assertEquals(config, autoRepairService.getAutoRepairConfig());
    }

    @Test
    public void testsetAutoRepairHistoryClearDeleteHostsBufferInSecV2()
    {
        autoRepairService.setAutoRepairHistoryClearDeleteHostsBufferDuration("100s");

        assertEquals(100, config.getAutoRepairHistoryClearDeleteHostsBufferInterval().toSeconds());
    }

    @Test
    public void testsetAutoRepairMaxRetriesCount()
    {
        autoRepairService.setAutoRepairMaxRetriesCount(AutoRepairConfig.RepairType.INCREMENTAL.name(), 101);

        assertEquals(101, config.getRepairMaxRetries(AutoRepairConfig.RepairType.INCREMENTAL));
    }

    @Test
    public void testsetAutoRepairRetryBackoffInSec()
    {
        autoRepairService.setAutoRepairRetryBackoff(AutoRepairConfig.RepairType.INCREMENTAL.name(), "102s");

        assertEquals(102, config.getRepairRetryBackoff(AutoRepairConfig.RepairType.INCREMENTAL).toSeconds());
    }

    @Test(expected = ConfigurationException.class)
    public void testSetAutoRepairEnabledThrowsWithSchedulerDisabled()
    {
        autoRepairService.config = new AutoRepairConfig(false);

        autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL.name(), true);
    }

    @Test
    public void testSetAutoRepairEnabledDoesNotThrowForIRWithMVReplayButMVRepairDisabled()
    {
        autoRepairService.config = new AutoRepairConfig(true);
        autoRepairService.config.setMaterializedViewRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL, false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL.name(), true);
    }

    @Test(expected = ConfigurationException.class)
    public void testSetAutoRepairEnabledThrowsForIRWithMVReplay()
    {
        autoRepairService.config = new AutoRepairConfig(true);
        autoRepairService.config.setMaterializedViewRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL, true);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL.name(), true);
    }

    @Test
    public void testSetAutoRepairEnabledDoesNotThrowForIRWithMVReplayDisabled()
    {
        autoRepairService.config = new AutoRepairConfig(true);
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL.name(), true);
    }

    @Test
    public void testSetAutoRepairEnabledDoesNotThrowForIRWithCDCReplayButCDCDisabled()
    {
        autoRepairService.config = new AutoRepairConfig(true);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        DatabaseDescriptor.setCDCEnabled(false);
        autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL.name(), true);
    }

    @Test(expected = ConfigurationException.class)
    public void testSetAutoRepairEnabledThrowsForIRWithCDCReplay()
    {
        autoRepairService.config = new AutoRepairConfig(true);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        DatabaseDescriptor.setCDCEnabled(true);
        autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL.name(), true);
    }

    @Test
    public void testSetAutoRepairEnabledDoesNotThrowForIRWithCDCReplayDisabled()
    {
        autoRepairService.config = new AutoRepairConfig(true);
        DatabaseDescriptor.setCDCEnabled(true);
        autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL.name(), true);
    }
}
