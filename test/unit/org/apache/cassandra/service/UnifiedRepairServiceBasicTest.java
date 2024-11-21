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
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairConfig;

import static org.junit.Assert.assertEquals;

public class UnifiedRepairServiceBasicTest extends CQLTester {
    private static UnifiedRepairService unifiedRepairService;
    private static UnifiedRepairConfig config;

    @Before
    public void setUp() {
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        DatabaseDescriptor.setCDCEnabled(false);
        config = new UnifiedRepairConfig();
        unifiedRepairService = new UnifiedRepairService();
        unifiedRepairService.config = config;
    }

    @Test
    public void testSetup() {
        UnifiedRepairService.instance.config = null;

        UnifiedRepairService.setup();

        assertEquals(DatabaseDescriptor.getUnifiedRepairConfig(), UnifiedRepairService.instance.config);
    }

    @Test
    public void testGetUnifiedRepairConfigReturnsConfig() {
        assertEquals(config, unifiedRepairService.getUnifiedRepairConfig());
    }

    @Test
    public void testsetUnifiedRepairHistoryClearDeleteHostsBufferInSecV2() {
        unifiedRepairService.setUnifiedRepairHistoryClearDeleteHostsBufferDuration("100s");

        assertEquals(100, config.getUnifiedRepairHistoryClearDeleteHostsBufferInterval().toSeconds());
    }


    @Test
    public void testsetUnifiedRepairMaxRetriesCount() {
        unifiedRepairService.setUnifiedRepairMaxRetriesCount(101);

        assertEquals(101, config.getRepairMaxRetries());
    }


    @Test
    public void testsetUnifiedRepairRetryBackoffInSec() {
        unifiedRepairService.setUnifiedRepairRetryBackoff("102s");

        assertEquals(102, config.getRepairRetryBackoff().toSeconds());
    }

    @Test(expected = ConfigurationException.class)
    public void testSetUnifiedRepairEnabledThrowsWithSchedulerDisabled() {
        unifiedRepairService.config = new UnifiedRepairConfig(false);

        unifiedRepairService.setUnifiedRepairEnabled(UnifiedRepairConfig.RepairType.incremental, true);
    }

    @Test(expected = ConfigurationException.class)
    public void testSetUnifiedRepairEnabledThrowsForIRWithMVReplay() {
        unifiedRepairService.config = new UnifiedRepairConfig(true);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        unifiedRepairService.setUnifiedRepairEnabled(UnifiedRepairConfig.RepairType.incremental, true);
    }

    @Test
    public void testSetUnifiedRepairEnabledDoesNotThrowForIRWithMVReplayDisabled() {
        unifiedRepairService.config = new UnifiedRepairConfig(true);
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        unifiedRepairService.setUnifiedRepairEnabled(UnifiedRepairConfig.RepairType.incremental, true);
    }

    @Test(expected = ConfigurationException.class)
    public void testSetUnifiedRepairEnabledThrowsForIRWithCDCReplay() {
        unifiedRepairService.config = new UnifiedRepairConfig(true);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        unifiedRepairService.setUnifiedRepairEnabled(UnifiedRepairConfig.RepairType.incremental, true);
    }

    @Test
    public void testSetUnifiedRepairEnabledDoesNotThrowForIRWithCDCReplayDisabled() {
        unifiedRepairService.config = new UnifiedRepairConfig(true);
        DatabaseDescriptor.setCDCEnabled(true);
        unifiedRepairService.setUnifiedRepairEnabled(UnifiedRepairConfig.RepairType.incremental, true);
    }
}
