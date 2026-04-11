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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AutoRepairMetricsTest extends CQLTester
{

    private AutoRepairMetrics metrics;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        setAutoRepairEnabled(true);
        requireNetwork();
        AutoRepairUtils.setup();
        StorageService.instance.doAutoRepairSetup();

        // Set min repair interval to an hour.
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairMinInterval(RepairType.FULL, "1h");
    }

    @Before
    public void setup()
    {
        metrics = AutoRepairMetricsManager.getMetrics(RepairType.FULL);
    }

    @Test
    public void testShouldRecordRepairStartLagAndResetOnMyTurn()
    {
        // record a last finish repair time of one day.
        long oneDayAgo = AutoRepair.instance.currentTimeMs() - 86_400_000;
        metrics.recordRepairStartLag(oneDayAgo);

        // expect a recorded lag time of approximately 1 day (last repair finish time) - 1 hour (min repair interval)
        long expectedLag = 86400 - 3600;
        long recordedLag = metrics.repairStartLagSec.getValue();
        assertTrue(String.format("Expected at last 23h of lag (%d) but got (%d)", expectedLag, recordedLag),
                   recordedLag >= expectedLag);
        // Given timing, allow at most 5 seconds of skew.
        assertTrue(String.format("Expected 23h of lag (%d) but got a larger value (%d)", expectedLag, recordedLag),
                   recordedLag <= expectedLag + 5);

        // expect lag time to be restarted when recording a turn.
        metrics.recordTurn(RepairTurn.MY_TURN);
        assertEquals(0, metrics.repairStartLagSec.getValue().intValue());
    }

    @Test
    public void testShouldRecordRepairStartLagOfZeroWhenFinishTimeIsWithinMinRepairInterval()
    {
        // record a last finish repair time of one 30 minutes
        long thirtyMinutesAgo = AutoRepair.instance.currentTimeMs() - 1_800_000;
        metrics.recordRepairStartLag(thirtyMinutesAgo);

        // expect 0 lag because last repair finish time was less than min repair interval
        assertEquals(0, metrics.repairStartLagSec.getValue().intValue());
    }
}
