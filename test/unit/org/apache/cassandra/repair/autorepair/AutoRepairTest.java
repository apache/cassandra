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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.AutoRepair}
 */
public class AutoRepairTest extends CQLTester
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        setAutoRepairEnabled(true);
        requireNetwork();
        AutoRepairUtils.setup();
        StorageService.instance.doAutoRepairSetup();
    }

    @Before
    public void setup()
    {
        AutoRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(RepairType.FULL, true);
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(RepairType.INCREMENTAL, true);
        AutoRepairService.setup();
    }

    @Test
    public void testSetup()
    {
        AutoRepair.instance.setup();
        assertEquals(RepairType.values().length, AutoRepair.instance.repairExecutors.size());
        for (RepairType repairType : AutoRepair.instance.repairExecutors.keySet())
        {
            int expectedTasks = AutoRepair.instance.repairExecutors.get(repairType).getPendingTaskCount()
                    + AutoRepair.instance.repairExecutors.get(repairType).getActiveTaskCount();
            assertTrue(String.format("Expected > 0 task in queue for %s but was %s", repairType, expectedTasks),
                         expectedTasks > 0);
        }
    }

    @Test
    public void testSafeGuardSetupCall()
    {
        // only one should be setup, and rest should be ignored
        AutoRepair.instance.setup();
        AutoRepair.instance.setup();
        AutoRepair.instance.setup();

        assertEquals(RepairType.values().length, AutoRepair.instance.repairExecutors.size());
        for (RepairType repairType : AutoRepair.instance.repairExecutors.keySet())
        {
            int expectedTasks = AutoRepair.instance.repairExecutors.get(repairType).getPendingTaskCount()
                                + AutoRepair.instance.repairExecutors.get(repairType).getActiveTaskCount();
            assertTrue(String.format("Expected > 0 task in queue for %s but was %s", repairType, expectedTasks),
                       expectedTasks > 0);
        }
    }

    @Test(expected = ConfigurationException.class)
    public void testSetupFailsWhenIREnabledWithCDCReplay()
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(RepairType.INCREMENTAL, true);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        AutoRepair.instance.isSetupDone = false;
        AutoRepair.instance.setup();
    }

    @Test
    public void testNoFailureIfMVRepairOnButConfigIsOff()
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(RepairType.INCREMENTAL, true);
        DatabaseDescriptor.getAutoRepairConfig().setMaterializedViewRepairEnabled(RepairType.INCREMENTAL, false);
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        AutoRepair.instance.setup();
    }

    @Test(expected = ConfigurationException.class)
    public void testSetupFailsWhenIREnabledWithMVReplay()
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(RepairType.INCREMENTAL, true);
        DatabaseDescriptor.getAutoRepairConfig().setMaterializedViewRepairEnabled(RepairType.INCREMENTAL, true);
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        AutoRepair.instance.isSetupDone = false;
        AutoRepair.instance.setup();
    }

    @Test
    public void testCheckNTSreplicationNodeInsideOutsideDC()
    {
        String ksname1 = "ks_nts1";
        String ksname2 = "ks_nts2";
        Map<String, String> configOptions1 = new HashMap<>();
        configOptions1.put("datacenter1", "3");
        configOptions1.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(ksname1, KeyspaceParams.create(false, configOptions1)), false);
        Map<String, String> configOptions2 = new HashMap<>();
        configOptions2.put("datacenter2", "3");
        configOptions2.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(ksname2, KeyspaceParams.create(false, configOptions2)), false);

        for (Keyspace ks : Keyspace.all())
        {
            if (ks.getName().equals(ksname1))
            {
                // case 1 :
                // node reside in "datacenter1"
                // keyspace has replica in "datacenter1"
                Assert.assertTrue(AutoRepairUtils.shouldConsiderKeyspace(ks));
            }
            else if (ks.getName().equals(ksname2))
            {
                // case 2 :
                // node reside in "datacenter1"
                // keyspace has replica in "datacenter2"
                Assert.assertFalse(AutoRepairUtils.shouldConsiderKeyspace(ks));
            }
        }
    }

    @Test
    public void testForceRepairBypassesMinRepairInterval()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        long now = System.currentTimeMillis();

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Seed auto_repair_history directly with a recently completed repair and force_repair=true
        QueryProcessor.executeInternal(String.format(
            "INSERT INTO %s.%s (repair_type, host_id, repair_start_ts, repair_finish_ts, force_repair) VALUES (?, ?, ?, ?, true)",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY),
            repairType.toString(), myId, new java.util.Date(now - 1000), new java.util.Date(now));

        // Verify force repair is detected
        assertTrue(AutoRepairUtils.isForceRepairSetForNode(repairType, myId));

        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        AutoRepairState repairState = RepairType.getAutoRepairState(repairType, config);

        // Even though min_repair_interval hasn't passed, shouldSkipRepairDueToInterval returns false
        // because force repair is set
        assertFalse(AutoRepair.instance.shouldSkipRepairDueToInterval(repairType, repairState, config, myId));
    }

    @Test
    public void testShouldSkipRepairDueToIntervalWithoutForceRepair()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        long now = System.currentTimeMillis();

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Seed auto_repair_history directly with a recently completed repair and force_repair=false
        QueryProcessor.executeInternal(String.format(
            "INSERT INTO %s.%s (repair_type, host_id, repair_start_ts, repair_finish_ts, force_repair) VALUES (?, ?, ?, ?, false)",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY),
            repairType.toString(), myId, new java.util.Date(now - 1000), new java.util.Date(now));

        // Verify force repair is NOT set
        assertFalse(AutoRepairUtils.isForceRepairSetForNode(repairType, myId));

        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        AutoRepairState repairState = RepairType.getAutoRepairState(repairType, config);

        // Without force repair, should skip because min_repair_interval hasn't passed
        assertTrue(AutoRepair.instance.shouldSkipRepairDueToInterval(repairType, repairState, config, myId));
    }

    @Test
    public void testIsForceRepairSetForNodeReturnsFalseWhenNotSet()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        long now = System.currentTimeMillis();

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Seed auto_repair_history directly without setting force_repair
        QueryProcessor.executeInternal(String.format(
            "INSERT INTO %s.%s (repair_type, host_id, repair_start_ts, repair_finish_ts) VALUES (?, ?, ?, ?)",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY),
            repairType.toString(), myId, new java.util.Date(now - 1000), new java.util.Date(now));

        // Verify force repair is not set
        assertFalse(AutoRepairUtils.isForceRepairSetForNode(repairType, myId));
    }

    @Test
    public void testIsForceRepairSetForNodeReturnsFalseWhenNoHistory()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Verify force repair is not set when no history exists
        assertFalse(AutoRepairUtils.isForceRepairSetForNode(repairType, myId));
    }

    @Test
    public void testForceRepairBypassesMinRepairIntervalEndToEnd()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        long now = System.currentTimeMillis();

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Insert a recently completed repair so tooSoonToRunRepair would normally block
        QueryProcessor.executeInternal(String.format(
            "INSERT INTO %s.%s (repair_type, host_id, repair_start_ts, repair_finish_ts, force_repair) VALUES (?, ?, ?, ?, true)",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY),
            repairType.toString(), myId, new java.util.Date(now - 1000), new java.util.Date(now));

        // Invoke the full repair path end-to-end; with force repair set, the
        // shouldSkipRepairDueToInterval check is bypassed and this should not throw.
        AutoRepair.instance.repair(repairType);
    }

    @Test
    public void testTooSoonToRunRepairAllowsResumeOfInProgressRepair()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        long now = System.currentTimeMillis();

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Insert with start_ts > finish_ts to simulate in-progress repair (crashed before completion)
        AutoRepairUtils.insertNewRepairHistory(repairType, myId, now, now - 1000);

        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        AutoRepairState repairState = RepairType.getAutoRepairState(repairType, config);

        // Even though finish_ts was very recent, should return false because repair is in progress
        assertFalse(AutoRepair.instance.tooSoonToRunRepair(repairType, repairState, config, myId));
    }

    @Test
    public void testTooSoonToRunRepairReturnsTrueWhenRepairCompletedRecently()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        long now = System.currentTimeMillis();

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Insert with finish_ts > start_ts to simulate completed repair
        AutoRepairUtils.insertNewRepairHistory(repairType, myId, now - 1000, now);

        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        AutoRepairState repairState = RepairType.getAutoRepairState(repairType, config);

        // Should return true because repair completed recently and min_repair_interval hasn't passed
        assertTrue(AutoRepair.instance.tooSoonToRunRepair(repairType, repairState, config, myId));
    }

    @Test
    public void testTooSoonToRunRepairAllowsResumeWhenStartEqualsFinish()
    {
        RepairType repairType = RepairType.FULL;
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        long now = System.currentTimeMillis();

        // Truncate history table to start fresh
        QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));

        // Insert with start_ts == finish_ts (edge case: repair never started progressing)
        AutoRepairUtils.insertNewRepairHistory(repairType, myId, now, now);

        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        AutoRepairState repairState = RepairType.getAutoRepairState(repairType, config);

        // Should return false because repair is incomplete (start >= finish edge case)
        assertFalse(AutoRepair.instance.tooSoonToRunRepair(repairType, repairState, config, myId));
    }
}
