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

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import com.google.common.collect.Sets;

import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.cassandra.repair.AutoRepairUtils.RepairTurn.*;

public class AutoRepairTest extends CQLTester
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private static TableMetadata cfm;
    ColumnFamilyStore cfs;

    public AutoRepairTest()
    {
        requireNetwork();
        AutoRepair.instance.setup();
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        cfm = TableMetadata.builder(KEYSPACE, TABLE)
                           .addPartitionKeyColumn("k", UTF8Type.instance)
                           .addStaticColumn("s", UTF8Type.instance)
                           .addClusteringColumn("i", IntegerType.instance)
                           .addRegularColumn("v", UTF8Type.instance)
                           .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm);
        cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
    }

    @Before
    public void truncate()
    {
        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.truncateBlocking();

        cfs = Keyspace.open(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME).getColumnFamilyStore(AutoRepairKeyspace.AUTO_REPAIR_STATUS);
        cfs.truncateBlocking();

        AutoRepairService.instance.startAutoRepair();
        executeCQL();
    }

    private void executeCQL()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k', 's')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    @Test
    public void testRepairTurn() throws Throwable
    {
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepairUtils.myTurnToRunRepair(myId) != NOT_MY_TURN);
    }

    @Test
    public void testRepair() throws Throwable
    {
        AutoRepairService.instance.setRepairMinFrequencyInHours(-1);
        AutoRepair.repair(0);
        long lastRepairTime = AutoRepair.instance.getLastRepairTime();
        //if repair was done then lastRepairTime should be non-zero
        Assert.assertTrue(String.format("Expected lastRepairTime > 0, actual value lastRepairTime %d",
                lastRepairTime), lastRepairTime > 0);
    }

    @Test
    public void testTooFrequentRepairs() throws Throwable
    {
        //in the first round let repair run
        AutoRepairService.instance.setRepairMinFrequencyInHours(-1);
        AutoRepair.repair(0);
        long lastRepairTime1 = AutoRepair.instance.getLastRepairTime();
        Assert.assertNotSame(String.format("Expected total repaired tables > 0, actual value %s ", AutoRepair.instance
                .getTotalTablesConsideredForRepair()), AutoRepair.instance.getTotalTablesConsideredForRepair(), 0);

        //if repair was done in last 24 hours then it should not trigger another repair
        AutoRepairService.instance.setRepairMinFrequencyInHours(24);
        AutoRepair.repair(0);
        long lastRepairTime2 = AutoRepair.instance.getLastRepairTime();
        Assert.assertEquals(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                "lastRepairTime2 %d", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
        Assert.assertEquals("Expected total repaired tables = 0, actual value: " + AutoRepair.instance
                .getTotalTablesConsideredForRepair(), AutoRepair.instance
                .getTotalTablesConsideredForRepair(), 0);
    }

    @Test
    public void testNonFrequentRepairs() throws Throwable
    {
        AutoRepairService.instance.setRepairMinFrequencyInHours(-1);
        AutoRepair.repair(0);
        long lastRepairTime1 = AutoRepair.instance.getLastRepairTime();
        Assert.assertTrue(String.format("Expected lastRepairTime1 > 0, actual value lastRepairTime1 %d",
                lastRepairTime1), lastRepairTime1 > 0);
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepairUtils.myTurnToRunRepair(myId) !=
                NOT_MY_TURN);
        AutoRepair.repair(0);
        long lastRepairTime2 = AutoRepair.instance.getLastRepairTime();
        Assert.assertNotSame(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                "lastRepairTime2 ", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
    }

    @Test
    public void testGetPriorityHosts() throws Throwable
    {
        AutoRepairService.instance.setRepairMinFrequencyInHours(-1);
        Assert.assertSame(String.format("Priority host count is not same, actual value %d, expected value %d",
                AutoRepairUtils.getPriorityHosts().size(), 0), AutoRepairUtils.getPriorityHosts().size(), 0);
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepairUtils.myTurnToRunRepair(myId) !=
                NOT_MY_TURN);
        AutoRepair.repair(0);
        AutoRepairUtils.addPriorityHost(Sets.newHashSet(FBUtilities.getBroadcastAddressAndPort()));
        AutoRepair.repair(0);
        Assert.assertSame(String.format("Priority host count is not same actual value %d, expected value %d", AutoRepairUtils
                .getPriorityHosts().size(), 0), AutoRepairUtils.getPriorityHosts().size(), 0);
    }

    @Test
    public void testCheckAutoRepairStartStop() throws Throwable
    {
        AutoRepairService.instance.setRepairMinFrequencyInHours(-1);
        AutoRepairService.instance.stopAutoRepair();
        long lastRepairTime1 = AutoRepair.instance.getLastRepairTime();
        AutoRepair.repair(0);
        long lastRepairTime2 = AutoRepair.instance.getLastRepairTime();
        //Since repair has not happened, both the last repair times should be same
        Assert.assertEquals(String.format("Expected lastRepairTime1 %d, and lastRepairTime2 %d to be same",
                lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);

        AutoRepairService.instance.startAutoRepair();
        AutoRepair.repair(0);
        //since repair is done now, so lastRepairTime1/lastRepairTime2 and lastRepairTime3 should not be same
        long lastRepairTime3 = AutoRepair.instance.getLastRepairTime();
        Assert.assertNotSame(String.format("Expected lastRepairTime1 %d, and lastRepairTime3 %d to be not same",
                lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime3);
    }

    @Test
    public void testCheckNTSreplicationNodeInsideOutsideDC()
    {
        String ksname1 = "ks_nts1";
        String ksname2 = "ks_nts2";
        Map<String, String> configOptions1 = new HashMap<>();
        configOptions1.put("datacenter1", "3");
        configOptions1.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");
        KeyspaceMetadata meta1 = KeyspaceMetadata.create(ksname1, KeyspaceParams.create(false, configOptions1));
        SchemaTestUtil.addOrUpdateKeyspace(meta1, false);
        Map<String, String> configOptions2 = new HashMap<>();
        configOptions2.put("datacenter2", "3");
        configOptions2.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");
        KeyspaceMetadata meta2 = KeyspaceMetadata.create(ksname2, KeyspaceParams.create(false, configOptions2));
        SchemaTestUtil.addOrUpdateKeyspace(meta2, false);

        for (Keyspace ks : Keyspace.all()) {
            if (ks.getName().equals(ksname1)) {
                // case 1 :
                // node reside in "datacenter1"
                // keyspace has replica in "datacenter1"
                Assert.assertTrue(AutoRepairUtils.checkNodeContainsKeyspaceReplica(ks));
            }
            else if (ks.getName().equals(ksname2)) {
                // case 2 :
                // node reside in "datacenter1"
                // keyspace has replica in "datacenter2"
                Assert.assertFalse(AutoRepairUtils.checkNodeContainsKeyspaceReplica(ks));
            }
        }
    }
}
