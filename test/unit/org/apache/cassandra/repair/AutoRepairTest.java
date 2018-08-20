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

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.repair.AutoRepair;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
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

import java.util.UUID;

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
        StorageService.instance.startAutoRepair();
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
        Assert.assertTrue("Expected my turn for the repair", AutoRepair.instance.myTurnToRunRepair(myId));
    }

    @Test
    public void testRepair() throws Throwable
    {
        AutoRepair.instance.setMinRepairFrequencyInHours(-1);
        AutoRepair.repair(false);
        long lastRepairTime = AutoRepair.instance.getLastRepairTime();
        //if repair was done then lastRepairTime should be non-zero
        Assert.assertTrue("Expected lastRepairTime > 0, actual value lastRepairTime: " + lastRepairTime, lastRepairTime > 0);
    }

    @Test
    public void testTooFrequentRepairs() throws Throwable
    {
        //in the first round let repair run
        AutoRepair.instance.setMinRepairFrequencyInHours(-1);
        AutoRepair.repair(false);
        long lastRepairTime1 = AutoRepair.instance.getLastRepairTime();
        Assert.assertNotSame("Expected total repaired tables > 0, actual value: " + AutoRepair.instance
                .getTotalTablesConsideredForRepair(), AutoRepair.instance.getTotalTablesConsideredForRepair(), 0);

        //if repair was done in last 24 hours then it should not trigger another repair
        AutoRepair.instance.setMinRepairFrequencyInHours(24);
        AutoRepair.repair(false);
        long lastRepairTime2 = AutoRepair.instance.getLastRepairTime();
        Assert.assertEquals("Expected repair time to be same, actual value lastRepairTime1: " + lastRepairTime1 + "," +
                " lastRepairTime2: " + lastRepairTime2, lastRepairTime1, lastRepairTime2);
        Assert.assertEquals("Expected total repaired tables = 0, actual value: " + AutoRepair.instance
                .getTotalTablesConsideredForRepair(), AutoRepair.instance
                .getTotalTablesConsideredForRepair(), 0);
    }

    @Test
    public void testNonFrequentRepairs() throws Throwable
    {
        AutoRepair.instance.setMinRepairFrequencyInHours(-1);
        AutoRepair.repair(false);
        long lastRepairTime1 = AutoRepair.instance.getLastRepairTime();
        Assert.assertTrue("Expected lastRepairTime1 > 0, actual value lastRepairTime1: " + lastRepairTime1, lastRepairTime1 > 0);
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepair.instance.myTurnToRunRepair(myId));
        AutoRepair.repair(false);
        long lastRepairTime2 = AutoRepair.instance.getLastRepairTime();
        Assert.assertNotSame("Expected repair time to be same, actual value lastRepairTime1: " + lastRepairTime1 +
                ", lastRepairTime2: " + lastRepairTime2, lastRepairTime1, lastRepairTime2);
    }
}
