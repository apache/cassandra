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

import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.schema.SchemaTestUtil;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.junit.Assert.assertEquals;

public class AutoRepairTest extends CQLTester
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        setAutoRepairEnabled(true);
        requireNetwork();
    }

    @Test
    public void testSetup()
    {
        AutoRepair instance = new AutoRepair();

        instance.setup();

        assertEquals(RepairType.values().length, instance.repairExecutors.size());
        for (RepairType repairType : instance.repairExecutors.keySet())
        {
            assertEquals(String.format("Expected 1 task in queue for %s", repairType),
                         1, instance.repairExecutors.get(repairType).getPendingTaskCount()
                            + instance.repairExecutors.get(repairType).getActiveTaskCount());
        }
    }

    @Test(expected = ConfigurationException.class)
    public void testSetupFailsWhenIREnabledWithCDC()
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(RepairType.incremental, true);
        DatabaseDescriptor.setCDCEnabled(true);

        AutoRepair instance = new AutoRepair();
        instance.setup();
    }

    @Test(expected = ConfigurationException.class)
    public void testSetupFailsWhenIREnabledWithMV()
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(RepairType.incremental, true);
        DatabaseDescriptor.setMaterializedViewsEnabled(true);

        AutoRepair instance = new AutoRepair();
        instance.setup();
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
                Assert.assertTrue(AutoRepairUtils.checkNodeContainsKeyspaceReplica(ks));
            }
            else if (ks.getName().equals(ksname2))
            {
                // case 2 :
                // node reside in "datacenter1"
                // keyspace has replica in "datacenter2"
                Assert.assertFalse(AutoRepairUtils.checkNodeContainsKeyspaceReplica(ks));
            }
        }
    }
}
