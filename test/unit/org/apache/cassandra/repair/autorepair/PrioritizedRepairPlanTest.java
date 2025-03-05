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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.PrioritizedRepairPlan}
 */
public class PrioritizedRepairPlanTest extends CQLTester
{
    @Test
    public void testBuildWithDifferentPriorities()
    {
        // Test reordering assignments with different priorities
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '3'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '1'}");

        List<PrioritizedRepairPlan> prioritizedRepairPlans = PrioritizedRepairPlan.buildSingleKeyspacePlan(AutoRepairConfig.RepairType.FULL, KEYSPACE, table1, table2, table3);
        assertEquals(3, prioritizedRepairPlans.size());

        // Verify the order is by descending priority and matches the expected tables
        assertEquals(3, prioritizedRepairPlans.get(0).getPriority());
        assertEquals(table2, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().get(0).getTableNames().get(0));

        assertEquals(2, prioritizedRepairPlans.get(1).getPriority());
        assertEquals(table1, prioritizedRepairPlans.get(1).getKeyspaceRepairPlans().get(0).getTableNames().get(0));

        assertEquals(1, prioritizedRepairPlans.get(2).getPriority());
        assertEquals(table3, prioritizedRepairPlans.get(2).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
    }

    @Test
    public void testBuildWithSamePriority()
    {
        // Test reordering assignments with the same priority
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '2'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '2'}");

        // Expect only 1 plan since all tables share the same priority
        List<PrioritizedRepairPlan> prioritizedRepairPlans = PrioritizedRepairPlan.buildSingleKeyspacePlan(AutoRepairConfig.RepairType.FULL, KEYSPACE, table1, table2, table3);
        assertEquals(1, prioritizedRepairPlans.size());

        // Verify all tables present in the plan
        assertEquals(1, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().size());
        KeyspaceRepairPlan keyspaceRepairPlan = prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().get(0);

        List<String> tableNames = keyspaceRepairPlan.getTableNames();
        assertEquals(3, tableNames.size());
        assertEquals(table1, tableNames.get(0));
        assertEquals(table2, tableNames.get(1));
        assertEquals(table3, tableNames.get(2));
    }

    @Test
    public void testBuildWithMixedPriorities()
    {
        String ks1 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String table1 = createTable(ks1, "CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '2'}");
        String table2 = createTable(ks1, "CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '3'}");
        String table3 = createTable(ks1, "CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '2'}");
        String table4 = createTable(ks1, "CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '1'}");
        // No priority table should be bucketed at priority 0
        String table5 = createTable(ks1,"CREATE TABLE %s (k INT PRIMARY KEY, v INT)");

        // Create a new keyspace to ensure its tables get grouped with appropriate priority bucket
        String ks2 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String table6 = createTable(ks2,"CREATE TABLE %s (k INT PRIMARY KEY, v INT)");
        String table7 = createTable(ks2,"CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '1'}");

        Map<String, List<String>> keyspaceToTableMap = new HashMap<>();
        keyspaceToTableMap.put(ks1, Lists.newArrayList(table1, table2, table3, table4, table5));
        keyspaceToTableMap.put(ks2, Lists.newArrayList(table6, table7));

        // Expect 4 plans
        List<PrioritizedRepairPlan> prioritizedRepairPlans = PrioritizedRepairPlan.build(keyspaceToTableMap, AutoRepairConfig.RepairType.FULL, java.util.Collections::sort);
        assertEquals(4, prioritizedRepairPlans.size());

        // Verify the order is by descending priority and matches the expected tables
        assertEquals(3, prioritizedRepairPlans.get(0).getPriority());
        assertEquals(1, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().size());
        assertEquals(ks1, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().get(0).getKeyspaceName());
        assertEquals(table2, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().get(0).getTableNames().get(0));

        assertEquals(2, prioritizedRepairPlans.get(1).getPriority());
        assertEquals(1, prioritizedRepairPlans.get(1).getKeyspaceRepairPlans().size());

        assertEquals(ks1, prioritizedRepairPlans.get(1).getKeyspaceRepairPlans().get(0).getKeyspaceName());
        assertEquals(table1, prioritizedRepairPlans.get(1).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
        assertEquals(table3, prioritizedRepairPlans.get(1).getKeyspaceRepairPlans().get(0).getTableNames().get(1));

        assertEquals(1, prioritizedRepairPlans.get(2).getPriority());
        // 2 keyspaces should be present at priority 1
        assertEquals(2, prioritizedRepairPlans.get(2).getKeyspaceRepairPlans().size());
        // ks1.table4 expected in first plan
        assertEquals(ks1, prioritizedRepairPlans.get(2).getKeyspaceRepairPlans().get(0).getKeyspaceName());
        assertEquals(table4, prioritizedRepairPlans.get(2).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
        // ks2.table7 expected in second plan
        assertEquals(ks2, prioritizedRepairPlans.get(2).getKeyspaceRepairPlans().get(1).getKeyspaceName());
        assertEquals(table7, prioritizedRepairPlans.get(2).getKeyspaceRepairPlans().get(1).getTableNames().get(0));

        // Tables without priority should get bucketed at priority 0
        assertEquals(0, prioritizedRepairPlans.get(3).getPriority());
        // 2 keyspaces expected
        assertEquals(2, prioritizedRepairPlans.get(3).getKeyspaceRepairPlans().size());
        // ks1.table5 expected in first plan
        assertEquals(ks1, prioritizedRepairPlans.get(3).getKeyspaceRepairPlans().get(0).getKeyspaceName());
        assertEquals(table5, prioritizedRepairPlans.get(3).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
        // ks2.table6 expected in second plan
        assertEquals(ks2, prioritizedRepairPlans.get(3).getKeyspaceRepairPlans().get(1).getKeyspaceName());
        assertEquals(table6, prioritizedRepairPlans.get(3).getKeyspaceRepairPlans().get(1).getTableNames().get(0));
    }

    @Test
    public void testBuildWithEmptyTableList()
    {
        // Test with an empty table list (should remain empty)
        List<PrioritizedRepairPlan> prioritizedRepairPlans = PrioritizedRepairPlan.buildSingleKeyspacePlan(AutoRepairConfig.RepairType.FULL, KEYSPACE);
        assertTrue(prioritizedRepairPlans.isEmpty());
    }

    @Test
    public void testBuildWithOneTable()
    {
        // Test with a single element (should remain unchanged)
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH auto_repair = {'full_enabled': 'true', 'priority': '5'}");

        // Expect only 1 plans
        List<PrioritizedRepairPlan> prioritizedRepairPlans = PrioritizedRepairPlan.buildSingleKeyspacePlan(AutoRepairConfig.RepairType.FULL, KEYSPACE, table1);
        assertEquals(1, prioritizedRepairPlans.size());

        // Verify the order is by descending priority and matches the expected tables
        assertEquals(5, prioritizedRepairPlans.get(0).getPriority());
        assertEquals(table1, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
    }
}
