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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrioritizedRepairPlanTest extends CQLTester
{

    @Test
    public void testBuildWithDifferentPriorities()
    {
        // Test reordering assignments with different priorities
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '3'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '1'}");

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
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");

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
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '3'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table4 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '1'}");

        // Expect only 3 plans
        List<PrioritizedRepairPlan> prioritizedRepairPlans = PrioritizedRepairPlan.buildSingleKeyspacePlan(AutoRepairConfig.RepairType.FULL, KEYSPACE, table1, table2, table3, table4);
        assertEquals(3, prioritizedRepairPlans.size());

        // Verify the order is by descending priority and matches the expected tables
        assertEquals(3, prioritizedRepairPlans.get(0).getPriority());
        assertEquals(table2, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().get(0).getTableNames().get(0));

        assertEquals(2, prioritizedRepairPlans.get(1).getPriority());
        assertEquals(table1, prioritizedRepairPlans.get(1).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
        assertEquals(table3, prioritizedRepairPlans.get(1).getKeyspaceRepairPlans().get(0).getTableNames().get(1));

        assertEquals(1, prioritizedRepairPlans.get(2).getPriority());
        assertEquals(table4, prioritizedRepairPlans.get(2).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
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
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '5'}");

        // Expect only 1 plans
        List<PrioritizedRepairPlan> prioritizedRepairPlans = PrioritizedRepairPlan.buildSingleKeyspacePlan(AutoRepairConfig.RepairType.FULL, KEYSPACE, table1);
        assertEquals(1, prioritizedRepairPlans.size());

        // Verify the order is by descending priority and matches the expected tables
        assertEquals(5, prioritizedRepairPlans.get(0).getPriority());
        assertEquals(table1, prioritizedRepairPlans.get(0).getKeyspaceRepairPlans().get(0).getTableNames().get(0));
    }

}
