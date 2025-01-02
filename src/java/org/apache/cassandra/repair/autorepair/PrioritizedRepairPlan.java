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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Encapsulates a devised plan to repair tables, grouped by their keyspace and a given priority.  This is used
 * by {@link AutoRepair} to pass in an organized plan to
 * {@link IAutoRepairTokenRangeSplitter#getRepairAssignments(boolean, List)} which
 * can iterate over this plan in order to generate {@link RepairAssignment}s.
 */
public class PrioritizedRepairPlan
{
    private final int priority;

    private final List<KeyspaceRepairPlan> keyspaceRepairPlans;

    public PrioritizedRepairPlan(int priority, List<KeyspaceRepairPlan> keyspaceRepairPlans)
    {
        this.priority = priority;
        this.keyspaceRepairPlans = keyspaceRepairPlans;
    }

    public int getPriority()
    {
        return priority;
    }

    public List<KeyspaceRepairPlan> getKeyspaceRepairPlans()
    {
        return keyspaceRepairPlans;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        PrioritizedRepairPlan that = (PrioritizedRepairPlan) o;
        return priority == that.priority && Objects.equals(keyspaceRepairPlans, that.keyspaceRepairPlans);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(priority, keyspaceRepairPlans);
    }

    @Override
    public String toString()
    {
        return "PrioritizedRepairPlan{" +
               "priority=" + priority +
               ", keyspaceRepairPlans=" + keyspaceRepairPlans +
               '}';
    }

    /**
     * Builds a list of {@link PrioritizedRepairPlan}s for the given keyspace and table map, ordered by priority from
     * highest to lowest, where priority is derived from table schema's defined priority for the given repair type.
     * <p>
     * If a keyspace has tables with differing priorities, those tables will be included in the PrioritizedRepairPlan
     * for their given priority.
     *
     * @param keyspacesToTableNames A mapping keyspace to table names
     * @param repairType The repair type that is being executed
     * @param orderFunc A function to order keyspace and tables in the returned plan.
     * @return Ordered list of plan's by table priorities.
     */
    public static List<PrioritizedRepairPlan> build(Map<String, List<String>> keyspacesToTableNames, AutoRepairConfig.RepairType repairType, Consumer<List<String>> orderFunc)
    {
        // Build a map of priority -> (keyspace -> tables)
        Map<Integer, Map<String, List<String>>> plans = new HashMap<>();
        for (Map.Entry<String, List<String>> keyspaceToTableNames : keyspacesToTableNames.entrySet())
        {
            String keyspaceName = keyspaceToTableNames.getKey();
            for (String tableName : keyspaceToTableNames.getValue())
            {
                int priority = getPriority(repairType, keyspaceName, tableName);
                Map<String, List<String>> keyspacesForPriority = plans.computeIfAbsent(priority, p -> new HashMap<>());
                List<String> tableNamesAtPriority = keyspacesForPriority.computeIfAbsent(keyspaceName, k -> new ArrayList<>());
                tableNamesAtPriority.add(tableName);
            }
        }

        // Extract map into a List<PrioritizedRepairPlan> ordered by priority from highest to lowest.
        List<PrioritizedRepairPlan> planList = new ArrayList<>(plans.size());
        TreeSet<Integer> priorities = new TreeSet<>(Comparator.reverseOrder());
        priorities.addAll(plans.keySet());
        for (int priority : priorities)
        {
            Map<String, List<String>> keyspacesAndTables = plans.get(priority);
            List<KeyspaceRepairPlan> keyspaceRepairPlans = new ArrayList<>(keyspacesAndTables.size());
            planList.add(new PrioritizedRepairPlan(priority, keyspaceRepairPlans));

            // Order keyspace and table names based on the input function (typically, this would shuffle the keyspace
            // and table names randomly).
            List<String> keyspaceNames = new ArrayList<>(keyspacesAndTables.keySet());
            orderFunc.accept(keyspaceNames);

            for(String keyspaceName : keyspaceNames)
            {
               List<String> tableNames = keyspacesAndTables.get(keyspaceName);
               orderFunc.accept(tableNames);
               KeyspaceRepairPlan keyspaceRepairPlan = new KeyspaceRepairPlan(keyspaceName, new ArrayList<>(tableNames));
               keyspaceRepairPlans.add(keyspaceRepairPlan);
            }
        }

        return planList;
    }

    /**
     * Convenience method to build a repair plan for a single keyspace with tables. Primarily useful in testing.
     * @param keyspaceName Keyspace to repair
     * @param tableNames tables to repair for the given keyspace.
     * @return Single repair plan.
     */
    static List<PrioritizedRepairPlan> buildSingleKeyspacePlan(AutoRepairConfig.RepairType repairType, String keyspaceName, String ... tableNames)
    {
        Map<String, List<String>> keyspaceMap = new HashMap<>();
        keyspaceMap.put(keyspaceName, Arrays.asList(tableNames));
        return build(keyspaceMap, repairType, (l) -> {});
    }

    /**
     * @return The priority of the given table if defined, otherwise 0.
     */
    private static int getPriority(AutoRepairConfig.RepairType repairType, String keyspaceName, String tableName)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspaceName, tableName);
        return cfs != null ? cfs.metadata().params.autoRepair.priority() : 0;
    }
}
