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

/**
 * A grouping of repair assignments that were generated for a particular keyspace for a given priority.
 */
public class KeyspaceRepairAssignments
{
    private final int priority;
    private final String keyspaceName;
    private final List<RepairAssignment> repairAssignments;

    public KeyspaceRepairAssignments(int priority, String keyspaceName, List<RepairAssignment> repairAssignments)
    {
        this.priority = priority;
        this.keyspaceName = keyspaceName;
        this.repairAssignments = repairAssignments;
    }

    public int getPriority()
    {
        return priority;
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public List<RepairAssignment> getRepairAssignments()
    {
        return repairAssignments;
    }
}
