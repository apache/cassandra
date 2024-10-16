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

package org.apache.cassandra.service.consensus.migration;

public class ConsensusMigrationRepairType
{
    public static final ConsensusMigrationRepairType INELIGIBLE = new ConsensusMigrationRepairType(false, false ,false);

    public final boolean repairedData;
    public final boolean repairedPaxos;
    public final boolean repairedAccord;

    public ConsensusMigrationRepairType(boolean repairedData, boolean repairedPaxos, boolean repairedAccord)
    {
        this.repairedData = repairedData;
        this.repairedPaxos = repairedPaxos;
        this.repairedAccord = repairedAccord;
    }

    public boolean migrationToAccordEligible()
    {
        return repairedData;
    }

    public boolean migrationToPaxosEligible()
    {
        return repairedAccord;
    }

    // Require both data and Paxos repair since Paxos only repairs to QUORUM and Accord needs ALL
    public boolean repairsPaxos()
    {
        return repairedData && repairedPaxos;
    }

    public boolean ineligibleForMigration()
    {
        return !migrationToAccordEligible() && !migrationToPaxosEligible();
    }

    @Override
    public String toString()
    {
        return "ConsensusMigrationRepairType{" +
               "repairedData=" + repairedData +
               ", repairedPaxos=" + repairedPaxos +
               ", repairedAccord=" + repairedAccord +
               '}';
    }
}
