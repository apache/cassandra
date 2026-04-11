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

package org.apache.cassandra.simulator.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationTarget;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.cluster.OnInstanceRepair.RepairType;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;

class OnClusterMigrateConsensusOneRange extends Action
{
    private final KeyspaceActions actions;
    private final int repairOn;
    private final Map.Entry<String, String> startMigrationRange;
    private final TransactionalMode targetMode;

    OnClusterMigrateConsensusOneRange(KeyspaceActions actions, int repairOn, Map.Entry<String, String> startMigrationRange, TransactionalMode targetMode)
    {
        super("Performing consensus migration of range " + startMigrationRange + " to " + targetMode, NONE, NONE);
        this.actions = actions;
        this.repairOn = repairOn;
        this.startMigrationRange = startMigrationRange;
        this.targetMode = targetMode;
    }

    public ActionList performSimple()
    {
        List<Action> result = new ArrayList<>();
        result.add(new OnInstanceStartConsensusMigration(actions, repairOn, startMigrationRange));
        boolean migrateViaRepair = actions.random.decide(0.5f);
        String rangeString = startMigrationRange.getKey() + ":" + startMigrationRange.getValue();
        String targetString = targetMode.accordIsEnabled ? ConsensusMigrationTarget.accord.toString() : ConsensusMigrationTarget.paxos.toString();
        if (targetMode == TransactionalMode.off)
        {
            if (migrateViaRepair)
                result.add(new OnInstanceRepair(actions, repairOn, RepairType.ACCORD_ONLY, startMigrationRange, false));
            else
                result.add(new OnInstanceFinishConsensusMigration(actions, repairOn, rangeString, targetString));
        }
        else
        {
            if (migrateViaRepair)
            {
                result.add(new OnInstanceRepair(actions, repairOn, RepairType.DATA_INCREMENTAL, startMigrationRange, false));
                result.add(new OnInstanceRepair(actions, repairOn, RepairType.DATA_AND_PAXOS_FULL, startMigrationRange, false));
            }
            else
            {
                result.add(new OnInstanceFinishConsensusMigration(actions, repairOn, rangeString, targetString));
                result.add(new OnInstanceFinishConsensusMigration(actions, repairOn, rangeString, targetString));
            }
        }

        return ActionList.of(result).setStrictlySequential();
    }
}
