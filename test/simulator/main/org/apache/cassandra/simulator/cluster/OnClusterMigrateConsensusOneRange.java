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

import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;

class OnClusterMigrateConsensusOneRange extends Action
{
    private final KeyspaceActions actions;
    private final int repairOn;
    Map.Entry<String, String> startMigrationRange;

    OnClusterMigrateConsensusOneRange(KeyspaceActions actions, int repairOn, Map.Entry<String, String> startMigrationRange)
    {
        super("Performing consensus migration one range " + startMigrationRange, STRICT, NONE);
        this.actions = actions;
        this.repairOn = repairOn;
        this.startMigrationRange = startMigrationRange;
    }

    public ActionList performSimple()
    {
        return ActionList.of(new OnInstanceStartConsensusMigration(actions, 1, startMigrationRange ),
                             new OnClusterRepairRanges(actions, new int[] { repairOn }, true, false, ImmutableList.of(startMigrationRange)));
    }
}
