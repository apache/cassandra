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

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.concurrent.Future;

public class NormalRepairTask extends AbstractRepairTask
{
    protected NormalRepairTask(RepairCoordinator coordinator)
    {
        super(coordinator);
    }

    @Override
    public String name()
    {
        return "Repair";
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor)
    {
        return runRepair(coordinator.state.id, false, executor, commonRanges, coordinator.neighborsAndRanges.excludedDeadParticipants, coordinator.getColumnFamilyNames().toArray(new String[0]));
    }
}
