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

import java.util.List;
import java.util.UUID;

import org.apache.cassandra.repair.messages.RepairOption;

/**
 * Keeps track of the state for a single repair.
 *
 * This class is responsible for all sub-tasks (session, job, validation, sync, etc.) so must have a accurate
 * view of the state.
 *
 * The definition of success and progress are relative to sub-tasks, so a repair's progress is based off sessions which
 * are based off jobs which are based off validations/sync.  Since some tasks are remote (validation, sync, etc.) the
 * state of these tasks are expected to be stale and require refresh from time to time.
 *
 * This class is expected to be seperate from {@link RepairRunnable}, {@link RepairSession}, and {@link RepairJob} to
 * make sure any and all resources (objects, threads, etc.) can be GCed once the repair is complete. The state
 * should survive longer so it can be exposed to operators (via virtual tables and JMX).
 *
 * Repair has some special cases to worry about when it comes to state mangment; this mostly comes from the fact that
 * each repair ({@link RepairRunnable}) and tasks are actors which can fail on their own.  For this reason its possible
 * to have a repair succeed or fail without any child tasks; same is true for {@link RepairSession} as well.
 */
public class RepairState
{
    public enum Phase {
        INIT,
        STARTING, STARTED,
        PREPARE_SUBMIT, PREPARE_COMPLETE,
        SESSIONS_SUBMIT, SESSIONS_COMPLETE,
        SKIPPED, COMPLETE, FAILURE // represents RepairRunnable early termination
    }
    // required on object init
    UUID id;
    int cmd; // managed by org.apache.cassandra.service.ActiveRepairService.repairStatusByCmd.  TODO can this be removed in favor of this class?
    String keyspace;
    RepairOption options;

    // calculated once repair is in phase STARTED
    List<CommonRange> commonRanges; // each CommonRange will spawn a new RepairSession
    String[] cfnames; // TODO when can this be different than org.apache.cassandra.repair.messages.RepairOption.columnFamilies?  Is it only in the empty case?
}
