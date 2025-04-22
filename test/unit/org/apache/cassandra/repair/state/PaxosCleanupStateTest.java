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

package org.apache.cassandra.repair.state;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.AutoRepairService;

import static org.apache.cassandra.repair.RepairParallelism.PARALLEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PaxosCleanupStateTest
{
    @Before
    public void setup()
    {
        ServerTestUtils.daemonInitialization();
        AutoRepairService.setup();
    }

    @Test
    public void testPaxosCleanupOptions()
    {
        AutoRepairState state = AutoRepairStateFactory.getAutoRepairState(AutoRepairConfig.RepairType.paxos_cleanup);
        assert state instanceof PaxosCleanupState;
        RepairRunnable runnable = state.getRepairRunnable("test", ImmutableList.of("test"), ImmutableSet.of(), true, null);
        RepairOption option = runnable.state.options;
        assertEquals(PARALLEL, option.getParallelism());
        assertTrue(option.isPrimaryRange());
        assertFalse(option.isIncremental());
        assertFalse(option.isTraced());
        assertEquals(1, option.getJobThreads());
        assertEquals(ImmutableSet.of(), option.getRanges());
        assertFalse(option.isSubrangeRepair());
        assertFalse(option.isPullRepair());
        assertFalse(option.isForcedRepair());
        assertFalse(option.isPreview());
        assertFalse(option.optimiseStreams());
        assertTrue(option.ignoreUnreplicatedKeyspaces());
        // here repair-paxos option will be ignored, as we set paxos-only
        assertFalse(option.repairPaxos());
        assertTrue(option.paxosOnly());
    }
}
