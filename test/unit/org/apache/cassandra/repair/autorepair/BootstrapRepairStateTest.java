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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.AutoRepairService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BootstrapRepairStateTest extends CQLTester
{
    @Test
    public void testBootstrapRepairOptionsAllRanges() {
        AutoRepairState state = RepairType.getAutoRepairState(RepairType.BOOTSTRAP);
        assert state instanceof BootstrapRepairState;
        AutoRepairService.setup();
        RepairRunnable runnable = state.getRepairRunnable(KEYSPACE, ImmutableList.of("test"), ImmutableSet.of(), false);
        RepairOption options = runnable.state.options;
        assertEquals(RepairParallelism.PARALLEL, options.getParallelism());
        assertFalse(options.isPrimaryRange());
        assertFalse(options.isTraced());
        assertFalse(options.isIncremental());
        assertEquals(1,options.getJobThreads());
        assertEquals(ImmutableSet.of(), options.getRanges());
        assertFalse(options.isPreview());
        assertTrue(options.isForcedRepair());
        assertTrue(options.ignoreUnreplicatedKeyspaces());
        assertFalse(options.optimiseStreams());
        assertFalse(options.repairPaxos());
        assertFalse(options.paxosOnly());
    }

    @Test
    public void testBootstrapRepairOptionsPrimaryRangesOnly() {
        AutoRepairState state = RepairType.getAutoRepairState(RepairType.BOOTSTRAP);
        assert state instanceof BootstrapRepairState;
        AutoRepairService.setup();
        RepairRunnable runnable = state.getRepairRunnable(KEYSPACE, ImmutableList.of("test"), ImmutableSet.of(), true);
        RepairOption options = runnable.state.options;
        assertEquals(RepairParallelism.PARALLEL, options.getParallelism());
        assertTrue(options.isPrimaryRange());
        assertFalse(options.isTraced());
        assertFalse(options.isIncremental());
        assertEquals(1,options.getJobThreads());
        assertEquals(ImmutableSet.of(), options.getRanges());
        assertFalse(options.isPreview());
        assertTrue(options.isForcedRepair());
        assertTrue(options.ignoreUnreplicatedKeyspaces());
        assertFalse(options.optimiseStreams());
        assertFalse(options.repairPaxos());
        assertFalse(options.paxosOnly());
    }

    @Test
    public void testCalculatingTurn() {
        assertEquals(AutoRepairUtils.RepairTurn.NOT_MY_TURN, RepairType.getAutoRepairState(RepairType.BOOTSTRAP).calcRepairTurn(null));
    }
}
