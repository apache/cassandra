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

package org.apache.cassandra.db.compaction;

import org.junit.Test;

import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompactionGroupTest
{
    private static final long REPAIRED_AT = 1234L;
    private static final long UNREPAIRED = ActiveRepairService.UNREPAIRED_SSTABLE;
    private static final TimeUUID NO_PENDING = ActiveRepairService.NO_PENDING_REPAIR;

    private static ImmutableCoordinatorLogOffsets offsets(long logId, int offset)
    {
        return new ImmutableCoordinatorLogOffsets.Builder().add(new MutationId(logId, (long) offset)).build();
    }

    private static ImmutableCoordinatorLogOffsets noOffsets()
    {
        return new ImmutableCoordinatorLogOffsets.Builder().build();
    }

    /**
     * Repaired wins over offsets. Promotion clears the offsets in the same metadata mutation that sets {@code repairedAt},
     * so this should be unreachable, but we still don't want repaired and unreconciled data being compacted if that's not
     * working properly
     */
    @Test
    public void repairedOutranksOffsets()
    {
        assertEquals(CompactionGroup.REPAIRED, CompactionGroup.of(REPAIRED_AT, NO_PENDING, noOffsets()));
        assertEquals(CompactionGroup.REPAIRED, CompactionGroup.of(REPAIRED_AT, NO_PENDING, offsets(1, 0)));
    }

    /**
     * Pending repair outranks offsets too, so an sstable in a session is not routed to a tracked silo.
     */
    @Test
    public void pendingRepairOutranksOffsets()
    {
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        assertEquals(CompactionGroup.PENDING_REPAIR, CompactionGroup.of(UNREPAIRED, session, noOffsets()));
        assertEquals(CompactionGroup.PENDING_REPAIR, CompactionGroup.of(UNREPAIRED, session, offsets(1, 0)));
    }

    /**
     * Any offsets at all mean tracked data awaiting reconciliation. Transfers and mutations are not told apart here.
     */
    @Test
    public void offsetsOnUnrepairedDataMeanUnreconciled()
    {
        assertEquals(CompactionGroup.UNRECONCILED, CompactionGroup.of(UNREPAIRED, NO_PENDING, offsets(1, 0)));
    }

    /**
     * No offsets on unrepaired data is untracked. A null offsets object reads the same way, which streaming relies on.
     */
    @Test
    public void noOffsetsOnUnrepairedDataMeansUntracked()
    {
        assertEquals(CompactionGroup.UNREPAIRED, CompactionGroup.of(UNREPAIRED, NO_PENDING, noOffsets()));
        assertEquals(CompactionGroup.UNREPAIRED, CompactionGroup.of(UNREPAIRED, NO_PENDING, null));
    }

    /**
     * repairedAt and pendingRepair can't both be set
     */
    @Test
    public void repairedAndPendingIsRejected()
    {
        try
        {
            CompactionGroup.of(REPAIRED_AT, TimeUUID.Generator.nextTimeUUID(), null);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }
}
