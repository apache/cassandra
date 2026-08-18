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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.db.rows.Cell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeletionTimeTest
{
    /**
     * NO_DELETION_TIME (Long.MAX_VALUE) is the canonical "no deletion" long value and must
     * round-trip through the long-based reset to a LIVE deletion time, mirroring
     * {@link Cell#deletionTimeLongToUnsignedInteger}. It used to be classified as invalid,
     * so {@code reset(LIVE.markedForDeleteAt(), LIVE.localDeletionTime())} produced a
     * NON-live deletion. Latent hardening rather than an observed bug: the only production
     * caller of this overload is {@code SerializationHeader.readDeletionTime}, which passes a
     * vint delta read from the data file rebased on the header's minLocalDeletionTime — a value
     * that cannot reach Long.MAX_VALUE. This pins the sentinel so a future caller resetting from
     * LIVE — the obvious use of the overload — cannot silently produce a non-live marker.
     */
    @Test
    public void resetWithLiveLongsStaysLive()
    {
        DeletionTime.ReusableDeletionTime reusable = DeletionTime.ReusableDeletionTime.live();
        assertTrue(reusable.isLive());

        reusable.reset(DeletionTime.LIVE.markedForDeleteAt(), DeletionTime.LIVE.localDeletionTime());
        assertTrue("reset with LIVE's long values must stay live, got mfda=" + reusable.markedForDeleteAt() +
                   " ldt=" + reusable.localDeletionTime(), reusable.isLive());
        assertEquals(DeletionTime.LIVE.localDeletionTime(), reusable.localDeletionTime());
    }

    @Test
    public void resetWithRealAndInvalidValues()
    {
        DeletionTime.ReusableDeletionTime reusable = DeletionTime.ReusableDeletionTime.live();

        reusable.reset(123456789L, 1_700_000_000L);
        assertFalse(reusable.isLive());
        assertEquals(123456789L, reusable.markedForDeleteAt());
        assertEquals(1_700_000_000L, reusable.localDeletionTime());

        // negative and beyond-max (but not NO_DELETION_TIME) stay classified as invalid
        reusable.reset(1L, -5L);
        assertFalse(reusable.isLive());
        assertFalse(reusable.validate());

        reusable.reset(1L, Cell.MAX_DELETION_TIME + 1);
        assertFalse(reusable.isLive());
        assertFalse(reusable.validate());
    }

    /**
     * A LIVE deletion shadows nothing, at any timestamp. {@link DeletionTime#deletes(long)} cannot say so
     * on its own: LIVE's {@code markedForDeleteAt} is Long.MIN_VALUE, which is also
     * {@link LivenessInfo#NO_TIMESTAMP}, so {@code timestamp <= markedForDeleteAt} is TRUE for a cell
     * carrying that timestamp. {@code deletesCellAt} tests the LIVE case first, as
     * {@link DeletionTime#deletes(Cell)} already did.
     *
     * The row-liveness overload {@link DeletionTime#deletes(LivenessInfo)} deliberately keeps the
     * unguarded answer, so it is pinned here too. The reason is parity, not a caller's dependency: the
     * iterator path applies that same unguarded form to row liveness ({@code Row.Merger.merge},
     * {@code BTreeRow}), so guarding it would move the reference as well as the cursor. It is also
     * behaviour-neutral where the cursor reads it — an empty row liveness has nothing to shadow — which is
     * why the guard belongs to the cell contract alone.
     */
    @Test
    public void liveDeletionShadowsNoCellAtAnyTimestamp()
    {
        assertFalse("LIVE must not shadow a NO_TIMESTAMP cell",
                    DeletionTime.LIVE.deletesCellAt(LivenessInfo.NO_TIMESTAMP));
        assertFalse(DeletionTime.LIVE.deletesCellAt(Long.MIN_VALUE + 1));
        assertFalse(DeletionTime.LIVE.deletesCellAt(0L));
        assertFalse(DeletionTime.LIVE.deletesCellAt(Long.MAX_VALUE));

        // both unguarded forms answer the other way on that one input, which is why the cell path cannot
        // use either. NO_TIMESTAMP is a long, so the second line binds deletes(long), not the
        // LivenessInfo overload — the two are pinned separately on purpose.
        assertTrue(DeletionTime.LIVE.deletes(LivenessInfo.EMPTY));
        assertTrue(DeletionTime.LIVE.deletes(LivenessInfo.NO_TIMESTAMP));

        // a real deletion is unaffected: at, below and above its markedForDeleteAt
        DeletionTime at100 = DeletionTime.build(100L, 1_700_000_000L);
        assertTrue(at100.deletesCellAt(99L));
        assertTrue(at100.deletesCellAt(100L));
        assertFalse(at100.deletesCellAt(101L));
        assertTrue("a real deletion shadows a NO_TIMESTAMP cell, as the unguarded form does",
                   at100.deletesCellAt(LivenessInfo.NO_TIMESTAMP));
    }
}
