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

package org.apache.cassandra.db.rows;

/**
 * The liveness contract of a single cell, as defined by {@link Cell} and {@link Cells} — distinct from
 * {@link org.apache.cassandra.db.LivenessInfo}, which is a row's primary-key liveness. The two disagree on
 * enough of their shared vocabulary that holding both in one type leaves half of it valid in only one role:
 * a cell's expiration field is a local deletion time, which a tombstone carries with no TTL, and a cell has
 * no notion of being empty.
 *
 * It also houses the same-timestamp reconciliation tie-break, which two independent decision tables
 * previously carried by hand.
 */
public interface CellLivenessInfo
{
    /** @see Cell#timestamp() */
    long timestamp();

    /** {@code Cell.NO_TTL} unless this is an expiring cell. @see Cell#ttl() */
    int ttl();

    /**
     * {@code Cell.NO_DELETION_TIME} unless this is a tombstone or an expiring cell. This is the member the
     * two contracts differ on most sharply: the row contract's equivalent field is a TTL expiration time and
     * exists only alongside a TTL, whereas a cell tombstone carries a deletion time with no TTL.
     *
     * @see Cell#localDeletionTime()
     */
    long localDeletionTime();

    /** "Has a TTL", not "has an expiration time" — a tombstone has the latter. @see Cell#isExpiring() */
    boolean isExpiring();

    /** @see Cell#isTombstone() */
    boolean isTombstone();

    /** @see Cell#isLive(long) */
    boolean isLive(long nowInSec);

    /**
     * The cell liveness rule over primitives: a cell with no deletion time is live, otherwise it must carry a
     * TTL that has not lapsed. Held here so {@link Cell} and {@link ReusableCellLivenessInfo} cannot drift,
     * and reachable without an instance for callers that already hold the three fields.
     */
    static boolean isLive(long nowInSec, long localDeletionTime, int ttl)
    {
        return localDeletionTime == Cell.NO_DELETION_TIME || ttl != Cell.NO_TTL && nowInSec < localDeletionTime;
    }

    /**
     * Which side of a same-timestamp reconciliation wins, or that the decision falls through to the
     * callers' own value comparison. Callers differ in how they compare values — the reference consults
     * the deserialized cell values, the cursor compares raw wire bytes — so only the decision is shared.
     */
    enum Resolution
    {
        LEFT, RIGHT, COMPARE
    }

    /**
     * The whole non-counter cell reconciliation decision: the newer timestamp outright, else the
     * same-timestamp tie-break below. Reached by {@code Cells.resolveRegular} on the read path and by
     * {@code CursorCompactor.mergeCells} on the cursor compaction path.
     *
     * {@code ttl()} is read only inside the tie-break, so the ordinary live-versus-live tie costs the two
     * {@code localDeletionTime()} reads it always cost and no more.
     *
     * <b>Callers holding a cell must narrow their arguments before calling.</b> The two callers hold
     * unrelated types calling through this one interface method, which shares one receiver-type profile
     * across both — see {@code Cells.resolveRegular} for why that pollutes without narrowing.
     */
    static Resolution resolve(CellLivenessInfo left, CellLivenessInfo right)
    {
        long leftTimestamp = left.timestamp();
        long rightTimestamp = right.timestamp();
        if (leftTimestamp != rightTimestamp)
            return leftTimestamp > rightTimestamp ? Resolution.LEFT : Resolution.RIGHT;

        long leftLocalDeletionTime = left.localDeletionTime();
        long rightLocalDeletionTime = right.localDeletionTime();
        if (leftLocalDeletionTime == Cell.NO_DELETION_TIME && rightLocalDeletionTime == Cell.NO_DELETION_TIME)
            return Resolution.COMPARE;

        return resolveSameTimestampTie(left.ttl(), leftLocalDeletionTime, right.ttl(), rightLocalDeletionTime);
    }

    /**
     * The same-timestamp tie-break, reached from {@link #resolve} once at least one side carries a deletion
     * time. Primitive and timestamp-free, so it stays callable by anything already holding the four fields.
     *
     * The precondition is asserted rather than assumed, so a future caller that reaches it directly without
     * establishing the guard fails loudly instead of receiving a plausible COMPARE.
     *
     * @param leftTtl                {@code Cell.NO_TTL} for a tombstone or a live cell
     * @param leftLocalDeletionTime  {@code Cell.NO_DELETION_TIME} for a live cell
     */
    static Resolution resolveSameTimestampTie(int leftTtl, long leftLocalDeletionTime,
                                               int rightTtl, long rightLocalDeletionTime)
    {
        boolean leftIsExpiringOrTombstone = leftLocalDeletionTime != Cell.NO_DELETION_TIME;
        boolean rightIsExpiringOrTombstone = rightLocalDeletionTime != Cell.NO_DELETION_TIME;
        assert leftIsExpiringOrTombstone || rightIsExpiringOrTombstone
             : "neither side carries a deletion time; the deletion-time guard was skipped";

        // Tombstones always win reconciliation with live cells of the same timstamp
        // CASSANDRA-14592: for consistency of reconciliation, regardless of system clock at time of reconciliation
        // this requires us to treat expiring cells (which will become tombstones at some future date) the same wrt regular cells
        if (leftIsExpiringOrTombstone != rightIsExpiringOrTombstone)
            return leftIsExpiringOrTombstone ? Resolution.LEFT : Resolution.RIGHT;

        // for most historical consistency, we still prefer tombstones over expiring cells.
        // While this leads to an inconsistency over which is chosen
        // (i.e. before expiry, the pure tombstone; after expiry, whichever is more recent)
        // this inconsistency has no user-visible distinction, as at this point they are both logically tombstones
        // (the only possible difference is the time at which the cells become purgeable)
        //
        // Both sides carry a deletion time here, so "no TTL" is exactly "is a tombstone" — the primitive form
        // of the reference's !isExpiring(), which does not need to consider the deletion time either.
        boolean leftIsTombstone = leftTtl == Cell.NO_TTL;
        boolean rightIsTombstone = rightTtl == Cell.NO_TTL;
        if (leftIsTombstone != rightIsTombstone)
            return leftIsTombstone ? Resolution.LEFT : Resolution.RIGHT;

        // ==> (leftIsExpiring && rightIsExpiring) or (leftIsTombstone && rightIsTombstone)
        // if both are expiring, we do not want to consult the value bytes if we can avoid it, as like with C-14592
        // the value bytes implicitly depend on the system time at reconciliation, as a
        // would otherwise always win (unless it had an empty value), until it expired and was translated to a tombstone
        if (leftLocalDeletionTime != rightLocalDeletionTime)
            return leftLocalDeletionTime > rightLocalDeletionTime ? Resolution.LEFT : Resolution.RIGHT;

        // Both cells are either tombstones or expiring at the same timestamp. If expiring and the
        // TTLs differ, write the lower one -- the write is probably from a more recent
        // UPDATE USING TTL AND TIMESTAMP, so select the most recent one to be deterministic and be
        // closest to client intent.
        if (!leftIsTombstone && leftTtl != rightTtl)
        {
            assert !rightIsTombstone;
            return leftTtl < rightTtl ? Resolution.LEFT : Resolution.RIGHT;
        }

        return Resolution.COMPARE;
    }
}
