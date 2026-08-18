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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link ReusableLivenessInfo} carries row primary-key liveness only; the cell role has its own type and its
 * own test. Every assertion here is against {@link LivenessInfo#withExpirationTime}, the reference that
 * decides which of the three row implementations a given (ttl, expiration time) is, so the mutable holder
 * cannot drift from any of them.
 *
 * {@code isLive} in particular follows the row contract, which disagrees with the cell one on three inputs;
 * {@code ReusableCellLivenessInfoTest} covers that side.
 */
public class ReusableLivenessInfoTest
{
    private static final long TIMESTAMP = 1000L;
    private static final long NOW_IN_SEC = 1_700_000_000L;

    /** Every (ttl, expiration time) shape the row references distinguish, including the view-maintenance
     * {@code EXPIRED_LIVENESS_TTL} marker, and an expiration time before, at and after now. */
    private static int[] ttls()
    {
        return new int[]{ LivenessInfo.NO_TTL, 1, 100, LivenessInfo.EXPIRED_LIVENESS_TTL };
    }

    private static long[] expirationTimes()
    {
        return new long[]{ LivenessInfo.NO_EXPIRATION_TIME, NOW_IN_SEC - 10, NOW_IN_SEC, NOW_IN_SEC + 10 };
    }

    /**
     * A liveness carrying an expiration time with no TTL is not a well-formed ROW liveness — the reader only
     * reads an expiration time inside {@code hasTTL(flags)} — and it is the one shape where this holder cannot
     * mirror the reference, because {@link LivenessInfo#withExpirationTime} DISCARDS the expiration time when
     * {@code ttl == NO_TTL} while a raw mutable holder keeps what it was given.
     */
    private static boolean referenceNormalisesAway(int ttl, long expirationTime)
    {
        return ttl == LivenessInfo.NO_TTL && expirationTime != LivenessInfo.NO_EXPIRATION_TIME;
    }

    /**
     * The mirror shape: a TTL with no expiration time. The reference cannot hold it —
     * {@code ExpiringLivenessInfo}'s constructor asserts {@code ttl != NO_TTL && localExpirationTime !=
     * NO_EXPIRATION_TIME} — so these shapes are skipped rather than asserted, with nothing to compare against.
     */
    private static boolean referenceRejects(int ttl, long expirationTime)
    {
        return ttl != LivenessInfo.NO_TTL && expirationTime == LivenessInfo.NO_EXPIRATION_TIME;
    }

    private static ReusableLivenessInfo reusable(long timestamp, int ttl, long expirationTime)
    {
        ReusableLivenessInfo liveness = new ReusableLivenessInfo();
        liveness.reset(timestamp, ttl, expirationTime);
        return liveness;
    }

    /**
     * For row liveness the reference is {@link LivenessInfo#withExpirationTime}, which is expiring precisely
     * when it has a TTL and discards the expiration time otherwise.
     */
    @Test
    public void predicatesAgreeWithRowReference()
    {
        int asserted = 0;
        int skipped = 0;
        for (int ttl : ttls())
        {
            for (long ldt : expirationTimes())
            {
                if (referenceRejects(ttl, ldt))
                {
                    skipped++;
                    continue;
                }
                ReusableLivenessInfo liveness = reusable(TIMESTAMP, ttl, ldt);
                LivenessInfo reference = LivenessInfo.withExpirationTime(TIMESTAMP, ttl, ldt);
                String at = " at ttl=" + ttl + " localExpirationTime=" + ldt;

                assertEquals("isExpiring must match LivenessInfo.isExpiring()" + at,
                             reference.isExpiring(), liveness.isExpiring());
                assertEquals("isExpired must match LivenessInfo.isExpired()" + at,
                             reference.isExpired(), liveness.isExpired());
                assertEquals("isEmpty must match LivenessInfo.isEmpty()" + at,
                             reference.isEmpty(), liveness.isEmpty());
                assertEquals("isLive must match LivenessInfo.isLive()" + at,
                             reference.isLive(NOW_IN_SEC), liveness.isLive(NOW_IN_SEC));
                asserted++;
            }
        }
        assertEquals("the asserted set changed size", 13, asserted);
        assertEquals("the reference-rejected set changed size", 3, skipped);
    }

    /**
     * Empty liveness is the input the cell reading got wrong, so it is pinned on its own: the row references
     * treat it as not live ({@code ImmutableLivenessInfo.isLive} is {@code !isEmpty()}), where the cell rule
     * reports live because there is no deletion time.
     */
    @Test
    public void emptyLivenessIsNotLive()
    {
        ReusableLivenessInfo fresh = new ReusableLivenessInfo();
        assertTrue(fresh.isEmpty());
        assertFalse(fresh.isExpiring());
        assertFalse(fresh.isExpired());
        assertEquals(LivenessInfo.EMPTY.isLive(NOW_IN_SEC), fresh.isLive(NOW_IN_SEC));
        assertFalse("empty row liveness is not live", fresh.isLive(NOW_IN_SEC));

        // and resetting to the empty sentinel gets there too, not just a fresh instance
        assertFalse(reusable(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_TTL, LivenessInfo.NO_EXPIRATION_TIME)
                    .isLive(NOW_IN_SEC));
    }

    /**
     * {@code supersedes} is the row-side analogue of cell reconciliation, is load-bearing in the cursor's row
     * merge, and is inherited default behaviour on a mutable holder — so it is asserted against the reference
     * over the whole shape cross product rather than left to the default's own tests.
     *
     * Pairs where either side is a shape the reference {@link #referenceRejects} or
     * {@link #referenceNormalisesAway} are counted, not asserted, since the reference can't hold the same
     * input; both counts are themselves asserted so neither set can change size unnoticed.
     */
    @Test
    public void supersedesAgreesWithRowReference()
    {
        long[] timestamps = { TIMESTAMP, TIMESTAMP + 1 };
        int asserted = 0;
        int normalised = 0;
        for (long leftTs : timestamps)
            for (int leftTtl : ttls())
                for (long leftLdt : expirationTimes())
                    for (long rightTs : timestamps)
                        for (int rightTtl : ttls())
                            for (long rightLdt : expirationTimes())
                            {
                                if (referenceRejects(leftTtl, leftLdt) || referenceRejects(rightTtl, rightLdt)
                                    || referenceNormalisesAway(leftTtl, leftLdt) || referenceNormalisesAway(rightTtl, rightLdt))
                                {
                                    normalised++;
                                    continue;
                                }
                                LivenessInfo leftRef = LivenessInfo.withExpirationTime(leftTs, leftTtl, leftLdt);
                                LivenessInfo rightRef = LivenessInfo.withExpirationTime(rightTs, rightTtl, rightLdt);
                                ReusableLivenessInfo left = reusable(leftTs, leftTtl, leftLdt);
                                ReusableLivenessInfo right = reusable(rightTs, rightTtl, rightLdt);

                                assertEquals("supersedes must match the reference for left(ts=" + leftTs
                                             + " ttl=" + leftTtl + " let=" + leftLdt + ") right(ts=" + rightTs
                                             + " ttl=" + rightTtl + " let=" + rightLdt + ')',
                                             leftRef.supersedes(rightRef), left.supersedes(right));
                                asserted++;
                            }
        assertEquals("the asserted set changed size; a shape was added or dropped silently", 400, asserted);
        assertEquals("the skipped set changed size", 624, normalised);
    }

    /**
     * The one shape where this holder cannot mirror the reference, pinned rather than excluded.
     * {@code withExpirationTime} drops an expiration time that arrives without a TTL, so the reference ends up
     * comparing {@code NO_EXPIRATION_TIME} against {@code NO_EXPIRATION_TIME} and reports no supersession; the
     * holder keeps both values and reports that the later one wins.
     *
     * Only reachable from a corrupt or degenerate sstable whose TTL delta decodes to zero: the reader reads an
     * expiration time only inside {@code hasTTL(flags)}. {@code StatefulCursor.hasInvalidRowLiveness} does not
     * flag it either, because its whole body is guarded by {@code ttl != NO_TTL}.
     */
    @Test
    public void supersedesDivergesOnLivenessThatTheReferenceNormalises()
    {
        assertTrue(referenceNormalisesAway(LivenessInfo.NO_TTL, NOW_IN_SEC));

        ReusableLivenessInfo later = reusable(TIMESTAMP, LivenessInfo.NO_TTL, NOW_IN_SEC);
        ReusableLivenessInfo earlier = reusable(TIMESTAMP, LivenessInfo.NO_TTL, NOW_IN_SEC - 10);
        assertTrue("the holder compares the expiration times it was given", later.supersedes(earlier));

        LivenessInfo laterRef = LivenessInfo.withExpirationTime(TIMESTAMP, LivenessInfo.NO_TTL, NOW_IN_SEC);
        LivenessInfo earlierRef = LivenessInfo.withExpirationTime(TIMESTAMP, LivenessInfo.NO_TTL, NOW_IN_SEC - 10);
        assertEquals("the reference discards both expiration times",
                     LivenessInfo.NO_EXPIRATION_TIME, laterRef.localExpirationTime());
        assertFalse("so the reference sees two identical livenesses", laterRef.supersedes(earlierRef));

        // isLive is unaffected: both answer live for this shape, which is why only supersedes diverges
        assertEquals(laterRef.isLive(NOW_IN_SEC), later.isLive(NOW_IN_SEC));
    }
}
