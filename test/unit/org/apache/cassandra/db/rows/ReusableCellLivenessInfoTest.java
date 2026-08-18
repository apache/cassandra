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

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The cell half of what {@code ReusableLivenessInfo} used to answer for both roles.
 *
 * {@code isExpiring} and {@code isTombstone} are asserted against the reference, {@link AbstractCell}, which
 * is an independent implementation of each. {@code isLive} is NOT: {@code Cell.isLive(long, long, int)}
 * delegates to {@link CellLivenessInfo#isLive}, so the reference and this class now share one body and
 * comparing them is a tautology. That also means the differential suite stopped being an oracle for the cell
 * liveness rule — both paths would be wrong identically and the output bytes would still match. So the rule is
 * pinned against expected literals here, and this class is the only thing in the tree that does so. Do not
 * "simplify" those assertions back into a comparison against {@link AbstractCell}: that is what made them
 * unable to fail.
 */
public class ReusableCellLivenessInfoTest
{
    private static final long TIMESTAMP = 1000L;
    private static final long NOW_IN_SEC = 1_700_000_000L;

    private static ColumnMetadata column;
    private static ByteBuffer value;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        column = ColumnMetadata.regularColumn("ks", "tbl", "v", Int32Type.instance, 0);
        value = Int32Type.instance.decompose(1);
    }

    /**
     * {@code isExpiring()} is "has a TTL" and {@code isTombstone()} is "has a deletion time and no TTL",
     * exactly as the cell reference defines them — a tombstone carries a deletion time without a TTL, which
     * is why the two cannot share a predicate.
     */
    @Test
    public void predicatesAgreeWithCellReference()
    {
        // (ttl, localDeletionTime): live, tombstone, expiring-and-not-yet-expired, expiring-and-EXPIRED, and
        // the corrupt TTL with no deletion time. All five are representable by a Cell. The expired one is what
        // exercises `nowInSec < localDeletionTime` as false with a TTL present; without it the expiry half of
        // the rule is never evaluated.
        int[] ttls = { Cell.NO_TTL, Cell.NO_TTL, 100, 100, 100 };
        long[] ldts = { Cell.NO_DELETION_TIME, NOW_IN_SEC - 10, NOW_IN_SEC + 10, NOW_IN_SEC - 10, Cell.NO_DELETION_TIME };
        boolean[] expectedLive = { true, false, true, false, true };
        boolean[] expectedTombstone = { false, true, false, false, false };
        boolean[] expectedExpiring = { false, false, true, true, true };

        for (int i = 0; i < ttls.length; i++)
        {
            ReusableCellLivenessInfo liveness = new ReusableCellLivenessInfo();
            liveness.reset(TIMESTAMP, ttls[i], ldts[i]);
            Cell<?> reference = new BufferCell(column, TIMESTAMP, ttls[i], ldts[i], value, null);
            String at = " at ttl=" + ttls[i] + " localDeletionTime=" + ldts[i];

            assertEquals("timestamp must match" + at, reference.timestamp(), liveness.timestamp());
            assertEquals("ttl must match" + at, reference.ttl(), liveness.ttl());
            assertEquals("localDeletionTime must match" + at,
                         reference.localDeletionTime(), liveness.localDeletionTime());
            // against the reference, which implements these two independently
            assertEquals("isExpiring must match AbstractCell.isExpiring()" + at,
                         reference.isExpiring(), liveness.isExpiring());
            assertEquals("isTombstone must match AbstractCell.isTombstone()" + at,
                         reference.isTombstone(), liveness.isTombstone());

            // against literals, because reference and holder share one isLive body
            assertEquals("isExpiring" + at, expectedExpiring[i], liveness.isExpiring());
            assertEquals("isTombstone" + at, expectedTombstone[i], liveness.isTombstone());
            assertEquals("isLive" + at, expectedLive[i], liveness.isLive(NOW_IN_SEC));
            assertEquals("the static must answer as the instance does" + at,
                         expectedLive[i], CellLivenessInfo.isLive(NOW_IN_SEC, ldts[i], ttls[i]));
            assertEquals("the reference shares that body, so it must agree too" + at,
                         expectedLive[i], reference.isLive(NOW_IN_SEC));
        }
    }

    /**
     * Converting a lapsed TTL to a tombstone must produce what the reference produces:
     * {@code AbstractCell.purge} builds {@code BufferCell.tombstone(column, timestamp(),
     * localDeletionTime() - ttl(), path())}. Asserted against that cell rather than against a hand-computed
     * {@code NOW_IN_SEC - 100}, so the mirror is structural — if the reference's arithmetic changes, this
     * fails instead of silently encoding the old one.
     */
    @Test
    public void ttlToTombstoneMatchesTheReferenceTombstone()
    {
        ReusableCellLivenessInfo liveness = new ReusableCellLivenessInfo();
        liveness.reset(TIMESTAMP, 100, NOW_IN_SEC);
        assertTrue(liveness.isExpiring());
        assertFalse(liveness.isTombstone());

        Cell<?> expiring = new BufferCell(column, TIMESTAMP, 100, NOW_IN_SEC, value, null);
        Cell<?> reference = BufferCell.tombstone(column, expiring.timestamp(),
                                                 expiring.localDeletionTime() - expiring.ttl(), expiring.path());

        liveness.ttlToTombstone();

        assertEquals("timestamp is unchanged", reference.timestamp(), liveness.timestamp());
        assertEquals("the deletion time is the reference's", reference.localDeletionTime(), liveness.localDeletionTime());
        assertEquals("the TTL is dropped, as the reference drops it", reference.ttl(), liveness.ttl());
        assertEquals("isTombstone must match the reference tombstone",
                     reference.isTombstone(), liveness.isTombstone());
        assertEquals("isExpiring must match the reference tombstone",
                     reference.isExpiring(), liveness.isExpiring());
    }

    /**
     * A fresh instance carries no deletion time, so under the cell contract it is live. The row contract
     * answers the opposite for its own fresh instance — see {@code ReusableLivenessInfoTest} — and that pair
     * of assertions is the point of having split the two classes.
     */
    @Test
    public void freshInstanceIsLiveAndNeitherExpiringNorTombstone()
    {
        ReusableCellLivenessInfo liveness = new ReusableCellLivenessInfo();
        assertFalse(liveness.isExpiring());
        assertFalse(liveness.isTombstone());
        assertTrue("no deletion time means live under the cell contract", liveness.isLive(NOW_IN_SEC));
    }

    /**
     * {@code isExpired(long)} is cell-only and drives the TTL-to-tombstone conversion in the merge, and was
     * untested both here and in the class this was split out of.
     */
    @Test
    public void isExpiredIsTheTtlLapseTest()
    {
        ReusableCellLivenessInfo liveness = new ReusableCellLivenessInfo();
        liveness.reset(TIMESTAMP, 100, NOW_IN_SEC);
        assertTrue("at the expiration second the TTL has lapsed", liveness.isExpired(NOW_IN_SEC));
        assertTrue(liveness.isExpired(NOW_IN_SEC + 1));
        assertFalse(liveness.isExpired(NOW_IN_SEC - 1));

        // the merge guards this on the expiring flag, so combined they are AbstractCell.purge's condition.
        // Asserted at BOTH polarities: at the expiration second the pair is false/true, and a second
        // earlier it is true/false, so an implementation that returned a constant fails one of them.
        assertEquals("guarded by isExpiring, isExpired is the negation of isLive",
                     liveness.isLive(NOW_IN_SEC), !liveness.isExpired(NOW_IN_SEC));
        assertEquals("and at the other polarity, a second before expiry",
                     liveness.isLive(NOW_IN_SEC - 1), !liveness.isExpired(NOW_IN_SEC - 1));
    }

    /** The diagnostic that {@code UnfilteredValidation.handleInvalid} prints for a corrupt cell. */
    @Test
    public void toStringNamesTheFields()
    {
        ReusableCellLivenessInfo liveness = new ReusableCellLivenessInfo();
        assertTrue("a fresh instance must render as empty, got " + liveness,
                   liveness.toString().contains("NONE"));

        liveness.reset(TIMESTAMP, 100, NOW_IN_SEC);
        String rendered = liveness.toString();
        assertTrue("must name the timestamp, got " + rendered, rendered.contains(String.valueOf(TIMESTAMP)));
        assertTrue("must name the ttl, got " + rendered, rendered.contains("ttl=100"));
        assertTrue("must name the deletion time, got " + rendered,
                   rendered.contains("localDeletionTime=" + NOW_IN_SEC));
    }
}
