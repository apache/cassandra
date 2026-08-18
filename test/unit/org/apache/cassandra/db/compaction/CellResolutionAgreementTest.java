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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellLivenessInfo.Resolution;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.index.sai.utils.CellWithSource;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.COMPARE;
import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.LEFT;
import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.RIGHT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * The cursor compaction path and the reference path reach one decision, {@link CellLivenessInfo#resolve}.
 * They did not always: the cursor carried a hand-mirrored copy of {@code Cells.resolveRegular}'s
 * same-timestamp table, and the copies drifted — the cursor's used {@code !isExpiring()} to mean "is a
 * tombstone" while {@code isExpiring()} still meant "has an expiration time", which is true of tombstones
 * too. Both arms of the tombstone-vs-expiring tie-break were therefore dead, and the
 * {@code localDeletionTime} comparison below it picked an expiring cell over a genuine tombstone at the same
 * timestamp — resurrecting deleted data, with its value. That was found by reading, not by a test.
 *
 * With one decision, agreement between the two entry points can no longer distinguish "both right" from "both
 * wrong the same way", so what this class pins is threefold: the shared decision's verdict per rule against
 * literals; that the reference path's mapping of that verdict onto a cell is faithful to it; and that neither
 * depends on which cell class carries the liveness, since the reference path narrows its arguments to one
 * subtree for the JIT's sake and has a second branch for everything else.
 *
 * The reference is reached through the public {@link Cells#reconcile}, which dispatches to its private
 * {@code resolveRegular} for non-counter cells, so no upstream visibility is widened.
 */
public class CellResolutionAgreementTest
{
    private static final long NOW = 1_700_000_000L;
    private static final long TIMESTAMP = 42L;

    /** The cursor's holder is built by reset(...), never by LivenessInfo.withExpirationTime — see the
     * javadoc on {@link #shapes()}. */
    private static final TableMetadata METADATA =
    TableMetadata.builder("CellResolutionAgreementTest", "Test")
                 .addPartitionKeyColumn("key", AsciiType.instance)
                 .addClusteringColumn("clustering", Int32Type.instance)
                 .addRegularColumn("v", Int32Type.instance)
                 .build();

    private static final ColumnMetadata COLUMN = METADATA.getColumn(ByteBufferUtil.bytes("v"));

    /** A same-timestamp cell shape: everything {@code resolveRegular} reads apart from the value. */
    private static final class Shape
    {
        final int ttl;
        final long localDeletionTime;

        Shape(int ttl, long localDeletionTime)
        {
            this.ttl = ttl;
            this.localDeletionTime = localDeletionTime;
        }

        public String toString()
        {
            return "(ttl=" + (ttl == Cell.NO_TTL ? "NO_TTL" : ttl)
                   + ", ldt=" + (localDeletionTime == Cell.NO_DELETION_TIME ? "NO_DELETION_TIME"
                                                                           : (localDeletionTime - NOW) + "+NOW") + ')';
        }
    }

    /**
     * The full cross product, deliberately including two shapes that are not well-formed cells:
     *
     *  - {@code ttl != NO_TTL} with {@code ldt == NO_DELETION_TIME} is corrupt (an expiring cell must carry
     *    an expiration time), but it is constructible as a {@link BufferCell} and both tables read it, so
     *    excluding it would leave a live disagreement uncovered.
     *  - {@code ldt} in the past, at, and after {@code NOW} separates the tie-break's expiration comparison
     *    from any clock-dependent notion of "already expired"; neither table consults a clock.
     *
     * {@code LivenessInfo.EXPIRED_LIVENESS_TTL} is excluded: it is a ROW liveness marker produced only by
     * view maintenance, never a cell TTL, so no cell of that shape can reach either table.
     */
    private static List<Shape> shapes()
    {
        List<Shape> shapes = new ArrayList<>();
        for (int ttl : new int[]{ Cell.NO_TTL, 1, 100 })
            for (long ldt : new long[]{ Cell.NO_DELETION_TIME, NOW - 10, NOW, NOW + 10 })
                shapes.add(new Shape(ttl, ldt));
        return shapes;
    }

    /**
     * The cursor returns a verdict; the reference returns a cell. One run cannot separate LEFT from COMPARE,
     * because {@code compareValues(left, right) >= 0 ? left : right} hands back the left cell on equal
     * values. So each pair is run twice, with equal values and with a strictly smaller left value, and the
     * verdict is read off the pair of outcomes:
     *
     * <pre>
     *   reference picks   equal values   left &lt; right      verdict
     *   ---------------------------------------------------------
     *   LEFT              left           left              LEFT
     *   RIGHT             right          right              RIGHT
     *   COMPARE           left           right              COMPARE
     * </pre>
     *
     * ("reference picks right on equal values, left when left is smaller" is not producible by either table
     * and is asserted against as a fourth case.)
     */
    @Test
    public void theTwoTablesAgreeOnEveryOrderedPair()
    {
        List<Shape> shapes = shapes();
        int pairs = 0;
        for (Shape left : shapes)
        {
            for (Shape right : shapes)
            {
                Resolution cursor = cursorVerdict(left, right);
                Resolution reference = referenceVerdict(left, right);
                assertEquals("tables disagree for left=" + left + " right=" + right,
                             reference, cursor);
                pairs++;
            }
        }
        assertEquals("the enumeration shrank; a shape was dropped silently", 144, pairs);
    }

    /**
     * Both tables are folded left-to-right across N sources, so a non-commutative tie-break would make the
     * output depend on sstable order. Asserted on the VERDICT, not on the returned cell: the cell-level
     * property does not hold, because the value tail returns the left argument both ways on equal values.
     *
     * Associativity is relied on by both folds and is NOT asserted here.
     */
    @Test
    public void theVerdictIsCommutative()
    {
        for (Shape left : shapes())
        {
            for (Shape right : shapes())
            {
                Resolution forward = cursorVerdict(left, right);
                Resolution mirrored = mirror(cursorVerdict(right, left));
                assertEquals("verdict is not commutative for left=" + left + " right=" + right,
                             forward, mirrored);
            }
        }
    }

    private static Resolution mirror(Resolution resolution)    {
        switch (resolution)
        {
            case LEFT: return RIGHT;
            case RIGHT: return LEFT;
            default: return COMPARE;
        }
    }

    private static Resolution cursorVerdict(Shape left, Shape right)
    {
        return cursorVerdict(TIMESTAMP, left, TIMESTAMP, right);
    }

    private static Resolution cursorVerdict(long leftTimestamp, Shape left, long rightTimestamp, Shape right)
    {
        ReusableCellLivenessInfo leftLiveness = new ReusableCellLivenessInfo();
        ReusableCellLivenessInfo rightLiveness = new ReusableCellLivenessInfo();
        leftLiveness.reset(leftTimestamp, left.ttl, left.localDeletionTime);
        rightLiveness.reset(rightTimestamp, right.ttl, right.localDeletionTime);
        return CellLivenessInfo.resolve(leftLiveness, rightLiveness);
    }

    private static Resolution referenceVerdict(Shape left, Shape right)
    {
        return referenceVerdict(TIMESTAMP, left, TIMESTAMP, right);
    }

    private static Resolution referenceVerdict(long leftTimestamp, Shape left, long rightTimestamp, Shape right)
    {
        boolean equalPicksLeft = reconcilePicksLeft(leftTimestamp, left, rightTimestamp, right, 1, 1);
        boolean smallerLeftPicksLeft = reconcilePicksLeft(leftTimestamp, left, rightTimestamp, right, 1, 2);

        if (equalPicksLeft && smallerLeftPicksLeft)
            return LEFT;
        if (!equalPicksLeft && !smallerLeftPicksLeft)
            return RIGHT;
        if (equalPicksLeft)
            return COMPARE;
        throw new AssertionError("reference picked right on equal values and left on a smaller left value, "
                                 + "which neither table can produce: left=" + left + " right=" + right);
    }

    private static boolean reconcilePicksLeft(long leftTimestamp, Shape left, long rightTimestamp, Shape right,
                                              int leftValue, int rightValue)
    {
        Cell<?> leftCell = cell(leftTimestamp, left, leftValue);
        Cell<?> rightCell = cell(rightTimestamp, right, rightValue);
        return Cells.reconcile(leftCell, rightCell) == leftCell;
    }

    private static Cell<?> cell(long timestamp, Shape shape, int value)
    {
        ByteBuffer bytes = Int32Type.instance.decompose(value);
        return new BufferCell(COLUMN, timestamp, shape.ttl, shape.localDeletionTime, bytes, null);
    }

    /**
     * The same cell reached through a class outside the {@link org.apache.cassandra.db.rows.HeapAbstractCell}
     * subtree. {@link CellWithSource} is a real one — SAI wraps cells in it to carry the source table — and it
     * delegates every liveness accessor, so the verdict must not depend on which of the two arrives.
     */
    private static Cell<?> cellOutsideTheHeapSubtree(long timestamp, Shape shape, int value)
    {
        return new CellWithSource<>(cast(cell(timestamp, shape, value)), new SequenceBasedSSTableId(value));
    }

    @SuppressWarnings("unchecked")
    private static <T> Cell<T> cast(Cell<?> cell)
    {
        return (Cell<T>) cell;
    }

    /**
     * The verdict must be a function of the liveness fields alone, not of the concrete cell class carrying
     * them. Nothing else here varies that: every other assertion builds {@link BufferCell}s, so a reference
     * path that dispatched on the cell's class — or narrowed to one subtree and mishandled the rest — would
     * leave the whole rest of this class green.
     *
     * Each pair is run twice, with equal values and with a strictly smaller left value, because on equal
     * values the value tail returns the left cell for both LEFT and COMPARE. One run cannot tell those apart,
     * so a branch that collapsed COMPARE to LEFT would agree with itself across cell classes and pass.
     */
    @Test
    public void theVerdictDoesNotDependOnTheCellClass()
    {
        List<Shape> shapes = shapes();
        int pairs = 0;
        for (Shape left : shapes)
        {
            for (Shape right : shapes)
            {
                for (long rightTimestamp : new long[]{ TIMESTAMP, TIMESTAMP + 1 })
                {
                    for (int leftValue : new int[]{ 1, 2 })
                    {
                        assertSameAcrossCellClasses(left, right, rightTimestamp, leftValue);
                        pairs++;
                    }
                }
            }
        }
        assertEquals("the enumeration shrank; a shape was dropped silently", 576, pairs);
    }

    private static void assertSameAcrossCellClasses(Shape left, Shape right, long rightTimestamp, int leftValue)
    {
        String where = " for left=" + left + " right=" + right + " rightTimestamp=" + rightTimestamp
                       + " leftValue=" + leftValue;

        Cell<?> heapLeft = cell(TIMESTAMP, left, leftValue);
        Cell<?> heapRight = cell(rightTimestamp, right, 2);
        boolean heapPicksLeft = Cells.reconcile(heapLeft, heapRight) == heapLeft;

        Cell<?> wrappedLeft = cellOutsideTheHeapSubtree(TIMESTAMP, left, leftValue);
        Cell<?> wrappedRight = cellOutsideTheHeapSubtree(rightTimestamp, right, 2);
        boolean wrappedPicksLeft = Cells.reconcile(wrappedLeft, wrappedRight) == wrappedLeft;
        assertEquals("the reference path disagrees with itself across cell classes" + where,
                     heapPicksLeft, wrappedPicksLeft);

        // and mixing the two classes across one pair resolves the same way, in both arrangements
        boolean mixedRightPicksLeft = Cells.reconcile(heapLeft, wrappedRight) == heapLeft;
        assertEquals("the reference path disagrees on a heap/wrapped pair" + where,
                     heapPicksLeft, mixedRightPicksLeft);

        boolean mixedLeftPicksLeft = Cells.reconcile(wrappedLeft, heapRight) == wrappedLeft;
        assertEquals("the reference path disagrees on a wrapped/heap pair" + where,
                     heapPicksLeft, mixedLeftPicksLeft);
    }

    /**
     * The answer, not merely agreement. Both callers now delegate to one table, so agreement alone can no
     * longer distinguish "both right" from "both wrong the same way", and the shared entry point interleaves
     * two types across two sides — {@code (int ttl, long localDeletionTime)} per side — so transposing a
     * side's pair, or swapping the sides at one call site, is the easiest available error and would leave
     * both callers agreeing on a wrong verdict.
     *
     * One case per rule, plus the two rules above the table.
     */
    @Test
    public void theSharedTableGivesTheExpectedVerdict()
    {
        Shape live = new Shape(Cell.NO_TTL, Cell.NO_DELETION_TIME);
        Shape tombstoneAtNow = new Shape(Cell.NO_TTL, NOW);
        Shape tombstoneLater = new Shape(Cell.NO_TTL, NOW + 10);
        Shape expiringAtNow = new Shape(100, NOW);
        Shape expiringLater = new Shape(100, NOW + 10);
        Shape expiringShortTtl = new Shape(1, NOW);

        // the deletion-time guard: live-vs-live never enters the table and falls through to the value
        // comparison. The rule above it, differing timestamps, is pinned by theNewerTimestampWinsOutright.
        assertEquals(COMPARE, cursorVerdict(live, live));

        // (a) anything with a deletion time beats a live cell, in either position
        assertEquals(LEFT, cursorVerdict(tombstoneAtNow, live));
        assertEquals(RIGHT, cursorVerdict(live, tombstoneAtNow));
        assertEquals(LEFT, cursorVerdict(expiringAtNow, live));
        assertEquals(RIGHT, cursorVerdict(live, expiringAtNow));

        // (b) a tombstone beats an expiring cell, even one expiring later — this is the arm that was dead
        assertEquals(LEFT, cursorVerdict(tombstoneAtNow, expiringLater));
        assertEquals(RIGHT, cursorVerdict(expiringLater, tombstoneAtNow));

        // (c) same kind, later expiration wins
        assertEquals(RIGHT, cursorVerdict(tombstoneAtNow, tombstoneLater));
        assertEquals(LEFT, cursorVerdict(tombstoneLater, tombstoneAtNow));
        assertEquals(RIGHT, cursorVerdict(expiringAtNow, expiringLater));

        // (d) both expiring, same expiration, lower TTL wins
        assertEquals(LEFT, cursorVerdict(expiringShortTtl, expiringAtNow));
        assertEquals(RIGHT, cursorVerdict(expiringAtNow, expiringShortTtl));

        // identical shapes fall through to the value comparison, for tombstones and expiring cells alike
        assertEquals(COMPARE, cursorVerdict(tombstoneAtNow, tombstoneAtNow));
        assertEquals(COMPARE, cursorVerdict(expiringAtNow, expiringAtNow));
    }

    /**
     * The differing-timestamp rule outranks everything below it, and nothing else here varies the timestamp
     * — every other assertion resets both sides to the same one. So inverting it would leave the rest of this
     * class green while compaction silently reverted an UPDATE: at differing timestamps the older cell would
     * win.
     */
    @Test
    public void theNewerTimestampWinsOutright()
    {
        Shape live = new Shape(Cell.NO_TTL, Cell.NO_DELETION_TIME);
        Shape tombstone = new Shape(Cell.NO_TTL, NOW);

        // asserted through both entry points, since the reference path maps the verdict onto a cell itself
        assertEquals(LEFT, cursorVerdict(TIMESTAMP + 1, live, TIMESTAMP, live));
        assertEquals(RIGHT, cursorVerdict(TIMESTAMP, live, TIMESTAMP + 1, live));
        assertEquals(LEFT, referenceVerdict(TIMESTAMP + 1, live, TIMESTAMP, live));
        assertEquals(RIGHT, referenceVerdict(TIMESTAMP, live, TIMESTAMP + 1, live));

        // and it outranks the tie-break entirely: a newer LIVE cell beats an older tombstone, where at equal
        // timestamps the tombstone would win
        assertEquals(LEFT, cursorVerdict(TIMESTAMP + 1, live, TIMESTAMP, tombstone));
        assertEquals(RIGHT, cursorVerdict(TIMESTAMP, tombstone, TIMESTAMP + 1, live));
        assertEquals(LEFT, referenceVerdict(TIMESTAMP + 1, live, TIMESTAMP, tombstone));
        assertEquals(RIGHT, referenceVerdict(TIMESTAMP, tombstone, TIMESTAMP + 1, live));
        assertEquals("at equal timestamps the tombstone wins, which is what makes the pair above meaningful",
                     LEFT, cursorVerdict(tombstone, live));
    }

    /**
     * The tie-break asserts its own precondition — at least one side carries a deletion time — because
     * {@link CellLivenessInfo#resolve} establishes it above the call, and a future caller reaching the
     * tie-break directly would otherwise get a plausible {@code COMPARE} instead of a failure. {@code -ea} is
     * live in production here, so this pins that the assert is present and reachable rather than optimised
     * into nothing.
     */
    @Test
    public void theSharedTableRejectsALiveVersusLivePair()
    {
        try
        {
            CellLivenessInfo.resolveSameTimestampTie(Cell.NO_TTL, Cell.NO_DELETION_TIME,
                                                     Cell.NO_TTL, Cell.NO_DELETION_TIME);
            fail("the precondition assert did not fire; is -ea enabled for this fork?");
        }
        catch (AssertionError expected)
        {
            // the callers' guard is what keeps this unreachable in production
        }
    }
}
