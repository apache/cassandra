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

package org.apache.cassandra.db.tries;

import org.junit.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class CursorTest
{
    static final int EXAMPLES = 100_000;

    private static final Gen<Integer> DEPTH_GEN = integers().between(0, 1000);
    private static final Gen<Integer> TRANSITION_GEN = integers().between(0, 0xFF);
    private static final Gen<Direction> DIRECTION_GEN = SourceDSL.arbitrary().enumValues(Direction.class);

    @Test
    public void testDepth()
    {
        // Test with forward direction
        long pos = Cursor.encode(0, 0, Direction.FORWARD);
        assertEquals(0, Cursor.depth(pos));

        pos = Cursor.encode(1, 0, Direction.FORWARD);
        assertEquals(1, Cursor.depth(pos));

        pos = Cursor.encode(42, 0, Direction.FORWARD);
        assertEquals(42, Cursor.depth(pos));

        // Test with reverse direction
        pos = Cursor.encode(0, 0, Direction.REVERSE);
        assertEquals(0, Cursor.depth(pos));

        pos = Cursor.encode(1, 0, Direction.REVERSE);
        assertEquals(1, Cursor.depth(pos));

        // Test with negative depth (exhausted position)
        pos = Cursor.EXHAUSTED_POSITION_FORWARD;
        assertEquals(-1, Cursor.depth(pos));
    }

    @Test
    public void testIsExhausted()
    {
        // Non-exhausted positions
        long pos = Cursor.encode(0, 0, Direction.FORWARD);
        assertFalse(Cursor.isExhausted(pos));

        pos = Cursor.encode(1, 0x12, Direction.FORWARD);
        assertFalse(Cursor.isExhausted(pos));

        // Exhausted positions
        assertTrue(Cursor.isExhausted(Cursor.EXHAUSTED_POSITION_FORWARD));
        assertTrue(Cursor.isExhausted(Cursor.EXHAUSTED_POSITION_REVERSE));
    }

    @Test
    public void testDepthCorrectionValue()
    {
        qt().withExamples(EXAMPLES)
            .forAll(DEPTH_GEN, DEPTH_GEN, TRANSITION_GEN, DIRECTION_GEN)
            .checkAssert((depth, diff, transition, direction) -> {
                long pos = Cursor.encode(depth, transition, direction);
                long diffPos = Cursor.encode(diff, transition, direction);
                long adjustment = Cursor.depthCorrectionValue(diffPos);

                long newPos = pos + adjustment;
                assertEquals(depth + diff, Cursor.depth(newPos));
                assertEquals(direction, Cursor.direction(newPos));
                assertEquals(transition.intValue(), Cursor.incomingTransition(newPos));
            });
    }

    @Test
    public void testIncomingTransition()
    {
        // Test forward direction
        long pos = Cursor.encode(1, 0x12, Direction.FORWARD);
        assertEquals(0x12, Cursor.incomingTransition(pos));

        // Test reverse direction (should be the same as forward)
        pos = Cursor.encode(1, 0x12, Direction.REVERSE);
        assertEquals(0x12, Cursor.incomingTransition(pos));

        // Test with different transitions
        for (int i = 0; i < 0x100; i++)
        {
            pos = Cursor.encode(1, i, Direction.FORWARD);
            assertEquals(i, Cursor.incomingTransition(pos));
        }
    }

    @Test
    public void testUndecodedTransition()
    {
        // Test forward direction
        long pos = Cursor.encode(1, 0x12, Direction.FORWARD);
        assertEquals(0x12 << 1, VerificationCursor.undecodedTransition(pos));

        // Test reverse direction (should have bit 0x100 set)
        pos = Cursor.encode(1, 0x12, Direction.REVERSE);
        assertEquals((0x12 ^ 0xFF) << 1, VerificationCursor.undecodedTransition(pos));

        // Test with different transitions
        for (int i = 0; i < 0x100; i++)
        {
            // Forward direction
            pos = Cursor.encode(1, i, Direction.FORWARD);
            assertEquals(i << 1, VerificationCursor.undecodedTransition(pos));

            // Reverse direction
            pos = Cursor.encode(1, i, Direction.REVERSE);
            assertEquals((i ^ 0xFF) << 1, VerificationCursor.undecodedTransition(pos));
        }
    }

    @Test
    public void testDirection()
    {
        // Test forward direction
        long pos = Cursor.encode(1, 0x12, Direction.FORWARD);
        assertEquals(Direction.FORWARD, Cursor.direction(pos));

        // Test reverse direction
        pos = Cursor.encode(1, 0x12, Direction.REVERSE);
        assertEquals(Direction.REVERSE, Cursor.direction(pos));
    }

    @Test
    public void testCompareForward()
    {
        testCompare(Direction.FORWARD);
    }

    @Test
    public void testCompareReverse()
    {
        testCompare(Direction.REVERSE);
    }

    public void testCompare(Direction direction)
    {
        qt().withExamples(EXAMPLES)
            .forAll(DEPTH_GEN, TRANSITION_GEN, DEPTH_GEN, TRANSITION_GEN)
            .checkAssert((depth1, transition1, depth2, transition2) -> {
                long pos1 = Cursor.encode(depth1, transition1, direction);
                long pos2 = Cursor.encode(depth2, transition2, direction);

                long diff = Cursor.compare(pos1, pos2);
                int cmp = Long.signum(diff);

                int cmpExpected = Integer.compare(depth2, depth1); // higher depth is earlier
                // if equal, check directed difference in transitions
                if (cmpExpected == 0 && transition1.intValue() != transition2.intValue())
                    cmpExpected = direction.lt(transition1, transition2) ? -1 : 1;

                assertEquals(cmpExpected, cmp);
            });

    }

    @Test
    public void testCompareSimple()
    {
        // Equal positions
        long pos1 = Cursor.encode(1, 0x12, Direction.FORWARD);
        long pos2 = Cursor.encode(1, 0x12, Direction.FORWARD);
        assertEquals(0, Cursor.compare(pos1, pos2));

        // Different depths
        pos1 = Cursor.encode(2, 0x12, Direction.FORWARD);
        pos2 = Cursor.encode(1, 0x12, Direction.FORWARD);
        assertTrue(Cursor.compare(pos1, pos2) < 0);
        assertTrue(Cursor.compare(pos2, pos1) > 0);

        // Same depth, different transitions
        pos1 = Cursor.encode(1, 0x11, Direction.FORWARD);
        pos2 = Cursor.encode(1, 0x12, Direction.FORWARD);
        assertTrue(Cursor.compare(pos1, pos2) < 0);
        assertTrue(Cursor.compare(pos2, pos1) > 0);

        // Different directions
        pos1 = Cursor.encode(1, 0x12, Direction.FORWARD);
        pos2 = Cursor.encode(1, 0x12, Direction.REVERSE);
        assertNotEquals(0, Cursor.compare(pos1, pos2));
    }

    @Test
    public void testRootPosition()
    {
        // Test forward direction
        long pos = Cursor.rootPosition(Direction.FORWARD);
        assertEquals(0, Cursor.depth(pos));
        assertEquals(0, Cursor.incomingTransition(pos));
        assertEquals(Direction.FORWARD, Cursor.direction(pos));

        // Test reverse direction
        pos = Cursor.rootPosition(Direction.REVERSE);
        assertEquals(0, Cursor.depth(pos));
        assertEquals(0, Cursor.incomingTransition(pos));
        assertEquals(Direction.REVERSE, Cursor.direction(pos));
    }

    @Test
    public void testExhaustedPosition()
    {
        // Test forward direction
        long pos = Cursor.exhaustedPosition(Direction.FORWARD);
        assertEquals(-1, Cursor.depth(pos));
        assertEquals(0, Cursor.incomingTransition(pos));
        assertEquals(Direction.FORWARD, Cursor.direction(pos));

        // Test reverse direction
        pos = Cursor.exhaustedPosition(Direction.REVERSE);
        assertEquals(-1, Cursor.depth(pos));
        assertEquals(0, Cursor.incomingTransition(pos));
        assertEquals(Direction.REVERSE, Cursor.direction(pos));
    }

    @Test
    public void testExhaustedPositionFromPrevious()
    {
        qt().withExamples(EXAMPLES)
            .forAll(DEPTH_GEN, TRANSITION_GEN, DIRECTION_GEN)
            .checkAssert((depth, transition, direction) -> {
                long prevPos = Cursor.encode(depth, transition, direction);
                long pos = Cursor.exhaustedPosition(prevPos);
                assertEquals(-1, Cursor.depth(pos));
                assertEquals(0, Cursor.incomingTransition(pos));
                assertEquals(direction, Cursor.direction(pos));
            });
    }

    @Test
    public void testEncode()
    {
        qt().withExamples(EXAMPLES)
            .forAll(DEPTH_GEN, TRANSITION_GEN, DIRECTION_GEN)
            .checkAssert((depth, transition, direction) -> {
                long pos = Cursor.encode(depth, transition, direction);
                assertEquals(depth.intValue(), Cursor.depth(pos));
                assertEquals(transition.intValue(), Cursor.incomingTransition(pos));
                assertEquals(direction, Cursor.direction(pos));
            });
    }

    @Test
    public void testPositionForDescentWithByte()
    {
        qt().withExamples(EXAMPLES)
            .forAll(DEPTH_GEN, TRANSITION_GEN, TRANSITION_GEN, DIRECTION_GEN)
            .checkAssert((depth, prev, transition, direction) -> {
                long pos = Cursor.encode(depth, prev, direction);
                long newPos = Cursor.positionForDescentWithByte(pos, transition);
                assertEquals(depth.intValue() + 1, Cursor.depth(newPos));
                assertEquals(transition.intValue(), Cursor.incomingTransition(newPos));
                assertEquals(direction, Cursor.direction(newPos));
            });
    }

    @Test
    public void testPositionForSkippingBranch()
    {
        qt().withExamples(EXAMPLES)
            .forAll(DEPTH_GEN, TRANSITION_GEN, DIRECTION_GEN)
            .checkAssert((depth, transition, direction) -> {
                long pos = Cursor.encode(depth, transition, direction);
                long newPos = Cursor.positionForSkippingBranch(pos);
                assertTrue(Cursor.compare(pos, newPos) < 0);
                assertEquals(depth.intValue(), Cursor.depth(newPos));
                if (transition.intValue() != direction.select(0xFF, 0x00))
                    assertEquals(transition + direction.increase, Cursor.incomingTransition(newPos));
                else
                    assertEquals(0x200, VerificationCursor.undecodedTransition(newPos));
                assertEquals(direction, Cursor.direction(newPos));
            });
    }

    @Test
    public void testAscendedForward()
    {
        testAscended(Direction.FORWARD);
    }

    @Test
    public void testAscendedReverse()
    {
        testAscended(Direction.REVERSE);
    }

    public void testAscended(Direction direction)
    {
        qt().withExamples(EXAMPLES)
            .forAll(DEPTH_GEN, TRANSITION_GEN, DEPTH_GEN, TRANSITION_GEN)
            .checkAssert((depth, transition, newDepth, newTransition) -> {
                if (depth.intValue() == 0)
                    transition = 0; // non-zero is not valid for the root position
                // ensure that the next position is a valid advance target.
                if (newDepth.intValue() == 0)
                {
                    newDepth = -1;
                    newTransition = 0;
                }
                else if (newDepth > depth + 1)
                    newDepth = depth + 1;
                else if (newDepth.intValue() == depth.intValue() && direction.le(newTransition, transition))
                {
                    if (transition.intValue() != direction.select(0xFF, 0x00))
                        newTransition = transition + direction.increase;
                    else
                        --depth;
                }

                long prevPos = Cursor.encode(depth, transition, direction);
                long nextPos = Cursor.encode(newDepth, newTransition, direction);

                assertEquals(newDepth <= depth, Cursor.ascended(nextPos, prevPos));
            });
    }
}
