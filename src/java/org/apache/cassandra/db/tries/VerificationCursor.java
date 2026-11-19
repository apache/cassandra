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

import java.util.Arrays;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.DirectBuffer;

import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

interface VerificationCursor
{
    /// Returns a directed version of the incoming transition, including the return path bit and an overflow bit so that
    /// a 0x100 value (which can be result of applying [Cursor#positionForSkippingBranch]) can be correctly returned.
    @VisibleForTesting
    static int undecodedTransition(long encodedPosition)
    {
        return (int) (encodedPosition >> (Cursor.TRANSITION_SHIFT - 1)) & 0x3FF;
    }

    /// Verifies:
    /// - `advance` does advance, `depth <= prevDepth + 1` and transition is higher than previous at the same depth
    ///   (this requires path tracking)
    /// - `skipTo` is not called with earlier or equal position (including lower levels)
    /// - `maybeSkipTo` is not called with earlier position that can't be identified with depth/incomingTransition only
    ///   (i.e. seeks to lower depth with an incoming transition that lower than the previous at that depth)
    /// - exhausted state matches `Cursor.exhaustedPosition(direction)`
    /// - start state matches `Cursor.rootPosition(direction)`
    class Plain<T, C extends Cursor<T>> implements Cursor<T>, Cursor.TransitionsReceiver
    {
        static
        {
            try
            {
                assert false;
                throw new IllegalStateException("Assertions need to be turned on for verification cursors.");
            }
            catch (AssertionError e)
            {
                // correct path
            }
        }

        final Direction direction;
        final C source;
        long returnedPosition;
        byte[] path;

        Cursor.TransitionsReceiver chainedReceiver = null;
        boolean advanceMultipleCalledReceiver;

        Plain(C cursor)
        {
            this.direction = cursor.direction();
            this.source = cursor;
            this.returnedPosition = source.encodedPosition();
            this.path = new byte[16];
            assert Cursor.direction(returnedPosition) == direction :
                String.format("Invalid direction bit %d in root position %s (%016x)\n%s",
                              (returnedPosition >> DIRECTION_BIT) & 1,
                              Cursor.toString(returnedPosition),
                              returnedPosition,
                              this);
            assert Cursor.compare(returnedPosition, Cursor.rootPosition(direction)) == 0 :
                String.format("Invalid initial position %s (must be %s)\n%s",
                              Cursor.toString(returnedPosition),
                              Cursor.toString(Cursor.rootPosition(direction)),
                              this);
        }

        @Override
        public long encodedPosition()
        {
            long reportedPosition = source.encodedPosition();
            assert Cursor.compare(reportedPosition, returnedPosition) == 0 :
                String.format("Position changed without advance: %s -> %s (reported bits %016x vs expected %016x)\n%s",
                              Cursor.toString(returnedPosition),
                              Cursor.toString(reportedPosition),
                              reportedPosition,
                              returnedPosition,
                              this);
            return returnedPosition;
        }

        @Override
        public T content()
        {
            assert !Cursor.isExhausted(returnedPosition) :
                String.format("Cannot query content on exhausted cursor.\n%s",
                              this);

            T content = source.content();
            if (content != null)
                assert (returnedPosition & MAY_HAVE_CONTENT_BIT) != 0 :
                    String.format("Non-null content for position without MAY_HAVE_CONTENT_BIT: %s\n%s",
                                  Cursor.toString(returnedPosition),
                                  this);
            return content;
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public long advance()
        {
            return verify(source.advance());
        }

        @Override
        public long advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            advanceMultipleCalledReceiver = false;
            chainedReceiver = receiver;
            // Note: if the code below calls the receiver (us), returnedPosition will be adjusted to reflect descent.
            long position = source.advanceMultiple(this);
            chainedReceiver = null;
            int depth = Cursor.depth(position);
            int prevDepth = Cursor.depth(returnedPosition);
            assert !advanceMultipleCalledReceiver || depth == prevDepth + 1 :
                String.format("advanceMultiple returned depth %s did not match depth %s after added characters\n%s",
                              depth,
                              prevDepth + 1,
                              this);
            return verify(position);
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            verifySkipRequest(encodedSkipPosition);
            long newPosition = source.skipTo(encodedSkipPosition);
            assert Cursor.compare(newPosition, encodedSkipPosition) >= 0 ||
                   (Cursor.isExhausted(encodedSkipPosition) && Cursor.isExhausted(newPosition)) :
                String.format("Skip advanced to a position %s before seek target %s\n%s",
                              Cursor.toString(newPosition),
                              Cursor.toString(encodedSkipPosition),
                              this);
            return verify(newPosition);
        }

        private void verifySkipRequest(long encodedSkipPosition)
        {
            int skipDepth = Cursor.depth(encodedSkipPosition);
            int currDepth = Cursor.depth(returnedPosition);
            assert skipDepth <= currDepth + 1 :
                String.format("Skip descends more than one level: %s -> %s\n%s",
                              Cursor.toString(returnedPosition),
                              Cursor.toString(encodedSkipPosition),
                              this);
            int skipTransition = Cursor.incomingTransition(encodedSkipPosition);
            if (skipDepth <= currDepth && skipDepth > 0)
            {
                int visitedByte = getByte(skipDepth);
                assert direction.gt(skipTransition, visitedByte) || skipTransition == visitedByte && Cursor.isOnReturnPath(encodedSkipPosition) :
                    String.format("Skip goes backwards to %s where it already visited byte %2x\n%s",
                                  Cursor.toString(encodedSkipPosition),
                                  visitedByte,
                                  this);
            }

        }

        private long verify(long newPosition)
        {
            int newDepth = Cursor.depth(newPosition);
            int oldDepth = Cursor.depth(returnedPosition);
            assert newDepth <= oldDepth + 1 :
                String.format("Cursor advanced more than one level: %s -> %s\n%s",
                              Cursor.toString(returnedPosition),
                              Cursor.toString(newPosition),
                              this);
            assert Cursor.direction(newPosition) == direction :
                String.format("Invalid direction bit %d in position %s (%016x)\n%s",
                              (newPosition >> DIRECTION_BIT) & 1,
                              Cursor.toString(newPosition),
                              newPosition,
                              this);

            if (Cursor.isExhausted(newPosition))
            {
                assert Cursor.compare(newPosition, Cursor.exhaustedPosition(direction)) == 0 :
                    String.format("Cursor exhausted state should be %s but was %s\n%s",
                                  Cursor.toString(Cursor.exhaustedPosition(direction)),
                                  Cursor.toString(newPosition),
                                  this);
            }
            else if (newDepth == 0)
            {
                // For range/set tries it is possible to ascend back to the root on the return path.
                assert Cursor.isOnReturnPath(newPosition) : "Ascend to depth 0 is only possible on the return path";
                assert Cursor.incomingTransition(newPosition) == 0 : "Invalid incoming transition " + Cursor.incomingTransition(newPosition) + " for depth 0";
            }
            else
            {
                if (newDepth <= oldDepth)
                {
                    assert ((getByte(newDepth) ^ direction.select(0x00, 0xFF)) << 1)
                           < undecodedTransition(newPosition) :
                        String.format("Cursor went backwards to %s where it already visited byte %s\n%s",
                                      Cursor.toString(newPosition),
                                      getByte(newDepth),
                                      this);
                }
                int undecodedTransition = undecodedTransition(newPosition) >> 1;
                assert undecodedTransition >= 0 && undecodedTransition <= 0xFF :
                    String.format("Cursor returned invalid incoming transition as %s (%016x)\n%s",
                                  Cursor.toString(newPosition),
                                  newPosition,
                                  this);
                addByte(newPosition);
            }
            returnedPosition = newPosition;
            return newPosition;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Plain<T, C> tailCursor(Direction direction)
        {
            return new Plain<>((C) source.tailCursor(direction));
        }


        @Override
        public void addPathByte(int nextByte)
        {
            advanceMultipleCalledReceiver = true;
            returnedPosition = Cursor.positionForDescentWithByte(returnedPosition, nextByte);
            addByte(returnedPosition);

            if (chainedReceiver != null)
                chainedReceiver.addPathByte(nextByte);
        }

        private void addByte(long asPosition)
        {
            addByte(Cursor.incomingTransition(asPosition), Cursor.depth(asPosition));
        }

        private void addByte(int nextByteEncoded, int depth)
        {
            int index = depth - 1;
            if (index >= path.length)
                path = Arrays.copyOf(path, path.length * 2);
            path[index] = (byte) nextByteEncoded;
        }

        private int getByte(int depth)
        {
            return path[depth - 1] & 0xFF;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            advanceMultipleCalledReceiver = true;
            for (int i = 0; i < count; ++i)
            {
                int nextByte = buffer.getByte(pos + i) & 0xFF;
                returnedPosition = Cursor.positionForDescentWithByte(returnedPosition, nextByte);
                addByte(returnedPosition);
            }
            if (chainedReceiver != null)
                chainedReceiver.addPathBytes(buffer, pos, count);
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append(source.getClass().getTypeName()
                                 .replace(source.getClass().getPackageName() + '.', ""));
            builder.append(" pos ");
            builder.append(Cursor.toString(returnedPosition));
            if (Cursor.isExhausted(returnedPosition))
            {
                builder.append(" exhausted");
            }
            else
            {
                builder.append(" at ");
                builder.append(Hex.bytesToHex(path, 0, Cursor.depth(returnedPosition)));
            }
            return builder.toString();
        }
    }

    abstract class WithRanges<S extends RangeState<S>, C extends RangeCursor<S>>
    extends Plain<S, C>
    implements RangeCursor<S>
    {
        S currentPrecedingState;
        S nextPrecedingState;

        WithRanges(C source)
        {
            super(source);

            currentPrecedingState = verifyCoveringStateProperties(source.precedingState());
            assert currentPrecedingState == null :
                String.format("Cursor starts with non-null preceeding state %s\n%s",
                              currentPrecedingState,
                              this);
            final S content = source.content();
            nextPrecedingState = content != null ? verifyBoundaryStateProperties(content).succedingState(direction)
                                                 : currentPrecedingState;
        }

        void verifyEndState()
        {
            // We cannot be carrying an open covering state when exhausted (open-ended sets/ranges must close the range
            // by stopping on the root in the return path).
            assert currentPrecedingState == null :
                String.format("Cursor ends with non-null covering state %s\n%s",
                              currentPrecedingState,
                              this);
        }

        @Override
        public long advance()
        {
            currentPrecedingState = nextPrecedingState;
            return verifyState(super.advance());
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            currentPrecedingState = nextPrecedingState;
            return verifyState(super.advanceMultiple(receiver));
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return verifySkipState(super.skipTo(encodedSkipPosition));
        }

        @Override
        public S precedingState()
        {
            assert currentPrecedingState == source.precedingState() ||
                   currentPrecedingState != null && currentPrecedingState.equals(source.precedingState()) :
                String.format("Preceding state changed without advance: %s -> %s.\n%s",
                              currentPrecedingState, source.precedingState(),
                              this);
            return currentPrecedingState;
        }

        @Override
        public S state()
        {
            S returnedState = source.state();
            if (Cursor.isExhausted(returnedPosition))
                assert returnedState == null :
                    String.format("Non-null state on exhausted cursor: %s.\n%s",
                                  returnedState,
                                  this);

            return returnedState;
        }

        boolean agree(S left, S right)
        {
            return Objects.equals(left, right);
        }

        private long verifyState(long position)
        {
            if (Cursor.isExhausted(position))
                verifyEndState();

            S precedingState = source.precedingState();
            boolean equal = agree(currentPrecedingState, precedingState);
            assert equal : String.format("Unexpected change to covering state: %s -> %s\n%s",
                                         currentPrecedingState, precedingState, this);
            currentPrecedingState = precedingState;

            S content = Cursor.isExhausted(position) ? null : source.content();
            S fullState = source.state();
            if (content != null)
            {
                assert agree(fullState, content) : String.format("State %s not equal to content %s\n%s",
                                                            fullState, content, this);
                assert agree(currentPrecedingState, content.precedingState(direction)) :
                String.format("Range end %s does not close covering state %s\n%s",
                              content.precedingState(direction), currentPrecedingState, this);
                verifyBoundaryStateProperties(content);
                nextPrecedingState = content.succedingState(direction);
            }
            else
                assert fullState == precedingState : String.format("State %s not equal to precedingState %s\n%s",
                                                                   fullState, precedingState, this);

            return position;
        }

        private long verifySkipState(long encodedSkipPosition)
        {
            // The covering state information is invalidated by a skip.
            if (!Cursor.isExhausted(encodedSkipPosition))
            {
                currentPrecedingState = verifyCoveringStateProperties(source.precedingState());
                nextPrecedingState = currentPrecedingState;
            }
            else
                currentPrecedingState = nextPrecedingState = null;

            return verifyState(encodedSkipPosition);
        }

        S verifyCoveringStateProperties(S state)
        {
            if (state == null)
                return null;
            assert !state.isBoundary() :
                String.format("Boundary state %s was returned where a covering state was expected\n%s",
                              state,
                              this);
            final S precedingState = state.precedingState(Direction.FORWARD);
            final S succeedingState = state.succedingState(Direction.FORWARD);
            assert precedingState == state && succeedingState == state :
                String.format("State %s must return itself its preceding and succeeding state (returned %s/%s)\n%s",
                              state,
                              precedingState,
                              succeedingState,
                              this);
            return state;
        }

        S verifyBoundaryStateProperties(S state)
        {
            if (state == null)
                return null;
            assert state.isBoundary() :
                String.format("Covering state %s was returned where a boundary state was expected\n%s",
                              state,
                              this);
            final S precedingState = state.precedingState(Direction.FORWARD);
            final S succeedingState = state.succedingState(Direction.FORWARD);
            verifyCoveringStateProperties(precedingState);
            verifyCoveringStateProperties(succeedingState);
            return state;
        }


        @Override
        public abstract WithRanges<S, C> tailCursor(Direction direction);

        @Override
        public String toString()
        {
            return super.toString() + (Cursor.isExhausted(returnedPosition) ? "" : " state " + state());
        }
    }


    class Range<S extends RangeState<S>> extends WithRanges<S, RangeCursor<S>> implements RangeCursor<S>
    {
        Range(RangeCursor<S> source)
        {
            super(source);
        }

        @Override
        public Range<S> tailCursor(Direction direction)
        {
            return new Range<>(source.tailCursor(direction));
        }

        @Override
        public RangeCursor<S> precedingStateCursor(Direction direction)
        {
            RangeCursor<S> cursor = source.precedingStateCursor(direction);
            return cursor != null ? new Range<>(cursor) : null;
        }
    }

    class TrieSet extends WithRanges<TrieSetCursor.RangeState, TrieSetCursor> implements TrieSetCursor
    {
        TrieSet(TrieSetCursor source)
        {
            super(source);
        }

        @Override
        public RangeState nonNullState()
        {
            RangeState rangeState = source.nonNullState();
            assert rangeState != null : String.format("Null nonNullState()\n%s", this);
            return rangeState;
        }

        @Override
        public TrieSet tailCursor(Direction direction)
        {
            return new TrieSet(source.tailCursor(direction));
        }

        @Override
        public TrieSetCursor precedingStateCursor(Direction direction)
        {
            TrieSetCursor cursor = source.precedingStateCursor(direction);
            return cursor != null ? new TrieSet(cursor) : null;
        }
    }

    class DeletionAware<T, D extends RangeState<D>>
    extends VerificationCursor.Plain<T, DeletionAwareCursor<T, D>>
    implements DeletionAwareCursor<T, D>
    {
        int deletionBranchDepth;

        DeletionAware(DeletionAwareCursor<T, D> source)
        {
            super(source);
            this.deletionBranchDepth = -1;
            verifyDeletionBranch(returnedPosition);
        }

        @Override
        public long advance()
        {
            return verifyDeletionBranch(super.advance());
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return verifyDeletionBranch(super.advanceMultiple(receiver));
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return verifyDeletionBranch(super.skipTo(encodedSkipPosition));
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            // deletionBranch is already verified
            final RangeCursor<D> deletionBranch = source.deletionBranchCursor(direction);
            if (deletionBranch == null)
                return null;
            return new Range<>(deletionBranch);
        }

        long verifyDeletionBranch(long position)
        {
            int depth = Cursor.depth(position);
            if (depth <= deletionBranchDepth)
                deletionBranchDepth = -1;
            if (Cursor.isExhausted(position))
                return position;

            RangeCursor<D> deletionBranch = source.deletionBranchCursor(direction);
            if (deletionBranch != null)
            {
                assert (source.encodedPosition() & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0 :
                    String.format("Deletion branch found but MAY_HAVE_DELETION_BRANCH_BIT was not set on position: %s\n%s",
                                  Cursor.toString(source.encodedPosition()),
                                  this);
                assert deletionBranchDepth == -1 :
                    String.format("Deletion branch at position %s covered by another deletion branch at parent depth %s\n%s",
                                  Cursor.toString(position),
                                  deletionBranchDepth,
                                  this);
                assert Cursor.compare(deletionBranch.encodedPosition(), Cursor.rootPosition(direction)) == 0 :
                    String.format("Invalid deletion branch initial position %s\n%s",
                                  Cursor.toString(deletionBranch.encodedPosition()),
                                  this);
                assert deletionBranch.precedingState() == null :
                    String.format("Deletion branch starts with active deletion %s\n%s",
                                  deletionBranch.precedingState(),
                                  this);
                deletionBranch.skipTo(Cursor.exhaustedPosition(direction));
                assert deletionBranch.precedingState() == null :
                    String.format("Deletion branch ends with active deletion %s\n%s",
                                  deletionBranch.precedingState(),
                                  this);
                deletionBranchDepth = Cursor.depth(position);
            }
            return position;
        }

        @Override
        public DeletionAware<T, D> tailCursor(Direction direction)
        {
            return new DeletionAware<>(source.tailCursor(direction));
        }
    }
}
