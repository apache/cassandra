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

import com.google.common.base.Preconditions;

import org.agrona.DirectBuffer;

import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface VerificationCursor
{
    int EXHAUSTED_DEPTH = -1;
    int EXHAUSTED_TRANSITION = -1;
    int INITIAL_TRANSITION = -1;

    /// Verifies:
    /// - `advance` does advance, `depth <= prevDepth + 1` and transition is higher than previous at the same depth
    ///   (this requires path tracking)
    /// - `skipTo` is not called with earlier or equal position (including lower levels)
    /// - `maybeSkipTo` is not called with earlier position that can't be identified with depth/incomingTransition only
    ///   (i.e. seeks to lower depth with an incoming transition that lower than the previous at that depth)
    /// - exhausted state is `depth = -1, incomingTransition = -1`
    /// - start state is `depth = 0, incomingTransition = -1`
    class Plain<T, C extends Cursor<T>> implements Cursor<T>, Cursor.TransitionsReceiver
    {
        final Direction direction;
        final C source;
        final int minDepth;
        int returnedDepth;
        int returnedTransition;
        byte[] path;

        Cursor.TransitionsReceiver chainedReceiver = null;
        boolean advanceMultipleCalledReceiver;

        Plain(C cursor)
        {
            this(cursor, 0, 0, INITIAL_TRANSITION);
        }

        Plain(C cursor, int minDepth, int expectedDepth, int expectedTransition)
        {
            this.direction = cursor.direction();
            this.source = cursor;
            this.minDepth = minDepth;
            this.returnedDepth = expectedDepth;
            this.returnedTransition = expectedTransition;
            this.path = new byte[16];
            Preconditions.checkState(source.depth() == expectedDepth && source.incomingTransition() == expectedTransition,
                                     "Invalid initial depth %s with incoming transition %s (must be %s, %s)\n%s",
                                     source.depth(), source.incomingTransition(),
                                     expectedDepth, expectedTransition, this);
        }

        @Override
        public int depth()
        {
            Preconditions.checkState(returnedDepth == source.depth(),
                                     "Depth changed without advance: %s -> %s\n%s",
                                     returnedDepth, source.depth(), this);
            return returnedDepth;
        }

        @Override
        public int incomingTransition()
        {
            Preconditions.checkState(returnedTransition == source.incomingTransition(),
                                     "Transition changed without advance: %s -> %s\n%s",
                                     returnedTransition, source.incomingTransition(), this);
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public int advance()
        {
            return verify(source.advance());
        }

        @Override
        public int advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            advanceMultipleCalledReceiver = false;
            chainedReceiver = receiver;
            int depth = source.advanceMultiple(this);
            chainedReceiver = null;
            Preconditions.checkState(!advanceMultipleCalledReceiver || depth == returnedDepth + 1,
                                     "advanceMultiple returned depth %s did not match depth %s after added characters\n%s",
                                     depth, returnedDepth + 1, this);
            return verify(depth);
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            verifySkipRequest(skipDepth, skipTransition);
            return verify(source.skipTo(skipDepth, skipTransition));
        }

        private void verifySkipRequest(int skipDepth, int skipTransition)
        {
            Preconditions.checkState(skipDepth <= returnedDepth + 1,
                                     "Skip descends more than one level: %s -> %s\n%s",
                                     returnedDepth,
                                     skipDepth,
                                     this);
            if (skipDepth <= returnedDepth && skipDepth > minDepth)
                Preconditions.checkState(direction.lt(getByte(skipDepth), skipTransition),
                                         "Skip goes backwards to %s at depth %s where it already visited %s\n%s",
                                         skipTransition, skipDepth, getByte(skipDepth), this);

        }

        private int verify(int depth)
        {
            Preconditions.checkState(depth <= returnedDepth + 1,
                                     "Cursor advanced more than one level: %s -> %s\n%s",
                                     returnedDepth,
                                     depth,
                                     this);
            Preconditions.checkState(depth < 0 || depth > minDepth,
                                     "Cursor ascended to depth %s beyond its minimum depth %s\n%s",
                                     depth, minDepth, this);
            final int transition = source.incomingTransition();
            if (depth < 0)
            {
                Preconditions.checkState(depth == EXHAUSTED_DEPTH && transition == EXHAUSTED_TRANSITION,
                                         "Cursor exhausted state should be %s, %s but was %s, %s\n%s",
                                         EXHAUSTED_DEPTH, EXHAUSTED_TRANSITION,
                                         depth, transition, this);
            }
            else if (depth <= returnedDepth)
            {
                Preconditions.checkState(direction.lt(getByte(depth), transition),
                                         "Cursor went backwards to %s at depth %s where it already visited %s\n%s",
                                         transition, depth, getByte(depth), this);
            }
            returnedDepth = depth;
            returnedTransition = transition;
            if (depth >= 0)
                addByte(returnedTransition, depth);
            return depth;
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
            addByte(nextByte, ++returnedDepth);
            returnedTransition = nextByte;
            if (chainedReceiver != null)
                chainedReceiver.addPathByte(nextByte);
        }

        private void addByte(int nextByte, int depth)
        {
            advanceMultipleCalledReceiver = true;
            int index = depth - minDepth - 1;
            if (index >= path.length)
                path = Arrays.copyOf(path, path.length * 2);
            path[index] = (byte) nextByte;
        }

        private int getByte(int depth)
        {
            return path[depth - minDepth - 1] & 0xFF;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            for (int i = 0; i < count; ++i)
                addByte(buffer.getByte(pos + i), returnedDepth + 1 + i);
            returnedDepth += count;
            returnedTransition = buffer.getByte(pos + count - 1) & 0xFF;
            if (chainedReceiver != null)
                chainedReceiver.addPathBytes(buffer, pos, count);
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append(source.getClass().getTypeName()
                                 .replace(source.getClass().getPackageName() + '.', ""));
            if (returnedDepth < 0)
            {
                builder.append(" exhausted");
            }
            else
            {
                builder.append(" at ");
                builder.append(Hex.bytesToHex(path, 0, returnedDepth - minDepth));
            }
            return builder.toString();
        }
    }

    abstract class WithRanges<S extends RangeState<S>, C extends RangeCursor<S>>
    extends Plain<S, C>
    implements RangeCursor<S>
    {
        S currentPrecedingState = null;
        S nextPrecedingState = null;
        int maxNextDepth = Integer.MAX_VALUE;

        WithRanges(C source)
        {
            super(source);
            // start state can be non-null for sets
            currentPrecedingState = verifyCoveringStateProperties(source.precedingState());
            final S content = source.content();
            nextPrecedingState = content != null ? verifyBoundaryStateProperties(content).precedingState(direction.opposite())
                                                 : currentPrecedingState;
        }

        void verifyEndState()
        {
            // end state can be non-null for sets
        }

        @Override
        public int advance()
        {
            currentPrecedingState = nextPrecedingState;
            checkIfDescentShouldBeForbidden();
            return verifyState(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            currentPrecedingState = nextPrecedingState;
            checkIfDescentShouldBeForbidden();
            return verifyState(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            checkIfDescentShouldBeForbidden();
            return verifySkipState(super.skipTo(skipDepth, skipTransition));
        }

        private void checkIfDescentShouldBeForbidden()
        {
            maxNextDepth = source.content() != null ? source.depth() : Integer.MAX_VALUE;
        }

        @Override
        public S precedingState()
        {
            Preconditions.checkState(currentPrecedingState == source.precedingState() ||
                                     currentPrecedingState != null && currentPrecedingState.equals(source.precedingState()),
                                     "Preceding state changed without advance: %s -> %s.\n%s",
                                     currentPrecedingState, source.precedingState(),
                                     this);
            return currentPrecedingState;
        }

        @Override
        public S state()
        {
            return source.state();
        }

        boolean agree(S left, S right)
        {
            return Objects.equals(left, right);
        }

        private int verifyState(int depth)
        {
            S precedingState = source.precedingState();
            boolean equal = agree(currentPrecedingState, precedingState);
            Preconditions.checkState(equal,
                                     "Unexpected change to covering state: %s -> %s\n%s",
                                     currentPrecedingState, precedingState, this);
            Preconditions.checkState(depth <= maxNextDepth,
                                     "Cursor descended after reporting an included branch\n%s",
                                     this);
            currentPrecedingState = precedingState;

            S content = source.content();
            if (content != null)
            {
                Preconditions.checkState(agree(currentPrecedingState, content.precedingState(direction)),
                                         "Range end %s does not close covering state %s\n%s",
                                         content.precedingState(direction), currentPrecedingState, this);
                verifyBoundaryStateProperties(content);
                nextPrecedingState = content.precedingState(direction.opposite());
            }

            if (depth < 0)
                verifyEndState();
            return depth;
        }

        private int verifySkipState(int depth)
        {
            // The covering state information is invalidated by a skip.
            currentPrecedingState = verifyCoveringStateProperties(source.precedingState());
            nextPrecedingState = currentPrecedingState;
            return verifyState(depth);
        }

        S verifyCoveringStateProperties(S state)
        {
            if (state == null)
                return null;
            Preconditions.checkState(!state.isBoundary(),
                                     "Boundary state %s was returned where a covering state was expected\n%s",
                                     state,
                                     this);
            final S precedingState = state.precedingState(Direction.FORWARD);
            final S succeedingState = state.precedingState(Direction.REVERSE);
            Preconditions.checkState(precedingState == state && succeedingState == state,
                                     "State %s must return itself its preceding and succeeding state (returned %s/%s)\n%s",
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
            Preconditions.checkState(state.isBoundary(),
                                     "Covering state %s was returned where a boundary state was expected\n%s",
                                     state,
                                     this);
            final S precedingState = state.precedingState(Direction.FORWARD);
            final S succeedingState = state.precedingState(Direction.REVERSE);
            verifyCoveringStateProperties(precedingState);
            verifyCoveringStateProperties(succeedingState);
            return state;
        }


        @Override
        public abstract WithRanges<S, C> tailCursor(Direction direction);

        @Override
        public String toString()
        {
            return super.toString() + " state " + state();
        }
    }


    class Range<S extends RangeState<S>> extends WithRanges<S, RangeCursor<S>> implements RangeCursor<S>
    {
        Range(RangeCursor<S> source)
        {
            super(source);
            Preconditions.checkState(currentPrecedingState == null,
                                     "Initial preceding state %s should be null for range cursor\n%s",
                                     currentPrecedingState, this);
        }

        @Override
        void verifyEndState()
        {
            Preconditions.checkState(currentPrecedingState == null,
                                     "End state %s should be null for range cursor\n%s",
                                     currentPrecedingState, this);
        }

        @Override
        public Range<S> tailCursor(Direction direction)
        {
            return new Range<>(source.tailCursor(direction));
        }
    }

    class TrieSet extends WithRanges<TrieSetCursor.RangeState, TrieSetCursor> implements TrieSetCursor
    {
        TrieSet(TrieSetCursor source)
        {
            super(source);
            // start and end state can be non-null for sets
        }

        @Override
        public TrieSetCursor.RangeState state()
        {
            return Preconditions.checkNotNull(source.state());
        }

        @Override
        public TrieSet tailCursor(Direction direction)
        {
            return new TrieSet(source.tailCursor(direction));
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
            verifyDeletionBranch(0);
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public int advance()
        {
            return verifyDeletionBranch(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return verifyDeletionBranch(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return verifyDeletionBranch(super.skipTo(skipDepth, skipTransition));
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

        int verifyDeletionBranch(int depth)
        {
            if (depth <= deletionBranchDepth)
                deletionBranchDepth = -1;

            RangeCursor<D> deletionBranch = source.deletionBranchCursor(direction);
            if (deletionBranch != null)
            {
                Preconditions.checkState(deletionBranchDepth == -1,
                                         "Deletion branch at depth %s covered by another deletion branch at parent depth %s",
                                         depth, deletionBranchDepth);
                Preconditions.checkState(deletionBranch.depth() == 0,
                                         "Invalid deletion branch initial depth %s",
                                         deletionBranch.depth());
                Preconditions.checkState(deletionBranch.incomingTransition() == INITIAL_TRANSITION,
                                         "Invalid deletion branch initial transition %s",
                                         deletionBranch.incomingTransition());
                Preconditions.checkState(deletionBranch.precedingState() == null,
                                         "Deletion branch starts with active deletion %s",
                                         deletionBranch.precedingState());
                deletionBranch.skipTo(EXHAUSTED_DEPTH, EXHAUSTED_TRANSITION);
                Preconditions.checkState(deletionBranch.precedingState() == null,
                                         "Deletion branch ends with active deletion %s",
                                         deletionBranch.precedingState());
                deletionBranchDepth = depth;
            }
            return depth;
        }

        @Override
        public DeletionAware<T, D> tailCursor(Direction direction)
        {
            return new DeletionAware<>(source.tailCursor(direction));
        }
    }
}
