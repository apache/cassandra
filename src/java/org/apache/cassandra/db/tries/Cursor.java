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

import java.util.function.Function;

import javax.annotation.Nullable;

import org.agrona.DirectBuffer;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// A trie cursor.
///
/// This is the internal representation of a trie, which enables efficient walks and basic operations (merge,
/// slice) on tries.
///
/// The cursor represents the state of a walk over the nodes of trie. It provides three main features:
///   - the current [#depth] or descend-depth in the trie;
///   - the [#incomingTransition], i.e. the byte that was used to reach the current point;
///   - the [#content] associated with the current node,
///
/// and provides methods for advancing to the next position.  This is enough information to extract all paths, and
/// also to easily compare cursors over different tries that are advanced together. Advancing is always done in
/// order; if one imagines the set of nodes in the trie with their associated paths, a cursor may only advance from a
/// node with a lexicographically smaller path to one with bigger. The [#advance] operation moves to the immediate
/// next, it is also possible to skip over some items to a specific position ahead ([#skipTo]).
///
/// Moving to the immediate next position in the lexicographic order is accomplished by:
///   - if the current node has children, moving to its first child;
///   - otherwise, ascend the parent chain and return the next child of the closest parent that still has any.
///
/// As long as the trie is not exhausted, advancing always takes one step down, from the current node, or from a node
/// on the parent chain. By comparing the new depth with the one before the advance, one can tell if the former was
/// the case (if `newDepth == oldDepth + 1`) and how many steps up we had to take (`oldDepth + 1 - newDepth`) if it
/// wasn't. When following a path down, the cursor will stop on all prefixes.
///
/// When it is created the cursor is placed on the root node with `depth = 0`, `incomingTransition = 0`.
/// Since tries can have mappings for empty, `content()` can possibly be non-null. The cursor is exhausted when it
/// returns a depth of -1 (the operations that advance a cursor return the depth, and `depth()` will also
/// return -1 if queried afterwards). It is not allowed for a cursor to start in exhausted state; once a cursor is
/// exhausted, calling any of the advance methods or `tailTrie` is an error.
///
/// For example, the following trie:
/// <pre>
///  t
///   r
///    e
///     e *
///    i
///     e *
///     p *
///  w
///   i
///    n  *
/// </pre>
/// has nodes reachable with the paths
/// `"", t, tr, tre, tree*, tri, trie*, trip*, w, wi, win*`
/// and the cursor will list them with the following `(depth, incomingTransition)` pairs:
/// `(0, -1), (1, t), (2, r), (3, e), (4, e)*, (3, i), (4, e)*, (4, p)*, (1, w), (2, i), (3, n)*`
///
/// Because we exhaust transitions on bigger depths before we go the next transition on the smaller ones, when
/// cursors are advanced together, i.e. when we walk the combination of two or more cursors in order, where we would
/// want to advance the one that is earlier in the comparison order until it catches up, their positions can be easily
/// compared using only the `depth` and
/// `incomingTransition`:
///   - one that is higher in depth is before one that is lower;
///   - for equal depths, the one with smaller incomingTransition is first.
///
/// If we consider walking the trie above in parallel with this:
/// <pre>
///  t
///   r
///    i
///     c
///      k *
///  u
///   p *
/// </pre>
/// the combined iteration will proceed as follows:<pre>
///  (0, -1)+  (0, -1)+          cursors equal, advance both
///  (1, t)+   (1, t)+   t       cursors equal, advance both
///  (2, r)+   (2, r)+   tr      cursors equal, advance both
///  (3, e)+ < (3, i)    tre     cursors not equal, advance smaller (3 = 3, e < i)
///  (4, e)+ < (3, i)    tree*   cursors not equal, advance smaller (4 > 3)
///  (3, i)+   (3, i)+   tri     cursors equal, advance both
///  (4, e)  > (4, c)+   tric    cursors not equal, advance smaller (4 = 4, e > c)
///  (4, e)  > (5, k)+   trick*  cursors not equal, advance smaller (4 < 5)
///  (4, e)+ < (1, u)    trie*   cursors not equal, advance smaller (4 > 1)
///  (4, p)+ < (1, u)    trip*   cursors not equal, advance smaller (4 > 1)
///  (1, w)  > (1, u)+   u       cursors not equal, advance smaller (1 = 1, w > u)
///  (1, w)  > (2, p)+   up*     cursors not equal, advance smaller (1 < 2)
///  (1, w)+ < (-1, -1)  w       cursors not equal, advance smaller (1 > -1)
///  (2, i)+ < (-1, -1)  wi      cursors not equal, advance smaller (2 > -1)
///  (3, n)+ < (-1, -1)  win*    cursors not equal, advance smaller (3 > -1)
///  (-1, -1)  (-1, -1)          both exhasted
///  </pre>
///
/// To improve performance, `depth` and `incomingTransition` are encoded together in a single long that is set up in
/// a way that lets us compare positions (in the sense of the demonstration above) by comparing the encoded position
/// as an integer.
///
/// Cursors are created with a direction (forward or reverse), which specifies the order in which a node's children
/// are iterated (smaller first or larger first). Note that entries returned in reverse direction are in
/// lexicographic order for the inverted alphabet, which is not the same as being presented in reverse. For example,
/// a cursor for a trie containing "ab", "abc" and "cba", will visit the nodes in order "cba", "ab", "abc", i.e.
/// prefixes will still be reported before their descendants.
///
/// Also see [Trie.md](./Trie.md) for further documentation.
interface Cursor<T>
{
    int DEPTH_SHIFT = 32;
    int TRANSITION_SHIFT = 20;

    /// 1 for reverse direction, 0 for forward. Used to xor transition bits for incomingTransition.
    /// This takes part in comparisons but this does not matter positions are always compared with same direction.
    /// This _must_ be 31, the code below takes advantage of this bit being the sign bit of (int) encodedPosition.
    int DIRECTION_BIT = 31;

    /// An additional transition bit used to revisit positions on the way back after iterating the branch.
    /// Used for sets and ranges to correctly define the range states for branch-inclusive ranges.
    long ON_RETURN_PATH_BIT = 1L << 19;

    /// Mask of the transition bits including the direction. We apply xor with this value to form a position in the
    /// reverse direction.
    long TRANSITION_MASK = 0x8FFL << TRANSITION_SHIFT;

    long ROOT_POSITION_FORWARD = encode(0, 0, Direction.FORWARD);
    long ROOT_POSITION_REVERSE = encode(0, 0, Direction.REVERSE);
    long ROOT_POSITION_DEPTH = ROOT_POSITION_FORWARD & 0xFFFFFFFF00000000L;

    long EXHAUSTED_POSITION_FORWARD = encode(-1, 0, Direction.FORWARD);
    long EXHAUSTED_POSITION_REVERSE = encode(-1, 0, Direction.REVERSE);
    long EXHAUSTED_POSITION_DEPTH = EXHAUSTED_POSITION_FORWARD & 0xFFFFFFFF00000000L;

    long DEPTH_ADJUSTMENT_ONE = -1L << DEPTH_SHIFT;

    static int depth(long encodedPosition)
    {
        return ~(int) (encodedPosition >> DEPTH_SHIFT);
    }

    static boolean isExhausted(long encodedPosition)
    {
        // Depth of -1 translates to positive encoding.
        // This must also be true for other positive values that may be the result of adjusting depths.
        return encodedPosition >= 0;
    }

    /// Construct a "depth correction" adjustment that can be added to or subtract from positions to adjust the depth
    /// by the depth of the given encoded position.
    ///
    /// The value is such that
    ///   `depth(somePosition) + depth(initialPosition) = depth(somePosition + depthCorrectionValue(initialPosition))`
    /// including
    ///   `depth(ROOT_POSITION + depthCorrectionValue(encodedPosition)) == depth(encodedPosition)`
    static long depthCorrectionValue(long encodedPosition)
    {
        return ((long) -depth(encodedPosition)) << DEPTH_SHIFT;
    }

    static int incomingTransition(long encodedPosition)
    {
        int transitionInt = (int) encodedPosition;
        transitionInt ^= transitionInt >> DIRECTION_BIT; // flip the transition bits if the direction bit is 1
        return (transitionInt >> TRANSITION_SHIFT) & 0xFF;
    }

    static Direction direction(long encodedPosition)
    {
        return Direction.values()[((int) encodedPosition >>> DIRECTION_BIT) & 1];
    }

    /// Returns true if this position is on the return/ascent path. Positions on the ascent path are used to present
    /// content or boundaries after the children of a node have been seen, e.g. to correctly order prefix content after
    /// children in reverse order, or to close a range covering a branch.
    static boolean isOnReturnPath(long encodedPosition)
    {
        return (encodedPosition & ON_RETURN_PATH_BIT) != 0;
    }

    static long compare(long encoded1, long encoded2)
    {
        // This can support depth of 2^31 - 1 without overflowing.
        return encoded1 - encoded2;
    }

    static long rootPosition(Direction direction)
    {
        return direction.select(ROOT_POSITION_FORWARD, ROOT_POSITION_REVERSE);
    }

    /// Returns the ascent path position for the root, i.e. the last point before going exhausted, as used by open-ended
    /// ranges and sets.
    static long rootReturnPosition(long prevPosition)
    {
        return ROOT_POSITION_DEPTH | ((((long) ((int) prevPosition) >> DIRECTION_BIT)) & TRANSITION_MASK) | ON_RETURN_PATH_BIT;
    }

    static long exhaustedPosition(Direction direction)
    {
        return direction.select(EXHAUSTED_POSITION_FORWARD, EXHAUSTED_POSITION_REVERSE);
    }

    static long exhaustedPosition(long prevPosition)
    {
        return EXHAUSTED_POSITION_DEPTH | ((((long) ((int) prevPosition) >> DIRECTION_BIT)) & TRANSITION_MASK);
    }

    static boolean isRootPosition(long encodedPosition)
    {
        return encodedPosition == ROOT_POSITION_FORWARD || encodedPosition == ROOT_POSITION_REVERSE;
    }

    static long encode(int depth, int transition, Direction direction)
    {
        assert depth >= -1;
        assert transition <= 0xFF && transition >= 0;
        // The xor below flips transition bits and also sets the direction bit to 1 for REVERSE direction.
        long transitionXored = (transition << TRANSITION_SHIFT) ^ direction.select(0, TRANSITION_MASK);
        return ((long) ~depth << DEPTH_SHIFT) | transitionXored;
    }

    /// Returns the position that descends one byte from the given, i.e.
    ///   `positionForDescentWithByte(encodedPosition, incomingByte) ==
    ///        encode(depth(encodedPosition) + 1, incomingByte, direction(encodedPosition))`
    static long positionForDescentWithByte(long encodedPosition, int incomingByte)
    {
        assert !isOnReturnPath(encodedPosition) : "Can't descend from a return path position " + toString(encodedPosition);
        long depthPart = (encodedPosition + DEPTH_ADJUSTMENT_ONE) & 0xFFFFFFFF00000000L;
        long transitionXored = incomingByte ^ (((int) encodedPosition) >> DIRECTION_BIT);
        return depthPart | ((transitionXored << TRANSITION_SHIFT) & TRANSITION_MASK);
    }

    /// Returns a position that can be used to skip over the given branch. Note that this can only work when the
    /// returned encoded position is a valid `skipTo` position for the current state.
    static long positionForSkippingBranch(long encodedBranchPosition)
    {
        return encodedBranchPosition + (1L << TRANSITION_SHIFT);
    }

    /// Returns true if the given `currPosition` as returned by `advance`, `advanceMultiple` or `skipTo` is the result
    /// of ascending in the trie structure (i.e. if its depth is the same or smaller).
    static boolean ascended(long currPosition, long prevPosition)
    {
        // Descending increases the depth, and thus results in a position that is "earlier" according to our comparison
        // order. Any possible ascent position will be ordered higher than the current (it is either a lower depth or
        // the same depth with a higher incoming transition).
        return compare(currPosition, prevPosition) > 0;
    }

    static String toString(long encodedPosition)
    {
        return String.format("depth %d incomingTransition %02x%s %s",
                             depth(encodedPosition),
                             incomingTransition(encodedPosition),
                             isOnReturnPath(encodedPosition) ? "↑" : " ",
                             direction(encodedPosition));
    }

    /// Returns the cursor's current position encoded as a long. This combines the depth and incoming transition as well
    /// as other flags that are part of the state. Use the static methods above to compare, manipulate, decode or encode
    /// cursor positions.
    long encodedPosition();

    /// @return the content associated with the current node. This may be non-null for any presented node, including
    ///         the root.
    @Nullable
    T content();

    /// Returns the direction in which this cursor is progressing.
    default Direction direction()
    {
        return direction(encodedPosition());
    }

    /// Returns the byte-comparable version that this trie uses.
    ByteComparable.Version byteComparableVersion();

    /// Advance one position to the node whose associated path is next lexicographically.
    /// This can be either:
    ///   - descending one level to the first child of the current node,
    ///   - ascending to the closest parent that has remaining children, and then descending one level to its next
    ///     child.
    ///
    /// It is an error to call this after the trie has already been exhausted (i.e. when `depth() == -1`);
    /// for performance reasons we won't always check this.
    ///
    /// @return encoded position after the advance (see [#encodedPosition()]).
    long advance();

    /// Advance, descending multiple levels if the cursor can do this for the current position without extra work
    /// (e.g. when positioned on a chain node in a memtable trie). If the current node does not have children this
    /// is exactly the same as advance(), otherwise it may take multiple steps down (but will not necessarily, even
    /// if they exist).
    ///
    /// Note that if any positions are skipped, their content must be null.
    ///
    /// This is an optional optimization; the default implementation falls back to calling advance.
    ///
    /// It is an error to call this after the trie has already been exhausted (i.e. when `depth() == -1`);
    /// for performance reasons we won't always check this.
    ///
    /// @param receiver object that will receive all transitions taken except the last;
    ///                                 on ascend, or if only one step down was taken, it will not receive any
    /// @return encoded position after the advance (see [#encodedPosition()]).
    default long advanceMultiple(TransitionsReceiver receiver)
    {
        return advance();
    }

    /// Advance all the way to the next node with non-null content.
    ///
    /// It is an error to call this after the trie has already been exhausted (i.e. when `depth() == -1`);
    /// for performance reasons we won't always check this.
    ///
    /// @param receiver object that will receive all taken transitions
    /// @return the content, null if the trie is exhausted
    default T advanceToContent(ResettingTransitionsReceiver receiver)
    {
        long prevPosition = encodedPosition();
        while (true)
        {
            long currPosition = advanceMultiple(receiver);
            if (isExhausted(currPosition))
                return null;
            if (receiver != null)
            {
                if (ascended(currPosition, prevPosition))
                {
                    int depth = depth(currPosition);
                    if (depth > 0)
                    {
                        receiver.resetPathLength(depth - 1);
                        receiver.addPathByte(incomingTransition(currPosition));
                    }
                    else
                    {
                        receiver.resetPathLength(0);
                    }
                }
                else
                    receiver.addPathByte(incomingTransition(currPosition));

                if (isOnReturnPath(currPosition))
                    receiver.onReturnPath();
            }
            T content = content();
            if (content != null)
                return content;
            prevPosition = currPosition;
        }
    }

    /// Advance to the specified depth and incoming transition or the first valid position that is after the specified
    /// position. The inputs must be something that could be returned by a single call to [#advance] (i.e.
    /// `depth` must be <= current depth + 1, and `incomingTransition` must be higher than what the
    /// current state saw at the requested depth).
    /// This method must also support a transition value of 0x100, which may be used to request ascent from the current
    /// position.
    ///
    /// @return encoded position after the skip; the new position will satisfy
    ///         `compare(returnedSkipPosition, encodedSkipPosition) >= 0`.
    long skipTo(long encodedSkipPosition);

    /// A version of [#skipTo] which checks if the requested position is ahead of the cursor's current position and only
    /// advances if it is. This can only be used if the [#skipTo] instruction is issued from a position that is behind
    /// this cursor's (i.e. if the [#skipTo] request is to descend, it is assumed to descend from a position _before_
    /// this cursor's and will not be acted on).
    ///
    /// Used for parallel walks when one of the source cursors is known to be ahead of the current position.
    default long skipToWhenAhead(long encodedSkipPosition)
    {
        long current = encodedPosition();
        if (compare(encodedSkipPosition, current) > 0)
            return skipTo(encodedSkipPosition);
        else
            return current;
    }

    /// Descend into the cursor with the given path.
    ///
    /// @return True if the descent is positioned at the end of the given path, false if the trie did not have a path
    /// for it. In the latter case the cursor is positioned at the first node that follows the given key in iteration
    /// order.
    default boolean descendAlong(ByteSource bytes)
    {
        int next = bytes.next();
        long position = encodedPosition();
        while (next != ByteSource.END_OF_STREAM)
        {
            long nextPosition = positionForDescentWithByte(position, next);
            if (compare(skipTo(nextPosition), nextPosition) != 0)
                return false;
            next = bytes.next();
            position = nextPosition;
        }
        return true;
    }

    /// Returns a tail cursor, i.e. a cursor whose root is the current position. Walking a tail cursor will list all
    /// descendants of the current position with depth adjusted by the current depth.
    ///
    /// It is an error to call `tailCursor` on an exhausted cursor.
    ///
    /// Descendants that override this class should return their specific cursor type.
    Cursor<T> tailCursor(Direction direction);

    /// Used by [#advanceMultiple] to feed the transitions taken.
    interface TransitionsReceiver
    {
        /// Add a single byte to the path.
        void addPathByte(int nextByte);
        /// Add the count bytes from position pos in the given buffer.
        void addPathBytes(DirectBuffer buffer, int pos, int count);

        /// Called when the current position is on the return path. Sets and ranges use these positions to return
        /// end-inclusive ranges.
        default void onReturnPath()
        {
            // nothing by default
        }
    }

    /// Used by [#advanceToContent] to track the transitions and backtracking taken.
    interface ResettingTransitionsReceiver extends TransitionsReceiver
    {
        /// Delete all bytes beyond the given length.
        void resetPathLength(int newLength);
    }

    /// A push interface for walking over a trie. Builds upon [TransitionsReceiver] to be given the bytes of the
    /// path, and adds methods called on encountering content and completion.
    /// See [TrieDumper] for an example of how this can be used, and [TrieEntriesWalker] as a base class
    /// for other common usages.
    interface Walker<T, R> extends Cursor.ResettingTransitionsReceiver
    {
        /// Called when content is found.
        void content(T content);

        /// Called at the completion of the walk.
        R complete();
    }

    /// Process the trie using the given [Walker].
    /// This method must only be called on a freshly constructed cursor.
    default <R> R process(Cursor.Walker<? super T, R> walker)
    {
        assertFresh();
        T content = content();   // handle content on the root node
        if (content == null)
            content = advanceToContent(walker);

        while (content != null)
        {
            walker.content(content);
            content = advanceToContent(walker);
        }
        return walker.complete();
    }

    /// Process the trie using the given [Walker], skipping over branches where content is found.
    /// In other words, it walks the top levels of the trie until it finds a content-bearing node. When it does, it
    /// presents this content and continues with the next sibling of that node, ignoring all substructure below it.
    /// This is useful, for example, when the user uses content/metadata to mark levels of internal hierarchy and wants
    /// to visit only the top-level elements.
    ///
    /// This is similar to [Trie#tailTries], but able to access only the content instead of the full branch.
    ///
    /// This method should only be called on a freshly constructed cursor.
    default <R> R processSkippingBranches(Cursor.Walker<? super T, R> walker)
    {
        assertFresh();
        T content = content();   // handle content on the root node
        if (content != null)
        {
            walker.content(content);
            return walker.complete();
        }
        content = advanceToContent(walker);

        while (content != null)
        {
            walker.content(content);
            // skip over the branch by requesting a position that is beyond
            long current = skipTo(positionForSkippingBranch(encodedPosition()));
            if (isExhausted(current))
                break;
            walker.resetPathLength(depth(current) - 1);
            walker.addPathByte(incomingTransition(current));
            content = content();
            if (content == null)
                content = advanceToContent(walker);
        }
        return walker.complete();
    }

    class Empty<T> implements Cursor<T>
    {
        private final ByteComparable.Version byteComparableVersion;
        long position;

        Empty(Direction direction, ByteComparable.Version byteComparableVersion)
        {
            assert byteComparableVersion != null;
            this.byteComparableVersion = byteComparableVersion;
            position = Cursor.rootPosition(direction);
        }

        public long advance()
        {
            return position = exhaustedPosition(position);
        }

        public long skipTo(long encodedSkipPosition)
        {
            return advance();
        }

        public ByteComparable.Version byteComparableVersion()
        {
            return byteComparableVersion;
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            assert position == Cursor.rootPosition(direction) : "tailTrie called on exhausted cursor";
            return new Empty<>(direction, byteComparableVersion);
        }

        @Override
        public long encodedPosition()
        {
            return position;
        }

        @Override
        public T content()
        {
            return null;
        }
    }

    /// Dump the current branch. To be used for debugging only.
    @SuppressWarnings("unused")
    private String dumpBranch()
    {
        return dumpBranch(Direction.FORWARD);
    }

    /// Dump the current branch. To be used for debugging only.
    @SuppressWarnings("unused")
    private String dumpBranch(Direction direction)
    {
        return dumpBranch(direction, Object::toString);
    }

    /// Dump the current branch. To be used for debugging only.
    private String dumpBranch(Direction direction, Function<T, String> toStringFunction)
    {
        TrieDumper<T> dumper = new TrieDumper.Plain<>(toStringFunction);
        tailCursor(direction).process(dumper);
        return dumper.complete();
    }

    default void assertFresh()
    {
        assert depth(encodedPosition()) == 0 : "The provided cursor has already been advanced.";
    }
}
