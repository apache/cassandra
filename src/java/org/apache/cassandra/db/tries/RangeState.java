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

/// A range state interface used for range tries.
///
/// This interface combines two logical concepts:
/// - A range marker / boundary point, which is a point in the trie that either starts or ends a range, or switches
///   between ranges. Such markers are the content of a range trie and by themselves are sufficient to define and
///   recreate the trie.
///
///   Markers return `true` for [#isBoundary()] and usually return different values for [#precedingState] in the two
///   directions (which can be `null` if no range applies). It is also possible for a marker to specify a point of
///   coverage, in which the preceding state is the same in both directions.
///
/// - A covering range state, which describes the range that applies to an iteration position which is inside a covered
///   range. These are necessary to be able to efficiently jump inside range tries, for example when constructing the
///   intersection between a range trie and a set trie to answer a query. When a cursor skips to a position after a
///   point requested in a [Cursor#skipTo] call, the preceding state that the cursor returns is a covering state,
///   which describes the range that applies to (i.e. covers) the position the user requested.
///
///   Covering states return `false` for [#isBoundary()] and must return themselves for [#precedingState] in both
///   directions.
///
/// Using this combination instead of separate concepts simplifies and improves the performance of the implementation.
public interface RangeState<S extends RangeState<S>>
{
    /// True if this is a boundary point. Boundary points are reported by `content()` and usually apply a different
    /// state before and after the point (i.e. `precedingState(FORWARD) != precedingState(REVERSE)`).
    boolean isBoundary();

    /// Returns the state that applies to the positions preceding this marker in the given iteration order, if any.
    ///
    /// This must always be a covering state (i.e. [#isBoundary()] must be `false` and the forward and reverse
    /// preceding states are equal to itself).
    S precedingState(Direction direction);

    /// Assuming this is a boundary, returns an intersected version of this state, which may drop parts of a marker that
    /// are not covered by the intersecting range.
    S restrict(boolean applicableBefore, boolean applicableAfter);

    /// Assuming this is a covering state, promote it to a boundary active in the specified direction.
    S asBoundary(Direction direction);

    /// Assuming this is a covering state, convert it to a point deletion.
    default S asPoint()
    {
        return null;
    }
}
