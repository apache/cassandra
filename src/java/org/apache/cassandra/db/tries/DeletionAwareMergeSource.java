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

import java.util.function.BiFunction;

import javax.annotation.Nullable;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A cursor applying deletions to a deletion-aware cursor, where the deletions can be dynamically added.
/// Based on [RangeApplyCursor] and used by [MergeCursor.DeletionAware] to process each source with the deletions of the
/// other. The cursor will present the content of the data trie modified by any applicable/covering range of the
/// deletion trie, and will leave the deletion branches unmodied (allowing the merger to process them).
class DeletionAwareMergeSource<T, D extends RangeState<D>, E extends RangeState<E>> implements DeletionAwareCursor<T, D>
{
    final BiFunction<E, T, T> resolver;
    final Direction direction;
    final DeletionAwareCursor<T, D> data;
    @Nullable RangeCursor<E> deletions;
    int deletionsDepthCorrection;

    boolean atDeletions;

    DeletionAwareMergeSource(BiFunction<E, T, T> resolver, DeletionAwareCursor<T, D> data)
    {
        this(resolver, data, null);
    }

    DeletionAwareMergeSource(BiFunction<E, T, T> resolver, DeletionAwareCursor<T, D> data, RangeCursor<E> deletions)
    {
        this.direction = data.direction();
        this.resolver = resolver;
        this.deletions = deletions;
        this.data = data;
        this.deletionsDepthCorrection = 0;
        assert data.depth() == 0;
        assert deletions == null || deletions.depth() == 0;
        atDeletions = deletions != null;
    }

    @Override
    public int depth()
    {
        return data.depth();
    }

    @Override
    public int incomingTransition()
    {
        return data.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        assert deletions == null || deletions.byteComparableVersion() == data.byteComparableVersion() :
        "Merging cursors with different byteComparableVersions: " +
        deletions.byteComparableVersion() + " vs " + data.byteComparableVersion();
        return data.byteComparableVersion();
    }

    @Override
    public int advance()
    {
        int newDataDepth = data.advance();

        if (deletions == null)
            return newDataDepth;
        else if (atDeletions) // if both cursors were at the same position, always advance the deletions' cursor to catch up.
            return skipDeletionsToDataPosition(newDataDepth);
        else // otherwise skip deletions to the new data position only if it advances past the deletions' current position.
            return maybeSkipDeletions(newDataDepth);
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        int newDataDepth = data.skipTo(skipDepth, skipTransition);

        if (deletions == null)
            return newDataDepth;
        else if (atDeletions) // if both cursors were at the same position, always advance the deletions' cursor to catch up.
            return skipDeletionsToDataPosition(newDataDepth);
        else // otherwise skip deletions to the new data position only if it advances past the deletions' current position.
            return maybeSkipDeletions(newDataDepth);
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        if (deletions == null)
            return data.advanceMultiple(receiver);

        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atDeletions)
            return skipDeletionsToDataPosition(data.advance());
        else // atData only
            return maybeSkipDeletions(data.advanceMultiple(receiver));
    }

    int maybeSkipDeletions(int dataDepth)
    {
        int deletionsDepth = deletions.depth() + deletionsDepthCorrection;
        
        // If data position is at or before the deletions position, we are good.
        if (deletionsDepth < dataDepth)
            return setAtDeletionsAndReturnDepth(false, dataDepth);

        if (deletionsDepth == dataDepth)
        {
            int dataTrans = data.incomingTransition();
            int deletionsTrans = deletions.incomingTransition();
            if (direction.le(dataTrans, deletionsTrans))
                return setAtDeletionsAndReturnDepth(dataTrans == deletionsTrans, dataDepth);
        }

        // Deletions cursor is before data cursor.
        return skipDeletionsToDataPosition(dataDepth);
    }

    private int skipDeletionsToDataPosition(int dataDepth)
    {
        // Skip deletions cursor to the data position; if that is beyond the branch's root, no need to skip, just leave it.
        int dataTrans = data.incomingTransition();
        int deletionsSkipDepth = dataDepth - deletionsDepthCorrection;
        int deletionsDepthUncorrected = deletionsSkipDepth > 0 ? deletions.skipTo(deletionsSkipDepth, dataTrans) : -1;
        if (deletionsDepthUncorrected < 0)
            return leaveDeletionsBranch(dataDepth);
        else
            return setAtDeletionsAndReturnDepth(deletionsDepthUncorrected + deletionsDepthCorrection == dataDepth && deletions.incomingTransition() == dataTrans,
                                                dataDepth);
    }

    private int leaveDeletionsBranch(int dataDepth)
    {
        deletions = null;
        return setAtDeletionsAndReturnDepth(false, dataDepth);
    }

    private int setAtDeletionsAndReturnDepth(boolean atDeletions, int depth)
    {
        this.atDeletions = atDeletions;
        return depth;
    }

    @Override
    public T content()
    {
        T content = data.content();
        if (content == null)
            return null;
        if (deletions == null)
            return content;

        E applicableDeletions = atDeletions ? deletions.content() : null;
        if (applicableDeletions == null)
        {
            applicableDeletions = deletions.precedingState();
            if (applicableDeletions == null)
                return content;
        }

        return resolver.apply(applicableDeletions, content);
    }

    @Override
    public DeletionAwareMergeSource<T, D, E> tailCursor(Direction direction)
    {
        if (atDeletions)
            return new DeletionAwareMergeSource<>(resolver, data.tailCursor(direction), deletions.tailCursor(direction));
        else
            return new DeletionAwareMergeSource<>(resolver, data.tailCursor(direction));
    }

    @Override
    public RangeCursor<D> deletionBranchCursor(Direction direction)
    {
        // Return unchanged, to be handled by MergeCursor.
        return data.deletionBranchCursor(direction);
    }

    public void addDeletions(RangeCursor<E> deletions)
    {
        assert this.deletions == null;
        assert deletions.depth() == 0;
        this.deletions = deletions;
        this.deletionsDepthCorrection = data.depth();
        this.atDeletions = true;
    }

    public boolean hasDeletions()
    {
        return deletions != null;
    }
}
