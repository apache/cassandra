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
    final DeletionAwareCursor<T, D> data;
    @Nullable RangeCursor<E> deletions;
    long deletionsDepthCorrection;

    boolean atDeletions;

    DeletionAwareMergeSource(BiFunction<E, T, T> resolver, DeletionAwareCursor<T, D> data)
    {
        this(resolver, data, null);
    }

    DeletionAwareMergeSource(BiFunction<E, T, T> resolver, DeletionAwareCursor<T, D> data, RangeCursor<E> deletions)
    {
        this.resolver = resolver;
        this.deletions = deletions;
        this.data = data;
        this.deletionsDepthCorrection = 0;
        data.assertFresh();
        if (deletions != null)
            deletions.assertFresh();
        atDeletions = deletions != null;
    }

    @Override
    public long encodedPosition()
    {
        return data.encodedPosition();
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
    public long advance()
    {
        long newDataPosition = data.advance();

        if (deletions == null)
            return newDataPosition;
        else if (atDeletions) // if both cursors were at the same position, always advance the deletions' cursor to catch up.
            return skipDeletionsToDataPosition(newDataPosition);
        else // otherwise skip deletions to the new data position only if it advances past the deletions' current position.
            return maybeSkipDeletions(newDataPosition);
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        long newDataPosition = data.skipTo(encodedSkipPosition);

        if (deletions == null)
            return newDataPosition;
        else if (atDeletions) // if both cursors were at the same position, always advance the deletions' cursor to catch up.
            return skipDeletionsToDataPosition(newDataPosition);
        else // otherwise skip deletions to the new data position only if it advances past the deletions' current position.
            return maybeSkipDeletions(newDataPosition);
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (deletions == null)
            return data.advanceMultiple(receiver);

        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atDeletions)
            return skipDeletionsToDataPosition(data.advance());
        else // atData only
            return maybeSkipDeletions(data.advanceMultiple(receiver));
    }

    long maybeSkipDeletions(long dataPosition)
    {
        long deletionsPosition = deletions.encodedPosition() + deletionsDepthCorrection;
        
        // If data position is at or before the deletions position, we are good.
        long cmp = Cursor.compare(dataPosition, deletionsPosition);
        if (cmp <= 0)
            return setAtDeletionsAndReturnPosition(cmp == 0, dataPosition);

        // Deletions cursor is before data cursor.
        return skipDeletionsToDataPosition(dataPosition);
    }

    private long skipDeletionsToDataPosition(long dataPosition)
    {
        // Skip deletions cursor to the data position; if that is beyond the branch's root, no need to skip, just leave it.
        long deletionsSkipPosition = dataPosition - deletionsDepthCorrection;
        long deletionsPositionUncorrected = !Cursor.isExhausted(deletionsSkipPosition)
                                            ? deletions.skipTo(deletionsSkipPosition)
                                            : Cursor.exhaustedPosition(deletionsSkipPosition);
        if (Cursor.isExhausted(deletionsPositionUncorrected))
            return leaveDeletionsBranch(dataPosition);
        else
            return setAtDeletionsAndReturnPosition(Cursor.compare(deletionsPositionUncorrected, deletionsSkipPosition) == 0,
                                                   dataPosition);
    }

    private long leaveDeletionsBranch(long dataPosition)
    {
        deletions = null;
        return setAtDeletionsAndReturnPosition(false, dataPosition);
    }

    private long setAtDeletionsAndReturnPosition(boolean atDeletions, long position)
    {
        this.atDeletions = atDeletions;
        return position;
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
        else if (deletions != null)
            return new DeletionAwareMergeSource<>(resolver, data.tailCursor(direction), deletions.precedingStateCursor(direction));
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
        deletions.assertFresh();
        this.deletions = deletions;
        this.deletionsDepthCorrection = Cursor.depthCorrectionValue(data.encodedPosition());
        this.atDeletions = true;
    }

    public boolean hasDeletions()
    {
        return deletions != null;
    }

    public static <T, D extends RangeState<D>, E extends RangeState<E>>
    DeletionAwareMergeSource<T, D, E> empty(Direction direction, ByteComparable.Version version)
    {
        return new DeletionAwareMergeSource<T, D, E>(null, new DeletionAwareCursor.Empty<>(direction, version));
    }
}
