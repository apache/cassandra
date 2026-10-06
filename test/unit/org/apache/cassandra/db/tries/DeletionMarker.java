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

import java.util.Collection;
import java.util.Objects;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

class DeletionMarker implements DataPoint, RangeState<DeletionMarker>
{
    final ByteComparable position;
    final int leftSide;
    final int rightSide;

    final int at;
    final boolean isBoundary;

    final DeletionMarker leftSideAsCovering;
    final DeletionMarker rightSideAsCovering;

    DeletionMarker(ByteComparable position, int leftSide, int at, int rightSide)
    {
        this.position = position;
        this.leftSide = leftSide;
        this.rightSide = rightSide;
        this.at = at;
        this.isBoundary = at != leftSide || leftSide != rightSide;

        if (!isBoundary)
            leftSideAsCovering = rightSideAsCovering = this;
        else
        {
            if (this.leftSide < 0)
                leftSideAsCovering = null;
            else
                leftSideAsCovering = new DeletionMarker(this.position, this.leftSide, this.leftSide, this.leftSide);

            if (this.rightSide < 0)
                rightSideAsCovering = null;
            else
                rightSideAsCovering = new DeletionMarker(this.position, this.rightSide, this.rightSide, this.rightSide);
        }
    }

    static DeletionMarker combine(DeletionMarker m1, DeletionMarker m2)
    {
        int newLeft = Math.max(m1.leftSide, m2.leftSide);
        int newAt = Math.max(m1.at, m2.at);
        int newRight = Math.max(m1.rightSide, m2.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;

        return new DeletionMarker(m2.position, newLeft, newAt, newRight);
    }


    public static DeletionMarker combineCollection(Collection<DeletionMarker> rangeMarkers)
    {
        int newLeft = -1;
        int newAt = -1;
        int newRight = -1;
        ByteComparable position = null;
        for (DeletionMarker marker : rangeMarkers)
        {
            newLeft = Math.max(newLeft, marker.leftSide);
            newAt = Math.max(newAt, marker.at);
            newRight = Math.max(newRight, marker.rightSide);
            position = marker.position;
        }
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;

        return new DeletionMarker(position, newLeft, newAt, newRight);
    }

    DeletionMarker[] withPoint(int value)
    {
        return new DeletionMarker[]
        {
            new DeletionMarker(position, leftSide, value, value),
            new DeletionMarker(replaceTerminator(position, ByteSource.GT_NEXT_COMPONENT), value, rightSide, rightSide)
        };
    }

    ByteComparable replaceTerminator(ByteComparable c, int terminator)
    {
        byte[] key = c.asByteComparableArray(TrieUtil.VERSION);
        key[key.length - 1] = (byte) terminator;
        return ByteComparable.preencoded(TrieUtil.VERSION, key);
    }

    @Override
    public DeletionMarker marker()
    {
        return this;
    }

    @Override
    public LivePoint live()
    {
        return null;
    }

    @Override
    public ByteComparable position()
    {
        return position;
    }

    @Override
    public DeletionMarker withMarker(DeletionMarker newMarker)
    {
        return newMarker;
    }

    @Override
    public DeletionMarker remap(ByteComparable newKey)
    {
        return new DeletionMarker(newKey, leftSide, at, rightSide);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletionMarker that = (DeletionMarker) o;
        return leftSide == that.leftSide
               && rightSide == that.rightSide
               && at == that.at;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(position, leftSide, at, rightSide);
    }

    @Override
    public String toString()
    {
        boolean hasAt = at >= 0 && at != leftSide && at != rightSide;
        String left = leftSide != at ? "<" : "<=";
        String right = rightSide != at ? "<" : "<=";

        return (leftSide >= 0 ? leftSide + left : "") +
               '{' + DataPoint.toString(position) + '}' +
               (hasAt ? "=" + at : "") +
               (rightSide >= 0 ? right + rightSide : "");
    }

    @Override
    public DeletionMarker toContent()
    {
        return isBoundary ? this : null;
    }

    @Override
    public DeletionMarker restrict(boolean applicableBefore, boolean applicableAfter)
    {
        assert isBoundary;
        if ((applicableBefore || leftSide < 0) && (applicableAfter || rightSide < 0))
            return this;
        int newLeft = applicableBefore ? leftSide : -1;
        int newRight = applicableAfter ? rightSide : -1;
        int newAt = applicableAfter ? at : -1;
        if (newLeft >= 0 || newRight >= 0 || newAt >= 0)
            return new DeletionMarker(position, newLeft, newRight, newAt);
        else
            return null;
    }

    @Override
    public DeletionMarker precedingState(Direction direction)
    {
        return direction.select(leftSideAsCovering, rightSideAsCovering);
    }

    @Override
    public DeletionMarker asBoundary(Direction direction)
    {
        assert !isBoundary;
        final boolean isForward = direction.isForward();
        int newLeft = !isForward ? leftSide : -1;
        int newRight = isForward ? rightSide : -1;
        int newAt = isForward ? at : -1;
        return new DeletionMarker(position, newLeft, newRight, newAt);
    }

    @Override
    public boolean isBoundary()
    {
        return isBoundary;
    }

    public LivePoint applyTo(LivePoint content)
    {
        return content.delete(at);
    }
}
