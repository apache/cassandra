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
import java.util.Collection;
import java.util.Objects;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class DeletionMarker implements DataPoint, RangeState<DeletionMarker>
{
    final ByteComparable position;
    final int leftSide;
    final int rightSide;

    final DeletionMarker leftSideAsCovering;
    final DeletionMarker rightSideAsCovering;

    DeletionMarker(ByteComparable position, int leftSide, int rightSide)
    {
        this.position = position;
        this.leftSide = leftSide;
        this.rightSide = rightSide;

        if (leftSide == rightSide)
            leftSideAsCovering = rightSideAsCovering = this;
        else
        {
            if (this.leftSide < 0)
                leftSideAsCovering = null;
            else
                leftSideAsCovering = new DeletionMarker(this.position, this.leftSide, this.leftSide);

            if (this.rightSide < 0)
                rightSideAsCovering = null;
            else
                rightSideAsCovering = new DeletionMarker(this.position, this.rightSide, this.rightSide);
        }
    }

    static DeletionMarker combine(DeletionMarker m1, DeletionMarker m2)
    {
        return combineCollection(Arrays.asList(m1, m2));
    }


    public static DeletionMarker combineCollection(Collection<DeletionMarker> rangeMarkers)
    {
        int newLeft = -1;
        int newRight = -1;
        ByteComparable position = null;
        for (DeletionMarker marker : rangeMarkers)
        {
            newLeft = Math.max(newLeft, marker.leftSide);
            newRight = Math.max(newRight, marker.rightSide);
            position = marker.position;
        }
        if (newLeft < 0 && newRight < 0)
            return null;

        return new DeletionMarker(position, newLeft, newRight);
    }

    DeletionMarker[] withPoint(int value)
    {
        return new DeletionMarker[]
        {
            new DeletionMarker(position, leftSide, value),
            new DeletionMarker(replaceTerminator(position, ByteSource.GT_NEXT_COMPONENT), value, rightSide)
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
        return new DeletionMarker(newKey, leftSide, rightSide);
    }

    @Override
    public String toString()
    {

        return (leftSide >= 0 ? leftSide + "<" : "") +
               '"' + DataPoint.toString(position) + '"' +
               (rightSide >= 0 ? "<" + rightSide : "") +
               (isBoundary() ? "" : " not reportable");
    }

    @Override
    public boolean isBoundary()
    {
        return leftSide != rightSide;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletionMarker that = (DeletionMarker) o;
        return leftSide == that.leftSide
               && rightSide == that.rightSide;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(position, leftSide, rightSide);
    }

    @Override
    public DeletionMarker toContent()
    {
        return isBoundary() ? this : null;
    }

    @Override
    public DeletionMarker restrict(boolean applicableBefore, boolean applicableAfter)
    {
        assert isBoundary();
        if ((applicableBefore || leftSide < 0) && (applicableAfter || rightSide < 0))
            return this;
        int newLeft = applicableBefore ? leftSide : -1;
        int newRight = applicableAfter ? rightSide : -1;
        if (newLeft >= 0 || newRight >= 0)
            return new DeletionMarker(position, newLeft, newRight);
        else
            return null;
    }

    @Override
    public DeletionMarker precedingState(Direction direction)
    {
        return direction.select(leftSideAsCovering, rightSideAsCovering);
    }

    @Override
    public DeletionMarker succedingState(Direction direction)
    {
        return direction.select(rightSideAsCovering, leftSideAsCovering);
    }

    @Override
    public DeletionMarker asBoundary(Direction direction)
    {
        assert !isBoundary();
        final boolean isForward = direction.isForward();
        int newLeft = !isForward ? leftSide : -1;
        int newRight = isForward ? rightSide : -1;
        return new DeletionMarker(position, newLeft, newRight);
    }

    public LivePoint applyTo(LivePoint content)
    {
        return content.delete(rightSide);
    }
}
