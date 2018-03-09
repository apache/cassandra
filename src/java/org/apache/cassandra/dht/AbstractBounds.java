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
package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractBounds<T extends RingPosition<T>> implements Serializable
{
    private static final long serialVersionUID = 1L;
    public static final IPartitionerDependentSerializer<AbstractBounds<Token>> tokenSerializer =
            new AbstractBoundsSerializer<Token>(Token.serializer);
    public static final IPartitionerDependentSerializer<AbstractBounds<PartitionPosition>> rowPositionSerializer =
            new AbstractBoundsSerializer<PartitionPosition>(PartitionPosition.serializer);

    private enum Type
    {
        RANGE,
        BOUNDS
    }

    public final T left;
    public final T right;

    public AbstractBounds(T left, T right)
    {
        assert left.getPartitioner() == right.getPartitioner();
        this.left = left;
        this.right = right;
    }

    /**
     * Given token T and AbstractBounds ?L,R?, returns Pair(?L,T], (T,R?),
     * where ? means that the same type of AbstractBounds is returned as the original.
     *
     * Put another way, returns a Pair of everything this AbstractBounds contains
     * up to and including the split position, and everything it contains after
     * (not including the split position).
     *
     * The original AbstractBounds must either contain the position T, or T
     * should be equals to the left bound L.
     *
     * If the split would only yield the same AbstractBound, null is returned
     * instead.
     */
    public abstract Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position);
    public abstract boolean inclusiveLeft();
    public abstract boolean inclusiveRight();

    /**
     * Whether {@code left} and {@code right} forms a wrapping interval, that is if unwrapping wouldn't be a no-op.
     * <p>
     * Note that the semantic is slightly different from {@link Range#isWrapAround()} in the sense that if both
     * {@code right} are minimal (for the partitioner), this methods return false (doesn't wrap) while
     * {@link Range#isWrapAround()} returns true (does wrap). This is confusing and we should fix it by
     * refactoring/rewriting the whole AbstractBounds hierarchy with cleaner semantics, but we don't want to risk
     * breaking something by changing {@link Range#isWrapAround()} in the meantime.
     */
    public static <T extends RingPosition<T>> boolean strictlyWrapsAround(T left, T right)
    {
        return !(left.compareTo(right) <= 0 || right.isMinimum());
    }

    public static <T extends RingPosition<T>> boolean noneStrictlyWrapsAround(Collection<AbstractBounds<T>> bounds)
    {
        for (AbstractBounds<T> b : bounds)
        {
            if (strictlyWrapsAround(b.left, b.right))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        return 31 * left.hashCode() + right.hashCode();
    }

    /** return true if @param range intersects any of the given @param ranges */
    public boolean intersects(Iterable<Range<T>> ranges)
    {
        for (Range<T> range2 : ranges)
        {
            if (range2.intersects(this))
                return true;
        }
        return false;
    }

    public abstract boolean contains(T start);

    public abstract List<? extends AbstractBounds<T>> unwrap();

    public String getString(AbstractType<?> keyValidator)
    {
        return getOpeningString() + format(left, keyValidator) + ", " + format(right, keyValidator) + getClosingString();
    }

    private String format(T value, AbstractType<?> keyValidator)
    {
        if (value instanceof DecoratedKey)
        {
            return keyValidator.getString(((DecoratedKey)value).getKey());
        }
        else
        {
            return value.toString();
        }
    }

    protected abstract String getOpeningString();
    protected abstract String getClosingString();

    public abstract boolean isStartInclusive();
    public abstract boolean isEndInclusive();

    public abstract AbstractBounds<T> withNewRight(T newRight);

    public static class AbstractBoundsSerializer<T extends RingPosition<T>> implements IPartitionerDependentSerializer<AbstractBounds<T>>
    {
        private static final int IS_TOKEN_FLAG        = 0x01;
        private static final int START_INCLUSIVE_FLAG = 0x02;
        private static final int END_INCLUSIVE_FLAG   = 0x04;

        IPartitionerDependentSerializer<T> serializer;

        // Use for pre-3.0 protocol
        private static int kindInt(AbstractBounds<?> ab)
        {
            int kind = ab instanceof Range ? Type.RANGE.ordinal() : Type.BOUNDS.ordinal();
            if (!(ab.left instanceof Token))
                kind = -(kind + 1);
            return kind;
        }

        // For from 3.0 onwards
        private static int kindFlags(AbstractBounds<?> ab)
        {
            int flags = 0;
            if (ab.left instanceof Token)
                flags |= IS_TOKEN_FLAG;
            if (ab.isStartInclusive())
                flags |= START_INCLUSIVE_FLAG;
            if (ab.isEndInclusive())
                flags |= END_INCLUSIVE_FLAG;
            return flags;
        }

        public AbstractBoundsSerializer(IPartitionerDependentSerializer<T> serializer)
        {
            this.serializer = serializer;
        }

        public void serialize(AbstractBounds<T> range, DataOutputPlus out, int version) throws IOException
        {
            /*
             * The first int tells us if it's a range or bounds (depending on the value) _and_ if it's tokens or keys (depending on the
             * sign). We use negative kind for keys so as to preserve the serialization of token from older version.
             */
            // !WARNING! While we don't support the pre-3.0 messaging protocol, we serialize the token range in the
            // system table (see SystemKeypsace.rangeToBytes) using the old/pre-3.0 format and until we deal with that
            // problem, we have to preserve this code.
            if (version < MessagingService.VERSION_30)
                out.writeInt(kindInt(range));
            else
                out.writeByte(kindFlags(range));
            serializer.serialize(range.left, out, version);
            serializer.serialize(range.right, out, version);
        }

        public AbstractBounds<T> deserialize(DataInput in, IPartitioner p, int version) throws IOException
        {
            boolean isToken, startInclusive, endInclusive;
            // !WARNING! See serialize method above for why we still need to have that condition.
            if (version < MessagingService.VERSION_30)
            {
                int kind = in.readInt();
                isToken = kind >= 0;
                if (!isToken)
                    kind = -(kind+1);

                // Pre-3.0, everything that wasa not a Range was (wrongly) serialized as a Bound;
                startInclusive = kind != Type.RANGE.ordinal();
                endInclusive = true;
            }
            else
            {
                int flags = in.readUnsignedByte();
                isToken = (flags & IS_TOKEN_FLAG) != 0;
                startInclusive = (flags & START_INCLUSIVE_FLAG) != 0;
                endInclusive = (flags & END_INCLUSIVE_FLAG) != 0;
            }

            T left = serializer.deserialize(in, p, version);
            T right = serializer.deserialize(in, p, version);
            assert isToken == left instanceof Token;

            if (startInclusive)
                return endInclusive ? new Bounds<T>(left, right) : new IncludingExcludingBounds<T>(left, right);
            else
                return endInclusive ? new Range<T>(left, right) : new ExcludingBounds<T>(left, right);
        }

        public long serializedSize(AbstractBounds<T> ab, int version)
        {
            // !WARNING! See serialize method above for why we still need to have that condition.
            int size = version < MessagingService.VERSION_30
                     ? TypeSizes.sizeof(kindInt(ab))
                     : 1;
            size += serializer.serializedSize(ab.left, version);
            size += serializer.serializedSize(ab.right, version);
            return size;
        }
    }

    public static <T extends RingPosition<T>> AbstractBounds<T> bounds(Boundary<T> min, Boundary<T> max)
    {
        return bounds(min.boundary, min.inclusive, max.boundary, max.inclusive);
    }
    public static <T extends RingPosition<T>> AbstractBounds<T> bounds(T min, boolean inclusiveMin, T max, boolean inclusiveMax)
    {
        if (inclusiveMin && inclusiveMax)
            return new Bounds<T>(min, max);
        else if (inclusiveMax)
            return new Range<T>(min, max);
        else if (inclusiveMin)
            return new IncludingExcludingBounds<T>(min, max);
        else
            return new ExcludingBounds<T>(min, max);
    }

    // represents one side of a bounds (which side is not encoded)
    public static class Boundary<T extends RingPosition<T>>
    {
        public final T boundary;
        public final boolean inclusive;
        public Boundary(T boundary, boolean inclusive)
        {
            this.boundary = boundary;
            this.inclusive = inclusive;
        }
    }

    public Boundary<T> leftBoundary()
    {
        return new Boundary<>(left, inclusiveLeft());
    }

    public Boundary<T> rightBoundary()
    {
        return new Boundary<>(right, inclusiveRight());
    }

    public static <T extends RingPosition<T>> boolean isEmpty(Boundary<T> left, Boundary<T> right)
    {
        int c = left.boundary.compareTo(right.boundary);
        return c > 0 || (c == 0 && !(left.inclusive && right.inclusive));
    }

    public static <T extends RingPosition<T>> Boundary<T> minRight(Boundary<T> right1, T right2, boolean isInclusiveRight2)
    {
        return minRight(right1, new Boundary<T>(right2, isInclusiveRight2));
    }

    public static <T extends RingPosition<T>> Boundary<T> minRight(Boundary<T> right1, Boundary<T> right2)
    {
        int c = right1.boundary.compareTo(right2.boundary);
        if (c != 0)
            return c < 0 ? right1 : right2;
        // return the exclusive version, if either
        return right2.inclusive ? right1 : right2;
    }

    public static <T extends RingPosition<T>> Boundary<T> maxLeft(Boundary<T> left1, T left2, boolean isInclusiveLeft2)
    {
        return maxLeft(left1, new Boundary<T>(left2, isInclusiveLeft2));
    }

    public static <T extends RingPosition<T>> Boundary<T> maxLeft(Boundary<T> left1, Boundary<T> left2)
    {
        int c = left1.boundary.compareTo(left2.boundary);
        if (c != 0)
            return c > 0 ? left1 : left2;
        // return the exclusive version, if either
        return left2.inclusive ? left1 : left2;
    }
}
