package org.apache.cassandra.dht;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractBounds<T extends RingPosition> implements Serializable
{
    private static final long serialVersionUID = 1L;
    private static AbstractBoundsSerializer serializer = new AbstractBoundsSerializer();

    public static AbstractBoundsSerializer serializer()
    {
        return serializer;
    }

    private enum Type
    {
        RANGE,
        BOUNDS
    }

    public final T left;
    public final T right;

    protected transient final IPartitioner partitioner;

    public AbstractBounds(T left, T right, IPartitioner partitioner)
    {
        this.left = left;
        this.right = right;
        this.partitioner = partitioner;
    }

    /**
     * Given token T and AbstractBounds ?L,R], returns Pair(?L,T], ]T,R])
     * (where ? means that the same type of Bounds is returned -- Range or Bounds -- as the original.)
     * The original AbstractBounds must contain the token T.
     * If the split would cause one of the left or right side to be empty, it will be null in the result pair.
     */
    public Pair<AbstractBounds<T>,AbstractBounds<T>> split(T pos)
    {
        assert left.equals(pos) || contains(pos);
        AbstractBounds<T> lb = createFrom(pos);
        // we contain this token, so only one of the left or right can be empty
        AbstractBounds<T> rb = lb != null && pos.equals(right) ? null : new Range<T>(pos, right);
        return new Pair<AbstractBounds<T>,AbstractBounds<T>>(lb, rb);
    }

    @Override
    public int hashCode()
    {
        return 31 * left.hashCode() + right.hashCode();
    }

    public abstract boolean contains(T start);

    /** @return A clone of this AbstractBounds with a new right T, or null if an identical range would be created. */
    public abstract AbstractBounds<T> createFrom(T right);

    public abstract List<? extends AbstractBounds<T>> unwrap();

    /**
     * @return A copy of the given list of with all bounds unwrapped, sorted by bound.left and with overlapping bounds merged.
     * This method does not allow allow to mix Range and Bound in the input list.
     */
    public static <T extends RingPosition> List<AbstractBounds<T>> normalize(Collection<? extends AbstractBounds<T>> bounds)
    {
        // unwrap all
        List<AbstractBounds<T>> output = new ArrayList<AbstractBounds<T>>();
        for (AbstractBounds<T> bound : bounds)
            output.addAll(bound.unwrap());

        // sort by left
        Collections.sort(output, new Comparator<AbstractBounds<T>>()
        {
            public int compare(AbstractBounds<T> b1, AbstractBounds<T> b2)
            {
                return b1.left.compareTo(b2.left);
            }
        });

        // deoverlap
        return deoverlap(output);
    }

    /**
     * Given a list of unwrapped bounds sorted by left token, return a list a equivalent
     * list of bounds but with no overlapping bounds.
     */
    private static <T extends RingPosition> List<AbstractBounds<T>> deoverlap(List<AbstractBounds<T>> bounds)
    {
        if (bounds.isEmpty())
            return bounds;

        List<AbstractBounds<T>> output = new ArrayList<AbstractBounds<T>>();

        Iterator<AbstractBounds<T>> iter = bounds.iterator();
        AbstractBounds<T> current = iter.next();
        boolean isRange = current instanceof Range;

        T min = (T) current.partitioner.minValue(current.left.getClass());
        while (iter.hasNext())
        {
            if (current.right.equals(min))
            {
                // If one of the bound is the full range, we return only that
                if (current.left.equals(min))
                    return Collections.<AbstractBounds<T>>singletonList(current);

                output.add(current.createFrom(min));
                return output;
            }

            AbstractBounds<T> next = iter.next();
            assert isRange ? next instanceof Range : next instanceof Bounds;

            // For Ranges, if next left is equal to current right, we do not intersect per se, but replacing (A, B] and (B, C] by (A, C] is
            // legit, and since this actually avoid special casing and will result in more "optimal" ranges, we do this transformation
            if (next.left.compareTo(current.right) <= 0)
            {
                // We do overlap
                // (we've handler current.right.equals(min) already)
                T newRight = next.right.equals(min) || current.right.compareTo(next.right) < 0 ? next.right : current.right;
                current = current.createFrom(newRight);
                if (current == null)
                    // current is the full ring, can only happen for Range
                    return Collections.<AbstractBounds<T>>singletonList(new Range<T>(min, min));
            }
            else
            {
                output.add(current);
                current = next;
            }
        }
        output.add(current);
        return output;
    }

    /**
     * Transform this abstract bounds to equivalent covering bounds of row positions.
     * If this abstract bounds was already an abstractBounds of row positions, this is a noop.
     */
    public abstract AbstractBounds<RowPosition> toRowBounds();

    /**
     * Transform this abstract bounds to a token abstract bounds.
     * If this abstract bounds was already an abstractBounds of token, this is a noop, otherwise this use the row position tokens.
     */
    public abstract AbstractBounds<Token> toTokenBounds();

    public static class AbstractBoundsSerializer implements IVersionedSerializer<AbstractBounds<?>>
    {
        public void serialize(AbstractBounds<?> range, DataOutput out, int version) throws IOException
        {
            // Older version don't know how to handle abstract bounds of keys
            // However, the serialization has been designed so that token bounds are serialized the same way that before 1.1
            if (version < MessagingService.VERSION_11)
                range = range.toTokenBounds();

            /*
             * The first int tells us if it's a range or bounds (depending on the value) _and_ if it's tokens or keys (depending on the
             * sign). We use negative kind for keys so as to preserve the serialization of token from older version.
             */
            boolean isToken = range.left instanceof Token;
            int kind = range instanceof Range ? Type.RANGE.ordinal() : Type.BOUNDS.ordinal();
            if (!isToken)
                kind = -(kind+1);
            out.writeInt(kind);
            if (isToken)
            {
                Token.serializer().serialize((Token)range.left, out);
                Token.serializer().serialize((Token)range.right, out);
            }
            else
            {
                RowPosition.serializer().serialize((RowPosition)range.left, out);
                RowPosition.serializer().serialize((RowPosition)range.right, out);
            }
        }

        public AbstractBounds<?> deserialize(DataInput in, int version) throws IOException
        {
            int kind = in.readInt();
            boolean isToken = kind >= 0;
            if (!isToken)
                kind = -(kind+1);

            RingPosition left, right;
            if (isToken)
            {
                left = Token.serializer().deserialize(in);
                right = Token.serializer().deserialize(in);
            }
            else
            {
                left = RowPosition.serializer().deserialize(in);
                right = RowPosition.serializer().deserialize(in);
            }

            if (kind == Type.RANGE.ordinal())
                return new Range(left, right);
            return new Bounds(left, right);
        }

        public long serializedSize(AbstractBounds<?> abstractBounds, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
