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

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractBounds implements Serializable
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

    public final Token left;
    public final Token right;

    protected transient final IPartitioner partitioner;

    public AbstractBounds(Token left, Token right, IPartitioner partitioner)
    {
        this.left = left;
        this.right = right;
        this.partitioner = partitioner;
    }

    /**
     * Given token T and AbstractBounds ?L,R], returns Pair(?L,T], ?T,R])
     * (where ? means that the same type of Bounds is returned -- Range or Bounds -- as the original.)
     * The original AbstractBounds must contain the token T.
     * If the split would cause one of the left or right side to be empty, it will be null in the result pair.
     */
    public Pair<AbstractBounds,AbstractBounds> split(Token token)
    {
        assert left.equals(token) || contains(token);
        AbstractBounds lb = createFrom(token);
        // we contain this token, so only one of the left or right can be empty
        AbstractBounds rb = lb != null && token.equals(right) ? null : new Range(token, right);
        return new Pair<AbstractBounds,AbstractBounds>(lb, rb);
    }

    @Override
    public int hashCode()
    {
        return 31 * left.hashCode() + right.hashCode();
    }

    public abstract boolean equals(Object obj);

    public abstract boolean contains(Token start);

    /** @return A clone of this AbstractBounds with a new right Token, or null if an identical range would be created. */
    public abstract AbstractBounds createFrom(Token right);

    public abstract List<AbstractBounds> unwrap();

    /**
     * @return A copy of the given list of with all bounds unwrapped, sorted by bound.left and with overlapping bounds merged.
     * This method does not allow allow to mix Range and Bound in the input list.
     */
    public static List<AbstractBounds> normalize(Collection<? extends AbstractBounds> bounds)
    {
        // unwrap all
        List<AbstractBounds> output = new ArrayList<AbstractBounds>();
        for (AbstractBounds bound : bounds)
            output.addAll(bound.unwrap());

        // sort by left
        Collections.sort(output, new Comparator<AbstractBounds>()
        {
            public int compare(AbstractBounds b1, AbstractBounds b2)
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
    private static List<AbstractBounds> deoverlap(List<AbstractBounds> bounds)
    {
        if (bounds.isEmpty())
            return bounds;

        List<AbstractBounds> output = new ArrayList<AbstractBounds>();

        Iterator<AbstractBounds> iter = bounds.iterator();
        AbstractBounds current = iter.next();
        boolean isRange = current instanceof Range;

        Token min = current.partitioner.getMinimumToken();
        while (iter.hasNext())
        {
            if (current.right.equals(min))
            {
                // If one of the bound is the full range, we return only that
                if (current.left.equals(min))
                    return Collections.<AbstractBounds>singletonList(current);

                output.add(current.createFrom(min));
                return output;
            }

            AbstractBounds next = iter.next();
            assert isRange ? next instanceof Range : next instanceof Bounds;

            // For Ranges, if next left is equal to current right, we do not intersect per se, but replacing (A, B] and (B, C] by (A, C] is
            // legit, and since this actually avoid special casing and will result in more "optimal" ranges, we do this transformation
            if (next.left.compareTo(current.right) <= 0)
            {
                // We do overlap
                // (we've handler current.right.equals(min) already)
                Token newRight = next.right.equals(min) || current.right.compareTo(next.right) < 0 ? next.right : current.right;
                current = current.createFrom(newRight);
                if (current == null)
                    // current is the full ring, can only happen for Range
                    return Collections.<AbstractBounds>singletonList(new Range(min, min));
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

    public static class AbstractBoundsSerializer implements ISerializer<AbstractBounds>
    {
        public void serialize(AbstractBounds range, DataOutput out) throws IOException
        {
            out.writeInt(range instanceof Range ? Type.RANGE.ordinal() : Type.BOUNDS.ordinal());
            Token.serializer().serialize(range.left, out);
            Token.serializer().serialize(range.right, out);
        }

        public AbstractBounds deserialize(DataInput in) throws IOException
        {
            if (in.readInt() == Type.RANGE.ordinal())
                return new Range(Token.serializer().deserialize(in), Token.serializer().deserialize(in));
            return new Bounds(Token.serializer().deserialize(in), Token.serializer().deserialize(in));
        }

        public long serializedSize(AbstractBounds abstractBounds)
        {
            throw new UnsupportedOperationException();
        }
    }
}

