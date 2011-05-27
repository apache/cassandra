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

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractBounds implements Serializable
{
    private static final long serialVersionUID = 1L;
    private static AbstractBoundsSerializer serializer = new AbstractBoundsSerializer();

    public static ICompactSerializer2<AbstractBounds> serializer()
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
     * @return A copy of the given list of non-intersecting bounds with all bounds unwrapped, sorted by bound.left.
     * This method does not allow overlapping ranges as input.
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
        return output;
    }

    private static class AbstractBoundsSerializer implements ICompactSerializer2<AbstractBounds>
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
    }
}

