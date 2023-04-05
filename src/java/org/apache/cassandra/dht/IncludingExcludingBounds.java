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

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.utils.Pair;

/**
 * AbstractBounds containing only its left endpoint: [left, right).  Used by {@code CQL key >= X AND key < Y} range scans.
 */
public class IncludingExcludingBounds<T extends RingPosition<T>> extends AbstractBounds<T>
{
    public IncludingExcludingBounds(T left, T right)
    {
        super(left, right);
        // unlike a Range, an IncludingExcludingBounds may not wrap, nor have
        // right == left unless the right is the min token
        assert !strictlyWrapsAround(left, right) && (right.isMinimum() || left.compareTo(right) != 0) : "(" + left + "," + right + ")";
    }

    public boolean contains(T position)
    {
        return (Range.contains(left, right, position) || left.equals(position)) && !right.equals(position);
    }

    public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position)
    {
        assert contains(position);
        AbstractBounds<T> lb = new Bounds<T>(left, position);
        AbstractBounds<T> rb = new ExcludingBounds<T>(position, right);
        return Pair.create(lb, rb);
    }

    public boolean inclusiveLeft()
    {
        return true;
    }

    public boolean inclusiveRight()
    {
        return false;
    }

    public List<? extends AbstractBounds<T>> unwrap()
    {
        // IncludingExcludingBounds objects never wrap
        return Collections.<AbstractBounds<T>>singletonList(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof IncludingExcludingBounds))
            return false;
        IncludingExcludingBounds<?> rhs = (IncludingExcludingBounds<?>)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public String toString()
    {
        return "[" + left + "," + right + ")";
    }

    protected String getOpeningString()
    {
        return "[";
    }

    protected String getClosingString()
    {
        return ")";
    }

    public boolean isStartInclusive()
    {
        return true;
    }

    public boolean isEndInclusive()
    {
        return false;
    }

    public AbstractBounds<T> withNewRight(T newRight)
    {
        return new IncludingExcludingBounds<T>(left, newRight);
    }
}
