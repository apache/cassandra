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
 * AbstractBounds containing neither of its endpoints: (left, right).  Used by CQL key > X AND key < Y range scans.
 */
public class ExcludingBounds<T extends RingPosition<T>> extends AbstractBounds<T>
{
    public ExcludingBounds(T left, T right)
    {
        super(left, right);
        // unlike a Range, an ExcludingBounds may not wrap, nor be empty
        assert left.compareTo(right) < 0 || right.isMinimum() : "(" + left + "," + right + ")";
    }

    public boolean contains(T position)
    {
        return Range.contains(left, right, position) && !right.equals(position);
    }

    public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position)
    {
        assert contains(position) || left.equals(position);
        if (left.equals(position))
            return null;
        AbstractBounds<T> lb = new Range<T>(left, position);
        AbstractBounds<T> rb = new ExcludingBounds<T>(position, right);
        return Pair.create(lb, rb);
    }

    public boolean inclusiveLeft()
    {
        return false;
    }

    public boolean inclusiveRight()
    {
        return false;
    }

    public List<? extends AbstractBounds<T>> unwrap()
    {
        // ExcludingBounds objects never wrap
        return Collections.<AbstractBounds<T>>singletonList(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ExcludingBounds))
            return false;
        ExcludingBounds<T> rhs = (ExcludingBounds<T>)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public String toString()
    {
        return "(" + left + "," + right + ")";
    }

    protected String getOpeningString()
    {
        return "(";
    }

    protected String getClosingString()
    {
        return ")";
    }

    public AbstractBounds<T> withNewRight(T newRight)
    {
        return new ExcludingBounds<T>(left, newRight);
    }
}
