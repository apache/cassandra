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


import java.util.*;

import org.apache.cassandra.service.StorageService;

public class Bounds extends AbstractBounds
{
    public Bounds(Token left, Token right)
    {
        this(left, right, StorageService.getPartitioner());
    }

    Bounds(Token left, Token right, IPartitioner partitioner)
    {
        super(left, right, partitioner);
        // unlike a Range, a Bounds may not wrap
        assert left.compareTo(right) <= 0 || right.equals(partitioner.getMinimumToken()) : "[" + left + "," + right + "]";
    }

    @Override
    public boolean contains(Token token)
    {
        return Range.contains(left, right, token) || left.equals(token);
    }

    public Set<AbstractBounds> restrictTo(Range range)
    {
        Token min = partitioner.getMinimumToken();

        // special case Bounds where left=right (single Token)
        if (this.left.equals(this.right) && !this.right.equals(min))
            return range.contains(this.left)
                   ? Collections.unmodifiableSet(new HashSet<AbstractBounds>(Arrays.asList(this)))
                   : Collections.<AbstractBounds>emptySet();

        // get the intersection of a Range w/ same left & right
        Set<Range> ranges = range.intersectionWith(new Range(this.left, this.right));
        // if range doesn't contain left token anyway, that's the correct answer
        if (!range.contains(this.left))
            return (Set) ranges;
        // otherwise, add back in the left token
        Set<AbstractBounds> S = new HashSet<AbstractBounds>(ranges.size());
        for (Range restricted : ranges)
        {
            if (restricted.left.equals(this.left))
                S.add(new Bounds(restricted.left, restricted.right));
            else
                S.add(restricted);
        }
        return Collections.unmodifiableSet(S);
    }

    public List<AbstractBounds> unwrap()
    {
        // Bounds objects never wrap
        return (List)Arrays.asList(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Bounds))
            return false;
        Bounds rhs = (Bounds)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    public String toString()
    {
        return "[" + left + "," + right + "]";
    }
}
