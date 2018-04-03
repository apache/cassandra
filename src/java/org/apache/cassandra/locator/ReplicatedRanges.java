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

package org.apache.cassandra.locator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class ReplicatedRanges
{
    public static Collection<Range<Token>> asRanges(Collection<ReplicatedRange> replicated)
    {
        return Collections2.transform(replicated, ReplicatedRange::getRange);
    }

    public static Collection<Range<Token>> fullReplicaRanges(Collection<ReplicatedRange> replicated)
    {
        return asRanges(Collections2.filter(replicated, ReplicatedRange::isFull));
    }

    public static Collection<ReplicatedRange> decorateRanges(Collection<Range<Token>> ranges, boolean full)
    {
        return Collections2.transform(ranges, r -> new ReplicatedRange(r.left, r.right, full));
    }

    public static List<ReplicatedRange> normalize(Collection<ReplicatedRange> ranges)
    {
        if (Iterables.all(ranges, ReplicatedRange::isFull))
        {
            Preconditions.checkArgument(Iterables.all(ranges, ReplicatedRange::isFull));
            List<Range<Token>> normalized = Range.normalize(asRanges(ranges));
            List<ReplicatedRange> result = new ArrayList<>(normalized.size());
            result.addAll(decorateRanges(normalized, true));
            return result;
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static Set<ReplicatedRange> subtractAll(ReplicatedRange range, Collection<ReplicatedRange> toSubtract)
    {
        if (range.isFull() && Iterables.all(toSubtract, ReplicatedRange::isFull))
        {
            return Sets.newHashSet(decorateRanges(range.getRange().subtractAll(asRanges(toSubtract)), true));
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static boolean intersects(ReplicatedRange lhs, ReplicatedRange rhs)
    {
        if (lhs.isFull() && rhs.isFull())
        {
            return lhs.getRange().intersects(rhs.getRange());
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(ReplicatedRange range)
    {
        if (!range.isFull())
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(Iterable<ReplicatedRange> ranges)
    {
        if (!Iterables.all(ranges, ReplicatedRange::isFull))
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }
}
