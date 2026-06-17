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

import java.util.Collection;
import java.util.function.ToIntFunction;

import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Composite response tracker: broadcasts responses to all child trackers and succeeds when at least
 * `blockFor` children are individually successful, or fails when `failures > count - blockFor`
 *
 * <ul>
 *   <li>{@code blockFor == children.length} gives AND semantics (all must succeed)</li>
 *   <li>{@code blockFor == quorum(children.length)} gives majority-quorum semantics</li>
 * </ul>
 */
public class CompositeTracker implements ResponseTracker
{
    private final ResponseTracker[] children;
    private final int blockFor;

    public CompositeTracker(int blockFor, ResponseTracker... children)
    {
        if (children == null || children.length == 0)
            throw new IllegalArgumentException("children cannot be null or empty");
        if (blockFor < 1 || blockFor > children.length)
            throw new IllegalArgumentException("blockFor must be between 1 and " + children.length);

        this.children = children;
        this.blockFor = blockFor;
    }

    public CompositeTracker(int blockFor, Collection<ResponseTracker> children)
    {
        this(blockFor, children.toArray(ResponseTracker[]::new));
    }

    public static int quorum(int count)
    {
        return (count / 2) + 1;
    }

    @Override
    public boolean isSuccessful()
    {
        int successful = 0;
        for (ResponseTracker child : children)
            if (child.isSuccessful())
                successful++;
        return successful >= blockFor;
    }

    @Override
    public void onResponse(InetAddressAndPort from)
    {
        for (ResponseTracker child : children)
            child.onResponse(from);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        for (ResponseTracker child : children)
            child.onFailure(from, reason);
    }

    @Override
    public boolean isComplete()
    {
        if (isSuccessful())
            return true;

        // Count children that have definitively failed
        int failed = 0;
        for (ResponseTracker child : children)
            if (child.isComplete() && !child.isSuccessful())
                failed++;
        int maxPossible = children.length - failed;
        return maxPossible < blockFor;
    }

    @Override
    public int required()
    {
        return count(ResponseTracker::required);
    }

    @Override
    public int received()
    {
        return count(ResponseTracker::received);
    }

    @Override
    public int failures()
    {
        return count(ResponseTracker::failures);
    }

    @Override
    public boolean countsTowardQuorum(InetAddressAndPort from)
    {
        for (ResponseTracker child : children)
            if (child.countsTowardQuorum(from))
                return true;
        return false;
    }

    @Override
    public boolean isPending(InetAddressAndPort from)
    {
        for (ResponseTracker child : children)
            if (child.isPending(from))
                return true;
        return false;
    }

    @Override
    public int totalContacts()
    {
        return count(ResponseTracker::totalContacts);
    }

    @Override
    public int pendingContacts()
    {
        return count(ResponseTracker::pendingContacts);
    }

    @Override
    public String toString()
    {
        return String.format("CompositeTracker[children=%d, blockFor=%d]", children.length, blockFor);
    }

    private int count(ToIntFunction<ResponseTracker> function)
    {
        int total = 0;
        for (ResponseTracker child : children)
            total += function.applyAsInt(child);
        return total;
    }

    @Override
    public ResponseTracker resetCopy()
    {
        ResponseTracker[] resetTrackers = new ResponseTracker[children.length];
        for (int i=0; i<children.length; i++)
            resetTrackers[i] = children[i].resetCopy();
        return new CompositeTracker(blockFor, resetTrackers);
    }
}
