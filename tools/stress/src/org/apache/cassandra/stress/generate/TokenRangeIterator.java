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

package org.apache.cassandra.stress.generate;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.datastax.driver.core.TokenRange;
import org.apache.cassandra.stress.settings.StressSettings;

public class TokenRangeIterator
{
    private final Set<TokenRange> tokenRanges;
    private final ConcurrentLinkedQueue<TokenRange> pendingRanges;
    private final boolean wrap;

    public TokenRangeIterator(StressSettings settings, Set<TokenRange> tokenRanges)
    {
        this.tokenRanges = maybeSplitRanges(tokenRanges, settings.tokenRange.splitFactor);
        this.pendingRanges = new ConcurrentLinkedQueue<>(this.tokenRanges);
        this.wrap = settings.tokenRange.wrap;
    }

    private static Set<TokenRange> maybeSplitRanges(Set<TokenRange> tokenRanges, int splitFactor)
    {
        if (splitFactor <= 1)
            return tokenRanges;

        Set<TokenRange> ret = new TreeSet<>();
        for (TokenRange range : tokenRanges)
            ret.addAll(range.splitEvenly(splitFactor));

        return ret;
    }

    public void update()
    {
        // we may race and add to the queue twice but no bad consequence so it's fine if that happens
        // as ultimately only the permits determine when to stop if wrap is true
        if (wrap && pendingRanges.isEmpty())
            pendingRanges.addAll(tokenRanges);
    }

    public TokenRange next()
    {
        return pendingRanges.poll();
    }

    public boolean exhausted()
    {
        return pendingRanges.isEmpty();
    }
}
