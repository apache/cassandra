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

package org.apache.cassandra.stress.generate;

import java.util.concurrent.atomic.AtomicLong;

public class DistributionSequence extends Distribution
{

    private final long start;
    private final long totalCount;
    private final AtomicLong next = new AtomicLong();

    public DistributionSequence(long start, long end)
    {
        if (start > end)
            throw new IllegalStateException();
        this.start = start;
        this.totalCount = 1 + end - start;
    }

    private long nextWithWrap()
    {
        long next = this.next.getAndIncrement();
        return start + (next % totalCount);
    }

    @Override
    public long next()
    {
        return nextWithWrap();
    }

    @Override
    public double nextDouble()
    {
        return nextWithWrap();
    }

    @Override
    public long inverseCumProb(double cumProb)
    {
        return (long) (start + (totalCount-1) * cumProb);
    }

    @Override
    public void setSeed(long seed)
    {
        next.set(seed);
    }

}

