package org.apache.cassandra.stress.generate;
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


import org.apache.commons.math3.distribution.AbstractRealDistribution;

public class DistributionBoundApache extends Distribution
{

    final AbstractRealDistribution delegate;
    final long min, max;

    public DistributionBoundApache(AbstractRealDistribution delegate, long min, long max)
    {
        this.delegate = delegate;
        this.min = min;
        this.max = max;
    }

    @Override
    public long next()
    {
        return bound(min, max, delegate.sample());
    }

    public double nextDouble()
    {
        return boundDouble(min, max, delegate.sample());
    }

    @Override
    public long inverseCumProb(double cumProb)
    {
        return bound(min, max, delegate.inverseCumulativeProbability(cumProb));
    }

    public void setSeed(long seed)
    {
        delegate.reseedRandomGenerator(seed);
    }

    private static long bound(long min, long max, double val)
    {
        long r = (long) val;
        if ((r >= min) && (r <= max))
            return r;
        if (r < min)
            return min;
        if (r > max)
            return max;
        throw new IllegalStateException();
    }

    private static double boundDouble(long min, long max, double r)
    {
        if ((r >= min) && (r <= max))
            return r;
        if (r < min)
            return min;
        if (r > max)
            return max;
        throw new IllegalStateException();
    }

}
