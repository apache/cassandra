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

public class DistributionOffsetApache extends Distribution
{

    final AbstractRealDistribution delegate;
    final long min, delta;

    public DistributionOffsetApache(AbstractRealDistribution delegate, long min, long max)
    {
        this.delegate = delegate;
        this.min = min;
        this.delta = max - min;
    }

    public void setSeed(long seed)
    {
        delegate.reseedRandomGenerator(seed);
    }

    @Override
    public long next()
    {
        return offset(min, delta, delegate.sample());
    }

    public double nextDouble()
    {
        return offsetDouble(min, delta, delegate.sample());
    }

    @Override
    public long inverseCumProb(double cumProb)
    {
        return offset(min, delta, delegate.inverseCumulativeProbability(cumProb));
    }

    private long offset(long min, long delta, double val)
    {
        long r = (long) val;
        if (r < 0)
            r = 0;
        if (r > delta)
            r = delta;
        return min + r;
    }

    private double offsetDouble(long min, long delta, double r)
    {
        if (r < 0)
            r = 0;
        if (r > delta)
            r = delta;
        return min + r;
    }

}
