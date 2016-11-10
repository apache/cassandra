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


import java.util.Arrays;
import java.util.Random;

import org.apache.cassandra.stress.Stress;

public class DistributionQuantized extends Distribution
{

    final Distribution delegate;
    final long[] bounds;
    final Random random = new Random();

    public DistributionQuantized(Distribution delegate, int quantas)
    {
        this.delegate = delegate;
        this.bounds = new long[quantas + 1];
        bounds[0] = delegate.minValue();
        bounds[quantas] = delegate.maxValue() + 1;
        for (int i = 1 ; i < quantas ; i++)
            bounds[i] = delegate.inverseCumProb(i / (double) quantas);
    }

    @Override
    public long next()
    {
        int quanta = quanta(delegate.next());
        return bounds[quanta] + (long) (random.nextDouble() * ((bounds[quanta + 1] - bounds[quanta])));
    }

    public double nextDouble()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long inverseCumProb(double cumProb)
    {
        long val = delegate.inverseCumProb(cumProb);
        int quanta = quanta(val);
        if (quanta < 0)
            return bounds[0];
        if (quanta >= bounds.length - 1)
            return bounds[bounds.length - 1] - 1;
        cumProb -= (quanta / ((double) bounds.length - 1));
        cumProb *= (double) bounds.length - 1;
        return bounds[quanta] + (long) (cumProb * (bounds[quanta + 1] - bounds[quanta]));
    }

    int quanta(long val)
    {
        int i = Arrays.binarySearch(bounds, val);
        if (i < 0)
            return -2 -i;
        return i - 1;
    }

    public void setSeed(long seed)
    {
        delegate.setSeed(seed);
    }

    public static void main(String[] args) throws Exception
    {
        Stress.main(new String[] { "print", "dist=qextreme(1..1M,2,2)"});
    }

}
