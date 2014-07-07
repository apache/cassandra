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


import java.io.Serializable;

public abstract class Distribution implements Serializable
{

    public abstract long next();
    public abstract double nextDouble();
    public abstract long inverseCumProb(double cumProb);
    public abstract void setSeed(long seed);

    public long maxValue()
    {
        return inverseCumProb(1d);
    }

    public long minValue()
    {
        return inverseCumProb(0d);
    }

    // approximation of the average; slightly costly to calculate, so should not be invoked frequently
    public long average()
    {
        double sum = 0;
        int count = 0;
        for (float d = 0 ; d <= 1.0d ; d += 0.02d)
        {
            sum += inverseCumProb(d);
            count += 1;
        }
        return (long) (sum / count);
    }

}
