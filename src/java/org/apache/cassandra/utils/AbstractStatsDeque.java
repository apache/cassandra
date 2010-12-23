/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.util.Iterator;

public abstract class AbstractStatsDeque implements Iterable<Double>
{
    public abstract Iterator<Double> iterator();
    public abstract int size();
    public abstract void add(double o);
    public abstract void clear();

    //
    // statistical methods
    //

    public double sum()
    {
        double sum = 0d;
        for (Double interval : this)
        {
            sum += interval;
        }
        return sum;
    }

    public double sumOfDeviations()
    {
        double sumOfDeviations = 0d;
        double mean = mean();

        for (Double interval : this)
        {
            double v = interval - mean;
            sumOfDeviations += v * v;
        }

        return sumOfDeviations;
    }

    public double mean()
    {
        return sum() / size();
    }

    public double variance()
    {
        return sumOfDeviations() / size();
    }

    public double stdev()
    {
        return Math.sqrt(variance());
    }
}
