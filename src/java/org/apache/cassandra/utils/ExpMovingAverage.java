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

package org.apache.cassandra.utils;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * Sample-based exponential moving average. On every update a fraction of the current average is replaced by the new
 * sample. New values have greater representation in the average, and older samples' effect exponentially decays with
 * new data.
 */
public class ExpMovingAverage implements MovingAverage
{
    /** The ratio of decay, between 0 and 1, where smaller alpha means values are averaged over more samples */
    private final double alpha;

    /** The long term average with exponential decay */
    private final AtomicDouble average = new AtomicDouble(Double.NaN);

    /**
     * Create a {@link ExpMovingAverage} where older values have less than 1% effect after 1000 samples.
     */
    public static MovingAverage decayBy1000()
    {
        return new ExpMovingAverage(0.0046);
    }

    /**
     * Create a {@link ExpMovingAverage} where older values have less than 1% effect after 100 samples.
     */
    public static ExpMovingAverage decayBy100()
    {
        return new ExpMovingAverage(0.045);
    }

    /**
     * Create a {@link ExpMovingAverage} where older values have less than 1% effect after 10 samples.
     */
    public static ExpMovingAverage decayBy10()
    {
        return new ExpMovingAverage(0.37);
    }

    /**
     * Create a {@link ExpMovingAverage} where older values have less effect than the given ratio after the given
     * number of samples.
     */
    public static ExpMovingAverage withDecay(double ratio, int samples)
    {
        assert ratio > 0.0 && ratio < 1.0;
        assert samples > 0;
        return new ExpMovingAverage(1 - Math.pow(ratio, 1.0 / samples));
    }

    ExpMovingAverage(double alpha)
    {
        assert alpha > 0.0 && alpha <= 1.0;
        this.alpha = alpha;
    }

    @Override
    public MovingAverage update(double val)
    {
        double current, update;
        do
        {
            current = average.get();

            if (!Double.isNaN(current))
                update = current + alpha * (val - current);
            else
                update = val;   // Not initialized yet. Incidentally, passing NaN will cause reinitialization on the
                                // next update.
        }
        while (!average.compareAndSet(current, update));

        return this;
    }

    @Override
    public double get()
    {
        return average.get();
    }

    @Override
    public String toString()
    {
        return String.format("%.2f", get());
    }
}
