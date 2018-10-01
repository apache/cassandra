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

package org.apache.cassandra.metrics;

import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAdder;


/**
 * A high performance, thread safe, exponentially moving average of some input. Note that unlike the codahale
 * EWMA classes etc this class does not care about being accurate over any given timescale
 * (e.g. 5 minutes, 10 minutes etc), it _just_ keeps an Exponential Moving Average of an input
 * datastream as a way of reducing noise.
 *
 * We care only about performance, nothing else
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class ExponentialMovingAverage
{
    protected volatile DoubleAccumulator avg = null;

    public ExponentialMovingAverage(double alpha, double initialValue)
    {
        updateParameter(alpha, initialValue);
    }

    public void updateParameter(double alpha, double initialValue)
    {
        avg = new DoubleAccumulator((prev, datum) -> alpha * datum + (1 - alpha) * prev, initialValue);
    }

    public void update(double datum)
    {
        avg.accumulate(datum);
    }

    public double getAvg()
    {
        return avg.get();
    }

    public void reset()
    {
        avg.reset();
    }
}