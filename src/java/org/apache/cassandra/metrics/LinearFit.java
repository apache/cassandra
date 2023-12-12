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

import org.apache.cassandra.utils.Pair;

public class LinearFit
{
    /**
     * Computes the intercept and slope for the best linear fit to the given values.
     */
    public static Pair<Double, Double> interceptSlopeFor(PairedSlidingWindowReservoir.IntIntPair[] values)
    {
        double xSum = 0;
        double ySum = 0;
        for (var pair : values)
        {
            xSum += pair.x;
            ySum += pair.y;
        }
        double xMean = xSum / values.length;
        double yMean = ySum / values.length;

        double covariance = 0;
        double variance = 0;
        for (var pair : values)
        {
            double dX = pair.x - xMean;
            double dY = pair.y - yMean;
            covariance += dX * dY;
            variance += dX * dX;
        }

        double slope = covariance / variance;
        double intercept = yMean - slope * xMean;
        return Pair.create(intercept, slope);
    }
}
