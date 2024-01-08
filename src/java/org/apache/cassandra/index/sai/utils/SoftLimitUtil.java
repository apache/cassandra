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

package org.apache.cassandra.index.sai.utils;

import org.apache.commons.math3.distribution.PascalDistribution;

public class SoftLimitUtil
{
    /**
     * Computes the number of items (e.g. keys, rows) that should be requested from a lower-layer of the system
     * (e.g. storage) so that we obtain at least targetLimit number of items with given probability.
     * It assumes that each item may randomly fail, in which case it is not delivered, thus the number of items
     * delivered may be smaller than the number of items requested. Items are assumed to fail independently.
     * <p>
     * For example, if we want to deliver 100 rows to the user, but we know 20% of rows are tombstoned and would
     * be rejected, then we should request `softLimit(100, 0.95, 0.8)` rows from the storage, and that would deliver
     * in 95% of cases a sufficient number of rows, without having to query again for more.
     *
     * @param targetLimit the number of items that should be delivered to the user or upper layer in the system
     * @param confidenceLevel the desired probability we obtain enough items in range, given in range [0.0, 1.0),
     *                        typically you want to set it close to 1.0.
     * @param perItemSuccessRate the probability of obtaining an item, given in range [0.0, 1.0]
     * @return the number of items that should be requested from the lower layer of the system; >= targetLimit;
     *    if the true result is greater than Integer.MAX_VALUE it is clamped to Integer.MAX_VALUE
     */
    public static int softLimit(int targetLimit, double confidenceLevel, double perItemSuccessRate)
    {
        if (Double.isNaN(confidenceLevel))
            throw new IllegalArgumentException("confidenceLevel must not be NaN");
        if (confidenceLevel < 0.0 || confidenceLevel >= 1.0)
            throw new IllegalArgumentException("confidenceLevel out of range [0.0, 1.0): " + confidenceLevel);
        if (Double.isNaN(perItemSuccessRate))
            throw new IllegalArgumentException("perItemSuccessRate must not be NaN");
        if (perItemSuccessRate < 0.0 || perItemSuccessRate > 1.0)
            throw new IllegalArgumentException("perItemSuccessRate out of range [0.0, 1.0]: " + perItemSuccessRate);
        if (targetLimit < 0)
            throw new IllegalArgumentException("targetLimit must not be < 0: " + targetLimit);

        // PascalDistribution (see further) cannot handle this case properly
        if (targetLimit == 0)
            return 0;

        // Consider we perform attempts until we get R successes (=targetLimit), where the probability of success is
        // P (=perItemSuccessRate). In this case the number of failures is described by a negative binomial
        // distribution NB(R, P). We use PascalDistribution, which is an optimized special case
        // of NB for dealing with integers.
        final var failureDistrib = new PascalDistribution(targetLimit, perItemSuccessRate);
        long maxExpectedFailures = failureDistrib.inverseCumulativeProbability(confidenceLevel);

        long softLimit = (long) targetLimit + maxExpectedFailures;
        return (int) Math.min(softLimit, Integer.MAX_VALUE);
    }

}
