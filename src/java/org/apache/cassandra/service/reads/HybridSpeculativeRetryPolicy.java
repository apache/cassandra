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

package org.apache.cassandra.service.reads;

import com.google.common.base.Objects;

import com.codahale.metrics.Timer;

public class HybridSpeculativeRetryPolicy implements SpeculativeRetryPolicy
{
    public enum Function
    {
        MIN, MAX
    }

    private final PercentileSpeculativeRetryPolicy percentilePolicy;
    private final FixedSpeculativeRetryPolicy fixedPolicy;
    private final Function function;

    public HybridSpeculativeRetryPolicy(PercentileSpeculativeRetryPolicy percentilePolicy,
                                        FixedSpeculativeRetryPolicy fixedPolicy,
                                        Function function)
    {
        this.percentilePolicy = percentilePolicy;
        this.fixedPolicy = fixedPolicy;
        this.function = function;
    }

    @Override
    public long calculateThreshold(Timer readLatency)
    {
        long percentileThreshold = percentilePolicy.calculateThreshold(readLatency);
        long fixedThreshold = fixedPolicy.calculateThreshold(readLatency);

        return function == Function.MIN
             ? Math.min(percentileThreshold, fixedThreshold)
             : Math.max(percentileThreshold, fixedThreshold);
    }

    @Override
    public Kind kind()
    {
        return Kind.HYBRID;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof HybridSpeculativeRetryPolicy))
            return false;
        HybridSpeculativeRetryPolicy rhs = (HybridSpeculativeRetryPolicy) obj;
        return function == rhs.function
            && Objects.equal(percentilePolicy, rhs.percentilePolicy)
            && Objects.equal(fixedPolicy, rhs.fixedPolicy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(function, percentilePolicy, fixedPolicy);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s,%s)", function, percentilePolicy, fixedPolicy);
    }
}
