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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.SnapshottingTimer;
import org.apache.cassandra.schema.TableParams;

public class HybridSpeculativeRetryPolicy implements SpeculativeRetryPolicy
{
    private static final Pattern PATTERN =
        Pattern.compile("^(?<fun>MIN|MAX)\\((?<val1>[0-9.]+[a-z]+)\\s*,\\s*(?<val2>[0-9.]+[a-z]+)\\)$",
                        Pattern.CASE_INSENSITIVE);

    public enum Function
    {
        MIN, MAX;

        long call(long val1, long val2)
        {
            return this == MIN ? Math.min(val1, val2) : Math.max(val1, val2);
        }
    }

    private final PercentileSpeculativeRetryPolicy percentilePolicy;
    private final FixedSpeculativeRetryPolicy fixedPolicy;
    private final Function function;

    HybridSpeculativeRetryPolicy(PercentileSpeculativeRetryPolicy percentilePolicy,
                                 FixedSpeculativeRetryPolicy fixedPolicy,
                                 Function function)
    {
        this.percentilePolicy = percentilePolicy;
        this.fixedPolicy = fixedPolicy;
        this.function = function;
    }

    @Override
    public long calculateThreshold(SnapshottingTimer latency, long existingValue)
    {
        Snapshot snapshot = latency.getPercentileSnapshot();
        
        if (snapshot.size() <= 0)
            return existingValue;
        
        return function.call(percentilePolicy.calculateThreshold(snapshot, existingValue), 
                             fixedPolicy.calculateThreshold(null, existingValue));
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

    static HybridSpeculativeRetryPolicy fromString(String str)
    {
        Matcher matcher = PATTERN.matcher(str);

        if (!matcher.matches())
            throw new IllegalArgumentException();

        String val1 = matcher.group("val1");
        String val2 = matcher.group("val2");

        SpeculativeRetryPolicy value1, value2;
        try
        {
            value1 = SpeculativeRetryPolicy.fromString(val1);
            value2 = SpeculativeRetryPolicy.fromString(val2);
        }
        catch (ConfigurationException e)
        {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s'", str, TableParams.Option.SPECULATIVE_RETRY));
        }

        if (value1.kind() == value2.kind())
        {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s': MIN()/MAX() arguments " +
                                                           "should be of different types, but both are of type %s",
                                                           str, TableParams.Option.SPECULATIVE_RETRY, value1.kind()));
        }

        SpeculativeRetryPolicy policy1 = value1 instanceof PercentileSpeculativeRetryPolicy ? value1 : value2;
        SpeculativeRetryPolicy policy2 = value1 instanceof FixedSpeculativeRetryPolicy ? value1 : value2;

        Function function = Function.valueOf(matcher.group("fun").toUpperCase());
        return new HybridSpeculativeRetryPolicy((PercentileSpeculativeRetryPolicy) policy1, (FixedSpeculativeRetryPolicy) policy2, function);
    }

    static boolean stringMatches(String str)
    {
        return PATTERN.matcher(str).matches();
    }
}
