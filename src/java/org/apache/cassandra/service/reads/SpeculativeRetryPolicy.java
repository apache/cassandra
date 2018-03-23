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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.Timer;
import org.apache.cassandra.exceptions.ConfigurationException;

import org.apache.cassandra.service.reads.HybridSpeculativeRetryPolicy.Function;

public interface SpeculativeRetryPolicy
{
    public enum Kind
    {
        // we need to keep NONE around for legacy tables, but going
        // forward we can use the better named NEVER Kind instead
        NEVER, NONE, FIXED, PERCENTILE, HYBRID, ALWAYS
    }

    static final Pattern RETRY_PATTERN = Pattern.compile("^(?<function>MIN|MAX|NEVER|NONE|ALWAYS)?\\(?" +
                                                         "(?<val1>[0-9.]+)?(?<op1>P|PERCENTILE|MS)?([,\\s]*)" +
                                                         "(?<val2>[0-9.]+)?(?<op2>P|PERCENTILE|MS)?\\)?$",
                                                         Pattern.CASE_INSENSITIVE);
    public static final SpeculativeRetryPolicy DEFAULT = new PercentileSpeculativeRetryPolicy(99.0);

    long calculateThreshold(Timer readLatency);

    Kind kind();

    public static SpeculativeRetryPolicy nonHybridPolicyFromString(String typeStr, String valueStr) throws ConfigurationException
    {
        switch (typeStr.toLowerCase())
        {
            case "p":
            case "percentile":
            {
                double value = Double.parseDouble(valueStr);
                if (value >= 100.0 || value <= 0)
                    throw new ConfigurationException(String.format("PERCENTILE should be between 0 and 100 " +
                                                                   "(not %s)", valueStr));
                return new PercentileSpeculativeRetryPolicy(value);
            }
            case "ms":
            {
                // we've always treated this as long, but let's parse as double for compatibility for now
                double value = Double.parseDouble(valueStr);
                return new FixedSpeculativeRetryPolicy((int) value);
            }
            default:
                throw new ConfigurationException(String.format("invalid speculative_retry type: %s", typeStr));
        }
    }

    public static SpeculativeRetryPolicy fromString(String schemaStrValue) throws ConfigurationException
    {
        Matcher matcher = RETRY_PATTERN.matcher(schemaStrValue);
        if (matcher.find())
        {
            String functionStr = matcher.group("function");
            String val1 = matcher.group("val1");
            String op1 = matcher.group("op1");
            String val2 = matcher.group("val2");
            String op2 = matcher.group("op2");

            String normalizedFunction = (functionStr != null) ? functionStr.toUpperCase(Locale.ENGLISH) : null;
            if (normalizedFunction != null && val1 != null && val2 != null)
            {
                Function hybridFunction;
                try
                {
                    hybridFunction = Function.valueOf(normalizedFunction);
                }
                catch (IllegalArgumentException e)
                {
                    throw new ConfigurationException(String.format("Specified comparator [%s] is not supported", normalizedFunction));
                }

                SpeculativeRetryPolicy leftPolicy = nonHybridPolicyFromString(op1, val1);
                SpeculativeRetryPolicy rightPolicy = nonHybridPolicyFromString(op2, val2);

                if (leftPolicy.kind() == rightPolicy.kind())
                {
                    throw new ConfigurationException(String.format("Speculative Retry Parameters must be " +
                                                                   "unique types. Both were found to be %s",
                                                                   leftPolicy.kind().toString()));
                }

                return (leftPolicy.kind() == Kind.PERCENTILE)
                       ? new HybridSpeculativeRetryPolicy((PercentileSpeculativeRetryPolicy) leftPolicy,
                                                          (FixedSpeculativeRetryPolicy) rightPolicy, hybridFunction)
                       : new HybridSpeculativeRetryPolicy((PercentileSpeculativeRetryPolicy) rightPolicy,
                                                          (FixedSpeculativeRetryPolicy) leftPolicy, hybridFunction);
            }
            else if (normalizedFunction != null && val1 == null && op1 == null)
            {
                try
                {
                    Kind kind = Kind.valueOf(normalizedFunction);
                    switch (kind)
                    {
                        case NONE:
                        case NEVER: return NeverSpeculativeRetryPolicy.INSTANCE;
                        case ALWAYS: return AlwaysSpeculativeRetryPolicy.INSTANCE;
                        default: throw new ConfigurationException(String.format("Specified comparator [%s] is not supported", normalizedFunction));
                    }
                }
                catch (IllegalArgumentException e)
                {
                    throw new ConfigurationException(String.format("Specified comparator [%s] is not supported", normalizedFunction));
                }
            }
            else
            {
                if (op1 == null || val1 == null)
                {
                    throw new ConfigurationException(String.format("Specified Speculative Retry Policy [%s] is not supported", schemaStrValue));
                }
                else
                {
                    return nonHybridPolicyFromString(op1, val1);
                }
            }
        }
        else
        {
            throw new ConfigurationException(String.format("Specified Speculative Retry Policy [%s] is not supported", schemaStrValue));
        }
    }
}
