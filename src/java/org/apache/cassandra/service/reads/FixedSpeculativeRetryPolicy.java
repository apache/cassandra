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

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.SnapshottingTimer;
import org.apache.cassandra.schema.TableParams;

public class FixedSpeculativeRetryPolicy implements SpeculativeRetryPolicy
{
    private static final Pattern PATTERN = Pattern.compile("^(?<val>[0-9.]+)ms$", Pattern.CASE_INSENSITIVE);

    private final int speculateAtMilliseconds;

    FixedSpeculativeRetryPolicy(int speculateAtMilliseconds)
    {
        this.speculateAtMilliseconds = speculateAtMilliseconds;
    }

    @Override
    public long calculateThreshold(SnapshottingTimer latency, long existingValue)
    {
        return TimeUnit.MILLISECONDS.toMicros(speculateAtMilliseconds);
    }

    @Override
    public Kind kind()
    {
        return Kind.FIXED;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof FixedSpeculativeRetryPolicy))
            return false;
        FixedSpeculativeRetryPolicy rhs = (FixedSpeculativeRetryPolicy) obj;
        return speculateAtMilliseconds == rhs.speculateAtMilliseconds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(kind(), speculateAtMilliseconds);
    }

    @Override
    public String toString()
    {
        return String.format("%dms", speculateAtMilliseconds);
    }

    static FixedSpeculativeRetryPolicy fromString(String str)
    {
        Matcher matcher = PATTERN.matcher(str);

        if (!matcher.matches())
            throw new IllegalArgumentException();

        String val = matcher.group("val");
        try
        {
             // historically we've always parsed this as double, but treated as int; so we keep doing it for compatibility
            return new FixedSpeculativeRetryPolicy((int) Double.parseDouble(val));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s'", str, TableParams.Option.SPECULATIVE_RETRY));
        }
    }

    static boolean stringMatches(String str)
    {
        return PATTERN.matcher(str).matches();
    }
}
