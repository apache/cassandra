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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.SnapshottingTimer;
import org.apache.cassandra.schema.TableParams;

public class PercentileSpeculativeRetryPolicy implements SpeculativeRetryPolicy
{
    public static final PercentileSpeculativeRetryPolicy NINETY_NINE_P = new PercentileSpeculativeRetryPolicy(99.0);

    private static final Pattern PATTERN = Pattern.compile("^(?<val>[0-9.]+)p(ercentile)?$", Pattern.CASE_INSENSITIVE);
    /**
     * The pattern above uses dot as decimal separator, so we use {@link Locale#ENGLISH} to enforce that. (CASSANDRA-14374)
     */
    private static final DecimalFormat FORMATTER = new DecimalFormat("#.####", new DecimalFormatSymbols(Locale.ENGLISH));

    private final double percentile;

    public PercentileSpeculativeRetryPolicy(double percentile)
    {
        this.percentile = percentile;
    }

    @Override
    public long calculateThreshold(SnapshottingTimer latency, long existingValue)
    {
        return calculateThreshold(latency.getPercentileSnapshot(), existingValue);
    }

    public long calculateThreshold(Snapshot snapshot, long existingValue)
    {
        if (snapshot.size() <= 0)
            return existingValue;
        // latency snapshot uses a default timer so is in microseconds, so just return percentile
        return (long) snapshot.getValue(percentile / 100);
    }

    @Override
    public Kind kind()
    {
        return Kind.PERCENTILE;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof PercentileSpeculativeRetryPolicy))
            return false;
        PercentileSpeculativeRetryPolicy rhs = (PercentileSpeculativeRetryPolicy) obj;
        return percentile == rhs.percentile;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(kind(), percentile);
    }

    @Override
    public String toString()
    {
        return String.format("%sp", FORMATTER.format(percentile));
    }

    static PercentileSpeculativeRetryPolicy fromString(String str)
    {
        Matcher matcher = PATTERN.matcher(str);

        if (!matcher.matches())
            throw new IllegalArgumentException();

        String val = matcher.group("val");

        double percentile;
        try
        {
            percentile = Double.parseDouble(val);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s'", str, TableParams.Option.SPECULATIVE_RETRY));
        }

        if (percentile <= 0.0 || percentile >= 100.0)
        {
            throw new ConfigurationException(String.format("Invalid value %s for PERCENTILE option '%s': must be between (0.0 and 100.0)",
                                                           str, TableParams.Option.SPECULATIVE_RETRY));
        }

        return new PercentileSpeculativeRetryPolicy(percentile);
    }

    static boolean stringMatches(String str)
    {
        return PATTERN.matcher(str).matches();
    }
}
