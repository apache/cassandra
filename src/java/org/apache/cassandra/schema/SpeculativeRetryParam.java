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
package org.apache.cassandra.schema;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.Locale;

import com.google.common.base.Objects;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

public final class SpeculativeRetryParam
{
    public enum Kind
    {
        NONE, CUSTOM, PERCENTILE, ALWAYS
    }

    public static final SpeculativeRetryParam NONE = none();
    public static final SpeculativeRetryParam ALWAYS = always();
    public static final SpeculativeRetryParam DEFAULT = percentile(99);

    private final Kind kind;
    private final double value;

    // pre-processed (divided by 100 for PERCENTILE), multiplied by 1M for CUSTOM (to nanos)
    private final double threshold;

    private SpeculativeRetryParam(Kind kind, double value)
    {
        this.kind = kind;
        this.value = value;

        if (kind == Kind.PERCENTILE)
            threshold = value / 100;
        else if (kind == Kind.CUSTOM)
            threshold = TimeUnit.MILLISECONDS.toNanos((long) value);
        else
            threshold = value;
    }

    public Kind kind()
    {
        return kind;
    }

    public double threshold()
    {
        return threshold;
    }

    public static SpeculativeRetryParam none()
    {
        return new SpeculativeRetryParam(Kind.NONE, 0);
    }

    public static SpeculativeRetryParam always()
    {
        return new SpeculativeRetryParam(Kind.ALWAYS, 0);
    }

    public static SpeculativeRetryParam custom(double value)
    {
        return new SpeculativeRetryParam(Kind.CUSTOM, value);
    }

    public static SpeculativeRetryParam percentile(double value)
    {
        return new SpeculativeRetryParam(Kind.PERCENTILE, value);
    }

    public static SpeculativeRetryParam fromString(String value)
    {
        if (value.toLowerCase(Locale.ENGLISH).endsWith("ms"))
        {
            try
            {
                return custom(Double.parseDouble(value.substring(0, value.length() - "ms".length())));
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(format("Invalid value %s for option '%s'", value, TableParams.Option.SPECULATIVE_RETRY));
            }
        }

        if (value.toUpperCase(Locale.ENGLISH).endsWith(Kind.PERCENTILE.toString()))
        {
            double threshold;
            try
            {
                threshold = Double.parseDouble(value.substring(0, value.length() - Kind.PERCENTILE.toString().length()));
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(format("Invalid value %s for option '%s'", value, TableParams.Option.SPECULATIVE_RETRY));
            }

            if (threshold >= 0.0 && threshold <= 100.0)
                return percentile(threshold);

            throw new ConfigurationException(format("Invalid value %s for PERCENTILE option '%s': must be between 0.0 and 100.0",
                                                    value,
                                                    TableParams.Option.SPECULATIVE_RETRY));
        }

        if (value.equals(Kind.NONE.toString()))
            return NONE;

        if (value.equals(Kind.ALWAYS.toString()))
            return ALWAYS;

        throw new ConfigurationException(format("Invalid value %s for option '%s'", value, TableParams.Option.SPECULATIVE_RETRY));
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SpeculativeRetryParam))
            return false;
        SpeculativeRetryParam srp = (SpeculativeRetryParam) o;
        return kind == srp.kind && threshold == srp.threshold;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(kind, threshold);
    }

    @Override
    public String toString()
    {
        switch (kind)
        {
            case CUSTOM:
                return format("%sms", value);
            case PERCENTILE:
                return format("%sPERCENTILE", new DecimalFormat("#.#####").format(value));
            default: // NONE and ALWAYS
                return kind.toString();
        }
    }
}
