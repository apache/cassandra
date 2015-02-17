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
package org.apache.cassandra.db.compaction;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class DateTieredCompactionStrategyOptions
{
    protected static final TimeUnit DEFAULT_TIMESTAMP_RESOLUTION = TimeUnit.MICROSECONDS;
    protected static final double DEFAULT_MAX_SSTABLE_AGE_DAYS = 365;
    protected static final long DEFAULT_BASE_TIME_SECONDS = 60;
    protected static final String TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
    protected static final String MAX_SSTABLE_AGE_KEY = "max_sstable_age_days";
    protected static final String BASE_TIME_KEY = "base_time_seconds";

    protected final long maxSSTableAge;
    protected final long baseTime;

    public DateTieredCompactionStrategyOptions(Map<String, String> options)
    {
        String optionValue = options.get(TIMESTAMP_RESOLUTION_KEY);
        TimeUnit timestampResolution = optionValue == null ? DEFAULT_TIMESTAMP_RESOLUTION : TimeUnit.valueOf(optionValue);
        optionValue = options.get(MAX_SSTABLE_AGE_KEY);
        double fractionalDays = optionValue == null ? DEFAULT_MAX_SSTABLE_AGE_DAYS : Double.parseDouble(optionValue);
        maxSSTableAge = Math.round(fractionalDays * timestampResolution.convert(1, TimeUnit.DAYS));
        optionValue = options.get(BASE_TIME_KEY);
        baseTime = timestampResolution.convert(optionValue == null ? DEFAULT_BASE_TIME_SECONDS : Long.parseLong(optionValue), TimeUnit.SECONDS);
    }

    public DateTieredCompactionStrategyOptions()
    {
        maxSSTableAge = Math.round(DEFAULT_MAX_SSTABLE_AGE_DAYS * DEFAULT_TIMESTAMP_RESOLUTION.convert(1, TimeUnit.DAYS));
        baseTime = DEFAULT_TIMESTAMP_RESOLUTION.convert(DEFAULT_BASE_TIME_SECONDS, TimeUnit.SECONDS);
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws  ConfigurationException
    {
        String optionValue = options.get(TIMESTAMP_RESOLUTION_KEY);
        try
        {
            if (optionValue != null)
                TimeUnit.valueOf(optionValue);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("timestamp_resolution %s is not valid", optionValue));
        }

        optionValue = options.get(MAX_SSTABLE_AGE_KEY);
        try
        {
            double maxSStableAge = optionValue == null ? DEFAULT_MAX_SSTABLE_AGE_DAYS : Double.parseDouble(optionValue);
            if (maxSStableAge < 0)
            {
                throw new ConfigurationException(String.format("%s must be non-negative: %.2f", MAX_SSTABLE_AGE_KEY, maxSStableAge));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MAX_SSTABLE_AGE_KEY), e);
        }

        optionValue = options.get(BASE_TIME_KEY);
        try
        {
            long baseTime = optionValue == null ? DEFAULT_BASE_TIME_SECONDS : Long.parseLong(optionValue);
            if (baseTime <= 0)
            {
                throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", BASE_TIME_KEY, baseTime));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, BASE_TIME_KEY), e);
        }

        uncheckedOptions.remove(MAX_SSTABLE_AGE_KEY);
        uncheckedOptions.remove(BASE_TIME_KEY);
        uncheckedOptions.remove(TIMESTAMP_RESOLUTION_KEY);

        return uncheckedOptions;
    }
}
