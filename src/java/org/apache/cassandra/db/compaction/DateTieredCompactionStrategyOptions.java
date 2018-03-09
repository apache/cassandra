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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class DateTieredCompactionStrategyOptions
{
    private static final Logger logger = LoggerFactory.getLogger(DateTieredCompactionStrategyOptions.class);
    protected static final TimeUnit DEFAULT_TIMESTAMP_RESOLUTION = TimeUnit.MICROSECONDS;
    @Deprecated
    protected static final double DEFAULT_MAX_SSTABLE_AGE_DAYS = 365*1000;
    protected static final long DEFAULT_BASE_TIME_SECONDS = 60;
    protected static final long DEFAULT_MAX_WINDOW_SIZE_SECONDS = TimeUnit.SECONDS.convert(1, TimeUnit.DAYS);

    protected static final int DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 60 * 10;
    protected static final String TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
    @Deprecated
    protected static final String MAX_SSTABLE_AGE_KEY = "max_sstable_age_days";
    protected static final String BASE_TIME_KEY = "base_time_seconds";
    protected static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY = "expired_sstable_check_frequency_seconds";
    protected static final String MAX_WINDOW_SIZE_KEY = "max_window_size_seconds";

    @Deprecated
    protected final long maxSSTableAge;
    protected final TimeUnit timestampResolution;
    protected final long baseTime;
    protected final long expiredSSTableCheckFrequency;
    protected final long maxWindowSize;

    public DateTieredCompactionStrategyOptions(Map<String, String> options)
    {
        String optionValue = options.get(TIMESTAMP_RESOLUTION_KEY);
        timestampResolution = optionValue == null ? DEFAULT_TIMESTAMP_RESOLUTION : TimeUnit.valueOf(optionValue);
        if (timestampResolution != DEFAULT_TIMESTAMP_RESOLUTION)
            logger.warn("Using a non-default timestamp_resolution {} - are you really doing inserts with USING TIMESTAMP <non_microsecond_timestamp> (or driver equivalent)?", timestampResolution);
        optionValue = options.get(MAX_SSTABLE_AGE_KEY);
        double fractionalDays = optionValue == null ? DEFAULT_MAX_SSTABLE_AGE_DAYS : Double.parseDouble(optionValue);
        maxSSTableAge = Math.round(fractionalDays * timestampResolution.convert(1, TimeUnit.DAYS));
        optionValue = options.get(BASE_TIME_KEY);
        baseTime = timestampResolution.convert(optionValue == null ? DEFAULT_BASE_TIME_SECONDS : Long.parseLong(optionValue), TimeUnit.SECONDS);
        optionValue = options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(optionValue == null ? DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS : Long.parseLong(optionValue), TimeUnit.SECONDS);
        optionValue = options.get(MAX_WINDOW_SIZE_KEY);
        maxWindowSize = timestampResolution.convert(optionValue == null ? DEFAULT_MAX_WINDOW_SIZE_SECONDS : Long.parseLong(optionValue), TimeUnit.SECONDS);
    }

    public DateTieredCompactionStrategyOptions()
    {
        maxSSTableAge = Math.round(DEFAULT_MAX_SSTABLE_AGE_DAYS * DEFAULT_TIMESTAMP_RESOLUTION.convert((long) DEFAULT_MAX_SSTABLE_AGE_DAYS, TimeUnit.DAYS));
        timestampResolution = DEFAULT_TIMESTAMP_RESOLUTION;
        baseTime = timestampResolution.convert(DEFAULT_BASE_TIME_SECONDS, TimeUnit.SECONDS);
        expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS, TimeUnit.SECONDS);
        maxWindowSize = timestampResolution.convert(1, TimeUnit.DAYS);
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

        optionValue = options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        try
        {
            long expiredCheckFrequency = optionValue == null ? DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS : Long.parseLong(optionValue);
            if (expiredCheckFrequency < 0)
            {
                throw new ConfigurationException(String.format("%s must not be negative, but was %d", EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, expiredCheckFrequency));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY), e);
        }

        optionValue = options.get(MAX_WINDOW_SIZE_KEY);
        try
        {
            long maxWindowSize = optionValue == null ? DEFAULT_MAX_WINDOW_SIZE_SECONDS : Long.parseLong(optionValue);
            if (maxWindowSize < 0)
            {
                throw new ConfigurationException(String.format("%s must not be negative, but was %d", MAX_WINDOW_SIZE_KEY, maxWindowSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MAX_WINDOW_SIZE_KEY), e);
        }


        uncheckedOptions.remove(MAX_SSTABLE_AGE_KEY);
        uncheckedOptions.remove(BASE_TIME_KEY);
        uncheckedOptions.remove(TIMESTAMP_RESOLUTION_KEY);
        uncheckedOptions.remove(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        uncheckedOptions.remove(MAX_WINDOW_SIZE_KEY);

        return uncheckedOptions;
    }
}
