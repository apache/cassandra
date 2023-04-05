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

import org.apache.cassandra.exceptions.ConfigurationException;

public final class SizeTieredCompactionStrategyOptions
{
    protected static final long DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    protected static final double DEFAULT_BUCKET_LOW = 0.5;
    protected static final double DEFAULT_BUCKET_HIGH = 1.5;
    protected static final String MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    protected static final String BUCKET_LOW_KEY = "bucket_low";
    protected static final String BUCKET_HIGH_KEY = "bucket_high";

    protected long minSSTableSize;
    protected double bucketLow;
    protected double bucketHigh;

    public SizeTieredCompactionStrategyOptions(Map<String, String> options)
    {
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
        optionValue = options.get(BUCKET_LOW_KEY);
        bucketLow = optionValue == null ? DEFAULT_BUCKET_LOW : Double.parseDouble(optionValue);
        optionValue = options.get(BUCKET_HIGH_KEY);
        bucketHigh = optionValue == null ? DEFAULT_BUCKET_HIGH : Double.parseDouble(optionValue);
    }

    public SizeTieredCompactionStrategyOptions()
    {
        minSSTableSize = DEFAULT_MIN_SSTABLE_SIZE;
        bucketLow = DEFAULT_BUCKET_LOW;
        bucketHigh = DEFAULT_BUCKET_HIGH;
    }

    private static double parseDouble(Map<String, String> options, String key, double defaultValue) throws ConfigurationException
    {
        String optionValue = options.get(key);
        try
        {
            return optionValue == null ? defaultValue : Double.parseDouble(optionValue);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable float for %s", optionValue, key), e);
        }
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        try
        {
            long minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
            if (minSSTableSize < 0)
            {
                throw new ConfigurationException(String.format("%s must be non negative: %d", MIN_SSTABLE_SIZE_KEY, minSSTableSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MIN_SSTABLE_SIZE_KEY), e);
        }

        double bucketLow = parseDouble(options, BUCKET_LOW_KEY, DEFAULT_BUCKET_LOW);
        double bucketHigh = parseDouble(options, BUCKET_HIGH_KEY, DEFAULT_BUCKET_HIGH);
        if (bucketHigh <= bucketLow)
        {
            throw new ConfigurationException(String.format("%s value (%s) is less than or equal to the %s value (%s)",
                                                           BUCKET_HIGH_KEY, bucketHigh, BUCKET_LOW_KEY, bucketLow));
        }

        uncheckedOptions.remove(MIN_SSTABLE_SIZE_KEY);
        uncheckedOptions.remove(BUCKET_LOW_KEY);
        uncheckedOptions.remove(BUCKET_HIGH_KEY);

        return uncheckedOptions;
    }

    @Override
    public String toString()
    {
        return String.format("Min sstable size: %d, bucket low: %f, bucket high: %f", minSSTableSize, bucketLow, bucketHigh);
    }

}
