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

package org.apache.cassandra.db.compression;

import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * Configuration options for manual compression dictionary training.
 * This class encapsulates the parsed and validated parameters needed for training.
 */
public class ManualTrainingOptions
{
    public static final String MAX_SAMPLING_DURATION_SECONDS_KEY = "maxSamplingDurationSeconds";

    private final int maxSamplingDurationSeconds;

    public ManualTrainingOptions(int maxSamplingDurationSeconds)
    {
        Preconditions.checkArgument(maxSamplingDurationSeconds > 0,
                                    "maxSamplingDurationSeconds must be positive, got: %s", maxSamplingDurationSeconds);
        this.maxSamplingDurationSeconds = maxSamplingDurationSeconds;
    }

    /**
     * Parse options from a string map, typically from JMX/MBean calls.
     *
     * @param options the string map containing training options
     * @return parsed and validated ManualTrainingOptions
     * @throws IllegalArgumentException if required parameters are missing or invalid
     */
    public static ManualTrainingOptions fromStringMap(Map<String, String> options)
    {
        if (options == null || !options.containsKey(MAX_SAMPLING_DURATION_SECONDS_KEY))
        {
            throw new IllegalArgumentException(MAX_SAMPLING_DURATION_SECONDS_KEY + " parameter is required for manual dictionary training");
        }

        String durationStr = options.get(MAX_SAMPLING_DURATION_SECONDS_KEY);
        int maxSamplingDurationSeconds;
        try
        {
            maxSamplingDurationSeconds = Integer.parseInt(durationStr);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid maxSamplingDurationSeconds value: " + durationStr, e);
        }

        return new ManualTrainingOptions(maxSamplingDurationSeconds);
    }

    public int getMaxSamplingDurationSeconds()
    {
        return maxSamplingDurationSeconds;
    }

    @Override
    public String toString()
    {
        return "ManualTrainingOptions{" +
               MAX_SAMPLING_DURATION_SECONDS_KEY + '=' + maxSamplingDurationSeconds +
               '}';
    }
}
