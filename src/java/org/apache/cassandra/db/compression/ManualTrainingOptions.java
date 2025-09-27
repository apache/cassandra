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

/**
 * Configuration options for manual compression dictionary training.
 * This class encapsulates the parsed and validated parameters needed for training.
 */
public class ManualTrainingOptions
{
    private final int maxSamplingDurationSeconds;

    public ManualTrainingOptions(int maxSamplingDurationSeconds)
    {
        if (maxSamplingDurationSeconds <= 0)
        {
            throw new IllegalArgumentException("maxSamplingDurationSeconds must be positive, got: " + maxSamplingDurationSeconds);
        }
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
        if (options == null || !options.containsKey("maxSamplingDurationSeconds"))
        {
            throw new IllegalArgumentException("maxSamplingDurationSeconds parameter is required for manual dictionary training");
        }

        String durationStr = options.get("maxSamplingDurationSeconds");
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
               "maxSamplingDurationSeconds=" + maxSamplingDurationSeconds +
               '}';
    }
}