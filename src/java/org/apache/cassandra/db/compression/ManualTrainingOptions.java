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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration options for manual compression dictionary training.
 * This class encapsulates the parsed and validated parameters needed for training.
 */
public class ManualTrainingOptions
{
    public static final String MAX_SAMPLING_DURATION_SECONDS_KEY = "maxSamplingDurationSeconds";
    public static final String USE_EXISTING_SSTABLES_KEY = "useExistingSSTables";

    public static final int DEFAULT_SAMPLING_DURATION_SECONDS = 600;

    private static Logger logger = LoggerFactory.getLogger(ManualTrainingOptions.class);

    private final int maxSamplingDurationSeconds;
    private final boolean useExistingSSTables;

    public ManualTrainingOptions(int maxSamplingDurationSeconds)
    {
        this(maxSamplingDurationSeconds, false);
    }

    public ManualTrainingOptions(int maxSamplingDurationSeconds, boolean useExistingSSTables)
    {
        Preconditions.checkArgument(maxSamplingDurationSeconds > 0,
                                    "maxSamplingDurationSeconds must be positive, got: %s", maxSamplingDurationSeconds);
        this.maxSamplingDurationSeconds = maxSamplingDurationSeconds;
        this.useExistingSSTables = useExistingSSTables;
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
        if (options == null)
        {
            return new ManualTrainingOptions(DEFAULT_SAMPLING_DURATION_SECONDS, false);
        }

        int maxSamplingDurationSeconds;
        String durationStr = options.get(MAX_SAMPLING_DURATION_SECONDS_KEY);
        if (durationStr == null)
        {
            maxSamplingDurationSeconds = DEFAULT_SAMPLING_DURATION_SECONDS;
        }
        else
        {
            try
            {
                maxSamplingDurationSeconds = Integer.parseInt(durationStr);
            }
            catch (NumberFormatException e)
            {
                logger.warn("Failed to parse {}:{}. Fallback to default value: {}",
                            MAX_SAMPLING_DURATION_SECONDS_KEY, durationStr, DEFAULT_SAMPLING_DURATION_SECONDS);
                maxSamplingDurationSeconds = DEFAULT_SAMPLING_DURATION_SECONDS;
            }
        }

        boolean useExistingSSTables = Boolean.parseBoolean(options.getOrDefault(USE_EXISTING_SSTABLES_KEY, "false"));

        return new ManualTrainingOptions(maxSamplingDurationSeconds, useExistingSSTables);
    }

    public int getMaxSamplingDurationSeconds()
    {
        return maxSamplingDurationSeconds;
    }

    public boolean useExistingSSTables()
    {
        return useExistingSSTables;
    }

    @Override
    public String toString()
    {
        return "ManualTrainingOptions{" +
               MAX_SAMPLING_DURATION_SECONDS_KEY + '=' + maxSamplingDurationSeconds +
               ", " + USE_EXISTING_SSTABLES_KEY + '=' + useExistingSSTables +
               '}';
    }
}
