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

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompressionParams;

import static java.lang.String.format;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.AUTO_TRAINING_ENABLED;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.AUTO_TRAINING_IMPROVEMENT_THRESHOLD_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.AUTO_TRAINING_TWCS_MAX_WINDOWS;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_AUTO_TRAINING_IMPROVEMENT_THRESHOLD_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_AUTO_TRAINING_TWCS_MAX_WINDOWS_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MIN_FREQUENCY;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MIN_FREQUENCY_PARAMETER_NAME;

/**
 * Configuration for dictionary training parameters.
 */
public class CompressionDictionaryTrainingConfig
{
    public final int maxDictionarySize;
    public final int maxTotalSampleSize;
    public final int acceptableTotalSampleSize;
    public final int chunkSize;
    public final int minTrainingFrequency;
    public final boolean autoTrainingEnabled;
    public final float autoTrainingImprovementThreshold;
    public final int autoTrainingTwcsMaxWindows;

    private CompressionDictionaryTrainingConfig(Builder builder)
    {
        this.maxDictionarySize = builder.maxDictionarySize;
        this.maxTotalSampleSize = builder.maxTotalSampleSize;
        this.acceptableTotalSampleSize = builder.maxTotalSampleSize / 10 * 8;
        this.chunkSize = builder.chunkSize;
        this.minTrainingFrequency = builder.minTrainingFrequency;
        this.autoTrainingEnabled = builder.autoTrainingEnabled;
        this.autoTrainingImprovementThreshold = builder.autoTrainingImprovementThreshold;
        this.autoTrainingTwcsMaxWindows = builder.autoTrainingTwcsMaxWindows;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int maxDictionarySize = 65536; // 64KB default
        private int maxTotalSampleSize = 10 * 1024 * 1024; // 10MB total
        private int chunkSize = 64 * 1024; // 64KB default
        private int minTrainingFrequency = 0; // in minutes
        private boolean autoTrainingEnabled = false;
        private float autoTrainingImprovementThreshold = Float.parseFloat(DEFAULT_AUTO_TRAINING_IMPROVEMENT_THRESHOLD_VALUE);
        private int autoTrainingTwcsMaxWindows = Integer.parseInt(DEFAULT_AUTO_TRAINING_TWCS_MAX_WINDOWS_VALUE);

        public Builder maxDictionarySize(int size)
        {
            this.maxDictionarySize = size;
            return this;
        }

        public Builder maxTotalSampleSize(int size)
        {
            this.maxTotalSampleSize = size;
            return this;
        }

        public Builder chunkSize(int chunkSize)
        {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder minTrainingFrequency(int minTrainingFrequency)
        {
            this.minTrainingFrequency = minTrainingFrequency;
            return this;
        }

        public Builder isAutoTrainingEnabled(boolean autoTrainingEnabled)
        {
            this.autoTrainingEnabled = autoTrainingEnabled;
            return this;
        }

        public Builder autoTrainingImprovementThreshold(float improvement)
        {
            this.autoTrainingImprovementThreshold = improvement;
            return this;
        }

        public Builder autoTrainingTwcsMaxWindows(int autoTrainingTwcsMaxWindows)
        {
            this.autoTrainingTwcsMaxWindows = autoTrainingTwcsMaxWindows;
            return this;
        }

        public CompressionDictionaryTrainingConfig build()
        {
            Preconditions.checkArgument(maxDictionarySize > 0, "maxDictionarySize must be positive");
            Preconditions.checkArgument(maxTotalSampleSize > 0, "maxTotalSampleSize must be positive");
            Preconditions.checkArgument(chunkSize > 0, "chunkSize must be positive");
            Preconditions.checkArgument(minTrainingFrequency >= 0, "min training frequency must be non-negative");
            Preconditions.checkArgument(autoTrainingImprovementThreshold > 0 && autoTrainingImprovementThreshold < 1,
                                        "auto training improvement threshold has to be (0, 1)");
            Preconditions.checkArgument(autoTrainingTwcsMaxWindows > 0, "auto_training_twcs_max_windows must be positive");
            return new CompressionDictionaryTrainingConfig(this);
        }
    }

    public static int getMaxDictionarySize(Map<String, String> params)
    {
        return validateSizeBasedTrainingParameter(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME,
                                                  params.getOrDefault(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME,
                                                                      DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE));
    }

    public static int getMaxTotalSampleSize(Map<String, String> params)
    {
        return validateSizeBasedTrainingParameter(TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME,
                                                  params.getOrDefault(TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME,
                                                                      DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE));
    }

    public static int getMinTrainingFrequency(Map<String, String> params)
    {
        return validateDurationBasedTrainingParameter(TRAINING_MIN_FREQUENCY_PARAMETER_NAME,
                                                      params.getOrDefault(TRAINING_MIN_FREQUENCY_PARAMETER_NAME,
                                                                          DEFAULT_TRAINING_MIN_FREQUENCY));
    }

    public static int getMaxDictionarySizeWithUserSuppliedParams(CompressionParams compressionParams, Map<String, String> parameters)
    {
        return internalTrainingParameterResolution(compressionParams,
                                                   parameters.get(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME),
                                                   TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME,
                                                   DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE);
    }

    public static int getMaxTotalSampleSizeWithUserSuppliedParams(CompressionParams compressionParams, Map<String, String> parameters)
    {
        return internalTrainingParameterResolution(compressionParams,
                                                   parameters.get(TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME),
                                                   TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME,
                                                   DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE);
    }

    public static boolean isAutoTrainingEnabled(Map<String, String> params)
    {
        return validateBoolean(AUTO_TRAINING_ENABLED,
                               params.getOrDefault(AUTO_TRAINING_ENABLED, "false"));
    }

    public static float getAutoTrainingImprovementThreshold(Map<String, String> params)
    {
        float parsedValue;

        try
        {
            String resolvedValue = params.getOrDefault(AUTO_TRAINING_IMPROVEMENT_THRESHOLD_NAME,
                                                       DEFAULT_AUTO_TRAINING_IMPROVEMENT_THRESHOLD_VALUE);

            parsedValue = Float.parseFloat(resolvedValue);
        }
        catch (Throwable t)
        {
            throw new ConfigurationException(String.format("Unable to parse float value for %s",
                                                           DEFAULT_AUTO_TRAINING_IMPROVEMENT_THRESHOLD_VALUE));
        }

        if (Float.isInfinite(parsedValue) || Float.isNaN(parsedValue) || parsedValue <= 0f || parsedValue >= 1f)
            throw new ConfigurationException(String.format("Invalid value for %s, can be (0, 1]",
                                                           AUTO_TRAINING_IMPROVEMENT_THRESHOLD_NAME));

        return parsedValue;
    }

    public static int getAutoTrainingTwcsMaxWindows(Map<String, String> params)
    {
        String resolvedValue = params.getOrDefault(AUTO_TRAINING_TWCS_MAX_WINDOWS,
                                                   DEFAULT_AUTO_TRAINING_TWCS_MAX_WINDOWS_VALUE);
        int parsedValue;
        try
        {
            parsedValue = Integer.parseInt(resolvedValue.trim());
        }
        catch (Throwable t)
        {
            throw new ConfigurationException(format("Unable to set value to parameter %s: %s. It has to be a positive integer.",
                                                    AUTO_TRAINING_TWCS_MAX_WINDOWS, resolvedValue));
        }

        if (parsedValue < 1)
            throw new ConfigurationException(format("Invalid value for %s: %s. It has to be a positive integer (>= 1).",
                                                    AUTO_TRAINING_TWCS_MAX_WINDOWS, resolvedValue));

        return parsedValue;
    }

    private static int internalTrainingParameterResolution(CompressionParams compressionParams,
                                                           String userSuppliedValue,
                                                           String parameterName,
                                                           String defaultParameterValue)
    {
        String resolvedValue = null;
        try
        {
            if (userSuppliedValue == null)
                resolvedValue = compressionParams.getOtherOptions().getOrDefault(parameterName, defaultParameterValue);
            else
                resolvedValue = userSuppliedValue;

            return new DataStorageSpec.IntBytesBound(resolvedValue).toBytes();
        }
        catch (Throwable t)
        {
            throw new IllegalArgumentException(String.format("Invalid value for %s: %s", parameterName, resolvedValue));
        }
    }

    /**
     * Validates value of a parameter for training purposes. The value to validate should
     * be accepted by {@link DataStorageSpec.IntKibibytesBound}. This method is used upon validation
     * of input parameters in the implementations of dictionary compressor.
     *
     * @param parameterName name of a parameter to validate
     * @param resolvedValue value to validate
     * @return resolved value in bytes
     */
    static int validateSizeBasedTrainingParameter(String parameterName, String resolvedValue)
    {
        try
        {
            return new DataStorageSpec.IntBytesBound(resolvedValue).toBytes();
        }
        catch (Throwable t)
        {
            throw new ConfigurationException(format("Unable to set value to parameter %s: %s. Reason: %s",
                                                    parameterName, resolvedValue, t.getMessage()));
        }
    }

    /**
     * Validates value of a parameter for training purposes. The value to validate should
     * be accepted by {@link DurationSpec.IntMinutesBound}. This method is used upon validation of input parameters
     * in the implementation of dictionary compressor.
     *
     * @param parameterName name of a parameter to validate
     * @param resolvedValue value to validate
     * @return resolved value in minutes
     */
    static int validateDurationBasedTrainingParameter(String parameterName, String resolvedValue)
    {
        try
        {
            return new DurationSpec.IntMinutesBound(resolvedValue).toMinutes();
        }
        catch (Throwable t)
        {
            throw new ConfigurationException(format("Unable to set value to parameter %s: %s. Reason: %s",
                                                    parameterName, resolvedValue, t.getMessage()));
        }
    }

    static boolean validateBoolean(String parameterName, String resolvedValue)
    {
        if (resolvedValue == null)
            return false;

        if (resolvedValue.equals("true") || resolvedValue.equals("false"))
        {
            return Boolean.parseBoolean(resolvedValue);
        }
        else
        {
            throw new ConfigurationException(format("Unable to set value to parameter %s: %s. It has to be 'true' or 'false'.",
                                                    parameterName, resolvedValue));
        }
    }
}
