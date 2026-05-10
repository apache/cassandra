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

    private CompressionDictionaryTrainingConfig(Builder builder)
    {
        this.maxDictionarySize = builder.maxDictionarySize;
        this.maxTotalSampleSize = builder.maxTotalSampleSize;
        this.acceptableTotalSampleSize = builder.maxTotalSampleSize / 10 * 8;
        this.chunkSize = builder.chunkSize;
        this.minTrainingFrequency = builder.minTrainingFrequency;
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

        public CompressionDictionaryTrainingConfig build()
        {
            Preconditions.checkArgument(maxDictionarySize > 0, "maxDictionarySize must be positive");
            Preconditions.checkArgument(maxTotalSampleSize > 0, "maxTotalSampleSize must be positive");
            Preconditions.checkArgument(chunkSize > 0, "chunkSize must be positive");
            Preconditions.checkArgument(minTrainingFrequency >= 0, "min training frequency must be non-negative");
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
}
