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

import com.google.common.base.Preconditions;

/**
 * Configuration for dictionary training parameters.
 */
public class CompressionDictionaryTrainingConfig
{
    public final int maxDictionarySize;
    public final int maxTotalSampleSize;
    public final int acceptableTotalSampleSize;
    public final float samplingRate;
    public final int chunkSize;

    private CompressionDictionaryTrainingConfig(Builder builder)
    {
        this.maxDictionarySize = builder.maxDictionarySize;
        this.maxTotalSampleSize = builder.maxTotalSampleSize;
        this.acceptableTotalSampleSize = builder.maxTotalSampleSize / 10 * 8;
        this.samplingRate = builder.samplingRate;
        this.chunkSize = builder.chunkSize;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int maxDictionarySize = 65536; // 64KB default
        private int maxTotalSampleSize = 10 * 1024 * 1024; // 10MB total
        private float samplingRate = 0.01f; // Sampling 1%
        private int chunkSize = 64 * 1024; // 64KB default

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

        public Builder samplingRate(float samplingRate)
        {
            if (samplingRate <= 0.0f || samplingRate > 1.0f)
                throw new IllegalArgumentException("Sampling rate has to be between (0.0;1], it is " + samplingRate);

            this.samplingRate = samplingRate;
            return this;
        }

        public Builder chunkSize(int chunkSize)
        {
            this.chunkSize = chunkSize;
            return this;
        }

        public CompressionDictionaryTrainingConfig build()
        {
            Preconditions.checkArgument(maxDictionarySize > 0, "maxDictionarySize must be positive");
            Preconditions.checkArgument(maxTotalSampleSize > 0, "maxTotalSampleSize must be positive");
            Preconditions.checkArgument(samplingRate > 0, "samplingRate must be positive");
            Preconditions.checkArgument(chunkSize > 0, "chunkSize must be positive");
            return new CompressionDictionaryTrainingConfig(this);
        }
    }
}
