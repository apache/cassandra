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

import org.junit.Test;

import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_SCOPE_PARAMETER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionDictionaryTrainingConfigTest
{
    @Test
    public void testScope()
    {
        assertThat(CompressionDictionaryTrainingConfig.getScope(Map.of())).isEqualTo(CompressionDictionaryScope.CLUSTER);
        assertThat(CompressionDictionaryTrainingConfig.getScope(Map.of(TRAINING_SCOPE_PARAMETER_NAME, "CLUSTER"))).isEqualTo(CompressionDictionaryScope.CLUSTER);
        // there is no cluster metadata, so the id of this node is not known
        for (String scope : new String[]{ "node", "rack", "dc" })
        {
            assertThatThrownBy(() -> CompressionDictionaryTrainingConfig.getScope(Map.of(TRAINING_SCOPE_PARAMETER_NAME, scope)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("The id of this node is not known");
        }
        assertThatThrownBy(() -> CompressionDictionaryTrainingConfig.getScope(Map.of(TRAINING_SCOPE_PARAMETER_NAME, "region")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid scope 'region', valid scopes are [node, rack, dc, cluster]");
    }

    @Test
    public void testBuilderDefaults()
    {
        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder().build();

        assertThat(config.maxDictionarySize)
        .as("Default max dictionary size should be 64KB")
        .isEqualTo(65536);
        assertThat(config.maxTotalSampleSize)
        .as("Default max total sample size should be 10MB")
        .isEqualTo(10 * 1024 * 1024);
    }

    @Test
    public void testCalculatedThresholds()
    {
        int dictSize = 16 * 1024; // 16KB
        int sampleSize = 2 * 1024 * 1024; // 2MB

        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                        .maxDictionarySize(dictSize)
                                                                                        .maxTotalSampleSize(sampleSize)
                                                                                        .build();

        // Verify all calculated values are consistent
        assertThat(config.maxDictionarySize).isEqualTo(dictSize);
        assertThat(config.maxTotalSampleSize).isEqualTo(sampleSize);
        assertThat(config.acceptableTotalSampleSize).isEqualTo(sampleSize / 10 * 8);

        // Verify relationship between max and acceptable sample sizes
        assertThat(config.acceptableTotalSampleSize)
        .as("Acceptable sample size should be less than or equal to max")
        .isLessThanOrEqualTo(config.maxTotalSampleSize);
    }
}
