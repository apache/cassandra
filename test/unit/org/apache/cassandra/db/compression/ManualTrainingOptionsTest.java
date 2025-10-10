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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ManualTrainingOptionsTest
{
    @Test
    public void testValidConstruction()
    {
        ManualTrainingOptions options = new ManualTrainingOptions(600);
        assertThat(options.getMaxSamplingDurationSeconds()).isEqualTo(600);
    }

    @Test
    public void testInvalidDurationThrows()
    {
        assertThatThrownBy(() -> new ManualTrainingOptions(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSamplingDurationSeconds must be positive, got: 0");

        assertThatThrownBy(() -> new ManualTrainingOptions(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSamplingDurationSeconds must be positive, got: -1");
    }

    @Test
    public void testFromStringMapValid()
    {
        Map<String, String> options = Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "300");
        ManualTrainingOptions trainingOptions = ManualTrainingOptions.fromStringMap(options);

        assertThat(trainingOptions.getMaxSamplingDurationSeconds()).isEqualTo(300);
    }

    @Test
    public void testFromStringMapMissingKey()
    {
        Map<String, String> invalidOptions = Map.of();
        ManualTrainingOptions options = ManualTrainingOptions.fromStringMap(invalidOptions);
        assertThat(options.getMaxSamplingDurationSeconds()).isEqualTo(ManualTrainingOptions.DEFAULT_SAMPLING_DURATION_SECONDS);
        assertThat(options.useExistingSSTables()).isEqualTo(false);
    }

    @Test
    public void testFromStringMapInvalidValue()
    {
        Map<String, String> invalidOptions = Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "invalid");
        ManualTrainingOptions options = ManualTrainingOptions.fromStringMap(invalidOptions);
        assertThat(options.getMaxSamplingDurationSeconds()).isEqualTo(ManualTrainingOptions.DEFAULT_SAMPLING_DURATION_SECONDS);
        assertThat(options.useExistingSSTables()).isEqualTo(false);
    }

    @Test
    public void testFromStringMapNegativeValue()
    {
        Map<String, String> negativeOptions = Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "-1");

        assertThatThrownBy(() -> ManualTrainingOptions.fromStringMap(negativeOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSamplingDurationSeconds must be positive, got: -1");
    }

    @Test
    public void testFromStringMapZeroValue()
    {
        Map<String, String> zeroOptions = Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "0");

        assertThatThrownBy(() -> ManualTrainingOptions.fromStringMap(zeroOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSamplingDurationSeconds must be positive, got: 0");
    }
}
