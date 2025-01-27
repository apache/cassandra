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

package org.apache.cassandra.sqel;

import java.util.Random;
import org.junit.Test;
import org.junit.Assert;

import org.assertj.core.api.Assertions;
import org.apache.cassandra.exceptions.ConfigurationException;

public class SampledQueryEventLoggerOptionsTest
{
    @Test
    public void testSampledQueryEventLoggerOptions()
    {
        // Use seeded random for reproducible tests
        Random random = new Random(42);
        
        // Generate random sampling rates between 0.0 and 1.0
        double querySuccessRate = random.nextDouble();
        double queryFailureRate = random.nextDouble();
        double batchSuccessRate = random.nextDouble();
        double batchFailureRate = random.nextDouble();
        double executeSuccessRate = random.nextDouble();
        double executeFailureRate = random.nextDouble();
        double prepareSuccessRate = random.nextDouble();
        double prepareFailureRate = random.nextDouble();
        
        // Create options using builder
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder()
            .withQuerySuccessSampleRate(querySuccessRate)
            .withQueryFailureSampleRate(queryFailureRate)
            .withBatchSuccessSampleRate(batchSuccessRate)
            .withBatchFailureSampleRate(batchFailureRate)
            .withExecuteSuccessSampleRate(executeSuccessRate)
            .withExecuteFailureSampleRate(executeFailureRate)
            .withPrepareSuccessSampleRate(prepareSuccessRate)
            .withPrepareFailureSampleRate(prepareFailureRate)
            .build();
            
        // Verify all rates were set correctly
        Assert.assertEquals(querySuccessRate, options.query_success_sample_rate, 0.000001);
        Assert.assertEquals(queryFailureRate, options.query_failure_sample_rate, 0.000001);
        Assert.assertEquals(batchSuccessRate, options.batch_success_sample_rate, 0.000001);
        Assert.assertEquals(batchFailureRate, options.batch_failure_sample_rate, 0.000001);
        Assert.assertEquals(executeSuccessRate, options.execute_success_sample_rate, 0.000001);
        Assert.assertEquals(executeFailureRate, options.execute_failure_sample_rate, 0.000001);
        Assert.assertEquals(prepareSuccessRate, options.prepare_success_sample_rate, 0.000001);
        Assert.assertEquals(prepareFailureRate, options.prepare_failure_sample_rate, 0.000001);
        
        // Verify validation works
        SampledQueryEventLoggerOptions validated = SampledQueryEventLoggerOptions.validate(options);
        Assert.assertNotNull(validated);
        
        // Test copy constructor via builder
        SampledQueryEventLoggerOptions copy = new SampledQueryEventLoggerOptions.Builder(options).build();
        Assert.assertEquals(options.toString(), copy.toString());
    }

    @Test
    public void testInvalidRateShouldThrow()
    {
        // Test values outside valid range [0.0, 1.0]
        double[] invalidRates = {
            -0.1,        // Slightly below minimum
            -1.0,        // Well below minimum
            1.1,         // Slightly above maximum
            2.0,         // Well above maximum
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.NaN
        };
        
        for (double invalidRate : invalidRates) {
            // Test each setter method
            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withQuerySuccessSampleRate(invalidRate)
                    .build());

            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withQueryFailureSampleRate(invalidRate)
                    .build());

            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withBatchSuccessSampleRate(invalidRate)
                    .build());

            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withBatchFailureSampleRate(invalidRate)
                    .build());
                    
            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withExecuteSuccessSampleRate(invalidRate)
                    .build());
                    
            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withExecuteFailureSampleRate(invalidRate)
                    .build());
                    
            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withPrepareSuccessSampleRate(invalidRate)
                    .build());
                    
            Assertions.assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> 
                new SampledQueryEventLoggerOptions.Builder()
                    .withPrepareFailureSampleRate(invalidRate)
                    .build());
        }
    }
}

