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


import org.apache.cassandra.exceptions.ConfigurationException;



public class SampledQueryEventLoggerOptions 
{
    public double query_success_sample_rate = 0.0;
    public double query_failure_sample_rate = 0.0;
    public double batch_success_sample_rate = 0.0;
    public double batch_failure_sample_rate = 0.0;
    public double execute_success_sample_rate = 0.0;
    public double execute_failure_sample_rate = 0.0;
    public double prepare_success_sample_rate = 0.0;
    public double prepare_failure_sample_rate = 0.0;

    public SampledQueryEventLoggerOptions() {}

    public static SampledQueryEventLoggerOptions validate(final SampledQueryEventLoggerOptions options) {
        validateRate(options.query_success_sample_rate);
        validateRate(options.query_failure_sample_rate);
        validateRate(options.batch_success_sample_rate);
        validateRate(options.batch_failure_sample_rate);
        validateRate(options.execute_success_sample_rate);
        validateRate(options.execute_failure_sample_rate);
        validateRate(options.prepare_success_sample_rate);
        validateRate(options.prepare_failure_sample_rate);

        return options;
    }

    private static void validateRate(final double rate) {
        if (Double.isNaN(rate) || Double.isInfinite(rate) || rate < 0.0 || rate > 1.0) {
            throw new ConfigurationException(String.format("rate must be in the range [0.0, 1.0], but was %f", rate));
        }
    }

    public static class Builder 
    {
        private double query_success_sample_rate;
        private double query_failure_sample_rate;
        private double batch_success_sample_rate;
        private double batch_failure_sample_rate;
        private double execute_success_sample_rate;
        private double execute_failure_sample_rate;
        private double prepare_success_sample_rate;
        private double prepare_failure_sample_rate;

        public Builder() 
        {
            this(new SampledQueryEventLoggerOptions());
        }

        public Builder(final SampledQueryEventLoggerOptions opts) 
        {
            this.query_success_sample_rate = opts.query_success_sample_rate;
            this.query_failure_sample_rate = opts.query_failure_sample_rate;
            this.batch_success_sample_rate = opts.batch_success_sample_rate;
            this.batch_failure_sample_rate = opts.batch_failure_sample_rate;
            this.execute_success_sample_rate = opts.execute_success_sample_rate;
            this.execute_failure_sample_rate = opts.execute_failure_sample_rate;
            this.prepare_success_sample_rate = opts.prepare_success_sample_rate;
            this.prepare_failure_sample_rate = opts.prepare_failure_sample_rate;
        }

        public Builder withQuerySuccessSampleRate(final double sampleRate) 
        {
            this.query_success_sample_rate = sampleRate;
            return this;
        }

        public Builder withQueryFailureSampleRate(final double sampleRate) 
        {
            this.query_failure_sample_rate = sampleRate;
            return this;
        }

        public Builder withBatchSuccessSampleRate(final double sampleRate) 
        {
            this.batch_success_sample_rate = sampleRate;
            return this;
        }

        public Builder withBatchFailureSampleRate(final double sampleRate) 
        {
            this.batch_failure_sample_rate = sampleRate;
            return this;
        }

        public Builder withExecuteSuccessSampleRate(final double sampleRate) 
        {
            this.execute_success_sample_rate = sampleRate;
            return this;
        }

        public Builder withExecuteFailureSampleRate(final double sampleRate) 
        {
            this.execute_failure_sample_rate = sampleRate;
            return this;
        }

        public Builder withPrepareSuccessSampleRate(final double sampleRate) 
        {
            this.prepare_success_sample_rate = sampleRate;
            return this;
        }

        public Builder withPrepareFailureSampleRate(final double sampleRate) 
        {
            this.prepare_failure_sample_rate = sampleRate;
            return this;
        }

        public SampledQueryEventLoggerOptions build()
        {
            final SampledQueryEventLoggerOptions opts = new SampledQueryEventLoggerOptions();
            opts.query_success_sample_rate = this.query_success_sample_rate;
            opts.query_failure_sample_rate = this.query_failure_sample_rate;
            opts.batch_success_sample_rate = this.batch_success_sample_rate;
            opts.batch_failure_sample_rate = this.batch_failure_sample_rate;
            opts.execute_success_sample_rate = this.execute_success_sample_rate;
            opts.execute_failure_sample_rate = this.execute_failure_sample_rate;
            opts.prepare_success_sample_rate = this.prepare_success_sample_rate;
            opts.prepare_failure_sample_rate = this.prepare_failure_sample_rate;

            SampledQueryEventLoggerOptions.validate(opts);

            return opts;
        }
    }

    public String toString() {
        return "SampledQueryEventLoggerOptions{" + '\'' +
            "query_success_sample_rate=" + query_success_sample_rate + '\'' +
            ", query_failure_sample_rate=" + query_failure_sample_rate + '\'' +
            ", batch_success_sample_rate=" + batch_success_sample_rate + '\'' +
            ", batch_failure_sample_rate=" + batch_failure_sample_rate + '\'' +
            ", execute_success_sample_rate=" + execute_success_sample_rate + '\'' +
            ", execute_failure_sample_rate=" + execute_failure_sample_rate + '\'' +
            ", prepare_success_sample_rate=" + prepare_success_sample_rate + '\'' +
            ", prepare_failure_sample_rate=" + prepare_failure_sample_rate + '\'' +
        '}';
    }
}