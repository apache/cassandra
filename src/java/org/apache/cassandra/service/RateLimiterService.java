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
package org.apache.cassandra.service;

import org.apache.cassandra.service.throttler.dynamic.ThrottlingOptions;
import org.apache.cassandra.utils.MBeanWrapper;

public class RateLimiterService implements RateLimiterServiceMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=RateLimiterService";

    private ThrottlingOptions throttlingOptions;

    public static final RateLimiterService instance = new RateLimiterService();

    private RateLimiterService()
    {
    }

    static {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }

    @Override
    public void setThrottlingOptions(ThrottlingOptions throttlingOptions)
    {
        this.throttlingOptions = throttlingOptions;
    }

    @Override
    public ThrottlingOptions getThrottlingOptions()
    {
        return throttlingOptions;
    }

    // setters for individual parameters starts from here
    @Override
    public void setEnabled(boolean enabled)
    {
        this.throttlingOptions.enabled = enabled;
    }

    @Override
    public void setCpuThresholdCur(long cpu_threshold_cur)
    {
        this.throttlingOptions.cpu_threshold_cur = cpu_threshold_cur;
    }

    @Override
    public void setCpuThresholdOneMinute(long cpu_threshold_one_minute)
    {
        this.throttlingOptions.cpu_threshold_one_minute = cpu_threshold_one_minute;
    }

    @Override
    public void setNrThrottlingThresholdCur(long nr_throttling_threshold_cur)
    {
        this.throttlingOptions.nr_throttling_threshold_cur = nr_throttling_threshold_cur;
    }

    @Override
    public void setNrThrottlingThresholdOneMinute(long nr_throttling_threshold_one_minute)
    {
        this.throttlingOptions.nr_throttling_threshold_one_minute = nr_throttling_threshold_one_minute;
    }

    @Override
    public void setPendingReadsThresholdCur(int pending_reads_threshold_cur)
    {
        this.throttlingOptions.pending_reads_threshold_cur = pending_reads_threshold_cur;
    }

    @Override
    public void setPendingReadsThresholdOneMinute(int pending_reads_threshold_one_minute)
    {
        this.throttlingOptions.pending_reads_threshold_one_minute = pending_reads_threshold_one_minute;
    }

    @Override
    public void setPendingMutationsThresholdCur(int pending_mutations_threshold_cur)
    {
        this.throttlingOptions.pending_mutations_threshold_cur = pending_mutations_threshold_cur;
    }

    @Override
    public void setPendingMutationsThresholdOneMinute(int pending_mutations_threshold_one_minute)
    {
        this.throttlingOptions.pending_mutations_threshold_one_minute = pending_mutations_threshold_one_minute;
    }

    @Override
    public void setPercentageOfTrafficeToThrottling(double percentage_of_traffice_to_throttling)
    {
        this.throttlingOptions.percentage_of_traffice_to_throttling = percentage_of_traffice_to_throttling;
    }

    @Override
    public void setMoreAggressiveThrottlingAfterInSec(int more_aggressive_throttling_after_in_sec)
    {
        this.throttlingOptions.more_aggressive_throttling_after_in_sec = more_aggressive_throttling_after_in_sec;
    }

    @Override
    public void setResetAfterNoThrottlingSeenInSec(int reset_after_no_throttling_seen_in_sec)
    {
        this.throttlingOptions.reset_after_no_throttling_seen_in_sec = reset_after_no_throttling_seen_in_sec;
    }

    @Override
    public void setAggressiveThrottlingQpsRatio(double aggressive_throttling_qps_ratio)
    {
        this.throttlingOptions.aggressive_throttling_qps_ratio = aggressive_throttling_qps_ratio;
    }
}
