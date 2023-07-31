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

public interface RateLimiterServiceMBean
{
    /**
     * Set throttling options
     */
    public void setThrottlingOptions(ThrottlingOptions throttlingOptions);

    /**
     * Get throttling options
     */
    public ThrottlingOptions getThrottlingOptions();

    /**
     * Set enabled
     */
    public void setEnabled(boolean enabled);

    /**
     * Set cpu_threshold_cur
     */
    public void setCpuThresholdCur(long cpu_threshold_cur);

    /**
     * Set cpu_threshold_one_minute
     */
    public void setCpuThresholdOneMinute(long cpu_threshold_one_minute);

    /**
     * Set nr_throttling_threshold_cur
     */
    public void setNrThrottlingThresholdCur(long nr_throttling_threshold_cur);

    /**
     * Set nr_throttling_threshold_one_minute
     */
    public void setNrThrottlingThresholdOneMinute(long nr_throttling_threshold_one_minute);

    /**
     * Set pending_reads_threshold_cur
     */
    public void setPendingReadsThresholdCur(int pending_reads_threshold_cur);

    /**
     * Set pending_reads_threshold_one_minute
     */
    public void setPendingReadsThresholdOneMinute(int pending_reads_threshold_one_minute);

    /**
     * Set pending_mutations_threshold_cur
     */
    public void setPendingMutationsThresholdCur(int pending_mutations_threshold_cur);

    /**
     * Set pending_mutations_threshold_one_minute
     */
    public void setPendingMutationsThresholdOneMinute(int pending_mutations_threshold_one_minute);

    /**
     * Set percentage_of_traffice_to_throttling
     */
    public void setPercentageOfTrafficeToThrottling(double percentage_of_traffice_to_throttling);

    /**
     * Set more_aggressive_throttling_after_in_sec
     */
    public void setMoreAggressiveThrottlingAfterInSec(int more_aggressive_throttling_after_in_sec);

    /**
     * Set reset_after_no_throttling_seen_in_sec
     */
    public void setResetAfterNoThrottlingSeenInSec(int reset_after_no_throttling_seen_in_sec);

    /**
     * Set aggressive_throttling_qps_ratio
     */
    public void setAggressiveThrottlingQpsRatio(double aggressive_throttling_qps_ratio);
}
