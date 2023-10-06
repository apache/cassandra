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
    public void setCpuThresholdCur(long cpuThresholdCur);

    /**
     * Set cpu_threshold_one_minute
     */
    public void setCpuThresholdOneMinute(long cpuThresholdOneMinute);

    /**
     * Set pending_reads_threshold_cur
     */
    public void setPendingReadsThresholdCur(int pendingReadsThresholdCur);

    /**
     * Set pending_reads_threshold_one_minute
     */
    public void setPendingReadsThresholdOneMinute(int pendingReadsThresholdOneMinute);

    /**
     * Set pending_mutations_threshold_cur
     */
    public void setPendingMutationsThresholdCur(int pendingMutationsThresholdCur);

    /**
     * Set pending_mutations_threshold_one_minute
     */
    public void setPendingMutationsThresholdOneMinute(int pendingMutationsThresholdOneMinute);

    /**
     * Set pending_native_transport_threshold_cur
     */
    public void setPendingNativeTransportThresholdCur(int pendingNativeTransportThresholdCur);

    /**
     * Set pending_native_transport_threshold_one_minute
     */
    public void setPendingNativeTransportThresholdOneMinute(int pendingNativeTransportThresholdOneMinute);

    /**
     * Set percentage_of_traffic_to_throttling
     */
    public void setPercentageOfTrafficToThrottling(double percentageOfTrafficToThrottling);

    /**
     * Set more_aggressive_throttling_after_in_sec
     */
    public void setMoreAggressiveThrottlingAfterInSec(int moreAggressiveThrottlingAfterInSec);

    /**
     * Set reset_after_no_throttling_seen_in_sec
     */
    public void setResetAfterNoThrottlingSeenInSec(int resetAfterNoThrottlingSeenInSec);

    /**
     * Set aggressive_throttling_qps_ratio
     */
    public void setAggressiveThrottlingQpsRatio(double aggressiveThrottlingQpsRatio);

    /**
     * Set aggressive_throttling_latency_ratio
     */
    public void setAggressiveThrottlingLatencyRatio(double aggressiveThrottlingLatencyRatio);

    /**
     * Set ignore_keyspaces
     */
    public void setIgnoreKeyspaces(String ignoreKeyspaces);

    /**
     * Set health_check_init_delay_in_sec
     */
    public void setHealthCheckInitDelayInSec(int healthCheckInitDelayInSec);

    /**
     * Set health_check_period_in_sec
     */
    public void setHealthCheckFreqInSec(int healthCheckPeriodInSec);
}
