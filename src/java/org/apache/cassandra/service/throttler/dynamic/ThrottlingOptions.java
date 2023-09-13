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

package org.apache.cassandra.service.throttler.dynamic;

import java.io.Serializable;
import java.util.regex.Pattern;

public class ThrottlingOptions implements Serializable
{
    // Also, adjust the default values based on the POC
    // TODO: think of the need to declare the following variables a vilotale, given they can be modified by nodetool at runtime

    // By default, we'd like to have rate limiter disabled.
    // When the field 'throttling_options' dones't exist in cassandra.yaml, which is the default, DatabaseDescriptor
    // will refer to the default value here.
    public boolean enabled = false;

    public long cpu_threshold_cur = 35;
    public long cpu_threshold_one_minute = 35;
    public long nr_throttling_threshold_cur = 1;
    public long nr_throttling_threshold_one_minute = 1;
    public int pending_reads_threshold_cur = 0;
    public int pending_reads_threshold_one_minute = 0;
    public int pending_mutations_threshold_cur = 0;
    public int pending_mutations_threshold_one_minute = 0;
    public double percentage_of_traffic_to_throttling = 0.1;
    public int more_aggressive_throttling_after_in_sec = 1 * 60; // 1 minutes
    public int reset_after_no_throttling_seen_in_sec = 15 * 60; // 15 minutes
    public double aggressive_throttling_qps_ratio = 4;
    public double aggressive_throttling_latency_ratio = 4;
    public String ignore_keyspaces = "system.*|pingless";
    public int health_check_init_delay_in_sec = 60;
    public int health_check_freq_in_sec = 1;

    private Pattern ignoreKeyspacesPattern = Pattern.compile(ignore_keyspaces);

    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public long getCpuThresholdCur()
    {
        return cpu_threshold_cur;
    }

    public void setCpuThresholdCur(long cpu_threshold_cur)
    {
        this.cpu_threshold_cur = cpu_threshold_cur;
    }

    public long getCpuThresholdOneMinute()
    {
        return cpu_threshold_one_minute;
    }

    public void setCpuThresholdOneMinute(long cpu_threshold_one_minute)
    {
        this.cpu_threshold_one_minute = cpu_threshold_one_minute;
    }

    public long getNrThrottlingThresholdCur()
    {
        return nr_throttling_threshold_cur;
    }

    public void setNrThrottlingThresholdCur(long nr_throttling_threshold_cur)
    {
        this.nr_throttling_threshold_cur = nr_throttling_threshold_cur;
    }

    public long getNrThrottlingThresholdOneMinute()
    {
        return nr_throttling_threshold_one_minute;
    }

    public void setNrThrottlingThresholdOneMinute(long nr_throttling_threshold_one_minute)
    {
        this.nr_throttling_threshold_one_minute = nr_throttling_threshold_one_minute;
    }

    public int getPendingReadsThresholdCur()
    {
        return pending_reads_threshold_cur;
    }

    public void setPendingReadsThresholdCur(int pending_reads_threshold_cur)
    {
        this.pending_reads_threshold_cur = pending_reads_threshold_cur;
    }

    public int getPendingReadsThresholdOneMinute()
    {
        return pending_reads_threshold_one_minute;
    }

    public void setPendingReadsThresholdOneMinute(int pending_reads_threshold_one_minute)
    {
        this.pending_reads_threshold_one_minute = pending_reads_threshold_one_minute;
    }

    public int getPendingMutationsThresholdCur()
    {
        return pending_mutations_threshold_cur;
    }

    public void setPendingMutationsThresholdCur(int pending_mutations_threshold_cur)
    {
        this.pending_mutations_threshold_cur = pending_mutations_threshold_cur;
    }

    public int getPendingMutationsThresholdOneMinute()
    {
        return pending_mutations_threshold_one_minute;
    }

    public void setPendingMutationsThresholdOneMinute(int pending_mutations_threshold_one_minute)
    {
        this.pending_mutations_threshold_one_minute = pending_mutations_threshold_one_minute;
    }

    public double getPercentageOfTrafficToThrottling()
    {
        return percentage_of_traffic_to_throttling;
    }

    public void setPercentageOfTrafficToThrottling(double percentage_of_traffic_to_throttling)
    {
        this.percentage_of_traffic_to_throttling = percentage_of_traffic_to_throttling;
    }

    public int getMoreAggressiveThrottlingAfterInSec()
    {
        return more_aggressive_throttling_after_in_sec;
    }

    public void setMoreAggressiveThrottlingAfterInSec(int more_aggressive_throttling_after_in_sec)
    {
        this.more_aggressive_throttling_after_in_sec = more_aggressive_throttling_after_in_sec;
    }

    public int getResetAfterNoThrottlingSeenInSec()
    {
        return reset_after_no_throttling_seen_in_sec;
    }

    public void setResetAfterNoThrottlingSeenInSec(int reset_after_no_throttling_seen_in_sec)
    {
        this.reset_after_no_throttling_seen_in_sec = reset_after_no_throttling_seen_in_sec;
    }

    public double getAggressiveThrottlingQpsRatio()
    {
        return aggressive_throttling_qps_ratio;
    }

    public void setAggressiveThrottlingQpsRatio(double aggressive_throttling_qps_ratio)
    {
        this.aggressive_throttling_qps_ratio = aggressive_throttling_qps_ratio;
    }

    public double getAggressiveThrottlingLatencyRatio()
    {
        return aggressive_throttling_latency_ratio;
    }

    public void setAggressiveThrottlingLatencyRatio(double aggressive_throttling_latency_ratio)
    {
        this.aggressive_throttling_latency_ratio = aggressive_throttling_latency_ratio;
    }

    public Pattern getIgnoreKeyspacesPattern()
    {
        return ignoreKeyspacesPattern;
    }

    public void setIgnoreKeyspaces(String ignoreKeyspaces)
    {
        ignore_keyspaces = ignoreKeyspaces;
        ignoreKeyspacesPattern = Pattern.compile(ignoreKeyspaces);
    }

    public int getHealthCheckInitDelayInSec()
    {
        return health_check_init_delay_in_sec;
    }

    public void setHealthCheckInitDelayInSec(int healthCheckInitDelayInSec)
    {
        this.health_check_init_delay_in_sec = healthCheckInitDelayInSec;
    }

    public int getHealthCheckPeriodInSec()
    {
        return health_check_freq_in_sec;
    }

    public void setHealthCheckFreqInSec(int healthCheckFreqInSec)
    {
        this.health_check_freq_in_sec = healthCheckFreqInSec;
    }

    public String toString()
    {
        return "enabled: " + enabled + "\n" +
               "current CPU threshold: " + cpu_threshold_cur + "\n" +
               "one minute CPU threshold: " + cpu_threshold_one_minute + "\n" +
               "current nr_throttled threshold: " + nr_throttling_threshold_cur + "\n" +
               "one minute nr_throttled threshold: " + nr_throttling_threshold_one_minute + "\n" +
               "current pending reads threshold: " + pending_reads_threshold_cur + "\n" +
               "one minute pending reads threshold: " + pending_reads_threshold_one_minute + "\n" +
               "current pending mutations threshold: " + pending_mutations_threshold_cur + "\n" +
               "one minute pending mutations threshold: " + pending_mutations_threshold_one_minute + "\n" +
               "percentage of traffic to throttle: " + percentage_of_traffic_to_throttling + "\n" +
               "more aggressive throttling after in seconds: " + more_aggressive_throttling_after_in_sec + "\n" +
               "reset after no throttling seen in seconds: " + reset_after_no_throttling_seen_in_sec + "\n" +
               "aggressive throttling qps ratio: " + aggressive_throttling_qps_ratio + "\n" +
               "aggressive throttling latency ratio: " + aggressive_throttling_latency_ratio + "\n" +
               "ignore keyspaces: '" + ignore_keyspaces + '\'' + "\n" +
               "health initial delay in sec: '" + health_check_init_delay_in_sec + '\'' + "\n" +
               "health check frequency in sec: '" + health_check_freq_in_sec + '\'';
    }
}
