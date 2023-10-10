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

        // ensure ignoreKeyspacesPattern aligns with ignore_keyspaces. This is because in the input throttlingOptions,
        // ignoreKeyspacesPattern might be not in sync with ignore_keyspaces, especially when the input comes from
        // DatabaseDescriptor which only updates the member variables that have literal representation in cassandra.yaml.
        // ignoreKeyspacesPattern doesn't have a literal representation in cassandra.yaml, as it is derived from
        // ignore_keyspaces.
        this.throttlingOptions.setIgnoreKeyspaces(throttlingOptions.ignore_keyspaces);
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
        this.throttlingOptions.setEnabled(enabled);
    }

    @Override
    public void setCpuThresholdCur(long cpuThresholdCur)
    {
        this.throttlingOptions.setCpuThresholdCur(cpuThresholdCur);
    }

    @Override
    public void setCpuThresholdOneMinute(long cpuThresholdOneMinute)
    {
        this.throttlingOptions.setCpuThresholdOneMinute(cpuThresholdOneMinute);
    }

    @Override
    public void setPendingReadsThresholdCur(int pendingReadsThresholdCur)
    {
        this.throttlingOptions.setPendingReadsThresholdCur(pendingReadsThresholdCur);
    }

    @Override
    public void setPendingReadsThresholdOneMinute(int pendingReadsThresholdOneMinute)
    {
        this.throttlingOptions.setPendingReadsThresholdOneMinute(pendingReadsThresholdOneMinute);
    }

    @Override
    public void setPendingMutationsThresholdCur(int pendingMutationsThresholdCur)
    {
        this.throttlingOptions.setPendingMutationsThresholdCur(pendingMutationsThresholdCur);
    }

    @Override
    public void setPendingMutationsThresholdOneMinute(int pendingMutationsThresholdOneMinute)
    {
        this.throttlingOptions.setPendingMutationsThresholdOneMinute(pendingMutationsThresholdOneMinute);
    }

    @Override
    public void setPercentageOfTrafficToThrottling(double percentageOfTrafficToThrottling)
    {
        this.throttlingOptions.setPercentageOfTrafficToThrottling(percentageOfTrafficToThrottling);
    }

    @Override
    public void setMoreAggressiveThrottlingAfterInSec(int moreAggressiveThrottlingAfterInSec)
    {
        this.throttlingOptions.setMoreAggressiveThrottlingAfterInSec(moreAggressiveThrottlingAfterInSec);
    }

    @Override
    public void setResetAfterNoThrottlingSeenInSec(int resetAfterNoThrottlingSeenInSec)
    {
        this.throttlingOptions.setResetAfterNoThrottlingSeenInSec(resetAfterNoThrottlingSeenInSec);
    }

    @Override
    public void setAggressiveThrottlingQpsRatio(double aggressiveThrottlingQpsRatio)
    {
        this.throttlingOptions.setAggressiveThrottlingQpsRatio(aggressiveThrottlingQpsRatio);
    }

    @Override
    public void setAggressiveThrottlingLatencyRatio(double aggressiveThrottlingLatencyRatio)
    {
        this.throttlingOptions.setAggressiveThrottlingLatencyRatio(aggressiveThrottlingLatencyRatio);
    }

    public void setIgnoreKeyspaces(String ignoreKeyspaces)
    {
        this.throttlingOptions.setIgnoreKeyspaces(ignoreKeyspaces);
    }

    @Override
    public void setHealthCheckInitDelayInSec(int healthCheckInitDelayInSec)
    {
        this.throttlingOptions.setHealthCheckInitDelayInSec(healthCheckInitDelayInSec);
    }

    @Override
    public void setHealthCheckFreqInSec(int healthCheckPeriodInSec)
    {
        this.throttlingOptions.setHealthCheckFreqInSec(healthCheckPeriodInSec);
    }
}
