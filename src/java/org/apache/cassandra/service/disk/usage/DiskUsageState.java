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

package org.apache.cassandra.service.disk.usage;

import org.apache.cassandra.db.guardrails.GuardrailsConfig;

public enum DiskUsageState
{
    /** Either disk usage guardrail is not enabled or gossip state is not ready. */
    NOT_AVAILABLE("Not Available"),

    /**
     * Disk usage is below both {@link GuardrailsConfig#getDataDiskUsagePercentageWarnThreshold()} ()} and
     * {@link GuardrailsConfig#getDataDiskUsagePercentageFailThreshold()}.
     */
    SPACIOUS("Spacious"),

    /**
     * Disk usage exceeds {@link GuardrailsConfig#getDataDiskUsagePercentageWarnThreshold()} but is below
     * {@link GuardrailsConfig#getDataDiskUsagePercentageFailThreshold()}.
     */
    STUFFED("Stuffed"),

    /** Disk usage exceeds {@link GuardrailsConfig#getDataDiskUsagePercentageFailThreshold()}. */
    FULL("Full");

    private final String msg;

    DiskUsageState(String msg)
    {
        this.msg = msg;
    }

    public boolean isFull()
    {
        return this == FULL;
    }

    public boolean isStuffed()
    {
        return this == STUFFED;
    }

    public boolean isStuffedOrFull()
    {
        return isFull() || isStuffed();
    }

    @Override
    public String toString()
    {
        return msg;
    }
}
