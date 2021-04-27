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

public enum DiskUsageState
{
    NOT_AVAILABLE("Not Available"), // either disk usage guardrail is not enabled or gossip state is not ready
    SPACIOUS("Spacious"),           // below disk_usage_percentage_warn_threshold
    STUFFED("Stuffed"),             // exceeds disk_usage_percentage_warn_threshold but below disk_usage_percentage_failure_threshold
    FULL("Full");                   // exceeds disk_usage_percentage_failure_threshold

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
