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
package org.apache.cassandra.config;

import org.apache.cassandra.exceptions.ConfigurationException;

public class TrackWarnings
{
    public volatile boolean enabled = false; // should set to true in 4.2
    public final LongByteThreshold coordinator_read_size = new LongByteThreshold();
    public final LongByteThreshold local_read_size = new LongByteThreshold();
    public final IntByteThreshold row_index_size = new IntByteThreshold();

    public void validate(String prefix)
    {
        prefix += ".";
        coordinator_read_size.validate(prefix + "coordinator_read_size");
        local_read_size.validate(prefix + "local_read_size");
        row_index_size.validate(prefix + "row_index_size");
    }

    public static class LongByteThreshold
    {
        public volatile long warn_threshold_kb = 0;
        public volatile long abort_threshold_kb = 0;

        public long getWarnThresholdKb()
        {
            return warn_threshold_kb;
        }

        public void setWarnThresholdKb(long value)
        {
            warn_threshold_kb = Math.max(value, 0);
        }

        public long getAbortThresholdKb()
        {
            return abort_threshold_kb;
        }

        public void setAbortThresholdKb(long value)
        {
            abort_threshold_kb = Math.max(value, 0);
        }

        public void validate(String prefix)
        {
            warn_threshold_kb = Math.max(warn_threshold_kb, 0);
            abort_threshold_kb = Math.max(abort_threshold_kb, 0);

            if (abort_threshold_kb != 0 && abort_threshold_kb < warn_threshold_kb)
                throw new ConfigurationException(String.format("abort_threshold_kb (%d) must be greater than or equal to warn_threshold_kb (%d); see %s",
                                                               abort_threshold_kb, warn_threshold_kb, prefix));
        }
    }

    public static class IntByteThreshold
    {
        public volatile int warn_threshold_kb = 0;
        public volatile int abort_threshold_kb = 0;

        public int getWarnThresholdKb()
        {
            return warn_threshold_kb;
        }

        public void setWarnThresholdKb(int value)
        {
            warn_threshold_kb = Math.max(value, 0);
        }

        public int getAbortThresholdKb()
        {
            return abort_threshold_kb;
        }

        public void setAbortThresholdKb(int value)
        {
            abort_threshold_kb = Math.max(value, 0);
        }

        public void validate(String prefix)
        {
            warn_threshold_kb = Math.max(warn_threshold_kb, 0);
            abort_threshold_kb = Math.max(abort_threshold_kb, 0);

            if (abort_threshold_kb != 0 && abort_threshold_kb < warn_threshold_kb)
                throw new ConfigurationException(String.format("abort_threshold_kb (%d) must be greater than or equal to warn_threshold_kb (%d); see %s",
                                                               abort_threshold_kb, warn_threshold_kb, prefix));
        }
    }
}
