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
        public volatile DataStorageSpec warn_threshold = new DataStorageSpec("0KiB");
        public volatile DataStorageSpec abort_threshold = new DataStorageSpec("0KiB");

        public long getWarnThresholdKiB()
        {
            return warn_threshold.toKibibytes();
        }

        public void setWarnThresholdKiB(long value)
        {
            warn_threshold = DataStorageSpec.inKibibytes(Math.max(value, 0));
        }

        public long getAbortThresholdKiB()
        {
            return abort_threshold.toKibibytes();
        }

        public void setAbortThresholdKiB(long value)
        {
            abort_threshold = DataStorageSpec.inKibibytes(Math.max(value, 0));
        }

        public void validate(String prefix)
        {
            warn_threshold = DataStorageSpec.inKibibytes(Math.max(warn_threshold.toKibibytes(), 0));
            abort_threshold = DataStorageSpec.inKibibytes(Math.max(abort_threshold.toKibibytes(), 0));

            if (abort_threshold.toKibibytes() != 0 && abort_threshold.toKibibytes() < warn_threshold.toKibibytes())
                throw new ConfigurationException(String.format("abort_threshold (%s) must be greater than or equal to warn_threshold (%s); see %s",
                                                               abort_threshold, warn_threshold, prefix));
        }
    }

    public static class IntByteThreshold
    {
        public volatile DataStorageSpec warn_threshold = new DataStorageSpec("0KiB");
        public volatile DataStorageSpec abort_threshold = new DataStorageSpec("0KiB");

        public int getWarnThresholdKb()
        {
            return warn_threshold.toKibibytesAsInt();
        }

        public void setWarnThresholdKb(int value)
        {
            warn_threshold = DataStorageSpec.inKibibytes(Math.max(value, 0));
        }

        public int getAbortThresholdKiB()
        {
            return abort_threshold.toKibibytesAsInt();
        }

        public void setAbortThresholdKiB(int value)
        {
            abort_threshold = DataStorageSpec.inKibibytes(Math.max(value, 0));
        }

        public void validate(String prefix)
        {
            warn_threshold = DataStorageSpec.inKibibytes(Math.max(warn_threshold.toKibibytes(), 0));
            abort_threshold = DataStorageSpec.inKibibytes(Math.max(abort_threshold.toKibibytes(), 0));

            if (abort_threshold.toKibibytesAsInt() != 0 && abort_threshold.toKibibytesAsInt() < warn_threshold.toKibibytesAsInt())
                throw new ConfigurationException(String.format("abort_threshold (%s) must be greater than or equal to warn_threshold (%s); see %s",
                                                               abort_threshold, warn_threshold, prefix));
        }
    }
}
