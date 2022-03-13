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

package org.apache.cassandra.utils.binlog;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

public class BinLogOptions
{
    public String archive_command = StringUtils.EMPTY;
    /**
     * How often to roll BinLog segments so they can potentially be reclaimed. Available options are:
     * MINUTELY, HOURLY, DAILY, LARGE_DAILY, XLARGE_DAILY, HUGE_DAILY.
     * For more options, refer: net.openhft.chronicle.queue.RollCycles
     */
    public String roll_cycle = "HOURLY";
    /**
     * Indicates if the BinLog should block if the it falls behind or should drop bin log records.
     * Default is set to true so that BinLog records wont be lost
     */
    public boolean block = true;

    /**
     * Maximum weight of in memory queue for records waiting to be written to the binlog file
     * before blocking or dropping the log records. For advanced configurations
     */
    public int max_queue_weight = 256 * 1024 * 1024;

    /**
     * Maximum size of the rolled files to retain on disk before deleting the oldest file. For advanced configurations.
     */
    public long max_log_size = 16L * 1024L * 1024L * 1024L;

    /**
     * Limit the number of times to retry a command.
     */
    public int max_archive_retries = 10;

    public static class Builder
    {
        private String archiveCommand;
        private String rollCycle;
        private boolean block = true;
        private int maxQueueWeight;
        private long maxLogSize;
        private int maxArchiveRetries;

        public Builder(BinLogOptions options)
        {
            this.archiveCommand = options.archive_command;
            this.rollCycle = options.roll_cycle;
            this.block = options.block;
            this.maxLogSize = options.max_log_size;
            this.maxQueueWeight = options.max_queue_weight;
            this.maxArchiveRetries = options.max_archive_retries;
        }

        public Builder()
        {
            this(new BinLogOptions());
        }

        public Builder withRollCycle(final String rollCycle)
        {
            sanitise(rollCycle).map(v -> this.rollCycle = v.toUpperCase());
            return this;
        }

        public Builder withArchiveCommand(final String archiveCommand)
        {
            if (archiveCommand != null)
            {
                this.archiveCommand = archiveCommand;
            }
            return this;
        }

        public Builder withBlock(final Boolean block)
        {
            if (block != null)
            {
                this.block = block;
            }
            return this;
        }

        public Builder withMaxLogSize(final long maxLogSize)
        {
            if (maxLogSize != Long.MIN_VALUE)
            {
                this.maxLogSize = maxLogSize;
            }
            return this;
        }

        public Builder withMaxArchiveRetries(final int maxArchiveRetries)
        {
            if (maxArchiveRetries != Integer.MIN_VALUE)
            {
                this.maxArchiveRetries = maxArchiveRetries;
            }
            return this;
        }

        public Builder withMaxQueueWeight(final int maxQueueWeight)
        {
            if (maxQueueWeight != Integer.MIN_VALUE)
            {
                this.maxQueueWeight = maxQueueWeight;
            }
            return this;
        }

        public BinLogOptions build()
        {
            BinLogOptions options = new BinLogOptions();
            options.max_queue_weight = this.maxQueueWeight;
            options.roll_cycle = this.rollCycle;
            options.max_log_size = this.maxLogSize;
            options.block = this.block;
            options.archive_command = this.archiveCommand;
            options.max_archive_retries = this.maxArchiveRetries;
            return options;
        }

        public static Optional<String> sanitise(final String input)
        {
            if (input == null || input.trim().isEmpty())
                return Optional.empty();

            return Optional.of(Arrays.stream(input.split(","))
                                     .map(String::trim)
                                     .map(Strings::emptyToNull)
                                     .filter(Objects::nonNull)
                                     .collect(Collectors.joining(",")));
        }
    }
}
