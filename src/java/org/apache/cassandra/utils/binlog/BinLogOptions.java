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

import org.apache.commons.lang3.StringUtils;

public class BinLogOptions
{
    public static final String DEFAULT_ARCHIVE_COMMAND = StringUtils.EMPTY;
    public static final String DEFAULT_ROLL_CYCLE = "HOURLY";
    public static final boolean DEFAULT_BLOCK = true;
    public static final int DEFAULT_MAX_QUEUE_WEIGHT = 256 * 1024 * 1024;
    public static final long DEFAULT_MAX_LOG_SIZE = 16L * 1024L * 1024L * 1024L;
    public static final int DEFAULT_MAX_ARCHIVE_RETRIES = 10;

    public String archive_command = DEFAULT_ARCHIVE_COMMAND;
    /**
     * How often to roll BinLog segments so they can potentially be reclaimed. Available options are:
     * MINUTELY, HOURLY, DAILY, LARGE_DAILY, XLARGE_DAILY, HUGE_DAILY.
     * For more options, refer: net.openhft.chronicle.queue.RollCycles
     */
    public String roll_cycle = DEFAULT_ROLL_CYCLE;
    /**
     * Indicates if the BinLog should block if the it falls behind or should drop bin log records.
     * Default is set to true so that BinLog records wont be lost
     */
    public boolean block = DEFAULT_BLOCK;

    /**
     * Maximum weight of in memory queue for records waiting to be written to the binlog file
     * before blocking or dropping the log records. For advanced configurations
     */
    public int max_queue_weight = DEFAULT_MAX_QUEUE_WEIGHT;

    /**
     * Maximum size of the rolled files to retain on disk before deleting the oldest file. For advanced configurations.
     */
    public long max_log_size = DEFAULT_MAX_LOG_SIZE;

    /**
     * Limit the number of times to retry a command.
     */
    public int max_archive_retries = DEFAULT_MAX_ARCHIVE_RETRIES;
}
