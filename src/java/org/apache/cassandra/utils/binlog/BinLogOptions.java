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
    public String archive_command = StringUtils.EMPTY;

    /**
     * enable if a user should be able to set the archive command via nodetool/jmx
     *
     * do not make this a hotprop.
     */
    public boolean allow_nodetool_archive_command = false;
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
}
