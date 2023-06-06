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

package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "enablefullquerylog", description = "Enable full query logging, defaults for the options are configured in cassandra.yaml")
public class EnableFullQueryLog extends NodeToolCmd
{
    @Option(title = "roll_cycle", name = {"--roll-cycle"}, description = "How often to roll the log file (MINUTELY, HOURLY, DAILY).")
    private String rollCycle = null;

    @Option(title = "blocking", name = {"--blocking"}, description = "If the queue is full whether to block producers or drop samples [true|false].")
    private String blocking = null;

    @Option(title = "max_queue_weight", name = {"--max-queue-weight"}, description = "Maximum number of bytes of query data to queue to disk before blocking or dropping samples.")
    private int maxQueueWeight = Integer.MIN_VALUE;

    @Option(title = "max_log_size", name = {"--max-log-size"}, description = "How many bytes of log data to store before dropping segments. Might not be respected if a log file hasn't rolled so it can be deleted.")
    private long maxLogSize = Long.MIN_VALUE;

    @Option(title = "path", name = {"--path"}, description = "Path to store the full query log at. Will have it's contents recursively deleted.")
    private String path = null;

    @Option(title = "archive_command", name = {"--archive-command"}, description = "Command that will handle archiving rolled full query log files." +
                                                                                   " Format is \"/path/to/script.sh %path\" where %path will be replaced with the file to archive" +
                                                                                   " Enable this by setting the full_query_logging_options.allow_nodetool_archive_command: true in the config.")
    private String archiveCommand = null;

    @Option(title = "archive_retries", name = {"--max-archive-retries"}, description = "Max number of archive retries.")
    private int archiveRetries = Integer.MIN_VALUE;

    @Override
    public void execute(NodeProbe probe)
    {
        Boolean bblocking = null;
        if (blocking != null)
        {
            if (!blocking.equalsIgnoreCase("TRUE") && !blocking.equalsIgnoreCase("FALSE"))
                throw new IllegalArgumentException("Invalid [" + blocking + "]. Blocking only accepts 'true' or 'false'.");
            else
                bblocking = Boolean.parseBoolean(blocking);
        }
        probe.enableFullQueryLogger(path, rollCycle, bblocking, maxQueueWeight, maxLogSize, archiveCommand, archiveRetries);
    }
}
