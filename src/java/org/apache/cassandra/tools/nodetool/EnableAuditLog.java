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

import java.util.Collections;

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "enableauditlog", description = "Enable the audit log")
public class EnableAuditLog extends AbstractCommand
{
    @Option(paramLabel = "logger", names = { "--logger" }, description = "Logger name to be used for AuditLogging. Default BinAuditLogger. If not set the value from cassandra.yaml will be used")
    private String logger = null;

    @Option(paramLabel = "included_keyspaces", names = { "--included-keyspaces" }, description = "Comma separated list of keyspaces to be included for audit log. If not set the value from cassandra.yaml will be used")
    private String included_keyspaces = null;

    @Option(paramLabel = "excluded_keyspaces", names = { "--excluded-keyspaces" }, description = "Comma separated list of keyspaces to be excluded for audit log. If not set the value from cassandra.yaml will be used")
    private String excluded_keyspaces = null;

    @Option(paramLabel = "included_categories", names = { "--included-categories" }, description = "Comma separated list of Audit Log Categories to be included for audit log. If not set the value from cassandra.yaml will be used")
    private String included_categories = null;

    @Option(paramLabel = "excluded_categories", names = { "--excluded-categories" }, description = "Comma separated list of Audit Log Categories to be excluded for audit log. If not set the value from cassandra.yaml will be used")
    private String excluded_categories = null;

    @Option(paramLabel = "included_users", names = { "--included-users" }, description = "Comma separated list of users to be included for audit log. If not set the value from cassandra.yaml will be used")
    private String included_users = null;

    @Option(paramLabel = "excluded_users", names = { "--excluded-users" }, description = "Comma separated list of users to be excluded for audit log. If not set the value from cassandra.yaml will be used")
    private String excluded_users = null;

    @Option(paramLabel = "roll_cycle", names = { "--roll-cycle" }, description = "How often to roll the log file (MINUTELY, HOURLY, DAILY).")
    private String rollCycle = null;

    @Option(paramLabel = "blocking", names = { "--blocking" }, description = "If the queue is full whether to block producers or drop samples [true|false].")
    private String blocking = null;

    @Option(paramLabel = "max_queue_weight", names = { "--max-queue-weight" }, description = "Maximum number of bytes of query data to queue to disk before blocking or dropping samples.")
    private int maxQueueWeight = Integer.MIN_VALUE;

    @Option(paramLabel = "max_log_size", names = { "--max-log-size" }, description = "How many bytes of log data to store before dropping segments. Might not be respected if a log file hasn't rolled so it can be deleted.")
    private long maxLogSize = Long.MIN_VALUE;

    @Option(paramLabel = "archive_command", names = { "--archive-command" }, description = "Command that will handle archiving rolled audit log files." +
                                                                                   " Format is \"/path/to/script.sh %%path\" where %%path will be replaced with the file to archive" +
                                                                                   " Enable this by setting the audit_logging_options.allow_nodetool_archive_command: true in the config.")

    private String archiveCommand = null;

    @Option(paramLabel = "archive_retries", names = { "--max-archive-retries" }, description = "Max number of archive retries.")
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
        probe.enableAuditLog(logger, Collections.EMPTY_MAP, included_keyspaces, excluded_keyspaces, included_categories, excluded_categories, included_users, excluded_users,
                             archiveRetries, bblocking, rollCycle, maxLogSize, maxQueueWeight, archiveCommand);
    }
}
