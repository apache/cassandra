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
import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "getauditlog", description = "Print configuration of audit log if enabled, otherwise the configuration reflected in cassandra.yaml")
public class GetAuditLog extends NodeTool.NodeToolCmd
{
    @Override
    protected void execute(NodeProbe probe)
    {
        final TableBuilder tableBuilder = new TableBuilder();

        tableBuilder.add("enabled", Boolean.toString(probe.getStorageService().isAuditLogEnabled()));

        final AuditLogOptions options = probe.getAuditLogOptions();

        tableBuilder.add("logger", options.logger.class_name);
        tableBuilder.add("audit_logs_dir", options.audit_logs_dir);
        tableBuilder.add("archive_command", options.archive_command);
        tableBuilder.add("roll_cycle", options.roll_cycle);
        tableBuilder.add("block", Boolean.toString(options.block));
        tableBuilder.add("max_log_size", Long.toString(options.max_log_size));
        tableBuilder.add("max_queue_weight", Integer.toString(options.max_queue_weight));
        tableBuilder.add("max_archive_retries", Long.toString(options.max_archive_retries));
        tableBuilder.add("included_keyspaces", options.included_keyspaces);
        tableBuilder.add("excluded_keyspaces", options.excluded_keyspaces);
        tableBuilder.add("included_categories", options.included_categories);
        tableBuilder.add("excluded_categories", options.excluded_categories);
        tableBuilder.add("included_users", options.included_users);
        tableBuilder.add("excluded_users", options.excluded_users);

        tableBuilder.printTo(probe.output().out);
    }
}
