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

@Command(name = "enableauditlog", description = "Enable the audit log")
public class EnableAuditLog extends NodeToolCmd
{
    @Option(title = "logger", name = { "--logger" }, description = "Logger name to be used for AuditLogging. Default BinAuditLogger. If not set the value from cassandra.yaml will be used")
    private String logger = null;

    @Option(title = "included_keyspaces", name = { "--included-keyspaces" }, description = "Comma separated list of keyspaces to be included for audit log. If not set the value from cassandra.yaml will be used")
    private String included_keyspaces = null;

    @Option(title = "excluded_keyspaces", name = { "--excluded-keyspaces" }, description = "Comma separated list of keyspaces to be excluded for audit log. If not set the value from cassandra.yaml will be used")
    private String excluded_keyspaces = null;

    @Option(title = "included_categories", name = { "--included-categories" }, description = "Comma separated list of Audit Log Categories to be included for audit log. If not set the value from cassandra.yaml will be used")
    private String included_categories = null;

    @Option(title = "excluded_categories", name = { "--excluded-categories" }, description = "Comma separated list of Audit Log Categories to be excluded for audit log. If not set the value from cassandra.yaml will be used")
    private String excluded_categories = null;

    @Option(title = "included_users", name = { "--included-users" }, description = "Comma separated list of users to be included for audit log. If not set the value from cassandra.yaml will be used")
    private String included_users = null;

    @Option(title = "excluded_users", name = { "--excluded-users" }, description = "Comma separated list of users to be excluded for audit log. If not set the value from cassandra.yaml will be used")
    private String excluded_users = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.enableAuditLog(logger, included_keyspaces, excluded_keyspaces, included_categories, excluded_categories, included_users, excluded_users);
    }
}