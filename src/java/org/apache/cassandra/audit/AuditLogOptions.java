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
package org.apache.cassandra.audit;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.utils.binlog.BinLogOptions;

public class AuditLogOptions extends BinLogOptions
{
    public volatile boolean enabled = false;
    public String logger = BinAuditLogger.class.getSimpleName();
    public String included_keyspaces = StringUtils.EMPTY;
    // CASSANDRA-14498: By default, system, system_schema and system_virtual_schema are excluded, but these can be included via cassandra.yaml
    public String excluded_keyspaces = "system,system_schema,system_virtual_schema";
    public String included_categories = StringUtils.EMPTY;
    public String excluded_categories = StringUtils.EMPTY;
    public String included_users = StringUtils.EMPTY;
    public String excluded_users = StringUtils.EMPTY;

    /**
     * AuditLogs directory can be configured using `cassandra.logdir.audit` or default is set to `cassandra.logdir` + /audit/
     */
    public String audit_logs_dir = System.getProperty("cassandra.logdir.audit",
                                                      System.getProperty("cassandra.logdir",".")+"/audit/");

    public String toString()
    {
        return "AuditLogOptions{" +
               "enabled=" + enabled +
               ", logger='" + logger + '\'' +
               ", included_keyspaces='" + included_keyspaces + '\'' +
               ", excluded_keyspaces='" + excluded_keyspaces + '\'' +
               ", included_categories='" + included_categories + '\'' +
               ", excluded_categories='" + excluded_categories + '\'' +
               ", included_users='" + included_users + '\'' +
               ", excluded_users='" + excluded_users + '\'' +
               ", audit_logs_dir='" + audit_logs_dir + '\'' +
               ", archive_command='" + archive_command + '\'' +
               ", roll_cycle='" + roll_cycle + '\'' +
               ", block=" + block +
               ", max_queue_weight=" + max_queue_weight +
               ", max_log_size=" + max_log_size +
               '}';
    }
}
