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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.binlog.BinLogOptions;

import static org.apache.cassandra.utils.binlog.BinLogOptions.Builder.sanitise;

public class AuditLogOptions extends BinLogOptions
{
    public volatile boolean enabled = false;
    public ParameterizedClass logger = new ParameterizedClass(BinAuditLogger.class.getSimpleName(), Collections.emptyMap());
    public String included_keyspaces = StringUtils.EMPTY;
    // CASSANDRA-14498: By default, system, system_schema and system_virtual_schema are excluded, but these can be included via cassandra.yaml
    public String excluded_keyspaces = "system,system_schema,system_virtual_schema";
    public String included_categories = StringUtils.EMPTY;
    public String excluded_categories = StringUtils.EMPTY;
    public String included_users = StringUtils.EMPTY;
    public String excluded_users = StringUtils.EMPTY;

    public String audit_logs_dir;

    public AuditLogOptions()
    {
        String auditLogDir = CassandraRelevantProperties.LOG_DIR_AUDIT.getString();
        String logDir = CassandraRelevantProperties.LOG_DIR.getString() + "/audit";
        Path path = auditLogDir == null ? Paths.get(logDir) : Paths.get(auditLogDir);
        audit_logs_dir = path.normalize().toString();
    }

    public static AuditLogOptions validate(final AuditLogOptions options) throws ConfigurationException
    {
        // not validating keyspaces nor users on purpose,
        // logging might be enabled on these entities before they exist
        // so they are picked up automatically

        validateCategories(options.included_categories);
        validateCategories(options.excluded_categories);

        // other fields in BinLogOptions are validated upon BinAuditLogger initialisation

        return options;
    }

    public static class Builder
    {
        private boolean enabled;
        private ParameterizedClass logger;
        private String includedKeyspaces;
        private String excludedKeyspaces;
        private String includedCategories;
        private String excludedCategories;
        private String includedUsers;
        private String excludedUsers;
        private String auditLogDir;
        private BinLogOptions binLogOptions;

        public Builder()
        {
            this(new AuditLogOptions());
        }

        public Builder(final BinLogOptions opts, final AuditLogOptions auditLogOptions)
        {
            this.binLogOptions = new BinLogOptions.Builder(opts).build();
            this.enabled = auditLogOptions.enabled;
            this.logger = auditLogOptions.logger;
            this.includedKeyspaces = auditLogOptions.included_keyspaces;
            this.excludedKeyspaces = auditLogOptions.excluded_keyspaces;
            this.includedCategories = auditLogOptions.included_categories;
            this.excludedCategories = auditLogOptions.excluded_categories;
            this.includedUsers = auditLogOptions.included_users;
            this.excludedUsers = auditLogOptions.excluded_users;
            this.auditLogDir = auditLogOptions.audit_logs_dir;
        }

        public Builder(final AuditLogOptions opts)
        {
            this(opts, opts);
        }

        public Builder withEnabled(boolean enabled)
        {
            this.enabled = enabled;
            return this;
        }

        public Builder withLogger(final String loggerName, Map<String, String> parameters)
        {
            if (loggerName != null && !loggerName.trim().isEmpty())
            {
                this.logger = new ParameterizedClass(loggerName.trim(), parameters);
            }

            return this;
        }

        public Builder withIncludedKeyspaces(final String includedKeyspaces)
        {
            sanitise(includedKeyspaces).map(v -> this.includedKeyspaces = v);
            return this;
        }

        public Builder withExcludedKeyspaces(final String excludedKeyspaces)
        {
            sanitise(excludedKeyspaces).map(v -> this.excludedKeyspaces = v);
            return this;
        }

        public Builder withIncludedCategories(final String includedCategories)
        {
            sanitise(includedCategories).map(v -> this.includedCategories = v.toUpperCase());
            return this;
        }

        public Builder withExcludedCategories(final String excludedCategories)
        {
            sanitise(excludedCategories).map(v -> this.excludedCategories = v.toUpperCase());
            return this;
        }

        public Builder withIncludedUsers(final String includedUsers)
        {
            sanitise(includedUsers).map(v -> this.includedUsers = v);
            return this;
        }

        public Builder withExcludedUsers(final String excludedUsers)
        {
            sanitise(excludedUsers).map(v -> this.excludedUsers = v);
            return this;
        }

        public Builder withAuditLogDir(final String auditLogDir)
        {
            this.auditLogDir = auditLogDir;
            return this;
        }

        public AuditLogOptions build()
        {
            final AuditLogOptions opts = new AuditLogOptions();
            opts.enabled = this.enabled;
            opts.logger = this.logger;
            sanitise(this.includedKeyspaces).map(v -> opts.included_keyspaces = v);
            sanitise(this.excludedKeyspaces).map(v -> opts.excluded_keyspaces = v);
            sanitise(this.includedCategories).map(v -> opts.included_categories = v.toUpperCase());
            sanitise(this.excludedCategories).map(v -> opts.excluded_categories = v.toUpperCase());
            sanitise(this.includedUsers).map(v -> opts.included_users = v);
            sanitise(this.excludedUsers).map(v -> opts.excluded_users = v);
            opts.audit_logs_dir = this.auditLogDir;
            opts.roll_cycle = binLogOptions.roll_cycle;
            opts.max_queue_weight = binLogOptions.max_queue_weight;
            opts.max_archive_retries = binLogOptions.max_archive_retries;
            opts.archive_command = binLogOptions.archive_command;
            opts.block = binLogOptions.block;
            opts.max_log_size = binLogOptions.max_log_size;

            AuditLogOptions.validate(opts);

            return opts;
        }
    }

    private static void validateCategories(final String categories)
    {
        assert categories != null;

        if (categories.isEmpty())
            return;

        for (final String includedCategory : categories.split(","))
        {
            try
            {
                AuditLogEntryCategory.valueOf(includedCategory);
            }
            catch (final IllegalArgumentException ex)
            {
                throw new ConfigurationException(String.format("category %s not found in %s",
                                                               includedCategory,
                                                               AuditLogEntryCategory.class.getName()),
                                                 ex);
            }
        }
    }

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
               ", max_archive_retries=" + max_archive_retries +
               '}';
    }
}
