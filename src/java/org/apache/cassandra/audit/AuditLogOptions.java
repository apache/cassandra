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

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.binlog.BinLogOptions;

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

    /**
     * AuditLogs directory can be configured using `cassandra.logdir.audit` or default is set to `cassandra.logdir` + /audit/
     */
    public String audit_logs_dir = Paths.get(System.getProperty("cassandra.logdir.audit",
                                                                System.getProperty("cassandra.logdir", ".") + "/audit/")).normalize().toString();

    public static AuditLogOptions validate(final AuditLogOptions options) throws ConfigurationException
    {
        // not validating keyspaces nor users on purpose,
        // logging might be enabled on these entities before they exist
        // so they are picked up automatically

        validateCategories(options.included_categories);
        validateCategories(options.excluded_categories);

        return options;
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
        private int maxQueueWeight;
        private int maxArchiveRetries;
        private String rollCycle;
        private String archiveCommand;
        private boolean block;
        private long maxLogSize;

        public Builder()
        {
            this(new AuditLogOptions());
        }

        public Builder(final AuditLogOptions defaultOptions)
        {
            this.enabled = defaultOptions.enabled;
            this.logger = defaultOptions.logger;
            this.includedKeyspaces = defaultOptions.included_keyspaces;
            this.excludedKeyspaces = defaultOptions.excluded_keyspaces;
            this.includedCategories = defaultOptions.included_categories;
            this.excludedCategories = defaultOptions.excluded_categories;
            this.includedUsers = defaultOptions.included_users;
            this.excludedUsers = defaultOptions.excluded_users;
            this.auditLogDir = defaultOptions.audit_logs_dir;
            this.maxQueueWeight = defaultOptions.max_queue_weight;
            this.maxArchiveRetries = defaultOptions.max_archive_retries;
            this.rollCycle = defaultOptions.roll_cycle;
            this.archiveCommand = defaultOptions.archive_command;
            this.block = defaultOptions.block;
            this.maxLogSize = defaultOptions.max_log_size;
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

        public Builder withMaxArchiveRetries(final int maxArchiveRetries)
        {
            this.maxArchiveRetries = maxArchiveRetries;
            return this;
        }

        public Builder withRollCycle(final String rollCycle)
        {
            this.rollCycle = rollCycle;
            return this;
        }

        public Builder withArchiveCommand(final String archiveCommand)
        {
            this.archiveCommand = archiveCommand;
            return this;
        }

        public Builder withBlock(final boolean block)
        {
            this.block = block;
            return this;
        }

        public Builder withMaxLogSize(final long maxLogSize)
        {
            this.maxLogSize = maxLogSize;
            return this;
        }

        public AuditLogOptions build()
        {
            final AuditLogOptions auditLogOptions = new AuditLogOptions();

            auditLogOptions.enabled = this.enabled;

            if (this.logger != null && this.logger.class_name != null)
            {
                Preconditions.checkState(FBUtilities.isAuditLoggerClassExists(this.logger.class_name),
                                         "Unable to find AuditLogger class: " + this.logger.class_name);
            }

            auditLogOptions.logger = this.logger;
            auditLogOptions.included_keyspaces = this.includedKeyspaces;
            auditLogOptions.excluded_keyspaces = this.excludedKeyspaces;
            auditLogOptions.included_categories = this.includedCategories;
            auditLogOptions.excluded_categories = this.excludedCategories;
            auditLogOptions.included_users = this.includedUsers;
            auditLogOptions.excluded_users = this.excludedUsers;
            auditLogOptions.audit_logs_dir = this.auditLogDir;
            auditLogOptions.max_queue_weight = this.maxQueueWeight;
            auditLogOptions.max_archive_retries = this.maxArchiveRetries;
            auditLogOptions.roll_cycle = this.rollCycle;
            auditLogOptions.archive_command = this.archiveCommand;
            auditLogOptions.block = this.block;
            auditLogOptions.max_log_size = this.maxLogSize;

            AuditLogOptions.validate(auditLogOptions);

            return auditLogOptions;
        }

        public static AuditLogOptions sanitise(final AuditLogOptions other)
        {
            final AuditLogOptions options = new AuditLogOptions();

            options.enabled = other.enabled;
            options.logger = other.logger;
            sanitise(other.included_keyspaces).map(v -> options.included_keyspaces = v);
            sanitise(other.excluded_keyspaces).map(v -> options.excluded_keyspaces = v);
            // important to have it uppercased because if a user enters "ddl,dcl", then it will not be found
            // if categories are named DDL and DCL upon filtering
            sanitise(other.included_categories).map(v -> options.included_categories = v.toUpperCase());
            sanitise(other.excluded_categories).map(v -> options.excluded_categories = v.toUpperCase());
            sanitise(other.included_users).map(v -> options.included_users = v);
            sanitise(other.excluded_users).map(v -> options.excluded_users = v);

            return options;
        }

        private static Optional<String> sanitise(final String input)
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
