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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
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

    public String audit_logs_dir;

    public AuditLogOptions()
    {
        String auditLogDir = CassandraRelevantProperties.LOG_DIR_AUDIT.getString();
        String logDir = CassandraRelevantProperties.LOG_DIR.getString() + "/audit";
        Path path = auditLogDir == null ? File.getPath(logDir) : File.getPath(auditLogDir);
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

        public Builder(final AuditLogOptions opts)
        {
            this.enabled = opts.enabled;
            this.logger = opts.logger;
            this.includedKeyspaces = opts.included_keyspaces;
            this.excludedKeyspaces = opts.excluded_keyspaces;
            this.includedCategories = opts.included_categories;
            this.excludedCategories = opts.excluded_categories;
            this.includedUsers = opts.included_users;
            this.excludedUsers = opts.excluded_users;
            this.auditLogDir = opts.audit_logs_dir;
            this.maxQueueWeight = opts.max_queue_weight;
            this.maxArchiveRetries = opts.max_archive_retries;
            this.rollCycle = opts.roll_cycle;
            this.archiveCommand = opts.archive_command;
            this.block = opts.block;
            this.maxLogSize = opts.max_log_size;
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
            opts.roll_cycle = this.rollCycle;
            opts.audit_logs_dir = this.auditLogDir;
            opts.max_queue_weight = this.maxQueueWeight;
            opts.max_archive_retries = this.maxArchiveRetries;
            opts.archive_command = this.archiveCommand;
            opts.block = this.block;
            opts.max_log_size = this.maxLogSize;

            AuditLogOptions.validate(opts);

            return opts;
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
