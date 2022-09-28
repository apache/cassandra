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

package org.apache.cassandra.diag;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.binlog.BinLogOptions;

public class DiagnosticLogOptions extends BinLogOptions
{
    public volatile boolean enabled = false;
    public ParameterizedClass logger = new ParameterizedClass(BinDiagnosticLogger.class.getSimpleName(), Collections.emptyMap());
    public String diagnostic_log_dir;

    public DiagnosticLogOptions()
    {
        String diagnosticLogDir = CassandraRelevantProperties.LOG_DIR_DIAGNOSTIC.getString();
        String logDir = CassandraRelevantProperties.LOG_DIR.getString() + "/diagnostic";
        Path path = diagnosticLogDir == null ? Paths.get(logDir) : Paths.get(diagnosticLogDir);
        diagnostic_log_dir = path.normalize().toString();
    }

    public static class Builder
    {
        private boolean enabled;
        private ParameterizedClass logger;
        private String diagnosticLogDir;
        private BinLogOptions binLogOptions;


        public Builder()
        {
            this(new DiagnosticLogOptions());
        }

        public Builder(DiagnosticLogOptions opts)
        {
            this(opts, opts);
        }

        public Builder(final BinLogOptions binLogOpts, final DiagnosticLogOptions diagnosticLogOptions)
        {
            this.binLogOptions = new BinLogOptions.Builder(binLogOpts).build();
            this.enabled = diagnosticLogOptions.enabled;
            this.logger = diagnosticLogOptions.logger;
            this.diagnosticLogDir = diagnosticLogOptions.diagnostic_log_dir;
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

        public Builder withDiagnosticLogDir(final String diagnosticLogDir)
        {
            this.diagnosticLogDir = diagnosticLogDir;
            return this;
        }

        public DiagnosticLogOptions build()
        {
            final DiagnosticLogOptions opts = new DiagnosticLogOptions();

            opts.enabled = this.enabled;
            opts.logger = this.logger;
            opts.diagnostic_log_dir = this.diagnosticLogDir;
            opts.roll_cycle = binLogOptions.roll_cycle;
            opts.max_queue_weight = binLogOptions.max_queue_weight;
            opts.max_archive_retries = binLogOptions.max_archive_retries;
            opts.archive_command = binLogOptions.archive_command;
            opts.block = binLogOptions.block;
            opts.max_log_size = binLogOptions.max_log_size;

            return opts;
        }
    }

    @Override
    public String toString()
    {
        return "DiagnosticLogOptions{" +
               "enabled=" + enabled +
               ", logger=" + logger +
               ", diagnostic_log_dir='" + diagnostic_log_dir + '\'' +
               ", archive_command='" + archive_command + '\'' +
               ", roll_cycle='" + roll_cycle + '\'' +
               ", block=" + block +
               ", max_queue_weight=" + max_queue_weight +
               ", max_log_size=" + max_log_size +
               ", max_archive_retries=" + max_archive_retries +
               '}';
    }
}
