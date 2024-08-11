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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.audit.AuditLogEntry.DEFAULT_FIELD_SEPARATOR;
import static org.apache.cassandra.audit.AuditLogEntry.DEFAULT_KEY_VALUE_SEPARATOR;

/**
 * Synchronous, file-based audit logger; just uses the standard logging mechansim.
 */
public class FileAuditLogger implements IAuditLogger
{
    protected static final Logger logger = LoggerFactory.getLogger(FileAuditLogger.class);

    private volatile boolean enabled;
    private final String keyValueSeparator;
    private final String fieldSeparator;

    public FileAuditLogger(Map<String, String> params)
    {
        enabled = true;
        keyValueSeparator = params != null
                            ? params.getOrDefault("key_value_separator", DEFAULT_KEY_VALUE_SEPARATOR)
                            : DEFAULT_KEY_VALUE_SEPARATOR;
        fieldSeparator = params != null
                         ? params.getOrDefault("field_separator", DEFAULT_FIELD_SEPARATOR)
                         : DEFAULT_FIELD_SEPARATOR;
    }

    @Override
    public boolean isEnabled()
    {
        return enabled;
    }

    @Override
    public void log(AuditLogEntry auditLogEntry)
    {
        // don't bother with the volatile read of enabled here. just go ahead and log, other components
        // will check the enbaled field.
        logger.info(auditLogEntry.getLogString(keyValueSeparator, fieldSeparator));
    }

    @Override
    public void stop()
    {
        enabled = false;
    }
}
