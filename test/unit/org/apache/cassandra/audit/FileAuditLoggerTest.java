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

import org.junit.Test;

import static org.apache.cassandra.audit.AuditLogEntry.DEFAULT_FIELD_SEPARATOR;
import static org.apache.cassandra.audit.AuditLogEntry.DEFAULT_KEY_VALUE_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the {@link FileAuditLogger} class
 */
public class FileAuditLoggerTest
{
    @Test
    public void testEnabled()
    {
        FileAuditLogger fileAuditLogger = new FileAuditLogger(null);
        assertThat(fileAuditLogger.isEnabled()).isTrue();
        fileAuditLogger.stop();
        assertThat(fileAuditLogger.isEnabled()).isFalse();
    }

    @Test
    public void testUseDefaultSeparators()
    {
        AuditLogEntry mockAuditLogEntry = mock(AuditLogEntry.class);

        // null map configuration
        new FileAuditLogger(null).log(mockAuditLogEntry);
        verify(mockAuditLogEntry, times(1)).getLogString(DEFAULT_KEY_VALUE_SEPARATOR, DEFAULT_FIELD_SEPARATOR);

        // empty map configuration
        new FileAuditLogger(Map.of()).log(mockAuditLogEntry);
        verify(mockAuditLogEntry, times(2)).getLogString(DEFAULT_KEY_VALUE_SEPARATOR, DEFAULT_FIELD_SEPARATOR);
    }

    @Test
    public void testCustomKeyValueSeparator()
    {
        AuditLogEntry mockAuditLogEntry = mock(AuditLogEntry.class);
        new FileAuditLogger(Map.of("key_value_separator", "=")).log(mockAuditLogEntry);
        verify(mockAuditLogEntry, times(1)).getLogString("=", DEFAULT_FIELD_SEPARATOR);
    }

    @Test
    public void testCustomFieldSeparator()
    {
        AuditLogEntry mockAuditLogEntry = mock(AuditLogEntry.class);
        new FileAuditLogger(Map.of("field_separator", "_")).log(mockAuditLogEntry);
        verify(mockAuditLogEntry, times(1)).getLogString(DEFAULT_KEY_VALUE_SEPARATOR, "_");
    }
}
