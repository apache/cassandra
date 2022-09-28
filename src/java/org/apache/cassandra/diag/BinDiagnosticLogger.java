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

import java.nio.file.Paths;

import com.google.common.annotations.VisibleForTesting;

import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.log.AbstractBinLogger;
import org.apache.cassandra.utils.binlog.BinLog;

public class BinDiagnosticLogger extends AbstractBinLogger<DiagnosticEvent, DiagnosticLogOptions> implements IDiagnosticLogger
{
    public static final long CURRENT_VERSION = 0;
    public static final String DIAGNOSTIC_LOG_TYPE = "diagnostic";
    public static final String DIAGNOSTIC_LOG_MESSAGE = "message";

    public BinDiagnosticLogger(DiagnosticLogOptions options)
    {
        super(options);
        binLog = new BinLog.Builder(options).path(Paths.get(options.diagnostic_log_dir)).build(false);
    }

    @Override
    public boolean isEnabled()
    {
        return DatabaseDescriptor.diagnosticEventsEnabled() && options.enabled;
    }

    @Override
    public synchronized void stop()
    {
        DiagnosticEventService.instance().unsubscribe(this);
        super.stop();
    }

    @Override
    public void accept(DiagnosticEvent diagnosticEvent)
    {
        if (!isEnabled() || binLog == null || diagnosticEvent == null)
            return;

        binLog.logRecord(new Message(diagnosticEvent.getLogString()));
    }

    @VisibleForTesting
    public static class Message extends AbstractMessage
    {
        public Message(String message)
        {
            super(message);
        }

        @Override
        protected long version()
        {
            return CURRENT_VERSION;
        }

        @Override
        protected String type()
        {
            return DIAGNOSTIC_LOG_TYPE;
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
            wire.write(DIAGNOSTIC_LOG_MESSAGE).text(message);
        }
    }
}
