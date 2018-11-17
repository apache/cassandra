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

import com.google.common.primitives.Ints;

import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

public class BinAuditLogger extends BinLogAuditLogger implements IAuditLogger
{
    public BinAuditLogger()
    {
        // due to the way that IAuditLogger instance are created in AuditLogManager, via reflection, we can't assume
        // the manager will call configure() (it won't). thus, we have to call it here from the constructor.
        AuditLogOptions auditLoggingOptions = DatabaseDescriptor.getAuditLoggingOptions();
        configure(Paths.get(auditLoggingOptions.audit_logs_dir),
                  auditLoggingOptions.roll_cycle,
                  auditLoggingOptions.block,
                  auditLoggingOptions.max_queue_weight,
                  auditLoggingOptions.max_log_size,
                  false,
                  auditLoggingOptions.archive_command,
                  auditLoggingOptions.max_archive_retries);
    }

    @Override
    public void log(AuditLogEntry auditLogEntry)
    {
        BinLog binLog = this.binLog;
        if (binLog == null || auditLogEntry == null)
        {
            return;
        }

        super.logRecord(new Message(auditLogEntry.getLogString()), binLog);
    }

    static class Message extends BinLog.ReleaseableWriteMarshallable implements WeightedQueue.Weighable
    {
        private final String message;

        Message(String message)
        {
            this.message = message;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write("type").text("AuditLog");
            wire.write("message").text(message);
        }

        @Override
        public void release()
        {

        }

        @Override
        public int weight()
        {
            return Ints.checkedCast(ObjectSizes.sizeOf(message));
        }
    }
}
