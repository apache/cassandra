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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

public class BinAuditLogger implements IAuditLogger
{
    public static final long CURRENT_VERSION = 0;
    public static final String AUDITLOG_TYPE = "audit";
    public static final String AUDITLOG_MESSAGE = "message";
    private static final Logger logger = LoggerFactory.getLogger(BinAuditLogger.class);

    private volatile BinLog binLog;

    public BinAuditLogger(AuditLogOptions auditLoggingOptions)
    {
        this.binLog = new BinLog.Builder().path(File.getPath(auditLoggingOptions.audit_logs_dir))
                                          .rollCycle(auditLoggingOptions.roll_cycle)
                                          .blocking(auditLoggingOptions.block)
                                          .maxQueueWeight(auditLoggingOptions.max_queue_weight)
                                          .maxLogSize(auditLoggingOptions.max_log_size)
                                          .archiveCommand(auditLoggingOptions.archive_command)
                                          .maxArchiveRetries(auditLoggingOptions.max_archive_retries)
                                          .build(false);
    }

    public BinAuditLogger(Map<String, String> params)
    {
        this(DatabaseDescriptor.getAuditLoggingOptions());
    }

    /**
     * Stop the audit log leaving behind any generated files.
     */
    public synchronized void stop()
    {
        try
        {
            logger.info("Deactivation of audit log requested.");
            if (binLog != null)
            {
                logger.info("Stopping audit logger");
                binLog.stop();
                binLog = null;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public boolean isEnabled()
    {
        return binLog != null;
    }

    @Override
    public void log(AuditLogEntry auditLogEntry)
    {
        BinLog binLog = this.binLog;
        if (binLog == null || auditLogEntry == null)
        {
            return;
        }
        binLog.logRecord(new Message(auditLogEntry.getLogString()));
    }


    @VisibleForTesting
    public static class Message extends BinLog.ReleaseableWriteMarshallable implements WeightedQueue.Weighable
    {
        /**
         * The shallow size of a {@code Message} object.
         */
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Message(""));

        private final String message;

        public Message(String message)
        {
            this.message = message;
        }

        protected long version()
        {
            return CURRENT_VERSION;
        }

        protected String type()
        {
            return AUDITLOG_TYPE;
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
            wire.write(AUDITLOG_MESSAGE).text(message);
        }

        @Override
        public void release()
        {

        }

        @Override
        public int weight()
        {
            return Ints.checkedCast(EMPTY_SIZE + ObjectSizes.sizeOf(message));
        }
    }
}
