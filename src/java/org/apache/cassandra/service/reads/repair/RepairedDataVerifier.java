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

package org.apache.cassandra.service.reads.repair;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.SnapshotVerbHandler;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NoSpamLogger;

public interface RepairedDataVerifier
{
    public void verify(RepairedDataTracker tracker);

    static RepairedDataVerifier verifier(ReadCommand command)
    {
        return DatabaseDescriptor.snapshotOnRepairedDataMismatch() ? snapshotting(command) : simple(command);
    }

    static RepairedDataVerifier simple(ReadCommand command)
    {
        return new SimpleVerifier(command);
    }

    static RepairedDataVerifier snapshotting(ReadCommand command)
    {
        return new SnapshottingVerifier(command);
    }

    static class SimpleVerifier implements RepairedDataVerifier
    {
        private static final Logger logger = LoggerFactory.getLogger(SimpleVerifier.class);
        protected final ReadCommand command;

        private static final String INCONSISTENCY_WARNING = "Detected mismatch between repaired datasets for table {}.{} during read of {}. {}";

        SimpleVerifier(ReadCommand command)
        {
            this.command = command;
        }

        @Override
        public void verify(RepairedDataTracker tracker)
        {
            Tracing.trace("Verifying repaired data tracker {}", tracker);

            // some mismatch occurred between the repaired datasets on the replicas
            if (tracker.digests.keySet().size() > 1)
            {
                // if any of the digests should be considered inconclusive, because there were
                // pending repair sessions which had not yet been committed or unrepaired partition
                // deletes which meant some sstables were skipped during reads, mark the inconsistency
                // as confirmed
                if (tracker.inconclusiveDigests.isEmpty())
                {
                    TableMetrics metrics = ColumnFamilyStore.metricsFor(command.metadata().id);
                    metrics.confirmedRepairedInconsistencies.mark();
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                                     INCONSISTENCY_WARNING, command.metadata().keyspace,
                                     command.metadata().name, command.toString(), tracker);
                }
                else if (DatabaseDescriptor.reportUnconfirmedRepairedDataMismatches())
                {
                    TableMetrics metrics = ColumnFamilyStore.metricsFor(command.metadata().id);
                    metrics.unconfirmedRepairedInconsistencies.mark();
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                                     INCONSISTENCY_WARNING, command.metadata().keyspace,
                                     command.metadata().name, command.toString(), tracker);
                }
            }
        }
    }

    static class SnapshottingVerifier extends SimpleVerifier
    {
        private static final Logger logger = LoggerFactory.getLogger(SnapshottingVerifier.class);
        private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE;
        private static final String SNAPSHOTTING_WARNING = "Issuing snapshot command for mismatch between repaired datasets for table {}.{} during read of {}. {}";

        // Issue at most 1 snapshot request per minute for any given table.
        // Replicas will only create one snapshot per day, but this stops us
        // from swamping the network if we start seeing mismatches.
        private static final long SNAPSHOT_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
        private static final ConcurrentHashMap<TableId, AtomicLong> LAST_SNAPSHOT_TIMES = new ConcurrentHashMap<>();

        SnapshottingVerifier(ReadCommand command)
        {
            super(command);
        }

        public void verify(RepairedDataTracker tracker)
        {
            super.verify(tracker);
            if (tracker.digests.keySet().size() > 1)
            {
                if (tracker.inconclusiveDigests.isEmpty() ||  DatabaseDescriptor.reportUnconfirmedRepairedDataMismatches())
                {
                    long now = System.nanoTime();
                    AtomicLong cached = LAST_SNAPSHOT_TIMES.computeIfAbsent(command.metadata().id, u -> new AtomicLong(0));
                    long last = cached.get();
                    if (now - last > SNAPSHOT_INTERVAL_NANOS && cached.compareAndSet(last, now))
                    {
                        logger.warn(SNAPSHOTTING_WARNING, command.metadata().keyspace, command.metadata().name, command.toString(), tracker);
                        Message<SnapshotCommand> msg = Message.out(Verb.SNAPSHOT_REQ,
                                                                   new SnapshotCommand(command.metadata().keyspace,
                                                                                       command.metadata().name,
                                                                                       getSnapshotName(),
                                                                                       false));
                        for (InetAddressAndPort replica : tracker.digests.values())
                            MessagingService.instance().send(msg, replica);
                    }
                }
            }
        }

        public static String getSnapshotName()
        {
            return String.format("%s%s",
                                 SnapshotVerbHandler.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX,
                                 DATE_FORMAT.format(LocalDate.now()));
        }
    }
}

