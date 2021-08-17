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

package org.apache.cassandra.utils;

import java.net.InetAddress;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.hsqldb.Table;

/**
 * Provides a means to take snapshots when triggered by anomalous events or when the breaking of invariants is
 * detected. When debugging certain classes of problems, having access to the relevant set of sstables when the problem
 * is detected (or as close to then as possible) can be invaluable.
 *
 * This class performs two functions; on a replica where an anomaly is detected, it provides methods to issue snapshot
 * requests to a provided set of replicas. For instance, if rows with duplicate clusterings are detected
 * (CASSANDRA-15789) during a read, a snapshot request will be issued to all participating replicas. If detected during
 * compaction, only the replica itself will receive the request. Requests are issued at a maximum rate of 1 per minute
 * for any given table. Any additional triggers for the same table during the 60 second window are dropped, regardless
 * of the replica set. This window is configurable via a system property (cassandra.diagnostic_snapshot_interval_nanos),
 * but this is intended for use in testing only and operators are not expected to override the default.
 *
 * The second function performed is to handle snapshot requests on replicas. Snapshot names are prefixed with strings
 * specific to the reason which triggered them. To manage consumption of disk space, replicas are restricted to taking
 * a single snapshot for each prefix in a single calendar day. So if duplicate rows are detected by multiple
 * coordinators during reads with the same replica set (or overlapping sets) on the same table, the coordinators may
 * each issue snapshot  requests, but the replicas will only accept the first one they receive. Further requests will
 * be dropped on the replica side.
 */
public class DiagnosticSnapshotService
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticSnapshotService.class);

    public static final DiagnosticSnapshotService instance =
        new DiagnosticSnapshotService(Executors.newSingleThreadExecutor(new NamedThreadFactory("DiagnosticSnapshot")));

    public static final String REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX = "RepairedDataMismatch-";
    public static final String DUPLICATE_ROWS_DETECTED_SNAPSHOT_PREFIX = "DuplicateRows-";

    private final Executor executor;

    private DiagnosticSnapshotService(Executor executor)
    {
        this.executor = executor;
    }

    // Issue at most 1 snapshot request per minute for any given table.
    // Replicas will only create one snapshot per day, but this stops us
    // from swamping the network.
    // Overridable via system property for testing.
    private static final long SNAPSHOT_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE;
    private final ConcurrentHashMap<TableId, AtomicLong> lastSnapshotTimes = new ConcurrentHashMap<>();

    public static void duplicateRows(TableMetadata metadata, Iterable<InetAddressAndPort> replicas)
    {
        instance.maybeTriggerSnapshot(metadata, DUPLICATE_ROWS_DETECTED_SNAPSHOT_PREFIX, replicas);
    }

    public static void repairedDataMismatch(TableMetadata metadata, Iterable<InetAddressAndPort> replicas)
    {
        instance.maybeTriggerSnapshot(metadata, REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX, replicas);
    }

    public static boolean isDiagnosticSnapshotRequest(SnapshotCommand command)
    {
        return command.snapshot_name.startsWith(REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX)
            || command.snapshot_name.startsWith(DUPLICATE_ROWS_DETECTED_SNAPSHOT_PREFIX);
    }

    public static void snapshot(SnapshotCommand command, InetAddressAndPort initiator)
    {
        Preconditions.checkArgument(isDiagnosticSnapshotRequest(command));
        instance.maybeSnapshot(command, initiator);
    }

    public static String getSnapshotName(String prefix)
    {
        return String.format("%s%s", prefix, DATE_FORMAT.format(LocalDate.now()));
    }

    @VisibleForTesting
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }

    private void maybeTriggerSnapshot(TableMetadata metadata, String prefix, Iterable<InetAddressAndPort> endpoints)
    {
        long now = System.nanoTime();
        AtomicLong cached = lastSnapshotTimes.computeIfAbsent(metadata.id, u -> new AtomicLong(0));
        long last = cached.get();
        long interval = Long.getLong("cassandra.diagnostic_snapshot_interval_nanos", SNAPSHOT_INTERVAL_NANOS);
        if (now - last > interval && cached.compareAndSet(last, now))
        {
            Message<SnapshotCommand> msg = Message.out(Verb.SNAPSHOT_REQ,
                                                       new SnapshotCommand(metadata.keyspace,
                                                                           metadata.name,
                                                                           getSnapshotName(prefix),
                                                                           false));
            for (InetAddressAndPort replica : endpoints)
                MessagingService.instance().send(msg, replica);
        }
        else
        {
            logger.debug("Diagnostic snapshot request dropped due to throttling");
        }
    }

    private void maybeSnapshot(SnapshotCommand command, InetAddressAndPort initiator)
    {
        executor.execute(new DiagnosticSnapshotTask(command, initiator));
    }

    private static class DiagnosticSnapshotTask implements Runnable
    {
        final SnapshotCommand command;
        final InetAddressAndPort from;

        DiagnosticSnapshotTask(SnapshotCommand command, InetAddressAndPort from)
        {
            this.command = command;
            this.from = from;
        }

        public void run()
        {
            try
            {
                Keyspace ks = Keyspace.open(command.keyspace);
                if (ks == null)
                {
                    logger.info("Snapshot request received from {} for {}.{} but keyspace not found",
                                from,
                                command.keyspace,
                                command.column_family);
                    return;
                }

                ColumnFamilyStore cfs = ks.getColumnFamilyStore(command.column_family);
                if (cfs.snapshotExists(command.snapshot_name))
                {
                    logger.info("Received diagnostic snapshot request from {} for {}.{}, " +
                                "but snapshot with tag {} already exists",
                                from,
                                command.keyspace,
                                command.column_family,
                                command.snapshot_name);
                    return;
                }
                logger.info("Creating snapshot requested by {} of {}.{} tag: {}",
                            from,
                            command.keyspace,
                            command.column_family,
                            command.snapshot_name);
                cfs.snapshot(command.snapshot_name);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Snapshot request received from {} for {}.{} but CFS not found",
                            from,
                            command.keyspace,
                            command.column_family);
            }
        }
    }
}
