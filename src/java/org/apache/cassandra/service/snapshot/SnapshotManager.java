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
package org.apache.cassandra.service.snapshot;


import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;

import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ExecutorUtils;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.FBUtilities.now;

public class SnapshotManager {

    private static final ScheduledExecutorPlus executor = executorFactory().scheduled(false, "SnapshotCleanup");

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;
    private final SnapshotLoader snapshotLoader;

    @VisibleForTesting
    protected volatile ScheduledFuture<?> cleanupTaskFuture;

    /**
     * Expiring snapshots ordered by expiration date, to allow only iterating over snapshots
     * that need to be removed on {@link this#clearExpiredSnapshots()}
     */
    private final PriorityQueue<TableSnapshot> expiringSnapshots = new PriorityQueue<>(comparing(TableSnapshot::getExpiresAt));

    public SnapshotManager()
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt());
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds)
    {
        this.initialDelaySeconds = initialDelaySeconds;
        this.cleanupPeriodSeconds = cleanupPeriodSeconds;
        snapshotLoader = new SnapshotLoader(DatabaseDescriptor.getAllDataFileLocations());
    }

    public Collection<TableSnapshot> getExpiringSnapshots()
    {
        return expiringSnapshots;
    }

    public synchronized void start()
    {
        addSnapshots(loadSnapshots());
        resumeSnapshotCleanup();
    }

    public synchronized void stop() throws InterruptedException, TimeoutException
    {
        expiringSnapshots.clear();
        if (cleanupTaskFuture != null)
        {
            cleanupTaskFuture.cancel(false);
            cleanupTaskFuture = null;
        }
    }

    public synchronized void addSnapshot(TableSnapshot snapshot)
    {
        // We currently only care about expiring snapshots
        if (snapshot.isExpiring())
        {
            logger.debug("Adding expiring snapshot {}", snapshot);
            expiringSnapshots.add(snapshot);
        }
    }

    public synchronized Set<TableSnapshot> loadSnapshots(String keyspace)
    {
        return snapshotLoader.loadSnapshots(keyspace);
    }

    public synchronized Set<TableSnapshot> loadSnapshots()
    {
        return snapshotLoader.loadSnapshots();
    }

    @VisibleForTesting
    protected synchronized void addSnapshots(Collection<TableSnapshot> snapshots)
    {
        logger.debug("Adding snapshots: {}.", Joiner.on(", ").join(snapshots.stream().map(TableSnapshot::getId).collect(toList())));
        snapshots.forEach(this::addSnapshot);
    }

    // TODO: Support pausing snapshot cleanup
    @VisibleForTesting
    synchronized void resumeSnapshotCleanup()
    {
        if (cleanupTaskFuture == null)
        {
            logger.info("Scheduling expired snapshot cleanup with initialDelaySeconds={} and cleanupPeriodSeconds={}",
                        initialDelaySeconds, cleanupPeriodSeconds);
            cleanupTaskFuture = executor.scheduleWithFixedDelay(this::clearExpiredSnapshots, initialDelaySeconds,
                                                                cleanupPeriodSeconds, TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected synchronized void clearExpiredSnapshots()
    {
        TableSnapshot expiredSnapshot;
        while ((expiredSnapshot = expiringSnapshots.peek()) != null)
        {
            if (!expiredSnapshot.isExpired(now()))
                break; // the earliest expiring snapshot is not expired yet, so there is no more expired snapshots to remove

            logger.debug("Removing expired snapshot {}.", expiredSnapshot);
            clearSnapshot(expiredSnapshot);
        }
    }

    /**
     * Deletes snapshot and remove it from manager
     */
    public synchronized void clearSnapshot(TableSnapshot snapshot)
    {
        for (File snapshotDir : snapshot.getDirectories())
            Directories.removeSnapshotDirectory(DatabaseDescriptor.getSnapshotRateLimiter(), snapshotDir);

        expiringSnapshots.remove(snapshot);
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }
}
