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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import javax.management.openmbean.TabularData;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.TableDroppedNotification;
import org.apache.cassandra.notifications.TablePreScrubNotification;
import org.apache.cassandra.notifications.TruncationNotification;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.snapshot.ClearSnapshotTask.getClearSnapshotPredicate;
import static org.apache.cassandra.service.snapshot.ClearSnapshotTask.getPredicateForCleanedSnapshots;

public class SnapshotManager implements SnapshotManagerMBean, INotificationConsumer, AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private ScheduledExecutorPlus snapshotCleanupExecutor;

    public static final SnapshotManager instance = new SnapshotManager();

    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;

    private volatile ScheduledFuture<?> cleanupTaskFuture;

    private final String[] dataDirs;

    private volatile boolean started = false;

    /**
     * We read / list snapshots way more often than write / create them so COW is ideal to use here.
     * This enables us to not submit listing tasks or tasks computing snapshot sizes to any executor's queue as they
     * can be just run concurrently which gives way better throughput in case
     * of excessive listing from clients (dashboards and similar) where snapshot metrics are gathered.
     */
    private final List<TableSnapshot> snapshots = new CopyOnWriteArrayList<>();

    private SnapshotManager()
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt(),
             DatabaseDescriptor.getAllDataFileLocations());
    }

    @VisibleForTesting
    SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds, String[] dataDirs)
    {
        this.initialDelaySeconds = initialDelaySeconds;
        this.cleanupPeriodSeconds = cleanupPeriodSeconds;
        this.dataDirs = dataDirs;
        this.snapshotCleanupExecutor = createSnapshotCleanupExecutor();
    }

    public void registerMBean()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    public void unregisterMBean()
    {
        MBeanWrapper.instance.unregisterMBean(MBEAN_NAME);
    }

    public synchronized SnapshotManager start(boolean runPeriodicSnapshotCleaner)
    {
        if (started)
            return this;

        if (snapshotCleanupExecutor == null)
            snapshotCleanupExecutor = createSnapshotCleanupExecutor();

        executeTask(new ReloadSnapshotsTask(dataDirs));

        if (runPeriodicSnapshotCleaner)
            resumeSnapshotCleanup();

        started = true;
        return this;
    }

    @Override
    public synchronized void close()
    {
        if (!started)
            return;

        pauseSnapshotCleanup();

        shutdownAndWait(1, TimeUnit.MINUTES);
        snapshots.clear();

        started = false;
    }

    public synchronized void shutdownAndWait(long timeout, TimeUnit unit)
    {
        try
        {
            ExecutorUtils.shutdownNowAndWait(timeout, unit, snapshotCleanupExecutor);
        }
        catch (InterruptedException | TimeoutException ex)
        {
            throw new RuntimeException(ex);
        }
        finally
        {
            snapshotCleanupExecutor = null;
        }
    }

    public synchronized void restart(boolean runPeriodicSnapshotCleaner)
    {
        if (!started)
            return;

        logger.debug("Restarting SnapshotManager");
        close();
        start(runPeriodicSnapshotCleaner);
        logger.debug("SnapshotManager restarted");
    }


    public synchronized void restart()
    {
        restart(true);
    }

    private static class ReloadSnapshotsTask extends AbstractSnapshotTask<Set<TableSnapshot>>
    {
        private final String[] dataDirs;

        public ReloadSnapshotsTask(String[] dataDirs)
        {
            super(null);
            this.dataDirs = dataDirs;
        }

        @Override
        public Set<TableSnapshot> call()
        {
            Set<TableSnapshot> tableSnapshots = new SnapshotLoader(dataDirs).loadSnapshots();
            new ClearSnapshotTask(SnapshotManager.instance, snapshot -> true, false).call();
            for (TableSnapshot snapshot : tableSnapshots)
                SnapshotManager.instance.addSnapshot(snapshot);

            return tableSnapshots;
        }

        @Override
        public SnapshotTaskType getTaskType()
        {
            return SnapshotTaskType.RELOAD;
        }
    }

    void addSnapshot(TableSnapshot snapshot)
    {
        logger.debug("Adding snapshot {}", snapshot);
        snapshots.add(snapshot);
    }

    List<TableSnapshot> getSnapshots()
    {
        return snapshots;
    }

    public void resumeSnapshotCleanup()
    {
        if (cleanupTaskFuture == null)
        {
            logger.info("Scheduling expired snapshots cleanup with initialDelaySeconds={} and cleanupPeriodSeconds={}",
                        initialDelaySeconds, cleanupPeriodSeconds);

            cleanupTaskFuture = snapshotCleanupExecutor.scheduleWithFixedDelay(SnapshotManager.instance::clearExpiredSnapshots,
                                                                               initialDelaySeconds,
                                                                               cleanupPeriodSeconds,
                                                                               SECONDS);
        }
    }

    private void pauseSnapshotCleanup()
    {
        if (cleanupTaskFuture != null)
        {
            cleanupTaskFuture.cancel(false);
            cleanupTaskFuture = null;
        }
    }

    /**
     * Deletes snapshot and removes it from manager.
     *
     * @param snapshot snapshot to clear
     */
    void clearSnapshot(TableSnapshot snapshot)
    {
        executeTask(new ClearSnapshotTask(this, s -> s.equals(snapshot), true));
    }

    /**
     * Returns list of snapshots of given keyspace
     *
     * @param keyspace keyspace of a snapshot
     * @return list of snapshots of given keyspace.
     */
    public List<TableSnapshot> getSnapshots(String keyspace)
    {
        return getSnapshots(snapshot -> snapshot.getKeyspaceName().equals(keyspace));
    }

    /**
     * Return snapshots based on given parameters.
     *
     * @param skipExpiring     if expiring snapshots should be skipped
     * @param includeEphemeral if ephemeral snapshots should be included
     * @return snapshots based on given parameters
     */
    public List<TableSnapshot> getSnapshots(boolean skipExpiring, boolean includeEphemeral)
    {
        return getSnapshots(s -> (!skipExpiring || !s.isExpiring()) && (includeEphemeral || !s.isEphemeral()));
    }

    /**
     * Returns all snapshots passing the given predicate.
     *
     * @param predicate predicate to filter all snapshots of
     * @return list of snapshots passing the predicate
     */
    public List<TableSnapshot> getSnapshots(Predicate<TableSnapshot> predicate)
    {
        return new GetSnapshotsTask(this, predicate, true).call();
    }

    /**
     * Returns a snapshot or empty optional based on the given parameters.
     *
     * @param keyspace keyspace of a snapshot
     * @param table    table of a snapshot
     * @param tag      name of a snapshot
     * @return empty optional if there is not such snapshot, non-empty otherwise
     */
    public Optional<TableSnapshot> getSnapshot(String keyspace, String table, String tag)
    {
        List<TableSnapshot> foundSnapshots = new GetSnapshotsTask(this,
                                                                  snapshot -> snapshot.getKeyspaceName().equals(keyspace) &&
                                                                              snapshot.getTableName().equals(table) &&
                                                                              snapshot.getTag().equals(tag) || (tag != null && tag.isEmpty()),
                                                                  true).call();

        if (foundSnapshots.isEmpty())
            return Optional.empty();
        else
            return Optional.of(foundSnapshots.get(0));
    }

    /**
     * Checks whether a snapshot for given keyspace and table exists of a given name exists.
     *
     * @param keyspace keyspace to get a snapshot of
     * @param table table to get a snapshot of
     * @param tag name of a snapshot
     * @return true of a snapshot of given properties exist, false otherwise
     */
    public boolean exists(String keyspace, String table, String tag)
    {
        return getSnapshot(keyspace, table, tag).isPresent();
    }

    /**
     * Checks whether a snapshot which satisfies given predicate exists.
     *
     * @param predicate predicate to check the existence of a snapshot
     * @return true if a snapshot which satisfies a predicate exists, false otherwise
     */
    public boolean exists(Predicate<TableSnapshot> predicate)
    {
        return !getSnapshots(predicate).isEmpty();
    }

    /**
     * Clear snapshots of given tag from given keyspace. Does not remove ephemeral snapshots.
     * <p>
     *
     * @param tag      snapshot name
     * @param keyspace keyspace to clear all snapshots of a given tag of
     */
    public void clearSnapshots(String tag, String keyspace)
    {
        clearSnapshots(tag, Set.of(keyspace), Clock.Global.currentTimeMillis());
    }

    /**
     * Removes a snapshot.
     * <p>
     *
     * @param keyspace keyspace of a snapshot to remove
     * @param table    table of a snapshot to remove
     * @param tag      name of a snapshot to remove.
     */
    public void clearSnapshot(String keyspace, String table, String tag)
    {
        executeTask(new ClearSnapshotTask(this,
                                          snapshot -> snapshot.getKeyspaceName().equals(keyspace)
                                                      && snapshot.getTableName().equals(table)
                                                      && snapshot.getTag().equals(tag),
                                          true));
    }

    /**
     * Removes all snapshots for given keyspace and table.
     *
     * @param keyspace keyspace to remove snapshots for
     * @param table    table in a given keyspace to remove snapshots for
     */
    public void clearAllSnapshots(String keyspace, String table)
    {
        executeTask(new ClearSnapshotTask(this,
                                          snapshot -> snapshot.getKeyspaceName().equals(keyspace)
                                                      && snapshot.getTableName().equals(table),
                                          true));
    }

    /**
     * Clears all snapshots, expiring and ephemeral as well.
     */
    public void clearAllSnapshots()
    {
        executeTask(new ClearSnapshotTask(this, snapshot -> true, true));
    }

    /**
     * Clear snapshots based on a given predicate
     *
     * @param predicate predicate to filter snapshots on
     */
    public void clearSnapshot(Predicate<TableSnapshot> predicate)
    {
        executeTask(new ClearSnapshotTask(this, predicate, true));
    }

    /**
     * Clears all ephemeral snapshots in a node.
     */
    public void clearEphemeralSnapshots()
    {
        executeTask(new ClearSnapshotTask(this, TableSnapshot::isEphemeral, true));
    }

    /**
     * Clears all expired snapshots in a node.
     */
    public void clearExpiredSnapshots()
    {
        Instant now = FBUtilities.now();
        executeTask(new ClearSnapshotTask(this, s -> s.isExpired(now), true));
    }

    /**
     * Clear snapshots of given tag from given keyspaces.
     * <p>
     * If tag is not present / is empty, all snapshots are considered to be cleared.
     * If keyspaces are empty, all snapshots of given tag and older than maxCreatedAt are removed.
     *
     * @param tag          optional tag of snapshot to clear
     * @param keyspaces    keyspaces to remove snapshots for
     * @param maxCreatedAt clear all such snapshots which were created before this timestamp
     */
    private void clearSnapshots(String tag, Set<String> keyspaces, long maxCreatedAt)
    {
        executeTask(new ClearSnapshotTask(this, getClearSnapshotPredicate(tag, keyspaces, maxCreatedAt, false), true));
    }

    public List<TableSnapshot> takeSnapshot(SnapshotOptions options)
    {
        return executeTask(new TakeSnapshotTask(this, options));
    }

    // Super methods

    @Override
    public void takeSnapshot(String tag, String... entities)
    {
        takeSnapshot(SnapshotOptions.userSnapshot(tag, Map.of(), entities));
    }

    @Override
    public void takeSnapshot(String tag, Map<String, String> optMap, String... entities) throws IOException
    {
        try
        {
            takeSnapshot(SnapshotOptions.userSnapshot(tag, optMap, entities));
        }
        catch (Throwable ex)
        {
            // to be compatible with deprecated methods in StorageService
            throw new IOException(ex);
        }
    }

    @Override
    public void clearSnapshot(String tag, Map<String, Object> options, String... keyspaceNames)
    {
        executeTask(new ClearSnapshotTask(this, getPredicateForCleanedSnapshots(tag, options, keyspaceNames), true));
    }

    @Override
    public Map<String, TabularData> listSnapshots(Map<String, String> options)
    {
        return new ListSnapshotsTask(this, options, true).call();
    }

    @Override
    public long getTrueSnapshotSize()
    {
        return new TrueSnapshotSizeTask(this, s -> true).call();
    }

    @Override
    public long getTrueSnapshotsSize(String keyspace)
    {
        return new TrueSnapshotSizeTask(this, s -> s.getKeyspaceName().equals(keyspace)).call();
    }

    @Override
    public long getTrueSnapshotsSize(String keyspace, String table)
    {
        return new TrueSnapshotSizeTask(this, s -> s.getKeyspaceName().equals(keyspace) && s.getTableName().equals(table)).call();
    }

    @Override
    public long getTrueSnapshotsSize(String keyspace, String table, String snapshotName)
    {
        return new TrueSnapshotSizeTask(this,
                                        s -> s.getKeyspaceName().equals(keyspace)
                                             && s.getTableName().equals(table)
                                             && s.getTag().equals(snapshotName)).call();
    }

    @Override
    public void setSnapshotLinksPerSecond(long throttle)
    {
        logger.info("Setting snapshot throttle to {}", throttle);
        DatabaseDescriptor.setSnapshotLinksPerSecond(throttle);
    }

    @Override
    public long getSnapshotLinksPerSecond()
    {
        return DatabaseDescriptor.getSnapshotLinksPerSecond();
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof TruncationNotification)
        {
            TruncationNotification truncationNotification = (TruncationNotification) notification;
            ColumnFamilyStore cfs = truncationNotification.cfs;
            if (!truncationNotification.disableSnapshot && cfs.isAutoSnapshotEnabled())
            {
                SnapshotOptions opts = SnapshotOptions.systemSnapshot(cfs.name, SnapshotType.TRUNCATE, cfs.getKeyspaceTableName())
                                                      .ttl(truncationNotification.ttl).build();
                takeSnapshot(opts);
            }
        }
        else if (notification instanceof TableDroppedNotification)
        {
            TableDroppedNotification tableDroppedNotification = (TableDroppedNotification) notification;
            ColumnFamilyStore cfs = tableDroppedNotification.cfs;
            if (cfs.isAutoSnapshotEnabled())
            {
                SnapshotOptions opts = SnapshotOptions.systemSnapshot(cfs.name, SnapshotType.DROP, cfs.getKeyspaceTableName())
                                                      .cfs(cfs).ttl(tableDroppedNotification.ttl).build();
                takeSnapshot(opts);
            }
        }
        else if (notification instanceof TablePreScrubNotification)
        {
            ColumnFamilyStore cfs = ((TablePreScrubNotification) notification).cfs;
            SnapshotOptions opts = SnapshotOptions.systemSnapshot(cfs.name, SnapshotType.PRE_SCRUB, cfs.getKeyspaceTableName()).build();
            takeSnapshot(opts);
        }
    }

    @VisibleForTesting
    List<TableSnapshot> executeTask(TakeSnapshotTask task)
    {
        try
        {
            prePopulateSnapshots(task);
            return task.call();
        }
        catch (Throwable t)
        {
            throw new RuntimeException(String.format("Exception occured while executing %s: %s", task.toString(), t.getMessage()), t);
        }
    }

    @VisibleForTesting
    <T> T executeTask(AbstractSnapshotTask<T> task)
    {
        try
        {
            return task.call();
        }
        catch (Throwable t)
        {
            throw new RuntimeException(String.format("Exception occured while executing %s", task.toString()), t);
        }
    }

    /**
     * Add table snapshots to snaphots cow list in advance in order to be sure that the snapshots to be created
     * are not existing already. "executeTask" method can be invoked by multiple threads, and they hit
     * this synchronized method which populates snapshots so next thread will fail to advance (exception is thrown)
     * if table snapshots of some other tasks are already present in snapshots cow list.
     *
     * Added snapshots to snapshot list are at this point "incomplete", they will not appear in listing output
     * until they are indeed taken and exist on disk, otherwise we would see "phantom" snapshots in listings,
     * they would be present but in fact they are just going through the process of being taken.
     *
     * @param task task to process
     */
    private synchronized void prePopulateSnapshots(TakeSnapshotTask task)
    {
        Map<ColumnFamilyStore, TableSnapshot> snapshotsToCreate = task.getSnapshotsToCreate();
        Map<ColumnFamilyStore, TableSnapshot> snapshotsToOverwrite = new HashMap<>();
        List<TableSnapshot> toCreate = new ArrayList<>(snapshotsToCreate.values());

        for (TableSnapshot existingSnapshot : snapshots)
        {
            for (Map.Entry<ColumnFamilyStore, TableSnapshot> toCreateEntry : snapshotsToCreate.entrySet())
            {
                TableSnapshot snapshotToCreate = toCreateEntry.getValue();
                if (existingSnapshot.equals(toCreateEntry.getValue()))
                {
                    if (!task.options.ephemeral)
                    {
                        throw new RuntimeException(format("Snapshot %s for %s.%s already exists.",
                                                          snapshotToCreate.getTag(),
                                                          snapshotToCreate.getKeyspaceName(),
                                                          snapshotToCreate.getTableName()));
                    }

                    toCreate.remove(toCreateEntry.getValue());
                    snapshotsToOverwrite.put(toCreateEntry.getKey(), existingSnapshot);
                }
            }
        }

        snapshotsToCreate.putAll(snapshotsToOverwrite);

        snapshots.addAll(toCreate);
    }

    private static ScheduledExecutorPlus createSnapshotCleanupExecutor()
    {
        return executorFactory().scheduled(false, "SnapshotCleanup");
    }
}