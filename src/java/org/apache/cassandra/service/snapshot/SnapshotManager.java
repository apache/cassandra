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

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import javax.management.openmbean.TabularData;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.snapshot.ClearSnapshotTask.getClearSnapshotPredicate;
import static org.apache.cassandra.service.snapshot.ClearSnapshotTask.getPredicateForCleanedSnapshots;
import static org.apache.cassandra.service.snapshot.ListSnapshotsTask.getListingSnapshotsPredicate;

public class SnapshotManager implements SnapshotManagerMBean, AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private static ScheduledExecutorPlus snapshotCleanupExecutor;
    private static ExecutorPlus tasksExecutor;

    public static final SnapshotManager instance = new SnapshotManager();

    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;

    private volatile ScheduledFuture<?> cleanupTaskFuture;

    private final String[] dataDirs;

    private volatile boolean started = false;

    private final Set<TableSnapshot> snapshots = Collections.synchronizedSet(new HashSet<>());

    private SnapshotManager()
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt());
    }

    private static ScheduledExecutorPlus createSnapshotCleanupExecutor()
    {
        return executorFactory().scheduled(false, "SnapshotCleanup");
    }

    private static ExecutorPlus createSnapshotTasksExecutor()
    {
        return executorFactory()
               .localAware()
               .configurePooled("SnapshotManager", 1)
               .withKeepAlive(1, TimeUnit.HOURS)
               .withQueueLimit(Integer.MAX_VALUE)
               .withRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy())
               .build();
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds)
    {
        this(initialDelaySeconds, cleanupPeriodSeconds, DatabaseDescriptor.getAllDataFileLocations());
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds, String[] dataDirs)
    {
        this.initialDelaySeconds = initialDelaySeconds;
        this.cleanupPeriodSeconds = cleanupPeriodSeconds;
        this.dataDirs = dataDirs;

        snapshotCleanupExecutor = createSnapshotCleanupExecutor();
        tasksExecutor = createSnapshotTasksExecutor();
    }

    public void registerMBean()
    {
        logger.debug("Registering SnapshotManagerMBean");
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    public void unregisterMBean()
    {
        MBeanWrapper.instance.unregisterMBean(MBEAN_NAME);
    }

    public static void shutdownAndWait(long timeout, TimeUnit unit)
    {
        try
        {
            ExecutorUtils.shutdownNowAndWait(timeout, unit, snapshotCleanupExecutor);
            ExecutorUtils.shutdownAndWait(timeout, unit, tasksExecutor);
        }
        catch (InterruptedException | TimeoutException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private static class LoadSnapshotsTask implements Callable<Set<TableSnapshot>>
    {
        private final String[] dataDirs;

        public LoadSnapshotsTask(String[] dataDirs)
        {
            this.dataDirs = dataDirs;
        }

        @Override
        public Set<TableSnapshot> call()
        {
            return new SnapshotLoader(dataDirs).loadSnapshots();
        }
    }

    public synchronized void start(boolean runPeriodicSnapshotCleaner)
    {
        if (started)
            return;

        for (TableSnapshot snapshot : executeTask(new LoadSnapshotsTask(dataDirs)))
            SnapshotManager.instance.addSnapshot(snapshot);

        if (runPeriodicSnapshotCleaner)
            resumeSnapshotCleanup();

        started = true;
    }

    public synchronized void start()
    {
        start(false);
    }

    @Override
    public synchronized void close()
    {
        pauseSnapshotCleanup();
        snapshots.clear();
    }

    public synchronized void close(boolean shutdownExecutor)
    {
        if (!started)
            return;

        close();
        if (shutdownExecutor)
            shutdownAndWait(1, TimeUnit.MINUTES);

        started = false;
    }

    public synchronized Set<TableSnapshot> loadSnapshots()
    {
        return executeTask(new LoadSnapshotsTask(dataDirs));
    }

    public synchronized void restart()
    {
        restart(true);
    }

    public synchronized void restart(boolean runPeriodicSnapshotCleaner)
    {
        if (!started)
            return;

        logger.debug("Restarting SnapshotManager");
        close(true);
        start(runPeriodicSnapshotCleaner);
        logger.debug("SnapshotManager restarted");
    }

    synchronized void addSnapshot(TableSnapshot snapshot)
    {
        logger.debug("Adding snapshot {}", snapshot);
        snapshots.add(snapshot);
    }

    synchronized Set<TableSnapshot> getSnapshots()
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
    synchronized void clearSnapshot(TableSnapshot snapshot)
    {
        executeTask(new ClearSnapshotTask(s -> s.equals(snapshot), true));
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
    public synchronized List<TableSnapshot> getSnapshots(Predicate<TableSnapshot> predicate)
    {
        return executeTask(new GetSnapshotsTask(predicate, true));
    }

    /**
     * Returns a snapshot or empty optional based on the given parameters.
     *
     * @param keyspace keyspace of a snapshot
     * @param table    table of a snapshot
     * @param tag      name of a snapshot
     * @return empty optional if there is not such snapshot, non-empty otherwise
     */
    public synchronized Optional<TableSnapshot> getSnapshot(String keyspace, String table, String tag)
    {
        List<TableSnapshot> foundSnapshots = executeTask(new GetSnapshotsTask(snapshot -> snapshot.getKeyspaceName().equals(keyspace) &&
                                                                                          snapshot.getTableName().equals(table) &&
                                                                                          snapshot.getTag().equals(tag) || (tag != null && tag.isEmpty()),
                                                                              false));

        if (foundSnapshots.isEmpty())
            return Optional.empty();
        else
            return Optional.of(foundSnapshots.get(0));
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
        getSnapshot(keyspace, table, tag).ifPresent(this::clearSnapshot);
    }

    /**
     * Removes all snapshots for given keyspace and table.
     *
     * @param keyspace keyspace to remove snapshots for
     * @param table    table in a given keyspace to remove snapshots for
     */
    public void clearAllSnapshots(String keyspace, String table)
    {
        executeTask(new ClearSnapshotTask(snapshot -> snapshot.getKeyspaceName().equals(keyspace) && snapshot.getTableName().equals(table), true));
    }

    /**
     * Clears all snapshots, expiring and ephemeral as well.
     */
    public void clearAllSnapshots()
    {
        executeTask(new ClearSnapshotTask(snapshot -> true, true));
    }

    /**
     * Clears all ephemeral snapshots in a node.
     */
    public void clearEphemeralSnapshots()
    {
        executeTask(new ClearSnapshotTask(TableSnapshot::isEphemeral, true));
    }

    /**
     * Clears all expired snapshots in a node.
     */
    public synchronized void clearExpiredSnapshots()
    {
        Instant now = FBUtilities.now();
        executeTask(new ClearSnapshotTask(s -> s.isExpired(now), true));
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
    private synchronized void clearSnapshots(String tag, Set<String> keyspaces, long maxCreatedAt)
    {
        executeTask(new ClearSnapshotTask(getClearSnapshotPredicate(tag, keyspaces, maxCreatedAt, false), true));
    }

    /**
     * Takes snapshot(s) for given task which was constructed outside of this manager to fine-tune the snapshotting task.
     *
     * @param takeSnapshotTask task to take snapshots for
     * @return list of taken snapshots
     */
    public synchronized List<TableSnapshot> takeSnapshot(TakeSnapshotTask takeSnapshotTask)
    {
        return executeTask(takeSnapshotTask);
    }

    /**
     * Takes snapshot of a given name for given keyspace and table.
     *
     * @param snapshotName  name of snapshot to take
     * @param keyspaceTable keyspace and table pair in form "keyspace.table"
     * @return taken snapshot
     */
    public TableSnapshot takeSnapshot(String snapshotName, String keyspaceTable)
    {
        return takeSnapshot(new TakeSnapshotTask.Builder(snapshotName, keyspaceTable).build()).get(0);
    }

    /**
     * Takes snapshot of given name against given keyspace and table name.
     *
     * @param snapshotName name of snapshot to take
     * @param keyspace     keyspace name to take a snapshot for
     * @param table        table name to take a snapshot for
     * @return taken snapshot
     */
    public TableSnapshot takeSnapshot(String snapshotName, String keyspace, String table)
    {
        return takeSnapshot(new TakeSnapshotTask.Builder(snapshotName, keyspace + '.' + table).build()).get(0);
    }

    // MBean methods

    @Override
    public void takeSnapshot(String tag, Map<String, String> options, String... entities)
    {
        logger.info("Taking snapshot ...");
        TakeSnapshotTask.Builder builder = new TakeSnapshotTask.Builder(tag, entities).ttl(options.get(TakeSnapshotTask.TTL));
        if (Boolean.parseBoolean(options.getOrDefault(TakeSnapshotTask.SKIP_FLUSH, Boolean.FALSE.toString())))
            builder.skipFlush();

        takeSnapshot(builder.build());
    }

    @Override
    public void clearSnapshot(String tag, Map<String, Object> options, String... keyspaceNames)
    {
        executeTask(new ClearSnapshotTask(getPredicateForCleanedSnapshots(tag, options, keyspaceNames), true));
    }

    @Override
    public Map<String, TabularData> listSnapshots(Map<String, String> options)
    {
        return executeTask(new ListSnapshotsTask(getListingSnapshotsPredicate(options), true));
    }

    @Override
    public synchronized long getTrueSnapshotSize()
    {
        return executeTask(new TrueSnapshotSizeTask(s -> true));
    }

    @Override
    public synchronized long getTrueSnapshotsSize(String keyspace)
    {
        return executeTask(new TrueSnapshotSizeTask(s -> s.getKeyspaceName().equals(keyspace)));
    }

    @Override
    public synchronized long getTrueSnapshotsSize(String keyspace, String table)
    {
        return executeTask(new TrueSnapshotSizeTask(s -> s.getKeyspaceName().equals(keyspace) && s.getTableName().equals(table)));
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

    private <T> T executeTask(Callable<T> task)
    {
        try
        {
            return tasksExecutor.submit(task).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(String.format("Unable to execute task %s", task.getClass().getName()));
        }
    }
}
