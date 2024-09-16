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
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SnapshotDetailsTabularData;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.schema.SchemaConstants.isLocalSystemKeyspace;

public class SnapshotManager implements SnapshotManagerMBean, AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private static final ScheduledExecutorPlus executor = executorFactory().scheduled(false, "SnapshotCleanup");

    public static final SnapshotManager instance = new SnapshotManager();

    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;
    private final SnapshotLoader snapshotLoader;
    public final RateLimiter snapshotRateLimiter;

    private volatile ScheduledFuture<?> cleanupTaskFuture;

    private final Set<TableSnapshot> liveSnapshots = Collections.synchronizedSet(new HashSet<>());

    /**
     * Expiring snapshots ordered by expiration date, to allow only iterating over snapshots
     * that need to be removed
     */
    private final PriorityBlockingQueue<TableSnapshot> expiringSnapshots = new PriorityBlockingQueue<>(10, comparing(TableSnapshot::getExpiresAt));

    private SnapshotManager()
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt());
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
        snapshotLoader = new SnapshotLoader(dataDirs);
        snapshotRateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();
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

    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }

    public synchronized void start(boolean runPeriodicSnapshotCleaner)
    {
        addSnapshots(loadSnapshots());
        if (runPeriodicSnapshotCleaner)
            resumeSnapshotCleanup();
    }

    public synchronized void start()
    {
        start(false);
    }

    @Override
    public synchronized void close()
    {
        pauseSnapshotCleanup();
        expiringSnapshots.clear();
        liveSnapshots.clear();
    }

    public synchronized void close(boolean shutdownExecutor) throws Exception
    {
        close();
        if (shutdownExecutor)
            shutdownAndWait(1, TimeUnit.MINUTES);
    }

    public synchronized Set<TableSnapshot> loadSnapshots()
    {
        return snapshotLoader.loadSnapshots();
    }

    public synchronized void restart()
    {
        restart(true);
    }

    public synchronized void restart(boolean runPeriodicSnapshotCleaner)
    {
        logger.debug("Restarting SnapshotManager");
        close();
        start(runPeriodicSnapshotCleaner);
        logger.debug("SnapshotManager restarted");
    }

    synchronized void addSnapshot(TableSnapshot snapshot)
    {
        logger.debug("Adding snapshot {}", snapshot);

        if (snapshot.isExpiring())
            expiringSnapshots.add(snapshot);
        else
            liveSnapshots.add(snapshot);
    }

    synchronized void addSnapshots(Collection<TableSnapshot> snapshots)
    {
        snapshots.forEach(this::addSnapshot);
    }

    public synchronized void resumeSnapshotCleanup()
    {
        if (cleanupTaskFuture == null)
        {
            logger.info("Scheduling expired snapshots cleanup with initialDelaySeconds={} and cleanupPeriodSeconds={}",
                        initialDelaySeconds, cleanupPeriodSeconds);

            cleanupTaskFuture = executor.scheduleWithFixedDelay(this::clearExpiredSnapshots,
                                                                initialDelaySeconds,
                                                                cleanupPeriodSeconds,
                                                                SECONDS);
        }
    }

    synchronized void pauseSnapshotCleanup()
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
        clearSnapshot(snapshot, true);
    }

    synchronized void clearSnapshot(TableSnapshot snapshot, boolean deleteData)
    {
        logger.debug("Removing snapshot {}{}", snapshot, deleteData ? ", deleting data" : "");

        if (deleteData)
        {
            for (File snapshotDir : snapshot.getDirectories())
            {
                try
                {
                    removeSnapshotDirectory(snapshotDir);
                }
                catch (Exception ex)
                {
                    logger.warn("Unable to remove snapshot directory {}", snapshotDir, ex);
                }
            }
        }

        if (snapshot.isExpiring())
            expiringSnapshots.remove(snapshot);
        else
            liveSnapshots.remove(snapshot);
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
     * Returns list of snapshots from given keyspace and table.
     *
     * @param keyspace keyspace of a snapshot
     * @param table    table of a snapshot
     * @return list of snapshots from given keyspace and table
     */
    public List<TableSnapshot> getSnapshots(String keyspace, String table)
    {
        return getSnapshots(snapshot -> snapshot.getKeyspaceName().equals(keyspace) &&
                                        snapshot.getTableName().equals(table));
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
        // we do not use the predicate here because we want to stop the loop as soon as
        // we find the snapshot we are looking for, looping until the end is not necessary
        for (TableSnapshot snapshot : Iterables.concat(liveSnapshots, expiringSnapshots))
        {
            if (snapshot.getKeyspaceName().equals(keyspace) &&
                snapshot.getTableName().equals(table) &&
                snapshot.getTag().equals(tag) || (tag != null && tag.isEmpty()))
            {
                return Optional.of(snapshot);
            }
        }

        return Optional.empty();
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
     * @return all ephemeral snapshots in a node
     */
    public List<TableSnapshot> getEphemeralSnapshots()
    {
        return getSnapshots(TableSnapshot::isEphemeral);
    }

    /**
     * Returns all snapshots passing the given predicate.
     *
     * @param predicate predicate to filter all snapshots of
     * @return list of snapshots passing the predicate
     */
    public synchronized List<TableSnapshot> getSnapshots(Predicate<TableSnapshot> predicate)
    {
        List<TableSnapshot> notExistingAnymore = new ArrayList<>();
        List<TableSnapshot> snapshots = new ArrayList<>();
        for (TableSnapshot snapshot : Iterables.concat(liveSnapshots, expiringSnapshots))
        {
            if (predicate.test(snapshot))
            {
                if (!snapshot.hasManifest())
                    notExistingAnymore.add(snapshot);
                else
                    snapshots.add(snapshot);
            }
        }

        for (TableSnapshot tableSnapshot : notExistingAnymore)
            clearSnapshot(tableSnapshot, false);

        return snapshots;
    }

    public Collection<TableSnapshot> getExpiringSnapshots()
    {
        return expiringSnapshots;
    }

    /**
     * Clear snapshots of given tag from given keyspaces.
     * <p>
     * If tag is not present / is empty, all snapshots are considered to be cleared.
     * If keyspaces are empty, all snapshots of given tag and older than maxCreatedAt are removed.
     * <p>
     * Ephemeral snapshots are not included.
     *
     * @param tag          optional tag of snapshot to clear
     * @param keyspaces    keyspaces to remove snapshots for
     * @param maxCreatedAt clear all such snapshots which were created before this timestamp
     */
    public void clearSnapshots(String tag, Set<String> keyspaces, long maxCreatedAt)
    {
        clearSnapshots(tag, keyspaces, maxCreatedAt, false);
    }

    /**
     * Clear snapshots of given tag from given keyspace.
     * <p>
     *
     * @param tag      snapshot name
     * @param keyspace keyspace to clear all snapshots of a given tag of
     */
    public void clearSnapshots(String tag, String keyspace)
    {
        clearSnapshots(tag, Set.of(keyspace), Clock.Global.currentTimeMillis(), false);
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
     * Clears all ephemeral snapshots in a node.
     */
    public void clearEphemeralSnapshots()
    {
        getEphemeralSnapshots().forEach(this::clearSnapshot);
    }

    /**
     * Clears all expired snapshots in a node.
     */
    public synchronized void clearExpiredSnapshots()
    {
        Instant now = FBUtilities.now();
        getSnapshots(s -> s.isExpired(now)).forEach(this::clearSnapshot);
    }

    /**
     * Clear snapshots of given tag from given keyspaces.
     * <p>
     * If tag is not present / is empty, all snapshots are considered to be cleared.
     * If keyspaces are empty, all snapshots of given tag and older than maxCreatedAt are removed.
     *
     * @param tag              optional tag of snapshot to clear
     * @param keyspaces        keyspaces to remove snapshots for
     * @param maxCreatedAt     clear all such snapshots which were created before this timestamp
     * @param includeEphemeral include ephemeral snaphots for removal or not
     */
    synchronized void clearSnapshots(String tag, Set<String> keyspaces,
                                     long maxCreatedAt,
                                     boolean includeEphemeral)
    {
        Predicate<TableSnapshot> predicate = shouldClearSnapshot(tag, keyspaces, maxCreatedAt, includeEphemeral);
        getSnapshots(predicate).forEach(this::clearSnapshot);
    }

    /**
     * Returns a predicate based on which a snapshot will be included for deletion or not.
     *
     * @param tag                name of snapshot to remove
     * @param keyspaces          keyspaces this snapshot belongs to
     * @param olderThanTimestamp clear the snapshot if it is older than given timestamp
     * @param includeEphemeral   whether to include ephemeral snapshots as well
     * @return predicate which filters snapshots on given parameters
     */
    static Predicate<TableSnapshot> shouldClearSnapshot(String tag,
                                                        Set<String> keyspaces,
                                                        long olderThanTimestamp,
                                                        boolean includeEphemeral)
    {
        return ts ->
        {
            // When no tag is supplied, all snapshots must be cleared
            boolean clearAll = tag == null || tag.isEmpty();
            if (!clearAll && ts.isEphemeral() && !includeEphemeral)
                logger.info("Skipping deletion of ephemeral snapshot '{}' in keyspace {}. " +
                            "Ephemeral snapshots are not removable by a user.",
                            tag, ts.getKeyspaceName());
            boolean passedEphemeralTest = !ts.isEphemeral() || (ts.isEphemeral() && includeEphemeral);
            boolean shouldClearTag = clearAll || ts.getTag().equals(tag);
            boolean byTimestamp = true;

            if (olderThanTimestamp > 0L)
            {
                Instant createdAt = ts.getCreatedAt();
                if (createdAt != null)
                    byTimestamp = createdAt.isBefore(Instant.ofEpochMilli(olderThanTimestamp));
            }

            boolean byKeyspace = (keyspaces.isEmpty() || keyspaces.contains(ts.getKeyspaceName()));

            return passedEphemeralTest && shouldClearTag && byTimestamp && byKeyspace;
        };
    }

    /**
     * Takes a snapshot by creating hardlinks into snapshot directories. This method also
     * creates manifests and schema files and such snapshot will be added among tracked ones in this manager.
     *
     * @param cfs          column family to create a snapshot for
     * @param tag          name of snapshot
     * @param ephemeral    true if the snapshot is ephemeral, false otherwise
     * @param ttl          time after the created snapshot will be removed
     * @param creationTime time the snapshot was created
     * @param rateLimiter  limiter for hard-links creation, if null, limiter from DatabaseDescriptor will be used
     * @return logical representation of a snapshot
     */
    public TableSnapshot createSnapshot(ColumnFamilyStore cfs,
                                        String tag,
                                        com.google.common.base.Predicate<SSTableReader> predicate,
                                        boolean ephemeral,
                                        DurationSpec.IntSecondsBound ttl,
                                        Instant creationTime,
                                        RateLimiter rateLimiter)
    {
        if (ephemeral && ttl != null)
            throw new IllegalStateException(format("can not take ephemeral snapshot (%s) while ttl is specified too", tag));

        RateLimiter limiter = rateLimiter;
        if (limiter == null)
            limiter = SnapshotManager.instance.snapshotRateLimiter;

        Set<SSTableReader> sstables = new LinkedHashSet<>();
        for (ColumnFamilyStore aCfs : cfs.concatWithIndexes())
        {
            try (ColumnFamilyStore.RefViewFragment currentView = aCfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, tag);
                    ssTable.createLinks(snapshotDirectory.path(), limiter); // hard links
                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", cfs.keyspace, ssTable.getFilename(), snapshotDirectory);
                    sstables.add(ssTable);
                }
            }
        }

        List<String> dataComponents = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            dataComponents.add(sstable.descriptor.relativeFilenameFor(SSTableFormat.Components.DATA));

        SnapshotManifest manifest = new SnapshotManifest(dataComponents, ttl, creationTime, ephemeral);

        Set<File> snapshotDirs = cfs.getDirectories().getSnapshotDirs(tag);

        for (File snapshotDir : snapshotDirs)
        {
            writeSnapshotManifest(manifest, Directories.getSnapshotManifestFile(snapshotDir));

            if (!SchemaConstants.isLocalSystemKeyspace(cfs.metadata.keyspace)
                && !SchemaConstants.isReplicatedSystemKeyspace(cfs.metadata.keyspace))
            {
                writeSnapshotSchema(Directories.getSnapshotSchemaFile(snapshotDir), cfs);
            }
        }

        TableSnapshot snapshot = new TableSnapshot(cfs.metadata.keyspace,
                                                   cfs.metadata.name,
                                                   cfs.metadata.id.asUUID(),
                                                   tag,
                                                   creationTime,
                                                   SnapshotManifest.computeExpiration(ttl, creationTime),
                                                   snapshotDirs,
                                                   ephemeral);

        addSnapshot(snapshot);
        return snapshot;
    }


    public void takeSnapshot(TakeSnapshotTask takeSnapshotTask) throws IOException
    {
        addSnapshots(takeSnapshotTask.call());
    }

    // MBean methods

    @Override
    public void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException
    {
        takeSnapshot(new TakeSnapshotTask(tag, options, entities));
    }

    @Override
    public void clearSnapshot(String tag, Map<String, Object> options, String... keyspaceNames)
    {
        if (options == null)
            options = Collections.emptyMap();

        Object olderThan = options.get("older_than");
        Object olderThanTimestamp = options.get("older_than_timestamp");

        long maxCreatedAt = Clock.Global.currentTimeMillis();
        if (olderThan != null)
        {
            assert olderThan instanceof String : "it is expected that older_than is an instance of java.lang.String";
            maxCreatedAt -= new DurationSpec.LongSecondsBound((String) olderThan).toMilliseconds();
        }
        else if (olderThanTimestamp != null)
        {
            assert olderThanTimestamp instanceof String : "it is expected that older_than_timestamp is an instance of java.lang.String";
            try
            {
                maxCreatedAt = Instant.parse((String) olderThanTimestamp).toEpochMilli();
            }
            catch (DateTimeParseException ex)
            {
                throw new RuntimeException("Parameter older_than_timestamp has to be a valid instant in ISO format.");
            }
        }

        clearSnapshots(tag, Set.of(keyspaceNames), maxCreatedAt, false);

        if (logger.isDebugEnabled())
            logger.debug("Cleared out snapshot directories tag={} keyspaces={} maxCreatedAt={}", tag, keyspaceNames, maxCreatedAt);
    }

    @Override
    public Map<String, TabularData> listSnapshots(Map<String, String> options)
    {
        boolean skipExpiring = options != null && Boolean.parseBoolean(options.getOrDefault("no_ttl", "false"));
        boolean includeEphemeral = options != null && Boolean.parseBoolean(options.getOrDefault("include_ephemeral", "false"));
        String selectedKeyspace = options != null ? options.get("keyspace") : null;
        String selectedTable = options != null ? options.get("table") : null;
        String selectedSnapshotName = options != null ? options.get("snapshot") : null;

        Map<String, TabularData> snapshotMap = new HashMap<>();

        Set<String> tags = new HashSet<>();

        List<TableSnapshot> snapshots = SnapshotManager.instance.getSnapshots(s -> {
            if (selectedSnapshotName != null && !s.getTag().equals(selectedSnapshotName))
                return false;

            if (skipExpiring && s.isExpiring())
                return false;

            if (!includeEphemeral && s.isEphemeral())
                return false;

            if (selectedKeyspace != null && !s.getKeyspaceName().equals(selectedKeyspace))
                return false;

            if (selectedTable != null && !s.getTableName().equals(selectedTable))
                return false;

            return true;
        });

        for (TableSnapshot t : snapshots)
            tags.add(t.getTag());

        for (String tag : tags)
            snapshotMap.put(tag, new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE));

        Map<String, Set<String>> keyspaceTables = new HashMap<>();
        for (TableSnapshot s : snapshots)
        {
            keyspaceTables.computeIfAbsent(s.getKeyspaceName(), ignore -> new HashSet<>());
            keyspaceTables.get(s.getKeyspaceName()).add(s.getTableName());
        }

        Map<String, Set<String>> cfsFiles = new HashMap<>();

        for (Map.Entry<String, Set<String>> entry : keyspaceTables.entrySet())
        {
            for (String table : entry.getValue())
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(entry.getKey(), table);
                if (cfs == null)
                    continue;

                try
                {
                    cfsFiles.put(cfs.getKeyspaceName() + '.' + cfs.name, cfs.getFilesOfCfs());
                }
                catch (Throwable t)
                {
                    logger.debug("Unable to get all files of live SSTables for {}.{}", cfs.getKeyspaceName(), cfs.name);
                }
            }
        }

        for (TableSnapshot snapshot : snapshots)
        {
            TabularDataSupport data = (TabularDataSupport) snapshotMap.get(snapshot.getTag());
            SnapshotDetailsTabularData.from(snapshot, data, cfsFiles.get(snapshot.getKeyspaceTable()));
        }

        return snapshotMap;
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
    public synchronized long getTrueSnapshotSize()
    {
        long total = 0;
        for (Keyspace keyspace : Keyspace.all())
        {
            if (isLocalSystemKeyspace(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
                total += cfStore.trueSnapshotsSize();
        }

        return total;
    }


    private void removeSnapshotDirectory(File snapshotDir)
    {
        if (snapshotDir.exists())
        {
            logger.trace("Removing snapshot directory {}", snapshotDir);
            try
            {
                FileUtils.deleteRecursiveWithThrottle(snapshotDir, snapshotRateLimiter);
            }
            catch (RuntimeException ex)
            {
                if (!snapshotDir.exists())
                    return; // ignore
                throw ex;
            }
        }
    }
}
