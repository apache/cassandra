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
package org.apache.cassandra.db;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CounterCacheKey;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.FutureTask;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Flushing;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.streaming.CassandraStreamManager;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.metrics.Sampler.Sample;
import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.repair.TableRepairManager;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.repair.consistent.admin.PendingStat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.service.paxos.TablePaxosRepairHistory;
import org.apache.cassandra.service.snapshot.SnapshotLoader;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.streaming.TableStreamManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.DefaultValue;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.DatabaseDescriptor.getFlushWriters;
import static org.apache.cassandra.db.commitlog.CommitLogPosition.NONE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean, Memtable.Owner, SSTable.Owner
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    /*
    We keep a pool of threads for each data directory, size of each pool is memtable_flush_writers.
    When flushing we start a Flush runnable in the flushExecutor. Flush calculates how to split the
    memtable ranges over the existing data directories and creates a FlushRunnable for each of the directories.
    The FlushRunnables are executed in the perDiskflushExecutors and the Flush will block until all FlushRunnables
    are finished. By having flushExecutor size the same size as each of the perDiskflushExecutors we make sure we can
    have that many flushes going at the same time.
    */
    private static final ExecutorPlus flushExecutor = executorFactory()
            .withJmxInternal()
            .pooled("MemtableFlushWriter", getFlushWriters());

    // post-flush executor is single threaded to provide guarantee that any flush Future on a CF will never return until prior flushes have completed
    private static final ExecutorPlus postFlushExecutor = executorFactory()
            .withJmxInternal()
            .sequential("MemtablePostFlush");

    private static final ExecutorPlus reclaimExecutor = executorFactory()
            .withJmxInternal()
            .sequential("MemtableReclaimMemory");

    private static final PerDiskFlushExecutors perDiskflushExecutors = new PerDiskFlushExecutors(DatabaseDescriptor.getFlushWriters(),
                                                                                                 DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations(),
                                                                                                 DatabaseDescriptor.useSpecificLocationForLocalSystemData());

    /**
     * Reason for initiating a memtable flush.
     */
    public enum FlushReason
    {
        COMMITLOG_DIRTY,
        MEMTABLE_LIMIT,
        MEMTABLE_PERIOD_EXPIRED,
        INDEX_BUILD_STARTED,
        INDEX_BUILD_COMPLETED,
        INDEX_REMOVED,
        INDEX_TABLE_FLUSH,
        VIEW_BUILD_STARTED,
        INTERNALLY_FORCED,  // explicitly requested flush, necessary for the operation of an internal table
        USER_FORCED, // flush explicitly requested by the user (e.g. nodetool flush)
        STARTUP,
        DRAIN,
        SNAPSHOT,
        TRUNCATE,
        DROP,
        STREAMING,
        STREAMS_RECEIVED,
        VALIDATION,
        ANTICOMPACTION,
        SCHEMA_CHANGE,
        OWNED_RANGES_CHANGE,
        UNIT_TESTS // explicitly requested flush needed for a test
    }

    private static final String[] COUNTER_NAMES = new String[]{"table", "count", "error", "value"};
    private static final String[] COUNTER_DESCS = new String[]
    { "keyspace.tablename",
      "number of occurances",
      "error bounds",
      "value" };
    private static final CompositeType COUNTER_COMPOSITE_TYPE;

    private static final String SAMPLING_RESULTS_NAME = "SAMPLING_RESULTS";

    public static final String SNAPSHOT_TRUNCATE_PREFIX = "truncated";
    public static final String SNAPSHOT_DROP_PREFIX = "dropped";
    static final String TOKEN_DELIMITER = ":";

    /** Special values used when the local ranges are not changed with ring changes (e.g. local tables). */
    public static final int RING_VERSION_IRRELEVANT = -1;

    static
    {
        try
        {
            OpenType<?>[] counterTypes = new OpenType[] { SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.STRING };
            COUNTER_COMPOSITE_TYPE = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_NAMES, COUNTER_DESCS, counterTypes);
        } catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public final Keyspace keyspace;
    public final String name;
    public final TableMetadataRef metadata;
    private final String mbeanName;
    /** @deprecated See CASSANDRA-9448 */
    @Deprecated(since = "3.0")
    private final String oldMBeanName;
    private volatile boolean valid = true;

    private volatile Memtable.Factory memtableFactory;

    /**
     * Memtables and SSTables on disk for this column family.
     *
     * We synchronize on the Tracker to ensure isolation when we want to make sure
     * that the memtable we're acting on doesn't change out from under us.  I.e., flush
     * syncronizes on it to make sure it can submit on both executors atomically,
     * so anyone else who wants to make sure flush doesn't interfere should as well.
     */
    private final Tracker data;

    /* The read order, used to track accesses to off-heap memtable storage */
    public final OpOrder readOrdering = new OpOrder();

    /* This is used to generate the next index for a SSTable */
    private final Supplier<? extends SSTableId> sstableIdGenerator;

    public final SecondaryIndexManager indexManager;
    public final TableViews viewManager;

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultValue<Integer> minCompactionThreshold;
    private volatile DefaultValue<Integer> maxCompactionThreshold;
    private volatile DefaultValue<Double> crcCheckChance;

    private final CompactionStrategyManager compactionStrategyManager;

    private final Directories directories;

    public final TableMetrics metric;
    public volatile long sampleReadLatencyMicros;
    public volatile long additionalWriteLatencyMicros;

    private final CassandraTableWriteHandler writeHandler;
    private final CassandraStreamManager streamManager;

    private final TableRepairManager repairManager;
    public final TopPartitionTracker topPartitions;

    private final SSTableImporter sstableImporter;

    private volatile boolean compactionSpaceCheck = true;

    // Tombtone partitions that ignore the gc_grace_seconds during compaction
    private final Set<DecoratedKey> partitionKeySetIgnoreGcGrace = ConcurrentHashMap.newKeySet();

    @VisibleForTesting
    final DiskBoundaryManager diskBoundaryManager = new DiskBoundaryManager();
    private volatile ShardBoundaries cachedShardBoundaries = null;

    private volatile boolean neverPurgeTombstones = false;

    private class PaxosRepairHistoryLoader
    {
        private TablePaxosRepairHistory history;

        TablePaxosRepairHistory get()
        {
            if (history != null)
                return history;

            synchronized (this)
            {
                if (history != null)
                    return history;

                history = TablePaxosRepairHistory.load(getKeyspaceName(), name);
                return history;
            }
        }

    }

    private final PaxosRepairHistoryLoader paxosRepairHistory = new PaxosRepairHistoryLoader();

    public static void shutdownPostFlushExecutor() throws InterruptedException
    {
        postFlushExecutor.shutdown();
        postFlushExecutor.awaitTermination(60, TimeUnit.SECONDS);
    }

    public static void shutdownExecutorsAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> executors = new ArrayList<>();
        Collections.addAll(executors, reclaimExecutor, postFlushExecutor, flushExecutor);
        perDiskflushExecutors.appendAllExecutors(executors);
        ExecutorUtils.shutdownAndWait(timeout, unit, executors);
    }

    public void reload()
    {
        // metadata object has been mutated directly. make all the members jibe with new settings.

        // only update these runtime-modifiable settings if they have not been modified.
        if (!minCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.minCompactionThreshold = new DefaultValue(metadata().params.compaction.minCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultValue(metadata().params.compaction.maxCompactionThreshold());
        if (!crcCheckChance.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.crcCheckChance = new DefaultValue(metadata().params.crcCheckChance);

        compactionStrategyManager.maybeReloadParamsFromSchema(metadata().params.compaction);

        indexManager.reload();

        memtableFactory = metadata().params.memtable.factory();
        switchMemtableOrNotify(FlushReason.SCHEMA_CHANGE, Memtable::metadataUpdated);
    }

    public static Runnable getBackgroundCompactionTaskSubmitter()
    {
        return () -> {
            for (Keyspace keyspace : Keyspace.all())
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                    CompactionManager.instance.submitBackground(cfs);
        };
    }

    public Map<String, String> getCompactionParameters()
    {
        return compactionStrategyManager.getCompactionParams().asMap();
    }

    public String getCompactionParametersJson()
    {
        return JsonUtils.writeAsJsonString(getCompactionParameters());
    }

    public void setCompactionParameters(Map<String, String> options)
    {
        try
        {
            CompactionParams compactionParams = CompactionParams.fromMap(options);
            compactionParams.validate();
            compactionStrategyManager.overrideLocalParams(compactionParams);
        }
        catch (Throwable t)
        {
            logger.error("Could not set new local compaction strategy", t);
            // dont propagate the ConfigurationException over jmx, user will only see a ClassNotFoundException
            throw new IllegalArgumentException("Could not set new local compaction strategy: "+t.getMessage());
        }
    }

    public void setCompactionParametersJson(String options)
    {
        setCompactionParameters(JsonUtils.fromJsonMap(options));
    }

    public Map<String,String> getCompressionParameters()
    {
        return metadata.getLocal().params.compression.asMap();
    }

    public String getCompressionParametersJson()
    {
        return JsonUtils.writeAsJsonString(getCompressionParameters());
    }

    public void setCompressionParameters(Map<String,String> opts)
    {
        try
        {
            CompressionParams params = CompressionParams.fromMap(opts);
            params.validate();
            metadata.setLocalOverrides(metadata().unbuild().compression(params).build());
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void setCompressionParametersJson(String options)
    {
        setCompressionParameters(JsonUtils.fromJsonMap(options));
    }

    @VisibleForTesting
    public ColumnFamilyStore(Keyspace keyspace,
                             String columnFamilyName,
                             Supplier<? extends SSTableId> sstableIdGenerator,
                             TableMetadataRef metadata,
                             Directories directories,
                             boolean loadSSTables,
                             boolean registerBookeeping,
                             boolean offline)
    {
        assert directories != null;
        assert metadata != null : "null metadata for " + keyspace + ':' + columnFamilyName;

        this.keyspace = keyspace;
        this.metadata = metadata;
        this.directories = directories;
        name = columnFamilyName;
        minCompactionThreshold = new DefaultValue<>(metadata.get().params.compaction.minCompactionThreshold());
        maxCompactionThreshold = new DefaultValue<>(metadata.get().params.compaction.maxCompactionThreshold());
        crcCheckChance = new DefaultValue<>(metadata.get().params.crcCheckChance);
        viewManager = keyspace.viewManager.forTable(metadata.id);
        this.sstableIdGenerator = sstableIdGenerator;
        sampleReadLatencyMicros = DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MICROSECONDS) / 2;
        additionalWriteLatencyMicros = DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MICROSECONDS) / 2;
        memtableFactory = metadata.get().params.memtable.factory();

        logger.info("Initializing {}.{}", getKeyspaceName(), name);

        // Create Memtable and its metrics object only on online
        Memtable initialMemtable = null;
        TableMetrics.ReleasableMetric memtableMetrics = null;
        if (DatabaseDescriptor.isDaemonInitialized())
        {
            initialMemtable = createMemtable(new AtomicReference<>(CommitLog.instance.getCurrentPosition()));
            memtableMetrics = memtableFactory.createMemtableMetrics(metadata);
        }
        data = new Tracker(this, initialMemtable, loadSSTables);

        // Note that this needs to happen before we load the first sstables, or the global sstable tracker will not
        // be notified on the initial loading.
        data.subscribe(StorageService.instance.sstablesTracker);

        Collection<SSTableReader> sstables = null;
        // scan for sstables corresponding to this cf and load them
        if (data.loadsstables)
        {
            Directories.SSTableLister sstableFiles = directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
            sstables = SSTableReader.openAll(this, sstableFiles.list().entrySet(), metadata);
            data.addInitialSSTablesWithoutUpdatingSize(sstables);
        }

        // compaction strategy should be created after the CFS has been prepared
        compactionStrategyManager = new CompactionStrategyManager(this);

        if (maxCompactionThreshold.value() <= 0 || minCompactionThreshold.value() <=0)
        {
            logger.warn("Disabling compaction strategy by setting compaction thresholds to 0 is deprecated, set the compaction option 'enabled' to 'false' instead.");
            this.compactionStrategyManager.disable();
        }

        // create the private ColumnFamilyStores for the secondary column indexes
        indexManager = new SecondaryIndexManager(this);
        for (IndexMetadata info : metadata.get().indexes)
        {
            indexManager.addIndex(info, true);
        }

        metric = new TableMetrics(this, memtableMetrics);

        if (data.loadsstables)
        {
            data.updateInitialSSTableSize(sstables);
        }

        if (registerBookeeping)
        {
            // register the mbean
            mbeanName = getTableMBeanName(getKeyspaceName(), name, isIndex());
            oldMBeanName = getColumnFamilieMBeanName(getKeyspaceName(), name, isIndex());

            String[] objectNames = {mbeanName, oldMBeanName};
            for (String objectName : objectNames)
                MBeanWrapper.instance.registerMBean(this, objectName);
        }
        else
        {
            mbeanName = null;
            oldMBeanName= null;
        }
        writeHandler = new CassandraTableWriteHandler(this);
        streamManager = new CassandraStreamManager(this);
        repairManager = new CassandraTableRepairManager(this);
        sstableImporter = new SSTableImporter(this);

        if (DatabaseDescriptor.isClientOrToolInitialized() || SchemaConstants.isSystemKeyspace(getKeyspaceName()))
            topPartitions = null;
        else
            topPartitions = new TopPartitionTracker(metadata());
    }

    public static String getTableMBeanName(String ks, String name, boolean isIndex)
    {
        return String.format("org.apache.cassandra.db:type=%s,keyspace=%s,table=%s",
                      isIndex ? "IndexTables" : "Tables",
                      ks, name);
    }

    public static String getColumnFamilieMBeanName(String ks, String name, boolean isIndex)
    {
       return String.format("org.apache.cassandra.db:type=%s,keyspace=%s,columnfamily=%s",
                            isIndex ? "IndexColumnFamilies" : "ColumnFamilies",
                            ks, name);
    }

    public void updateSpeculationThreshold()
    {
        try
        {
            sampleReadLatencyMicros = metadata().params.speculativeRetry.calculateThreshold(metric.coordinatorReadLatency, sampleReadLatencyMicros);
            additionalWriteLatencyMicros = metadata().params.additionalWritePolicy.calculateThreshold(metric.coordinatorWriteLatency, additionalWriteLatencyMicros);
        }
        catch (Throwable e)
        {
            logger.error("Exception caught while calculating speculative retry threshold for {}: {}", metadata(), e);
        }
    }

    public TableWriteHandler getWriteHandler()
    {
        return writeHandler;
    }

    public TableStreamManager getStreamManager()
    {
        return streamManager;
    }

    public TableRepairManager getRepairManager()
    {
        return repairManager;
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public Directories getDirectories()
    {
        return directories;
    }

    @Override
    public List<String> getDataPaths() throws IOException
    {
        List<String> dataPaths = new ArrayList<>();
        for (File dataPath : directories.getCFDirectories())
        {
            dataPaths.add(dataPath.canonicalPath());
        }

        return dataPaths;
    }

    public boolean writesShouldSkipCommitLog()
    {
        return memtableFactory.writesShouldSkipCommitLog();
    }

    public boolean memtableWritesAreDurable()
    {
        return memtableFactory.writesAreDurable();
    }

    public boolean streamToMemtable()
    {
        return memtableFactory.streamToMemtable();
    }

    public boolean streamFromMemtable()
    {
        return memtableFactory.streamFromMemtable();
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, TimeUUID pendingRepair, boolean isTransient, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        return createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, null, 0, header, lifecycleNewTracker);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, TimeUUID pendingRepair, boolean isTransient, IntervalSet<CommitLogPosition> commitLogPositions, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        return createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, commitLogPositions, 0, header, lifecycleNewTracker);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, TimeUUID pendingRepair, boolean isTransient, IntervalSet<CommitLogPosition> commitLogPositions, int sstableLevel, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        return getCompactionStrategyManager().createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, commitLogPositions, sstableLevel, header, indexManager.listIndexGroups(), lifecycleNewTracker);
    }

    public boolean supportsEarlyOpen()
    {
        return compactionStrategyManager.supportsEarlyOpen();
    }

    /** call when dropping or renaming a CF. Performs mbean housekeeping and invalidates CFS to other operations */
    public void invalidate()
    {
        invalidate(true, true);
    }

    public void invalidate(boolean expectMBean)
    {
        invalidate(expectMBean, true);
    }

    public void invalidate(boolean expectMBean, boolean dropData)
    {
        // disable and cancel in-progress compactions before invalidating
        valid = false;

        try
        {
            unregisterMBean();
        }
        catch (Exception e)
        {
            if (expectMBean)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // this shouldn't block anything.
                logger.warn("Failed unregistering mbean: {}", mbeanName, e);
            }
        }

        compactionStrategyManager.shutdown();

        // Do not remove truncation records for index CFs, given they have the same ID as their backing/base tables.
        if (!metadata.get().isIndex())
            SystemKeyspace.removeTruncationRecord(metadata.id);

        if (dropData)
        {
            data.dropSSTables();
            LifecycleTransaction.waitForDeletions();
        }
        indexManager.dropAllIndexes(dropData);

        invalidateCaches();
        if (topPartitions != null)
            topPartitions.close();
    }

    /**
     * Removes every SSTable in the directory from the Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void maybeRemoveUnreadableSSTables(File directory)
    {
        data.removeUnreadableSSTables(directory);
    }

    void unregisterMBean() throws MalformedObjectNameException
    {
        ObjectName[] objectNames = {new ObjectName(mbeanName), new ObjectName(oldMBeanName)};
        for (ObjectName objectName : objectNames)
        {
            if (MBeanWrapper.instance.isRegistered(objectName))
                MBeanWrapper.instance.unregisterMBean(objectName);
        }

        // unregister metrics
        metric.release();
    }


    public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, TableMetadataRef metadata, boolean loadSSTables)
    {
        return createColumnFamilyStore(keyspace, metadata.name, metadata, loadSSTables);
    }

    public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                         String columnFamily,
                                                                         TableMetadataRef metadata,
                                                                         boolean loadSSTables)
    {
        Directories directories = new Directories(metadata.get());
        return createColumnFamilyStore(keyspace, columnFamily, metadata, directories, loadSSTables, true, false);
    }

    /** This is only directly used by offline tools */
    public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                         String columnFamily,
                                                                         TableMetadataRef metadata,
                                                                         Directories directories,
                                                                         boolean loadSSTables,
                                                                         boolean registerBookkeeping,
                                                                         boolean offline)
    {
        return new ColumnFamilyStore(keyspace, columnFamily,
                                     directories.getUIDGenerator(SSTableIdFactory.instance.defaultBuilder()),
                                     metadata, directories, loadSSTables, registerBookkeeping, offline);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public static void  scrubDataDirectories(TableMetadata metadata) throws StartupException
    {
        Directories directories = new Directories(metadata);
        Set<File> cleanedDirectories = new HashSet<>();

        // clear ephemeral snapshots that were not properly cleared last session (CASSANDRA-7357)
        clearEphemeralSnapshots(directories);

        directories.removeTemporaryDirectories();

        logger.trace("Removing temporary or obsoleted files from unfinished operations for table {}", metadata.name);
        if (!LifecycleTransaction.removeUnfinishedLeftovers(metadata))
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Cannot remove temporary or obsoleted files for %s due to a problem with transaction " +
                                                     "log files. Please check records with problems in the log messages above and fix them. " +
                                                     "Refer to the 3.0 upgrading instructions in NEWS.txt " +
                                                     "for a description of transaction log files.", metadata.toString()));

        logger.trace("Further extra check for orphan sstable files for {}", metadata.name);
        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : directories.sstableLister(Directories.OnTxnErr.IGNORE).list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            File directory = desc.directory;
            Set<Component> components = sstableFiles.getValue();

            if (!cleanedDirectories.contains(directory))
            {
                cleanedDirectories.add(directory);
                for (File tmpFile : desc.getTemporaryFiles())
                {
                    logger.info("Removing unfinished temporary file {}", tmpFile);
                    tmpFile.tryDelete();
                }
            }

            desc.getFormat().deleteOrphanedComponents(desc, components);
        }

        // cleanup incomplete saved caches
        Pattern tmpCacheFilePattern = Pattern.compile(metadata.keyspace + '-' + metadata.name + "-(Key|Row)Cache.*\\.tmp$");
        File dir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (dir.exists())
        {
            assert dir.isDirectory();
            for (File file : dir.tryList())
                if (tmpCacheFilePattern.matcher(file.name()).matches())
                    if (!file.tryDelete())
                        logger.warn("could not delete {}", file.absolutePath());
        }

        // also clean out any index leftovers.
        for (IndexMetadata index : metadata.indexes)
            if (!index.isCustom())
            {
                TableMetadata indexMetadata = CassandraIndex.indexCfsMetadata(metadata, index);
                scrubDataDirectories(indexMetadata);
            }
    }

    /**
     * See #{@code StorageService.importNewSSTables} for more info
     *
     * @param ksName The keyspace name
     * @param cfName The columnFamily name
     */
    public static void loadNewSSTables(String ksName, String cfName)
    {
        /* ks/cf existence checks will be done by open and getCFS methods for us */
        Keyspace keyspace = Keyspace.open(ksName);
        keyspace.getColumnFamilyStore(cfName).loadNewSSTables();
    }

    /** @deprecated See CASSANDRA-6719 */
    @Deprecated(since = "4.0")
    public void loadNewSSTables()
    {

        SSTableImporter.Options options = SSTableImporter.Options.options().resetLevel(true).build();
        sstableImporter.importNewSSTables(options);
    }

    /**
     * #{@inheritDoc}
     */
    public synchronized List<String> importNewSSTables(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired, boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches, boolean extendedVerify, boolean copyData)
    {
        SSTableImporter.Options options = SSTableImporter.Options.options(srcPaths)
                                                                 .resetLevel(resetLevel)
                                                                 .clearRepaired(clearRepaired)
                                                                 .verifySSTables(verifySSTables)
                                                                 .verifyTokens(verifyTokens)
                                                                 .invalidateCaches(invalidateCaches)
                                                                 .extendedVerify(extendedVerify)
                                                                 .copyData(copyData).build();

        return sstableImporter.importNewSSTables(options);
    }

    public List<String> importNewSSTables(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired, boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches, boolean extendedVerify)
    {
        return importNewSSTables(srcPaths, resetLevel, clearRepaired, verifySSTables, verifyTokens, invalidateCaches, extendedVerify, false);
    }

    Descriptor getUniqueDescriptorFor(Descriptor descriptor, File targetDirectory)
    {
        Descriptor newDescriptor;
        do
        {
            newDescriptor = new Descriptor(descriptor.version,
                                           targetDirectory,
                                           descriptor.ksname,
                                           descriptor.cfname,
                                           // Increment the generation until we find a filename that doesn't exist. This is needed because the new
                                           // SSTables that are being loaded might already use these generation numbers.
                                           sstableIdGenerator.get());
        }
        while (newDescriptor.fileFor(Components.DATA).exists());
        return newDescriptor;
    }

    public void rebuildSecondaryIndex(String idxName)
    {
        rebuildSecondaryIndex(getKeyspaceName(), metadata.name, idxName);
    }

    public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);

        logger.info("User Requested secondary index re-build for {}/{} indexes: {}", ksName, cfName, Joiner.on(',').join(idxNames));
        cfs.indexManager.rebuildIndexesBlocking(Sets.newHashSet(Arrays.asList(idxNames)));
    }

    public AbstractCompactionStrategy createCompactionStrategyInstance(CompactionParams compactionParams)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor =
                compactionParams.klass().getConstructor(ColumnFamilyStore.class, Map.class);
            return constructor.newInstance(this, compactionParams.options());
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** @deprecated See CASSANDRA-9448 */
    @Deprecated(since = "3.0")
    public String getColumnFamilyName()
    {
        return getTableName();
    }

    public String getTableName()
    {
        return name;
    }

    public String getKeyspaceName()
    {
        return keyspace.getName();
    }

    public Descriptor newSSTableDescriptor(File directory)
    {
        return newSSTableDescriptor(directory, DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion());
    }

    public Descriptor newSSTableDescriptor(File directory, SSTableFormat<?, ?> format)
    {
        return newSSTableDescriptor(directory, format.getLatestVersion());
    }

    public Descriptor newSSTableDescriptor(File directory, Version version)
    {
        Descriptor newDescriptor = new Descriptor(version,
                                                  directory,
                                                  getKeyspaceName(),
                                                  name,
                                                  sstableIdGenerator.get());
        assert !newDescriptor.fileFor(Components.DATA).exists();
        return newDescriptor;
    }

    /**
     * Checks with the memtable if it should be switched for the given reason, and if not, calls the specified
     * notification method.
     */
    private void switchMemtableOrNotify(FlushReason reason, Consumer<Memtable> elseNotify)
    {
        Memtable currentMemtable = data.getView().getCurrentMemtable();
        if (currentMemtable.shouldSwitch(reason))
            switchMemtableIfCurrent(currentMemtable, reason);
        else
            elseNotify.accept(currentMemtable);
    }

    /**
     * Switches the memtable iff the live memtable is the one provided
     *
     * @param memtable
     */
    public Future<CommitLogPosition> switchMemtableIfCurrent(Memtable memtable, FlushReason reason)
    {
        synchronized (data)
        {
            if (data.getView().getCurrentMemtable() == memtable)
                return switchMemtable(reason);
        }
        logger.debug("Memtable is no longer current, returning future that completes when current flushing operation completes");
        return waitForFlushes();
    }

    /*
     * switchMemtable puts Memtable.getSortedContents on the writer executor.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables where it is available for reads.
     * This method does not block except for synchronizing on Tracker, but the Future it returns will
     * not complete until the Memtable (and all prior Memtables) have been successfully flushed, and the CL
     * marked clean up to the position owned by the Memtable.
     */
    @VisibleForTesting
    public Future<CommitLogPosition> switchMemtable(FlushReason reason)
    {
        synchronized (data)
        {
            logFlush(reason);
            Flush flush = new Flush(false);
            flushExecutor.execute(flush);
            postFlushExecutor.execute(flush.postFlushTask);
            return flush.postFlushTask;
        }
    }

    // print out size of all memtables we're enqueuing
    private void logFlush(FlushReason reason)
    {
        // reclaiming includes that which we are GC-ing;
        Memtable.MemoryUsage usage = Memtable.newMemoryUsage();
        getTracker().getView().getCurrentMemtable().addMemoryUsageTo(usage);

        for (ColumnFamilyStore indexCfs : indexManager.getAllIndexColumnFamilyStores())
            indexCfs.getTracker().getView().getCurrentMemtable().addMemoryUsageTo(usage);

        logger.info("Enqueuing flush of {}.{}, Reason: {}, Usage: {}", getKeyspaceName(), name, reason, usage);
    }


    /**
     * Flush if there is unflushed data in the memtables
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public Future<CommitLogPosition> forceFlush(FlushReason reason)
    {
        synchronized (data)
        {
            Memtable current = data.getView().getCurrentMemtable();
            for (ColumnFamilyStore cfs : concatWithIndexes())
                if (!cfs.data.getView().getCurrentMemtable().isClean())
                    return flushMemtable(current, reason);
            return waitForFlushes();
        }
    }

    /**
     * Flush if there is unflushed data that was written to the CommitLog before @param flushIfDirtyBefore
     * (inclusive).
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public Future<?> forceFlush(CommitLogPosition flushIfDirtyBefore)
    {
        // we don't loop through the remaining memtables since here we only care about commit log dirtiness
        // and this does not vary between a table and its table-backed indexes
        Memtable current = data.getView().getCurrentMemtable();
        if (current.mayContainDataBefore(flushIfDirtyBefore))
            return flushMemtable(current, FlushReason.COMMITLOG_DIRTY);
        return waitForFlushes();
    }

    private Future<CommitLogPosition> flushMemtable(Memtable current, FlushReason reason)
    {
        if (current.shouldSwitch(reason))
            return switchMemtableIfCurrent(current, reason);
        else
            return waitForFlushes();
    }

    /**
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    private Future<CommitLogPosition> waitForFlushes()
    {
        // we grab the current memtable; once any preceding memtables have flushed, we know its
        // commitLogLowerBound has been set (as this it is set with the upper bound of the preceding memtable)
        final Memtable current = data.getView().getCurrentMemtable();
        return postFlushExecutor.submit(current::getCommitLogLowerBound);
    }

    public CommitLogPosition forceBlockingFlush(FlushReason reason)
    {
        return FBUtilities.waitOnFuture(forceFlush(reason));
    }

    /**
     * Both synchronises custom secondary indexes and provides ordering guarantees for futures on switchMemtable/flush
     * etc, which expect to be able to wait until the flush (and all prior flushes) requested have completed.
     */
    private final class PostFlush implements Callable<CommitLogPosition>
    {
        final CountDownLatch latch = newCountDownLatch(1);
        final Memtable mainMemtable;
        volatile Throwable flushFailure = null;

        private PostFlush(Memtable mainMemtable)
        {
            this.mainMemtable = mainMemtable;
        }

        public CommitLogPosition call()
        {
            try
            {
                // we wait on the latch for the commitLogUpperBound to be set, and so that waiters
                // on this task can rely on all prior flushes being complete
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }

            CommitLogPosition commitLogUpperBound = NONE;
            // If a flush errored out but the error was ignored, make sure we don't discard the commit log.
            if (flushFailure == null && mainMemtable != null)
            {
                commitLogUpperBound = mainMemtable.getFinalCommitLogUpperBound();
                CommitLog.instance.discardCompletedSegments(metadata.id, mainMemtable.getCommitLogLowerBound(), commitLogUpperBound);
            }

            metric.pendingFlushes.dec();

            if (flushFailure != null)
            {
                Throwables.throwIfUnchecked(flushFailure);
                throw new RuntimeException(flushFailure);
            }

            return commitLogUpperBound;
        }
    }

    /**
     * Should only be constructed/used from switchMemtable() or truncate(), with ownership of the Tracker monitor.
     * In the constructor the current memtable(s) are swapped, and a barrier on outstanding writes is issued;
     * when run by the flushWriter the barrier is waited on to ensure all outstanding writes have completed
     * before all memtables are immediately written, and the CL is either immediately marked clean or, if
     * there are custom secondary indexes, the post flush clean up is left to update those indexes and mark
     * the CL clean
     */
    private final class Flush implements Runnable
    {
        final OpOrder.Barrier writeBarrier;
        final Map<ColumnFamilyStore, Memtable> memtables;
        final FutureTask<CommitLogPosition> postFlushTask;
        final PostFlush postFlush;
        final boolean truncate;

        private Flush(boolean truncate)
        {
            if (logger.isTraceEnabled())
                logger.trace("Creating flush task {}@{}", hashCode(), name);
            // if true, we won't flush, we'll just wait for any outstanding writes, switch the memtable, and discard
            this.truncate = truncate;

            metric.pendingFlushes.inc();
            /*
             * To ensure correctness of switch without blocking writes, run() needs to wait for all write operations
             * started prior to the switch to complete. We do this by creating a Barrier on the writeOrdering
             * that all write operations register themselves with, and assigning this barrier to the memtables,
             * after which we *.issue()* the barrier. This barrier is used to direct write operations started prior
             * to the barrier.issue() into the memtable we have switched out, and any started after to its replacement.
             * In doing so it also tells the write operations to update the commitLogUpperBound of the memtable, so
             * that we know the CL position we are dirty to, which can be marked clean when we complete.
             */
            writeBarrier = Keyspace.writeOrder.newBarrier();

            memtables = new LinkedHashMap<>();

            // submit flushes for the memtable for any indexed sub-cfses, and our own
            AtomicReference<CommitLogPosition> commitLogUpperBound = new AtomicReference<>();
            for (ColumnFamilyStore cfs : concatWithIndexes())
            {
                // switch all memtables, regardless of their dirty status, setting the barrier
                // so that we can reach a coordinated decision about cleanliness once they
                // are no longer possible to be modified
                Memtable newMemtable = cfs.createMemtable(commitLogUpperBound);
                Memtable oldMemtable = cfs.data.switchMemtable(truncate, newMemtable);
                oldMemtable.switchOut(writeBarrier, commitLogUpperBound);
                memtables.put(cfs, oldMemtable);
            }

            // we then ensure an atomic decision is made about the upper bound of the continuous range of commit log
            // records owned by this memtable
            setCommitLogUpperBound(commitLogUpperBound);

            // we then issue the barrier; this lets us wait for all operations started prior to the barrier to complete;
            // since this happens after wiring up the commitLogUpperBound, we also know all operations with earlier
            // commit log segment position have also completed, i.e. the memtables are done and ready to flush
            writeBarrier.issue();
            postFlush = new PostFlush(Iterables.get(memtables.values(), 0, null));
            postFlushTask = new FutureTask<>(postFlush);
        }

        public void run()
        {
            if (logger.isTraceEnabled())
                logger.trace("Flush task {}@{} starts executing, waiting on barrier", hashCode(), name);

            long start = nanoTime();

            // mark writes older than the barrier as blocking progress, permitting them to exceed our memory limit
            // if they are stuck waiting on it, then wait for them all to complete
            writeBarrier.markBlocking();
            writeBarrier.await();

            if (logger.isTraceEnabled())
                logger.trace("Flush task for task {}@{} waited {} ms at the barrier", hashCode(), name, TimeUnit.NANOSECONDS.toMillis(nanoTime() - start));

            // mark all memtables as flushing, removing them from the live memtable list
            for (Map.Entry<ColumnFamilyStore, Memtable> entry : memtables.entrySet())
                entry.getKey().data.markFlushing(entry.getValue());

            metric.memtableSwitchCount.inc();

            try
            {
                boolean first = true;
                // Flush "data" memtable with non-cf 2i first;
                for (Map.Entry<ColumnFamilyStore, Memtable> entry : memtables.entrySet())
                {
                    flushMemtable(entry.getKey(), entry.getValue(), first);
                    first = false;
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                postFlush.flushFailure = t;
            }

            if (logger.isTraceEnabled())
                logger.trace("Flush task {}@{} signaling post flush task", hashCode(), name);

            // signal the post-flush we've done our work
            postFlush.latch.decrement();

            if (logger.isTraceEnabled())
                logger.trace("Flush task task {}@{} finished", hashCode(), name);
        }

        public Collection<SSTableReader> flushMemtable(ColumnFamilyStore cfs, Memtable memtable, boolean flushNonCf2i)
        {
            if (logger.isTraceEnabled())
                logger.trace("Flush task task {}@{} flushing memtable {}", hashCode(), name, memtable);

            if (memtable.isClean() || truncate)
            {
                cfs.replaceFlushed(memtable, Collections.emptyList());
                reclaim(memtable);
                return Collections.emptyList();
            }

            List<Future<SSTableMultiWriter>> futures = new ArrayList<>();
            long totalBytesOnDisk = 0;
            long maxBytesOnDisk = 0;
            long minBytesOnDisk = Long.MAX_VALUE;
            List<SSTableReader> sstables = new ArrayList<>();
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
            {
                List<Flushing.FlushRunnable> flushRunnables = null;
                List<SSTableMultiWriter> flushResults = null;

                try
                {
                    // flush the memtable
                    flushRunnables = Flushing.flushRunnables(cfs, memtable, txn);
                    ExecutorPlus[] executors = perDiskflushExecutors.getExecutorsFor(getKeyspaceName(), name);

                    for (int i = 0; i < flushRunnables.size(); i++)
                        futures.add(executors[i].submit(flushRunnables.get(i)));

                    /**
                     * we can flush 2is as soon as the barrier completes, as they will be consistent with (or ahead of) the
                     * flushed memtables and CL position, which is as good as we can guarantee.
                     * TODO: SecondaryIndex should support setBarrier(), so custom implementations can co-ordinate exactly
                     * with CL as we do with memtables/CFS-backed SecondaryIndexes.
                     */
                    if (flushNonCf2i)
                        indexManager.flushAllNonCFSBackedIndexesBlocking(memtable);

                    flushResults = Lists.newArrayList(FBUtilities.waitOnFutures(futures));
                }
                catch (Throwable t)
                {
                    t = Flushing.abortRunnables(flushRunnables, t);
                    t = txn.abort(t);
                    Throwables.throwIfUnchecked(t);
                    throw new RuntimeException(t);
                }

                try
                {
                    Iterator<SSTableMultiWriter> writerIterator = flushResults.iterator();
                    while (writerIterator.hasNext())
                    {
                        SSTableMultiWriter writer = writerIterator.next();
                        if (writer.getBytesWritten() > 0)
                        {
                            writer.setOpenResult(true).prepareToCommit();
                        }
                        else
                        {
                            maybeFail(writer.abort(null));
                            writerIterator.remove();
                        }
                    }
                }
                catch (Throwable t)
                {
                    for (SSTableMultiWriter writer : flushResults)
                        t = writer.abort(t);
                    t = txn.abort(t);
                    Throwables.throwIfUnchecked(t);
                    throw new RuntimeException(t);
                }

                txn.prepareToCommit();

                Throwable accumulate = null;
                for (SSTableMultiWriter writer : flushResults)
                {
                    accumulate = writer.commit(accumulate);
                    metric.flushSizeOnDisk.update(writer.getOnDiskBytesWritten());
                }

                maybeFail(txn.commit(accumulate));

                for (SSTableMultiWriter writer : flushResults)
                {
                    Collection<SSTableReader> flushedSSTables = writer.finished();
                    for (SSTableReader sstable : flushedSSTables)
                    {
                        if (sstable != null)
                        {
                            sstables.add(sstable);
                            long size = sstable.bytesOnDisk();
                            totalBytesOnDisk += size;
                            maxBytesOnDisk = Math.max(maxBytesOnDisk, size);
                            minBytesOnDisk = Math.min(minBytesOnDisk, size);
                        }
                    }
                }
            }
            cfs.replaceFlushed(memtable, sstables);
            reclaim(memtable);
            cfs.compactionStrategyManager.compactionLogger.flush(sstables);
            logger.debug("Flushed to {} ({} sstables, {}), biggest {}, smallest {}",
                         sstables,
                         sstables.size(),
                         FBUtilities.prettyPrintMemory(totalBytesOnDisk),
                         FBUtilities.prettyPrintMemory(maxBytesOnDisk),
                         FBUtilities.prettyPrintMemory(minBytesOnDisk));
            return sstables;
        }

        private void reclaim(final Memtable memtable)
        {
            // issue a read barrier for reclaiming the memory, and offload the wait to another thread
            final OpOrder.Barrier readBarrier = readOrdering.newBarrier();
            readBarrier.issue();
            postFlushTask.addListener(new WrappedRunnable()
            {
                public void runMayThrow()
                {
                    readBarrier.await();
                    memtable.discard();
                }
            }, reclaimExecutor);
        }

        @Override
        public String toString()
        {
            return "Flush " + keyspace + '.' + name;
        }
    }

    public Memtable createMemtable(AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        return memtableFactory.create(commitLogUpperBound, metadata, this);
    }

    // atomically set the upper bound for the commit log
    private static void setCommitLogUpperBound(AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        // we attempt to set the holder to the current commit log context. at the same time all writes to the memtables are
        // also maintaining this value, so if somebody sneaks ahead of us somehow (should be rare) we simply retry,
        // so that we know all operations prior to the position have not reached it yet
        CommitLogPosition lastReplayPosition;
        while (true)
        {
            lastReplayPosition = new Memtable.LastCommitLogPosition((CommitLog.instance.getCurrentPosition()));
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if ((currentLast == null || currentLast.compareTo(lastReplayPosition) <= 0)
                && commitLogUpperBound.compareAndSet(currentLast, lastReplayPosition))
                break;
        }
    }

    @Override
    public Future<CommitLogPosition> signalFlushRequired(Memtable memtable, FlushReason reason)
    {
        return switchMemtableIfCurrent(memtable, reason);
    }

    @Override
    public Memtable getCurrentMemtable()
    {
        return data.getView().getCurrentMemtable();
    }

    public static Iterable<Memtable> activeMemtables()
    {
        return Iterables.transform(ColumnFamilyStore.all(),
                                   cfs -> cfs.getTracker().getView().getCurrentMemtable());
    }

    @Override
    public Iterable<Memtable> getIndexMemtables()
    {
        return Iterables.transform(indexManager.getAllIndexColumnFamilyStores(),
                                   cfs -> cfs.getTracker().getView().getCurrentMemtable());
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Keyspace.switchLock
     * @param update to be applied
     * @param context write context for current update
     * @param updateIndexes whether secondary indexes should be updated
     */
    public void apply(PartitionUpdate update, CassandraWriteContext context, boolean updateIndexes)

    {
        long start = nanoTime();
        OpOrder.Group opGroup = context.getGroup();
        CommitLogPosition commitLogPosition = context.getPosition();
        try
        {
            Memtable mt = data.getMemtableFor(opGroup, commitLogPosition);
            UpdateTransaction indexer = newUpdateTransaction(update, context, updateIndexes, mt);
            long timeDelta = mt.put(update, indexer, opGroup);
            DecoratedKey key = update.partitionKey();
            invalidateCachedPartition(key);
            metric.topWritePartitionFrequency.addSample(key.getKey(), 1);
            if (metric.topWritePartitionSize.isEnabled()) // dont compute datasize if not needed
                metric.topWritePartitionSize.addSample(key.getKey(), update.dataSize());
            StorageHook.instance.reportWrite(metadata.id, update);
            metric.writeLatency.addNano(nanoTime() - start);
            // CASSANDRA-11117 - certain resolution paths on memtable put can result in very
            // large time deltas, either through a variety of sentinel timestamps (used for empty values, ensuring
            // a minimal write, etc). This limits the time delta to the max value the histogram
            // can bucket correctly. This also filters the Long.MAX_VALUE case where there was no previous value
            // to update.
            if(timeDelta < Long.MAX_VALUE)
                metric.colUpdateTimeDeltaHistogram.update(Math.min(18165375903306L, timeDelta));
        }
        catch (RuntimeException e)
        {
            String message = e.getMessage() + " for ks: " + keyspace.getName() + ", table: " + name;

            if (e instanceof InvalidRequestException)
                throw new InvalidRequestException(message, e);

            throw new RuntimeException(message, e);
        }
    }
    
    private UpdateTransaction newUpdateTransaction(PartitionUpdate update, CassandraWriteContext context, boolean updateIndexes, Memtable memtable)
    {
        return updateIndexes
               ? indexManager.newUpdateTransaction(update, context, FBUtilities.nowInSeconds(), memtable)
               : UpdateTransaction.NO_OP;
    }

    public static class VersionedLocalRanges extends ArrayList<Splitter.WeightedRange>
    {
        public final long ringVersion;

        public VersionedLocalRanges(long ringVersion, int initialSize)
        {
            super(initialSize);
            this.ringVersion = ringVersion;
        }
    }

    public VersionedLocalRanges localRangesWeighted()
    {
        if (!SchemaConstants.isLocalSystemKeyspace(getKeyspaceName())
            && getPartitioner() == StorageService.instance.getTokenMetadata().partitioner)
        {
            DiskBoundaryManager.VersionedRangesAtEndpoint versionedLocalRanges = DiskBoundaryManager.getVersionedLocalRanges(this);
            Set<Range<Token>> localRanges = versionedLocalRanges.rangesAtEndpoint.ranges();
            long ringVersion = versionedLocalRanges.ringVersion;

            if (!localRanges.isEmpty())
            {
                VersionedLocalRanges weightedRanges = new VersionedLocalRanges(ringVersion, localRanges.size());
                for (Range<Token> r : localRanges)
                {
                    // WeightedRange supports only unwrapped ranges as it relies
                    // on right - left == num tokens equality
                    for (Range<Token> u: r.unwrap())
                        weightedRanges.add(new Splitter.WeightedRange(1.0, u));
                }
                weightedRanges.sort(Comparator.comparing(Splitter.WeightedRange::left));
                return weightedRanges;
            }
            else
            {
                return fullWeightedRange(ringVersion, getPartitioner());
            }
        }
        else
        {
            // Local tables need to cover the full token range and don't care about ring changes.
            // We also end up here if the table's partitioner is not the database's, which can happen in tests.
            return fullWeightedRange(RING_VERSION_IRRELEVANT, getPartitioner());
        }
    }

    @Override
    public ShardBoundaries localRangeSplits(int shardCount)
    {
        if (shardCount == 1 || !getPartitioner().splitter().isPresent())
            return ShardBoundaries.NONE;

        ShardBoundaries shardBoundaries = cachedShardBoundaries;

        if (shardBoundaries == null ||
            shardBoundaries.shardCount() != shardCount ||
            (shardBoundaries.ringVersion != RING_VERSION_IRRELEVANT &&
             shardBoundaries.ringVersion != StorageService.instance.getTokenMetadata().getRingVersion()))
        {
            VersionedLocalRanges weightedRanges = localRangesWeighted();

            List<Token> boundaries = getPartitioner().splitter().get().splitOwnedRanges(shardCount, weightedRanges, false);
            shardBoundaries = new ShardBoundaries(boundaries.subList(0, boundaries.size() - 1),
                                                  weightedRanges.ringVersion);
            cachedShardBoundaries = shardBoundaries;
            logger.debug("Memtable shard boundaries for {}.{}: {}", getKeyspaceName(), getTableName(), boundaries);
        }
        return shardBoundaries;
    }

    @VisibleForTesting
    public static VersionedLocalRanges fullWeightedRange(long ringVersion, IPartitioner partitioner)
    {
        VersionedLocalRanges ranges = new VersionedLocalRanges(ringVersion, 1);
        ranges.add(new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken())));
        return ranges;
    }

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    public Collection<SSTableReader> getOverlappingLiveSSTables(Iterable<SSTableReader> sstables)
    {
        logger.trace("Checking for sstables overlapping {}", sstables);

        // a normal compaction won't ever have an empty sstables list, but we create a skeleton
        // compaction controller for streaming, and that passes an empty list.
        if (!sstables.iterator().hasNext())
            return ImmutableSet.of();

        View view = data.getView();

        List<SSTableReader> sortedByFirst = Lists.newArrayList(sstables);
        sortedByFirst.sort(SSTableReader.firstKeyComparator);

        List<AbstractBounds<PartitionPosition>> bounds = new ArrayList<>();
        DecoratedKey first = null, last = null;
        /*
        normalize the intervals covered by the sstables
        assume we have sstables like this (brackets representing first/last key in the sstable);
        [   ] [   ]    [   ]   [  ]
           [   ]         [       ]
        then we can, instead of searching the interval tree 6 times, normalize the intervals and
        only query the tree 2 times, for these intervals;
        [         ]    [          ]
         */
        for (SSTableReader sstable : sortedByFirst)
        {
            if (first == null)
            {
                first = sstable.getFirst();
                last = sstable.getLast();
            }
            else
            {
                if (sstable.getFirst().compareTo(last) <= 0) // we do overlap
                {
                    if (sstable.getLast().compareTo(last) > 0)
                        last = sstable.getLast();
                }
                else
                {
                    bounds.add(AbstractBounds.bounds(first, true, last, true));
                    first = sstable.getFirst();
                    last = sstable.getLast();
                }
            }
        }
        bounds.add(AbstractBounds.bounds(first, true, last, true));
        Set<SSTableReader> results = new HashSet<>();

        for (AbstractBounds<PartitionPosition> bound : bounds)
            Iterables.addAll(results, view.liveSSTablesInBounds(bound.left, bound.right));

        return Sets.difference(results, ImmutableSet.copyOf(sstables));
    }

    /**
     * like getOverlappingSSTables, but acquires references before returning
     */
    public Refs<SSTableReader> getAndReferenceOverlappingLiveSSTables(Iterable<SSTableReader> sstables)
    {
        while (true)
        {
            Iterable<SSTableReader> overlapped = getOverlappingLiveSSTables(sstables);
            Refs<SSTableReader> refs = Refs.tryRef(overlapped);
            if (refs != null)
                return refs;
        }
    }

    /*
     * Called after a BinaryMemtable flushes its in-memory data, or we add a file
     * via bootstrap. This information is cached in the ColumnFamilyStore.
     * This is useful for reads because the ColumnFamilyStore first looks in
     * the in-memory store and the into the disk to find the key. If invoked
     * during recoveryMode the onMemtableFlush() need not be invoked.
     *
     * param @ filename - filename just flushed to disk
     */
    public void addSSTable(SSTableReader sstable)
    {
        assert sstable.getColumnFamilyName().equals(name);
        addSSTables(Collections.singletonList(sstable));
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        data.addSSTables(sstables);
        CompactionManager.instance.submitBackground(this);
    }

    /**
     * Calculate expected file size of SSTable after compaction.
     *
     * If operation type is {@code CLEANUP} and we're not dealing with an index sstable,
     * then we calculate expected file size with checking token range to be eliminated.
     *
     * Otherwise, we just add up all the files' size, which is the worst case file
     * size for compaction of all the list of files given.
     *
     * @param sstables SSTables to calculate expected compacted file size
     * @param operation Operation type
     * @return Expected file size of SSTable after compaction
     */
    public long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operation)
    {
        if (operation != OperationType.CLEANUP || isIndex())
        {
            return SSTableReader.getTotalBytes(sstables);
        }

        // cleanup size estimation only counts bytes for keys local to this node
        long expectedFileSize = 0;
        Collection<Range<Token>> ranges = StorageService.instance.getLocalReplicas(getKeyspaceName()).ranges();
        for (SSTableReader sstable : sstables)
        {
            List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(ranges);
            for (SSTableReader.PartitionPositionBounds position : positions)
                expectedFileSize += position.upperPosition - position.lowerPosition;
        }

        double compressionRatio = metric.compressionRatio.getValue();
        if (compressionRatio > 0d)
            expectedFileSize *= compressionRatio;

        return expectedFileSize;
    }

    /*
     *  Find the maximum size file in the list .
     */
    public SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables)
    {
        long maxSize = 0L;
        SSTableReader maxFile = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.onDiskLength() > maxSize)
            {
                maxSize = sstable.onDiskLength();
                maxFile = sstable;
            }
        }
        return maxFile;
    }

    public CompactionManager.AllSSTableOpStatus forceCleanup(int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performCleanup(ColumnFamilyStore.this, jobs);
    }

    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, IScrubber.Options options, int jobs) throws ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, false, options, jobs);
    }

    @VisibleForTesting
    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean alwaysFail, IScrubber.Options options, int jobs) throws ExecutionException, InterruptedException
    {
        // skip snapshot creation during scrub, SEE JIRA 5891
        if(!disableSnapshot)
        {
            Instant creationTime = now();
            String snapshotName = "pre-scrub-" + creationTime.toEpochMilli();
            snapshotWithoutMemtable(snapshotName, creationTime);
        }

        try
        {
            return CompactionManager.instance.performScrub(ColumnFamilyStore.this, options, jobs);
        }
        catch(Throwable t)
        {
            if (!rebuildOnFailedScrub(t))
                throw t;

            return alwaysFail ? CompactionManager.AllSSTableOpStatus.ABORTED : CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        }
    }

    /**
     * CASSANDRA-5174 : For an index cfs we may be able to discard everything and just rebuild
     * the index when a scrub fails.
     *
     * @return true if we are an index cfs and we successfully rebuilt the index
     */
    public boolean rebuildOnFailedScrub(Throwable failure)
    {
        if (!isIndex() || !SecondaryIndexManager.isIndexColumnFamilyStore(this))
            return false;

        truncateBlocking();

        logger.warn("Rebuilding index for {} because of <{}>", name, failure.getMessage());

        ColumnFamilyStore parentCfs = SecondaryIndexManager.getParentCfs(this);
        assert parentCfs.indexManager.getAllIndexColumnFamilyStores().contains(this);

        String indexName = SecondaryIndexManager.getIndexName(this);

        parentCfs.rebuildSecondaryIndex(indexName);
        return true;
    }

    public CompactionManager.AllSSTableOpStatus verify(IVerifier.Options options) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performVerify(ColumnFamilyStore.this, options);
    }

    /**
     * Rewrites all SSTables according to specified parameters
     *
     * @param skipIfCurrentVersion - if {@link true}, will rewrite only SSTables that have version older than the current one ({@link SSTableFormat#getLatestVersion()})
     * @param skipIfNewerThanTimestamp - max timestamp (local creation time) for SSTable; SSTables created _after_ this timestamp will be excluded from compaction
     * @param skipIfCompressionMatches - if {@link true}, will rewrite only SSTables whose compression parameters are different from {@code TableMetadata#params#getCompressionParameters()}
     * @param jobs number of jobs for parallel execution
     */
    public CompactionManager.AllSSTableOpStatus sstablesRewrite(final boolean skipIfCurrentVersion,
                                                                final long skipIfNewerThanTimestamp,
                                                                final boolean skipIfCompressionMatches,
                                                                final int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performSSTableRewrite(ColumnFamilyStore.this, skipIfCurrentVersion, skipIfNewerThanTimestamp, skipIfCompressionMatches, jobs);
    }

    public CompactionManager.AllSSTableOpStatus relocateSSTables(int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.relocateSSTables(this, jobs);
    }

    public CompactionManager.AllSSTableOpStatus garbageCollect(TombstoneOption tombstoneOption, int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performGarbageCollection(this, tombstoneOption, jobs);
    }

    public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        assert !sstables.isEmpty();
        maybeFail(data.dropSSTables(Predicates.in(sstables), compactionType, null));
    }

    void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables)
    {
        data.replaceFlushed(memtable, sstables);
        if (sstables != null && !sstables.isEmpty())
            CompactionManager.instance.submitBackground(this);
    }

    public boolean isValid()
    {
        return valid;
    }

    /**
     * Package protected for access from the CompactionManager.
     */
    public Tracker getTracker()
    {
        return data;
    }

    public Set<SSTableReader> getLiveSSTables()
    {
        return data.getView().liveSSTables();
    }

    public Iterable<SSTableReader> getSSTables(SSTableSet sstableSet)
    {
        return data.getView().select(sstableSet);
    }

    public Iterable<SSTableReader> getUncompactingSSTables()
    {
        return data.getUncompacting();
    }

    public Map<TimeUUID, PendingStat> getPendingRepairStats()
    {
        Map<TimeUUID, PendingStat.Builder> builders = new HashMap<>();
        for (SSTableReader sstable : getLiveSSTables())
        {
            TimeUUID session = sstable.getPendingRepair();
            if (session == null)
                continue;

            if (!builders.containsKey(session))
                builders.put(session, new PendingStat.Builder());

            builders.get(session).addSSTable(sstable);
        }

        Map<TimeUUID, PendingStat> stats = new HashMap<>();
        for (Map.Entry<TimeUUID, PendingStat.Builder> entry : builders.entrySet())
        {
            stats.put(entry.getKey(), entry.getValue().build());
        }
        return stats;
    }

    /**
     * promotes (or demotes) data attached to an incremental repair session that has either completed successfully,
     * or failed
     *
     * @return session ids whose data could not be released
     */
    public CleanupSummary releaseRepairData(Collection<TimeUUID> sessions, boolean force)
    {
        if (force)
        {
            Predicate<SSTableReader> predicate = sst -> {
                TimeUUID session = sst.getPendingRepair();
                return session != null && sessions.contains(session);
            };
            return runWithCompactionsDisabled(() -> compactionStrategyManager.releaseRepairData(sessions),
                                              predicate, OperationType.STREAM, false, true, true);
        }
        else
        {
            return compactionStrategyManager.releaseRepairData(sessions);
        }
    }

    public boolean isFilterFullyCoveredBy(ClusteringIndexFilter filter,
                                          DataLimits limits,
                                          CachedPartition cached,
                                          long nowInSec,
                                          boolean enforceStrictLiveness)
    {
        // We can use the cached value only if we know that no data it doesn't contain could be covered
        // by the query filter, that is if:
        //   1) either the whole partition is cached
        //   2) or we can ensure than any data the filter selects is in the cached partition

        // We can guarantee that a partition is fully cached if the number of rows it contains is less than
        // what we're caching. Wen doing that, we should be careful about expiring cells: we should count
        // something expired that wasn't when the partition was cached, or we could decide that the whole
        // partition is cached when it's not. This is why we use CachedPartition#cachedLiveRows.
        if (cached.cachedLiveRows() < metadata().params.caching.rowsPerPartitionToCache())
            return true;

        // If the whole partition isn't cached, then we must guarantee that the filter cannot select data that
        // is not in the cache. We can guarantee that if either the filter is a "head filter" and the cached
        // partition has more live rows that queried (where live rows refers to the rows that are live now),
        // or if we can prove that everything the filter selects is in the cached partition based on its content.
        return (filter.isHeadFilter() && limits.hasEnoughLiveData(cached,
                                                                  nowInSec,
                                                                  filter.selectsAllPartition(),
                                                                  enforceStrictLiveness))
               || filter.isFullyCoveredBy(cached);
    }

    public PaxosRepairHistory getPaxosRepairHistory()
    {
        return paxosRepairHistory.get().getHistory();
    }

    public PaxosRepairHistory getPaxosRepairHistoryForRanges(Collection<Range<Token>> ranges)
    {
        return paxosRepairHistory.get().getHistoryForRanges(ranges);
    }

    public void syncPaxosRepairHistory(PaxosRepairHistory sync, boolean flush)
    {
        paxosRepairHistory.get().merge(sync, flush);
    }

    public void onPaxosRepairComplete(Collection<Range<Token>> ranges, Ballot highBallot)
    {
        paxosRepairHistory.get().add(ranges, highBallot, true);
    }

    public Ballot getPaxosRepairLowBound(DecoratedKey key)
    {
        return paxosRepairHistory.get().getBallotForToken(key.getToken());
    }

    public long gcBefore(long nowInSec)
    {
        return nowInSec - metadata().params.gcGraceSeconds;
    }

    public RefViewFragment selectAndReference(Function<View, Iterable<SSTableReader>> filter)
    {
        long failingSince = -1L;
        while (true)
        {
            ViewFragment view = select(filter);
            Refs<SSTableReader> refs = Refs.tryRef(view.sstables);
            if (refs != null)
                return new RefViewFragment(view.sstables, view.memtables, refs);
            if (failingSince <= 0)
            {
                failingSince = nanoTime();
            }
            else if (nanoTime() - failingSince > TimeUnit.MILLISECONDS.toNanos(100))
            {
                List<SSTableReader> released = new ArrayList<>();
                for (SSTableReader reader : view.sstables)
                    if (reader.selfRef().globalCount() == 0)
                        released.add(reader);
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                 "Spinning trying to capture readers {}, released: {}, ", view.sstables, released);
                failingSince = nanoTime();
            }
        }
    }

    public ViewFragment select(Function<View, Iterable<SSTableReader>> filter)
    {
        View view = data.getView();
        List<SSTableReader> sstables = Lists.newArrayList(Objects.requireNonNull(filter.apply(view)));
        return new ViewFragment(sstables, view.getAllMemtables());
    }

    // WARNING: this returns the set of LIVE sstables only, which may be only partially written
    public List<String> getSSTablesForKey(String key)
    {
        return getSSTablesForKey(key, false);
    }

    public List<String> getSSTablesForKey(String key, boolean hexFormat)
    {
        return withSSTablesForKey(key, hexFormat, SSTableReader::getFilename);
    }

    public Map<Integer, Set<String>> getSSTablesForKeyWithLevel(String key, boolean hexFormat)
    {
        List<Pair<Integer, String>> ssts = withSSTablesForKey(key, hexFormat, sstr -> Pair.create(sstr.getSSTableLevel(), sstr.getFilename()));
        HashMap<Integer, Set<String>> result = new HashMap<>();
        for (Pair<Integer, String> sst : ssts)
        {
            Set<String> perLevel = result.get(sst.left);
            if (perLevel == null)
            {
                perLevel = new HashSet<>();
                result.put(sst.left, perLevel);
            }

            perLevel.add(sst.right);
        }

        return result;
    }

    public <T> List<T> withSSTablesForKey(String key, boolean hexFormat, Function<SSTableReader, T> mapper)
    {
        ByteBuffer keyBuffer = hexFormat ? ByteBufferUtil.hexToBytes(key) : metadata().partitionKeyType.fromString(key);
        DecoratedKey dk = decorateKey(keyBuffer);
        try (OpOrder.Group op = readOrdering.start())
        {
            List<T> mapped = new ArrayList<>();
            for (SSTableReader sstr : select(View.select(SSTableSet.LIVE, dk)).sstables)
            {
                // check if the key actually exists in this sstable, without updating cache and stats
                if (sstr.getPosition(dk, SSTableReader.Operator.EQ, false) >= 0)
                    mapped.add(mapper.apply(sstr));
            }
            return mapped;
        }
    }

    @Override
    public void beginLocalSampling(String sampler, int capacity, int durationMillis)
    {
        metric.samplers.get(SamplerType.valueOf(sampler)).beginSampling(capacity, durationMillis);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<CompositeData> finishLocalSampling(String sampler, int count) throws OpenDataException
    {
        Sampler samplerImpl = metric.samplers.get(SamplerType.valueOf(sampler));
        List<Sample> samplerResults = samplerImpl.finishSampling(count);
        List<CompositeData> result = new ArrayList<>(count);
        for (Sample counter : samplerResults)
        {
            //Not duplicating the buffer for safety because AbstractSerializer and ByteBufferUtil.bytesToHex
            //don't modify position or limit
            result.add(new CompositeDataSupport(COUNTER_COMPOSITE_TYPE, COUNTER_NAMES, new Object[] {
                    getKeyspaceName() + "." + name,
                    counter.count,
                    counter.error,
                    samplerImpl.toString(counter.value) })); // string
        }
        return result;
    }

    @Override
    public boolean isCompactionDiskSpaceCheckEnabled()
    {
        return compactionSpaceCheck;
    }

    @Override
    public void compactionDiskSpaceCheck(boolean enable)
    {
        compactionSpaceCheck = enable;
    }

    public void cleanupCache()
    {
        Collection<Range<Token>> ranges = StorageService.instance.getLocalReplicas(getKeyspaceName()).ranges();

        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.sameTable(metadata()) && !Range.isInRanges(dk.getToken(), ranges))
                invalidateCachedPartition(dk);
        }

        if (metadata().isCounter())
        {
            for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
                 keyIter.hasNext(); )
            {
                CounterCacheKey key = keyIter.next();
                DecoratedKey dk = decorateKey(key.partitionKey());
                if (key.sameTable(metadata()) && !Range.isInRanges(dk.getToken(), ranges))
                    CacheService.instance.counterCache.remove(key);
            }
        }
    }

    public ClusteringComparator getComparator()
    {
        return metadata().comparator;
    }

    public TableSnapshot snapshotWithoutMemtable(String snapshotName)
    {
        return snapshotWithoutMemtable(snapshotName, now());
    }

    public TableSnapshot snapshotWithoutMemtable(String snapshotName, Instant creationTime)
    {
        return snapshotWithoutMemtable(snapshotName, null, false, null, null, creationTime);
    }

    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned during next startup
     */
    public TableSnapshot snapshotWithoutMemtable(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, DurationSpec.IntSecondsBound ttl, RateLimiter rateLimiter, Instant creationTime)
    {
        if (ephemeral && ttl != null)
        {
            throw new IllegalStateException(String.format("can not take ephemeral snapshot (%s) while ttl is specified too", snapshotName));
        }

        if (rateLimiter == null)
            rateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();

        Set<SSTableReader> snapshottedSSTables = new LinkedHashSet<>();
        for (ColumnFamilyStore cfs : concatWithIndexes())
        {
            try (RefViewFragment currentView = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.path(), rateLimiter); // hard links
                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", keyspace, ssTable.getFilename(), snapshotDirectory);
                    snapshottedSSTables.add(ssTable);
                }
            }
        }

        return createSnapshot(snapshotName, ephemeral, ttl, snapshottedSSTables, creationTime);
    }

    protected TableSnapshot createSnapshot(String tag, boolean ephemeral, DurationSpec.IntSecondsBound ttl, Set<SSTableReader> sstables, Instant creationTime) {
        Set<File> snapshotDirs = sstables.stream()
                                         .map(s -> Directories.getSnapshotDirectory(s.descriptor, tag).toAbsolute())
                                         .filter(dir -> !Directories.isSecondaryIndexFolder(dir)) // Remove secondary index subdirectory
                                         .collect(Collectors.toCollection(HashSet::new));

        // Create and write snapshot manifest
        SnapshotManifest manifest = new SnapshotManifest(mapToDataFilenames(sstables), ttl, creationTime, ephemeral);
        File manifestFile = getDirectories().getSnapshotManifestFile(tag);
        writeSnapshotManifest(manifest, manifestFile);
        snapshotDirs.add(manifestFile.parent().toAbsolute()); // manifest may create empty snapshot dir

        // Write snapshot schema
        if (!SchemaConstants.isLocalSystemKeyspace(metadata.keyspace) && !SchemaConstants.isReplicatedSystemKeyspace(metadata.keyspace))
        {
            File schemaFile = getDirectories().getSnapshotSchemaFile(tag);
            writeSnapshotSchema(schemaFile);
            snapshotDirs.add(schemaFile.parent().toAbsolute()); // schema may create empty snapshot dir
        }

        TableSnapshot snapshot = new TableSnapshot(metadata.keyspace, metadata.name, metadata.id.asUUID(),
                                                   tag, manifest.createdAt, manifest.expiresAt, snapshotDirs,
                                                   manifest.ephemeral);

        StorageService.instance.addSnapshot(snapshot);
        return snapshot;
    }

    private SnapshotManifest writeSnapshotManifest(SnapshotManifest manifest, File manifestFile)
    {
        try
        {
            manifestFile.parent().tryCreateDirectories();
            manifest.serializeToJsonFile(manifestFile);
            return manifest;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, manifestFile);
        }
    }

    private List<String> mapToDataFilenames(Collection<SSTableReader> sstables)
    {
        return sstables.stream().map(s -> s.descriptor.relativeFilenameFor(Components.DATA)).collect(Collectors.toList());
    }

    private void writeSnapshotSchema(File schemaFile)
    {
        try
        {
            if (!schemaFile.parent().exists())
                schemaFile.parent().tryCreateDirectories();

            try (PrintStream out = new PrintStream(new FileOutputStreamPlus(schemaFile)))
            {
                SchemaCQLHelper.reCreateStatementsForSchemaCql(metadata(),
                                                               keyspace.getMetadata())
                               .forEach(out::println);
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, schemaFile);
        }
    }

    protected static void clearEphemeralSnapshots(Directories directories)
    {
        RateLimiter clearSnapshotRateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();

        List<TableSnapshot> ephemeralSnapshots = new SnapshotLoader(directories).loadSnapshots()
                                                                                .stream()
                                                                                .filter(TableSnapshot::isEphemeral)
                                                                                .collect(Collectors.toList());

        for (TableSnapshot ephemeralSnapshot : ephemeralSnapshots)
        {
            logger.trace("Clearing ephemeral snapshot {} leftover from previous session.", ephemeralSnapshot.getId());
            Directories.clearSnapshot(ephemeralSnapshot.getTag(), directories.getCFDirectories(), clearSnapshotRateLimiter);
        }
    }

    public Refs<SSTableReader> getSnapshotSSTableReaders(String tag) throws IOException
    {
        Map<SSTableId, SSTableReader> active = new HashMap<>();
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            active.put(sstable.descriptor.id, sstable);
        Map<Descriptor, Set<Component>> snapshots = getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(tag).list();
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            for (Map.Entry<Descriptor, Set<Component>> entries : snapshots.entrySet())
            {
                // Try acquire reference to an active sstable instead of snapshot if it exists,
                // to avoid opening new sstables. If it fails, use the snapshot reference instead.
                SSTableReader sstable = active.get(entries.getKey().id);
                if (sstable == null || !refs.tryRef(sstable))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("using snapshot sstable {}", entries.getKey());
                    // open offline so we don't modify components or track hotness.
                    sstable = SSTableReader.open(this, entries.getKey(), entries.getValue(), metadata, true, true);
                    refs.tryRef(sstable);
                    // release the self ref as we never add the snapshot sstable to DataTracker where it is otherwise released
                    sstable.selfRef().release();
                }
                else if (logger.isTraceEnabled())
                {
                    logger.trace("using active sstable {}", entries.getKey());
                }
            }
        }
        catch (FSReadError | RuntimeException e)
        {
            // In case one of the snapshot sstables fails to open,
            // we must release the references to the ones we opened so far
            refs.release();
            throw e;
        }
        return refs;
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     */
    public TableSnapshot snapshot(String snapshotName)
    {
        return snapshot(snapshotName, null);
    }

    public TableSnapshot snapshot(String snapshotName, DurationSpec.IntSecondsBound ttl)
    {
        return snapshot(snapshotName, false, ttl, null, now());
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     * @param skipMemtable Skip flushing the memtable
     * @param ttl duration after which the taken snapshot is removed automatically, if supplied with null, it will never be automatically removed
     * @param rateLimiter Rate limiter for hardlinks-per-second
     * @param creationTime time when this snapshot was taken
     */
    public TableSnapshot snapshot(String snapshotName, boolean skipMemtable, DurationSpec.IntSecondsBound ttl, RateLimiter rateLimiter, Instant creationTime)
    {
        return snapshot(snapshotName, null, false, skipMemtable, ttl, rateLimiter, creationTime);
    }


    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     * @param skipMemtable Skip flushing the memtable
     */
    public TableSnapshot snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, boolean skipMemtable)
    {
        return snapshot(snapshotName, predicate, ephemeral, skipMemtable, null, null, now());
    }

    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     * @param skipMemtable Skip flushing the memtable
     * @param ttl duration after which the taken snapshot is removed automatically, if supplied with null, it will never be automatically removed
     * @param rateLimiter Rate limiter for hardlinks-per-second
     * @param creationTime time when this snapshot was taken
     */
    public TableSnapshot snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, boolean skipMemtable, DurationSpec.IntSecondsBound ttl, RateLimiter rateLimiter, Instant creationTime)
    {
        if (!skipMemtable)
        {
            Memtable current = getTracker().getView().getCurrentMemtable();
            if (!current.isClean())
            {
                if (current.shouldSwitch(FlushReason.SNAPSHOT))
                    FBUtilities.waitOnFuture(switchMemtableIfCurrent(current, FlushReason.SNAPSHOT));
                else
                    current.performSnapshot(snapshotName);
            }
        }
        return snapshotWithoutMemtable(snapshotName, predicate, ephemeral, ttl, rateLimiter, creationTime);
    }

    public boolean snapshotExists(String snapshotName)
    {
        return getDirectories().snapshotExists(snapshotName);
    }


    /**
     * Clear all the snapshots for a given column family.
     *
     * @param snapshotName the user supplied snapshot name. If left empty,
     *                     all the snapshots will be cleaned.
     */
    public void clearSnapshot(String snapshotName)
    {
        RateLimiter clearSnapshotRateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();

        List<File> snapshotDirs = getDirectories().getCFDirectories();
        Directories.clearSnapshot(snapshotName, snapshotDirs, clearSnapshotRateLimiter);
    }
    /**
     *
     * @return  Return a map of all snapshots to space being used
     * The pair for a snapshot has true size and size on disk.
     */
    public Map<String, TableSnapshot> listSnapshots()
    {
        return getDirectories().listSnapshots();
    }

    /**
     * @return the cached partition for @param key if it is already present in the cache.
     * Not that this will not readAndCache the parition if it is not present, nor
     * are these calls counted in cache statistics.
     *
     * Note that this WILL cause deserialization of a SerializingCache partition, so if all you
     * need to know is whether a partition is present or not, use containsCachedParition instead.
     */
    public CachedPartition getRawCachedPartition(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return null;
        IRowCacheEntry cached = CacheService.instance.rowCache.getInternal(new RowCacheKey(metadata(), key));
        return cached == null || cached instanceof RowCacheSentinel ? null : (CachedPartition)cached;
    }

    private void invalidateCaches()
    {
        CacheService.instance.invalidateKeyCacheForCf(metadata());
        CacheService.instance.invalidateRowCacheForCf(metadata());
        if (metadata().isCounter())
            CacheService.instance.invalidateCounterCacheForCf(metadata());
    }

    public int invalidateRowCache(Collection<Bounds<Token>> boundsToInvalidate)
    {
        int invalidatedKeys = 0;
        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.sameTable(metadata()) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
            {
                invalidateCachedPartition(dk);
                invalidatedKeys++;
            }
        }
        return invalidatedKeys;
    }

    public int invalidateCounterCache(Collection<Bounds<Token>> boundsToInvalidate)
    {
        int invalidatedKeys = 0;
        for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
             keyIter.hasNext(); )
        {
            CounterCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(key.partitionKey());
            if (key.sameTable(metadata()) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
            {
                CacheService.instance.counterCache.remove(key);
                invalidatedKeys++;
            }
        }
        return invalidatedKeys;
    }

    /**
     * @return true if @param key is contained in the row cache
     */
    public boolean containsCachedParition(DecoratedKey key)
    {
        return CacheService.instance.rowCache.getCapacity() != 0 && CacheService.instance.rowCache.containsKey(new RowCacheKey(metadata(), key));
    }

    public void invalidateCachedPartition(RowCacheKey key)
    {
        CacheService.instance.rowCache.remove(key);
    }

    public void invalidateCachedPartition(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return;

        invalidateCachedPartition(new RowCacheKey(metadata(), key));
    }

    public ClockAndCount getCachedCounter(ByteBuffer partitionKey, Clustering<?> clustering, ColumnMetadata column, CellPath path)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return null;
        return CacheService.instance.counterCache.get(CounterCacheKey.create(metadata(), partitionKey, clustering, column, path));
    }

    public void putCachedCounter(ByteBuffer partitionKey, Clustering<?> clustering, ColumnMetadata column, CellPath path, ClockAndCount clockAndCount)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return;
        CacheService.instance.counterCache.put(CounterCacheKey.create(metadata(), partitionKey, clustering, column, path), clockAndCount);
    }

    public void forceMajorCompaction()
    {
        forceMajorCompaction(false);
    }

    public void forceMajorCompaction(boolean splitOutput)
    {
        CompactionManager.instance.performMaximal(this, splitOutput);
    }

    @Override
    public void forceCompactionForTokenRange(Collection<Range<Token>> tokenRanges) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.forceCompactionForTokenRange(this, tokenRanges);
    }

    @Override
    public void forceCompactionForTokenRanges(String... strings)
    {
        CompactionManager.instance.forceCompactionForTokenRange(this, toTokenRanges(DatabaseDescriptor.getPartitioner(), strings));
    }

    static Set<Range<Token>> toTokenRanges(IPartitioner partitioner, String... strings)
    {
        Token.TokenFactory tokenFactory = partitioner.getTokenFactory();
        Set<Range<Token>> tokenRanges = new HashSet<>();
        for (String str : strings)
        {
            String[] splits = str.split(TOKEN_DELIMITER);
            assert splits.length == 2 : String.format("Unable to parse token range %s; needs to have two tokens separated by %s", str, TOKEN_DELIMITER);
            String lhsStr = splits[0];
            assert !Strings.isNullOrEmpty(lhsStr) : String.format("Unable to parse token range %s; left hand side of the token separater is empty", str);
            String rhsStr = splits[1];
            assert !Strings.isNullOrEmpty(rhsStr) : String.format("Unable to parse token range %s; right hand side of the token separater is empty", str);
            Token lhs = tokenFactory.fromString(lhsStr);
            Token rhs = tokenFactory.fromString(rhsStr);
            tokenRanges.add(new Range<>(lhs, rhs));
        }
        return tokenRanges;
    }

    public void forceCompactionForKey(DecoratedKey key)
    {
        CompactionManager.instance.forceCompactionForKey(this, key);
    }

    public void forceCompactionKeysIgnoringGcGrace(String... partitionKeysIgnoreGcGrace)
    {
        List<DecoratedKey> decoratedKeys = new ArrayList<>();
        try
        {
            partitionKeySetIgnoreGcGrace.clear();

            for (String key : partitionKeysIgnoreGcGrace) {
                DecoratedKey dk = decorateKey(metadata().partitionKeyType.fromString(key));
                partitionKeySetIgnoreGcGrace.add(dk);
                decoratedKeys.add(dk);
            }

            CompactionManager.instance.forceCompactionForKeys(this, decoratedKeys);
        } finally
        {
            partitionKeySetIgnoreGcGrace.clear();
        }
    }

    public boolean shouldIgnoreGcGraceForKey(DecoratedKey dk)
    {
        return partitionKeySetIgnoreGcGrace.contains(dk);
    }

    public static Iterable<ColumnFamilyStore> all()
    {
        List<Iterable<ColumnFamilyStore>> stores = new ArrayList<>(Schema.instance.getKeyspaces().size());
        for (Keyspace keyspace : Keyspace.all())
        {
            stores.add(keyspace.getColumnFamilyStores());
        }
        return Iterables.concat(stores);
    }

    public Iterable<DecoratedKey> keySamples(Range<Token> range)
    {
        try (RefViewFragment view = selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            Iterable<DecoratedKey>[] samples = new Iterable[view.sstables.size()];
            int i = 0;
            for (SSTableReader sstable: view.sstables)
            {
                samples[i++] = sstable.getKeySamples(range);
            }
            return Iterables.concat(samples);
        }
    }

    public long estimatedKeysForRange(Range<Token> range)
    {
        try (RefViewFragment view = selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            long count = 0;
            for (SSTableReader sstable : view.sstables)
                count += sstable.estimatedKeysForRanges(Collections.singleton(range));
            return count;
        }
    }

    public void writeAndAddMemtableRanges(TimeUUID repairSessionID,
                                          Supplier<Collection<Range<PartitionPosition>>> rangesSupplier,
                                          Refs<SSTableReader> placeIntoRefs)
    {
        SSTableMultiWriter memtableContent = writeMemtableRanges(rangesSupplier, repairSessionID);
        if (memtableContent != null)
        {
            try
            {
                Collection<SSTableReader> sstables = memtableContent.finish(true);
                try (Refs sstableReferences = Refs.ref(sstables))
                {
                    // This moves all references to placeIntoRefs, clearing sstableReferences
                    placeIntoRefs.addAll(sstableReferences);
                }

                // Release the reference any written sstables start with.
                for (SSTableReader rdr : sstables)
                {
                    rdr.selfRef().release();
                    logger.info("Memtable ranges (keys {} size {}) written in {}",
                                rdr.estimatedKeys(),
                                rdr.getDataChannel().size(),
                                rdr);
                }
            }
            catch (Throwable t)
            {
                memtableContent.close();
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }
    }

    private SSTableMultiWriter writeMemtableRanges(Supplier<Collection<Range<PartitionPosition>>> rangesSupplier,
                                                   TimeUUID repairSessionID)
    {
        if (!streamFromMemtable())
            return null;

        Collection<Range<PartitionPosition>> ranges = rangesSupplier.get();
        Memtable current = getTracker().getView().getCurrentMemtable();
        if (current.isClean())
            return null;

        List<Memtable.FlushablePartitionSet<?>> dataSets = new ArrayList<>(ranges.size());
        IntervalSet.Builder<CommitLogPosition> commitLogIntervals = new IntervalSet.Builder();
        long keys = 0;
        for (Range<PartitionPosition> range : ranges)
        {
            Memtable.FlushablePartitionSet<?> dataSet = current.getFlushSet(range.left, range.right);
            dataSets.add(dataSet);
            commitLogIntervals.add(dataSet.commitLogLowerBound(), dataSet.commitLogUpperBound());
            keys += dataSet.partitionCount();
        }
        if (keys == 0)
            return null;

        // TODO: Can we write directly to stream, skipping disk?
        Memtable.FlushablePartitionSet<?> firstDataSet = dataSets.get(0);
        SSTableMultiWriter writer = createSSTableMultiWriter(newSSTableDescriptor(directories.getDirectoryForNewSSTables()),
                                                             keys,
                                                             0,
                                                             repairSessionID,
                                                             false,
                                                             commitLogIntervals.build(),
                                                             new SerializationHeader(true,
                                                                                     firstDataSet.metadata(),
                                                                                     firstDataSet.columns(),
                                                                                     firstDataSet.encodingStats()),
                                                             DO_NOT_TRACK);
        try
        {
            for (Memtable.FlushablePartitionSet<?> dataSet : dataSets)
                new Flushing.FlushRunnable(dataSet, writer, metric, false).call();  // executes on this thread

            return writer;
        }
        catch (Error | RuntimeException t)
        {
            writer.abort(t);
            throw t;
        }
    }

    private static final LifecycleNewTracker DO_NOT_TRACK = new LifecycleNewTracker()
    {
        public void trackNew(SSTable table)
        {
            // not tracking
        }

        public void untrackNew(SSTable table)
        {
            // not tracking
        }

        public OperationType opType()
        {
            return OperationType.FLUSH;
        }
    };

    /**
     * For testing.  No effort is made to clear historical or even the current memtables, nor for
     * thread safety.  All we do is wipe the sstable containers clean, while leaving the actual
     * data files present on disk.  (This allows tests to easily call loadNewSSTables on them.)
     */
    @VisibleForTesting
    public void clearUnsafe()
    {
        for (final ColumnFamilyStore cfs : concatWithIndexes())
        {
            cfs.runWithCompactionsDisabled((Callable<Void>) () -> {
                cfs.data.reset(memtableFactory.create(new AtomicReference<>(CommitLogPosition.NONE), cfs.metadata, cfs));
                return null;
            }, OperationType.P0, true, false);
        }
    }

    public void truncateBlocking()
    {
        truncateBlocking(false);
    }

    public void truncateBlockingWithoutSnapshot()
    {
        truncateBlocking(true);
    }

    /**
     * Truncate deletes the entire column family's data with no expensive tombstone creation
     * @param noSnapshot if {@code true} no snapshot will be taken
     */
    private void truncateBlocking(boolean noSnapshot)
    {
        // We have two goals here:
        // - truncate should delete everything written before truncate was invoked
        // - but not delete anything that isn't part of the snapshot we create.
        // We accomplish this by first flushing manually, then snapshotting, and
        // recording the timestamp IN BETWEEN those actions. Any sstables created
        // with this timestamp or greater time, will not be marked for delete.
        //
        // Bonus complication: since we store commit log segment position in sstable metadata,
        // truncating those sstables means we will replay any CL segments from the
        // beginning if we restart before they [the CL segments] are discarded for
        // normal reasons post-truncate.  To prevent this, we store truncation
        // position in the System keyspace.
        logger.info("Truncating {}.{}", getKeyspaceName(), name);

        viewManager.stopBuild();

        final long truncatedAt;
        final CommitLogPosition replayAfter;

        if (!noSnapshot &&
               ((keyspace.getMetadata().params.durableWrites && !memtableWritesAreDurable())  // need to clear dirty regions
               || isAutoSnapshotEnabled()))
        {
            replayAfter = forceBlockingFlush(FlushReason.TRUNCATE);
            viewManager.forceBlockingFlush(FlushReason.TRUNCATE);
        }
        else
        {
            // just nuke the memtable data w/o writing to disk first
            // note: this does not wait for the switch to complete, but because the post-flush processing is serial,
            // the call below does.
            viewManager.dumpMemtables();
            replayAfter = FBUtilities.waitOnFuture(dumpMemtable());
        }

        long now = currentTimeMillis();
        // make sure none of our sstables are somehow in the future (clock drift, perhaps)
        for (ColumnFamilyStore cfs : concatWithIndexes())
            for (SSTableReader sstable : cfs.getLiveSSTables())
                now = Math.max(now, sstable.maxDataAge);
        truncatedAt = now;

        Runnable truncateRunnable = new Runnable()
        {
            public void run()
            {
                logger.info("Truncating {}.{} with truncatedAt={}", getKeyspaceName(), getTableName(), truncatedAt);
                // since truncation can happen at different times on different nodes, we need to make sure
                // that any repairs are aborted, otherwise we might clear the data on one node and then
                // stream in data that is actually supposed to have been deleted
                ActiveRepairService.instance().abort((prs) -> prs.getTableIds().contains(metadata.id),
                                                   "Stopping parent sessions {} due to truncation of tableId="+metadata.id);
                data.notifyTruncated(truncatedAt);

            if (!noSnapshot && isAutoSnapshotEnabled())
                snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(name, SNAPSHOT_TRUNCATE_PREFIX), DatabaseDescriptor.getAutoSnapshotTtl());

            discardSSTables(truncatedAt);

            indexManager.truncateAllIndexesBlocking(truncatedAt);
            viewManager.truncateBlocking(replayAfter, truncatedAt);

                SystemKeyspace.saveTruncationRecord(ColumnFamilyStore.this, truncatedAt, replayAfter);
                logger.trace("cleaning out row cache");
                invalidateCaches();

            }
        };

        runWithCompactionsDisabled(FutureTask.callable(truncateRunnable), OperationType.P0, true, true);

        viewManager.build();

        logger.info("Truncate of {}.{} is complete", getKeyspaceName(), name);
    }

    /**
     * Drops current memtable without flushing to disk. This should only be called when truncating a column family
     * that cannot have dirty intervals in the commit log (i.e. one which is not durable, or where the memtable itself
     * performs durable writes).
     */
    public Future<CommitLogPosition> dumpMemtable()
    {
        synchronized (data)
        {
            final Flush flush = new Flush(true);
            flushExecutor.execute(flush);
            postFlushExecutor.execute(flush.postFlushTask);
            return flush.postFlushTask;
        }
    }

    public void unloadCf()
    {
        if (keyspace.getMetadata().params.durableWrites && !memtableWritesAreDurable())  // need to clear dirty regions
            forceBlockingFlush(ColumnFamilyStore.FlushReason.DROP);
        else
            FBUtilities.waitOnFuture(dumpMemtable());
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, OperationType operationType, boolean interruptValidation, boolean interruptViews)
    {
        return runWithCompactionsDisabled(callable, (sstable) -> true, operationType, interruptValidation, interruptViews, true);
    }

    /**
     * Runs callable with compactions paused and compactions including sstables matching sstablePredicate stopped
     *
     * @param callable what to do when compactions are paused
     * @param sstablesPredicate which sstables should we cancel compactions for
     * @param interruptValidation if we should interrupt validation compactions
     * @param interruptViews if we should interrupt view compactions
     * @param interruptIndexes if we should interrupt compactions on indexes. NOTE: if you set this to true your sstablePredicate
     *                         must be able to handle LocalPartitioner sstables!
     */
    public <V> V runWithCompactionsDisabled(Callable<V> callable, Predicate<SSTableReader> sstablesPredicate, OperationType operationType, boolean interruptValidation, boolean interruptViews, boolean interruptIndexes)
    {
        // synchronize so that concurrent invocations don't re-enable compactions partway through unexpectedly,
        // and so we only run one major compaction at a time
        synchronized (this)
        {
            logger.debug("Cancelling in-progress compactions for {}", metadata.name);
            Iterable<ColumnFamilyStore> toInterruptFor = interruptIndexes
                                                         ? concatWithIndexes()
                                                         : Collections.singleton(this);

            toInterruptFor = interruptViews
                             ? Iterables.concat(toInterruptFor, viewManager.allViewsCfs())
                             : toInterruptFor;

            Iterable<TableMetadata> toInterruptForMetadata = Iterables.transform(toInterruptFor, ColumnFamilyStore::metadata);

            try (CompactionManager.CompactionPauser pause = CompactionManager.instance.pauseGlobalCompaction();
                 CompactionManager.CompactionPauser pausedStrategies = pauseCompactionStrategies(toInterruptFor))
            {
                List<CompactionInfo.Holder> uninterruptibleTasks = CompactionManager.instance.getCompactionsMatching(toInterruptForMetadata,
                                                                                                                     (info) -> info.getTaskType().priority <= operationType.priority);
                if (!uninterruptibleTasks.isEmpty())
                {
                    logger.info("Unable to cancel in-progress compactions, since they're running with higher or same priority: {}. You can abort these operations using `nodetool stop`.",
                                uninterruptibleTasks.stream().map((compaction) -> String.format("%s@%s (%s)",
                                                                                                compaction.getCompactionInfo().getTaskType(),
                                                                                                compaction.getCompactionInfo().getTable(),
                                                                                                compaction.getCompactionInfo().getTaskId()))
                                                    .collect(Collectors.joining(",")));
                    return null;
                }

                // interrupt in-progress compactions
                CompactionManager.instance.interruptCompactionForCFs(toInterruptFor, sstablesPredicate, interruptValidation);
                CompactionManager.instance.waitForCessation(toInterruptFor, sstablesPredicate);

                // doublecheck that we finished, instead of timing out
                for (ColumnFamilyStore cfs : toInterruptFor)
                {
                    if (cfs.getTracker().getCompacting().stream().anyMatch(sstablesPredicate))
                    {
                        logger.warn("Unable to cancel in-progress compactions for {}. " +
                                    "Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded.",
                                    metadata.name);
                        return null;
                    }
                }
                logger.trace("Compactions successfully cancelled");

                // run our task
                try
                {
                    return callable.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static CompactionManager.CompactionPauser pauseCompactionStrategies(Iterable<ColumnFamilyStore> toPause)
    {
        ArrayList<ColumnFamilyStore> successfullyPaused = new ArrayList<>();
        try
        {
            for (ColumnFamilyStore cfs : toPause)
            {
                successfullyPaused.ensureCapacity(successfullyPaused.size() + 1); // to avoid OOM:ing after pausing the strategies
                cfs.getCompactionStrategyManager().pause();
                successfullyPaused.add(cfs);
            }
            return () -> maybeFail(resumeAll(null, toPause));
        }
        catch (Throwable t)
        {
            resumeAll(t, successfullyPaused);
            throw t;
        }
    }

    private static Throwable resumeAll(Throwable accumulate, Iterable<ColumnFamilyStore> cfss)
    {
        for (ColumnFamilyStore cfs : cfss)
        {
            try
            {
                cfs.getCompactionStrategyManager().resume();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    public <T> T withAllSSTables(final OperationType operationType, Function<LifecycleTransaction, T> op)
    {
        Callable<LifecycleTransaction> callable = () -> {
            assert data.getCompacting().isEmpty() : data.getCompacting();
            Iterable<SSTableReader> sstables = getLiveSSTables();
            sstables = AbstractCompactionStrategy.filterSuspectSSTables(sstables);
            LifecycleTransaction modifier = data.tryModify(sstables, operationType);
            assert modifier != null: "something marked things compacting while compactions are disabled";
            return modifier;
        };

        try (LifecycleTransaction compacting = runWithCompactionsDisabled(callable, operationType, false, false))
        {
            return op.apply(compacting);
        }
    }

    @Override
    public String toString()
    {
        return "CFS(" +
               "Keyspace='" + getKeyspaceName() + '\'' +
               ", ColumnFamily='" + name + '\'' +
               ')';
    }

    public void disableAutoCompaction()
    {
        // we don't use CompactionStrategy.pause since we don't want users flipping that on and off
        // during runWithCompactionsDisabled
        compactionStrategyManager.disable();
    }

    public void enableAutoCompaction()
    {
        enableAutoCompaction(false);
    }

    /**
     * used for tests - to be able to check things after a minor compaction
     * @param waitForFutures if we should block until autocompaction is done
     */
    @VisibleForTesting
    public void enableAutoCompaction(boolean waitForFutures)
    {
        compactionStrategyManager.enable();
        List<Future<?>> futures = CompactionManager.instance.submitBackground(this);
        if (waitForFutures)
            FBUtilities.waitOnFutures(futures);
    }

    public boolean isAutoCompactionDisabled()
    {
        return !this.compactionStrategyManager.isEnabled();
    }

    /*
     JMX getters and setters for the Default<T>s.
       - get/set minCompactionThreshold
       - get/set maxCompactionThreshold
       - get     memsize
       - get     memops
       - get/set memtime
     */

    public CompactionStrategyManager getCompactionStrategyManager()
    {
        return compactionStrategyManager;
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        try
        {
            TableParams.builder().crcCheckChance(crcCheckChance).build().validate();
            for (ColumnFamilyStore cfs : concatWithIndexes())
            {
                cfs.crcCheckChance.set(crcCheckChance);
                cfs.metadata.setLocalOverrides(cfs.metadata().unbuild().crcCheckChance(crcCheckChance).build());
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    sstable.setCrcCheckChance(crcCheckChance);
            }
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }


    @Override
    public Double getCrcCheckChance()
    {
        return crcCheckChance.value();
    }

    public void setCompactionThresholds(int minThreshold, int maxThreshold)
    {
        validateCompactionThresholds(minThreshold, maxThreshold);

        minCompactionThreshold.set(minThreshold);
        maxCompactionThreshold.set(maxThreshold);
        CompactionManager.instance.submitBackground(this);
    }

    public int getMinimumCompactionThreshold()
    {
        return minCompactionThreshold.value();
    }

    public void setMinimumCompactionThreshold(int minCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold, maxCompactionThreshold.value());
        this.minCompactionThreshold.set(minCompactionThreshold);
    }

    public int getMaximumCompactionThreshold()
    {
        return maxCompactionThreshold.value();
    }

    public void setMaximumCompactionThreshold(int maxCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold.value(), maxCompactionThreshold);
        this.maxCompactionThreshold.set(maxCompactionThreshold);
    }

    private void validateCompactionThresholds(int minThreshold, int maxThreshold)
    {
        if (minThreshold > maxThreshold)
            throw new RuntimeException(String.format("The min_compaction_threshold cannot be larger than the max_compaction_threshold. " +
                                                     "Min is '%d', Max is '%d'.", minThreshold, maxThreshold));

        if (maxThreshold == 0 || minThreshold == 0)
            throw new RuntimeException("Disabling compaction by setting min_compaction_threshold or max_compaction_threshold to 0 " +
                                       "is deprecated, set the compaction strategy option 'enabled' to 'false' instead or use the nodetool command 'disableautocompaction'.");
    }

    // End JMX get/set.

    public int getMeanEstimatedCellPerPartitionCount()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            long n = sstable.getEstimatedCellPerPartitionCount().count();
            sum += sstable.getEstimatedCellPerPartitionCount().mean() * n;
            count += n;
        }
        return count > 0 ? (int) (sum / count) : 0;
    }

    public double getMeanPartitionSize()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            long n = sstable.getEstimatedPartitionSize().count();
            sum += sstable.getEstimatedPartitionSize().mean() * n;
            count += n;
        }
        return count > 0 ? sum * 1.0 / count : 0;
    }

    public int getMeanRowCount()
    {
        long totalRows = 0;
        long totalPartitions = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            totalPartitions += sstable.getEstimatedPartitionSize().count();
            totalRows += sstable.getTotalRows();
        }

        return totalPartitions > 0 ? (int) (totalRows / totalPartitions) : 0;
    }

    public long estimateKeys()
    {
        long n = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            n += sstable.estimatedKeys();
        return n;
    }

    public IPartitioner getPartitioner()
    {
        return metadata().partitioner;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return getPartitioner().decorateKey(key);
    }

    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return metadata().isIndex();
    }

    public Iterable<ColumnFamilyStore> concatWithIndexes()
    {
        // we return the main CFS first, which we rely on for simplicity in switchMemtable(), for getting the
        // latest commit log segment position
        return Iterables.concat(Collections.singleton(this), indexManager.getAllIndexColumnFamilyStores());
    }

    public List<String> getBuiltIndexes()
    {
        return indexManager.getBuiltIndexNames();
    }

    @Override
    public int getUnleveledSSTables()
    {
        return compactionStrategyManager.getUnleveledSSTables();
    }

    @Override
    public int[] getSSTableCountPerLevel()
    {
        return compactionStrategyManager.getSSTableCountPerLevel();
    }

    @Override
    public long[] getPerLevelSizeBytes()
    {
        return compactionStrategyManager.getPerLevelSizeBytes();
    }

    @Override
    public boolean isLeveledCompaction()
    {
        return compactionStrategyManager.isLeveledCompaction();
    }

    @Override
    public int[] getSSTableCountPerTWCSBucket()
    {
        return compactionStrategyManager.getSSTableCountPerTWCSBucket();
    }

    @Override
    public int getLevelFanoutSize()
    {
        return compactionStrategyManager.getLevelFanoutSize();
    }

    public static class ViewFragment
    {
        public final List<SSTableReader> sstables;
        public final Iterable<Memtable> memtables;

        public ViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables)
        {
            this.sstables = sstables;
            this.memtables = memtables;
        }
    }

    public static class RefViewFragment extends ViewFragment implements AutoCloseable
    {
        public final Refs<SSTableReader> refs;

        public RefViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables, Refs<SSTableReader> refs)
        {
            super(sstables, memtables);
            this.refs = refs;
        }

        public void release()
        {
            refs.release();
        }

        public void close()
        {
            refs.release();
        }
    }

    public boolean isEmpty()
    {
        return data.getView().isEmpty();
    }

    public boolean isRowCacheEnabled()
    {

        boolean retval = metadata().params.caching.cacheRows() && CacheService.instance.rowCache.getCapacity() > 0;
        assert(!retval || !isIndex());
        return retval;
    }

    public boolean isCounterCacheEnabled()
    {
        return metadata().isCounter() && CacheService.instance.counterCache.getCapacity() > 0;
    }

    public boolean isKeyCacheEnabled()
    {
        return metadata().params.caching.cacheKeys() && CacheService.instance.keyCache.getCapacity() > 0;
    }

    public boolean isAutoSnapshotEnabled()
    {
        return metadata().params.allowAutoSnapshot && DatabaseDescriptor.isAutoSnapshot();
    }

    public boolean isTableIncrementalBackupsEnabled()
    {
        return DatabaseDescriptor.isIncrementalBackupsEnabled() && metadata().params.incrementalBackups;
    }

    /**
     * Discard all SSTables that were created before given timestamp.
     *
     * Caller should first ensure that comapctions have quiesced.
     *
     * @param truncatedAt The timestamp of the truncation
     *                    (all SSTables before that timestamp are going be marked as compacted)
     */
    public void discardSSTables(long truncatedAt)
    {
        assert data.getCompacting().isEmpty() : data.getCompacting();

        List<SSTableReader> truncatedSSTables = new ArrayList<>();
        int keptSSTables = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            if (!sstable.newSince(truncatedAt))
            {
                truncatedSSTables.add(sstable);
            }
            else
            {
                keptSSTables++;
                logger.info("Truncation is keeping {} maxDataAge={} truncatedAt={}", sstable, sstable.maxDataAge, truncatedAt);
            }
        }

        if (!truncatedSSTables.isEmpty())
        {
            logger.info("Truncation is dropping {} sstables and keeping {} due to sstable.maxDataAge > truncatedAt", truncatedSSTables.size(), keptSSTables);
            markObsolete(truncatedSSTables, OperationType.UNKNOWN);
        }
    }

    @Override
    public double getDroppableTombstoneRatio()
    {
        double allDroppable = 0;
        long allColumns = 0;
        int localTime = (int)(currentTimeMillis() / 1000);

        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            allDroppable += sstable.getDroppableTombstonesBefore(localTime - metadata().params.gcGraceSeconds);
            allColumns += sstable.getEstimatedCellPerPartitionCount().mean() * sstable.getEstimatedCellPerPartitionCount().count();
        }
        return allColumns > 0 ? allDroppable / allColumns : 0;
    }

    @Override
    public long trueSnapshotsSize()
    {
        return getDirectories().trueSnapshotsSize();
    }

    /**
     * Returns a ColumnFamilyStore by id if it exists, null otherwise
     * Differently from others, this method does not throw exception if the table does not exist.
     */
    public static ColumnFamilyStore getIfExists(TableId id)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(id);
        if (metadata == null)
            return null;

        Keyspace keyspace = Keyspace.open(metadata.keyspace);
        if (keyspace == null)
            return null;

        return keyspace.hasColumnFamilyStore(id)
             ? keyspace.getColumnFamilyStore(id)
             : null;
    }

    /**
     * Returns a ColumnFamilyStore by ksname and cfname if it exists, null otherwise
     * Differently from others, this method does not throw exception if the keyspace or table does not exist.
     */
    public static ColumnFamilyStore getIfExists(String ksName, String cfName)
    {
        if (ksName == null || cfName == null)
            return null;

        Keyspace keyspace = Keyspace.open(ksName);
        if (keyspace == null)
            return null;

        TableMetadata table = Schema.instance.getTableMetadata(ksName, cfName);
        if (table == null)
            return null;

        return keyspace.getColumnFamilyStore(table.id);
    }

    public static TableMetrics metricsFor(TableId tableId)
    {
        return Objects.requireNonNull(getIfExists(tableId)).metric;
    }

    /**
     * Grabs the global first/last tokens among sstables and returns the range of data directories that start/end with those tokens.
     *
     * This is done to avoid grabbing the disk boundaries for every sstable in case of huge compactions.
     */
    public List<File> getDirectoriesForFiles(Set<SSTableReader> sstables)
    {
        Directories.DataDirectory[] writeableLocations = directories.getWriteableLocations();
        if (writeableLocations.length == 1 || sstables.isEmpty())
        {
            List<File> ret = new ArrayList<>(writeableLocations.length);
            for (Directories.DataDirectory ddir : writeableLocations)
                ret.add(getDirectories().getLocationForDisk(ddir));
            return ret;
        }

        DecoratedKey first = null;
        DecoratedKey last = null;
        for (SSTableReader sstable : sstables)
        {
            if (first == null || first.compareTo(sstable.getFirst()) > 0)
                first = sstable.getFirst();
            if (last == null || last.compareTo(sstable.getLast()) < 0)
                last = sstable.getLast();
        }

        DiskBoundaries diskBoundaries = getDiskBoundaries();
        return diskBoundaries.getDisksInBounds(first, last).stream().map(directories::getLocationForDisk).collect(Collectors.toList());
    }

    public DiskBoundaries getDiskBoundaries()
    {
        return diskBoundaryManager.getDiskBoundaries(this);
    }

    public void invalidateLocalRanges()
    {
        diskBoundaryManager.invalidate();

        switchMemtableOrNotify(FlushReason.OWNED_RANGES_CHANGE, Memtable::localRangesUpdated);
    }

    @Override
    public void setNeverPurgeTombstones(boolean value)
    {
        if (neverPurgeTombstones != value)
            logger.info("Changing neverPurgeTombstones for {}.{} from {} to {}", getKeyspaceName(), getTableName(), neverPurgeTombstones, value);
        else
            logger.info("Not changing neverPurgeTombstones for {}.{}, it is {}", getKeyspaceName(), getTableName(), neverPurgeTombstones);

        neverPurgeTombstones = value;
    }

    @Override
    public boolean getNeverPurgeTombstones()
    {
        return neverPurgeTombstones;
    }

    void onTableDropped()
    {
        indexManager.markAllIndexesRemoved();

        CompactionManager.instance.interruptCompactionForCFs(concatWithIndexes(), (sstable) -> true, true);

        if (isAutoSnapshotEnabled())
            snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(name, ColumnFamilyStore.SNAPSHOT_DROP_PREFIX), DatabaseDescriptor.getAutoSnapshotTtl());

        CommitLog.instance.forceRecycleAllSegments(Collections.singleton(metadata.id));

        compactionStrategyManager.shutdown();

        // wait for any outstanding reads/writes that might affect the CFS
        Keyspace.writeOrder.awaitNewBarrier();
        readOrdering.awaitNewBarrier();
    }

    /**
     * The thread pools used to flush memtables.
     *
     * <p>Each disk has its own set of thread pools to perform memtable flushes.</p>
     * <p>Based on the configuration. Local system keyspaces can have their own disk
     * to allow for special redundancy mechanism. If it is the case the executor services returned for
     * local system keyspaces will be different from the ones for the other keyspaces.</p>
     */
    private static final class PerDiskFlushExecutors
    {
        /**
         * The flush executors for non local system keyspaces.
         */
        private final ExecutorPlus[] nonLocalSystemflushExecutors;

        /**
         * The flush executors for the local system keyspaces.
         */
        private final ExecutorPlus[] localSystemDiskFlushExecutors;

        /**
         * {@code true} if local system keyspaces are stored in their own directory and use an extra flush executor,
         * {@code false} otherwise.
         */
        private final boolean useSpecificExecutorForSystemKeyspaces;

        public PerDiskFlushExecutors(int flushWriters,
                                     String[] locationsForNonSystemKeyspaces,
                                     boolean useSpecificLocationForSystemKeyspaces)
        {
            ExecutorPlus[] flushExecutors = createPerDiskFlushWriters(locationsForNonSystemKeyspaces.length, flushWriters);
            nonLocalSystemflushExecutors = flushExecutors;
            useSpecificExecutorForSystemKeyspaces = useSpecificLocationForSystemKeyspaces;
            localSystemDiskFlushExecutors = useSpecificLocationForSystemKeyspaces ? new ExecutorPlus[] {newThreadPool("LocalSystemKeyspacesDiskMemtableFlushWriter", flushWriters)}
                                                                                  : new ExecutorPlus[] {flushExecutors[0]};
        }

        private static ExecutorPlus[] createPerDiskFlushWriters(int numberOfExecutors, int flushWriters)
        {
            ExecutorPlus[] flushExecutors = new ExecutorPlus[numberOfExecutors];
            for (int i = 0; i < numberOfExecutors; i++)
            {
                flushExecutors[i] = newThreadPool("PerDiskMemtableFlushWriter_"+i, flushWriters);
            }
            return flushExecutors;
        }

        private static ExecutorPlus newThreadPool(String poolName, int size)
        {
            return executorFactory().withJmxInternal().pooled(poolName, size);
        }

        /**
         * Returns the flush executors for the specified keyspace.
         *
         * @param keyspaceName the keyspace name
         * @param tableName the table name
         * @return the flush executors that should be used for flushing the memtables of the specified keyspace.
         */
        public ExecutorPlus[] getExecutorsFor(String keyspaceName, String tableName)
        {
            return Directories.isStoredInLocalSystemKeyspacesDataLocation(keyspaceName, tableName) ? localSystemDiskFlushExecutors
                                                                                                   : nonLocalSystemflushExecutors;
        }

        /**
         * Appends all the {@code ExecutorService} used for flushes to the collection.
         *
         * @param collection the collection to append to.
         */
        public void appendAllExecutors(Collection<ExecutorService> collection)
        {
            Collections.addAll(collection, nonLocalSystemflushExecutors);
            if (useSpecificExecutorForSystemKeyspaces)
                Collections.addAll(collection, localSystemDiskFlushExecutors);
        }
    }

    /*
     * Check SSTables whether or not they are misplaced.
     * @return true if any of the SSTables is misplaced.
     *         If all SSTables are correctly placed or the partitioner does not support splitting, it returns false.
     */
    @Override
    public boolean hasMisplacedSSTables()
    {
        if (!getPartitioner().splitter().isPresent())
            return false;

        final DiskBoundaries diskBoundaries = getDiskBoundaries();
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            Directories.DataDirectory dataDirectory = getDirectories().getDataDirectoryForFile(sstable.descriptor);
            if (!diskBoundaries.isInCorrectLocation(sstable, dataDirectory))
                return true;
        }
        return false;
    }

    @Override
    public long getMaxSSTableSize()
    {
        return metric.maxSSTableSize.getValue();
    }

    @Override
    public long getMaxSSTableDuration()
    {
        return metric.maxSSTableDuration.getValue();
    }

    @Override
    public Map<String, Long> getTopSizePartitions()
    {
        if (topPartitions == null)
            return Collections.emptyMap();
        return topPartitions.getTopSizePartitionMap();
    }

    @Override
    public Long getTopSizePartitionsLastUpdate()
    {
        if (topPartitions == null)
            return null;
        return topPartitions.topSizes().lastUpdate;
    }

    @Override
    public Map<String, Long> getTopTombstonePartitions()
    {
        if (topPartitions == null)
            return Collections.emptyMap();
        return topPartitions.getTopTombstonePartitionMap();
    }

    @Override
    public Long getTopTombstonePartitionsLastUpdate()
    {
        if (topPartitions == null)
            return null;
        return topPartitions.topTombstones().lastUpdate;
    }

    @Override
    public OpOrder.Barrier newReadOrderingBarrier()
    {
        return readOrdering.newBarrier();
    }

    @Override
    public TableMetrics getMetrics()
    {
        return metric;
    }
}
