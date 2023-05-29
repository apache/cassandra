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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CounterCacheKey;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.AbstractTableOperation;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.db.compaction.CompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionStrategyContainer;
import org.apache.cassandra.db.compaction.CompactionStrategyFactory;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.CompactionStrategyOptions;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
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
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.BloomFilterTracker;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.StorageHandler;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.metrics.Sampler.Sample;
import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.repair.TableRepairManager;
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
import org.apache.cassandra.streaming.TableStreamManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.DefaultValue;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.OverlapIterator;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLED_AUTO_COMPACTION_PROPERTY;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;
import static org.apache.cassandra.utils.Throwables.perform;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean, Memtable.Owner, CompactionRealm
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
    private static final ThreadPoolExecutor flushExecutor = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                                                                             Stage.KEEP_ALIVE_SECONDS,
                                                                                             TimeUnit.SECONDS,
                                                                                             new LinkedBlockingQueue<>(),
                                                                                             new NamedThreadFactory("MemtableFlushWriter"),
                                                                                             "internal");

    private static final PerDiskFlushExecutors perDiskflushExecutors = new PerDiskFlushExecutors(DatabaseDescriptor.getFlushWriters(),
                                                                                                 DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations(),
                                                                                                 DatabaseDescriptor.useSpecificLocationForLocalSystemData());

    // post-flush executor is single threaded to provide guarantee that any flush Future on a CF will never return until prior flushes have completed
    private static final ThreadPoolExecutor postFlushExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                                 Stage.KEEP_ALIVE_SECONDS,
                                                                                                 TimeUnit.SECONDS,
                                                                                                 new LinkedBlockingQueue<>(),
                                                                                                 new NamedThreadFactory("MemtablePostFlush"),
                                                                                                 "internal");

    private static final ThreadPoolExecutor reclaimExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                               Stage.KEEP_ALIVE_SECONDS,
                                                                                               TimeUnit.SECONDS,
                                                                                               new LinkedBlockingQueue<>(),
                                                                                               new NamedThreadFactory("MemtableReclaimMemory"),
                                                                                               "internal");

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
        SHUTDOWN,
        SNAPSHOT,
        TRUNCATE,
        DROP,
        STREAMING,
        STREAMS_RECEIVED,
        REPAIR,
        SCHEMA_CHANGE,
        UNIT_TESTS, // explicitly requested flush needed for a test
        /** Flush performed to a remote storage. Used by remote commit log replay */
        REMOTE_REPLAY,
        BATCHLOG_REPLAY
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
    @Deprecated
    private final String oldMBeanName;

    public enum STATUS
    {
        /**
         * Initial status when CFS is created
         */
        VALID,
        /**
         * When table is invalidated with unloading data
         */
        INVALID_UNLOADED,
        /**
         * When table is invalidated with dropping data
         */
        INVALID_DROPPED;

        /**
         * @return true if CFS is not invalidated
         */
        public boolean isValid()
        {
            return this == VALID;
        }

        /**
         * @return true if CFS is invalidated and sstables should be dropped locally and remotely
         */
        public boolean isInvalidAndShouldDropData()
        {
            return this == INVALID_DROPPED;
        }
    }
    private volatile STATUS status = STATUS.VALID;

    private Memtable.Factory memtableFactory;

    /**
     * Memtables and SSTables on disk for this column family.
     *
     * We synchronize on the Tracker to ensure isolation when we want to make sure
     * that the memtable we're acting on doesn't change out from under us.  I.e., flush
     * synchronizes on it to make sure it can submit on both executors atomically,
     * so anyone else who wants to make sure flush doesn't interfere should as well.
     */
    private final Tracker data;

    /* The read order, used to track accesses to off-heap memtable storage */
    public final OpOrder readOrdering = new OpOrder();

    /* This is used to generate the next index for a SSTable */
    private final Supplier<? extends SSTableId> sstableIdGenerator;

    private final StorageHandler storageHandler;
    public final SecondaryIndexManager indexManager;
    public final TableViews viewManager;

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultValue<Integer> minCompactionThreshold;
    private volatile DefaultValue<Integer> maxCompactionThreshold;
    private volatile DefaultValue<Double> crcCheckChance;

    private final CompactionStrategyFactory strategyFactory;
    private volatile CompactionStrategyContainer strategyContainer;

    private final Directories directories;

    public volatile TableMetrics metric;
    public volatile long sampleReadLatencyNanos;
    public volatile long additionalWriteLatencyNanos;

    private final CassandraTableWriteHandler writeHandler;
    private final CassandraStreamManager streamManager;

    private final TableRepairManager repairManager;

    private final SSTableImporter sstableImporter;

    private volatile boolean compactionSpaceCheck = true;

    /** The local ranges are used by the {@link DiskBoundaryManager} to create the disk boundaries but can also be
     * used independently. They are created lazily and invalidated whenever {@link this#invalidateLocalRangesAndDiskBoundaries()}
     * is called.
     */
    private volatile SortedLocalRanges localRanges;

    @VisibleForTesting
    final DiskBoundaryManager diskBoundaryManager = new DiskBoundaryManager();
    ShardBoundaries cachedShardBoundaries = null;

    private volatile boolean neverPurgeTombstones = false;

    // BloomFilterTracker is updated from corresponding {@link SSTableReader}s. Metrics are queried via CFS instance.
    private final BloomFilterTracker bloomFilterTracker = BloomFilterTracker.createMeterTracker();

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

    public boolean isReadyToServeData()
    {
        return storageHandler.isReady();
    }

    public void reload()
    {
        // metadata object has been mutated directly. make all the members jibe with new settings.

        // only update these runtime-modifiable settings if they have not been modified.
        if (!minCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.minCompactionThreshold = new DefaultValue<>(metadata().params.compaction.minCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultValue(metadata().params.compaction.maxCompactionThreshold());
        if (!crcCheckChance.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.crcCheckChance = new DefaultValue(metadata().params.crcCheckChance);

        reloadCompactionStrategy(metadata().params.compaction, CompactionStrategyContainer.ReloadReason.METADATA_CHANGE);

        indexManager.reload();

        memtableFactory = metadata().params.memtable.factory;
        if (!data.getView().liveMemtables.isEmpty())
        {
            Memtable currentMemtable = data.getView().getCurrentMemtable();
            if (currentMemtable.shouldSwitch(FlushReason.SCHEMA_CHANGE))
                switchMemtableIfCurrent(currentMemtable, FlushReason.SCHEMA_CHANGE);
            else
                currentMemtable.metadataUpdated();
        }

        if (metric.metricsAggregation != TableMetrics.MetricsAggregation.fromMetadata(metadata()))
        { // Reload the metrics if histogram aggregation has changed
            metric.release(); // release first because of those static tables containing metric names
            metric = new TableMetrics(this, memtableFactory.createMemtableMetrics(metadata));
        }
    }

    /**
     * Reload the compaction strategy using the given compaction parameters and reason.
     */
    private void reloadCompactionStrategy(CompactionParams compactionParams, CompactionStrategyContainer.ReloadReason reason)
    {
        CompactionStrategyContainer previous = strategyContainer;
        strategyContainer = strategyFactory.reload(strategyContainer, compactionParams, reason, !storageHandler.isRemote());
        if (strategyContainer != previous)
        {
            getTracker().subscribe(strategyContainer);
            if (previous != null)
                getTracker().unsubscribe(previous);
        }
    }

    public static Runnable getBackgroundCompactionTaskSubmitter()
    {
        return () -> CompactionManager.instance.submitBackground(ImmutableSet.copyOf(all()));
    }

    @VisibleForTesting
    public CompactionStrategyFactory getCompactionFactory()
    {
        return strategyFactory;
    }

    public CompactionParams getCompactionParams()
    {
        return strategyContainer.getCompactionParams();
    }

    public Map<String, String> getCompactionParameters()
    {
        return getCompactionParams().asMap();
    }

    public String getCompactionParametersJson()
    {
        return FBUtilities.json(getCompactionParameters());
    }

    public void setCompactionParameters(Map<String, String> options)
    {
        try
        {
            reloadCompactionStrategy(CompactionParams.fromMap(options), CompactionStrategyContainer.ReloadReason.JMX_REQUEST);
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
        setCompactionParameters(FBUtilities.fromJsonMap(options));
    }

    public Map<String,String> getCompressionParameters()
    {
        return metadata.getLocal().params.compression.asMap();
    }

    public String getCompressionParametersJson()
    {
        return FBUtilities.json(getCompressionParameters());
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
        setCompressionParameters(FBUtilities.fromJsonMap(options));
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
        name = columnFamilyName;
        minCompactionThreshold = new DefaultValue<>(metadata.get().params.compaction.minCompactionThreshold());
        maxCompactionThreshold = new DefaultValue<>(metadata.get().params.compaction.maxCompactionThreshold());
        crcCheckChance = new DefaultValue<>(metadata.get().params.crcCheckChance);
        viewManager = keyspace.viewManager.forTable(metadata.id);
        this.sstableIdGenerator = sstableIdGenerator;
        sampleReadLatencyNanos = DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS) / 2;
        additionalWriteLatencyNanos = DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS) / 2;
        memtableFactory = metadata.get().params.memtable.factory;

        logger.info("Initializing {}.{}", keyspace.getName(), name);

        // Create Memtable only on online
        if (DatabaseDescriptor.enableMemtableAndCommitLog())
        {
            Memtable initialMemtable  = createMemtable(new AtomicReference<>(CommitLog.instance.getCurrentPosition()));
            data = new Tracker(this, initialMemtable, loadSSTables);
        }
        else
            data = new Tracker(this, null, false);

        // Note that this needs to happen before we load the first sstables, or the global sstable tracker will not
        // be notified on the initial loading.
        data.subscribe(StorageService.instance.sstablesTracker);

        /**
         * When creating a CFS offline we change the default logic needed by CASSANDRA-8671
         * and link the passed directories to be picked up by the compaction strategy
         */
        if (offline)
            this.directories = directories;
        else
            this.directories = new Directories(metadata.get());

        storageHandler = StorageHandler.create(metadata, directories, data);

        Collection<SSTableReader> sstables = null;
        // scan for sstables corresponding to this cf and load them
        if (loadSSTables)
            sstables = storageHandler.loadInitialSSTables();

        // compaction strategy should be created after the CFS has been prepared
        this.strategyFactory = new CompactionStrategyFactory(this);
        this.strategyContainer = strategyFactory.reload(null,
                                                        metadata.get().params.compaction,
                                                        CompactionStrategyContainer.ReloadReason.FULL,
                                                        !storageHandler.isRemote());
        getTracker().subscribe(strategyContainer);

        if (!strategyContainer.isEnabled() || DISABLED_AUTO_COMPACTION_PROPERTY.getBoolean())
        {
            logger.info("Strategy driven background compactions for {} are disabled: strategy container={}, {}={}",
                        metadata, strategyContainer.isEnabled(), DISABLED_AUTO_COMPACTION_PROPERTY.getKey(),
                        DISABLED_AUTO_COMPACTION_PROPERTY.getBoolean());
            this.strategyContainer.disable();
        }

        // create the private ColumnFamilyStores for the secondary column indexes
        indexManager = new SecondaryIndexManager(this);
        for (IndexMetadata info : metadata.get().indexes)
        {
            indexManager.addIndex(info, true);
        }

        metric = new TableMetrics(this, memtableFactory.createMemtableMetrics(metadata));

        if (data.loadsstables && sstables != null)
        {
            data.updateInitialSSTableSize(sstables);
        }

        if (registerBookeeping)
        {
            // register the mbean
            mbeanName = getTableMBeanName(keyspace.getName(), name, isIndex());
            oldMBeanName = getColumnFamilieMBeanName(keyspace.getName(), name, isIndex());

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
            sampleReadLatencyNanos = metadata().params.speculativeRetry.calculateThreshold(metric.coordinatorReadLatency.tableOrKeyspaceTimer().getSnapshot(), sampleReadLatencyNanos);
            additionalWriteLatencyNanos = metadata().params.additionalWritePolicy.calculateThreshold(metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getSnapshot(), additionalWriteLatencyNanos);
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

    public TableMetadataRef metadataRef()
    {
        return metadata;
    }

    public TableMetrics metrics()
    {
        return metric;
    }

    @Override
    public AbstractReplicationStrategy getKeyspaceReplicationStrategy()
    {
        return keyspace.getReplicationStrategy();
    }

    public Directories getDirectories()
    {
        return directories;
    }

    public StorageHandler getStorageHandler()
    {
        return storageHandler;
    }

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

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, boolean isTransient, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        return createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, IntervalSet.empty(), 0, header, lifecycleNewTracker);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, boolean isTransient, IntervalSet<CommitLogPosition> commitLogPositions, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        return createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, commitLogPositions, 0, header, lifecycleNewTracker);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, boolean isTransient, IntervalSet<CommitLogPosition> commitLogPositions, int sstableLevel, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        return getCompactionStrategy().createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, commitLogPositions, sstableLevel, header, indexManager.listIndexGroups(), lifecycleNewTracker);
    }

    public boolean supportsEarlyOpen()
    {
        return strategyContainer.supportsEarlyOpen();
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
        if (logger.isTraceEnabled())
        {
            logger.trace("Invalidating CFS {}, status: {}, expectMBean: {}, dropData: {}",
                         metadata.name, status, expectMBean, dropData);
        }

        // disable and cancel in-progress compactions before invalidating
        status = dropData ? STATUS.INVALID_DROPPED : STATUS.INVALID_UNLOADED;

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

        strategyContainer.shutdown();
        Nodes.local().removeTruncationRecord(metadata.id);

        storageHandler.runWithReloadingDisabled(() -> {
            if (status.isInvalidAndShouldDropData())
            {
                data.dropSSTables();

                indexManager.dropAllIndexes(dropData);
            }
            else
            {
                // In CNDB, because of multi-tenancy, we might just unload a CFS without deleting the data as
                // a tenant can be moved to a different set of nodes, which will then need to read data from remote storage
                data.unloadSSTables();

                indexManager.unloadAllIndexes();
            }

            storageHandler.unload();

            if (status.isInvalidAndShouldDropData())
            {
                LifecycleTransaction.waitForDeletions(); // just in case an index had a reference on the sstable
            }
        });

        invalidateCaches();
        if (logger.isTraceEnabled())
            logger.trace("CFS {} invalidated", metadata.name);
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

    public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
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

            File dataFile = desc.fileFor(Component.DATA);
            if (components.contains(Component.DATA) && dataFile.length() > 0)
                // everything appears to be in order... moving on.
                continue;

            // missing the DATA file! all components are orphaned
            logger.warn("Removing orphans for {}: {}", desc, components);
            for (Component component : components)
            {
                File file = desc.fileFor(component);
                if (file.exists())
                    desc.fileFor(component).delete();
            }
        }

        // cleanup incomplete saved caches
        Pattern tmpCacheFilePattern = Pattern.compile(metadata.keyspace + '-' + metadata.name + "-(Key|Row)Cache.*\\.tmp$");
        File dir = DatabaseDescriptor.getSavedCachesLocation();

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

    @Deprecated
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
                                           sstableIdGenerator.get(),
                                           descriptor.formatType);
        }
        while (newDescriptor.fileFor(Component.DATA).exists());
        return newDescriptor;
    }

    public void rebuildSecondaryIndex(String idxName)
    {
        rebuildSecondaryIndex(keyspace.getName(), metadata.name, idxName);
    }

    public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);

        logger.info("User Requested secondary index re-build for {}/{} indexes: {}", ksName, cfName, Joiner.on(',').join(idxNames));
        cfs.indexManager.rebuildIndexesBlocking(Sets.newHashSet(Arrays.asList(idxNames)));
    }

    @Deprecated
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

    public KeyspaceMetrics getKeyspaceMetrics()
    {
        return keyspace.metric;
    }

    public Descriptor newSSTableDescriptor(File directory)
    {
        return newSSTableDescriptor(directory, SSTableFormat.Type.current().info.getLatestVersion(), SSTableFormat.Type.current());
    }

    public Descriptor newSSTableDescriptor(File directory, SSTableFormat.Type format)
    {
        return newSSTableDescriptor(directory, format.info.getLatestVersion(), format);
    }

    public Descriptor newSSTableDescriptor(File directory, Version version, SSTableFormat.Type format)
    {
        Descriptor newDescriptor = new Descriptor(version,
                                                  directory,
                                                  keyspace.getName(),
                                                  name,
                                                  sstableIdGenerator.get(),
                                                  format);
        assert !newDescriptor.fileFor(Component.DATA).exists();
        return newDescriptor;
    }

    /**
     * Switches the memtable iff the live memtable is the one provided
     *
     * @param memtable
     */
    public ListenableFuture<CommitLogPosition> switchMemtableIfCurrent(Memtable memtable, FlushReason reason)
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
    public ListenableFuture<CommitLogPosition> switchMemtable(FlushReason reason)
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

        logger.info("Enqueuing flush of {} ({}): {}",
                     name,
                     reason,
                     usage);
    }


    /**
     * Flush if there is unflushed data in the memtables
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public ListenableFuture<CommitLogPosition> forceFlush(FlushReason reason)
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
    public ListenableFuture<?> forceFlush(CommitLogPosition flushIfDirtyBefore)
    {
        // we don't loop through the remaining memtables since here we only care about commit log dirtiness
        // and this does not vary between a table and its table-backed indexes
        Memtable current = data.getView().getCurrentMemtable();
        if (current.mayContainDataBefore(flushIfDirtyBefore))
            return flushMemtable(current, FlushReason.COMMITLOG_DIRTY);
        return waitForFlushes();
    }

    private ListenableFuture<CommitLogPosition> flushMemtable(Memtable current, FlushReason reason)
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
    private ListenableFuture<CommitLogPosition> waitForFlushes()
    {
        // we grab the current memtable; once any preceding memtables have flushed, we know its
        // commitLogLowerBound has been set (as this it is set with the upper bound of the preceding memtable)
        final Memtable current = data.getView().getCurrentMemtable();
        ListenableFutureTask<CommitLogPosition> task = ListenableFutureTask.create(current::getCommitLogLowerBound);
        postFlushExecutor.execute(task);
        return task;
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
        final CountDownLatch latch = new CountDownLatch(1);
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
                throw new IllegalStateException();
            }

            CommitLogPosition commitLogUpperBound = CommitLogPosition.NONE;
            // If a flush errored out but the error was ignored, make sure we don't discard the commit log.
            if (flushFailure == null && mainMemtable != null)
            {
                commitLogUpperBound = mainMemtable.getFinalCommitLogUpperBound();
                CommitLog.instance.discardCompletedSegments(metadata.id, mainMemtable.getCommitLogLowerBound(), commitLogUpperBound);
            }

            metric.pendingFlushes.dec();

            if (flushFailure != null)
                throw Throwables.propagate(flushFailure);

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
        final ListenableFutureTask<CommitLogPosition> postFlushTask;
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
            postFlushTask = ListenableFutureTask.create(postFlush);
        }

        public void run()
        {
            if (logger.isTraceEnabled())
                logger.trace("Flush task {}@{} starts executing, waiting on barrier", hashCode(), name);

            long start = System.nanoTime();

            // mark writes older than the barrier as blocking progress, permitting them to exceed our memory limit
            // if they are stuck waiting on it, then wait for them all to complete
            writeBarrier.markBlocking();
            writeBarrier.await();

            if (logger.isTraceEnabled())
                logger.trace("Flush task for task {}@{} waited {} ms at the barrier", hashCode(), name, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

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
            postFlush.latch.countDown();

            if (logger.isTraceEnabled())
                logger.trace("Flush task task {}@{} finished", hashCode(), name);
        }

        public Collection<SSTableReader> flushMemtable(ColumnFamilyStore cfs, Memtable memtable, boolean flushNonCf2i)
        {
            if (logger.isTraceEnabled())
                logger.trace("Flush task task {}@{} flushing memtable {}", hashCode(), name, memtable);

            if (memtable.isClean() || truncate)
            {
                cfs.replaceFlushed(memtable, Collections.emptyList(), Optional.empty());
                reclaim(memtable);
                return Collections.emptyList();
            }

            List<Future<SSTableMultiWriter>> futures = new ArrayList<>();
            long totalBytesOnDisk = 0;
            long maxBytesOnDisk = 0;
            long minBytesOnDisk = Long.MAX_VALUE;
            List<SSTableReader> sstables = new ArrayList<>();
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH, metadata))
            {
                List<Flushing.FlushRunnable> flushRunnables = null;
                List<SSTableMultiWriter> flushResults = null;

                try
                {
                    // flush the memtable
                    flushRunnables = Flushing.flushRunnables(cfs, memtable, txn);
                    ExecutorService[] executors = perDiskflushExecutors.getExecutorsFor(keyspace.getName(), name);

                    for (int i = 0; i < flushRunnables.size(); i++)
                        futures.add(executors[i].submit(flushRunnables.get(i)));

                    /**
                     * we can flush 2is as soon as the barrier completes, as they will be consistent with (or ahead of) the
                     * flushed memtables and CL position, which is as good as we can guarantee.
                     * TODO: SecondaryIndex should support setBarrier(), so custom implementations can co-ordinate exactly
                     * with CL as we do with memtables/CFS-backed SecondaryIndexes.
                     */
                    if (flushNonCf2i)
                        indexManager.flushAllNonCFSBackedIndexesBlocking();

                    // It may be worthwhile to add an early abort mechanism here if one of the futures throws.
                    // In such a case this code will run the other threads to completion and only then abort the operation.
                    flushResults = Lists.newArrayList(FBUtilities.waitOnFutures(futures));
                }
                catch (Throwable t)
                {
                    logger.error("Flushing {} failed with error", memtable.toString(), t);
                    t = Flushing.abortRunnables(flushRunnables, t);

                    // wait for any flush runnables that were submitted (after aborting they should complete immediately)
                    // this ensures that the writers are aborted by FlushRunnable.writeSortedContents(), in the worst
                    // case we'll repeat the same exception twice if the initial exception was thrown whilst waiting
                    // on a future
                    t = perform(t, () -> FBUtilities.waitOnFutures(futures));

                    //finally abort the transaction
                    t = txn.abort(t);

                    // and re-throw
                    throw Throwables.propagate(t);
                }

                try
                {
                    Iterator<SSTableMultiWriter> writerIterator = flushResults.iterator();
                    while (writerIterator.hasNext())
                    {
                        @SuppressWarnings("resource")
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

                    // This can throw on remote storage, e.g. if a file cannot be uploaded
                    txn.prepareToCommit();
                }
                catch (Throwable t)
                {
                    for (SSTableMultiWriter writer : flushResults)
                        t = writer.abort(t);
                    t = txn.abort(t);
                    Throwables.propagate(t);
                }

                Throwable accumulate = null;
                for (SSTableMultiWriter writer : flushResults)
                    accumulate = writer.commit(accumulate);

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

                cfs.replaceFlushed(memtable, sstables, Optional.of(txn.opId()));
            }
            reclaim(memtable);
            cfs.strategyFactory.getCompactionLogger().flush(sstables);
            if (logger.isTraceEnabled())
            {
                logger.trace("Flushed to {} ({} sstables, {}), biggest {}, smallest {}",
                             sstables,
                             sstables.size(),
                             FBUtilities.prettyPrintMemory(totalBytesOnDisk),
                             FBUtilities.prettyPrintMemory(maxBytesOnDisk),
                             FBUtilities.prettyPrintMemory(minBytesOnDisk));
            }
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

    public ListenableFuture<CommitLogPosition> signalFlushRequired(Memtable memtable, FlushReason reason)
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

    public Iterable<Memtable> getIndexMemtables()
    {
        return Iterables.transform(indexManager.getAllIndexColumnFamilyStores(),
                                   cfs -> cfs.getTracker().getView().getCurrentMemtable());
    }

    public SecondaryIndexManager getIndexManager()
    {
        return indexManager;
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Keyspace.switchLock
     * @param update to be applied
     * @param context write context for current update
     * @param updateIndexes whether secondary indexes should be updated
     */
    @SuppressWarnings("resource") // opGroup
    public void apply(PartitionUpdate update, CassandraWriteContext context, boolean updateIndexes)
    {
        long start = System.nanoTime();
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
            metric.bytesInserted.inc(update.dataSize());
            StorageHook.instance.reportWrite(metadata.id, update);
            metric.writeLatency.addNano(System.nanoTime() - start);
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
            throw new RuntimeException(e.getMessage()
                                       + " for ks: "
                                       + keyspace.getName() + ", table: " + name, e);
        }
    }

    private UpdateTransaction newUpdateTransaction(PartitionUpdate update, CassandraWriteContext context, boolean updateIndexes, Memtable memtable)
    {
        return updateIndexes
               ? indexManager.newUpdateTransaction(update, context, FBUtilities.nowInSeconds(), memtable)
               : UpdateTransaction.NO_OP;
    }

    public ShardBoundaries localRangeSplits(int shardCount)
    {
        if (shardCount == 1 || !getPartitioner().splitter().isPresent() || SchemaConstants.isLocalSystemKeyspace(keyspace.getName()))
            return ShardBoundaries.NONE;

        ShardBoundaries shardBoundaries = cachedShardBoundaries;
        if (shardBoundaries == null ||
            shardBoundaries.shardCount() != shardCount ||
            shardBoundaries.ringVersion != keyspace.getReplicationStrategy().getTokenMetadata().getRingVersion())
        {
            SortedLocalRanges localRanges = getLocalRanges();
            List<PartitionPosition> positions = localRanges.split(shardCount);
            shardBoundaries = new ShardBoundaries(positions.subList(0, positions.size() - 1),
                                                  localRanges.getRingVersion());
            cachedShardBoundaries = shardBoundaries;
            logger.info("Memtable shard boundaries for {}.{}: {}", keyspace.getName(), getTableName(), positions);
        }
        return shardBoundaries;
    }

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    public Set<SSTableReader> getOverlappingLiveSSTables(Iterable<? extends CompactionSSTable> sstables)
    {
        if (logger.isTraceEnabled())
            logger.trace("Checking for sstables overlapping {}", sstables);

        // a normal compaction won't ever have an empty sstables list, but we create a skeleton
        // compaction controller for streaming, and that passes an empty list.
        if (!sstables.iterator().hasNext())
            return ImmutableSet.of();

        View view = data.getView();

        List<CompactionSSTable> sortedByFirst = Lists.newArrayList(sstables);
        Collections.sort(sortedByFirst, CompactionSSTable.firstKeyComparator);

        List<AbstractBounds<PartitionPosition>> bounds = new ArrayList<>();
        PartitionPosition first = null, last = null;
        /*
        normalize the intervals covered by the sstables
        assume we have sstables like this (brackets representing first/last key in the sstable);
        [   ] [   ]    [   ]   [  ]
           [   ]         [       ]
        then we can, instead of searching the interval tree 6 times, normalize the intervals and
        only query the tree 2 times, for these intervals;
        [         ]    [          ]
         */
        for (CompactionSSTable sstable : sortedByFirst)
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
        Set<SSTableReader> overlaps = new HashSet<>();

        for (AbstractBounds<PartitionPosition> bound : bounds)
            Iterables.addAll(overlaps, view.liveSSTablesInBounds(bound.left, bound.right));

        for (CompactionSSTable sstable : sstables)
            overlaps.remove(sstable);
        return overlaps;
    }

    /**
     * like getOverlappingSSTables, but acquires references before returning
     */
    public Refs<SSTableReader> getAndReferenceOverlappingLiveSSTables(Iterable<SSTableReader> sstables)
    {
        while (true)
        {
            Set<SSTableReader> overlapped = getOverlappingLiveSSTables(sstables);
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
    @VisibleForTesting
    public void addSSTable(SSTableReader sstable)
    {
        assert sstable.getColumnFamilyName().equals(name);
        addSSTables(Collections.singletonList(sstable), OperationType.UNKNOWN);
    }

    @VisibleForTesting
    public void addSSTables(Collection<SSTableReader> sstables)
    {
        addSSTables(sstables, OperationType.UNKNOWN);
    }

    public void addSSTables(Collection<SSTableReader> sstables, OperationType operationType)
    {
        data.addSSTables(sstables, operationType);
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
            return CompactionSSTable.getTotalBytes(sstables);
        }

        // cleanup size estimation only counts bytes for keys local to this node
        long expectedFileSize = 0;
        Collection<Range<Token>> ranges = StorageService.instance.getLocalReplicas(keyspace.getName()).ranges();
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

    public CompactionManager.AllSSTableOpStatus forceCleanup(int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performCleanup(ColumnFamilyStore.this, jobs);
    }

    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs) throws ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, skipCorrupted, reinsertOverflowedTTL, false, checkData, jobs);
    }

    @VisibleForTesting
    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean reinsertOverflowedTTL, boolean alwaysFail, boolean checkData, int jobs) throws ExecutionException, InterruptedException
    {
        // skip snapshot creation during scrub, SEE JIRA 5891
        if(!disableSnapshot)
            snapshotWithoutMemtable("pre-scrub-" + System.currentTimeMillis());

        try
        {
            return CompactionManager.instance.performScrub(ColumnFamilyStore.this, skipCorrupted, checkData, reinsertOverflowedTTL, jobs);
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

    public CompactionManager.AllSSTableOpStatus verify(Verifier.Options options) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performVerify(ColumnFamilyStore.this, options);
    }

    public CompactionManager.AllSSTableOpStatus sstablesRewrite(boolean excludeCurrentVersion, int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performSSTableRewrite(ColumnFamilyStore.this, excludeCurrentVersion, jobs);
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

    void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables, Optional<UUID> operationId)
    {
        data.replaceFlushed(memtable, sstables, operationId);
        if (sstables != null && !sstables.isEmpty())
            CompactionManager.instance.submitBackground(this);
    }

    public boolean isValid()
    {
        return status.isValid();
    }

    /**
     * @return status of the current column family store
     */
    public STATUS status()
    {
        return status;
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
        return data.getLiveSSTables();
    }

    public Iterable<SSTableReader> getSSTables(SSTableSet sstableSet)
    {
        return data.getView().select(sstableSet);
    }

    public Iterable<SSTableReader> getNoncompactingSSTables()
    {
        return data.getNoncompacting();
    }

    public <S extends CompactionSSTable> Iterable<S> getNoncompactingSSTables(Iterable<S> candidates)
    {
        return data.getNoncompacting(candidates);
    }

    public Set<SSTableReader> getCompactingSSTables()
    {
        return data.getCompacting();
    }

    public Iterable<Memtable> getAllMemtables()
    {
        return data.getView().getAllMemtables();
    }

    public Map<UUID, PendingStat> getPendingRepairStats()
    {
        Map<UUID, PendingStat.Builder> builders = new HashMap<>();
        for (SSTableReader sstable : getLiveSSTables())
        {
            UUID session = sstable.getPendingRepair();
            if (session == null)
                continue;

            if (!builders.containsKey(session))
                builders.put(session, new PendingStat.Builder());

            builders.get(session).addSSTable(sstable);
        }

        Map<UUID, PendingStat> stats = new HashMap<>();
        for (Map.Entry<UUID, PendingStat.Builder> entry : builders.entrySet())
        {
            stats.put(entry.getKey(), entry.getValue().build());
        }
        return stats;
    }

    public boolean isFilterFullyCoveredBy(ClusteringIndexFilter filter,
                                          DataLimits limits,
                                          CachedPartition cached,
                                          int nowInSec,
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

    public int gcBefore(int nowInSec)
    {
        return nowInSec - metadata().params.gcGraceSeconds;
    }

    @SuppressWarnings("resource")
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
                failingSince = System.nanoTime();
            }
            else if (System.nanoTime() - failingSince > TimeUnit.MILLISECONDS.toNanos(100))
            {
                List<SSTableReader> released = new ArrayList<>();
                for (SSTableReader reader : view.sstables)
                    if (reader.selfRef().globalCount() == 0)
                        released.add(reader);
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                 "Spinning trying to capture readers {}, released: {}, ", view.sstables, released);
                failingSince = System.nanoTime();
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
        ByteBuffer keyBuffer = hexFormat ? ByteBufferUtil.hexToBytes(key) : metadata().partitionKeyType.fromString(key);
        DecoratedKey dk = decorateKey(keyBuffer);
        try (OpOrder.Group op = readOrdering.start())
        {
            List<String> files = new ArrayList<>();
            for (SSTableReader sstr : select(View.select(SSTableSet.LIVE, dk)).sstables)
            {
                // check if the key actually exists in this sstable, without updating cache and stats
                if (sstr.checkEntryExists(dk, SSTableReader.Operator.EQ, false))
                    files.add(sstr.getFilename());
            }
            return files;
        }
    }

    public void beginLocalSampling(String sampler, int capacity, int durationMillis)
    {
        metric.samplers.get(SamplerType.valueOf(sampler)).beginSampling(capacity, durationMillis);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
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
                    keyspace.getName() + "." + name,
                    counter.count,
                    counter.error,
                    samplerImpl.toString(counter.value) })); // string
        }
        return result;
    }

    public boolean isCompactionDiskSpaceCheckEnabled()
    {
        return compactionSpaceCheck;
    }

    public void compactionDiskSpaceCheck(boolean enable)
    {
        compactionSpaceCheck = enable;
    }

    public void cleanupCache()
    {
        Collection<Range<Token>> ranges = StorageService.instance.getLocalReplicas(keyspace.getName()).ranges();

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

    public void snapshotWithoutMemtable(String snapshotName)
    {
        snapshotWithoutMemtable(snapshotName, null, false, null);
    }

    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned during next startup
     */
    public Set<SSTableReader> snapshotWithoutMemtable(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, RateLimiter rateLimiter)
    {
        if (rateLimiter == null)
            rateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();

        Set<SSTableReader> snapshottedSSTables = new HashSet<>();
        final JSONArray filesJSONArr = new JSONArray();
        for (ColumnFamilyStore cfs : concatWithIndexes())
        {
            try (RefViewFragment currentView = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.path(), rateLimiter); // hard links
                    filesJSONArr.add(ssTable.descriptor.relativeFilenameFor(Component.DATA));

                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", keyspace, ssTable.getFilename(), snapshotDirectory);
                    snapshottedSSTables.add(ssTable);
                }
            }
        }

        writeSnapshotManifest(filesJSONArr, snapshotName);
        if (!SchemaConstants.isLocalSystemKeyspace(metadata.keyspace) && !SchemaConstants.isReplicatedSystemKeyspace(metadata.keyspace))
            writeSnapshotSchema(snapshotName);

        if (ephemeral)
            createEphemeralSnapshotMarkerFile(snapshotName);
        return snapshottedSSTables;
    }

    private void writeSnapshotManifest(final JSONArray filesJSONArr, final String snapshotName)
    {
        final File manifestFile = getDirectories().getSnapshotManifestFile(snapshotName);

        try
        {
            if (!manifestFile.parent().exists())
                manifestFile.parent().tryCreateDirectories();

            try (PrintStream out = new PrintStream(manifestFile.toJavaIOFile()))
            {
                final JSONObject manifestJSON = new JSONObject();
                manifestJSON.put("files", filesJSONArr);
                out.println(manifestJSON.toJSONString());
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, manifestFile);
        }
    }

    private void writeSnapshotSchema(final String snapshotName)
    {
        final File schemaFile = getDirectories().getSnapshotSchemaFile(snapshotName);

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

    private void createEphemeralSnapshotMarkerFile(final String snapshot)
    {
        final File ephemeralSnapshotMarker = getDirectories().getNewEphemeralSnapshotMarkerFile(snapshot);

        try
        {
            if (!ephemeralSnapshotMarker.parent().exists())
                ephemeralSnapshotMarker.parent().tryCreateDirectories();

            Files.createFile(ephemeralSnapshotMarker.toPath());
            if (logger.isTraceEnabled())
                logger.trace("Created ephemeral snapshot marker file on {}.", ephemeralSnapshotMarker.absolutePath());
        }
        catch (IOException e)
        {
            logger.warn(String.format("Could not create marker file %s for ephemeral snapshot %s. " +
                                      "In case there is a failure in the operation that created " +
                                      "this snapshot, you may need to clean it manually afterwards.",
                                      ephemeralSnapshotMarker.absolutePath(), snapshot), e);
        }
    }

    protected static void clearEphemeralSnapshots(Directories directories)
    {
        RateLimiter clearSnapshotRateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();

        for (String ephemeralSnapshot : directories.listEphemeralSnapshots())
        {
            logger.trace("Clearing ephemeral snapshot {} leftover from previous session.", ephemeralSnapshot);
            Directories.clearSnapshot(ephemeralSnapshot, directories.getCFDirectories(), clearSnapshotRateLimiter);
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
                    sstable = entries.getKey().getFormat().getReaderFactory().open(entries.getKey(), entries.getValue(), metadata, true, true);
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
    public Set<SSTableReader> snapshot(String snapshotName)
    {
        return snapshot(snapshotName, false, null);
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     * @param skipMemtable Skip flushing the memtable
     * @param rateLimiter Rate limiter for hardlinks-per-second
     */
    public Set<SSTableReader> snapshot(String snapshotName, boolean skipMemtable, RateLimiter rateLimiter)
    {
        return snapshot(snapshotName, null, false, skipMemtable, rateLimiter);
    }


    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     * @param skipMemtable Skip flushing the memtable
     */
    public Set<SSTableReader> snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, boolean skipMemtable)
    {
        return snapshot(snapshotName, predicate, ephemeral, skipMemtable, null);
    }

    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     * @param skipMemtable Skip flushing the memtable
     * @param rateLimiter Rate limiter for hardlinks-per-second
     */
    public Set<SSTableReader> snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, boolean skipMemtable, RateLimiter rateLimiter)
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
        return snapshotWithoutMemtable(snapshotName, predicate, ephemeral, rateLimiter);
    }

    public boolean snapshotExists(String snapshotName)
    {
        return getDirectories().snapshotExists(snapshotName);
    }

    public long getSnapshotCreationTime(String snapshotName)
    {
        return getDirectories().snapshotCreationTime(snapshotName);
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
    public Map<String, Directories.SnapshotSizeDetails> getSnapshotDetails()
    {
        return getDirectories().getSnapshotDetails();
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

    public void forceCompactionForTokenRange(Collection<Range<Token>> tokenRanges) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.forceCompactionForTokenRange(this, tokenRanges);
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

    public void writeAndAddMemtableRanges(UUID repairSessionID,
                                          Supplier<Collection<Range<PartitionPosition>>> rangesSupplier,
                                          Refs<SSTableReader> placeIntoRefs)
    {
        @SuppressWarnings("resource") // closed by finish or on exception
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
                Throwables.propagate(t);
            }
        }
    }

    private SSTableMultiWriter writeMemtableRanges(Supplier<Collection<Range<PartitionPosition>>> rangesSupplier,
                                                   UUID repairSessionID)
    {
        if (!streamFromMemtable())
            return null;

        Collection<Range<PartitionPosition>> ranges = rangesSupplier.get();
        Memtable current = getTracker().getView().getCurrentMemtable();
        if (current.isClean())
            return null;

        List<Memtable.FlushCollection<?>> dataSets = new ArrayList<>(ranges.size());
        IntervalSet.Builder<CommitLogPosition> commitLogIntervals = new IntervalSet.Builder();
        long keys = 0;
        for (Range<PartitionPosition> range : ranges)
        {
            Memtable.FlushCollection<?> dataSet = current.getFlushSet(range.left, range.right);
            dataSets.add(dataSet);
            commitLogIntervals.add(dataSet.commitLogLowerBound(), dataSet.commitLogUpperBound());
            keys += dataSet.partitionCount();
        }
        if (keys == 0)
            return null;

        // TODO: Can we write directly to stream, skipping disk?
        Memtable.FlushCollection<?> firstDataSet = dataSets.get(0);
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
            for (Memtable.FlushCollection<?> dataSet : dataSets)
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
                cfs.reloadCompactionStrategy(metadata().params.compaction, CompactionStrategyContainer.ReloadReason.FULL);
                return null;
            }, true, false);
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
        logger.info("Truncating {}.{}", keyspace.getName(), name);

        viewManager.stopBuild();

        final long truncatedAt;
        final CommitLogPosition replayAfter;

        // This is a no-op on local storage, but on remote storage where compaction runs offline, this
        // ensures that any live sstables created by the compaction process before it was interrupted,
        // will actually be obsoleted by one of the writers - since all writers must be running for
        // a truncate to run, then at least one writer per token range will load sstables created by compaction
        storageHandler.reloadSSTables(StorageHandler.ReloadReason.TRUNCATION);

        if (!noSnapshot && ((keyspace.getMetadata().params.durableWrites && !memtableWritesAreDurable()) || DatabaseDescriptor.isAutoSnapshot()))
        {
            replayAfter = forceBlockingFlush(FlushReason.TRUNCATE);
            viewManager.forceBlockingFlush(FlushReason.TRUNCATE);
        }
        else
        {
            // just nuke the memtable data w/o writing to disk first
            // note: this does not wait for the switch to complete, but because the post-flush processing is serial,
            // the call below does.
            viewManager.dumpMemtables(FlushReason.TRUNCATE);
            replayAfter = FBUtilities.waitOnFuture(dumpMemtable(FlushReason.TRUNCATE));
        }

        long now = System.currentTimeMillis();
        // make sure none of our sstables are somehow in the future (clock drift, perhaps)
        for (ColumnFamilyStore cfs : concatWithIndexes())
            for (SSTableReader sstable : cfs.getLiveSSTables())
                now = Math.max(now, sstable.maxDataAge);
        truncatedAt = now;

        Runnable truncateRunnable = new Runnable()
        {
            public void run()
            {
                logger.info("Truncating {}.{} with truncatedAt={}", keyspace.getName(), getTableName(), truncatedAt);
                // since truncation can happen at different times on different nodes, we need to make sure
                // that any repairs are aborted, otherwise we might clear the data on one node and then
                // stream in data that is actually supposed to have been deleted
                ActiveRepairService.instance.abort((prs) -> prs.getTableIds().contains(metadata.id),
                                                   "Stopping parent sessions {} due to truncation of tableId="+metadata.id);
                data.notifyTruncated(replayAfter, truncatedAt);
                
                if (!noSnapshot && DatabaseDescriptor.isAutoSnapshot()) 
                    snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(name, SNAPSHOT_TRUNCATE_PREFIX));

                discardSSTables(truncatedAt);

                indexManager.truncateAllIndexesBlocking(truncatedAt);
                viewManager.truncateBlocking(replayAfter, truncatedAt);

                Nodes.local().saveTruncationRecord(ColumnFamilyStore.this, truncatedAt, replayAfter);
                logger.trace("cleaning out row cache");
                invalidateCaches();

            }
        };

        storageHandler.runWithReloadingDisabled(() -> {
            runWithCompactionsDisabled(Executors.callable(truncateRunnable), true, true, AbstractTableOperation.StopTrigger.TRUNCATE);
        });

        viewManager.build();
        logger.info("Truncate of {}.{} is complete", keyspace.getName(), name);
    }

    /**
     * Drops current memtable without flushing to disk. This should only be called when truncating a column family which is not durable.
     */
    public Future<CommitLogPosition> dumpMemtable(FlushReason reason)
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
            FBUtilities.waitOnFuture(dumpMemtable(ColumnFamilyStore.FlushReason.DROP));
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews)
    {
        return runWithCompactionsDisabled(callable, (sstable) -> true, interruptValidation, interruptViews, true);
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews, AbstractTableOperation.StopTrigger trigger)
    {
        return runWithCompactionsDisabled(callable, (sstable) -> true, interruptValidation, interruptViews, true, trigger);
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, Predicate<SSTableReader> sstablesPredicate, boolean interruptValidation, boolean interruptViews, boolean interruptIndexes)
    {
        return runWithCompactionsDisabled(callable, sstablesPredicate, interruptValidation, interruptViews, interruptIndexes, AbstractTableOperation.StopTrigger.NONE);
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
    public <V> V runWithCompactionsDisabled(Callable<V> callable, Predicate<SSTableReader> sstablesPredicate, boolean interruptValidation, boolean interruptViews, boolean interruptIndexes, AbstractTableOperation.StopTrigger trigger)
    {
        // synchronize so that concurrent invocations don't re-enable compactions partway through unexpectedly,
        // and so we only run one major compaction at a time
        synchronized (this)
        {
            logger.trace("Cancelling in-progress compactions for {}", metadata.name);
            Iterable<ColumnFamilyStore> toInterruptFor = concatWith(interruptIndexes, interruptViews);
            try (CompactionManager.CompactionPauser pause = CompactionManager.instance.pauseGlobalCompaction();
                 CompactionManager.CompactionPauser pausedStrategies = pauseCompactionStrategies(toInterruptFor))
            {
                // interrupt in-progress compactions
                CompactionManager.instance.interruptCompactionForCFs(toInterruptFor, sstablesPredicate, interruptValidation, trigger);
                CompactionManager.instance.waitForCessation(toInterruptFor, sstablesPredicate);

                // doublecheck that we finished, instead of timing out
                for (ColumnFamilyStore cfs : toInterruptFor)
                {
                    if (cfs.getTracker().getCompacting().stream().anyMatch(sstablesPredicate))
                    {
                        logger.warn("Unable to cancel in-progress compactions for {}.  Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded.", metadata.name);
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
                cfs.getCompactionStrategy().pause();
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
                cfs.getCompactionStrategy().resume();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    public LifecycleTransaction markAllCompacting(final OperationType operationType)
    {
        Callable<LifecycleTransaction> callable = () -> {
            assert data.getCompacting().isEmpty() : data.getCompacting();
            Iterable<SSTableReader> sstables = Iterables.filter(getLiveSSTables(), sstable -> !sstable.isMarkedSuspect());
            LifecycleTransaction modifier = data.tryModify(sstables, operationType);
            assert modifier != null: "something marked things compacting while compactions are disabled";
            return modifier;
        };

        return runWithCompactionsDisabled(callable, false, false);
    }


    @Override
    public String toString()
    {
        return String.format("%s.%s", getKeyspaceName(), getTableName());
    }

    /**
     * Do not use outside of tests / benchmarks.
     * Ensures no flushing will run while the given runnable is executing.
     */
    @VisibleForTesting
    public void unsafeRunWithoutFlushing(Runnable r)
    {
        synchronized (data)
        {
            r.run();
        }
    }

    public void disableAutoCompaction()
    {
        // we don't use CompactionStrategy.pause since we don't want users flipping that on and off
        // during runWithCompactionsDisabled
        strategyContainer.disable();
    }

    public boolean compactionShouldBeEnabled()
    {
        return strategyContainer.getCompactionParams().isEnabled();
    }

    public void enableAutoCompaction()
    {
        enableAutoCompaction(false);
    }

    /**
     * used for tests - to be able to check things after a minor compaction
     * @param waitForFuture if we should block until autocompaction is done
     */
    @VisibleForTesting
    public void enableAutoCompaction(boolean waitForFuture)
    {
        strategyContainer.enable();
        Future<?> future = CompactionManager.instance.submitBackground(this);
        if (waitForFuture)
            FBUtilities.waitOnFuture(future);
    }

    public boolean isAutoCompactionDisabled()
    {
        return !this.strategyContainer.isEnabled();
    }

    public List<SSTableReader> getCandidatesForUpgrade()
    {
        Set<SSTableReader> compacting = getTracker().getCompacting();
        return getLiveSSTables().stream()
                                .filter(s -> !compacting.contains(s) && !s.descriptor.version.isLatestVersion())
                                .sorted((o1, o2) -> {
                                    File f1 = o1.descriptor.fileFor(Component.DATA);
                                    File f2 = o2.descriptor.fileFor(Component.DATA);
                                    return Longs.compare(f1.lastModified(), f2.lastModified());
                                }).collect(Collectors.toList());
    }

    public SortedLocalRanges getLocalRanges()
    {
        synchronized (this)
        {
            if (localRanges != null && !localRanges.isOutOfDate())
                return localRanges;

            localRanges = SortedLocalRanges.create(this);
            return localRanges;
        }
    }

    /**
     * Return the compaction strategy for this CFS. Even though internally the strategy container
     * implements the strategy, we would like to just expose {@link CompactionStrategy} externally.
     * This is not currently possible for the reasons explained in {@link this#getCompactionStrategyContainer()},
     * so we expose the container as well, but using a separate method, marked as deprecated.
     *
     * @return the compaction strategy for this CFS
     */
    public CompactionStrategy getCompactionStrategy()
    {
        return strategyContainer;
    }

    /**
     * The reasons for exposing the compaction strategy container are the following:
     *
     * - Unit tests
     * - Repair
     *
     * Eventually we would like to only expose the {@link CompactionStrategy}, so for new code call
     * {@link this#getCompactionStrategy()} instead.
     *
     * @return the compaction strategy container
     */
    @Deprecated
    @VisibleForTesting
    public CompactionStrategyContainer getCompactionStrategyContainer()
    {
        return strategyContainer;
    }

    /**
     * This option determines if tombstones should only be removed when the sstable has been repaired.
     * Because this option was introduced in patch releases (I'm guessing), the compaction parameters were
     * abused. Eventually this option should be moved out of the compaction parameters. TODO: move it
     * to the new compaction strategy interface.
     *
     * @return true if tombstones can only be removed if the sstable has been repaired
     */
    public boolean onlyPurgeRepairedTombstones()
    {
        // Here we need to ask the CSM for the parameters in case they were changed over JMX without changing the schema,
        // for now the CSM has the up-to-date copy of the params
        CompactionParams params = strategyContainer.getCompactionParams();
        return Boolean.parseBoolean(params.options().get(CompactionStrategyOptions.ONLY_PURGE_REPAIRED_TOMBSTONES));
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        try
        {
            TableParams.builder().crcCheckChance(crcCheckChance).build().validate();
            for (ColumnFamilyStore cfs : concatWithIndexes())
            {
                cfs.crcCheckChance.set(crcCheckChance);
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    sstable.setCrcCheckChance(crcCheckChance);
            }
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }


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

    public boolean isCompactionActive()
    {
        return getCompactionStrategyContainer().isActive();
    }

    public long getMaxSSTableBytes()
    {
        return getCompactionStrategy().getMaxSSTableBytes();
    }

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

    public BloomFilterTracker getBloomFilterTracker()
    {
        return bloomFilterTracker;
    }

    public long getBloomFilterFalsePositiveCount()
    {
        return bloomFilterTracker.getFalsePositiveCount();
    }

    public long getBloomFilterTruePositiveCount()
    {
        return bloomFilterTracker.getTruePositiveCount();
    }

    public long getBloomFilterTrueNegativeCount()
    {
        return bloomFilterTracker.getTrueNegativeCount();
    }

    public double getRecentBloomFilterFalsePositiveRate()
    {
        return bloomFilterTracker.getRecentFalsePositiveRate();
    }

    public double getRecentBloomFilterTruePositiveRate()
    {
        return bloomFilterTracker.getRecentTruePositiveRate();
    }

    public double getRecentBloomFilterTrueNegativeRate()
    {
        return bloomFilterTracker.getRecentTrueNegativeRate();
    }

    public long getReadRequests()
    {
        return metric == null ? 0 : metric.readRequests.getCount();
    }

    public long getBytesInserted()
    {
        return metric == null ? 0 : metric.bytesInserted.getCount();
    }

    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return metadata().isIndex();
    }

    public Iterable<ColumnFamilyStore> concatWithIndexes()
    {
        return concatWith(true, false);
    }

    public Iterable<ColumnFamilyStore> concatWith(boolean includeIndexes, boolean includeViews)
    {
        // we return the main CFS first, which we rely on for simplicity in switchMemtable(), for getting the
        // latest commit log segment position
        Set<ColumnFamilyStore> mainCFS = Collections.singleton(this);
        if (includeIndexes && includeViews)
            return Iterables.concat(mainCFS,
                                    indexManager.getAllIndexColumnFamilyStores(),
                                    viewManager.allViewsCfs());
        if (includeIndexes)
            return Iterables.concat(mainCFS, indexManager.getAllIndexColumnFamilyStores());
        if (includeViews)
            return Iterables.concat(mainCFS, viewManager.allViewsCfs());
        return mainCFS;
    }

    public List<String> getBuiltIndexes()
    {
        return indexManager.getBuiltIndexNames();
    }

    public int getUnleveledSSTables()
    {
        if (strategyContainer instanceof CompactionStrategyManager)
            return ((CompactionStrategyManager) strategyContainer).getUnleveledSSTables();
        else
            return 0;
    }

    public int[] getSSTableCountPerLevel()
    {
        return strategyContainer.getSSTableCountPerLevel();
    }

    public int getLevelFanoutSize()
    {
        return strategyContainer.getLevelFanoutSize();
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
            markObsolete(truncatedSSTables, OperationType.TRUNCATE_TABLE);
        }
    }

    /**
     * Discard sstables that matches given filter with provided operation type
     */
    public void discardSSTables(Iterable<SSTableReader> sstables, Predicate<SSTableReader> filter, OperationType operationType)
    {
        List<SSTableReader> discarded = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            if (filter.apply(sstable))
                discarded.add(sstable);
        }

        if (!discarded.isEmpty())
            markObsolete(discarded, operationType);
    }

    public double getDroppableTombstoneRatio()
    {
        double allDroppable = 0;
        long allColumns = 0;
        int localTime = (int)(System.currentTimeMillis()/1000);

        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            allDroppable += sstable.getDroppableTombstonesBefore(localTime - metadata().params.gcGraceSeconds);
            allColumns += sstable.getEstimatedCellPerPartitionCount().mean() * sstable.getEstimatedCellPerPartitionCount().count();
        }
        return allColumns > 0 ? allDroppable / allColumns : 0;
    }

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

    public DiskBoundaries getDiskBoundaries()
    {
        return diskBoundaryManager.getDiskBoundaries(this);
    }

    public void invalidateLocalRangesAndDiskBoundaries()
    {
        synchronized (this)
        {
            if (localRanges != null)
                localRanges.invalidate();
        }

        diskBoundaryManager.invalidate();
    }

    @Override
    public void setNeverPurgeTombstones(boolean value)
    {
        if (neverPurgeTombstones != value)
            logger.info("Changing neverPurgeTombstones for {}.{} from {} to {}", keyspace.getName(), getTableName(), neverPurgeTombstones, value);
        else
            logger.info("Not changing neverPurgeTombstones for {}.{}, it is {}", keyspace.getName(), getTableName(), neverPurgeTombstones);

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
        if (logger.isTraceEnabled())
            logger.trace("CFS {} is being dropped: indexes removed", name);

        CompactionManager.instance.interruptCompactionForCFs(concatWithIndexes(), (sstable) -> true, true);
        if (logger.isTraceEnabled())
            logger.trace("CFS {} is being dropped: compactions stopped", name);

        if (DatabaseDescriptor.isAutoSnapshot())
            snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(name, ColumnFamilyStore.SNAPSHOT_DROP_PREFIX));

        if (getTracker().isDummy())
        {
            // offline services (e.g. standalone compactor) don't have Memtables or CommitLog. An attempt to flush would
            // throw an exception
            logger.debug("Memtables and CommitLog are disabled; not recycling or flushing {}", metadata);
        }
        else
        {
            if (logger.isTraceEnabled())
                logger.trace("Recycling CL segments for dropping {}", metadata);
            CommitLog.instance.forceRecycleAllSegments(Collections.singleton(metadata.id));
        }

        if (logger.isTraceEnabled())
            logger.trace("Dropping CFS {}: shutting down compaction strategy", name);
        strategyContainer.shutdown();

        // wait for any outstanding reads/writes that might affect the CFS
        if (logger.isTraceEnabled())
            logger.trace("Dropping CFS {}: waiting for read and write barriers", name);
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
        private final ExecutorService[] nonLocalSystemflushExecutors;

        /**
         * The flush executors for the local system keyspaces.
         */
        private final ExecutorService[] localSystemDiskFlushExecutors;

        /**
         * {@code true} if local system keyspaces are stored in their own directory and use an extra flush executor,
         * {@code false} otherwise.
         */
        private final boolean useSpecificExecutorForSystemKeyspaces;

        public PerDiskFlushExecutors(int flushWriters,
                                     File[] locationsForNonSystemKeyspaces,
                                     boolean useSpecificLocationForSystemKeyspaces)
        {
            ExecutorService[] flushExecutors = createPerDiskFlushWriters(locationsForNonSystemKeyspaces.length, flushWriters);
            nonLocalSystemflushExecutors = flushExecutors;
            useSpecificExecutorForSystemKeyspaces = useSpecificLocationForSystemKeyspaces;
            localSystemDiskFlushExecutors = useSpecificLocationForSystemKeyspaces ? new ExecutorService[] {newThreadPool("LocalSystemKeyspacesDiskMemtableFlushWriter", flushWriters)}
                                                                                  : new ExecutorService[] {flushExecutors[0]};
        }

        private static ExecutorService[] createPerDiskFlushWriters(int numberOfExecutors, int flushWriters)
        {
            ExecutorService[] flushExecutors = new ExecutorService[numberOfExecutors];

            for (int i = 0; i < numberOfExecutors; i++)
            {
                flushExecutors[i] = newThreadPool("PerDiskMemtableFlushWriter_" + i, flushWriters);
            }
            return flushExecutors;
        }

        private static JMXEnabledThreadPoolExecutor newThreadPool(String poolName, int size)
        {
            return new JMXEnabledThreadPoolExecutor(size,
                                                    Stage.KEEP_ALIVE_SECONDS,
                                                    TimeUnit.SECONDS,
                                                    new LinkedBlockingQueue<>(),
                                                    new NamedThreadFactory(poolName),
                                                    "internal");
        }

        /**
         * Returns the flush executors for the specified keyspace.
         *
         * @param keyspaceName the keyspace name
         * @param tableName the table name
         * @return the flush executors that should be used for flushing the memtables of the specified keyspace.
         */
        public ExecutorService[] getExecutorsFor(String keyspaceName, String tableName)
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

    private static void verifyMetadata(SSTableReader sstable, long repairedAt, UUID pendingRepair, boolean isTransient)
    {
        if (!Objects.equals(pendingRepair, sstable.getPendingRepair()))
            throw new IllegalStateException(String.format("Failed setting pending repair to %s on %s (pending repair is %s)", pendingRepair, sstable, sstable.getPendingRepair()));
        if (repairedAt != sstable.getRepairedAt())
            throw new IllegalStateException(String.format("Failed setting repairedAt to %d on %s (repairedAt is %d)", repairedAt, sstable, sstable.getRepairedAt()));
        if (isTransient != sstable.isTransient())
            throw new IllegalStateException(String.format("Failed setting isTransient to %b on %s (isTransient is %b)", isTransient, sstable, sstable.isTransient()));
    }

    /**
     * This method is exposed for testing only
     * NotThreadSafe
     */
    @VisibleForTesting
    public int mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException
    {
        Set<SSTableReader> changed = new HashSet<>();
        try
        {
            for (SSTableReader sstable: sstables)
            {
                sstable.mutateRepairedAndReload(repairedAt, pendingRepair, isTransient);
                verifyMetadata(sstable, repairedAt, pendingRepair, isTransient);
                changed.add(sstable);
            }
        }
        finally
        {
            // if there was an exception mutating repairedAt, we should still notify for the
            // sstables that we were able to modify successfully before releasing the lock
            getTracker().notifySSTableRepairedStatusChanged(changed);
        }
        return changed.size();
    }

    /**
     * Mutates sstable repairedAt times and notifies listeners of the change with the writeLock held. Prevents races
     * with other processes between when the metadata is changed and when sstables are moved between strategies.
     */
    public int mutateRepaired(@Nullable final ReentrantReadWriteLock.WriteLock writeLock,
                              Collection<SSTableReader> sstables,
                              long repairedAt,
                              UUID pendingRepair,
                              boolean isTransient) throws IOException
    {
        if (writeLock == null)
            return mutateRepaired(sstables, repairedAt, pendingRepair, isTransient);

        writeLock.lock();
        try
        {
            return mutateRepaired(sstables, repairedAt, pendingRepair, isTransient);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public int mutateRepairedWithLock(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException
    {
        return mutateRepaired(getCompactionStrategyContainer().getWriteLock(), sstables, repairedAt, pendingRepair, isTransient);
    }

    public void repairSessionCompleted(UUID sessionID)
    {
        getCompactionStrategyContainer().repairSessionCompleted(sessionID);
    }

    public boolean hasPendingRepairSSTables(UUID sessionID)
    {
        return Iterables.any(data.getLiveSSTables(), pendingRepairPredicate(sessionID));
    }

    public Set<SSTableReader> getPendingRepairSSTables(UUID sessionID)
    {
        return Sets.filter(data.getLiveSSTables(), pendingRepairPredicate(sessionID));
    }

    public static Predicate<SSTableReader> pendingRepairPredicate(@Nonnull UUID sessionID)
    {
        return sstable -> sstable.getPendingRepair() != null && sessionID.equals(sstable.getPendingRepair());
    }

    @Override
    public LifecycleTransaction tryModify(Iterable<? extends CompactionSSTable> ssTableReaders,
                                          OperationType operationType,
                                          UUID id)
    {
        return data.tryModify(Iterables.transform(ssTableReaders, SSTableReader.class::cast), operationType, id);
    }

    public CompactionRealm.OverlapTracker getOverlapTracker(Iterable<SSTableReader> sources)
    {
        return new OverlapTracker(sources);
    }

    class OverlapTracker implements CompactionRealm.OverlapTracker
    {
        final Iterable<SSTableReader> compacting;
        private Refs<SSTableReader> overlappingSSTables;
        private OverlapIterator<PartitionPosition, SSTableReader> overlapIterator;

        OverlapTracker(Iterable<SSTableReader> compacting)
        {
            this.compacting = compacting;
            collectOverlaps();
        }

        public Collection<? extends CompactionSSTable> overlaps()
        {
            return overlappingSSTables;
        }

        public Collection<? extends CompactionSSTable> overlaps(DecoratedKey key)
        {
            overlapIterator.update(key);
            return overlapIterator.overlaps();
        }

        public <V> Iterable<V> openSelectedOverlappingSSTables(DecoratedKey key,
                                                               Predicate<CompactionSSTable> filter,
                                                               Function<SSTableReader, V> transformation)
        {
            overlapIterator.update(key);

            Iterable<SSTableReader> overlaps = overlapIterator.overlaps();
            Iterable<V> transformed = Iterables.transform(overlaps, sstable -> filter.apply(sstable)
                                                                               ? transformation.apply(sstable)
                                                                               : null);
            return Iterables.filter(transformed, Predicates.notNull());
        }

        public void close()
        {
            overlapIterator = null;
            overlappingSSTables.release();
        }

        public boolean maybeRefresh()
        {
            for (CompactionSSTable reader : overlappingSSTables)
            {
                if (reader.isMarkedCompacted())
                {
                    close();
                    collectOverlaps();
                    return true;
                }
            }
            return false;
        }

        private void collectOverlaps()
        {
            if (compacting == null)
                overlappingSSTables = Refs.tryRef(Collections.<SSTableReader>emptyList());
            else
                overlappingSSTables = getAndReferenceOverlappingLiveSSTables(compacting);
            this.overlapIterator = new OverlapIterator<>(SSTableIntervalTree.buildIntervals(overlappingSSTables));

            if (logger.isTraceEnabled())
                logger.trace("Refreshed overlaps: {}", overlappingSSTables);
        }
    }
}
