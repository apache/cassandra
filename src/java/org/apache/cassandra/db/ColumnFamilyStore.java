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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.management.*;
import javax.management.openmbean.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.*;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.Counter;
import org.apache.cassandra.cache.*;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.db.lifecycle.*;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TableMetrics.Sampler;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.TopKSampler.SamplerResult;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.cassandra.utils.Throwables.maybeFail;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    // The directories which will be searched for sstables on cfs instantiation.
    private static volatile Directories.DataDirectory[] initialDirectories = Directories.dataDirectories;

    /**
     * A hook to add additional directories to initialDirectories.
     * Any additional directories should be added prior to ColumnFamilyStore instantiation on startup
     *
     * Since the directories used by a given table are determined by the compaction strategy,
     * it's possible for sstables to be written to directories specified outside of cassandra.yaml.
     * By adding additional directories to initialDirectories, sstables in these extra locations are
     * made discoverable on sstable instantiation.
     */
    public static synchronized void addInitialDirectories(Directories.DataDirectory[] newDirectories)
    {
        assert newDirectories != null;

        Set<Directories.DataDirectory> existing = Sets.newHashSet(initialDirectories);

        List<Directories.DataDirectory> replacementList = Lists.newArrayList(initialDirectories);
        for (Directories.DataDirectory directory: newDirectories)
        {
            if (!existing.contains(directory))
            {
                replacementList.add(directory);
            }
        }

        Directories.DataDirectory[] replacementArray = new Directories.DataDirectory[replacementList.size()];
        replacementList.toArray(replacementArray);
        initialDirectories = replacementArray;
    }

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private static final ExecutorService flushExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                          StageManager.KEEPALIVE,
                                                                                          TimeUnit.SECONDS,
                                                                                          new LinkedBlockingQueue<Runnable>(),
                                                                                          new NamedThreadFactory("MemtableFlushWriter"),
                                                                                          "internal");

    private static final ExecutorService [] perDiskflushExecutors = new ExecutorService[DatabaseDescriptor.getAllDataFileLocations().length];
    static
    {
        for (int i = 0; i < DatabaseDescriptor.getAllDataFileLocations().length; i++)
        {
            perDiskflushExecutors[i] = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                                                        StageManager.KEEPALIVE,
                                                                        TimeUnit.SECONDS,
                                                                        new LinkedBlockingQueue<Runnable>(),
                                                                        new NamedThreadFactory("PerDiskMemtableFlushWriter_"+i),
                                                                        "internal");
        }
    }

    // post-flush executor is single threaded to provide guarantee that any flush Future on a CF will never return until prior flushes have completed
    private static final ExecutorService postFlushExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                              StageManager.KEEPALIVE,
                                                                                              TimeUnit.SECONDS,
                                                                                              new LinkedBlockingQueue<Runnable>(),
                                                                                              new NamedThreadFactory("MemtablePostFlush"),
                                                                                              "internal");

    private static final ExecutorService reclaimExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                            StageManager.KEEPALIVE,
                                                                                            TimeUnit.SECONDS,
                                                                                            new LinkedBlockingQueue<Runnable>(),
                                                                                            new NamedThreadFactory("MemtableReclaimMemory"),
                                                                                            "internal");

    private static final String[] COUNTER_NAMES = new String[]{"raw", "count", "error", "string"};
    private static final String[] COUNTER_DESCS = new String[]
    { "partition key in raw hex bytes",
      "value of this partition for given sampler",
      "value is within the error bounds plus or minus of this",
      "the partition key turned into a human readable format" };
    private static final CompositeType COUNTER_COMPOSITE_TYPE;
    private static final TabularType COUNTER_TYPE;

    private static final String[] SAMPLER_NAMES = new String[]{"cardinality", "partitions"};
    private static final String[] SAMPLER_DESCS = new String[]
    { "cardinality of partitions",
      "list of counter results" };

    private static final String SAMPLING_RESULTS_NAME = "SAMPLING_RESULTS";
    private static final CompositeType SAMPLING_RESULT;

    static
    {
        try
        {
            OpenType<?>[] counterTypes = new OpenType[] { SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.STRING };
            COUNTER_COMPOSITE_TYPE = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_NAMES, COUNTER_DESCS, counterTypes);
            COUNTER_TYPE = new TabularType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_COMPOSITE_TYPE, COUNTER_NAMES);

            OpenType<?>[] samplerTypes = new OpenType[] { SimpleType.LONG, COUNTER_TYPE };
            SAMPLING_RESULT = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, SAMPLER_NAMES, SAMPLER_DESCS, samplerTypes);
        } catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public final Keyspace keyspace;
    public final String name;
    public final CFMetaData metadata;
    private final String mbeanName;
    @Deprecated
    private final String oldMBeanName;
    private volatile boolean valid = true;

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
    private final AtomicInteger fileIndexGenerator = new AtomicInteger(0);

    public final SecondaryIndexManager indexManager;
    public final ViewManager.ForStore viewManager;

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultValue<Integer> minCompactionThreshold;
    private volatile DefaultValue<Integer> maxCompactionThreshold;
    private volatile DefaultValue<Double> crcCheckChance;

    private final CompactionStrategyManager compactionStrategyManager;

    private volatile Directories directories;

    public final TableMetrics metric;
    public volatile long sampleLatencyNanos;
    private final ScheduledFuture<?> latencyCalculator;

    public static void shutdownPostFlushExecutor() throws InterruptedException
    {
        postFlushExecutor.shutdown();
        postFlushExecutor.awaitTermination(60, TimeUnit.SECONDS);
    }

    public void reload()
    {
        // metadata object has been mutated directly. make all the members jibe with new settings.

        // only update these runtime-modifiable settings if they have not been modified.
        if (!minCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.minCompactionThreshold = new DefaultValue(metadata.params.compaction.minCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultValue(metadata.params.compaction.maxCompactionThreshold());
        if (!crcCheckChance.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.crcCheckChance = new DefaultValue(metadata.params.crcCheckChance);

        compactionStrategyManager.maybeReload(metadata);
        directories = compactionStrategyManager.getDirectories();

        scheduleFlush();

        indexManager.reload();

        // If the CF comparator has changed, we need to change the memtable,
        // because the old one still aliases the previous comparator.
        if (data.getView().getCurrentMemtable().initialComparator != metadata.comparator)
            switchMemtable();
    }

    void scheduleFlush()
    {
        int period = metadata.params.memtableFlushPeriodInMs;
        if (period > 0)
        {
            logger.trace("scheduling flush in {} ms", period);
            WrappedRunnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws Exception
                {
                    synchronized (data)
                    {
                        Memtable current = data.getView().getCurrentMemtable();
                        // if we're not expired, we've been hit by a scheduled flush for an already flushed memtable, so ignore
                        if (current.isExpired())
                        {
                            if (current.isClean())
                            {
                                // if we're still clean, instead of swapping just reschedule a flush for later
                                scheduleFlush();
                            }
                            else
                            {
                                // we'll be rescheduled by the constructor of the Memtable.
                                forceFlush();
                            }
                        }
                    }
                }
            };
            ScheduledExecutors.scheduledTasks.schedule(runnable, period, TimeUnit.MILLISECONDS);
        }
    }

    public static Runnable getBackgroundCompactionTaskSubmitter()
    {
        return new Runnable()
        {
            public void run()
            {
                for (Keyspace keyspace : Keyspace.all())
                    for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                        CompactionManager.instance.submitBackground(cfs);
            }
        };
    }

    public void setCompactionParametersJson(String options)
    {
        setCompactionParameters(FBUtilities.fromJsonMap(options));
    }

    public String getCompactionParametersJson()
    {
        return FBUtilities.json(getCompactionParameters());
    }

    public void setCompactionParameters(Map<String, String> options)
    {
        try
        {
            CompactionParams compactionParams = CompactionParams.fromMap(options);
            compactionParams.validate();
            compactionStrategyManager.setNewLocalCompactionStrategy(compactionParams);
        }
        catch (Throwable t)
        {
            logger.error("Could not set new local compaction strategy", t);
            // dont propagate the ConfigurationException over jmx, user will only see a ClassNotFoundException
            throw new IllegalArgumentException("Could not set new local compaction strategy: "+t.getMessage());
        }
    }

    public Map<String, String> getCompactionParameters()
    {
        return compactionStrategyManager.getCompactionParams().asMap();
    }

    public Map<String,String> getCompressionParameters()
    {
        return metadata.params.compression.asMap();
    }

    public void setCompressionParameters(Map<String,String> opts)
    {
        try
        {
            metadata.compression(CompressionParams.fromMap(opts));
            metadata.params.compression.validate();
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private ColumnFamilyStore(Keyspace keyspace,
                             String columnFamilyName,
                             int generation,
                             CFMetaData metadata,
                             Directories directories,
                             boolean loadSSTables)
    {
        this(keyspace, columnFamilyName, generation, metadata, directories, loadSSTables, true);
    }


    @VisibleForTesting
    public ColumnFamilyStore(Keyspace keyspace,
                              String columnFamilyName,
                              int generation,
                              CFMetaData metadata,
                              Directories directories,
                              boolean loadSSTables,
                              boolean registerBookkeeping)
    {
        assert directories != null;
        assert metadata != null : "null metadata for " + keyspace + ":" + columnFamilyName;

        this.keyspace = keyspace;
        this.metadata = metadata;
        name = columnFamilyName;
        minCompactionThreshold = new DefaultValue<>(metadata.params.compaction.minCompactionThreshold());
        maxCompactionThreshold = new DefaultValue<>(metadata.params.compaction.maxCompactionThreshold());
        crcCheckChance = new DefaultValue<>(metadata.params.crcCheckChance);
        indexManager = new SecondaryIndexManager(this);
        viewManager = keyspace.viewManager.forTable(metadata.cfId);
        metric = new TableMetrics(this);
        fileIndexGenerator.set(generation);
        sampleLatencyNanos = DatabaseDescriptor.getReadRpcTimeout() / 2;

        logger.info("Initializing {}.{}", keyspace.getName(), name);

        // scan for sstables corresponding to this cf and load them
        data = new Tracker(this, loadSSTables);

        if (data.loadsstables)
        {
            Directories.SSTableLister sstableFiles = directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
            Collection<SSTableReader> sstables = SSTableReader.openAll(sstableFiles.list().entrySet(), metadata);
            data.addInitialSSTables(sstables);
        }

        // compaction strategy should be created after the CFS has been prepared
        compactionStrategyManager = new CompactionStrategyManager(this);
        this.directories = compactionStrategyManager.getDirectories();

        if (maxCompactionThreshold.value() <= 0 || minCompactionThreshold.value() <=0)
        {
            logger.warn("Disabling compaction strategy by setting compaction thresholds to 0 is deprecated, set the compaction option 'enabled' to 'false' instead.");
            this.compactionStrategyManager.disable();
        }

        // create the private ColumnFamilyStores for the secondary column indexes
        for (IndexMetadata info : metadata.getIndexes())
            indexManager.addIndex(info);

        if (registerBookkeeping)
        {
            // register the mbean
            mbeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,table=%s",
                                         isIndex() ? "IndexTables" : "Tables",
                                         keyspace.getName(), name);
            oldMBeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,columnfamily=%s",
                                         isIndex() ? "IndexColumnFamilies" : "ColumnFamilies",
                                         keyspace.getName(), name);
            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                ObjectName[] objectNames = {new ObjectName(mbeanName), new ObjectName(oldMBeanName)};
                for (ObjectName objectName : objectNames)
                {
                    mbs.registerMBean(this, objectName);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            logger.trace("retryPolicy for {} is {}", name, this.metadata.params.speculativeRetry);
            latencyCalculator = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(new Runnable()
            {
                public void run()
                {
                    SpeculativeRetryParam retryPolicy = ColumnFamilyStore.this.metadata.params.speculativeRetry;
                    switch (retryPolicy.kind())
                    {
                        case PERCENTILE:
                            // get percentile in nanos
                            sampleLatencyNanos = (long) (metric.coordinatorReadLatency.getSnapshot().getValue(retryPolicy.threshold()) * 1000d);
                            break;
                        case CUSTOM:
                            sampleLatencyNanos = (long) retryPolicy.threshold();
                            break;
                        default:
                            sampleLatencyNanos = Long.MAX_VALUE;
                            break;
                    }
                }
            }, DatabaseDescriptor.getReadRpcTimeout(), DatabaseDescriptor.getReadRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        else
        {
            latencyCalculator = ScheduledExecutors.optionalTasks.schedule(Runnables.doNothing(), 0, TimeUnit.NANOSECONDS);
            mbeanName = null;
            oldMBeanName= null;
        }
    }

    public Directories getDirectories()
    {
        // todo, hack since we need to know the data directories when constructing the compaction strategy
        if (directories != null)
            return directories;
        return new Directories(metadata, initialDirectories);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, int sstableLevel, SerializationHeader header, LifecycleTransaction txn)
    {
        MetadataCollector collector = new MetadataCollector(metadata.comparator).sstableLevel(sstableLevel);
        return createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, txn);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, MetadataCollector metadataCollector, SerializationHeader header, LifecycleTransaction txn)
    {
        return getCompactionStrategyManager().createSSTableMultiWriter(descriptor, keyCount, repairedAt, metadataCollector, header, indexManager.listIndexes(), txn);
    }

    /** call when dropping or renaming a CF. Performs mbean housekeeping and invalidates CFS to other operations */
    public void invalidate()
    {
        invalidate(true);
    }

    public void invalidate(boolean expectMBean)
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

        latencyCalculator.cancel(false);
        compactionStrategyManager.shutdown();
        SystemKeyspace.removeTruncationRecord(metadata.cfId);

        data.dropSSTables();
        LifecycleTransaction.waitForDeletions();
        indexManager.invalidateAllIndexesBlocking();

        invalidateCaches();
    }

    /**
     * Removes every SSTable in the directory from the Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void maybeRemoveUnreadableSSTables(File directory)
    {
        data.removeUnreadableSSTables(directory);
    }

    void unregisterMBean() throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName[] objectNames = {new ObjectName(mbeanName), new ObjectName(oldMBeanName)};
        for (ObjectName objectName : objectNames)
        {
            if (mbs.isRegistered(objectName))
                mbs.unregisterMBean(objectName);
        }

        // unregister metrics
        metric.release();
    }


    public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, CFMetaData metadata, boolean loadSSTables)
    {
        return createColumnFamilyStore(keyspace, metadata.cfName, metadata, loadSSTables);
    }

    public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                         String columnFamily,
                                                                         CFMetaData metadata,
                                                                         boolean loadSSTables)
    {
        // get the max generation number, to prevent generation conflicts
        Directories directories = new Directories(metadata, initialDirectories);
        Directories.SSTableLister lister = directories.sstableLister(Directories.OnTxnErr.IGNORE).includeBackups(true);
        List<Integer> generations = new ArrayList<Integer>();
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor desc = entry.getKey();
            generations.add(desc.generation);
            if (!desc.isCompatible())
                throw new RuntimeException(String.format("Incompatible SSTable found. Current version %s is unable to read file: %s. Please run upgradesstables.",
                        desc.getFormat().getLatestVersion(), desc));
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;

        return new ColumnFamilyStore(keyspace, columnFamily, value, metadata, directories, loadSSTables);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public static void scrubDataDirectories(CFMetaData metadata) throws StartupException
    {
        Directories directories = new Directories(metadata);

         // clear ephemeral snapshots that were not properly cleared last session (CASSANDRA-7357)
        clearEphemeralSnapshots(directories);

        directories.removeTemporaryDirectories();

        logger.trace("Removing temporary or obsoleted files from unfinished operations for table {}", metadata.cfName);
        if (!LifecycleTransaction.removeUnfinishedLeftovers(metadata))
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Cannot remove temporary or obsoleted files for %s.%s due to a problem with transaction " +
                                                     "log files. Please check records with problems in the log messages above and fix them. " +
                                                     "Refer to the 3.0 upgrading instructions in NEWS.txt " +
                                                     "for a description of transaction log files.", metadata.ksName, metadata.cfName));

        logger.trace("Further extra check for orphan sstable files for {}", metadata.cfName);
        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : directories.sstableLister(Directories.OnTxnErr.IGNORE).list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            Set<Component> components = sstableFiles.getValue();

            for (File tmpFile : desc.getTemporaryFiles())
                tmpFile.delete();

            File dataFile = new File(desc.filenameFor(Component.DATA));
            if (components.contains(Component.DATA) && dataFile.length() > 0)
                // everything appears to be in order... moving on.
                continue;

            // missing the DATA file! all components are orphaned
            logger.warn("Removing orphans for {}: {}", desc, components);
            for (Component component : components)
            {
                File file = new File(desc.filenameFor(component));
                if (file.exists())
                    FileUtils.deleteWithConfirm(desc.filenameFor(component));
            }
        }

        // cleanup incomplete saved caches
        Pattern tmpCacheFilePattern = Pattern.compile(metadata.ksName + "-" + metadata.cfName + "-(Key|Row)Cache.*\\.tmp$");
        File dir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (dir.exists())
        {
            assert dir.isDirectory();
            for (File file : dir.listFiles())
                if (tmpCacheFilePattern.matcher(file.getName()).matches())
                    if (!file.delete())
                        logger.warn("could not delete {}", file.getAbsolutePath());
        }

        // also clean out any index leftovers.
        for (IndexMetadata index : metadata.getIndexes())
            if (!index.isCustom())
            {
                CFMetaData indexMetadata = CassandraIndex.indexCfsMetadata(metadata, index);
                scrubDataDirectories(indexMetadata);
            }
    }

    /**
     * See #{@code StorageService.loadNewSSTables(String, String)} for more info
     *
     * @param ksName The keyspace name
     * @param cfName The columnFamily name
     */
    public static synchronized void loadNewSSTables(String ksName, String cfName)
    {
        /** ks/cf existence checks will be done by open and getCFS methods for us */
        Keyspace keyspace = Keyspace.open(ksName);
        keyspace.getColumnFamilyStore(cfName).loadNewSSTables();
    }

    /**
     * #{@inheritDoc}
     */
    public synchronized void loadNewSSTables()
    {
        logger.info("Loading new SSTables for {}/{}...", keyspace.getName(), name);

        Set<Descriptor> currentDescriptors = new HashSet<>();
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            currentDescriptors.add(sstable.descriptor);
        Set<SSTableReader> newSSTables = new HashSet<>();

        Directories.SSTableLister lister = getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor descriptor = entry.getKey();

            if (currentDescriptors.contains(descriptor))
                continue; // old (initialized) SSTable found, skipping

            if (!descriptor.isCompatible())
                throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                        descriptor.getFormat().getLatestVersion(),
                        descriptor));

            // force foreign sstables to level 0
            try
            {
                if (new File(descriptor.filenameFor(Component.STATS)).exists())
                    descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
            }
            catch (IOException e)
            {
                SSTableReader.logOpenException(entry.getKey(), e);
                continue;
            }

            // Increment the generation until we find a filename that doesn't exist. This is needed because the new
            // SSTables that are being loaded might already use these generation numbers.
            Descriptor newDescriptor;
            do
            {
                newDescriptor = new Descriptor(descriptor.version,
                                               descriptor.directory,
                                               descriptor.ksname,
                                               descriptor.cfname,
                                               fileIndexGenerator.incrementAndGet(),
                                               descriptor.formatType,
                                               descriptor.digestComponent);
            }
            while (new File(newDescriptor.filenameFor(Component.DATA)).exists());

            logger.info("Renaming new SSTable {} to {}", descriptor, newDescriptor);
            SSTableWriter.rename(descriptor, newDescriptor, entry.getValue());

            SSTableReader reader;
            try
            {
                reader = SSTableReader.open(newDescriptor, entry.getValue(), metadata);
            }
            catch (IOException e)
            {
                SSTableReader.logOpenException(entry.getKey(), e);
                continue;
            }
            newSSTables.add(reader);
        }

        if (newSSTables.isEmpty())
        {
            logger.info("No new SSTables were found for {}/{}", keyspace.getName(), name);
            return;
        }

        logger.info("Loading new SSTables and building secondary indexes for {}/{}: {}", keyspace.getName(), name, newSSTables);

        try (Refs<SSTableReader> refs = Refs.ref(newSSTables))
        {
            data.addSSTables(newSSTables);
            indexManager.buildAllIndexesBlocking(newSSTables);
        }

        logger.info("Done loading load new SSTables for {}/{}", keyspace.getName(), name);
    }

    public void rebuildSecondaryIndex(String idxName)
    {
        rebuildSecondaryIndex(keyspace.getName(), metadata.cfName, idxName);
    }

    public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);

        Set<String> indexes = new HashSet<String>(Arrays.asList(idxNames));

        Iterable<SSTableReader> sstables = cfs.getSSTables(SSTableSet.CANONICAL);
        try (Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            logger.info("User Requested secondary index re-build for {}/{} indexes: {}", ksName, cfName, Joiner.on(',').join(idxNames));
            cfs.indexManager.rebuildIndexesBlocking(refs, indexes);
        }
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

    public String getSSTablePath(File directory)
    {
        return getSSTablePath(directory, DatabaseDescriptor.getSSTableFormat().info.getLatestVersion(), DatabaseDescriptor.getSSTableFormat());
    }

    public String getSSTablePath(File directory, SSTableFormat.Type format)
    {
        return getSSTablePath(directory, format.info.getLatestVersion(), format);
    }

    private String getSSTablePath(File directory, Version version, SSTableFormat.Type format)
    {
        Descriptor desc = new Descriptor(version,
                                         directory,
                                         keyspace.getName(),
                                         name,
                                         fileIndexGenerator.incrementAndGet(),
                                         format,
                                         Component.digestFor(BigFormat.latestVersion.uncompressedChecksumType()));
        return desc.filenameFor(Component.DATA);
    }

    /**
     * Switches the memtable iff the live memtable is the one provided
     *
     * @param memtable
     */
    public Future<?> switchMemtableIfCurrent(Memtable memtable)
    {
        synchronized (data)
        {
            if (data.getView().getCurrentMemtable() == memtable)
                return switchMemtable();
        }
        return Futures.immediateFuture(null);
    }

    /*
     * switchMemtable puts Memtable.getSortedContents on the writer executor.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables where it is available for reads.
     * This method does not block except for synchronizing on Tracker, but the Future it returns will
     * not complete until the Memtable (and all prior Memtables) have been successfully flushed, and the CL
     * marked clean up to the position owned by the Memtable.
     */
    public ListenableFuture<?> switchMemtable()
    {
        synchronized (data)
        {
            logFlush();
            Flush flush = new Flush(false);
            flushExecutor.execute(flush);
            ListenableFutureTask<?> task = ListenableFutureTask.create(flush.postFlush, null);
            postFlushExecutor.submit(task);
            return task;
        }
    }

    // print out size of all memtables we're enqueuing
    private void logFlush()
    {
        // reclaiming includes that which we are GC-ing;
        float onHeapRatio = 0, offHeapRatio = 0;
        long onHeapTotal = 0, offHeapTotal = 0;
        Memtable memtable = getTracker().getView().getCurrentMemtable();
        onHeapRatio +=  memtable.getAllocator().onHeap().ownershipRatio();
        offHeapRatio += memtable.getAllocator().offHeap().ownershipRatio();
        onHeapTotal += memtable.getAllocator().onHeap().owns();
        offHeapTotal += memtable.getAllocator().offHeap().owns();

        for (ColumnFamilyStore indexCfs : indexManager.getAllIndexColumnFamilyStores())
        {
            MemtableAllocator allocator = indexCfs.getTracker().getView().getCurrentMemtable().getAllocator();
            onHeapRatio += allocator.onHeap().ownershipRatio();
            offHeapRatio += allocator.offHeap().ownershipRatio();
            onHeapTotal += allocator.onHeap().owns();
            offHeapTotal += allocator.offHeap().owns();
        }

        logger.debug("Enqueuing flush of {}: {}",
                     name,
                     String.format("%s (%.0f%%) on-heap, %s (%.0f%%) off-heap",
                                   FBUtilities.prettyPrintMemory(onHeapTotal),
                                   onHeapRatio * 100,
                                   FBUtilities.prettyPrintMemory(offHeapTotal),
                                   offHeapRatio * 100));
    }


    public ListenableFuture<?> forceFlush()
    {
        return forceFlush(null);
    }

    /**
     * Flush if there is unflushed data that was written to the CommitLog before @param flushIfDirtyBefore
     * (inclusive).  If @param flushIfDirtyBefore is null, flush if there is any unflushed data.
     *
     * @return a Future such that when the future completes, all data inserted before forceFlush was called,
     * will be flushed.
     */
    public ListenableFuture<?> forceFlush(ReplayPosition flushIfDirtyBefore)
    {
        // we synchronize on the data tracker to ensure we don't race against other calls to switchMemtable(),
        // unnecessarily queueing memtables that are about to be made clean
        synchronized (data)
        {
            // during index build, 2ary index memtables can be dirty even if parent is not.  if so,
            // we want to flush the 2ary index ones too.
            boolean clean = true;
            for (ColumnFamilyStore cfs : concatWithIndexes())
                clean &= cfs.data.getView().getCurrentMemtable().isCleanAfter(flushIfDirtyBefore);

            if (clean)
            {
                // We could have a memtable for this column family that is being
                // flushed. Make sure the future returned wait for that so callers can
                // assume that any data inserted prior to the call are fully flushed
                // when the future returns (see #5241).
                ListenableFutureTask<?> task = ListenableFutureTask.create(new Runnable()
                {
                    public void run()
                    {
                        logger.trace("forceFlush requested but everything is clean in {}", name);
                    }
                }, null);
                postFlushExecutor.execute(task);
                return task;
            }

            return switchMemtable();
        }
    }

    public void forceBlockingFlush()
    {
        FBUtilities.waitOnFuture(forceFlush());
    }

    /**
     * Both synchronises custom secondary indexes and provides ordering guarantees for futures on switchMemtable/flush
     * etc, which expect to be able to wait until the flush (and all prior flushes) requested have completed.
     */
    private final class PostFlush implements Runnable
    {
        final boolean flushSecondaryIndexes;
        final OpOrder.Barrier writeBarrier;
        final CountDownLatch latch = new CountDownLatch(1);
        final ReplayPosition lastReplayPosition;
        volatile Throwable flushFailure = null;

        private PostFlush(boolean flushSecondaryIndexes, OpOrder.Barrier writeBarrier, ReplayPosition lastReplayPosition)
        {
            this.writeBarrier = writeBarrier;
            this.flushSecondaryIndexes = flushSecondaryIndexes;
            this.lastReplayPosition = lastReplayPosition;
        }

        public void run()
        {
            writeBarrier.await();

            /**
             * we can flush 2is as soon as the barrier completes, as they will be consistent with (or ahead of) the
             * flushed memtables and CL position, which is as good as we can guarantee.
             * TODO: SecondaryIndex should support setBarrier(), so custom implementations can co-ordinate exactly
             * with CL as we do with memtables/CFS-backed SecondaryIndexes.
             */

            if (flushSecondaryIndexes)
                indexManager.flushAllNonCFSBackedIndexesBlocking();

            try
            {
                // we wait on the latch for the lastReplayPosition to be set, and so that waiters
                // on this task can rely on all prior flushes being complete
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException();
            }

            // must check lastReplayPosition != null because Flush may find that all memtables are clean
            // and so not set a lastReplayPosition
            // If a flush errored out but the error was ignored, make sure we don't discard the commit log.
            if (lastReplayPosition != null && flushFailure == null)
            {
                CommitLog.instance.discardCompletedSegments(metadata.cfId, lastReplayPosition);
            }

            metric.pendingFlushes.dec();

            if (flushFailure != null)
                throw Throwables.propagate(flushFailure);
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
        final List<Memtable> memtables;
        final PostFlush postFlush;
        final boolean truncate;

        private Flush(boolean truncate)
        {
            // if true, we won't flush, we'll just wait for any outstanding writes, switch the memtable, and discard
            this.truncate = truncate;

            metric.pendingFlushes.inc();
            /**
             * To ensure correctness of switch without blocking writes, run() needs to wait for all write operations
             * started prior to the switch to complete. We do this by creating a Barrier on the writeOrdering
             * that all write operations register themselves with, and assigning this barrier to the memtables,
             * after which we *.issue()* the barrier. This barrier is used to direct write operations started prior
             * to the barrier.issue() into the memtable we have switched out, and any started after to its replacement.
             * In doing so it also tells the write operations to update the lastReplayPosition of the memtable, so
             * that we know the CL position we are dirty to, which can be marked clean when we complete.
             */
            writeBarrier = keyspace.writeOrder.newBarrier();
            memtables = new ArrayList<>();

            // submit flushes for the memtable for any indexed sub-cfses, and our own
            AtomicReference<ReplayPosition> lastReplayPositionHolder = new AtomicReference<>();
            for (ColumnFamilyStore cfs : concatWithIndexes())
            {
                // switch all memtables, regardless of their dirty status, setting the barrier
                // so that we can reach a coordinated decision about cleanliness once they
                // are no longer possible to be modified
                Memtable mt = cfs.data.switchMemtable(truncate);
                mt.setDiscarding(writeBarrier, lastReplayPositionHolder);
                memtables.add(mt);
            }

            // we now attempt to define the lastReplayPosition; we do this by grabbing the current limit from the CL
            // and attempting to set the holder to this value. at the same time all writes to the memtables are
            // also maintaining this value, so if somebody sneaks ahead of us somehow (should be rare) we simply retry,
            // so that we know all operations prior to the position have not reached it yet
            ReplayPosition lastReplayPosition;
            while (true)
            {
                lastReplayPosition = new Memtable.LastReplayPosition(CommitLog.instance.getContext());
                ReplayPosition currentLast = lastReplayPositionHolder.get();
                if ((currentLast == null || currentLast.compareTo(lastReplayPosition) <= 0)
                    && lastReplayPositionHolder.compareAndSet(currentLast, lastReplayPosition))
                    break;
            }

            // we then issue the barrier; this lets us wait for all operations started prior to the barrier to complete;
            // since this happens after wiring up the lastReplayPosition, we also know all operations with earlier
            // replay positions have also completed, i.e. the memtables are done and ready to flush
            writeBarrier.issue();
            postFlush = new PostFlush(!truncate, writeBarrier, lastReplayPosition);
        }

        public void run()
        {
            // mark writes older than the barrier as blocking progress, permitting them to exceed our memory limit
            // if they are stuck waiting on it, then wait for them all to complete
            writeBarrier.markBlocking();
            writeBarrier.await();

            // mark all memtables as flushing, removing them from the live memtable list, and
            // remove any memtables that are already clean from the set we need to flush
            Iterator<Memtable> iter = memtables.iterator();
            while (iter.hasNext())
            {
                Memtable memtable = iter.next();
                memtable.cfs.data.markFlushing(memtable);
                if (memtable.isClean() || truncate)
                {
                    memtable.cfs.replaceFlushed(memtable, null);
                    reclaim(memtable);
                    iter.remove();
                }
            }

            if (memtables.isEmpty())
            {
                postFlush.latch.countDown();
                return;
            }

            metric.memtableSwitchCount.inc();

            try
            {
                for (Memtable memtable : memtables)
                {
                    flushMemtable(memtable);
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                postFlush.flushFailure = t;
            }
            // signal the post-flush we've done our work
            postFlush.latch.countDown();
        }

        public void flushMemtable(Memtable memtable)
        {
            List<Future<SSTableMultiWriter>> futures = new ArrayList<>();
            long totalBytesOnDisk = 0;
            long maxBytesOnDisk = 0;
            long minBytesOnDisk = Long.MAX_VALUE;
            List<SSTableReader> sstables = new ArrayList<>();
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
            {
                List<Memtable.FlushRunnable> flushRunnables = null;
                List<SSTableMultiWriter> flushResults = null;

                try
                {
                    // flush the memtable
                    flushRunnables = memtable.flushRunnables(txn);

                    for (int i = 0; i < flushRunnables.size(); i++)
                        futures.add(perDiskflushExecutors[i].submit(flushRunnables.get(i)));

                    flushResults = Lists.newArrayList(FBUtilities.waitOnFutures(futures));
                }
                catch (Throwable t)
                {
                    t = memtable.abortRunnables(flushRunnables, t);
                    t = txn.abort(t);
                    throw Throwables.propagate(t);
                }

                try
                {
                    Iterator<SSTableMultiWriter> writerIterator = flushResults.iterator();
                    while (writerIterator.hasNext())
                    {
                        @SuppressWarnings("resource")
                        SSTableMultiWriter writer = writerIterator.next();
                        if (writer.getFilePointer() > 0)
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
                    Throwables.propagate(t);
                }

                txn.prepareToCommit();

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
            }
            memtable.cfs.replaceFlushed(memtable, sstables);
            reclaim(memtable);
                logger.debug("Flushed to {} ({} sstables, {}), biggest {}, smallest {}",
                             sstables,
                             sstables.size(),
                             FBUtilities.prettyPrintMemory(totalBytesOnDisk),
                             FBUtilities.prettyPrintMemory(maxBytesOnDisk),
                             FBUtilities.prettyPrintMemory(minBytesOnDisk));
        }

        private void reclaim(final Memtable memtable)
        {
            // issue a read barrier for reclaiming the memory, and offload the wait to another thread
            final OpOrder.Barrier readBarrier = readOrdering.newBarrier();
            readBarrier.issue();
            reclaimExecutor.execute(new WrappedRunnable()
            {
                public void runMayThrow() throws InterruptedException, ExecutionException
                {
                    readBarrier.await();
                    memtable.setDiscarded();
                }
            });
        }
    }

    /**
     * Finds the largest memtable, as a percentage of *either* on- or off-heap memory limits, and immediately
     * queues it for flushing. If the memtable selected is flushed before this completes, no work is done.
     */
    public static class FlushLargestColumnFamily implements Runnable
    {
        public void run()
        {
            float largestRatio = 0f;
            Memtable largest = null;
            float liveOnHeap = 0, liveOffHeap = 0;
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            {
                // we take a reference to the current main memtable for the CF prior to snapping its ownership ratios
                // to ensure we have some ordering guarantee for performing the switchMemtableIf(), i.e. we will only
                // swap if the memtables we are measuring here haven't already been swapped by the time we try to swap them
                Memtable current = cfs.getTracker().getView().getCurrentMemtable();

                // find the total ownership ratio for the memtable and all SecondaryIndexes owned by this CF,
                // both on- and off-heap, and select the largest of the two ratios to weight this CF
                float onHeap = 0f, offHeap = 0f;
                onHeap += current.getAllocator().onHeap().ownershipRatio();
                offHeap += current.getAllocator().offHeap().ownershipRatio();

                for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
                {
                    MemtableAllocator allocator = indexCfs.getTracker().getView().getCurrentMemtable().getAllocator();
                    onHeap += allocator.onHeap().ownershipRatio();
                    offHeap += allocator.offHeap().ownershipRatio();
                }

                float ratio = Math.max(onHeap, offHeap);
                if (ratio > largestRatio)
                {
                    largest = current;
                    largestRatio = ratio;
                }

                liveOnHeap += onHeap;
                liveOffHeap += offHeap;
            }

            if (largest != null)
            {
                float usedOnHeap = Memtable.MEMORY_POOL.onHeap.usedRatio();
                float usedOffHeap = Memtable.MEMORY_POOL.offHeap.usedRatio();
                float flushingOnHeap = Memtable.MEMORY_POOL.onHeap.reclaimingRatio();
                float flushingOffHeap = Memtable.MEMORY_POOL.offHeap.reclaimingRatio();
                float thisOnHeap = largest.getAllocator().onHeap().ownershipRatio();
                float thisOffHeap = largest.getAllocator().onHeap().ownershipRatio();
                logger.debug("Flushing largest {} to free up room. Used total: {}, live: {}, flushing: {}, this: {}",
                            largest.cfs, ratio(usedOnHeap, usedOffHeap), ratio(liveOnHeap, liveOffHeap),
                            ratio(flushingOnHeap, flushingOffHeap), ratio(thisOnHeap, thisOffHeap));
                largest.cfs.switchMemtableIfCurrent(largest);
            }
        }
    }

    private static String ratio(float onHeap, float offHeap)
    {
        return String.format("%.2f/%.2f", onHeap, offHeap);
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Keyspace.switchLock
     * param @ lock - lock that needs to be used.
     * param @ key - key for update/insert
     * param @ columnFamily - columnFamily changes
     */
    public void apply(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, ReplayPosition replayPosition)

    {
        long start = System.nanoTime();
        Memtable mt = data.getMemtableFor(opGroup, replayPosition);
        long timeDelta = mt.put(update, indexer, opGroup);
        DecoratedKey key = update.partitionKey();
        invalidateCachedPartition(key);
        metric.samplers.get(Sampler.WRITES).addSample(key.getKey(), key.hashCode(), 1);
        StorageHook.instance.reportWrite(metadata.cfId, update);
        metric.writeLatency.addNano(System.nanoTime() - start);
        if(timeDelta < Long.MAX_VALUE)
            metric.colUpdateTimeDeltaHistogram.update(timeDelta);
    }

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    public Collection<SSTableReader> getOverlappingSSTables(SSTableSet sstableSet, Iterable<SSTableReader> sstables)
    {
        logger.trace("Checking for sstables overlapping {}", sstables);

        // a normal compaction won't ever have an empty sstables list, but we create a skeleton
        // compaction controller for streaming, and that passes an empty list.
        if (!sstables.iterator().hasNext())
            return ImmutableSet.of();

        View view = data.getView();

        List<SSTableReader> sortedByFirst = Lists.newArrayList(sstables);
        Collections.sort(sortedByFirst, (o1, o2) -> o1.first.compareTo(o2.first));

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
                first = sstable.first;
                last = sstable.last;
            }
            else
            {
                if (sstable.first.compareTo(last) <= 0) // we do overlap
                {
                    if (sstable.last.compareTo(last) > 0)
                        last = sstable.last;
                }
                else
                {
                    bounds.add(AbstractBounds.bounds(first, true, last, true));
                    first = sstable.first;
                    last = sstable.last;
                }
            }
        }
        bounds.add(AbstractBounds.bounds(first, true, last, true));
        Set<SSTableReader> results = new HashSet<>();

        for (AbstractBounds<PartitionPosition> bound : bounds)
            Iterables.addAll(results, view.sstablesInBounds(sstableSet, bound.left, bound.right));

        return Sets.difference(results, ImmutableSet.copyOf(sstables));
    }

    /**
     * like getOverlappingSSTables, but acquires references before returning
     */
    public Refs<SSTableReader> getAndReferenceOverlappingSSTables(SSTableSet sstableSet, Iterable<SSTableReader> sstables)
    {
        while (true)
        {
            Iterable<SSTableReader> overlapped = getOverlappingSSTables(sstableSet, sstables);
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
        addSSTables(Arrays.asList(sstable));
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
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        for (SSTableReader sstable : sstables)
        {
            List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(ranges);
            for (Pair<Long, Long> position : positions)
                expectedFileSize += position.right - position.left;
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

    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs) throws ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, skipCorrupted, false, checkData, jobs);
    }

    @VisibleForTesting
    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean alwaysFail, boolean checkData, int jobs) throws ExecutionException, InterruptedException
    {
        // skip snapshot creation during scrub, SEE JIRA 5891
        if(!disableSnapshot)
            snapshotWithoutFlush("pre-scrub-" + System.currentTimeMillis());

        try
        {
            return CompactionManager.instance.performScrub(ColumnFamilyStore.this, skipCorrupted, checkData, jobs);
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

    public CompactionManager.AllSSTableOpStatus verify(boolean extendedVerify) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performVerify(ColumnFamilyStore.this, extendedVerify);
    }

    public CompactionManager.AllSSTableOpStatus sstablesRewrite(boolean excludeCurrentVersion, int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performSSTableRewrite(ColumnFamilyStore.this, excludeCurrentVersion, jobs);
    }

    public CompactionManager.AllSSTableOpStatus relocateSSTables(int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.relocateSSTables(this, jobs);
    }

    public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        assert !sstables.isEmpty();
        maybeFail(data.dropSSTables(Predicates.in(sstables), compactionType, null));
    }

    void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables)
    {
        compactionStrategyManager.replaceFlushed(memtable, sstables);
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
        return data.getView().sstables(sstableSet);
    }

    public Iterable<SSTableReader> getUncompactingSSTables()
    {
        return data.getUncompacting();
    }

    public boolean isFilterFullyCoveredBy(ClusteringIndexFilter filter, DataLimits limits, CachedPartition cached, int nowInSec)
    {
        // We can use the cached value only if we know that no data it doesn't contain could be covered
        // by the query filter, that is if:
        //   1) either the whole partition is cached
        //   2) or we can ensure than any data the filter selects is in the cached partition

        // We can guarantee that a partition is fully cached if the number of rows it contains is less than
        // what we're caching. Wen doing that, we should be careful about expiring cells: we should count
        // something expired that wasn't when the partition was cached, or we could decide that the whole
        // partition is cached when it's not. This is why we use CachedPartition#cachedLiveRows.
        if (cached.cachedLiveRows() < metadata.params.caching.rowsPerPartitionToCache())
            return true;

        // If the whole partition isn't cached, then we must guarantee that the filter cannot select data that
        // is not in the cache. We can guarantee that if either the filter is a "head filter" and the cached
        // partition has more live rows that queried (where live rows refers to the rows that are live now),
        // or if we can prove that everything the filter selects is in the cached partition based on its content.
        return (filter.isHeadFilter() && limits.hasEnoughLiveData(cached, nowInSec)) || filter.isFullyCoveredBy(cached);
    }

    public int gcBefore(int nowInSec)
    {
        return nowInSec - metadata.params.gcGraceSeconds;
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
                logger.info("Spinning trying to capture released readers {}", released);
                logger.info("Spinning trying to capture all readers {}", view.sstables);
                failingSince = System.nanoTime();
            }
        }
    }

    public ViewFragment select(Function<View, Iterable<SSTableReader>> filter)
    {
        View view = data.getView();
        List<SSTableReader> sstables = Lists.newArrayList(filter.apply(view));
        return new ViewFragment(sstables, view.getAllMemtables());
    }

    // WARNING: this returns the set of LIVE sstables only, which may be only partially written
    public List<String> getSSTablesForKey(String key)
    {
        return getSSTablesForKey(key, false);
    }

    public List<String> getSSTablesForKey(String key, boolean hexFormat)
    {
        ByteBuffer keyBuffer = hexFormat ? ByteBufferUtil.hexToBytes(key) : metadata.getKeyValidator().fromString(key);
        DecoratedKey dk = decorateKey(keyBuffer);
        try (OpOrder.Group op = readOrdering.start())
        {
            List<String> files = new ArrayList<>();
            for (SSTableReader sstr : select(View.select(SSTableSet.LIVE, dk)).sstables)
            {
                // check if the key actually exists in this sstable, without updating cache and stats
                if (sstr.getPosition(dk, SSTableReader.Operator.EQ, false) != null)
                    files.add(sstr.getFilename());
            }
            return files;
        }
    }


    public void beginLocalSampling(String sampler, int capacity)
    {
        metric.samplers.get(Sampler.valueOf(sampler)).beginSampling(capacity);
    }

    public CompositeData finishLocalSampling(String sampler, int count) throws OpenDataException
    {
        SamplerResult<ByteBuffer> samplerResults = metric.samplers.get(Sampler.valueOf(sampler))
                .finishSampling(count);
        TabularDataSupport result = new TabularDataSupport(COUNTER_TYPE);
        for (Counter<ByteBuffer> counter : samplerResults.topK)
        {
            //Not duplicating the buffer for safety because AbstractSerializer and ByteBufferUtil.bytesToHex
            //don't modify position or limit
            ByteBuffer key = counter.getItem();
            result.put(new CompositeDataSupport(COUNTER_COMPOSITE_TYPE, COUNTER_NAMES, new Object[] {
                    ByteBufferUtil.bytesToHex(key), // raw
                    counter.getCount(),  // count
                    counter.getError(),  // error
                    metadata.getKeyValidator().getString(key) })); // string
        }
        return new CompositeDataSupport(SAMPLING_RESULT, SAMPLER_NAMES, new Object[]{
                samplerResults.cardinality, result});
    }

    public void cleanupCache()
    {
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());

        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.ksAndCFName.equals(metadata.ksAndCFName) && !Range.isInRanges(dk.getToken(), ranges))
                invalidateCachedPartition(dk);
        }

        if (metadata.isCounter())
        {
            for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
                 keyIter.hasNext(); )
            {
                CounterCacheKey key = keyIter.next();
                DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.partitionKey));
                if (key.ksAndCFName.equals(metadata.ksAndCFName) && !Range.isInRanges(dk.getToken(), ranges))
                    CacheService.instance.counterCache.remove(key);
            }
        }
    }

    public ClusteringComparator getComparator()
    {
        return metadata.comparator;
    }

    public void snapshotWithoutFlush(String snapshotName)
    {
        snapshotWithoutFlush(snapshotName, null, false);
    }

    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned during next startup
     */
    public Set<SSTableReader> snapshotWithoutFlush(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral)
    {
        Set<SSTableReader> snapshottedSSTables = new HashSet<>();
        for (ColumnFamilyStore cfs : concatWithIndexes())
        {
            final JSONArray filesJSONArr = new JSONArray();
            try (RefViewFragment currentView = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.getPath()); // hard links
                    filesJSONArr.add(ssTable.descriptor.relativeFilenameFor(Component.DATA));

                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", keyspace, ssTable.getFilename(), snapshotDirectory);
                    snapshottedSSTables.add(ssTable);
                }

                writeSnapshotManifest(filesJSONArr, snapshotName);
            }
        }
        if (ephemeral)
            createEphemeralSnapshotMarkerFile(snapshotName);
        return snapshottedSSTables;
    }

    private void writeSnapshotManifest(final JSONArray filesJSONArr, final String snapshotName)
    {
        final File manifestFile = getDirectories().getSnapshotManifestFile(snapshotName);

        try
        {
            if (!manifestFile.getParentFile().exists())
                manifestFile.getParentFile().mkdirs();

            try (PrintStream out = new PrintStream(manifestFile))
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

    private void createEphemeralSnapshotMarkerFile(final String snapshot)
    {
        final File ephemeralSnapshotMarker = getDirectories().getNewEphemeralSnapshotMarkerFile(snapshot);

        try
        {
            if (!ephemeralSnapshotMarker.getParentFile().exists())
                ephemeralSnapshotMarker.getParentFile().mkdirs();

            Files.createFile(ephemeralSnapshotMarker.toPath());
            logger.trace("Created ephemeral snapshot marker file on {}.", ephemeralSnapshotMarker.getAbsolutePath());
        }
        catch (IOException e)
        {
            logger.warn(String.format("Could not create marker file %s for ephemeral snapshot %s. " +
                                      "In case there is a failure in the operation that created " +
                                      "this snapshot, you may need to clean it manually afterwards.",
                                      ephemeralSnapshotMarker.getAbsolutePath(), snapshot), e);
        }
    }

    protected static void clearEphemeralSnapshots(Directories directories)
    {
        for (String ephemeralSnapshot : directories.listEphemeralSnapshots())
        {
            logger.trace("Clearing ephemeral snapshot {} leftover from previous session.", ephemeralSnapshot);
            Directories.clearSnapshot(ephemeralSnapshot, directories.getCFDirectories());
        }
    }

    public Refs<SSTableReader> getSnapshotSSTableReader(String tag) throws IOException
    {
        Map<Integer, SSTableReader> active = new HashMap<>();
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            active.put(sstable.descriptor.generation, sstable);
        Map<Descriptor, Set<Component>> snapshots = getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(tag).list();
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            for (Map.Entry<Descriptor, Set<Component>> entries : snapshots.entrySet())
            {
                // Try acquire reference to an active sstable instead of snapshot if it exists,
                // to avoid opening new sstables. If it fails, use the snapshot reference instead.
                SSTableReader sstable = active.get(entries.getKey().generation);
                if (sstable == null || !refs.tryRef(sstable))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("using snapshot sstable {}", entries.getKey());
                    // open without tracking hotness
                    sstable = SSTableReader.open(entries.getKey(), entries.getValue(), metadata, true, false);
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
        catch (IOException | RuntimeException e)
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
        return snapshot(snapshotName, false);
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     * @param skipFlush Skip blocking flush of memtable
     */
    public Set<SSTableReader> snapshot(String snapshotName, boolean skipFlush)
    {
        return snapshot(snapshotName, null, false, skipFlush);
    }


    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     * @param skipFlush Skip blocking flush of memtable
     */
    public Set<SSTableReader> snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, boolean skipFlush)
    {
        if (!skipFlush)
        {
            forceBlockingFlush();
        }
        return snapshotWithoutFlush(snapshotName, predicate, ephemeral);
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
        List<File> snapshotDirs = getDirectories().getCFDirectories();
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }
    /**
     *
     * @return  Return a map of all snapshots to space being used
     * The pair for a snapshot has true size and size on disk.
     */
    public Map<String, Pair<Long,Long>> getSnapshotDetails()
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
        IRowCacheEntry cached = CacheService.instance.rowCache.getInternal(new RowCacheKey(metadata.ksAndCFName, key));
        return cached == null || cached instanceof RowCacheSentinel ? null : (CachedPartition)cached;
    }

    private void invalidateCaches()
    {
        CacheService.instance.invalidateKeyCacheForCf(metadata.ksAndCFName);
        CacheService.instance.invalidateRowCacheForCf(metadata.ksAndCFName);
        if (metadata.isCounter())
            CacheService.instance.invalidateCounterCacheForCf(metadata.ksAndCFName);
    }

    public int invalidateRowCache(Collection<Bounds<Token>> boundsToInvalidate)
    {
        int invalidatedKeys = 0;
        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.ksAndCFName.equals(metadata.ksAndCFName) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
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
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.partitionKey));
            if (key.ksAndCFName.equals(metadata.ksAndCFName) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
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
        return CacheService.instance.rowCache.getCapacity() != 0 && CacheService.instance.rowCache.containsKey(new RowCacheKey(metadata.ksAndCFName, key));
    }

    public void invalidateCachedPartition(RowCacheKey key)
    {
        CacheService.instance.rowCache.remove(key);
    }

    public void invalidateCachedPartition(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return;

        invalidateCachedPartition(new RowCacheKey(metadata.ksAndCFName, key));
    }

    public ClockAndCount getCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnDefinition column, CellPath path)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return null;
        return CacheService.instance.counterCache.get(CounterCacheKey.create(metadata.ksAndCFName, partitionKey, clustering, column, path));
    }

    public void putCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnDefinition column, CellPath path, ClockAndCount clockAndCount)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return;
        CacheService.instance.counterCache.put(CounterCacheKey.create(metadata.ksAndCFName, partitionKey, clustering, column, path), clockAndCount);
    }

    public void forceMajorCompaction() throws InterruptedException, ExecutionException
    {
        forceMajorCompaction(false);
    }


    public void forceMajorCompaction(boolean splitOutput) throws InterruptedException, ExecutionException
    {
        CompactionManager.instance.performMaximal(this, splitOutput);
    }

    public static Iterable<ColumnFamilyStore> all()
    {
        List<Iterable<ColumnFamilyStore>> stores = new ArrayList<Iterable<ColumnFamilyStore>>(Schema.instance.getKeyspaces().size());
        for (Keyspace keyspace : Keyspace.all())
        {
            stores.add(keyspace.getColumnFamilyStores());
        }
        return Iterables.concat(stores);
    }

    public Iterable<DecoratedKey> keySamples(Range<Token> range)
    {
        try (RefViewFragment view = selectAndReference(View.select(SSTableSet.CANONICAL)))
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
        try (RefViewFragment view = selectAndReference(View.select(SSTableSet.CANONICAL)))
        {
            long count = 0;
            for (SSTableReader sstable : view.sstables)
                count += sstable.estimatedKeysForRanges(Collections.singleton(range));
            return count;
        }
    }

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
            cfs.runWithCompactionsDisabled(new Callable<Void>()
            {
                public Void call()
                {
                    cfs.data.reset();
                    return null;
                }
            }, true, false);
        }
    }

    /**
     * Truncate deletes the entire column family's data with no expensive tombstone creation
     */
    public void truncateBlocking()
    {
        // We have two goals here:
        // - truncate should delete everything written before truncate was invoked
        // - but not delete anything that isn't part of the snapshot we create.
        // We accomplish this by first flushing manually, then snapshotting, and
        // recording the timestamp IN BETWEEN those actions. Any sstables created
        // with this timestamp or greater time, will not be marked for delete.
        //
        // Bonus complication: since we store replay position in sstable metadata,
        // truncating those sstables means we will replay any CL segments from the
        // beginning if we restart before they [the CL segments] are discarded for
        // normal reasons post-truncate.  To prevent this, we store truncation
        // position in the System keyspace.
        logger.trace("truncating {}", name);

        if (keyspace.getMetadata().params.durableWrites || DatabaseDescriptor.isAutoSnapshot())
        {
            // flush the CF being truncated before forcing the new segment
            forceBlockingFlush();

            viewManager.forceBlockingFlush();

            // sleep a little to make sure that our truncatedAt comes after any sstable
            // that was part of the flushed we forced; otherwise on a tie, it won't get deleted.
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
        }
        else
        {
            dumpMemtable();
            viewManager.dumpMemtables();
        }

        Runnable truncateRunnable = new Runnable()
        {
            public void run()
            {
                logger.trace("Discarding sstable data for truncated CF + indexes");

                final long truncatedAt = System.currentTimeMillis();
                data.notifyTruncated(truncatedAt);

                if (DatabaseDescriptor.isAutoSnapshot())
                    snapshot(Keyspace.getTimestampedSnapshotName(name));

                ReplayPosition replayAfter = discardSSTables(truncatedAt);

                indexManager.truncateAllIndexesBlocking(truncatedAt);

                viewManager.truncateBlocking(truncatedAt);

                SystemKeyspace.saveTruncationRecord(ColumnFamilyStore.this, truncatedAt, replayAfter);
                logger.trace("cleaning out row cache");
                invalidateCaches();
            }
        };

        runWithCompactionsDisabled(Executors.callable(truncateRunnable), true, true);
        logger.trace("truncate complete");
    }

    /**
     * Drops current memtable without flushing to disk. This should only be called when truncating a column family which is not durable.
     */
    public void dumpMemtable()
    {
        synchronized (data)
        {
            final Flush flush = new Flush(true);
            flushExecutor.execute(flush);
            postFlushExecutor.submit(flush.postFlush);
        }
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews)
    {
        // synchronize so that concurrent invocations don't re-enable compactions partway through unexpectedly,
        // and so we only run one major compaction at a time
        synchronized (this)
        {
            logger.trace("Cancelling in-progress compactions for {}", metadata.cfName);

            Iterable<ColumnFamilyStore> selfWithAuxiliaryCfs = interruptViews
                                                               ? Iterables.concat(concatWithIndexes(), viewManager.allViewsCfs())
                                                               : concatWithIndexes();

            for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                cfs.getCompactionStrategyManager().pause();
            try
            {
                // interrupt in-progress compactions
                CompactionManager.instance.interruptCompactionForCFs(selfWithAuxiliaryCfs, interruptValidation);
                CompactionManager.instance.waitForCessation(selfWithAuxiliaryCfs);

                // doublecheck that we finished, instead of timing out
                for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                {
                    if (!cfs.getTracker().getCompacting().isEmpty())
                    {
                        logger.warn("Unable to cancel in-progress compactions for {}.  Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded.", metadata.cfName);
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
            finally
            {
                for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                    cfs.getCompactionStrategyManager().resume();
            }
        }
    }

    public LifecycleTransaction markAllCompacting(final OperationType operationType)
    {
        Callable<LifecycleTransaction> callable = new Callable<LifecycleTransaction>()
        {
            public LifecycleTransaction call() throws Exception
            {
                assert data.getCompacting().isEmpty() : data.getCompacting();
                Collection<SSTableReader> sstables = Lists.newArrayList(AbstractCompactionStrategy.filterSuspectSSTables(getSSTables(SSTableSet.LIVE)));
                LifecycleTransaction modifier = data.tryModify(sstables, operationType);
                assert modifier != null: "something marked things compacting while compactions are disabled";
                return modifier;
            }
        };

        return runWithCompactionsDisabled(callable, false, false);
    }


    @Override
    public String toString()
    {
        return "CFS(" +
               "Keyspace='" + keyspace.getName() + '\'' +
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

    public int getMeanColumns()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            long n = sstable.getEstimatedColumnCount().count();
            sum += sstable.getEstimatedColumnCount().mean() * n;
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

    public long estimateKeys()
    {
        long n = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            n += sstable.estimatedKeys();
        return n;
    }

    public IPartitioner getPartitioner()
    {
        return metadata.partitioner;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return metadata.decorateKey(key);
    }

    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return metadata.isIndex();
    }

    public Iterable<ColumnFamilyStore> concatWithIndexes()
    {
        // we return the main CFS first, which we rely on for simplicity in switchMemtable(), for getting the
        // latest replay position
        return Iterables.concat(Collections.singleton(this), indexManager.getAllIndexColumnFamilyStores());
    }

    public List<String> getBuiltIndexes()
    {
       return indexManager.getBuiltIndexNames();
    }

    public int getUnleveledSSTables()
    {
        return compactionStrategyManager.getUnleveledSSTables();
    }

    public int[] getSSTableCountPerLevel()
    {
        return compactionStrategyManager.getSSTableCountPerLevel();
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

        boolean retval = metadata.params.caching.cacheRows() && CacheService.instance.rowCache.getCapacity() > 0;
        assert(!retval || !isIndex());
        return retval;
    }

    public boolean isCounterCacheEnabled()
    {
        return metadata.isCounter() && CacheService.instance.counterCache.getCapacity() > 0;
    }

    public boolean isKeyCacheEnabled()
    {
        return metadata.params.caching.cacheKeys() && CacheService.instance.keyCache.getCapacity() > 0;
    }

    /**
     * Discard all SSTables that were created before given timestamp.
     *
     * Caller should first ensure that comapctions have quiesced.
     *
     * @param truncatedAt The timestamp of the truncation
     *                    (all SSTables before that timestamp are going be marked as compacted)
     *
     * @return the most recent replay position of the truncated data
     */
    public ReplayPosition discardSSTables(long truncatedAt)
    {
        assert data.getCompacting().isEmpty() : data.getCompacting();

        List<SSTableReader> truncatedSSTables = new ArrayList<>();

        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            if (!sstable.newSince(truncatedAt))
                truncatedSSTables.add(sstable);
        }

        if (truncatedSSTables.isEmpty())
            return ReplayPosition.NONE;

        markObsolete(truncatedSSTables, OperationType.UNKNOWN);
        return ReplayPosition.getReplayPosition(truncatedSSTables);
    }

    public double getDroppableTombstoneRatio()
    {
        double allDroppable = 0;
        long allColumns = 0;
        int localTime = (int)(System.currentTimeMillis()/1000);

        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            allDroppable += sstable.getDroppableTombstonesBefore(localTime - sstable.metadata.params.gcGraceSeconds);
            allColumns += sstable.getEstimatedColumnCount().mean() * sstable.getEstimatedColumnCount().count();
        }
        return allColumns > 0 ? allDroppable / allColumns : 0;
    }

    public long trueSnapshotsSize()
    {
        return getDirectories().trueSnapshotsSize();
    }

    @VisibleForTesting
    void resetFileIndexGenerator()
    {
        fileIndexGenerator.set(0);
    }

    /**
     * Returns a ColumnFamilyStore by cfId if it exists, null otherwise
     * Differently from others, this method does not throw exception if the table does not exist.
     */
    public static ColumnFamilyStore getIfExists(UUID cfId)
    {
        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        if (kscf == null)
            return null;

        Keyspace keyspace = Keyspace.open(kscf.left);
        if (keyspace == null)
            return null;

        return keyspace.getColumnFamilyStore(cfId);
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

        UUID id = Schema.instance.getId(ksName, cfName);
        if (id == null)
            return null;

        return keyspace.getColumnFamilyStore(id);
    }
}
