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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
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

import org.apache.cassandra.io.FSWriteError;
import org.json.simple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.config.CFMetaData.SpeculativeRetry;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.metrics.ColumnFamilyMetrics.Sampler;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamLockfile;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.*;
import org.apache.cassandra.utils.TopKSampler.SamplerResult;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import com.clearspring.analytics.stream.Counter;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private static final ExecutorService flushExecutor = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                                                                          StageManager.KEEPALIVE,
                                                                                          TimeUnit.SECONDS,
                                                                                          new LinkedBlockingQueue<Runnable>(),
                                                                                          new NamedThreadFactory("MemtableFlushWriter"),
                                                                                          "internal");

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
    public final IPartitioner partitioner;
    private final String mbeanName;
    private volatile boolean valid = true;

    /**
     * Memtables and SSTables on disk for this column family.
     *
     * We synchronize on the DataTracker to ensure isolation when we want to make sure
     * that the memtable we're acting on doesn't change out from under us.  I.e., flush
     * syncronizes on it to make sure it can submit on both executors atomically,
     * so anyone else who wants to make sure flush doesn't interfere should as well.
     */
    private final DataTracker data;

    /* The read order, used to track accesses to off-heap memtable storage */
    public final OpOrder readOrdering = new OpOrder();

    /* This is used to generate the next index for a SSTable */
    private final AtomicInteger fileIndexGenerator = new AtomicInteger(0);

    public final SecondaryIndexManager indexManager;

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultInteger minCompactionThreshold;
    private volatile DefaultInteger maxCompactionThreshold;
    private final WrappingCompactionStrategy compactionStrategyWrapper;

    public final Directories directories;

    public final ColumnFamilyMetrics metric;
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
                cfs.minCompactionThreshold = new DefaultInteger(metadata.getMinCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultInteger(metadata.getMaxCompactionThreshold());

        compactionStrategyWrapper.maybeReloadCompactionStrategy(metadata);

        scheduleFlush();

        indexManager.reload();

        // If the CF comparator has changed, we need to change the memtable,
        // because the old one still aliases the previous comparator.
        if (data.getView().getCurrentMemtable().initialComparator != metadata.comparator)
            switchMemtable();
    }

    void scheduleFlush()
    {
        int period = metadata.getMemtableFlushPeriod();
        if (period > 0)
        {
            logger.debug("scheduling flush in {} ms", period);
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

    public void setCompactionStrategyClass(String compactionStrategyClass)
    {
        try
        {
            metadata.compactionStrategyClass = CFMetaData.createCompactionStrategy(compactionStrategyClass);
            compactionStrategyWrapper.maybeReloadCompactionStrategy(metadata);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public String getCompactionStrategyClass()
    {
        return metadata.compactionStrategyClass.getName();
    }

    public Map<String,String> getCompressionParameters()
    {
        return metadata.compressionParameters().asThriftOptions();
    }

    public void setCompressionParameters(Map<String,String> opts)
    {
        try
        {
            metadata.compressionParameters = CompressionParameters.create(opts);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        try
        {
            for (SSTableReader sstable : keyspace.getAllSSTables())
                if (sstable.compression)
                    sstable.getCompressionMetadata().parameters.setCrcCheckChance(crcCheckChance);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private ColumnFamilyStore(Keyspace keyspace,
                              String columnFamilyName,
                              IPartitioner partitioner,
                              int generation,
                              CFMetaData metadata,
                              Directories directories,
                              boolean loadSSTables)
    {
        assert metadata != null : "null metadata for " + keyspace + ":" + columnFamilyName;

        this.keyspace = keyspace;
        name = columnFamilyName;
        this.metadata = metadata;
        this.minCompactionThreshold = new DefaultInteger(metadata.getMinCompactionThreshold());
        this.maxCompactionThreshold = new DefaultInteger(metadata.getMaxCompactionThreshold());
        this.partitioner = partitioner;
        this.directories = directories;
        this.indexManager = new SecondaryIndexManager(this);
        this.metric = new ColumnFamilyMetrics(this);
        fileIndexGenerator.set(generation);
        sampleLatencyNanos = DatabaseDescriptor.getReadRpcTimeout() / 2;

        CachingOptions caching = metadata.getCaching();

        logger.info("Initializing {}.{}", keyspace.getName(), name);

        // scan for sstables corresponding to this cf and load them
        data = new DataTracker(this);

        if (loadSSTables)
        {
            Directories.SSTableLister sstableFiles = directories.sstableLister().skipTemporary(true);
            Collection<SSTableReader> sstables = SSTableReader.openAll(sstableFiles.list().entrySet(), metadata, this.partitioner);
            data.addInitialSSTables(sstables);
        }

        if (caching.keyCache.isEnabled())
            CacheService.instance.keyCache.loadSaved(this);

        // compaction strategy should be created after the CFS has been prepared
        this.compactionStrategyWrapper = new WrappingCompactionStrategy(this);

        if (maxCompactionThreshold.value() <= 0 || minCompactionThreshold.value() <=0)
        {
            logger.warn("Disabling compaction strategy by setting compaction thresholds to 0 is deprecated, set the compaction option 'enabled' to 'false' instead.");
            this.compactionStrategyWrapper.disable();
        }

        // create the private ColumnFamilyStores for the secondary column indexes
        for (ColumnDefinition info : metadata.allColumns())
        {
            if (info.getIndexType() != null)
                indexManager.addIndexedColumn(info);
        }

        // register the mbean
        String type = this.partitioner instanceof LocalPartitioner ? "IndexColumnFamilies" : "ColumnFamilies";
        mbeanName = "org.apache.cassandra.db:type=" + type + ",keyspace=" + this.keyspace.getName() + ",columnfamily=" + name;
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName nameObj = new ObjectName(mbeanName);
            mbs.registerMBean(this, nameObj);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        logger.debug("retryPolicy for {} is {}", name, this.metadata.getSpeculativeRetry());
        latencyCalculator = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(new Runnable()
        {
            public void run()
            {
                SpeculativeRetry retryPolicy = ColumnFamilyStore.this.metadata.getSpeculativeRetry();
                switch (retryPolicy.type)
                {
                    case PERCENTILE:
                        // get percentile in nanos
                        sampleLatencyNanos = (long) (metric.coordinatorReadLatency.getSnapshot().getValue(retryPolicy.value) * 1000d);
                        break;
                    case CUSTOM:
                        // convert to nanos, since configuration is in millisecond
                        sampleLatencyNanos = (long) (retryPolicy.value * 1000d * 1000d);
                        break;
                    default:
                        sampleLatencyNanos = Long.MAX_VALUE;
                        break;
                }
            }
        }, DatabaseDescriptor.getReadRpcTimeout(), DatabaseDescriptor.getReadRpcTimeout(), TimeUnit.MILLISECONDS);
    }

    /** call when dropping or renaming a CF. Performs mbean housekeeping and invalidates CFS to other operations */
    public void invalidate()
    {
        valid = false;

        try
        {
            unregisterMBean();
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            // this shouldn't block anything.
            logger.warn("Failed unregistering mbean: {}", mbeanName, e);
        }

        latencyCalculator.cancel(false);
        compactionStrategyWrapper.shutdown();
        SystemKeyspace.removeTruncationRecord(metadata.cfId);
        data.unreferenceSSTables();
        indexManager.invalidate();

        invalidateCaches();
    }

    /**
     * Removes every SSTable in the directory from the DataTracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void maybeRemoveUnreadableSSTables(File directory)
    {
        data.removeUnreadableSSTables(directory);
    }

    void unregisterMBean() throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName nameObj = new ObjectName(mbeanName);
        if (mbs.isRegistered(nameObj))
            mbs.unregisterMBean(nameObj);

        // unregister metrics
        metric.release();
    }


    public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, String columnFamily, boolean loadSSTables)
    {
        return createColumnFamilyStore(keyspace, columnFamily, StorageService.getPartitioner(), Schema.instance.getCFMetaData(keyspace.getName(), columnFamily), loadSSTables);
    }

    public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, String columnFamily, IPartitioner partitioner, CFMetaData metadata)
    {
        return createColumnFamilyStore(keyspace, columnFamily, partitioner, metadata, true);
    }

    private static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                         String columnFamily,
                                                                         IPartitioner partitioner,
                                                                         CFMetaData metadata,
                                                                         boolean loadSSTables)
    {
        // get the max generation number, to prevent generation conflicts
        Directories directories = new Directories(metadata);
        Directories.SSTableLister lister = directories.sstableLister().includeBackups(true);
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

        return new ColumnFamilyStore(keyspace, columnFamily, partitioner, value, metadata, directories, loadSSTables);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public static void scrubDataDirectories(CFMetaData metadata)
    {
        Directories directories = new Directories(metadata);

        // remove any left-behind SSTables from failed/stalled streaming
        FileFilter filter = new FileFilter()
        {
            public boolean accept(File pathname)
            {
                return pathname.getPath().endsWith(StreamLockfile.FILE_EXT);
            }
        };
        for (File dir : directories.getCFDirectories())
        {
            File[] lockfiles = dir.listFiles(filter);
            // lock files can be null if I/O error happens
            if (lockfiles == null || lockfiles.length == 0)
                continue;
            logger.info("Removing SSTables from failed streaming session. Found {} files to cleanup.", lockfiles.length);

            for (File lockfile : lockfiles)
            {
                StreamLockfile streamLockfile = new StreamLockfile(lockfile);
                streamLockfile.cleanup();
                streamLockfile.delete();
            }
        }

        logger.debug("Removing compacted SSTable files from {} (see http://wiki.apache.org/cassandra/MemtableSSTable)", metadata.cfName);

        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : directories.sstableLister().list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            Set<Component> components = sstableFiles.getValue();

            if (desc.type.isTemporary)
            {
                SSTable.delete(desc, components);
                continue;
            }

            File dataFile = new File(desc.filenameFor(Component.DATA));
            if (components.contains(Component.DATA) && dataFile.length() > 0)
                // everything appears to be in order... moving on.
                continue;

            // missing the DATA file! all components are orphaned
            logger.warn("Removing orphans for {}: {}", desc, components);
            for (Component component : components)
            {
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
        for (ColumnDefinition def : metadata.allColumns())
        {
            if (def.isIndexed())
            {
                CellNameType indexComparator = SecondaryIndex.getIndexComparator(metadata, def);
                if (indexComparator != null)
                {
                    CFMetaData indexMetadata = CFMetaData.newIndexMetadata(metadata, def, indexComparator);
                    scrubDataDirectories(indexMetadata);
                }
            }
        }
    }

    /**
     * Replacing compacted sstables is atomic as far as observers of DataTracker are concerned, but not on the
     * filesystem: first the new sstables are renamed to "live" status (i.e., the tmp marker is removed), then
     * their ancestors are removed.
     *
     * If an unclean shutdown happens at the right time, we can thus end up with both the new ones and their
     * ancestors "live" in the system.  This is harmless for normal data, but for counters it can cause overcounts.
     *
     * To prevent this, we record sstables being compacted in the system keyspace.  If we find unfinished
     * compactions, we remove the new ones (since those may be incomplete -- under LCS, we may create multiple
     * sstables from any given ancestor).
     */
    public static void removeUnfinishedCompactionLeftovers(CFMetaData metadata, Map<Integer, UUID> unfinishedCompactions)
    {
        Directories directories = new Directories(metadata);

        Set<Integer> allGenerations = new HashSet<>();
        for (Descriptor desc : directories.sstableLister().list().keySet())
            allGenerations.add(desc.generation);

        // sanity-check unfinishedCompactions
        Set<Integer> unfinishedGenerations = unfinishedCompactions.keySet();
        if (!allGenerations.containsAll(unfinishedGenerations))
        {
            HashSet<Integer> missingGenerations = new HashSet<>(unfinishedGenerations);
            missingGenerations.removeAll(allGenerations);
            logger.debug("Unfinished compactions of {}.{} reference missing sstables of generations {}",
                         metadata.ksName, metadata.cfName, missingGenerations);
        }

        // remove new sstables from compactions that didn't complete, and compute
        // set of ancestors that shouldn't exist anymore
        Set<Integer> completedAncestors = new HashSet<>();
        for (Map.Entry<Descriptor, Set<Component>> sstableFiles : directories.sstableLister().skipTemporary(true).list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();

            Set<Integer> ancestors;
            try
            {
                CompactionMetadata compactionMetadata = (CompactionMetadata) desc.getMetadataSerializer().deserialize(desc, MetadataType.COMPACTION);
                ancestors = compactionMetadata.ancestors;
            }
            catch (IOException e)
            {
                throw new FSReadError(e, desc.filenameFor(Component.STATS));
            }
            catch (NullPointerException e)
            {
                throw new FSReadError(e, "Failed to remove unfinished compaction leftovers (file: " + desc.filenameFor(Component.STATS) + ").  See log for details.");
            }

            if (!ancestors.isEmpty()
                && unfinishedGenerations.containsAll(ancestors)
                && allGenerations.containsAll(ancestors))
            {
                // any of the ancestors would work, so we'll just lookup the compaction task ID with the first one
                UUID compactionTaskID = unfinishedCompactions.get(ancestors.iterator().next());
                assert compactionTaskID != null;
                logger.debug("Going to delete unfinished compaction product {}", desc);
                SSTable.delete(desc, sstableFiles.getValue());
                SystemKeyspace.finishCompaction(compactionTaskID);
            }
            else
            {
                completedAncestors.addAll(ancestors);
            }
        }

        // remove old sstables from compactions that did complete
        for (Map.Entry<Descriptor, Set<Component>> sstableFiles : directories.sstableLister().list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            if (completedAncestors.contains(desc.generation))
            {
                // if any of the ancestors were participating in a compaction, finish that compaction
                logger.debug("Going to delete leftover compaction ancestor {}", desc);
                SSTable.delete(desc, sstableFiles.getValue());
                UUID compactionTaskID = unfinishedCompactions.get(desc.generation);
                if (compactionTaskID != null)
                    SystemKeyspace.finishCompaction(unfinishedCompactions.get(desc.generation));
            }
        }
    }

    // must be called after all sstables are loaded since row cache merges all row versions
    public void initRowCache()
    {
        if (!isRowCacheEnabled())
            return;

        long start = System.nanoTime();

        int cachedRowsRead = CacheService.instance.rowCache.loadSaved(this);
        if (cachedRowsRead > 0)
            logger.info("Completed loading ({} ms; {} keys) row cache for {}.{}",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
                        cachedRowsRead,
                        keyspace.getName(),
                        name);
    }

    public void initCounterCache()
    {
        if (!metadata.isCounter() || CacheService.instance.counterCache.getCapacity() == 0)
            return;

        long start = System.nanoTime();

        int cachedShardsRead = CacheService.instance.counterCache.loadSaved(this);
        if (cachedShardsRead > 0)
            logger.info("Completed loading ({} ms; {} shards) counter cache for {}.{}",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
                        cachedShardsRead,
                        keyspace.getName(),
                        name);
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

        Set<Descriptor> currentDescriptors = new HashSet<Descriptor>();
        for (SSTableReader sstable : data.getView().sstables)
            currentDescriptors.add(sstable.descriptor);
        Set<SSTableReader> newSSTables = new HashSet<>();

        Directories.SSTableLister lister = directories.sstableLister().skipTemporary(true);
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor descriptor = entry.getKey();

            if (currentDescriptors.contains(descriptor))
                continue; // old (initialized) SSTable found, skipping
            if (descriptor.type.isTemporary) // in the process of being written
                continue;

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
                                               Descriptor.Type.FINAL,
                                               descriptor.formatType);
            }
            while (new File(newDescriptor.filenameFor(Component.DATA)).exists());

            logger.info("Renaming new SSTable {} to {}", descriptor, newDescriptor);
            SSTableWriter.rename(descriptor, newDescriptor, entry.getValue());

            SSTableReader reader;
            try
            {
                reader = SSTableReader.open(newDescriptor, entry.getValue(), metadata, partitioner);
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
            indexManager.maybeBuildSecondaryIndexes(newSSTables, indexManager.allIndexesNames());
        }

        logger.info("Done loading load new SSTables for {}/{}", keyspace.getName(), name);
    }

    public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);

        Set<String> indexes = new HashSet<String>(Arrays.asList(idxNames));

        Collection<SSTableReader> sstables = cfs.getSSTables();

        try (Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            cfs.indexManager.setIndexRemoved(indexes);
            logger.info(String.format("User Requested secondary index re-build for %s/%s indexes", ksName, cfName));
            cfs.indexManager.maybeBuildSecondaryIndexes(sstables, indexes);
            cfs.indexManager.setIndexBuilt(indexes);
        }
    }

    public String getColumnFamilyName()
    {
        return name;
    }

    public String getTempSSTablePath(File directory)
    {
        return getTempSSTablePath(directory, DatabaseDescriptor.getSSTableFormat().info.getLatestVersion(), DatabaseDescriptor.getSSTableFormat());
    }

    public String getTempSSTablePath(File directory, SSTableFormat.Type format)
    {
        return getTempSSTablePath(directory, format.info.getLatestVersion(), format);
    }

    private String getTempSSTablePath(File directory, Version version, SSTableFormat.Type format)
    {
        Descriptor desc = new Descriptor(version,
                                         directory,
                                         keyspace.getName(),
                                         name,
                                         fileIndexGenerator.incrementAndGet(),
                                         Descriptor.Type.TEMP,
                                         format);
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
     * This method does not block except for synchronizing on DataTracker, but the Future it returns will
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
        Memtable memtable = getDataTracker().getView().getCurrentMemtable();
        onHeapRatio +=  memtable.getAllocator().onHeap().ownershipRatio();
        offHeapRatio += memtable.getAllocator().offHeap().ownershipRatio();
        onHeapTotal += memtable.getAllocator().onHeap().owns();
        offHeapTotal += memtable.getAllocator().offHeap().owns();

        for (SecondaryIndex index : indexManager.getIndexes())
        {
            if (index.getIndexCfs() != null)
            {
                MemtableAllocator allocator = index.getIndexCfs().getDataTracker().getView().getCurrentMemtable().getAllocator();
                onHeapRatio += allocator.onHeap().ownershipRatio();
                offHeapRatio += allocator.offHeap().ownershipRatio();
                onHeapTotal += allocator.onHeap().owns();
                offHeapTotal += allocator.offHeap().owns();
            }
        }

        logger.info("Enqueuing flush of {}: {}", name, String.format("%d (%.0f%%) on-heap, %d (%.0f%%) off-heap",
                                                                     onHeapTotal, onHeapRatio * 100, offHeapTotal, offHeapRatio * 100));
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
                        logger.debug("forceFlush requested but everything is clean in {}", name);
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
            {
                for (SecondaryIndex index : indexManager.getIndexesNotBackedByCfs())
                {
                    // flush any non-cfs backed indexes
                    logger.info("Flushing SecondaryIndex {}", index);
                    index.forceBlockingFlush();
                }
            }

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
            if (lastReplayPosition != null)
            {
                CommitLog.instance.discardCompletedSegments(metadata.cfId, lastReplayPosition);
            }

            metric.pendingFlushes.dec();
        }
    }

    /**
     * Should only be constructed/used from switchMemtable() or truncate(), with ownership of the DataTracker monitor.
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
                    memtable.setDiscarded();
                    iter.remove();
                }
            }

            if (memtables.isEmpty())
            {
                postFlush.latch.countDown();
                return;
            }

            metric.memtableSwitchCount.inc();

            for (final Memtable memtable : memtables)
            {
                // flush the memtable
                MoreExecutors.sameThreadExecutor().execute(memtable.flushRunnable());

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

            // signal the post-flush we've done our work
            postFlush.latch.countDown();
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
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            {
                // we take a reference to the current main memtable for the CF prior to snapping its ownership ratios
                // to ensure we have some ordering guarantee for performing the switchMemtableIf(), i.e. we will only
                // swap if the memtables we are measuring here haven't already been swapped by the time we try to swap them
                Memtable current = cfs.getDataTracker().getView().getCurrentMemtable();

                // find the total ownership ratio for the memtable and all SecondaryIndexes owned by this CF,
                // both on- and off-heap, and select the largest of the two ratios to weight this CF
                float onHeap = 0f, offHeap = 0f;
                onHeap += current.getAllocator().onHeap().ownershipRatio();
                offHeap += current.getAllocator().offHeap().ownershipRatio();

                for (SecondaryIndex index : cfs.indexManager.getIndexes())
                {
                    if (index.getIndexCfs() != null)
                    {
                        MemtableAllocator allocator = index.getIndexCfs().getDataTracker().getView().getCurrentMemtable().getAllocator();
                        onHeap += allocator.onHeap().ownershipRatio();
                        offHeap += allocator.offHeap().ownershipRatio();
                    }
                }

                float ratio = Math.max(onHeap, offHeap);

                if (ratio > largestRatio)
                {
                    largest = current;
                    largestRatio = ratio;
                }
            }

            if (largest != null)
                largest.cfs.switchMemtableIfCurrent(largest);
        }
    }

    public void maybeUpdateRowCache(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return;

        RowCacheKey cacheKey = new RowCacheKey(metadata.cfId, key);
        invalidateCachedRow(cacheKey);
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Keyspace.switchLock
     * param @ lock - lock that needs to be used.
     * param @ key - key for update/insert
     * param @ columnFamily - columnFamily changes
     */
    public void apply(DecoratedKey key, ColumnFamily columnFamily, SecondaryIndexManager.Updater indexer, OpOrder.Group opGroup, ReplayPosition replayPosition)
    {
        long start = System.nanoTime();

        Memtable mt = data.getMemtableFor(opGroup, replayPosition);
        final long timeDelta = mt.put(key, columnFamily, indexer, opGroup);
        maybeUpdateRowCache(key);
        metric.samplers.get(Sampler.WRITES).addSample(key.getKey(), key.hashCode(), 1);
        metric.writeLatency.addNano(System.nanoTime() - start);
        if(timeDelta < Long.MAX_VALUE)
            metric.colUpdateTimeDeltaHistogram.update(timeDelta);
    }

    /**
     * Purges gc-able top-level and range tombstones, returning `cf` if there are any columns or tombstones left,
     * null otherwise.
     * @param gcBefore a timestamp (in seconds); tombstones with a localDeletionTime before this will be purged
     */
    public static ColumnFamily removeDeletedCF(ColumnFamily cf, int gcBefore)
    {
        // purge old top-level and range tombstones
        cf.purgeTombstones(gcBefore);

        // if there are no columns or tombstones left, return null
        return !cf.hasColumns() && !cf.isMarkedForDelete() ? null : cf;
    }

    /**
     * Removes deleted columns and purges gc-able tombstones.
     * @return an updated `cf` if any columns or tombstones remain, null otherwise
     */
    public static ColumnFamily removeDeleted(ColumnFamily cf, int gcBefore)
    {
        return removeDeleted(cf, gcBefore, SecondaryIndexManager.nullUpdater);
    }

    /*
     This is complicated because we need to preserve deleted columns and columnfamilies
     until they have been deleted for at least GC_GRACE_IN_SECONDS.  But, we do not need to preserve
     their contents; just the object itself as a "tombstone" that can be used to repair other
     replicas that do not know about the deletion.
     */
    public static ColumnFamily removeDeleted(ColumnFamily cf, int gcBefore, SecondaryIndexManager.Updater indexer)
    {
        if (cf == null)
        {
            return null;
        }

        return removeDeletedCF(removeDeletedColumnsOnly(cf, gcBefore, indexer), gcBefore);
    }

    /**
     * Removes only per-cell tombstones, cells that are shadowed by a row-level or range tombstone, or
     * columns that have been dropped from the schema (for CQL3 tables only).
     * @return the updated ColumnFamily
     */
    public static ColumnFamily removeDeletedColumnsOnly(ColumnFamily cf, int gcBefore, SecondaryIndexManager.Updater indexer)
    {
        BatchRemoveIterator<Cell> iter = cf.batchRemoveIterator();
        DeletionInfo.InOrderTester tester = cf.inOrderDeletionTester();
        boolean hasDroppedColumns = !cf.metadata.getDroppedColumns().isEmpty();
        while (iter.hasNext())
        {
            Cell c = iter.next();
            // remove columns if
            // (a) the column itself is gcable or
            // (b) the column is shadowed by a CF tombstone
            // (c) the column has been dropped from the CF schema (CQL3 tables only)
            if (c.getLocalDeletionTime() < gcBefore || tester.isDeleted(c) || (hasDroppedColumns && isDroppedColumn(c, cf.metadata())))
            {
                iter.remove();
                indexer.remove(c);
            }
        }
        iter.commit();
        return cf;
    }

    // returns true if
    // 1. this column has been dropped from schema and
    // 2. if it has been re-added since then, this particular column was inserted before the last drop
    private static boolean isDroppedColumn(Cell c, CFMetaData meta)
    {
        Long droppedAt = meta.getDroppedColumns().get(c.name().cql3ColumnName(meta));
        return droppedAt != null && c.timestamp() <= droppedAt;
    }

    private void removeDroppedColumns(ColumnFamily cf)
    {
        if (cf == null || cf.metadata.getDroppedColumns().isEmpty())
            return;

        BatchRemoveIterator<Cell> iter = cf.batchRemoveIterator();
        while (iter.hasNext())
            if (isDroppedColumn(iter.next(), metadata))
                iter.remove();
        iter.commit();
    }

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    public Collection<SSTableReader> getOverlappingSSTables(Iterable<SSTableReader> sstables)
    {
        logger.debug("Checking for sstables overlapping {}", sstables);

        // a normal compaction won't ever have an empty sstables list, but we create a skeleton
        // compaction controller for streaming, and that passes an empty list.
        if (!sstables.iterator().hasNext())
            return ImmutableSet.of();

        DataTracker.SSTableIntervalTree tree = data.getView().intervalTree;

        Set<SSTableReader> results = null;
        for (SSTableReader sstable : sstables)
        {
            Set<SSTableReader> overlaps = ImmutableSet.copyOf(tree.search(Interval.<RowPosition, SSTableReader>create(sstable.first, sstable.last)));
            results = results == null ? overlaps : Sets.union(results, overlaps).immutableCopy();
        }
        results = Sets.difference(results, ImmutableSet.copyOf(sstables));

        return results;
    }

    /**
     * like getOverlappingSSTables, but acquires references before returning
     */
    public Refs<SSTableReader> getAndReferenceOverlappingSSTables(Iterable<SSTableReader> sstables)
    {
        while (true)
        {
            Iterable<SSTableReader> overlapped = getOverlappingSSTables(sstables);
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

    public CompactionManager.AllSSTableOpStatus forceCleanup() throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performCleanup(ColumnFamilyStore.this);
    }

    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted) throws ExecutionException, InterruptedException
    {
        // skip snapshot creation during scrub, SEE JIRA 5891
        if(!disableSnapshot)
            snapshotWithoutFlush("pre-scrub-" + System.currentTimeMillis());
        return CompactionManager.instance.performScrub(ColumnFamilyStore.this, skipCorrupted);
    }

    public CompactionManager.AllSSTableOpStatus sstablesRewrite(boolean excludeCurrentVersion) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performSSTableRewrite(ColumnFamilyStore.this, excludeCurrentVersion);
    }

    public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        assert !sstables.isEmpty();
        data.markObsolete(sstables, compactionType);
    }

    void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        compactionStrategyWrapper.replaceFlushed(memtable, sstable);
    }

    public boolean isValid()
    {
        return valid;
    }




    /**
     * Package protected for access from the CompactionManager.
     */
    public DataTracker getDataTracker()
    {
        return data;
    }

    public Collection<SSTableReader> getSSTables()
    {
        return data.getSSTables();
    }

    public Set<SSTableReader> getUncompactingSSTables()
    {
        return data.getUncompactingSSTables();
    }

    public ColumnFamily getColumnFamily(DecoratedKey key,
                                        Composite start,
                                        Composite finish,
                                        boolean reversed,
                                        int limit,
                                        long timestamp)
    {
        return getColumnFamily(QueryFilter.getSliceFilter(key, name, start, finish, reversed, limit, timestamp));
    }

    /**
     * Fetch the row and columns given by filter.key if it is in the cache; if not, read it from disk and cache it
     *
     * If row is cached, and the filter given is within its bounds, we return from cache, otherwise from disk
     *
     * If row is not cached, we figure out what filter is "biggest", read that from disk, then
     * filter the result and either cache that or return it.
     *
     * @param cfId the column family to read the row from
     * @param filter the columns being queried.
     * @return the requested data for the filter provided
     */
    private ColumnFamily getThroughCache(UUID cfId, QueryFilter filter)
    {
        assert isRowCacheEnabled()
               : String.format("Row cache is not enabled on table [" + name + "]");

        RowCacheKey key = new RowCacheKey(cfId, filter.key);

        // attempt a sentinel-read-cache sequence.  if a write invalidates our sentinel, we'll return our
        // (now potentially obsolete) data, but won't cache it. see CASSANDRA-3862
        // TODO: don't evict entire rows on writes (#2864)
        IRowCacheEntry cached = CacheService.instance.rowCache.get(key);
        if (cached != null)
        {
            if (cached instanceof RowCacheSentinel)
            {
                // Some other read is trying to cache the value, just do a normal non-caching read
                Tracing.trace("Row cache miss (race)");
                metric.rowCacheMiss.inc();
                return getTopLevelColumns(filter, Integer.MIN_VALUE);
            }

            ColumnFamily cachedCf = (ColumnFamily)cached;
            if (isFilterFullyCoveredBy(filter.filter, cachedCf, filter.timestamp))
            {
                metric.rowCacheHit.inc();
                Tracing.trace("Row cache hit");
                return filterColumnFamily(cachedCf, filter);
            }

            metric.rowCacheHitOutOfRange.inc();
            Tracing.trace("Ignoring row cache as cached value could not satisfy query");
            return getTopLevelColumns(filter, Integer.MIN_VALUE);
        }

        metric.rowCacheMiss.inc();
        Tracing.trace("Row cache miss");
        RowCacheSentinel sentinel = new RowCacheSentinel();
        boolean sentinelSuccess = CacheService.instance.rowCache.putIfAbsent(key, sentinel);
        ColumnFamily data = null;
        ColumnFamily toCache = null;
        try
        {
            // If we are explicitely asked to fill the cache with full partitions, we go ahead and query the whole thing
            if (metadata.getCaching().rowCache.cacheFullPartitions())
            {
                data = getTopLevelColumns(QueryFilter.getIdentityFilter(filter.key, name, filter.timestamp), Integer.MIN_VALUE);
                toCache = data;
                Tracing.trace("Populating row cache with the whole partition");
                if (sentinelSuccess && toCache != null)
                    CacheService.instance.rowCache.replace(key, sentinel, toCache);
                return filterColumnFamily(data, filter);
            }

            // Otherwise, if we want to cache the result of the query we're about to do, we must make sure this query
            // covers what needs to be cached. And if the user filter does not satisfy that, we sometimes extend said
            // filter so we can populate the cache but only if:
            //   1) we can guarantee it is a strict extension, i.e. that we will still fetch the data asked by the user.
            //   2) the extension does not make us query more than getRowsPerPartitionToCache() (as a mean to limit the
            //      amount of extra work we'll do on a user query for the purpose of populating the cache).
            //
            // In practice, we can only guarantee those 2 points if the filter is one that queries the head of the
            // partition (and if that filter actually counts CQL3 rows since that's what we cache and it would be
            // bogus to compare the filter count to the 'rows to cache' otherwise).
            if (filter.filter.isHeadFilter() && filter.filter.countCQL3Rows(metadata.comparator))
            {
                SliceQueryFilter sliceFilter = (SliceQueryFilter)filter.filter;
                int rowsToCache = metadata.getCaching().rowCache.rowsToCache;

                SliceQueryFilter cacheSlice = readFilterForCache();
                QueryFilter cacheFilter = new QueryFilter(filter.key, name, cacheSlice, filter.timestamp);

                // If the filter count is less than the number of rows cached, we simply extend it to make sure we do cover the
                // number of rows to cache, and if that count is greater than the number of rows to cache, we simply filter what
                // needs to be cached afterwards.
                if (sliceFilter.count < rowsToCache)
                {
                    toCache = getTopLevelColumns(cacheFilter, Integer.MIN_VALUE);
                    if (toCache != null)
                    {
                        Tracing.trace("Populating row cache ({} rows cached)", cacheSlice.lastCounted());
                        data = filterColumnFamily(toCache, filter);
                    }
                }
                else
                {
                    data = getTopLevelColumns(filter, Integer.MIN_VALUE);
                    if (data != null)
                    {
                        // The filter limit was greater than the number of rows to cache. But, if the filter had a non-empty
                        // finish bound, we may have gotten less than what needs to be cached, in which case we shouldn't cache it
                        // (otherwise a cache hit would assume the whole partition is cached which is not the case).
                        if (sliceFilter.finish().isEmpty() || sliceFilter.lastCounted() >= rowsToCache)
                        {
                            toCache = filterColumnFamily(data, cacheFilter);
                            Tracing.trace("Caching {} rows (out of {} requested)", cacheSlice.lastCounted(), sliceFilter.count);
                        }
                        else
                        {
                            Tracing.trace("Not populating row cache, not enough rows fetched ({} fetched but {} required for the cache)", sliceFilter.lastCounted(), rowsToCache);
                        }
                    }
                }

                if (sentinelSuccess && toCache != null)
                    CacheService.instance.rowCache.replace(key, sentinel, toCache);
                return data;
            }
            else
            {
                Tracing.trace("Fetching data but not populating cache as query does not query from the start of the partition");
                return getTopLevelColumns(filter, Integer.MIN_VALUE);
            }
        }
        finally
        {
            if (sentinelSuccess && toCache == null)
                invalidateCachedRow(key);
        }
    }

    public SliceQueryFilter readFilterForCache()
    {
        // We create a new filter everytime before for now SliceQueryFilter is unfortunatly mutable.
        return new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, metadata.getCaching().rowCache.rowsToCache, metadata.clusteringColumns().size());
    }

    public boolean isFilterFullyCoveredBy(IDiskAtomFilter filter, ColumnFamily cachedCf, long now)
    {
        // We can use the cached value only if we know that no data it doesn't contain could be covered
        // by the query filter, that is if:
        //   1) either the whole partition is cached
        //   2) or we can ensure than any data the filter selects are in the cached partition

        // When counting rows to decide if the whole row is cached, we should be careful with expiring
        // columns: if we use a timestamp newer than the one that was used when populating the cache, we might
        // end up deciding the whole partition is cached when it's really not (just some rows expired since the
        // cf was cached). This is the reason for Integer.MIN_VALUE below.
        boolean wholePartitionCached = cachedCf.liveCQL3RowCount(Integer.MIN_VALUE) < metadata.getCaching().rowCache.rowsToCache;

        // Contrarily to the "wholePartitionCached" check above, we do want isFullyCoveredBy to take the
        // timestamp of the query into account when dealing with expired columns. Otherwise, we could think
        // the cached partition has enough live rows to satisfy the filter when it doesn't because some
        // are now expired.
        return wholePartitionCached || filter.isFullyCoveredBy(cachedCf, now);
    }

    public int gcBefore(long now)
    {
        return (int) (now / 1000) - metadata.getGcGraceSeconds();
    }

    /**
     * get a list of columns starting from a given column, in a specified order.
     * only the latest version of a column is returned.
     * @return null if there is no data and no tombstones; otherwise a ColumnFamily
     */
    public ColumnFamily getColumnFamily(QueryFilter filter)
    {
        assert name.equals(filter.getColumnFamilyName()) : filter.getColumnFamilyName();

        ColumnFamily result = null;

        long start = System.nanoTime();
        try
        {
            int gcBefore = gcBefore(filter.timestamp);
            if (isRowCacheEnabled())
            {
                assert !isIndex(); // CASSANDRA-5732
                UUID cfId = metadata.cfId;

                ColumnFamily cached = getThroughCache(cfId, filter);
                if (cached == null)
                {
                    logger.trace("cached row is empty");
                    return null;
                }

                result = cached;
            }
            else
            {
                ColumnFamily cf = getTopLevelColumns(filter, gcBefore);

                if (cf == null)
                    return null;

                result = removeDeletedCF(cf, gcBefore);
            }

            removeDroppedColumns(result);

            if (filter.filter instanceof SliceQueryFilter)
            {
                // Log the number of tombstones scanned on single key queries
                metric.tombstoneScannedHistogram.update(((SliceQueryFilter) filter.filter).lastIgnored());
                metric.liveScannedHistogram.update(((SliceQueryFilter) filter.filter).lastLive());
            }
        }
        finally
        {
            metric.readLatency.addNano(System.nanoTime() - start);
        }

        return result;
    }

    /**
     *  Filter a cached row, which will not be modified by the filter, but may be modified by throwing out
     *  tombstones that are no longer relevant.
     *  The returned column family won't be thread safe.
     */
    ColumnFamily filterColumnFamily(ColumnFamily cached, QueryFilter filter)
    {
        if (cached == null)
            return null;

        ColumnFamily cf = cached.cloneMeShallow(ArrayBackedSortedColumns.factory, filter.filter.isReversed());
        int gcBefore = gcBefore(filter.timestamp);
        filter.collateOnDiskAtom(cf, filter.getIterator(cached), gcBefore);
        return removeDeletedCF(cf, gcBefore);
    }

    public Set<SSTableReader> getUnrepairedSSTables()
    {
        Set<SSTableReader> unRepairedSSTables = new HashSet<>(getSSTables());
        Iterator<SSTableReader> sstableIterator = unRepairedSSTables.iterator();
        while(sstableIterator.hasNext())
        {
            SSTableReader sstable = sstableIterator.next();
            if (sstable.isRepaired())
                sstableIterator.remove();
        }
        return unRepairedSSTables;
    }

    public Set<SSTableReader> getRepairedSSTables()
    {
        Set<SSTableReader> repairedSSTables = new HashSet<>(getSSTables());
        Iterator<SSTableReader> sstableIterator = repairedSSTables.iterator();
        while(sstableIterator.hasNext())
        {
            SSTableReader sstable = sstableIterator.next();
            if (!sstable.isRepaired())
                sstableIterator.remove();
        }
        return repairedSSTables;
    }

    public RefViewFragment selectAndReference(Function<DataTracker.View, List<SSTableReader>> filter)
    {
        while (true)
        {
            ViewFragment view = select(filter);
            Refs<SSTableReader> refs = Refs.tryRef(view.sstables);
            if (refs != null)
                return new RefViewFragment(view.sstables, view.memtables, refs);
        }
    }

    public ViewFragment select(Function<DataTracker.View, List<SSTableReader>> filter)
    {
        DataTracker.View view = data.getView();
        List<SSTableReader> sstables = view.intervalTree.isEmpty()
                                       ? Collections.<SSTableReader>emptyList()
                                       : filter.apply(view);
        return new ViewFragment(sstables, view.getAllMemtables());
    }


    /**
     * @return a ViewFragment containing the sstables and memtables that may need to be merged
     * for the given @param key, according to the interval tree
     */
    public Function<DataTracker.View, List<SSTableReader>> viewFilter(final DecoratedKey key)
    {
        assert !key.isMinimum();
        return new Function<DataTracker.View, List<SSTableReader>>()
        {
            public List<SSTableReader> apply(DataTracker.View view)
            {
                return compactionStrategyWrapper.filterSSTablesForReads(view.intervalTree.search(key));
            }
        };
    }

    /**
     * @return a ViewFragment containing the sstables and memtables that may need to be merged
     * for rows within @param rowBounds, inclusive, according to the interval tree.
     */
    public Function<DataTracker.View, List<SSTableReader>> viewFilter(final AbstractBounds<RowPosition> rowBounds)
    {
        return new Function<DataTracker.View, List<SSTableReader>>()
        {
            public List<SSTableReader> apply(DataTracker.View view)
            {
                return compactionStrategyWrapper.filterSSTablesForReads(view.sstablesInBounds(rowBounds));
            }
        };
    }

    /**
     * @return a ViewFragment containing the sstables and memtables that may need to be merged
     * for rows for all of @param rowBoundsCollection, inclusive, according to the interval tree.
     */
    public Function<DataTracker.View, List<SSTableReader>> viewFilter(final Collection<AbstractBounds<RowPosition>> rowBoundsCollection)
    {
        return new Function<DataTracker.View, List<SSTableReader>>()
        {
            public List<SSTableReader> apply(DataTracker.View view)
            {
                Set<SSTableReader> sstables = Sets.newHashSet();
                for (AbstractBounds<RowPosition> rowBounds : rowBoundsCollection)
                    sstables.addAll(view.sstablesInBounds(rowBounds));

                return ImmutableList.copyOf(sstables);
            }
        };
    }

    public List<String> getSSTablesForKey(String key)
    {
        DecoratedKey dk = partitioner.decorateKey(metadata.getKeyValidator().fromString(key));
        try (OpOrder.Group op = readOrdering.start())
        {
            List<String> files = new ArrayList<>();
            for (SSTableReader sstr : select(viewFilter(dk)).sstables)
            {
                // check if the key actually exists in this sstable, without updating cache and stats
                if (sstr.getPosition(dk, SSTableReader.Operator.EQ, false) != null)
                    files.add(sstr.getFilename());
            }
            return files;
        }
    }

    public ColumnFamily getTopLevelColumns(QueryFilter filter, int gcBefore)
    {
        Tracing.trace("Executing single-partition query on {}", name);
        CollationController controller = new CollationController(this, filter, gcBefore);
        ColumnFamily columns;
        try (OpOrder.Group op = readOrdering.start())
        {
            columns = controller.getTopLevelColumns(Memtable.MEMORY_POOL.needToCopyOnHeap());
        }
        if (columns != null)
            metric.samplers.get(Sampler.READS).addSample(filter.key.getKey(), filter.key.hashCode(), 1);
        metric.updateSSTableIterated(controller.getSstablesIterated());
        return columns;
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
            byte[] key = counter.getItem().array();
            result.put(new CompositeDataSupport(COUNTER_COMPOSITE_TYPE, COUNTER_NAMES, new Object[] {
                    Hex.bytesToHex(key), // raw
                    counter.getCount(),  // count
                    counter.getError(),  // error
                    metadata.getKeyValidator().getString(ByteBuffer.wrap(key)) })); // string
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
            DecoratedKey dk = partitioner.decorateKey(ByteBuffer.wrap(key.key));
            if (key.cfId.equals(metadata.cfId) && !Range.isInRanges(dk.getToken(), ranges))
                invalidateCachedRow(dk);
        }

        if (metadata.isCounter())
        {
            for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
                 keyIter.hasNext(); )
            {
                CounterCacheKey key = keyIter.next();
                DecoratedKey dk = partitioner.decorateKey(ByteBuffer.wrap(key.partitionKey));
                if (key.cfId.equals(metadata.cfId) && !Range.isInRanges(dk.getToken(), ranges))
                    CacheService.instance.counterCache.remove(key);
            }
        }
    }

    public static abstract class AbstractScanIterator extends AbstractIterator<Row> implements CloseableIterator<Row>
    {
        public boolean needsFiltering()
        {
            return true;
        }
    }

    /**
      * Iterate over a range of rows and columns from memtables/sstables.
      *
      * @param range The range of keys and columns within those keys to fetch
     */
    private AbstractScanIterator getSequentialIterator(final DataRange range, long now)
    {
        assert !(range.keyRange() instanceof Range) || !((Range<?>)range.keyRange()).isWrapAround() || range.keyRange().right.isMinimum() : range.keyRange();

        final ViewFragment view = select(viewFilter(range.keyRange()));
        Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), range.keyRange().getString(metadata.getKeyValidator()));

        final CloseableIterator<Row> iterator = RowIteratorFactory.getIterator(view.memtables, view.sstables, range, this, now);

        // todo this could be pushed into SSTableScanner
        return new AbstractScanIterator()
        {
            protected Row computeNext()
            {
                // pull a row out of the iterator
                if (!iterator.hasNext())
                    return endOfData();

                Row current = iterator.next();
                DecoratedKey key = current.key;

                if (!range.stopKey().isMinimum() && range.stopKey().compareTo(key) < 0)
                    return endOfData();

                // skipping outside of assigned range
                if (!range.contains(key))
                    return computeNext();

                if (logger.isTraceEnabled())
                    logger.trace("scanned {}", metadata.getKeyValidator().getString(key.getKey()));

                return current;
            }

            public void close() throws IOException
            {
                iterator.close();
            }
        };
    }

    @VisibleForTesting
    public List<Row> getRangeSlice(final AbstractBounds<RowPosition> range,
                                   List<IndexExpression> rowFilter,
                                   IDiskAtomFilter columnFilter,
                                   int maxResults)
    {
        return getRangeSlice(range, rowFilter, columnFilter, maxResults, System.currentTimeMillis());
    }

    public List<Row> getRangeSlice(final AbstractBounds<RowPosition> range,
                                   List<IndexExpression> rowFilter,
                                   IDiskAtomFilter columnFilter,
                                   int maxResults,
                                   long now)
    {
        return getRangeSlice(makeExtendedFilter(range, columnFilter, rowFilter, maxResults, false, false, now));
    }

    /**
     * Allows generic range paging with the slice column filter.
     * Typically, suppose we have rows A, B, C ... Z having each some columns in [1, 100].
     * And suppose we want to page through the query that for all rows returns the columns
     * within [25, 75]. For that, we need to be able to do a range slice starting at (row r, column c)
     * and ending at (row Z, column 75), *but* that only return columns in [25, 75].
     * That is what this method allows. The columnRange is the "window" of  columns we are interested
     * in each row, and columnStart (resp. columnEnd) is the start (resp. end) for the first
     * (resp. last) requested row.
     */
    public ExtendedFilter makeExtendedFilter(AbstractBounds<RowPosition> keyRange,
                                             SliceQueryFilter columnRange,
                                             Composite columnStart,
                                             Composite columnStop,
                                             List<IndexExpression> rowFilter,
                                             int maxResults,
                                             boolean countCQL3Rows,
                                             long now)
    {
        DataRange dataRange = new DataRange.Paging(keyRange, columnRange, columnStart, columnStop, metadata.comparator);
        return ExtendedFilter.create(this, dataRange, rowFilter, maxResults, countCQL3Rows, now);
    }

    public List<Row> getRangeSlice(AbstractBounds<RowPosition> range,
                                   List<IndexExpression> rowFilter,
                                   IDiskAtomFilter columnFilter,
                                   int maxResults,
                                   long now,
                                   boolean countCQL3Rows,
                                   boolean isPaging)
    {
        return getRangeSlice(makeExtendedFilter(range, columnFilter, rowFilter, maxResults, countCQL3Rows, isPaging, now));
    }

    public ExtendedFilter makeExtendedFilter(AbstractBounds<RowPosition> range,
                                             IDiskAtomFilter columnFilter,
                                             List<IndexExpression> rowFilter,
                                             int maxResults,
                                             boolean countCQL3Rows,
                                             boolean isPaging,
                                             long timestamp)
    {
        DataRange dataRange;
        if (isPaging)
        {
            assert columnFilter instanceof SliceQueryFilter;
            SliceQueryFilter sfilter = (SliceQueryFilter)columnFilter;
            assert sfilter.slices.length == 1;
            // create a new SliceQueryFilter that selects all cells, but pass the original slice start and finish
            // through to DataRange.Paging to be used on the first and last partitions
            SliceQueryFilter newFilter = new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, sfilter.isReversed(), sfilter.count);
            dataRange = new DataRange.Paging(range, newFilter, sfilter.start(), sfilter.finish(), metadata.comparator);
        }
        else
        {
            dataRange = new DataRange(range, columnFilter);
        }
        return ExtendedFilter.create(this, dataRange, rowFilter, maxResults, countCQL3Rows, timestamp);
    }

    public List<Row> getRangeSlice(ExtendedFilter filter)
    {
        long start = System.nanoTime();
        try (OpOrder.Group op = readOrdering.start())
        {
            return filter(getSequentialIterator(filter.dataRange, filter.timestamp), filter);
        }
        finally
        {
            metric.rangeLatency.addNano(System.nanoTime() - start);
        }
    }

    @VisibleForTesting
    public List<Row> search(AbstractBounds<RowPosition> range,
                            List<IndexExpression> clause,
                            IDiskAtomFilter dataFilter,
                            int maxResults)
    {
        return search(range, clause, dataFilter, maxResults, System.currentTimeMillis());
    }

    public List<Row> search(AbstractBounds<RowPosition> range,
                            List<IndexExpression> clause,
                            IDiskAtomFilter dataFilter,
                            int maxResults,
                            long now)
    {
        return search(makeExtendedFilter(range, dataFilter, clause, maxResults, false, false, now));
    }

    public List<Row> search(ExtendedFilter filter)
    {
        Tracing.trace("Executing indexed scan for {}", filter.dataRange.keyRange().getString(metadata.getKeyValidator()));
        return indexManager.search(filter);
    }

    public List<Row> filter(AbstractScanIterator rowIterator, ExtendedFilter filter)
    {
        logger.trace("Filtering {} for rows matching {}", rowIterator, filter);
        List<Row> rows = new ArrayList<Row>();
        int columnsCount = 0;
        int total = 0, matched = 0;
        boolean ignoreTombstonedPartitions = filter.ignoreTombstonedPartitions();

        try
        {
            while (rowIterator.hasNext() && matched < filter.maxRows() && columnsCount < filter.maxColumns())
            {
                // get the raw columns requested, and additional columns for the expressions if necessary
                Row rawRow = rowIterator.next();
                total++;
                ColumnFamily data = rawRow.cf;

                if (rowIterator.needsFiltering())
                {
                    IDiskAtomFilter extraFilter = filter.getExtraFilter(rawRow.key, data);
                    if (extraFilter != null)
                    {
                        ColumnFamily cf = filter.cfs.getColumnFamily(new QueryFilter(rawRow.key, name, extraFilter, filter.timestamp));
                        if (cf != null)
                            data.addAll(cf);
                    }

                    removeDroppedColumns(data);

                    if (!filter.isSatisfiedBy(rawRow.key, data, null, null))
                        continue;

                    logger.trace("{} satisfies all filter expressions", data);
                    // cut the resultset back to what was requested, if necessary
                    data = filter.prune(rawRow.key, data);
                }
                else
                {
                    removeDroppedColumns(data);
                }

                rows.add(new Row(rawRow.key, data));
                if (!ignoreTombstonedPartitions || !data.hasOnlyTombstones(filter.timestamp))
                    matched++;

                if (data != null)
                    columnsCount += filter.lastCounted(data);
                // Update the underlying filter to avoid querying more columns per slice than necessary and to handle paging
                filter.updateFilter(columnsCount);
            }

            return rows;
        }
        finally
        {
            try
            {
                rowIterator.close();
                Tracing.trace("Scanned {} rows and matched {}", total, matched);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public CellNameType getComparator()
    {
        return metadata.comparator;
    }

    public void snapshotWithoutFlush(String snapshotName)
    {
        snapshotWithoutFlush(snapshotName, null);
    }

    public Set<SSTableReader> snapshotWithoutFlush(String snapshotName, Predicate<SSTableReader> predicate)
    {
        Set<SSTableReader> snapshottedSSTables = new HashSet<>();
        for (ColumnFamilyStore cfs : concatWithIndexes())
        {
            final JSONArray filesJSONArr = new JSONArray();
            try (RefViewFragment currentView = cfs.selectAndReference(ALL_SSTABLES))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    if (ssTable.openReason == SSTableReader.OpenReason.EARLY || (predicate != null && !predicate.apply(ssTable)))
                    {
                        continue;
                    }

                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.getPath()); // hard links
                    filesJSONArr.add(ssTable.descriptor.relativeFilenameFor(Component.DATA));
                    if (logger.isDebugEnabled())
                        logger.debug("Snapshot for {} keyspace data file {} created in {}", keyspace, ssTable.getFilename(), snapshotDirectory);
                    snapshottedSSTables.add(ssTable);
                }

                writeSnapshotManifest(filesJSONArr, snapshotName);
            }
        }
        return snapshottedSSTables;
    }

    private void writeSnapshotManifest(final JSONArray filesJSONArr, final String snapshotName)
    {
        final File manifestFile = directories.getSnapshotManifestFile(snapshotName);

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

    public Refs<SSTableReader> getSnapshotSSTableReader(String tag) throws IOException
    {
        Map<Integer, SSTableReader> active = new HashMap<>();
        for (SSTableReader sstable : data.getView().sstables)
            active.put(sstable.descriptor.generation, sstable);
        Map<Descriptor, Set<Component>> snapshots = directories.sstableLister().snapshots(tag).list();
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
                    if (logger.isDebugEnabled())
                        logger.debug("using snapshot sstable {}", entries.getKey());
                    sstable = SSTableReader.open(entries.getKey(), entries.getValue(), metadata, partitioner);
                    // This is technically not necessary since it's a snapshot but makes things easier
                    refs.tryRef(sstable);
                }
                else if (logger.isDebugEnabled())
                {
                    logger.debug("using active sstable {}", entries.getKey());
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
        return snapshot(snapshotName, null);
    }

    public Set<SSTableReader> snapshot(String snapshotName, Predicate<SSTableReader> predicate)
    {
        forceBlockingFlush();
        return snapshotWithoutFlush(snapshotName, predicate);
    }

    public boolean snapshotExists(String snapshotName)
    {
        return directories.snapshotExists(snapshotName);
    }

    public long getSnapshotCreationTime(String snapshotName)
    {
        return directories.snapshotCreationTime(snapshotName);
    }

    /**
     * Clear all the snapshots for a given column family.
     *
     * @param snapshotName the user supplied snapshot name. If left empty,
     *                     all the snapshots will be cleaned.
     */
    public void clearSnapshot(String snapshotName)
    {
        List<File> snapshotDirs = directories.getCFDirectories();
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }
    /**
     *
     * @return  Return a map of all snapshots to space being used
     * The pair for a snapshot has true size and size on disk.
     */
    public Map<String, Pair<Long,Long>> getSnapshotDetails()
    {
        return directories.getSnapshotDetails();
    }

    public boolean hasUnreclaimedSpace()
    {
        return metric.liveDiskSpaceUsed.getCount() < metric.totalDiskSpaceUsed.getCount();
    }

    /**
     * @return the cached row for @param key if it is already present in the cache.
     * That is, unlike getThroughCache, it will not readAndCache the row if it is not present, nor
     * are these calls counted in cache statistics.
     *
     * Note that this WILL cause deserialization of a SerializingCache row, so if all you
     * need to know is whether a row is present or not, use containsCachedRow instead.
     */
    public ColumnFamily getRawCachedRow(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return null;

        IRowCacheEntry cached = CacheService.instance.rowCache.getInternal(new RowCacheKey(metadata.cfId, key));
        return cached == null || cached instanceof RowCacheSentinel ? null : (ColumnFamily)cached;
    }

    private void invalidateCaches()
    {
        CacheService.instance.invalidateKeyCacheForCf(metadata.cfId);
        CacheService.instance.invalidateRowCacheForCf(metadata.cfId);
        if (metadata.isCounter())
            CacheService.instance.invalidateCounterCacheForCf(metadata.cfId);
    }

    /**
     * @return true if @param key is contained in the row cache
     */
    public boolean containsCachedRow(DecoratedKey key)
    {
        return CacheService.instance.rowCache.getCapacity() != 0 && CacheService.instance.rowCache.containsKey(new RowCacheKey(metadata.cfId, key));
    }

    public void invalidateCachedRow(RowCacheKey key)
    {
        CacheService.instance.rowCache.remove(key);
    }

    public void invalidateCachedRow(DecoratedKey key)
    {
        UUID cfId = Schema.instance.getId(keyspace.getName(), this.name);
        if (cfId == null)
            return; // secondary index

        invalidateCachedRow(new RowCacheKey(cfId, key));
    }

    public ClockAndCount getCachedCounter(ByteBuffer partitionKey, CellName cellName)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return null;
        return CacheService.instance.counterCache.get(CounterCacheKey.create(metadata.cfId, partitionKey, cellName));
    }

    public void putCachedCounter(ByteBuffer partitionKey, CellName cellName, ClockAndCount clockAndCount)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return;
        CacheService.instance.counterCache.put(CounterCacheKey.create(metadata.cfId, partitionKey, cellName), clockAndCount);
    }

    public void forceMajorCompaction() throws InterruptedException, ExecutionException
    {
        CompactionManager.instance.performMaximal(this);
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
        try (RefViewFragment view = selectAndReference(ALL_SSTABLES))
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
        try (RefViewFragment view = selectAndReference(ALL_SSTABLES))
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
    public void clearUnsafe()
    {
        for (final ColumnFamilyStore cfs : concatWithIndexes())
        {
            cfs.runWithCompactionsDisabled(new Callable<Void>()
            {
                public Void call()
                {
                    cfs.data.init();
                    return null;
                }
            }, true);
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
        logger.debug("truncating {}", name);

        if (keyspace.metadata.durableWrites || DatabaseDescriptor.isAutoSnapshot())
        {
            // flush the CF being truncated before forcing the new segment
            forceBlockingFlush();

            // sleep a little to make sure that our truncatedAt comes after any sstable
            // that was part of the flushed we forced; otherwise on a tie, it won't get deleted.
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
        }
        else
        {
            // just nuke the memtable data w/o writing to disk first
            synchronized (data)
            {
                final Flush flush = new Flush(true);
                flushExecutor.execute(flush);
                postFlushExecutor.submit(flush.postFlush);
            }
        }

        Runnable truncateRunnable = new Runnable()
        {
            public void run()
            {
                logger.debug("Discarding sstable data for truncated CF + indexes");

                final long truncatedAt = System.currentTimeMillis();
                data.notifyTruncated(truncatedAt);

                if (DatabaseDescriptor.isAutoSnapshot())
                    snapshot(Keyspace.getTimestampedSnapshotName(name));

                ReplayPosition replayAfter = discardSSTables(truncatedAt);

                for (SecondaryIndex index : indexManager.getIndexes())
                    index.truncateBlocking(truncatedAt);

                SystemKeyspace.saveTruncationRecord(ColumnFamilyStore.this, truncatedAt, replayAfter);
                logger.debug("cleaning out row cache");
                invalidateCaches();
            }
        };

        runWithCompactionsDisabled(Executors.callable(truncateRunnable), true);
        logger.debug("truncate complete");
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation)
    {
        // synchronize so that concurrent invocations don't re-enable compactions partway through unexpectedly,
        // and so we only run one major compaction at a time
        synchronized (this)
        {
            logger.debug("Cancelling in-progress compactions for {}", metadata.cfName);

            Iterable<ColumnFamilyStore> selfWithIndexes = concatWithIndexes();
            for (ColumnFamilyStore cfs : selfWithIndexes)
                cfs.getCompactionStrategy().pause();
            try
            {
                // interrupt in-progress compactions
                Function<ColumnFamilyStore, CFMetaData> f = new Function<ColumnFamilyStore, CFMetaData>()
                {
                    public CFMetaData apply(ColumnFamilyStore cfs)
                    {
                        return cfs.metadata;
                    }
                };
                Iterable<CFMetaData> allMetadata = Iterables.transform(selfWithIndexes, f);
                CompactionManager.instance.interruptCompactionFor(allMetadata, interruptValidation);

                // wait for the interruption to be recognized
                long start = System.nanoTime();
                long delay = TimeUnit.MINUTES.toNanos(1);
                while (System.nanoTime() - start < delay)
                {
                    if (CompactionManager.instance.isCompacting(selfWithIndexes))
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    else
                        break;
                }

                // doublecheck that we finished, instead of timing out
                for (ColumnFamilyStore cfs : selfWithIndexes)
                {
                    if (!cfs.getDataTracker().getCompacting().isEmpty())
                    {
                        logger.warn("Unable to cancel in-progress compactions for {}.  Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded.", metadata.cfName);
                        return null;
                    }
                }
                logger.debug("Compactions successfully cancelled");

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
                for (ColumnFamilyStore cfs : selfWithIndexes)
                    cfs.getCompactionStrategy().resume();
            }
        }
    }

    public Iterable<SSTableReader> markAllCompacting()
    {
        Callable<Iterable<SSTableReader>> callable = new Callable<Iterable<SSTableReader>>()
        {
            public Iterable<SSTableReader> call() throws Exception
            {
                assert data.getCompacting().isEmpty() : data.getCompacting();
                Collection<SSTableReader> sstables = Lists.newArrayList(AbstractCompactionStrategy.filterSuspectSSTables(getSSTables()));
                if (Iterables.isEmpty(sstables))
                    return Collections.emptyList();
                boolean success = data.markCompacting(sstables);
                assert success : "something marked things compacting while compactions are disabled";
                return sstables;
            }
        };

        return runWithCompactionsDisabled(callable, false);
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
        this.compactionStrategyWrapper.disable();
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
        this.compactionStrategyWrapper.enable();
        List<Future<?>> futures = CompactionManager.instance.submitBackground(this);
        if (waitForFutures)
            FBUtilities.waitOnFutures(futures);
    }

    public boolean isAutoCompactionDisabled()
    {
        return !this.compactionStrategyWrapper.isEnabled();
    }

    /*
     JMX getters and setters for the Default<T>s.
       - get/set minCompactionThreshold
       - get/set maxCompactionThreshold
       - get     memsize
       - get     memops
       - get/set memtime
     */

    public AbstractCompactionStrategy getCompactionStrategy()
    {
        return compactionStrategyWrapper;
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
        return data.getMeanColumns();
    }

    public long estimateKeys()
    {
        return data.estimatedKeys();
    }

    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return partitioner instanceof LocalPartitioner;
    }

    public Iterable<ColumnFamilyStore> concatWithIndexes()
    {
        // we return the main CFS first, which we rely on for simplicity in switchMemtable(), for getting the
        // latest replay position
        return Iterables.concat(Collections.singleton(this), indexManager.getIndexesBackedByCfs());
    }

    public List<String> getBuiltIndexes()
    {
       return indexManager.getBuiltIndexes();
    }

    public int getUnleveledSSTables()
    {
        return this.compactionStrategyWrapper.getUnleveledSSTables();
    }

    public int[] getSSTableCountPerLevel()
    {
        return compactionStrategyWrapper.getSSTableCountPerLevel();
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

    /**
     * Returns the creation time of the oldest memtable not fully flushed yet.
     */
    public long oldestUnflushedMemtable()
    {
        return data.getView().getOldestMemtable().creationTime();
    }

    public boolean isEmpty()
    {
        DataTracker.View view = data.getView();
        return view.sstables.isEmpty() && view.getCurrentMemtable().getOperations() == 0 && view.getCurrentMemtable() == view.getOldestMemtable();
    }

    private boolean isRowCacheEnabled()
    {
        return metadata.getCaching().rowCache.isEnabled() && CacheService.instance.rowCache.getCapacity() > 0;
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

        for (SSTableReader sstable : getSSTables())
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
        return getDataTracker().getDroppableTombstoneRatio();
    }

    public long trueSnapshotsSize()
    {
        return directories.trueSnapshotsSize();
    }

    @VisibleForTesting
    void resetFileIndexGenerator()
    {
        fileIndexGenerator.set(0);
    }

    public static final Function<DataTracker.View, List<SSTableReader>> ALL_SSTABLES = new Function<DataTracker.View, List<SSTableReader>>()
    {
        public List<SSTableReader> apply(DataTracker.View view)
        {
            return new ArrayList<>(view.sstables);
        }
    };

    public static final Function<DataTracker.View, List<SSTableReader>> UNREPAIRED_SSTABLES = new Function<DataTracker.View, List<SSTableReader>>()
    {
        public List<SSTableReader> apply(DataTracker.View view)
        {
            List<SSTableReader> sstables = new ArrayList<>();
            for (SSTableReader sstable : view.sstables)
            {
                if (!sstable.isRepaired())
                    sstables.add(sstable);
            }
            return sstables;
        }
    };
}
