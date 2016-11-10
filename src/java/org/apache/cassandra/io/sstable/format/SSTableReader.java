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
package org.apache.cassandra.io.sstable.format;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.codahale.metrics.Counter;
import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;

import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;

/**
 * An SSTableReader can be constructed in a number of places, but typically is either
 * read from disk at startup, or constructed from a flushed memtable, or after compaction
 * to replace some existing sstables. However once created, an sstablereader may also be modified.
 *
 * A reader's OpenReason describes its current stage in its lifecycle, as follows:
 *
 *
 * <pre> {@code
 * NORMAL
 * From:       None        => Reader has been read from disk, either at startup or from a flushed memtable
 *             EARLY       => Reader is the final result of a compaction
 *             MOVED_START => Reader WAS being compacted, but this failed and it has been restored to NORMAL status
 *
 * EARLY
 * From:       None        => Reader is a compaction replacement that is either incomplete and has been opened
 *                            to represent its partial result status, or has been finished but the compaction
 *                            it is a part of has not yet completed fully
 *             EARLY       => Same as from None, only it is not the first time it has been
 *
 * MOVED_START
 * From:       NORMAL      => Reader is being compacted. This compaction has not finished, but the compaction result
 *                            is either partially or fully opened, to either partially or fully replace this reader.
 *                            This reader's start key has been updated to represent this, so that reads only hit
 *                            one or the other reader.
 *
 * METADATA_CHANGE
 * From:       NORMAL      => Reader has seen low traffic and the amount of memory available for index summaries is
 *                            constrained, so its index summary has been downsampled.
 *         METADATA_CHANGE => Same
 * } </pre>
 *
 * Note that in parallel to this, there are two different Descriptor types; TMPLINK and FINAL; the latter corresponds
 * to NORMAL state readers and all readers that replace a NORMAL one. TMPLINK is used for EARLY state readers and
 * no others.
 *
 * When a reader is being compacted, if the result is large its replacement may be opened as EARLY before compaction
 * completes in order to present the result to consumers earlier. In this case the reader will itself be changed to
 * a MOVED_START state, where its start no longer represents its on-disk minimum key. This is to permit reads to be
 * directed to only one reader when the two represent the same data. The EARLY file can represent a compaction result
 * that is either partially complete and still in-progress, or a complete and immutable sstable that is part of a larger
 * macro compaction action that has not yet fully completed.
 *
 * Currently ALL compaction results at least briefly go through an EARLY open state prior to completion, regardless
 * of if early opening is enabled.
 *
 * Since a reader can be created multiple times over the same shared underlying resources, and the exact resources
 * it shares between each instance differ subtly, we track the lifetime of any underlying resource with its own
 * reference count, which each instance takes a Ref to. Each instance then tracks references to itself, and once these
 * all expire it releases its Refs to these underlying resources.
 *
 * There is some shared cleanup behaviour needed only once all sstablereaders in a certain stage of their lifecycle
 * (i.e. EARLY or NORMAL opening), and some that must only occur once all readers of any kind over a single logical
 * sstable have expired. These are managed by the TypeTidy and GlobalTidy classes at the bottom, and are effectively
 * managed as another resource each instance tracks its own Ref instance to, to ensure all of these resources are
 * cleaned up safely and can be debugged otherwise.
 *
 * TODO: fill in details about Tracker and lifecycle interactions for tools, and for compaction strategies
 */
public abstract class SSTableReader extends SSTable implements SelfRefCounted<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);
    private static final int ACCURATE_BOUNDARIES_MAGIC_NUMBER = 248923458;

    private static final ScheduledThreadPoolExecutor syncExecutor = new ScheduledThreadPoolExecutor(1);
    static
    {
        // Immediately remove readMeter sync task when cancelled.
        syncExecutor.setRemoveOnCancelPolicy(true);
    }
    private static final RateLimiter meterSyncThrottle = RateLimiter.create(100.0);

    public static final Comparator<SSTableReader> maxTimestampComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            long ts1 = o1.getMaxTimestamp();
            long ts2 = o2.getMaxTimestamp();
            return (ts1 > ts2 ? -1 : (ts1 == ts2 ? 0 : 1));
        }
    };

    // it's just an object, which we use regular Object equality on; we introduce a special class just for easy recognition
    public static final class UniqueIdentifier {}

    public static final Comparator<SSTableReader> sstableComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return o1.first.compareTo(o2.first);
        }
    };

    public static final Ordering<SSTableReader> sstableOrdering = Ordering.from(sstableComparator);

    /**
     * maxDataAge is a timestamp in local server time (e.g. System.currentTimeMilli) which represents an upper bound
     * to the newest piece of data stored in the sstable. In other words, this sstable does not contain items created
     * later than maxDataAge.
     *
     * The field is not serialized to disk, so relying on it for more than what truncate does is not advised.
     *
     * When a new sstable is flushed, maxDataAge is set to the time of creation.
     * When a sstable is created from compaction, maxDataAge is set to max of all merged sstables.
     *
     * The age is in milliseconds since epoc and is local to this host.
     */
    public final long maxDataAge;

    public enum OpenReason
    {
        NORMAL,
        EARLY,
        METADATA_CHANGE,
        MOVED_START
    }

    public final OpenReason openReason;
    public final UniqueIdentifier instanceId = new UniqueIdentifier();

    // indexfile and datafile: might be null before a call to load()
    protected SegmentedFile ifile;
    protected SegmentedFile dfile;
    protected IndexSummary indexSummary;
    protected IFilter bf;

    protected final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    protected InstrumentingCache<KeyCacheKey, RowIndexEntry> keyCache;

    protected final BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    // technically isCompacted is not necessary since it should never be unreferenced unless it is also compacted,
    // but it seems like a good extra layer of protection against reference counting bugs to not delete data based on that alone
    protected final AtomicBoolean isSuspect = new AtomicBoolean(false);

    // not final since we need to be able to change level on a file.
    protected volatile StatsMetadata sstableMetadata;

    protected final AtomicLong keyCacheHit = new AtomicLong(0);
    protected final AtomicLong keyCacheRequest = new AtomicLong(0);

    private final InstanceTidier tidy = new InstanceTidier(descriptor, metadata);
    private final Ref<SSTableReader> selfRef = new Ref<>(this, tidy);

    private RestorableMeter readMeter;

    /**
     * Calculate approximate key count.
     * If cardinality estimator is available on all given sstables, then this method use them to estimate
     * key count.
     * If not, then this uses index summaries.
     *
     * @param sstables SSTables to calculate key count
     * @return estimated key count
     */
    public static long getApproximateKeyCount(Collection<SSTableReader> sstables)
    {
        long count = -1;

        // check if cardinality estimator is available for all SSTables
        boolean cardinalityAvailable = !sstables.isEmpty() && Iterators.all(sstables.iterator(), new Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader sstable)
            {
                return sstable.descriptor.version.hasNewStatsFile();
            }
        });

        // if it is, load them to estimate key count
        if (cardinalityAvailable)
        {
            boolean failed = false;
            ICardinality cardinality = null;
            for (SSTableReader sstable : sstables)
            {
                if (sstable.openReason == OpenReason.EARLY)
                    continue;

                try
                {
                    CompactionMetadata metadata = (CompactionMetadata) sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION);
                    // If we can't load the CompactionMetadata, we are forced to estimate the keys using the index
                    // summary. (CASSANDRA-10676)
                    if (metadata == null)
                    {
                        logger.warn("Reading cardinality from Statistics.db failed for {}", sstable.getFilename());
                        failed = true;
                        break;
                    }

                    if (cardinality == null)
                        cardinality = metadata.cardinalityEstimator;
                    else
                        cardinality = cardinality.merge(metadata.cardinalityEstimator);
                }
                catch (IOException e)
                {
                    logger.warn("Reading cardinality from Statistics.db failed.", e);
                    failed = true;
                    break;
                }
                catch (CardinalityMergeException e)
                {
                    logger.warn("Cardinality merge failed.", e);
                    failed = true;
                    break;
                }
            }
            if (cardinality != null && !failed)
                count = cardinality.cardinality();
        }

        // if something went wrong above or cardinality is not available, calculate using index summary
        if (count < 0)
        {
            for (SSTableReader sstable : sstables)
                count += sstable.estimatedKeys();
        }
        return count;
    }

    /**
     * Estimates how much of the keys we would keep if the sstables were compacted together
     */
    public static double estimateCompactionGain(Set<SSTableReader> overlapping)
    {
        Set<ICardinality> cardinalities = new HashSet<>(overlapping.size());
        for (SSTableReader sstable : overlapping)
        {
            try
            {
                ICardinality cardinality = ((CompactionMetadata) sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION)).cardinalityEstimator;
                if (cardinality != null)
                    cardinalities.add(cardinality);
                else
                    logger.trace("Got a null cardinality estimator in: {}", sstable.getFilename());
            }
            catch (IOException e)
            {
                logger.warn("Could not read up compaction metadata for {}", sstable, e);
            }
        }
        long totalKeyCountBefore = 0;
        for (ICardinality cardinality : cardinalities)
        {
            totalKeyCountBefore += cardinality.cardinality();
        }
        if (totalKeyCountBefore == 0)
            return 1;

        long totalKeyCountAfter = mergeCardinalities(cardinalities).cardinality();
        logger.trace("Estimated compaction gain: {}/{}={}", totalKeyCountAfter, totalKeyCountBefore, ((double)totalKeyCountAfter)/totalKeyCountBefore);
        return ((double)totalKeyCountAfter)/totalKeyCountBefore;
    }

    private static ICardinality mergeCardinalities(Collection<ICardinality> cardinalities)
    {
        ICardinality base = new HyperLogLogPlus(13, 25); // see MetadataCollector.cardinality
        try
        {
            base = base.merge(cardinalities.toArray(new ICardinality[cardinalities.size()]));
        }
        catch (CardinalityMergeException e)
        {
            logger.warn("Could not merge cardinalities", e);
        }
        return base;
    }

    public static SSTableReader open(Descriptor descriptor) throws IOException
    {
        CFMetaData metadata;
        if (descriptor.cfname.contains(SECONDARY_INDEX_NAME_SEPARATOR))
        {
            int i = descriptor.cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
            String parentName = descriptor.cfname.substring(0, i);
            CFMetaData parent = Schema.instance.getCFMetaData(descriptor.ksname, parentName);
            ColumnDefinition def = parent.getColumnDefinitionForIndex(descriptor.cfname.substring(i + 1));
            metadata = CFMetaData.newIndexMetadata(parent, def, SecondaryIndex.getIndexComparator(parent, def));
        }
        else
        {
            metadata = Schema.instance.getCFMetaData(descriptor.ksname, descriptor.cfname);
        }
        return open(descriptor, metadata);
    }

    public static SSTableReader open(Descriptor desc, CFMetaData metadata) throws IOException
    {
        IPartitioner p = desc.cfname.contains(SECONDARY_INDEX_NAME_SEPARATOR)
                ? new LocalPartitioner(metadata.getKeyValidator())
                : StorageService.getPartitioner();
        return open(desc, componentsFor(desc), metadata, p);
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        return open(descriptor, components, metadata, partitioner, true, true);
    }

    // use only for offline or "Standalone" operations
    public static SSTableReader openNoValidation(Descriptor descriptor, Set<Component> components, ColumnFamilyStore cfs) throws IOException
    {
        return open(descriptor, components, cfs.metadata, cfs.partitioner, false, false); // do not track hotness
    }

    /**
     * Open SSTable reader to be used in batch mode(such as sstableloader).
     *
     * @param descriptor
     * @param components
     * @param metadata
     * @param partitioner
     * @return opened SSTableReader
     * @throws IOException
     */
    public static SSTableReader openForBatch(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA) : "Data component is missing for sstable " + descriptor;
        assert components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;

        Map<MetadataType, MetadataComponent> sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor,
                EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS));
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            logger.error(String.format("Cannot open %s; partitioner %s does not match system partitioner %s.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                    descriptor, validationMetadata.partitioner, partitionerName));
            System.exit(1);
        }

        logger.debug("Opening {} ({} bytes)", descriptor, new File(descriptor.filenameFor(Component.DATA)).length());
        SSTableReader sstable = internalOpen(descriptor, components, metadata, partitioner, System.currentTimeMillis(),
                statsMetadata, OpenReason.NORMAL);

        // special implementation of load to use non-pooled SegmentedFile builders
        try(SegmentedFile.Builder ibuilder = new BufferedSegmentedFile.Builder();
            SegmentedFile.Builder dbuilder = sstable.compression
                ? new CompressedSegmentedFile.Builder(null)
                : new BufferedSegmentedFile.Builder())
        {
            if (!sstable.loadSummary(ibuilder, dbuilder))
                sstable.buildSummary(false, ibuilder, dbuilder, false, Downsampling.BASE_SAMPLING_LEVEL);
            sstable.ifile = ibuilder.complete(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX));
            sstable.dfile = dbuilder.complete(sstable.descriptor.filenameFor(Component.DATA));
            sstable.bf = FilterFactory.AlwaysPresent;
            sstable.setup(false);
            return sstable;
        }
    }

    public static SSTableReader open(Descriptor descriptor,
                                      Set<Component> components,
                                      CFMetaData metadata,
                                      IPartitioner partitioner,
                                      boolean validate,
                                      boolean trackHotness) throws IOException
    {
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA) : "Data component is missing for sstable " + descriptor;
        assert !validate || components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;

        Map<MetadataType, MetadataComponent> sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor,
                                                                                                               EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS));
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            logger.error(String.format("Cannot open %s; partitioner %s does not match system partitioner %s.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                    descriptor, validationMetadata.partitioner, partitionerName));
            System.exit(1);
        }

        logger.debug("Opening {} ({} bytes)", descriptor, new File(descriptor.filenameFor(Component.DATA)).length());
        SSTableReader sstable = internalOpen(descriptor, components, metadata, partitioner, System.currentTimeMillis(),
                                             statsMetadata, OpenReason.NORMAL);
        try
        {
            // load index and filter
            long start = System.nanoTime();
            sstable.load(validationMetadata);
            logger.trace("INDEX LOAD TIME for {}: {} ms.", descriptor, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

            sstable.setup(trackHotness);
            if (validate)
                sstable.validate();

            if (sstable.getKeyCache() != null)
                logger.trace("key cache contains {}/{} keys", sstable.getKeyCache().size(), sstable.getKeyCache().getCapacity());

            return sstable;
        }
        catch (Throwable t)
        {
            sstable.selfRef().release();
            throw t;
        }
    }

    public static void logOpenException(Descriptor descriptor, IOException e)
    {
        if (e instanceof FileNotFoundException)
            logger.error("Missing sstable component in {}; skipped because of {}", descriptor, e.getMessage());
        else
            logger.error("Corrupt sstable {}; skipped", descriptor, e);
    }

    public static Collection<SSTableReader> openAll(Set<Map.Entry<Descriptor, Set<Component>>> entries,
                                                    final CFMetaData metadata,
                                                    final IPartitioner partitioner)
    {
        final Collection<SSTableReader> sstables = new LinkedBlockingQueue<>();

        ExecutorService executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("SSTableBatchOpen", FBUtilities.getAvailableProcessors());
        for (final Map.Entry<Descriptor, Set<Component>> entry : entries)
        {
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    SSTableReader sstable;
                    try
                    {
                        sstable = open(entry.getKey(), entry.getValue(), metadata, partitioner);
                    }
                    catch (CorruptSSTableException ex)
                    {
                        FileUtils.handleCorruptSSTable(ex);
                        logger.error("Corrupt sstable {}; skipping table", entry, ex);
                        return;
                    }
                    catch (FSError ex)
                    {
                        FileUtils.handleFSError(ex);
                        logger.error("Cannot read sstable {}; file system error, skipping table", entry, ex);
                        return;
                    }
                    catch (IOException ex)
                    {
                        logger.error("Cannot read sstable {}; other IO error, skipping table", entry, ex);
                        return;
                    }
                    sstables.add(sstable);
                }
            };
            executor.submit(runnable);
        }

        executor.shutdown();
        try
        {
            executor.awaitTermination(7, TimeUnit.DAYS);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        return sstables;

    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    public static SSTableReader internalOpen(Descriptor desc,
                                      Set<Component> components,
                                      CFMetaData metadata,
                                      IPartitioner partitioner,
                                      SegmentedFile ifile,
                                      SegmentedFile dfile,
                                      IndexSummary isummary,
                                      IFilter bf,
                                      long maxDataAge,
                                      StatsMetadata sstableMetadata,
                                      OpenReason openReason)
    {
        assert desc != null && partitioner != null && ifile != null && dfile != null && isummary != null && bf != null && sstableMetadata != null;

        SSTableReader reader = internalOpen(desc, components, metadata, partitioner, maxDataAge, sstableMetadata, openReason);

        reader.bf = bf;
        reader.ifile = ifile;
        reader.dfile = dfile;
        reader.indexSummary = isummary;
        reader.setup(true);

        return reader;
    }


    private static SSTableReader internalOpen(final Descriptor descriptor,
                                            Set<Component> components,
                                            CFMetaData metadata,
                                            IPartitioner partitioner,
                                            Long maxDataAge,
                                            StatsMetadata sstableMetadata,
                                            OpenReason openReason)
    {
        Factory readerFactory = descriptor.getFormat().getReaderFactory();

        return readerFactory.open(descriptor, components, metadata, partitioner, maxDataAge, sstableMetadata, openReason);
    }

    protected SSTableReader(final Descriptor desc,
                            Set<Component> components,
                            CFMetaData metadata,
                            IPartitioner partitioner,
                            long maxDataAge,
                            StatsMetadata sstableMetadata,
                            OpenReason openReason)
    {
        super(desc, components, metadata, partitioner);
        this.sstableMetadata = sstableMetadata;
        this.maxDataAge = maxDataAge;
        this.openReason = openReason;
        this.rowIndexEntrySerializer = descriptor.version.getSSTableFormat().getIndexSerializer(metadata);
    }

    public static long getTotalBytes(Iterable<SSTableReader> sstables)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
            sum += sstable.onDiskLength();
        return sum;
    }

    public static long getTotalUncompressedBytes(Iterable<SSTableReader> sstables)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
            sum += sstable.uncompressedLength();

        return sum;
    }

    public boolean equals(Object that)
    {
        return that instanceof SSTableReader && ((SSTableReader) that).descriptor.equals(this.descriptor);
    }

    public int hashCode()
    {
        return this.descriptor.hashCode();
    }

    public String getFilename()
    {
        return dfile.path();
    }

    public void setupKeyCache()
    {
        // under normal operation we can do this at any time, but SSTR is also used outside C* proper,
        // e.g. by BulkLoader, which does not initialize the cache.  As a kludge, we set up the cache
        // here when we know we're being wired into the rest of the server infrastructure.
        keyCache = CacheService.instance.keyCache;
    }

    private void load(ValidationMetadata validation) throws IOException
    {
        if (metadata.getBloomFilterFpChance() == 1.0)
        {
            // bf is disabled.
            load(false, true);
            bf = FilterFactory.AlwaysPresent;
        }
        else if (!components.contains(Component.PRIMARY_INDEX))
        {
            // avoid any reading of the missing primary index component.
            // this should only happen during StandaloneScrubber
            load(false, false);
        }
        else if (!components.contains(Component.FILTER) || validation == null)
        {
            // bf is enabled, but filter component is missing.
            load(true, true);
        }
        else if (validation.bloomFilterFPChance != metadata.getBloomFilterFpChance())
        {
            // bf fp chance in sstable metadata and it has changed since compaction.
            load(true, true);
        }
        else
        {
            // bf is enabled and fp chance matches the currently configured value.
            load(false, true);
            loadBloomFilter();
        }
    }

    /**
     * Load bloom filter from Filter.db file.
     *
     * @throws IOException
     */
    private void loadBloomFilter() throws IOException
    {
        try (DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream(descriptor.filenameFor(Component.FILTER)))))
        {
            bf = FilterFactory.deserialize(stream, true);
        }
    }

    /**
     * Loads ifile, dfile and indexSummary, and optionally recreates the bloom filter.
     * @param saveSummaryIfCreated for bulk loading purposes, if the summary was absent and needed to be built, you can
     *                             avoid persisting it to disk by setting this to false
     */
    private void load(boolean recreateBloomFilter, boolean saveSummaryIfCreated) throws IOException
    {
        try(SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode(), false);
            SegmentedFile.Builder dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode(), compression))
        {
            boolean summaryLoaded = loadSummary(ibuilder, dbuilder);
            boolean builtSummary = false;
            if (recreateBloomFilter || !summaryLoaded)
            {
                buildSummary(recreateBloomFilter, ibuilder, dbuilder, summaryLoaded, Downsampling.BASE_SAMPLING_LEVEL);
                builtSummary = true;
            }

            if (components.contains(Component.PRIMARY_INDEX))
                ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));

            dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));

            // Check for an index summary that was downsampled even though the serialization format doesn't support
            // that.  If it was downsampled, rebuild it.  See CASSANDRA-8993 for details.
        if (!descriptor.version.hasSamplingLevel() && !builtSummary && !validateSummarySamplingLevel() && ifile != null)
            {
                indexSummary.close();
                ifile.close();
                dfile.close();

                logger.info("Detected erroneously downsampled index summary; will rebuild summary at full sampling");
                FileUtils.deleteWithConfirm(new File(descriptor.filenameFor(Component.SUMMARY)));

                try(SegmentedFile.Builder ibuilderRebuild = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode(), false);
                    SegmentedFile.Builder dbuilderRebuild = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode(), compression))
                {
                    buildSummary(false, ibuilderRebuild, dbuilderRebuild, false, Downsampling.BASE_SAMPLING_LEVEL);
                    ifile = ibuilderRebuild.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
                    dfile = dbuilderRebuild.complete(descriptor.filenameFor(Component.DATA));
                    saveSummary(ibuilderRebuild, dbuilderRebuild);
                }
            }
            else if (saveSummaryIfCreated && builtSummary)
            {
                saveSummary(ibuilder, dbuilder);
            }
        }
        catch (Throwable t)
        { // Because the tidier has not been set-up yet in SSTableReader.open(), we must release the files in case of error
            if (ifile != null)
            {
                ifile.close();
                ifile = null;
            }

            if (dfile != null)
            {
                dfile.close();
                dfile = null;
            }

            if (indexSummary != null)
            {
                indexSummary.close();
                indexSummary = null;
            }

            throw t;
        }
    }

    /**
     * Build index summary(and optionally bloom filter) by reading through Index.db file.
     *
     * @param recreateBloomFilter true if recreate bloom filter
     * @param ibuilder
     * @param dbuilder
     * @param summaryLoaded true if index summary is already loaded and not need to build again
     * @throws IOException
     */
    private void buildSummary(boolean recreateBloomFilter, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, boolean summaryLoaded, int samplingLevel) throws IOException
    {
         if (!components.contains(Component.PRIMARY_INDEX))
             return;

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        try (RandomAccessReader primaryIndex = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX))))
        {
            long indexSize = primaryIndex.length();
            long histogramCount = sstableMetadata.estimatedRowSize.count();
            long estimatedKeys = histogramCount > 0 && !sstableMetadata.estimatedRowSize.isOverflowed()
                    ? histogramCount
                    : estimateRowsFromIndex(primaryIndex); // statistics is supposed to be optional

            if (recreateBloomFilter)
                bf = FilterFactory.getFilter(estimatedKeys, metadata.getBloomFilterFpChance(), true);

            try (IndexSummaryBuilder summaryBuilder = summaryLoaded ? null : new IndexSummaryBuilder(estimatedKeys, metadata.getMinIndexInterval(), samplingLevel))
            {
                long indexPosition;
                RowIndexEntry.IndexSerializer rowIndexSerializer = descriptor.getFormat().getIndexSerializer(metadata);

                while ((indexPosition = primaryIndex.getFilePointer()) != indexSize)
                {
                    ByteBuffer key = ByteBufferUtil.readWithShortLength(primaryIndex);
                    RowIndexEntry indexEntry = rowIndexSerializer.deserialize(primaryIndex, descriptor.version);
                    DecoratedKey decoratedKey = partitioner.decorateKey(key);
                    if (first == null)
                        first = decoratedKey;
                    last = decoratedKey;

                    if (recreateBloomFilter)
                        bf.add(decoratedKey);

                    // if summary was already read from disk we don't want to re-populate it using primary index
                    if (!summaryLoaded)
                    {
                        summaryBuilder.maybeAddEntry(decoratedKey, indexPosition);
                        ibuilder.addPotentialBoundary(indexPosition);
                        dbuilder.addPotentialBoundary(indexEntry.position);
                    }
                }

                if (!summaryLoaded)
                    indexSummary = summaryBuilder.build(partitioner);
            }
        }

        first = getMinimalKey(first);
        last = getMinimalKey(last);
    }

    /**
     * Load index summary from Summary.db file if it exists.
     *
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and this returns false to rebuild summary.
     *
     * @param ibuilder
     * @param dbuilder
     * @return true if index summary is loaded successfully from Summary.db file.
     */
    @SuppressWarnings("resource")
    public boolean loadSummary(SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder)
    {
        File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
        if (!summariesFile.exists())
            return false;

        DataInputStream iStream = null;
        try
        {
            iStream = new DataInputStream(new FileInputStream(summariesFile));
            indexSummary = IndexSummary.serializer.deserialize(
                    iStream, partitioner, descriptor.version.hasSamplingLevel(),
                    metadata.getMinIndexInterval(), metadata.getMaxIndexInterval());
            first = partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            last = partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            ibuilder.deserializeBounds(iStream);
            dbuilder.deserializeBounds(iStream);

            boolean checkForRepair = true;
            try
            {
                int v = iStream.readInt();
                // check for our magic number, indicating this summary has been sampled correctly
                checkForRepair = v != ACCURATE_BOUNDARIES_MAGIC_NUMBER;
            }
            catch (Throwable t) {}

            // fix CASSANDRA-10357 on-the-fly
            if (checkForRepair && MmappedSegmentedFile.maybeRepair(metadata, descriptor, indexSummary, ibuilder, dbuilder))
                saveSummary(ibuilder, dbuilder);
        }
        catch (IOException e)
        {
            if (indexSummary != null)
                indexSummary.close();
            logger.trace("Cannot deserialize SSTable Summary File {}: {}", summariesFile.getPath(), e.getMessage());
            // corrupted; delete it and fall back to creating a new summary
            FileUtils.closeQuietly(iStream);
            // delete it and fall back to creating a new summary
            FileUtils.deleteWithConfirm(summariesFile);
            return false;
        }
        finally
        {
            FileUtils.closeQuietly(iStream);
        }

        return true;
    }

    /**
     * Validates that an index summary has full sampling, as expected when the serialization format does not support
     * persisting the sampling level.
     * @return true if the summary has full sampling, false otherwise
     */
    private boolean validateSummarySamplingLevel()
    {
        // We need to check index summary entries against the index to verify that none of them were dropped due to
        // downsampling.  Downsampling can drop any of the first BASE_SAMPLING_LEVEL entries (repeating that drop pattern
        // for the remainder of the summary).  Unfortunately, the first entry to be dropped is the entry at
        // index (BASE_SAMPLING_LEVEL - 1), so we need to check a full set of BASE_SAMPLING_LEVEL entries.
        if (ifile == null)
            return false;

        Iterator<FileDataInput> segments = ifile.iterator(0);
        int i = 0;
        int summaryEntriesChecked = 0;
        int expectedIndexInterval = getMinIndexInterval();
        while (segments.hasNext())
        {
            String path = null;
            try (FileDataInput in = segments.next())
            {
                path = in.getPath();
                while (!in.isEOF())
                {
                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                    if (i % expectedIndexInterval == 0)
                    {
                        ByteBuffer summaryKey = ByteBuffer.wrap(indexSummary.getKey(i / expectedIndexInterval));
                        if (!summaryKey.equals(indexKey))
                            return false;
                        summaryEntriesChecked++;

                        if (summaryEntriesChecked == Downsampling.BASE_SAMPLING_LEVEL)
                            return true;
                    }
                    RowIndexEntry.Serializer.skip(in);
                    i++;
                }
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, path);
            }
        }

        return true;
    }

    /**
     * Save index summary to Summary.db file.
     *
     * @param ibuilder
     * @param dbuilder
     */

    public void saveSummary(SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder)
    {
        saveSummary(this.descriptor, this.first, this.last, ibuilder, dbuilder, indexSummary);
    }

    private void saveSummary(SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, IndexSummary newSummary)
    {
        saveSummary(this.descriptor, this.first, this.last, ibuilder, dbuilder, newSummary);
    }
    /**
     * Save index summary to Summary.db file.
     */
    public static void saveSummary(Descriptor descriptor, DecoratedKey first, DecoratedKey last,
                                   SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, IndexSummary summary)
    {
        File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
        if (summariesFile.exists())
            FileUtils.deleteWithConfirm(summariesFile);

        try (DataOutputStreamPlus oStream = new BufferedDataOutputStreamPlus(new FileOutputStream(summariesFile));)
        {
            IndexSummary.serializer.serialize(summary, oStream, descriptor.version.hasSamplingLevel());
            ByteBufferUtil.writeWithLength(first.getKey(), oStream);
            ByteBufferUtil.writeWithLength(last.getKey(), oStream);
            ibuilder.serializeBounds(oStream);
            dbuilder.serializeBounds(oStream);
            // write a magic number, to indicate this summary has been sampled correctly
            oStream.writeInt(ACCURATE_BOUNDARIES_MAGIC_NUMBER);
        }
        catch (IOException e)
        {
            logger.trace("Cannot save SSTable Summary: ", e);

            // corrupted hence delete it and let it load it now.
            if (summariesFile.exists())
                FileUtils.deleteWithConfirm(summariesFile);
        }
    }

    public void setReplaced()
    {
        synchronized (tidy.global)
        {
            assert !tidy.isReplaced;
            tidy.isReplaced = true;
        }
    }

    public boolean isReplaced()
    {
        synchronized (tidy.global)
        {
            return tidy.isReplaced;
        }
    }

    // runOnClose must NOT be an anonymous or non-static inner class, nor must it retain a reference chain to this reader
    public void runOnClose(final Runnable runOnClose)
    {
        synchronized (tidy.global)
        {
            final Runnable existing = tidy.runOnClose;
            tidy.runOnClose = AndThen.get(existing, runOnClose);
        }
    }

    private static class AndThen implements Runnable
    {
        final Runnable runFirst;
        final Runnable runSecond;

        private AndThen(Runnable runFirst, Runnable runSecond)
        {
            this.runFirst = runFirst;
            this.runSecond = runSecond;
        }

        public void run()
        {
            runFirst.run();
            runSecond.run();
        }

        static Runnable get(Runnable runFirst, Runnable runSecond)
        {
            if (runFirst == null)
                return runSecond;
            return new AndThen(runFirst, runSecond);
        }
    }

    /**
     * Clone this reader with the provided start and open reason, and set the clone as replacement.
     *
     * @param newFirst the first key for the replacement (which can be different from the original due to the pre-emptive
     * opening of compaction results).
     * @param reason the {@code OpenReason} for the replacement.
     *
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private SSTableReader cloneAndReplace(DecoratedKey newFirst, OpenReason reason)
    {
        return cloneAndReplace(newFirst, reason, indexSummary.sharedCopy());
    }

    /**
     * Clone this reader with the new values and set the clone as replacement.
     *
     * @param newFirst the first key for the replacement (which can be different from the original due to the pre-emptive
     * opening of compaction results).
     * @param reason the {@code OpenReason} for the replacement.
     * @param newSummary the index summary for the replacement.
     *
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private SSTableReader cloneAndReplace(DecoratedKey newFirst, OpenReason reason, IndexSummary newSummary)
    {
        SSTableReader replacement = internalOpen(descriptor,
                                                 components,
                                                 metadata,
                                                 partitioner,
                                                 ifile != null ? ifile.sharedCopy() : null,
                                                 dfile.sharedCopy(),
                                                 newSummary,
                                                 bf.sharedCopy(),
                                                 maxDataAge,
                                                 sstableMetadata,
                                                 reason);
        replacement.first = newFirst;
        replacement.last = last;
        replacement.isSuspect.set(isSuspect.get());
        return replacement;
    }

    // runOnClose must NOT be an anonymous or non-static inner class, nor must it retain a reference chain to this reader
    public SSTableReader cloneWithNewStart(DecoratedKey newStart, final Runnable runOnClose)
    {
        synchronized (tidy.global)
        {
            assert openReason != OpenReason.EARLY;
            // TODO: merge with caller's firstKeyBeyond() work,to save time
            if (newStart.compareTo(first) > 0)
            {
                final long dataStart = getPosition(newStart, Operator.EQ).position;
                final long indexStart = getIndexScanPosition(newStart);
                this.tidy.runOnClose = new DropPageCache(dfile, dataStart, ifile, indexStart, runOnClose);
            }

            return cloneAndReplace(newStart, OpenReason.MOVED_START);
        }
    }

    private static class DropPageCache implements Runnable
    {
        final SegmentedFile dfile;
        final long dfilePosition;
        final SegmentedFile ifile;
        final long ifilePosition;
        final Runnable andThen;

        private DropPageCache(SegmentedFile dfile, long dfilePosition, SegmentedFile ifile, long ifilePosition, Runnable andThen)
        {
            this.dfile = dfile;
            this.dfilePosition = dfilePosition;
            this.ifile = ifile;
            this.ifilePosition = ifilePosition;
            this.andThen = andThen;
        }

        public void run()
        {
            dfile.dropPageCache(dfilePosition);

            if (ifile != null)
                ifile.dropPageCache(ifilePosition);
            andThen.run();
        }
    }

    /**
     * Returns a new SSTableReader with the same properties as this SSTableReader except that a new IndexSummary will
     * be built at the target samplingLevel.  This (original) SSTableReader instance will be marked as replaced, have
     * its DeletingTask removed, and have its periodic read-meter sync task cancelled.
     * @param samplingLevel the desired sampling level for the index summary on the new SSTableReader
     * @return a new SSTableReader
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException
    {
        assert descriptor.version.hasSamplingLevel();

        synchronized (tidy.global)
        {
            assert openReason != OpenReason.EARLY;

            int minIndexInterval = metadata.getMinIndexInterval();
            int maxIndexInterval = metadata.getMaxIndexInterval();
            double effectiveInterval = indexSummary.getEffectiveIndexInterval();

            IndexSummary newSummary;
            long oldSize = bytesOnDisk();

            // We have to rebuild the summary from the on-disk primary index in three cases:
            // 1. The sampling level went up, so we need to read more entries off disk
            // 2. The min_index_interval changed (in either direction); this changes what entries would be in the summary
            //    at full sampling (and consequently at any other sampling level)
            // 3. The max_index_interval was lowered, forcing us to raise the sampling level
            if (samplingLevel > indexSummary.getSamplingLevel() || indexSummary.getMinIndexInterval() != minIndexInterval || effectiveInterval > maxIndexInterval)
            {
                newSummary = buildSummaryAtLevel(samplingLevel);
            }
            else if (samplingLevel < indexSummary.getSamplingLevel())
            {
                // we can use the existing index summary to make a smaller one
                newSummary = IndexSummaryBuilder.downsample(indexSummary, samplingLevel, minIndexInterval, partitioner);
            }
            else
            {
                throw new AssertionError("Attempted to clone SSTableReader with the same index summary sampling level and " +
                        "no adjustments to min/max_index_interval");
            }

            //Always save the resampled index
            try(SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode(), false);
                SegmentedFile.Builder dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode(), compression))
            {
                for (long boundry : dfile.copyReadableBounds())
                    dbuilder.addPotentialBoundary(boundry);
                for (long boundry : ifile.copyReadableBounds())
                    ibuilder.addPotentialBoundary(boundry);

                saveSummary(ibuilder, dbuilder, newSummary);
            }

            long newSize = bytesOnDisk();
            StorageMetrics.load.inc(newSize - oldSize);
            parent.metric.liveDiskSpaceUsed.inc(newSize - oldSize);

            return cloneAndReplace(first, OpenReason.METADATA_CHANGE, newSummary);
        }
    }

    private IndexSummary buildSummaryAtLevel(int newSamplingLevel) throws IOException
    {
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader primaryIndex = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));
        try
        {
            long indexSize = primaryIndex.length();
            try (IndexSummaryBuilder summaryBuilder = new IndexSummaryBuilder(estimatedKeys(), metadata.getMinIndexInterval(), newSamplingLevel))
            {
                long indexPosition;
                while ((indexPosition = primaryIndex.getFilePointer()) != indexSize)
                {
                    summaryBuilder.maybeAddEntry(partitioner.decorateKey(ByteBufferUtil.readWithShortLength(primaryIndex)), indexPosition);
                    RowIndexEntry.Serializer.skip(primaryIndex);
                }

                return summaryBuilder.build(partitioner);
            }
        }
        finally
        {
            FileUtils.closeQuietly(primaryIndex);
        }
    }

    public RestorableMeter getReadMeter()
    {
        return readMeter;
    }

    public int getIndexSummarySamplingLevel()
    {
        return indexSummary.getSamplingLevel();
    }

    public long getIndexSummaryOffHeapSize()
    {
        return indexSummary.getOffHeapSize();
    }

    public int getMinIndexInterval()
    {
        return indexSummary.getMinIndexInterval();
    }

    public double getEffectiveIndexInterval()
    {
        return indexSummary.getEffectiveIndexInterval();
    }

    public void releaseSummary()
    {
        tidy.releaseSummary();
        indexSummary = null;
    }

    private void validate()
    {
        if (this.first.compareTo(this.last) > 0)
        {
            selfRef().release();
            throw new IllegalStateException(String.format("SSTable first key %s > last key %s", this.first, this.last));
        }
    }

    /**
     * Gets the position in the index file to start scanning to find the given key (at most indexInterval keys away,
     * modulo downsampling of the index summary). Always returns a value >= 0
     */
    public long getIndexScanPosition(RowPosition key)
    {
        if (openReason == OpenReason.MOVED_START && key.compareTo(first) < 0)
            key = first;

        return getIndexScanPositionFromBinarySearchResult(indexSummary.binarySearch(key), indexSummary);
    }

    @VisibleForTesting
    public static long getIndexScanPositionFromBinarySearchResult(int binarySearchResult, IndexSummary referencedIndexSummary)
    {
        if (binarySearchResult == -1)
            return 0;
        else
            return referencedIndexSummary.getPosition(getIndexSummaryIndexFromBinarySearchResult(binarySearchResult));
    }

    public static int getIndexSummaryIndexFromBinarySearchResult(int binarySearchResult)
    {
        if (binarySearchResult < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (binarySearchResult + 1) * -1;
            if (greaterThan == 0)
                return -1;
            return greaterThan - 1;
        }
        else
        {
            return binarySearchResult;
        }
    }

    /**
     * Returns the compression metadata for this sstable.
     * @throws IllegalStateException if the sstable is not compressed
     */
    public CompressionMetadata getCompressionMetadata()
    {
        if (!compression)
            throw new IllegalStateException(this + " is not compressed");

        CompressionMetadata cmd = ((ICompressedFile) dfile).getMetadata();

        //We need the parent cf metadata
        String cfName = metadata.isSecondaryIndex() ? metadata.getParentColumnFamilyName() : metadata.cfName;
        cmd.parameters.setLiveMetadata(Schema.instance.getCFMetaData(metadata.ksName, cfName));

        return cmd;
    }

    /**
     * Returns the amount of memory in bytes used off heap by the compression meta-data.
     * @return the amount of memory in bytes used off heap by the compression meta-data
     */
    public long getCompressionMetadataOffHeapSize()
    {
        if (!compression)
            return 0;

        return getCompressionMetadata().offHeapSize();
    }

    /**
     * For testing purposes only.
     */
    public void forceFilterFailures()
    {
        bf = FilterFactory.AlwaysPresent;
    }

    public IFilter getBloomFilter()
    {
        return bf;
    }

    public long getBloomFilterSerializedSize()
    {
        return bf.serializedSize();
    }

    /**
     * Returns the amount of memory in bytes used off heap by the bloom filter.
     * @return the amount of memory in bytes used off heap by the bloom filter
     */
    public long getBloomFilterOffHeapSize()
    {
        return bf.offHeapSize();
    }

    /**
     * @return An estimate of the number of keys in this SSTable based on the index summary.
     */
    public long estimatedKeys()
    {
        return indexSummary.getEstimatedKeyCount();
    }

    /**
     * @param ranges
     * @return An estimate of the number of keys for given ranges in this SSTable.
     */
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        long sampleKeyCount = 0;
        List<Pair<Integer, Integer>> sampleIndexes = getSampleIndexesForRanges(indexSummary, ranges);
        for (Pair<Integer, Integer> sampleIndexRange : sampleIndexes)
            sampleKeyCount += (sampleIndexRange.right - sampleIndexRange.left + 1);

        // adjust for the current sampling level: (BSL / SL) * index_interval_at_full_sampling
        long estimatedKeys = sampleKeyCount * ((long) Downsampling.BASE_SAMPLING_LEVEL * indexSummary.getMinIndexInterval()) / indexSummary.getSamplingLevel();
        return Math.max(1, estimatedKeys);
    }

    /**
     * Returns the number of entries in the IndexSummary.  At full sampling, this is approximately 1/INDEX_INTERVALth of
     * the keys in this SSTable.
     */
    public int getIndexSummarySize()
    {
        return indexSummary.size();
    }

    /**
     * Returns the approximate number of entries the IndexSummary would contain if it were at full sampling.
     */
    public int getMaxIndexSummarySize()
    {
        return indexSummary.getMaxNumberOfEntries();
    }

    /**
     * Returns the key for the index summary entry at `index`.
     */
    public byte[] getIndexSummaryKey(int index)
    {
        return indexSummary.getKey(index);
    }

    private static List<Pair<Integer,Integer>> getSampleIndexesForRanges(IndexSummary summary, Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Integer,Integer>> positions = new ArrayList<>();

        for (Range<Token> range : Range.normalize(ranges))
        {
            RowPosition leftPosition = range.left.maxKeyBound();
            RowPosition rightPosition = range.right.maxKeyBound();

            int left = summary.binarySearch(leftPosition);
            if (left < 0)
                left = (left + 1) * -1;
            else
                // left range are start exclusive
                left = left + 1;
            if (left == summary.size())
                // left is past the end of the sampling
                continue;

            int right = Range.isWrapAround(range.left, range.right)
                    ? summary.size() - 1
                    : summary.binarySearch(rightPosition);
            if (right < 0)
            {
                // range are end inclusive so we use the previous index from what binarySearch give us
                // since that will be the last index we will return
                right = (right + 1) * -1;
                if (right == 0)
                    // Means the first key is already stricly greater that the right bound
                    continue;
                right--;
            }

            if (left > right)
                // empty range
                continue;
            positions.add(Pair.create(left, right));
        }
        return positions;
    }

    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        final List<Pair<Integer, Integer>> indexRanges = getSampleIndexesForRanges(indexSummary, Collections.singletonList(range));

        if (indexRanges.isEmpty())
            return Collections.emptyList();

        return new Iterable<DecoratedKey>()
        {
            public Iterator<DecoratedKey> iterator()
            {
                return new Iterator<DecoratedKey>()
                {
                    private Iterator<Pair<Integer, Integer>> rangeIter = indexRanges.iterator();
                    private Pair<Integer, Integer> current;
                    private int idx;

                    public boolean hasNext()
                    {
                        if (current == null || idx > current.right)
                        {
                            if (rangeIter.hasNext())
                            {
                                current = rangeIter.next();
                                idx = current.left;
                                return true;
                            }
                            return false;
                        }

                        return true;
                    }

                    public DecoratedKey next()
                    {
                        byte[] bytes = indexSummary.getKey(idx++);
                        return partitioner.decorateKey(ByteBuffer.wrap(bytes));
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Determine the minimal set of sections that can be extracted from this SSTable to cover the given ranges.
     * @return A sorted list of (offset,end) pairs that cover the given ranges in the datafile for this SSTable.
     */
    public List<Pair<Long,Long>> getPositionsForRanges(Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Long,Long>> positions = new ArrayList<>();
        for (Range<Token> range : Range.normalize(ranges))
        {
            assert !range.isWrapAround() || range.right.isMinimum();
            // truncate the range so it at most covers the sstable
            AbstractBounds<RowPosition> bounds = Range.makeRowRange(range);
            RowPosition leftBound = bounds.left.compareTo(first) > 0 ? bounds.left : first.getToken().minKeyBound();
            RowPosition rightBound = bounds.right.isMinimum() ? last.getToken().maxKeyBound() : bounds.right;

            if (leftBound.compareTo(last) > 0 || rightBound.compareTo(first) < 0)
                continue;

            long left = getPosition(leftBound, Operator.GT).position;
            long right = (rightBound.compareTo(last) > 0)
                         ? uncompressedLength()
                         : getPosition(rightBound, Operator.GT).position;

            if (left == right)
                // empty range
                continue;

            assert left < right : String.format("Range=%s openReason=%s first=%s last=%s left=%d right=%d", range, openReason, first, last, left, right);
            positions.add(Pair.create(left, right));
        }
        return positions;
    }

    public KeyCacheKey getCacheKey(DecoratedKey key)
    {
        return new KeyCacheKey(metadata.ksAndCFName, descriptor, key.getKey());
    }

    public void cacheKey(DecoratedKey key, RowIndexEntry info)
    {
        CachingOptions caching = metadata.getCaching();

        if (!caching.keyCache.isEnabled()
                || keyCache == null
                || keyCache.getCapacity() == 0)
        {
            return;
        }

        KeyCacheKey cacheKey = new KeyCacheKey(metadata.ksAndCFName, descriptor, key.getKey());
        logger.trace("Adding cache entry for {} -> {}", cacheKey, info);
        keyCache.put(cacheKey, info);
    }

    public RowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return getCachedPosition(new KeyCacheKey(metadata.ksAndCFName, descriptor, key.getKey()), updateStats);
    }

    protected RowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        if (keyCache != null && keyCache.getCapacity() > 0 && metadata.getCaching().keyCache.isEnabled()) {
            if (updateStats)
            {
                RowIndexEntry cachedEntry = keyCache.get(unifiedKey);
                keyCacheRequest.incrementAndGet();
                if (cachedEntry != null)
                {
                    keyCacheHit.incrementAndGet();
                    bloomFilterTracker.addTruePositive();
                }
                return cachedEntry;
            }
            else
            {
                return keyCache.getInternal(unifiedKey);
            }
        }
        return null;
    }

    /**
     * Get position updating key cache and stats.
     * @see #getPosition(org.apache.cassandra.db.RowPosition, SSTableReader.Operator, boolean)
     */
    public RowIndexEntry getPosition(RowPosition key, Operator op)
    {
        return getPosition(key, op, true, false);
    }

    public RowIndexEntry getPosition(RowPosition key, Operator op, boolean updateCacheAndStats)
    {
        return getPosition(key, op, updateCacheAndStats, false);
    }
    /**
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    protected abstract RowIndexEntry getPosition(RowPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast);

    //Corresponds to a name column
    public abstract OnDiskAtomIterator iterator(DecoratedKey key, SortedSet<CellName> columns);
    public abstract OnDiskAtomIterator iterator(FileDataInput file, DecoratedKey key, SortedSet<CellName> columns, RowIndexEntry indexEntry);

    //Corresponds to a slice query
    public abstract OnDiskAtomIterator iterator(DecoratedKey key, ColumnSlice[] slices, boolean reverse);
    public abstract OnDiskAtomIterator iterator(FileDataInput file, DecoratedKey key, ColumnSlice[] slices, boolean reversed, RowIndexEntry indexEntry);

    /**
     * Finds and returns the first key beyond a given token in this SSTable or null if no such key exists.
     */
    public DecoratedKey firstKeyBeyond(RowPosition token)
    {
        if (token.compareTo(first) < 0)
            return first;

        long sampledPosition = getIndexScanPosition(token);

        if (ifile == null)
            return null;

        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition);
        while (segments.hasNext())
        {
            String path = null;
            try (FileDataInput in = segments.next();)
            {
                path = in.getPath();
                while (!in.isEOF())
                {
                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                    DecoratedKey indexDecoratedKey = partitioner.decorateKey(indexKey);
                    if (indexDecoratedKey.compareTo(token) > 0)
                        return indexDecoratedKey;

                    RowIndexEntry.Serializer.skip(in);
                }
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, path);
            }
        }

        return null;
    }

    /**
     * @return The length in bytes of the data for this SSTable. For
     * compressed files, this is not the same thing as the on disk size (see
     * onDiskLength())
     */
    public long uncompressedLength()
    {
        return dfile.length;
    }

    /**
     * @return The length in bytes of the on disk size for this SSTable. For
     * compressed files, this is not the same thing as the data length (see
     * length())
     */
    public long onDiskLength()
    {
        return dfile.onDiskLength;
    }

    /**
     * Mark the sstable as obsolete, i.e., compacted into newer sstables.
     *
     * When calling this function, the caller must ensure that the SSTableReader is not referenced anywhere
     * except for threads holding a reference.
     *
     * @return true if the this is the first time the file was marked obsolete.  Calling this
     * multiple times is usually buggy (see exceptions in Tracker.unmarkCompacting and removeOldSSTablesSize).
     */
    public boolean markObsolete(Tracker tracker)
    {
        if (logger.isTraceEnabled())
            logger.trace("Marking {} compacted", getFilename());

        synchronized (tidy.global)
        {
            assert !tidy.isReplaced;
        }
        if (!tidy.global.isCompacted.getAndSet(true))
        {
            tidy.type.markObsolete(this, tracker);
            return true;
        }
        return false;
    }

    public boolean isMarkedCompacted()
    {
        return tidy.global.isCompacted.get();
    }

    public void markSuspect()
    {
        if (logger.isTraceEnabled())
            logger.trace("Marking {} as a suspect for blacklisting.", getFilename());

        isSuspect.getAndSet(true);
    }

    public boolean isMarkedSuspect()
    {
        return isSuspect.get();
    }


    /**
     * I/O SSTableScanner
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner()
    {
        return getScanner((RateLimiter) null);
    }

    public ISSTableScanner getScanner(RateLimiter limiter)
    {
        return getScanner(DataRange.allData(partitioner), limiter);
    }

    /**
     *
     * @param dataRange filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(DataRange dataRange)
    {
        return getScanner(dataRange, null);
    }

    /**
     * Direct I/O SSTableScanner over a defined range of tokens.
     *
     * @param range the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Range<Token> range, RateLimiter limiter)
    {
        if (range == null)
            return getScanner(limiter);
        return getScanner(Collections.singletonList(range), limiter);
    }

    /**
     * Direct I/O SSTableScanner over a defined collection of ranges of tokens.
     *
     * @param ranges the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract ISSTableScanner getScanner(Collection<Range<Token>> ranges, RateLimiter limiter);

    /**
     *
     * @param dataRange filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract ISSTableScanner getScanner(DataRange dataRange, RateLimiter limiter);



    public FileDataInput getFileDataInput(long position)
    {
        return dfile.getSegment(position);
    }

    /**
     * Tests if the sstable contains data newer than the given age param (in localhost currentMilli time).
     * This works in conjunction with maxDataAge which is an upper bound on the create of data in this sstable.
     * @param age The age to compare the maxDataAre of this sstable. Measured in millisec since epoc on this host
     * @return True iff this sstable contains data that's newer than the given age parameter.
     */
    public boolean newSince(long age)
    {
        return maxDataAge > age;
    }

    public void createLinks(String snapshotDirectoryPath)
    {
        for (Component component : components)
        {
            File sourceFile = new File(descriptor.filenameFor(component));
            File targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
            FileUtils.createHardLink(sourceFile, targetLink);
        }
    }

    public boolean isRepaired()
    {
        return sstableMetadata.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
    }

    /**
     * TODO: Move someplace reusable
     */
    public abstract static class Operator
    {
        public static final Operator EQ = new Equals();
        public static final Operator GE = new GreaterThanOrEqualTo();
        public static final Operator GT = new GreaterThan();

        /**
         * @param comparison The result of a call to compare/compareTo, with the desired field on the rhs.
         * @return less than 0 if the operator cannot match forward, 0 if it matches, greater than 0 if it might match forward.
         */
        public abstract int apply(int comparison);

        final static class Equals extends Operator
        {
            public int apply(int comparison) { return -comparison; }
        }

        final static class GreaterThanOrEqualTo extends Operator
        {
            public int apply(int comparison) { return comparison >= 0 ? 0 : 1; }
        }

        final static class GreaterThan extends Operator
        {
            public int apply(int comparison) { return comparison > 0 ? 0 : 1; }
        }
    }

    public long getBloomFilterFalsePositiveCount()
    {
        return bloomFilterTracker.getFalsePositiveCount();
    }

    public long getRecentBloomFilterFalsePositiveCount()
    {
        return bloomFilterTracker.getRecentFalsePositiveCount();
    }

    public long getBloomFilterTruePositiveCount()
    {
        return bloomFilterTracker.getTruePositiveCount();
    }

    public long getRecentBloomFilterTruePositiveCount()
    {
        return bloomFilterTracker.getRecentTruePositiveCount();
    }

    public InstrumentingCache<KeyCacheKey, RowIndexEntry> getKeyCache()
    {
        return keyCache;
    }

    public EstimatedHistogram getEstimatedRowSize()
    {
        return sstableMetadata.estimatedRowSize;
    }

    public EstimatedHistogram getEstimatedColumnCount()
    {
        return sstableMetadata.estimatedColumnCount;
    }

    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        return sstableMetadata.getEstimatedDroppableTombstoneRatio(gcBefore);
    }

    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return sstableMetadata.getDroppableTombstonesBefore(gcBefore);
    }

    public double getCompressionRatio()
    {
        return sstableMetadata.compressionRatio;
    }

    public long getMinTimestamp()
    {
        return sstableMetadata.minTimestamp;
    }

    public long getMaxTimestamp()
    {
        return sstableMetadata.maxTimestamp;
    }

    public Set<Integer> getAncestors()
    {
        try
        {
            CompactionMetadata compactionMetadata = (CompactionMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.COMPACTION);
            if (compactionMetadata != null)
                return compactionMetadata.ancestors;
            return Collections.emptySet();
        }
        catch (IOException e)
        {
            SSTableReader.logOpenException(descriptor, e);
            return Collections.emptySet();
        }
    }

    public int getSSTableLevel()
    {
        return sstableMetadata.sstableLevel;
    }

    /**
     * Reloads the sstable metadata from disk.
     *
     * Called after level is changed on sstable, for example if the sstable is dropped to L0
     *
     * Might be possible to remove in future versions
     *
     * @throws IOException
     */
    public void reloadSSTableMetadata() throws IOException
    {
        this.sstableMetadata = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
    }

    public StatsMetadata getSSTableMetadata()
    {
        return sstableMetadata;
    }

    public RandomAccessReader openDataReader(RateLimiter limiter)
    {
        assert limiter != null;
        return dfile.createThrottledReader(limiter);
    }

    public RandomAccessReader openDataReader()
    {
        return dfile.createReader();
    }

    public RandomAccessReader openIndexReader()
    {
        if (ifile != null)
            return ifile.createReader();
        return null;
    }

    /**
     * @param component component to get timestamp.
     * @return last modified time for given component. 0 if given component does not exist or IO error occurs.
     */
    public long getCreationTimeFor(Component component)
    {
        return new File(descriptor.filenameFor(component)).lastModified();
    }

    /**
     * @return Number of key cache hit
     */
    public long getKeyCacheHit()
    {
        return keyCacheHit.get();
    }

    /**
     * @return Number of key cache request
     */
    public long getKeyCacheRequest()
    {
        return keyCacheRequest.get();
    }

    /**
     * Increment the total row read count and read rate for this SSTable.  This should not be incremented for range
     * slice queries, row cache hits, or non-query reads, like compaction.
     */
    public void incrementReadCount()
    {
        if (readMeter != null)
            readMeter.mark();
    }

    public static class SizeComparator implements Comparator<SSTableReader>
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return Longs.compare(o1.onDiskLength(), o2.onDiskLength());
        }
    }

    public Ref<SSTableReader> tryRef()
    {
        return selfRef.tryRef();
    }

    public Ref<SSTableReader> selfRef()
    {
        return selfRef;
    }

    public Ref<SSTableReader> ref()
    {
        return selfRef.ref();
    }

    void setup(boolean trackHotness)
    {
        tidy.setup(this, trackHotness);
        this.readMeter = tidy.global.readMeter;
    }

    @VisibleForTesting
    public void overrideReadMeter(RestorableMeter readMeter)
    {
        this.readMeter = tidy.global.readMeter = readMeter;
    }

    /**
     * One instance per SSTableReader we create. This references the type-shared tidy, which in turn references
     * the globally shared tidy, i.e.
     *
     * InstanceTidier => DescriptorTypeTitdy => GlobalTidy
     *
     * We can create many InstanceTidiers (one for every time we reopen an sstable with MOVED_START for example), but there can only be
     * two DescriptorTypeTidy (FINAL and TEMPLINK) and only one GlobalTidy for one single logical sstable.
     *
     * When the InstanceTidier cleansup, it releases its reference to its DescriptorTypeTidy; when all InstanceTidiers
     * for that type have run, the DescriptorTypeTidy cleansup. DescriptorTypeTidy behaves in the same way towards GlobalTidy.
     *
     * For ease, we stash a direct reference to both our type-shared and global tidier
     */
    private static final class InstanceTidier implements Tidy
    {
        private final Descriptor descriptor;
        private final CFMetaData metadata;
        private IFilter bf;
        private IndexSummary summary;

        private SegmentedFile dfile;
        private SegmentedFile ifile;
        private Runnable runOnClose;
        private boolean isReplaced = false;

        // a reference to our shared per-Descriptor.Type tidy instance, that
        // we will release when we are ourselves released
        private Ref<DescriptorTypeTidy> typeRef;

        // a convenience stashing of the shared per-descriptor-type tidy instance itself
        // and the per-logical-sstable globally shared state that it is linked to
        private DescriptorTypeTidy type;
        private GlobalTidy global;

        private volatile boolean setup;

        void setup(SSTableReader reader, boolean trackHotness)
        {
            this.setup = true;
            this.bf = reader.bf;
            this.summary = reader.indexSummary;
            this.dfile = reader.dfile;
            this.ifile = reader.ifile;
            // get a new reference to the shared descriptor-type tidy
            this.typeRef = DescriptorTypeTidy.get(reader);
            this.type = typeRef.get();
            this.global = type.globalRef.get();
            if (trackHotness)
                global.ensureReadMeter();
        }

        InstanceTidier(Descriptor descriptor, CFMetaData metadata)
        {
            this.descriptor = descriptor;
            this.metadata = metadata;
        }

        public void tidy()
        {
            if (logger.isTraceEnabled())
                logger.trace("Running instance tidier for {} with setup {}", descriptor, setup);

            // don't try to cleanup if the sstablereader was never fully constructed
            if (!setup)
                return;

            final ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.cfId);
            final OpOrder.Barrier barrier;
            if (cfs != null)
            {
                barrier = cfs.readOrdering.newBarrier();
                barrier.issue();
            }
            else
                barrier = null;

            ScheduledExecutors.nonPeriodicTasks.execute(new Runnable()
            {
                public void run()
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Async instance tidier for {}, before barrier", descriptor);

                    if (barrier != null)
                        barrier.await();

                    if (logger.isTraceEnabled())
                        logger.trace("Async instance tidier for {}, after barrier", descriptor);

                    if (bf != null)
                        bf.close();
                    if (summary != null)
                        summary.close();
                    if (runOnClose != null)
                        runOnClose.run();
                    if (dfile != null)
                        dfile.close();
                    if (ifile != null)
                        ifile.close();
                    typeRef.release();

                    if (logger.isTraceEnabled())
                        logger.trace("Async instance tidier for {}, completed", descriptor);
                }
            });
        }

        public String name()
        {
            return descriptor.toString();
        }

        void releaseSummary()
        {
            summary.close();
            assert summary.isCleanedUp();
            summary = null;
        }
    }

    /**
     * One shared between all instances of a given Descriptor.Type.
     * Performs only two things: the deletion of the sstables for the type,
     * if necessary; and the shared reference to the globally shared state.
     *
     * All InstanceTidiers, on setup(), ask the static get() method for their shared state,
     * and stash a reference to it to be released when they are. Once all such references are
     * released, the shared tidy will be performed.
     */
    static final class DescriptorTypeTidy implements Tidy
    {
        // keyed by REAL descriptor (TMPLINK/FINAL), mapping to the shared DescriptorTypeTidy for that descriptor
        static final ConcurrentMap<Descriptor, Ref<DescriptorTypeTidy>> lookup = new ConcurrentHashMap<>();

        private final Descriptor desc;
        private final Ref<GlobalTidy> globalRef;
        private final Set<Component> components;
        private long sizeOnDelete;
        private Counter totalDiskSpaceUsed;

        DescriptorTypeTidy(Descriptor desc, SSTableReader sstable)
        {
            this.desc = desc;
            // get a new reference to the shared global tidy
            this.globalRef = GlobalTidy.get(sstable);
            this.components = sstable.components;
        }

        void markObsolete(SSTableReader instance, Tracker tracker)
        {
            // the tracker is used only to notify listeners of deletion of the sstable;
            // since deletion of a non-final file is not really deletion of the sstable,
            // we don't want to notify the listeners in this event
            if (tracker != null && tracker.cfstore != null && desc.type == Descriptor.Type.FINAL)
            {
                sizeOnDelete = instance.bytesOnDisk();
                totalDiskSpaceUsed = tracker.cfstore.metric.totalDiskSpaceUsed;
                tracker.notifyDeleting(instance);
            }
        }

        public void tidy()
        {
            lookup.remove(desc);
            boolean isCompacted = globalRef.get().isCompacted.get();
            globalRef.release();
            switch (desc.type)
            {
                case FINAL:
                    if (isCompacted)
                        new SSTableDeletingTask(desc, components, totalDiskSpaceUsed, sizeOnDelete).run();
                    break;
                case TEMPLINK:
                    new SSTableDeletingTask(desc, components, null, 0).run();
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        public String name()
        {
            return desc.toString();
        }

        // get a new reference to the shared DescriptorTypeTidy for this sstable
        @SuppressWarnings("resource")
        public static Ref<DescriptorTypeTidy> get(SSTableReader sstable)
        {
            Descriptor desc = sstable.descriptor;
            if (sstable.openReason == OpenReason.EARLY)
                desc = desc.asType(Descriptor.Type.TEMPLINK);
            Ref<DescriptorTypeTidy> refc = lookup.get(desc);
            if (refc != null)
                return refc.ref();
            final DescriptorTypeTidy tidy = new DescriptorTypeTidy(desc, sstable);
            refc = new Ref<>(tidy, tidy);
            Ref<?> ex = lookup.putIfAbsent(desc, refc);
            if (ex != null)
            {
                refc.close();
                throw new AssertionError();
            }
            return refc;
        }
    }

    /**
     * One instance per logical sstable. This both tracks shared cleanup and some shared state related
     * to the sstable's lifecycle. All DescriptorTypeTidy instances, on construction, obtain a reference to us
     * via our static get(). There should only ever be at most two such references extant at any one time,
     * since only TMPLINK and FINAL type descriptors should be open as readers. When all files of both
     * kinds have been released, this shared tidy will be performed.
     */
    static final class GlobalTidy implements Tidy
    {
        // keyed by FINAL descriptor, mapping to the shared GlobalTidy for that descriptor
        static final ConcurrentMap<Descriptor, Ref<GlobalTidy>> lookup = new ConcurrentHashMap<>();

        private final Descriptor desc;
        // the readMeter that is shared between all instances of the sstable, and can be overridden in all of them
        // at once also, for testing purposes
        private RestorableMeter readMeter;
        // the scheduled persistence of the readMeter, that we will cancel once all instances of this logical
        // sstable have been released
        private ScheduledFuture readMeterSyncFuture;
        // shared state managing if the logical sstable has been compacted; this is used in cleanup both here
        // and in the FINAL type tidier
        private final AtomicBoolean isCompacted;

        GlobalTidy(final SSTableReader reader)
        {
            this.desc = reader.descriptor;
            this.isCompacted = new AtomicBoolean();
        }

        void ensureReadMeter()
        {
            if (readMeter != null)
                return;

            // Don't track read rates for tables in the system keyspace and don't bother trying to load or persist
            // the read meter when in client mode.
            if (SystemKeyspace.NAME.equals(desc.ksname))
            {
                readMeter = null;
                readMeterSyncFuture = null;
                return;
            }

            readMeter = SystemKeyspace.getSSTableReadMeter(desc.ksname, desc.cfname, desc.generation);
            // sync the average read rate to system.sstable_activity every five minutes, starting one minute from now
            readMeterSyncFuture = syncExecutor.scheduleAtFixedRate(new Runnable()
            {
                public void run()
                {
                    if (!isCompacted.get())
                    {
                        meterSyncThrottle.acquire();
                        SystemKeyspace.persistSSTableReadMeter(desc.ksname, desc.cfname, desc.generation, readMeter);
                    }
                }
            }, 1, 5, TimeUnit.MINUTES);
        }

        public void tidy()
        {
            lookup.remove(desc);
            if (readMeterSyncFuture != null)
                readMeterSyncFuture.cancel(true);
            if (isCompacted.get())
                SystemKeyspace.clearSSTableReadMeter(desc.ksname, desc.cfname, desc.generation);
            // don't ideally want to dropPageCache for the file until all instances have been released
            CLibrary.trySkipCache(desc.filenameFor(Component.DATA), 0, 0);
            CLibrary.trySkipCache(desc.filenameFor(Component.PRIMARY_INDEX), 0, 0);
        }

        public String name()
        {
            return desc.toString();
        }

        // get a new reference to the shared GlobalTidy for this sstable
        @SuppressWarnings("resource")
        public static Ref<GlobalTidy> get(SSTableReader sstable)
        {
            Descriptor descriptor = sstable.descriptor;
            Ref<GlobalTidy> refc = lookup.get(descriptor);
            if (refc != null)
                return refc.ref();
            final GlobalTidy tidy = new GlobalTidy(sstable);
            refc = new Ref<>(tidy, tidy);
            Ref<?> ex = lookup.putIfAbsent(descriptor, refc);
            if (ex != null)
            {
                refc.close();
                throw new AssertionError();
            }
            return refc;
        }
    }

    @VisibleForTesting
    public static void resetTidying()
    {
        GlobalTidy.lookup.clear();
        DescriptorTypeTidy.lookup.clear();
    }

    public static abstract class Factory
    {
        public abstract SSTableReader open(final Descriptor descriptor,
                                           Set<Component> components,
                                           CFMetaData metadata,
                                           IPartitioner partitioner,
                                           Long maxDataAge,
                                           StatsMetadata sstableMetadata,
                                           OpenReason openReason);

    }
}
