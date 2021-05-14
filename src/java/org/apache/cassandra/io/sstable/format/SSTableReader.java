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


import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.rows.UnfilteredSource;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.*;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;

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
public abstract class SSTableReader extends SSTable implements UnfilteredSource, SelfRefCounted<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    private static final boolean TRACK_ACTIVITY = CassandraRelevantProperties.DISABLE_SSTABLE_ACTIVITY_TRACKING.getBoolean();

    private static final ScheduledExecutorPlus syncExecutor = initSyncExecutor();
    private static ScheduledExecutorPlus initSyncExecutor()
    {
        if (DatabaseDescriptor.isClientOrToolInitialized())
            return null;

        // Do NOT start this thread pool in client mode

        ScheduledExecutorPlus syncExecutor = executorFactory().scheduled("read-hotness-tracker");
        // Immediately remove readMeter sync task when cancelled.
        // TODO: should we set this by default on all scheduled executors?
        if (syncExecutor instanceof ScheduledThreadPoolExecutor)
            ((ScheduledThreadPoolExecutor)syncExecutor).setRemoveOnCancelPolicy(true);
        return syncExecutor;
    }
    private static final RateLimiter meterSyncThrottle = RateLimiter.create(100.0);

    public static final Comparator<SSTableReader> maxTimestampDescending = (o1, o2) -> Long.compare(o2.getMaxTimestamp(), o1.getMaxTimestamp());
    public static final Comparator<SSTableReader> maxTimestampAscending = (o1, o2) -> Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp());

    // it's just an object, which we use regular Object equality on; we introduce a special class just for easy recognition
    public static final class UniqueIdentifier {}

    public static final Comparator<SSTableReader> sstableComparator = (o1, o2) -> o1.first.compareTo(o2.first);

    public static final Comparator<SSTableReader> idComparator = Comparator.comparing(t -> t.descriptor.id, SSTableIdFactory.COMPARATOR);
    public static final Comparator<SSTableReader> idReverseComparator = idComparator.reversed();

    public static final Ordering<SSTableReader> sstableOrdering = Ordering.from(sstableComparator);

    public static final Comparator<SSTableReader> sizeComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return Longs.compare(o1.onDiskLength(), o2.onDiskLength());
        }
    };

    /**
     * maxDataAge is a timestamp in local server time (e.g. Global.currentTimeMilli) which represents an upper bound
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
    protected final FileHandle ifile;
    protected final FileHandle dfile;
    protected final IFilter bf;
    public final IndexSummary indexSummary;

    protected final RowIndexEntry.IndexSerializer<?> rowIndexEntrySerializer;

    protected InstrumentingCache<KeyCacheKey, RowIndexEntry> keyCache;

    protected final BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    // technically isCompacted is not necessary since it should never be unreferenced unless it is also compacted,
    // but it seems like a good extra layer of protection against reference counting bugs to not delete data based on that alone
    protected final AtomicBoolean isSuspect = new AtomicBoolean(false);

    // not final since we need to be able to change level on a file.
    protected volatile StatsMetadata sstableMetadata;

    public final SerializationHeader header;

    protected final AtomicLong keyCacheHit = new AtomicLong(0);
    protected final AtomicLong keyCacheRequest = new AtomicLong(0);

    private final InstanceTidier tidy;
    private final Ref<SSTableReader> selfRef;

    private RestorableMeter readMeter;

    private volatile double crcCheckChance;

    /**
     * Calculate approximate key count.
     * If cardinality estimator is available on all given sstables, then this method use them to estimate
     * key count.
     * If not, then this uses index summaries.
     *
     * @param sstables SSTables to calculate key count
     * @return estimated key count
     */
    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = -1;

        if (Iterables.isEmpty(sstables))
            return count;

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

        // if something went wrong above or cardinality is not available, calculate using index summary
        if (count < 0)
        {
            count = 0;
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

    public static SSTableReader open(Descriptor descriptor)
    {
        TableMetadataRef metadata;
        if (descriptor.cfname.contains(SECONDARY_INDEX_NAME_SEPARATOR))
        {
            int i = descriptor.cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
            String indexName = descriptor.cfname.substring(i + 1);
            metadata = Schema.instance.getIndexTableMetadataRef(descriptor.ksname, indexName);
            if (metadata == null)
                throw new AssertionError("Could not find index metadata for index cf " + i);
        }
        else
        {
            metadata = Schema.instance.getTableMetadataRef(descriptor.ksname, descriptor.cfname);
        }
        return open(descriptor, metadata);
    }

    public static SSTableReader open(Descriptor desc, TableMetadataRef metadata)
    {
        return open(desc, componentsFor(desc), metadata);
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata)
    {
        return open(descriptor, components, metadata, true, false);
    }

    // use only for offline or "Standalone" operations
    public static SSTableReader openNoValidation(Descriptor descriptor, Set<Component> components, ColumnFamilyStore cfs)
    {
        return open(descriptor, components, cfs.metadata, false, true);
    }

    // use only for offline or "Standalone" operations
    public static SSTableReader openNoValidation(Descriptor descriptor, TableMetadataRef metadata)
    {
        return open(descriptor, componentsFor(descriptor), metadata, false, true);
    }

    /**
     * Open SSTable reader to be used in batch mode(such as sstableloader).
     *
     * @param descriptor
     * @param components
     * @param metadata
     * @return opened SSTableReader
     * @throws IOException
     */
    public static SSTableReader openForBatch(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata)
    {
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA) : "Data component is missing for sstable " + descriptor;
        assert components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;
        verifyCompressionInfoExistenceIfApplicable(descriptor, components);

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata;
        try
        {
             sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, descriptor.filenameFor(Component.STATS));
        }

        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                         descriptor, validationMetadata.partitioner, partitionerName);
            System.exit(1);
        }

        try
        {
            return new SSTableReaderBuilder.ForBatch(descriptor, metadata, components, statsMetadata, header.toHeader(metadata.get())).build();
        }
        catch (UnknownColumnException e)
        {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Open an SSTable for reading
     * @param descriptor SSTable to open
     * @param components Components included with this SSTable
     * @param metadata for this SSTables CF
     * @param validate Check SSTable for corruption (limited)
     * @param isOffline Whether we are opening this SSTable "offline", for example from an external tool or not for inclusion in queries (validations)
     *                  This stops regenerating BF + Summaries and also disables tracking of hotness for the SSTable.
     * @return {@link SSTableReader}
     * @throws IOException
     */
    public static SSTableReader open(Descriptor descriptor,
                                     Set<Component> components,
                                     TableMetadataRef metadata,
                                     boolean validate,
                                     boolean isOffline)
    {
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA) : "Data component is missing for sstable " + descriptor;
        assert !validate || components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;

        // For the 3.0+ sstable format, the (misnomed) stats component hold the serialization header which we need to deserialize the sstable content
        assert components.contains(Component.STATS) : "Stats component is missing for sstable " + descriptor;

        verifyCompressionInfoExistenceIfApplicable(descriptor, components);

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);

        Map<MetadataType, MetadataComponent> sstableMetadata;
        try
        {
            sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
        }
        catch (Throwable t)
        {
            throw new CorruptSSTableException(t, descriptor.filenameFor(Component.STATS));
        }
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        assert header != null;

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                         descriptor, validationMetadata.partitioner, partitionerName);
            System.exit(1);
        }

        SSTableReader sstable;
        try
        {
            sstable = new SSTableReaderBuilder.ForRead(descriptor,
                                                       metadata,
                                                       validationMetadata,
                                                       isOffline,
                                                       components,
                                                       statsMetadata,
                                                       header.toHeader(metadata.get())).build();
        }
        catch (UnknownColumnException e)
        {
            throw new IllegalStateException(e);
        }

        try
        {
            if (validate)
                sstable.validate();

            if (sstable.getKeyCache() != null)
                logger.trace("key cache contains {}/{} keys", sstable.getKeyCache().size(), sstable.getKeyCache().getCapacity());

            return sstable;
        }
        catch (Throwable t)
        {
            sstable.selfRef().release();
            throw new CorruptSSTableException(t, sstable.getFilename());
        }
    }

    public static Collection<SSTableReader> openAll(Set<Map.Entry<Descriptor, Set<Component>>> entries,
                                                    final TableMetadataRef metadata)
    {
        final Collection<SSTableReader> sstables = newBlockingQueue();

        ExecutorPlus executor = executorFactory().pooled("SSTableBatchOpen", FBUtilities.getAvailableProcessors());
        try
        {
            for (final Map.Entry<Descriptor, Set<Component>> entry : entries)
            {
                Runnable runnable = new Runnable()
                {
                    public void run()
                    {
                        SSTableReader sstable;
                        try
                        {
                            sstable = open(entry.getKey(), entry.getValue(), metadata);
                        }
                        catch (CorruptSSTableException ex)
                        {
                            JVMStabilityInspector.inspectThrowable(ex);
                            logger.error("Corrupt sstable {}; skipping table", entry, ex);
                            return;
                        }
                        catch (FSError ex)
                        {
                            JVMStabilityInspector.inspectThrowable(ex);
                            logger.error("Cannot read sstable {}; file system error, skipping table", entry, ex);
                            return;
                        }
                        sstables.add(sstable);
                    }
                };
                executor.submit(runnable);
            }
        }
        finally
        {
            executor.shutdown();
        }

        try
        {
            executor.awaitTermination(7, TimeUnit.DAYS);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        return sstables;

    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    public static SSTableReader internalOpen(Descriptor desc,
                                             Set<Component> components,
                                             TableMetadataRef metadata,
                                             FileHandle ifile,
                                             FileHandle dfile,
                                             IndexSummary summary,
                                             IFilter bf,
                                             long maxDataAge,
                                             StatsMetadata sstableMetadata,
                                             OpenReason openReason,
                                             SerializationHeader header)
    {
        assert desc != null && ifile != null && dfile != null && summary != null && bf != null && sstableMetadata != null;

        return new SSTableReaderBuilder.ForWriter(desc, metadata, maxDataAge, components, sstableMetadata, openReason, header)
                .bf(bf).ifile(ifile).dfile(dfile).summary(summary).build();
    }

    /**
     * Best-effort checking to verify the expected compression info component exists, according to the TOC file.
     * The verification depends on the existence of TOC file. If absent, the verification is skipped.
     * @param descriptor
     * @param actualComponents, actual components listed from the file system.
     * @throws CorruptSSTableException, if TOC expects compression info but not found from disk.
     * @throws FSReadError, if unable to read from TOC file.
     */
    public static void verifyCompressionInfoExistenceIfApplicable(Descriptor descriptor,
                                                                  Set<Component> actualComponents)
    throws CorruptSSTableException, FSReadError
    {
        File tocFile = new File(descriptor.filenameFor(Component.TOC));
        if (tocFile.exists())
        {
            try
            {
                Set<Component> expectedComponents = readTOC(descriptor, false);
                if (expectedComponents.contains(Component.COMPRESSION_INFO) && !actualComponents.contains(Component.COMPRESSION_INFO))
                {
                    String compressionInfoFileName = descriptor.filenameFor(Component.COMPRESSION_INFO);
                    throw new CorruptSSTableException(new NoSuchFileException(compressionInfoFileName), compressionInfoFileName);
                }
            }
            catch (IOException e)
            {
                throw new FSReadError(e, tocFile);
            }
        }
    }

    protected SSTableReader(SSTableReaderBuilder builder)
    {
        this(builder.descriptor,
             builder.components,
             builder.metadataRef,
             builder.maxDataAge,
             builder.statsMetadata,
             builder.openReason,
             builder.header,
             builder.summary,
             builder.dfile,
             builder.ifile,
             builder.bf);
    }

    protected SSTableReader(final Descriptor desc,
                            Set<Component> components,
                            TableMetadataRef metadata,
                            long maxDataAge,
                            StatsMetadata sstableMetadata,
                            OpenReason openReason,
                            SerializationHeader header,
                            IndexSummary summary,
                            FileHandle dfile,
                            FileHandle ifile,
                            IFilter bf)
    {
        super(desc, components, metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
        this.sstableMetadata = sstableMetadata;
        this.header = header;
        this.indexSummary = summary;
        this.dfile = dfile;
        this.ifile = ifile;
        this.bf = bf;
        this.maxDataAge = maxDataAge;
        this.openReason = openReason;
        this.rowIndexEntrySerializer = descriptor.version.getSSTableFormat().getIndexSerializer(metadata.get(), desc.version, header);
        tidy = new InstanceTidier(descriptor, metadata.id);
        selfRef = new Ref<>(this, tidy);
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

    public void setupOnline()
    {
        // under normal operation we can do this at any time, but SSTR is also used outside C* proper,
        // e.g. by BulkLoader, which does not initialize the cache.  As a kludge, we set up the cache
        // here when we know we're being wired into the rest of the server infrastructure.
        InstrumentingCache<KeyCacheKey, RowIndexEntry> maybeKeyCache = CacheService.instance.keyCache;
        if (maybeKeyCache.getCapacity() > 0)
            keyCache = maybeKeyCache;

        final ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata().id);
        if (cfs != null)
            setCrcCheckChance(cfs.getCrcCheckChance());
    }

    /**
     * Save index summary to Summary.db file.
     */
    public static void saveSummary(Descriptor descriptor, DecoratedKey first, DecoratedKey last, IndexSummary summary)
    {
        File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
        if (summariesFile.exists())
            FileUtils.deleteWithConfirm(summariesFile);

        try (DataOutputStreamPlus oStream = new FileOutputStreamPlus(summariesFile))
        {
            IndexSummary.serializer.serialize(summary, oStream);
            ByteBufferUtil.writeWithLength(first.getKey(), oStream);
            ByteBufferUtil.writeWithLength(last.getKey(), oStream);
        }
        catch (IOException e)
        {
            logger.error("Cannot save SSTable Summary: ", e);

            // corrupted hence delete it and let it load it now.
            if (summariesFile.exists())
                FileUtils.deleteWithConfirm(summariesFile);
        }
    }

    public static void saveBloomFilter(Descriptor descriptor, IFilter filter)
    {
        File filterFile = new File(descriptor.filenameFor(Component.FILTER));
        try (DataOutputStreamPlus stream = new FileOutputStreamPlus(filterFile))
        {
            BloomFilterSerializer.serialize((BloomFilter) filter, stream);
            stream.flush();
        }
        catch (IOException e)
        {
            logger.trace("Cannot save SSTable bloomfilter: ", e);

            // corrupted hence delete it and let it load it now.
            if (filterFile.exists())
                FileUtils.deleteWithConfirm(filterFile);
        }

    }

    /**
     * Execute provided task with sstable lock to avoid racing with index summary redistribution, SEE CASSANDRA-15861.
     *
     * @param task to be guarded by sstable lock
     */
    public <R> R runWithLock(CheckedFunction<Descriptor, R, IOException> task) throws IOException
    {
        synchronized (tidy.global)
        {
            return task.apply(descriptor);
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

    // These runnables must NOT be an anonymous or non-static inner class, nor must it retain a reference chain to this reader
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
                                                 ifile != null ? ifile.sharedCopy() : null,
                                                 dfile.sharedCopy(),
                                                 newSummary,
                                                 bf.sharedCopy(),
                                                 maxDataAge,
                                                 sstableMetadata,
                                                 reason,
                                                 header);

        replacement.first = newFirst;
        replacement.last = last;
        replacement.isSuspect.set(isSuspect.get());
        return replacement;
    }

    /**
     * Clone this reader with the new values and set the clone as replacement.
     *
     * @param newBloomFilter for the replacement
     *
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    @VisibleForTesting
    public SSTableReader cloneAndReplace(IFilter newBloomFilter)
    {
        SSTableReader replacement = internalOpen(descriptor,
                                                 components,
                                                 metadata,
                                                 ifile.sharedCopy(),
                                                 dfile.sharedCopy(),
                                                 indexSummary,
                                                 newBloomFilter,
                                                 maxDataAge,
                                                 sstableMetadata,
                                                 openReason,
                                                 header);

        replacement.first = first;
        replacement.last = last;
        replacement.isSuspect.set(isSuspect.get());
        return replacement;
    }

    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        synchronized (tidy.global)
        {
            return cloneAndReplace(restoredStart, OpenReason.NORMAL);
        }
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
        final FileHandle dfile;
        final long dfilePosition;
        final FileHandle ifile;
        final long ifilePosition;
        final Runnable andThen;

        private DropPageCache(FileHandle dfile, long dfilePosition, FileHandle ifile, long ifilePosition, Runnable andThen)
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
            if (andThen != null)
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
        assert openReason != OpenReason.EARLY;

        int minIndexInterval = metadata().params.minIndexInterval;
        int maxIndexInterval = metadata().params.maxIndexInterval;
        double effectiveInterval = indexSummary.getEffectiveIndexInterval();

        IndexSummary newSummary;

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
            newSummary = IndexSummaryBuilder.downsample(indexSummary, samplingLevel, minIndexInterval, getPartitioner());
        }
        else
        {
            throw new AssertionError("Attempted to clone SSTableReader with the same index summary sampling level and " +
                    "no adjustments to min/max_index_interval");
        }

        // Always save the resampled index with lock to avoid racing with entire-sstable streaming
        synchronized (tidy.global)
        {
            saveSummary(descriptor, first, last, newSummary);
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
            try (IndexSummaryBuilder summaryBuilder = new IndexSummaryBuilder(estimatedKeys(), metadata().params.minIndexInterval, newSamplingLevel))
            {
                long indexPosition;
                while ((indexPosition = primaryIndex.getFilePointer()) != indexSize)
                {
                    summaryBuilder.maybeAddEntry(decorateKey(ByteBufferUtil.readWithShortLength(primaryIndex)), indexPosition);
                    RowIndexEntry.Serializer.skip(primaryIndex, descriptor.version);
                }

                return summaryBuilder.build(getPartitioner());
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
    }

    private void validate()
    {
        if (this.first.compareTo(this.last) > 0)
        {
            throw new CorruptSSTableException(new IllegalStateException(String.format("SSTable first key %s > last key %s", this.first, this.last)), getFilename());
        }
    }

    /**
     * Gets the position in the index file to start scanning to find the given key (at most indexInterval keys away,
     * modulo downsampling of the index summary). Always returns a {@code value >= 0}
     */
    public long getIndexScanPosition(PartitionPosition key)
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

        return dfile.compressionMetadata().get();
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
        List<IndexesBounds> sampleIndexes = getSampleIndexesForRanges(indexSummary, ranges);
        for (IndexesBounds sampleIndexRange : sampleIndexes)
            sampleKeyCount += (sampleIndexRange.upperPosition - sampleIndexRange.lowerPosition + 1);

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

    private static List<IndexesBounds> getSampleIndexesForRanges(IndexSummary summary, Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<IndexesBounds> positions = new ArrayList<>();

        for (Range<Token> range : Range.normalize(ranges))
        {
            PartitionPosition leftPosition = range.left.maxKeyBound();
            PartitionPosition rightPosition = range.right.maxKeyBound();

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
            positions.add(new IndexesBounds(left, right));
        }
        return positions;
    }

    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        final List<IndexesBounds> indexRanges = getSampleIndexesForRanges(indexSummary, Collections.singletonList(range));

        if (indexRanges.isEmpty())
            return Collections.emptyList();

        return new Iterable<DecoratedKey>()
        {
            public Iterator<DecoratedKey> iterator()
            {
                return new Iterator<DecoratedKey>()
                {
                    private Iterator<IndexesBounds> rangeIter = indexRanges.iterator();
                    private IndexesBounds current;
                    private int idx;

                    public boolean hasNext()
                    {
                        if (current == null || idx > current.upperPosition)
                        {
                            if (rangeIter.hasNext())
                            {
                                current = rangeIter.next();
                                idx = current.lowerPosition;
                                return true;
                            }
                            return false;
                        }

                        return true;
                    }

                    public DecoratedKey next()
                    {
                        byte[] bytes = indexSummary.getKey(idx++);
                        return decorateKey(ByteBuffer.wrap(bytes));
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
    public List<PartitionPositionBounds> getPositionsForRanges(Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<PartitionPositionBounds> positions = new ArrayList<>();
        for (Range<Token> range : Range.normalize(ranges))
        {
            assert !range.isWrapAround() || range.right.isMinimum();
            // truncate the range so it at most covers the sstable
            AbstractBounds<PartitionPosition> bounds = Range.makeRowRange(range);
            PartitionPosition leftBound = bounds.left.compareTo(first) > 0 ? bounds.left : first.getToken().minKeyBound();
            PartitionPosition rightBound = bounds.right.isMinimum() ? last.getToken().maxKeyBound() : bounds.right;

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
            positions.add(new PartitionPositionBounds(left, right));
        }
        return positions;
    }

    public KeyCacheKey getCacheKey(DecoratedKey key)
    {
        return new KeyCacheKey(metadata(), descriptor, key.getKey());
    }

    public void cacheKey(DecoratedKey key, RowIndexEntry info)
    {
        CachingParams caching = metadata().params.caching;

        if (!caching.cacheKeys() || keyCache == null || keyCache.getCapacity() == 0)
            return;

        KeyCacheKey cacheKey = new KeyCacheKey(metadata(), descriptor, key.getKey());
        logger.trace("Adding cache entry for {} -> {}", cacheKey, info);
        keyCache.put(cacheKey, info);
    }

    public RowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        if (isKeyCacheEnabled())
            return getCachedPosition(new KeyCacheKey(metadata(), descriptor, key.getKey()), updateStats);
        return null;
    }

    protected RowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        if (isKeyCacheEnabled())
        {
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

    public boolean isKeyCacheEnabled()
    {
        return keyCache != null && metadata().params.caching.cacheKeys();
    }

    /**
     * Retrieves the position while updating the key cache and the stats.
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     */
    public final RowIndexEntry getPosition(PartitionPosition key, Operator op)
    {
        return getPosition(key, op, SSTableReadsListener.NOOP_LISTENER);
    }

    /**
     * Retrieves the position while updating the key cache and the stats.
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param listener the {@code SSTableReaderListener} that must handle the notifications.
     */
    public final RowIndexEntry getPosition(PartitionPosition key, Operator op, SSTableReadsListener listener)
    {
        return getPosition(key, op, true, false, listener);
    }

    public final RowIndexEntry getPosition(PartitionPosition key,
                                           Operator op,
                                           boolean updateCacheAndStats)
    {
        return getPosition(key, op, updateCacheAndStats, false, SSTableReadsListener.NOOP_LISTENER);
    }

    /**
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @param listener a listener used to handle internal events
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    protected abstract RowIndexEntry getPosition(PartitionPosition key,
                                                 Operator op,
                                                 boolean updateCacheAndStats,
                                                 boolean permitMatchPastLast,
                                                 SSTableReadsListener listener);

    public abstract UnfilteredRowIterator rowIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed);

    public abstract UnfilteredRowIterator simpleIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, boolean tombstoneOnly);

    /**
     * Finds and returns the first key beyond a given token in this SSTable or null if no such key exists.
     */
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        if (token.compareTo(first) < 0)
            return first;

        long sampledPosition = getIndexScanPosition(token);

        if (ifile == null)
            return null;

        String path = null;
        try (FileDataInput in = ifile.createReader(sampledPosition))
        {
            path = in.getPath();
            while (!in.isEOF())
            {
                ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                DecoratedKey indexDecoratedKey = decorateKey(indexKey);
                if (indexDecoratedKey.compareTo(token) > 0)
                    return indexDecoratedKey;

                RowIndexEntry.Serializer.skip(in, descriptor.version);
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, path);
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
        return dfile.dataLength();
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

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return crcCheckChance;
    }

    /**
     * Set the value of CRC check chance. The argument supplied is obtained
     * from the the property of the owning CFS. Called when either the SSTR
     * is initialized, or the CFS's property is updated via JMX
     * @param crcCheckChance
     */
    public void setCrcCheckChance(double crcCheckChance)
    {
        this.crcCheckChance = crcCheckChance;
        dfile.compressionMetadata().ifPresent(metadata -> metadata.parameters.setCrcCheckChance(crcCheckChance));
    }

    /**
     * Mark the sstable as obsolete, i.e., compacted into newer sstables.
     *
     * When calling this function, the caller must ensure that the SSTableReader is not referenced anywhere
     * except for threads holding a reference.
     *
     * multiple times is usually buggy (see exceptions in Tracker.unmarkCompacting and removeOldSSTablesSize).
     */
    public void markObsolete(Runnable tidier)
    {
        if (logger.isTraceEnabled())
            logger.trace("Marking {} compacted", getFilename());

        synchronized (tidy.global)
        {
            assert !tidy.isReplaced;
            assert tidy.global.obsoletion == null: this + " was already marked compacted";

            tidy.global.obsoletion = tidier;
            tidy.global.stopReadMeterPersistence();
        }
    }

    public boolean isMarkedCompacted()
    {
        return tidy.global.obsoletion != null;
    }

    public void markSuspect()
    {
        if (logger.isTraceEnabled())
            logger.trace("Marking {} as a suspect to be excluded from reads.", getFilename());

        isSuspect.getAndSet(true);
    }

    @VisibleForTesting
    public void unmarkSuspect()
    {
        isSuspect.getAndSet(false);
    }

    public boolean isMarkedSuspect()
    {
        return isSuspect.get();
    }

    /**
     * Direct I/O SSTableScanner over a defined range of tokens.
     *
     * @param range the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Range<Token> range)
    {
        if (range == null)
            return getScanner();
        return getScanner(Collections.singletonList(range));
    }

    /**
     * Direct I/O SSTableScanner over the entirety of the sstable..
     *
     * @return A Scanner over the full content of the SSTable.
     */
    public abstract ISSTableScanner getScanner();

    /**
     * Direct I/O SSTableScanner over a defined collection of ranges of tokens.
     *
     * @param ranges the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract ISSTableScanner getScanner(Collection<Range<Token>> ranges);

    /**
     * Direct I/O SSTableScanner over an iterator of bounds.
     *
     * @param rangeIterator the keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator);

    public FileDataInput getFileDataInput(long position)
    {
        return dfile.createReader(position);
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
        createLinks(snapshotDirectoryPath, null);
    }

    public void createLinks(String snapshotDirectoryPath, RateLimiter rateLimiter)
    {
        createLinks(descriptor, components, snapshotDirectoryPath, rateLimiter);
    }

    public static void createLinks(Descriptor descriptor, Set<Component> components, String snapshotDirectoryPath)
    {
        createLinks(descriptor, components, snapshotDirectoryPath, null);
    }

    public static void createLinks(Descriptor descriptor, Set<Component> components, String snapshotDirectoryPath, RateLimiter limiter)
    {
        for (Component component : components)
        {
            File sourceFile = new File(descriptor.filenameFor(component));
            if (!sourceFile.exists())
                continue;
            if (null != limiter)
                limiter.acquire();
            File targetLink = new File(snapshotDirectoryPath, sourceFile.name());
            FileUtils.createHardLink(sourceFile, targetLink);
        }
    }

    public boolean isRepaired()
    {
        return sstableMetadata.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
    }

    public DecoratedKey keyAt(long indexPosition) throws IOException
    {
        DecoratedKey key;
        try (FileDataInput in = ifile.createReader(indexPosition))
        {
            if (in.isEOF())
                return null;

            key = decorateKey(ByteBufferUtil.readWithShortLength(in));

            // hint read path about key location if caching is enabled
            // this saves index summary lookup and index file iteration which whould be pretty costly
            // especially in presence of promoted column indexes
            if (isKeyCacheEnabled())
                cacheKey(key, rowIndexEntrySerializer.deserialize(in));
        }

        return key;
    }

    public boolean isPendingRepair()
    {
        return sstableMetadata.pendingRepair != ActiveRepairService.NO_PENDING_REPAIR;
    }

    public TimeUUID getPendingRepair()
    {
        return sstableMetadata.pendingRepair;
    }

    public long getRepairedAt()
    {
        return sstableMetadata.repairedAt;
    }

    public boolean isTransient()
    {
        return sstableMetadata.isTransient;
    }

    public boolean intersects(Collection<Range<Token>> ranges)
    {
        Bounds<Token> range = new Bounds<>(first.getToken(), last.getToken());
        return Iterables.any(ranges, r -> r.intersects(range));
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

    public long getBloomFilterTrueNegativeCount()
    {
        return bloomFilterTracker.getTrueNegativeCount();
    }

    public long getRecentBloomFilterTrueNegativeCount()
    {
        return bloomFilterTracker.getRecentTrueNegativeCount();
    }

    public InstrumentingCache<KeyCacheKey, RowIndexEntry> getKeyCache()
    {
        return keyCache;
    }

    public EstimatedHistogram getEstimatedPartitionSize()
    {
        return sstableMetadata.estimatedPartitionSize;
    }

    public EstimatedHistogram getEstimatedCellPerPartitionCount()
    {
        return sstableMetadata.estimatedCellPerPartitionCount;
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

    public int getMinLocalDeletionTime()
    {
        return sstableMetadata.minLocalDeletionTime;
    }

    public int getMaxLocalDeletionTime()
    {
        return sstableMetadata.maxLocalDeletionTime;
    }

    /**
     * Whether the sstable may contain tombstones or if it is guaranteed to not contain any.
     * <p>
     * Note that having that method return {@code false} guarantees the sstable has no tombstones whatsoever (so no
     * cell tombstone, no range tombstone maker and no expiring columns), but having it return {@code true} doesn't
     * guarantee it contains any as it may simply have non-expired cells.
     */
    public boolean mayHaveTombstones()
    {
        // A sstable is guaranteed to have no tombstones if minLocalDeletionTime is still set to its default,
        // Cell.NO_DELETION_TIME, which is bigger than any valid deletion times.
        return getMinLocalDeletionTime() != Cell.NO_DELETION_TIME;
    }

    public int getMinTTL()
    {
        return sstableMetadata.minTTL;
    }

    public int getMaxTTL()
    {
        return sstableMetadata.maxTTL;
    }

    public long getTotalColumnsSet()
    {
        return sstableMetadata.totalColumnsSet;
    }

    public long getTotalRows()
    {
        return sstableMetadata.totalRows;
    }

    public int getAvgColumnSetPerRow()
    {
        return sstableMetadata.totalRows < 0
             ? -1
             : (sstableMetadata.totalRows == 0 ? 0 : (int)(sstableMetadata.totalColumnsSet / sstableMetadata.totalRows));
    }

    public int getSSTableLevel()
    {
        return sstableMetadata.sstableLevel;
    }

    /**
     * Mutate sstable level with a lock to avoid racing with entire-sstable-streaming and then reload sstable metadata
     */
    public void mutateLevelAndReload(int newLevel) throws IOException
    {
        synchronized (tidy.global)
        {
            descriptor.getMetadataSerializer().mutateLevel(descriptor, newLevel);
            reloadSSTableMetadata();
        }
    }

    /**
     * Mutate sstable repair metadata with a lock to avoid racing with entire-sstable-streaming and then reload sstable metadata
     */
    public void mutateRepairedAndReload(long newRepairedAt, TimeUUID newPendingRepair, boolean isTransient) throws IOException
    {
        synchronized (tidy.global)
        {
            descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, newRepairedAt, newPendingRepair, isTransient);
            reloadSSTableMetadata();
        }
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
        return dfile.createReader(limiter);
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

    public ChannelProxy getDataChannel()
    {
        return dfile.channel;
    }

    public ChannelProxy getIndexChannel()
    {
        return ifile.channel;
    }

    public FileHandle getIndexFile()
    {
        return ifile;
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
     * Increment the total read count and read rate for this SSTable.  This should not be incremented for non-query reads,
     * like compaction.
     */
    public void incrementReadCount()
    {
        if (readMeter != null)
            readMeter.mark();
    }

    public EncodingStats stats()
    {
        // We could return sstable.header.stats(), but this may not be as accurate than the actual sstable stats (see
        // SerializationHeader.make() for details) so we use the latter instead.
        return sstableMetadata.encodingStats;
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
        tidy.setup(this, TRACK_ACTIVITY && trackHotness);
        this.readMeter = tidy.global.readMeter;
    }

    @VisibleForTesting
    public void overrideReadMeter(RestorableMeter readMeter)
    {
        this.readMeter = tidy.global.readMeter = readMeter;
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        identities.add(this);
        identities.add(tidy.globalRef);
        dfile.addTo(identities);
        ifile.addTo(identities);
        bf.addTo(identities);
        indexSummary.addTo(identities);

    }

    public boolean maybePresent(DecoratedKey key)
    {
        // if we don't have bloom filter(bf_fp_chance=1.0 or filter file is missing),
        // we check index file instead.
        return bf instanceof AlwaysPresentFilter && getPosition(key, Operator.EQ, false) != null || bf.isPresent(key);
    }

    /**
     * One instance per SSTableReader we create.
     *
     * We can create many InstanceTidiers (one for every time we reopen an sstable with MOVED_START for example),
     * but there can only be one GlobalTidy for one single logical sstable.
     *
     * When the InstanceTidier cleansup, it releases its reference to its GlobalTidy; when all InstanceTidiers
     * for that type have run, the GlobalTidy cleans up.
     */
    private static final class InstanceTidier implements Tidy
    {
        private final Descriptor descriptor;
        private final TableId tableId;
        private IFilter bf;
        private IndexSummary summary;

        private FileHandle dfile;
        private FileHandle ifile;
        private Runnable runOnClose;
        private boolean isReplaced = false;

        // a reference to our shared tidy instance, that
        // we will release when we are ourselves released
        private Ref<GlobalTidy> globalRef;
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
            this.globalRef = GlobalTidy.get(reader);
            this.global = globalRef.get();
            if (trackHotness)
                global.ensureReadMeter();
        }

        InstanceTidier(Descriptor descriptor, TableId tableId)
        {
            this.descriptor = descriptor;
            this.tableId = tableId;
        }

        public void tidy()
        {
            if (logger.isTraceEnabled())
                logger.trace("Running instance tidier for {} with setup {}", descriptor, setup);

            // don't try to cleanup if the sstablereader was never fully constructed
            if (!setup)
                return;

            final ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);
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
                    globalRef.release();

                    if (logger.isTraceEnabled())
                        logger.trace("Async instance tidier for {}, completed", descriptor);
                }

                @Override
                public String toString()
                {
                    return "Tidy " + descriptor.ksname + '.' + descriptor.cfname + '-' + descriptor.id;
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
     * One instance per logical sstable. This both tracks shared cleanup and some shared state related
     * to the sstable's lifecycle.
     *
     * All InstanceTidiers, on setup(), ask the static get() method for their shared state,
     * and stash a reference to it to be released when they are. Once all such references are
     * released, this shared tidy will be performed.
     */
    static final class GlobalTidy implements Tidy
    {
        static final WeakReference<ScheduledFuture<?>> NULL = new WeakReference<>(null);
        // keyed by descriptor, mapping to the shared GlobalTidy for that descriptor
        static final ConcurrentMap<Descriptor, Ref<GlobalTidy>> lookup = new ConcurrentHashMap<>();

        private final Descriptor desc;
        // the readMeter that is shared between all instances of the sstable, and can be overridden in all of them
        // at once also, for testing purposes
        private RestorableMeter readMeter;
        // the scheduled persistence of the readMeter, that we will cancel once all instances of this logical
        // sstable have been released
        private WeakReference<ScheduledFuture<?>> readMeterSyncFuture = NULL;
        // shared state managing if the logical sstable has been compacted; this is used in cleanup
        private volatile Runnable obsoletion;

        GlobalTidy(final SSTableReader reader)
        {
            this.desc = reader.descriptor;
        }

        void ensureReadMeter()
        {
            if (readMeter != null)
                return;

            // Don't track read rates for tables in the system keyspace and don't bother trying to load or persist
            // the read meter when in client mode.
            // Also, do not track read rates when running in client or tools mode (syncExecuter isn't available in these modes)
            if (!TRACK_ACTIVITY || SchemaConstants.isLocalSystemKeyspace(desc.ksname) || DatabaseDescriptor.isClientOrToolInitialized())
            {
                readMeter = null;
                readMeterSyncFuture = NULL;
                return;
            }

            readMeter = SystemKeyspace.getSSTableReadMeter(desc.ksname, desc.cfname, desc.id);
            // sync the average read rate to system.sstable_activity every five minutes, starting one minute from now
            readMeterSyncFuture = new WeakReference<>(syncExecutor.scheduleAtFixedRate(new Runnable()
            {
                public void run()
                {
                    if (obsoletion == null)
                    {
                        meterSyncThrottle.acquire();
                        SystemKeyspace.persistSSTableReadMeter(desc.ksname, desc.cfname, desc.id, readMeter);
                    }
                }
            }, 1, 5, TimeUnit.MINUTES));
        }

        private void stopReadMeterPersistence()
        {
            ScheduledFuture<?> readMeterSyncFutureLocal = readMeterSyncFuture.get();
            if (readMeterSyncFutureLocal != null)
            {
                readMeterSyncFutureLocal.cancel(true);
                readMeterSyncFuture = NULL;
            }
        }

        public void tidy()
        {
            lookup.remove(desc);

            if (obsoletion != null)
                obsoletion.run();

            // don't ideally want to dropPageCache for the file until all instances have been released
            NativeLibrary.trySkipCache(desc.filenameFor(Component.DATA), 0, 0);
            NativeLibrary.trySkipCache(desc.filenameFor(Component.PRIMARY_INDEX), 0, 0);
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

            while (true)
            {
                Ref<GlobalTidy> ref = lookup.get(descriptor);
                if (ref == null)
                {
                    final GlobalTidy tidy = new GlobalTidy(sstable);
                    ref = new Ref<>(tidy, tidy);
                    Ref<GlobalTidy> ex = lookup.putIfAbsent(descriptor, ref);
                    if (ex == null)
                        return ref;
                    ref = ex;
                }

                Ref<GlobalTidy> newRef = ref.tryRef();
                if (newRef != null)
                    return newRef;

                // raced with tidy
                lookup.remove(descriptor, ref);
            }
        }
    }

    @VisibleForTesting
    public static void resetTidying()
    {
        GlobalTidy.lookup.clear();
    }

    public static abstract class Factory
    {
        public abstract SSTableReader open(SSTableReaderBuilder builder);
    }

    public static class PartitionPositionBounds
    {
        public final long lowerPosition;
        public final long upperPosition;

        public PartitionPositionBounds(long lower, long upper)
        {
            this.lowerPosition = lower;
            this.upperPosition = upper;
        }

        @Override
        public final int hashCode()
        {
            int hashCode = (int) lowerPosition ^ (int) (lowerPosition >>> 32);
            return 31 * (hashCode ^ (int) ((int) upperPosition ^  (upperPosition >>> 32)));
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof PartitionPositionBounds))
                return false;
            PartitionPositionBounds that = (PartitionPositionBounds)o;
            return lowerPosition == that.lowerPosition && upperPosition == that.upperPosition;
        }
    }

    public static class IndexesBounds
    {
        public final int lowerPosition;
        public final int upperPosition;

        public IndexesBounds(int lower, int upper)
        {
            this.lowerPosition = lower;
            this.upperPosition = upper;
        }

        @Override
        public final int hashCode()
        {
            return 31 * lowerPosition * upperPosition;
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof IndexesBounds))
                return false;
            IndexesBounds that = (IndexesBounds) o;
            return lowerPosition == that.lowerPosition && upperPosition == that.upperPosition;
        }
    }

    /**
     * Moves the sstable in oldDescriptor to a new place (with generation etc) in newDescriptor.
     *
     * All components given will be moved/renamed
     */
    public static SSTableReader moveAndOpenSSTable(ColumnFamilyStore cfs, Descriptor oldDescriptor, Descriptor newDescriptor, Set<Component> components, boolean copyData)
    {
        if (!oldDescriptor.isCompatible())
            throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                                                     oldDescriptor.getFormat().getLatestVersion(),
                                                     oldDescriptor));

        boolean isLive = cfs.getLiveSSTables().stream().anyMatch(r -> r.descriptor.equals(newDescriptor)
                                                                      || r.descriptor.equals(oldDescriptor));
        if (isLive)
        {
            String message = String.format("Can't move and open a file that is already in use in the table %s -> %s", oldDescriptor, newDescriptor);
            logger.error(message);
            throw new RuntimeException(message);
        }
        if (new File(newDescriptor.filenameFor(Component.DATA)).exists())
        {
            String msg = String.format("File %s already exists, can't move the file there", newDescriptor.filenameFor(Component.DATA));
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        if (copyData)
        {
            try
            {
                logger.info("Hardlinking new SSTable {} to {}", oldDescriptor, newDescriptor);
                SSTableWriter.hardlink(oldDescriptor, newDescriptor, components);
            }
            catch (FSWriteError ex)
            {
                logger.warn("Unable to hardlink new SSTable {} to {}, falling back to copying", oldDescriptor, newDescriptor, ex);
                SSTableWriter.copy(oldDescriptor, newDescriptor, components);
            }
        }
        else
        {
            logger.info("Moving new SSTable {} to {}", oldDescriptor, newDescriptor);
            SSTableWriter.rename(oldDescriptor, newDescriptor, components);
        }

        SSTableReader reader;
        try
        {
            reader = SSTableReader.open(newDescriptor, components, cfs.metadata);
        }
        catch (Throwable t)
        {
            logger.error("Aborting import of sstables. {} was corrupt", newDescriptor);
            throw new RuntimeException(newDescriptor + " is corrupt, can't import", t);
        }
        return reader;
    }

    public static void shutdownBlocking(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {

        ExecutorUtils.shutdownNowAndWait(timeout, unit, syncExecutor);
        resetTidying();
    }
}
