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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSource;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.CheckedFunction;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseable;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;
import static org.apache.cassandra.utils.concurrent.SharedCloseable.sharedCopyOrNull;

/**
 * An SSTableReader can be constructed in a number of places, but typically is either read from disk at startup, or
 * constructed from a flushed memtable, or after compaction to replace some existing sstables. However once created,
 * an sstablereader may also be modified.
 * <p>
 * A reader's {@link OpenReason} describes its current stage in its lifecycle. Note that in parallel to this, there are
 * two different Descriptor types; TMPLINK and FINAL; the latter corresponds to {@link OpenReason#NORMAL} state readers
 * and all readers that replace a {@link OpenReason#NORMAL} one. TMPLINK is used for {@link OpenReason#EARLY} state
 * readers and no others.
 * <p>
 * When a reader is being compacted, if the result is large its replacement may be opened as {@link OpenReason#EARLY}
 * before compaction completes in order to present the result to consumers earlier. In this case the reader will itself
 * be changed to a {@link OpenReason#MOVED_START} state, where its start no longer represents its on-disk minimum key.
 * This is to permit reads to be directed to only one reader when the two represent the same data.
 * The {@link OpenReason#EARLY} file can represent a compaction result that is either partially complete and still
 * in-progress, or a complete and immutable sstable that is part of a larger macro compaction action that has not yet
 * fully completed.
 * <p>
 * Currently ALL compaction results at least briefly go through an {@link OpenReason#EARLY} open state prior to completion,
 * regardless of if early opening is enabled.
 * <p>
 * Since a reader can be created multiple times over the same shared underlying resources, and the exact resources it
 * shares between each instance differ subtly, we track the lifetime of any underlying resource with its own reference
 * count, which each instance takes a {@link Ref} to. Each instance then tracks references to itself, and once these
 * all expire it releases all its {@link Ref} to these underlying resources.
 * <p>
 * There is some shared cleanup behaviour needed only once all readers in a certain stage of their lifecycle
 * (i.e. {@link OpenReason#EARLY} or {@link OpenReason#NORMAL} opening), and some that must only occur once all readers
 * of any kind over a single logical sstable have expired. These are managed by the {@link InstanceTidier} and
 * {@link GlobalTidy} classes at the bottom, and are effectively managed as another resource each instance tracks its
 * own {@link Ref} instance to, to ensure all of these resources are cleaned up safely and can be debugged otherwise.
 * <p>
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
            ((ScheduledThreadPoolExecutor) syncExecutor).setRemoveOnCancelPolicy(true);
        return syncExecutor;
    }

    private static final RateLimiter meterSyncThrottle = RateLimiter.create(100.0);

    public static final Comparator<SSTableReader> maxTimestampAscending = Comparator.comparingLong(SSTableReader::getMaxTimestamp);
    public static final Comparator<SSTableReader> maxTimestampDescending = maxTimestampAscending.reversed();

    // it's just an object, which we use regular Object equality on; we introduce a special class just for easy recognition
    public static final class UniqueIdentifier
    {
    }
    public final UniqueIdentifier instanceId = new UniqueIdentifier();

    public static final Comparator<SSTableReader> firstKeyComparator = (o1, o2) -> o1.getFirst().compareTo(o2.getFirst());
    public static final Ordering<SSTableReader> firstKeyOrdering = Ordering.from(firstKeyComparator);
    public static final Comparator<SSTableReader> lastKeyComparator = (o1, o2) -> o1.getLast().compareTo(o2.getLast());

    public static final Comparator<SSTableReader> idComparator = Comparator.comparing(t -> t.descriptor.id, SSTableIdFactory.COMPARATOR);
    public static final Comparator<SSTableReader> idReverseComparator = idComparator.reversed();

    public static final Comparator<SSTableReader> sizeComparator = (o1, o2) -> Longs.compare(o1.onDiskLength(), o2.onDiskLength());

    /**
     * maxDataAge is a timestamp in local server time (e.g. Global.currentTimeMilli) which represents an upper bound
     * to the newest piece of data stored in the sstable. In other words, this sstable does not contain items created
     * later than maxDataAge.
     * <p>
     * The field is not serialized to disk, so relying on it for more than what truncate does is not advised.
     * <p>
     * When a new sstable is flushed, maxDataAge is set to the time of creation.
     * When a sstable is created from compaction, maxDataAge is set to max of all merged sstables.
     * <p>
     * The age is in milliseconds since epoc and is local to this host.
     */
    public final long maxDataAge;

    public enum OpenReason
    {
        /**
         * <ul>
         * <li>From {@code None} - Reader has been read from disk, either at startup or from a flushed memtable</li>
         * <li>From {@link #EARLY} - Reader is the final result of a compaction</li>
         * <li>From {@link #MOVED_START} - Reader WAS being compacted, but this failed and it has been restored
         *     to {code NORMAL}status</li>
         * </ul>
         */
        NORMAL,

        /**
         * <ul>
         * <li>From {@code None} - Reader is a compaction replacement that is either incomplete and has been opened
         *     to represent its partial result status, or has been finished but the compaction it is a part of has not
         *     yet completed fully</li>
         * <li>From {@link #EARLY} - Same as from {@code None}, only it is not the first time it has been
         * </ul>
         */
        EARLY,

        /**
         * From:
         * <ul>
         * <li>From {@link #NORMAL} - Reader has seen low traffic and the amount of memory available for index summaries
         *     is constrained, so its index summary has been downsampled</li>
         * <li>From {@link #METADATA_CHANGE} - Same
         * </ul>
         */
        METADATA_CHANGE,

        /**
         * <ul>
         * <li>From {@link #NORMAL} - Reader is being compacted. This compaction has not finished, but the compaction
         *     result is either partially or fully opened, to either partially or fully replace this reader. This reader's
         *     start key has been updated to represent this, so that reads only hit one or the other reader.</li>
         * </ul>
         */
        MOVED_START
    }

    public final OpenReason openReason;

    protected final FileHandle dfile;

    // technically isCompacted is not necessary since it should never be unreferenced unless it is also compacted,
    // but it seems like a good extra layer of protection against reference counting bugs to not delete data based on that alone
    public final AtomicBoolean isSuspect = new AtomicBoolean(false);

    // not final since we need to be able to change level on a file.
    protected volatile StatsMetadata sstableMetadata;

    public final SerializationHeader header;

    private final InstanceTidier tidy;
    private final Ref<SSTableReader> selfRef;

    private RestorableMeter readMeter;

    private volatile double crcCheckChance;

    protected final DecoratedKey first;
    protected final DecoratedKey last;
    public final AbstractBounds<Token> bounds;

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
                CompactionMetadata metadata = StatsComponent.load(sstable.descriptor).compactionMetadata();
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

    public static SSTableReader open(SSTable.Owner owner, Descriptor descriptor)
    {
        return open(owner, descriptor, null);
    }

    public static SSTableReader open(SSTable.Owner owner, Descriptor desc, TableMetadataRef metadata)
    {
        return open(owner, desc, null, metadata);
    }

    public static SSTableReader open(SSTable.Owner owner, Descriptor descriptor, Set<Component> components, TableMetadataRef metadata)
    {
        return open(owner, descriptor, components, metadata, true, false);
    }

    // use only for offline or "Standalone" operations
    public static SSTableReader openNoValidation(Descriptor descriptor, Set<Component> components, ColumnFamilyStore cfs)
    {
        return open(cfs, descriptor, components, cfs.metadata, false, true);
    }

    // use only for offline or "Standalone" operations
    public static SSTableReader openNoValidation(SSTable.Owner owner, Descriptor descriptor, TableMetadataRef metadata)
    {
        return open(owner, descriptor, null, metadata, false, true);
    }

    /**
     * Open SSTable reader to be used in batch mode(such as sstableloader).
     */
    public static SSTableReader openForBatch(SSTable.Owner owner, Descriptor descriptor, Set<Component> components, TableMetadataRef metadata)
    {
        return open(owner, descriptor, components, metadata, true, true);
    }

    /**
     * Open an SSTable for reading
     *
     * @param owner      owning entity
     * @param descriptor SSTable to open
     * @param components Components included with this SSTable
     * @param metadata   for this SSTables CF
     * @param validate   Check SSTable for corruption (limited)
     * @param isOffline  Whether we are opening this SSTable "offline", for example from an external tool or not for inclusion in queries (validations)
     *                   This stops regenerating BF + Summaries and also disables tracking of hotness for the SSTable.
     * @return {@link SSTableReader}
     */
    public static SSTableReader open(Owner owner,
                                     Descriptor descriptor,
                                     Set<Component> components,
                                     TableMetadataRef metadata,
                                     boolean validate,
                                     boolean isOffline)
    {
        SSTableReaderLoadingBuilder<?, ?> builder = descriptor.getFormat().getReaderFactory().loadingBuilder(descriptor, metadata, components);

        return builder.build(owner, validate, !isOffline);
    }

    public static Collection<SSTableReader> openAll(SSTable.Owner owner, Set<Map.Entry<Descriptor, Set<Component>>> entries,
                                                    final TableMetadataRef metadata)
    {
        final Collection<SSTableReader> sstables = newBlockingQueue();

        ExecutorPlus executor = executorFactory().pooled("SSTableBatchOpen", FBUtilities.getAvailableProcessors());
        try
        {
            for (final Map.Entry<Descriptor, Set<Component>> entry : entries)
            {
                Runnable runnable = () -> {
                    SSTableReader sstable;
                    try
                    {
                        sstable = open(owner, entry.getKey(), entry.getValue(), metadata);
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

    protected SSTableReader(Builder<?, ?> builder, Owner owner)
    {
        super(builder, owner);

        this.sstableMetadata = builder.getStatsMetadata();
        this.header = builder.getSerializationHeader();
        this.dfile = builder.getDataFile();
        this.maxDataAge = builder.getMaxDataAge();
        this.openReason = builder.getOpenReason();
        this.first = builder.getFirst();
        this.last = builder.getLast();
        this.bounds = first == null || last == null || AbstractBounds.strictlyWrapsAround(first.getToken(), last.getToken())
                      ? null // this will cause the validation to fail, but the reader is opened with no validation,
                             // e.g. for scrubbing, we should accept screwed bounds
                      : AbstractBounds.bounds(first.getToken(), true, last.getToken(), true);

        tidy = new InstanceTidier(descriptor, owner);
        selfRef = new Ref<>(this, tidy);
    }

    @Override
    public DecoratedKey getFirst()
    {
        return first;
    }

    @Override
    public DecoratedKey getLast()
    {
        return last;
    }

    @Override
    public AbstractBounds<Token> getBounds()
    {
        return Objects.requireNonNull(bounds, "Bounds were not created because the sstable is out of order");
    }

    public DataIntegrityMetadata.ChecksumValidator maybeGetChecksumValidator() throws IOException
    {
        if (descriptor.fileFor(Components.CRC).exists())
            return new DataIntegrityMetadata.ChecksumValidator(descriptor.fileFor(Components.DATA), descriptor.fileFor(Components.CRC));
        else
            return null;
    }

    public DataIntegrityMetadata.FileDigestValidator maybeGetDigestValidator() throws IOException
    {
        if (descriptor.fileFor(Components.DIGEST).exists())
            return new DataIntegrityMetadata.FileDigestValidator(descriptor.fileFor(Components.DATA), descriptor.fileFor(Components.DIGEST));
        else
            return null;
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
         owner().ifPresent(o -> setCrcCheckChance(o.getCrcCheckChance()));
    }

    /**
     * Execute provided task with sstable lock to avoid racing with index summary redistribution, SEE CASSANDRA-15861.
     *
     * @param task to be guarded by sstable lock
     */
    public <R, E extends Exception> R runWithLock(CheckedFunction<Descriptor, R, E> task) throws E
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

    /**
     * The runnable passed to this method must not be an anonymous or non-static inner class. It can be a lambda or a
     * method reference provided that it does not retain a reference chain to this reader.
     */
    public void runOnClose(final Runnable runOnClose)
    {
        if (runOnClose == null)
            return;

        synchronized (tidy.global)
        {
            final Runnable existing = tidy.runOnClose;
            if (existing == null)
                tidy.runOnClose = runOnClose;
            else
                tidy.runOnClose = () -> {
                    existing.run();
                    runOnClose.run();
                };
        }
    }

    /**
     * The method sets fields specific to this {@link SSTableReader} and the parent {@link SSTable} on the provided
     * {@link Builder}. The method is intended to be called from the overloaded {@code unbuildTo} method in subclasses.
     *
     * @param builder    the builder on which the fields should be set
     * @param sharedCopy whether the {@link SharedCloseable} resources should be passed as shared copies or directly;
     *                   note that the method will overwrite the fields representing {@link SharedCloseable} only if
     *                   they are not set in the builder yet (the relevant fields in the builder are {@code null}).
     * @return the same instance of builder as provided
     */
    protected final <B extends Builder<?, B>> B unbuildTo(B builder, boolean sharedCopy)
    {
        B b = super.unbuildTo(builder, sharedCopy);
        if (builder.getDataFile() == null)
            b.setDataFile(sharedCopy ? sharedCopyOrNull(dfile) : dfile);

        b.setStatsMetadata(sstableMetadata);
        b.setSerializationHeader(header);
        b.setMaxDataAge(maxDataAge);
        b.setOpenReason(openReason);
        b.setFirst(first);
        b.setLast(last);
        b.setSuspected(isSuspect.get());
        return b;
    }

    public abstract SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart);

    public abstract SSTableReader cloneWithNewStart(DecoratedKey newStart);

    public RestorableMeter getReadMeter()
    {
        return readMeter;
    }

    /**
     * All the resources which should be released upon closing this sstable reader are registered with in
     * {@link GlobalTidy}. This method lets close a provided resource explicitly any time and unregister it from
     * {@link GlobalTidy} so that it is not tried to be released twice.
     *
     * @param closeable a resource to be closed
     */
    protected void closeInternalComponent(AutoCloseable closeable)
    {
        synchronized (tidy.global)
        {
            boolean removed = tidy.closeables.remove(closeable);
            Preconditions.checkState(removed);
            try
            {
                closeable.close();
            }
            catch (Exception ex)
            {
                throw new RuntimeException("Failed to close " + closeable, ex);
            }
        }
    }

    /**
     * This method is expected to close the components which occupy memory but are not needed when we just want to
     * stream the components (for example, when SSTable is opened with SSTableLoader). The method should call
     * {@link #closeInternalComponent(AutoCloseable)} for each such component. Leaving the implementation empty is
     * valid given there are not such resources to release.
     */
    public abstract void releaseInMemoryComponents();

    /**
     * Perform any validation needed for the reader upon creation before returning it from the {@link Builder}.
     */
    public void validate()
    {
        if (this.first.compareTo(this.last) > 0 || bounds == null)
        {
            throw new CorruptSSTableException(new IllegalStateException(String.format("SSTable first key %s > last key %s", this.first, this.last)), getFilename());
        }
    }

    /**
     * Returns the compression metadata for this sstable. Note that the compression metdata is a resource and should not
     * be closed by the caller.
     * TODO do not return a closeable resource or return a shared copy
     *
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
     *
     * @return the amount of memory in bytes used off heap by the compression meta-data
     */
    public long getCompressionMetadataOffHeapSize()
    {
        if (!compression)
            return 0;

        return getCompressionMetadata().offHeapSize();
    }

    /**
     * Calculates an estimate of the number of keys in the sstable represented by this reader.
     */
    public abstract long estimatedKeys();

    /**
     * Calculates an estimate of the number of keys for the given ranges in the sstable represented by this reader.
     */
    public abstract long estimatedKeysForRanges(Collection<Range<Token>> ranges);

    /**
     * Returns whether methods like {@link #estimatedKeys()} or {@link #estimatedKeysForRanges(Collection)} can return
     * sensible estimations.
     */
    public abstract boolean isEstimationInformative();

    /**
     * Returns sample keys for the provided token range.
     */
    public abstract Iterable<DecoratedKey> getKeySamples(final Range<Token> range);

    /**
     * Determine the minimal set of sections that can be extracted from this SSTable to cover the given ranges.
     *
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

            long left = getPosition(leftBound, Operator.GT);
            long right = (rightBound.compareTo(last) > 0)
                         ? uncompressedLength()
                         : getPosition(rightBound, Operator.GT);

            if (left == right)
                // empty range
                continue;

            assert left < right : String.format("Range=%s openReason=%s first=%s last=%s left=%d right=%d", range, openReason, first, last, left, right);
            positions.add(new PartitionPositionBounds(left, right));
        }
        return positions;
    }

    /**
     * Retrieves the position while updating the key cache and the stats.
     *
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     *            allow key selection by token bounds but only if op != * EQ
     * @param op  The Operator defining matching keys: the nearest key to the target matching the operator wins.
     */
    public final long getPosition(PartitionPosition key, Operator op)
    {
        return getPosition(key, op, SSTableReadsListener.NOOP_LISTENER);
    }

    public final long getPosition(PartitionPosition key, Operator op, SSTableReadsListener listener)
    {
        return getPosition(key, op, true, listener);
    }

    public final long getPosition(PartitionPosition key,
                                  Operator op,
                                  boolean updateStats)
    {
        return getPosition(key, op, updateStats, SSTableReadsListener.NOOP_LISTENER);
    }

    /**
     * Retrieve a position in data file according to the provided key and operator.
     *
     * @param key         The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     *                    allow key selection by token bounds but only if op != * EQ
     * @param op          The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateStats true if updating stats and cache
     * @param listener    a listener used to handle internal events
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    protected long getPosition(PartitionPosition key,
                               Operator op,
                               boolean updateStats,
                               SSTableReadsListener listener)
    {
        AbstractRowIndexEntry rie = getRowIndexEntry(key, op, updateStats, listener);
        return rie != null ? rie.position : -1;
    }

    /**
     * Retrieve an index entry for the partition found according to the provided key and operator.
     *
     * @param key         The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     *                    allow key selection by token bounds but only if op != * EQ
     * @param op          The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateStats true if updating stats and cache
     * @param listener    a listener used to handle internal events
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    @VisibleForTesting
    protected abstract AbstractRowIndexEntry getRowIndexEntry(PartitionPosition key,
                                                              Operator op,
                                                              boolean updateStats,
                                                              SSTableReadsListener listener);

    public UnfilteredRowIterator simpleIterator(FileDataInput file, DecoratedKey key, long dataPosition, boolean tombstoneOnly)
    {
        return SSTableIdentityIterator.create(this, file, dataPosition, key, tombstoneOnly);
    }

    /**
     * Returns a {@link KeyReader} over all keys in the sstable.
     */
    public abstract KeyReader keyReader() throws IOException;

    /**
     * Returns a {@link KeyIterator} over all keys in the sstable.
     */
    public KeyIterator keyIterator() throws IOException
    {
        return new KeyIterator(keyReader(), getPartitioner(), uncompressedLength(), new ReentrantReadWriteLock());
    }

    /**
     * Finds and returns the first key beyond a given token in this SSTable or null if no such key exists.
     */
    public abstract DecoratedKey firstKeyBeyond(PartitionPosition token);

    /**
     * Returns the length in bytes of the (uncompressed) data for this SSTable. For compressed files, this is not
     * the same thing as the on disk size (see {@link #onDiskLength()}).
     */
    public long uncompressedLength()
    {
        return dfile.dataLength();
    }

    /**
     * @return the fraction of the token space for which this sstable has content. In the simplest case this is just the
     * size of the interval returned by {@link #getBounds()}, but the sstable may contain "holes" when the locally-owned
     * range is not contiguous (e.g. with vnodes).
     * As this is affected by the local ranges which can change, the token space fraction is calculated at the time of
     * writing the sstable and stored with its metadata.
     * For older sstables that do not contain this metadata field, this method returns NaN.
     */
    public double tokenSpaceCoverage()
    {
        return sstableMetadata.tokenSpaceCoverage;
    }

    /**
     * The length in bytes of the on disk size for this SSTable. For compressed files, this is not the same thing
     * as the data length (see {@link #uncompressedLength()}).
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
     * Set the value of CRC check chance. The argument supplied is obtained from the property of the owning CFS.
     * Called when either the SSTR is initialized, or the CFS's property is updated via JMX
     */
    public void setCrcCheckChance(double crcCheckChance)
    {
        this.crcCheckChance = crcCheckChance;
    }

    /**
     * Mark the sstable as obsolete, i.e., compacted into newer sstables.
     * <p>
     * When calling this function, the caller must ensure that the SSTableReader is not referenced anywhere except for
     * threads holding a reference.
     * <p>
     * Calling it multiple times is usually buggy.
     */
    public void markObsolete(Runnable tidier)
    {
        if (logger.isTraceEnabled())
            logger.trace("Marking {} compacted", getFilename());

        synchronized (tidy.global)
        {
            assert !tidy.isReplaced;
            assert tidy.global.obsoletion == null : this + " was already marked compacted";

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

    /**
     * Create a {@link FileDataInput} for the data file of the sstable represented by this reader. This method returns
     * a newly opened resource which must be closed by the caller.
     *
     * @param position the data input will be opened and seek to this position
     */
    public FileDataInput getFileDataInput(long position)
    {
        return dfile.createReader(position);
    }

    /**
     * Tests if the sstable contains data newer than the given age param (in localhost currentMillis time).
     * This works in conjunction with maxDataAge which is an upper bound on the data in the sstable represented
     * by this reader.
     *
     * @return {@code true} iff this sstable contains data that's newer than the given timestamp
     */
    public boolean newSince(long timestampMillis)
    {
        return maxDataAge > timestampMillis;
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
            File sourceFile = descriptor.fileFor(component);
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

    /**
     * Reads the key stored at the position saved in SASI.
     * <p>
     * When SASI is created, it uses key locations retrieved from {@link KeyReader#keyPositionForSecondaryIndex()}.
     * This method is to read the key stored at such position. It is up to the concrete SSTable format implementation
     * what that position means and which file it refers. The only requirement is that it is consistent with what
     * {@link KeyReader#keyPositionForSecondaryIndex()} returns.
     *
     * @return key if found, {@code null} otherwise
     */
    public abstract DecoratedKey keyAtPositionFromSecondaryIndex(long keyPositionFromSecondaryIndex) throws IOException;

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
            public int apply(int comparison)
            {
                return -comparison;
            }
        }

        final static class GreaterThanOrEqualTo extends Operator
        {
            public int apply(int comparison)
            {
                return comparison >= 0 ? 0 : 1;
            }
        }

        final static class GreaterThan extends Operator
        {
            public int apply(int comparison)
            {
                return comparison > 0 ? 0 : 1;
            }
        }
    }

    public EstimatedHistogram getEstimatedPartitionSize()
    {
        return sstableMetadata.estimatedPartitionSize;
    }

    public EstimatedHistogram getEstimatedCellPerPartitionCount()
    {
        return sstableMetadata.estimatedCellPerPartitionCount;
    }

    public double getEstimatedDroppableTombstoneRatio(long gcBefore)
    {
        return sstableMetadata.getEstimatedDroppableTombstoneRatio(gcBefore);
    }

    public double getDroppableTombstonesBefore(long gcBefore)
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

    public long getMinLocalDeletionTime()
    {
        return sstableMetadata.minLocalDeletionTime;
    }

    public long getMaxLocalDeletionTime()
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
               : (sstableMetadata.totalRows == 0 ? 0 : (int) (sstableMetadata.totalColumnsSet / sstableMetadata.totalRows));
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
     * <p>
     * Called after level is changed on sstable, for example if the sstable is dropped to L0
     * <p>
     * Might be possible to remove in future versions
     *
     * @throws IOException
     */
    public void reloadSSTableMetadata() throws IOException
    {
        this.sstableMetadata = StatsComponent.load(descriptor).statsMetadata();
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

    public void trySkipFileCacheBefore(DecoratedKey key)
    {
        long position = getPosition(key, SSTableReader.Operator.GE);
        NativeLibrary.trySkipCache(descriptor.fileFor(Components.DATA).absolutePath(), 0, position < 0 ? 0 : position);
    }

    public ChannelProxy getDataChannel()
    {
        return dfile.channel;
    }

    /**
     * @return last modified time for data component. 0 if given component does not exist or IO error occurs.
     */
    public long getDataCreationTime()
    {
        return descriptor.fileFor(Components.DATA).lastModified();
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

    protected List<AutoCloseable> setupInstance(boolean trackHotness)
    {
        return Collections.singletonList(dfile);
    }

    public void setup(boolean trackHotness)
    {
        assert tidy.closeables == null;
        trackHotness &= TRACK_ACTIVITY;
        tidy.setup(this, trackHotness, setupInstance(trackHotness));
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
        tidy.closeables.forEach(c -> {
            if (c instanceof SharedCloseable)
                ((SharedCloseable) c).addTo(identities);
        });
    }

    /**
     * The method verifies whether the sstable may contain the provided key. The method does approximation using
     * Bloom filter if it is present and if it is not, performs accurate check in the index.
     */
    public abstract boolean mayContainAssumingKeyIsInRange(DecoratedKey key);

    /**
     * One instance per SSTableReader we create.
     * <p>
     * We can create many InstanceTidiers (one for every time we reopen an sstable with MOVED_START for example),
     * but there can only be one GlobalTidy for one single logical sstable.
     * <p>
     * When the InstanceTidier cleansup, it releases its reference to its GlobalTidy; when all InstanceTidiers
     * for that type have run, the GlobalTidy cleans up.
     */
    protected static final class InstanceTidier implements Tidy
    {
        private final Descriptor descriptor;
        private final WeakReference<Owner> owner;

        private List<? extends AutoCloseable> closeables;
        private Runnable runOnClose;

        private boolean isReplaced = false;

        // a reference to our shared tidy instance, that
        // we will release when we are ourselves released
        private Ref<GlobalTidy> globalRef;
        private GlobalTidy global;

        private volatile boolean setup;

        public void setup(SSTableReader reader, boolean trackHotness, Collection<? extends AutoCloseable> closeables)
        {
            // get a new reference to the shared descriptor-type tidy
            this.globalRef = GlobalTidy.get(reader);
            this.global = globalRef.get();
            if (trackHotness)
                global.ensureReadMeter();
            this.closeables = new ArrayList<>(closeables);
            // to avoid tidy seeing partial state, set setup=true at the end
            this.setup = true;
        }

        private InstanceTidier(Descriptor descriptor, Owner owner)
        {
            this.descriptor = descriptor;
            this.owner = new WeakReference<>(owner);
        }

        @Override
        public void tidy()
        {
            if (logger.isTraceEnabled())
                logger.trace("Running instance tidier for {} with setup {}", descriptor, setup);

            // don't try to cleanup if the sstablereader was never fully constructed
            if (!setup)
                return;

            final OpOrder.Barrier barrier;
            Owner owner = this.owner.get();
            if (owner != null)
            {
                barrier = owner.newReadOrderingBarrier();
                barrier.issue();
            }
            else
            {
                barrier = null;
            }

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

                    Throwable exceptions = null;
                    if (runOnClose != null) try
                    {
                        runOnClose.run();
                    }
                    catch (RuntimeException | Error ex)
                    {
                        logger.error("Failed to run on-close listeners for sstable " + descriptor.baseFile(), ex);
                        exceptions = ex;
                    }

                    Throwable closeExceptions = Throwables.close(null, Iterables.filter(closeables, Objects::nonNull));
                    if (closeExceptions != null)
                    {
                        logger.error("Failed to close some sstable components of " + descriptor.baseFile(), closeExceptions);
                        exceptions = Throwables.merge(exceptions, closeExceptions);
                    }

                    try
                    {
                        globalRef.release();
                    }
                    catch (RuntimeException | Error ex)
                    {
                        logger.error("Failed to release the global ref of " + descriptor.baseFile(), ex);
                        exceptions = Throwables.merge(exceptions, ex);
                    }

                    if (exceptions != null)
                        JVMStabilityInspector.inspectThrowable(exceptions);

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

        @Override
        public String name()
        {
            return descriptor.toString();
        }
    }

    /**
     * One instance per logical sstable. This both tracks shared cleanup and some shared state related
     * to the sstable's lifecycle.
     * <p>
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
            readMeterSyncFuture = new WeakReference<>(syncExecutor.scheduleAtFixedRate(this::maybePersistSSTableReadMeter, 1, 5, TimeUnit.MINUTES));
        }

        void maybePersistSSTableReadMeter()
        {
            if (obsoletion == null && DatabaseDescriptor.getSStableReadRatePersistenceEnabled())
            {
                meterSyncThrottle.acquire();
                SystemKeyspace.persistSSTableReadMeter(desc.ksname, desc.cfname, desc.id, readMeter);
            }
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
            for (Component c : desc.discoverComponents())
                NativeLibrary.trySkipCache(desc.fileFor(c).absolutePath(), 0, 0);
        }

        public String name()
        {
            return desc.toString();
        }

        // get a new reference to the shared GlobalTidy for this sstable
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
            return 31 * (hashCode ^ (int) ((int) upperPosition ^ (upperPosition >>> 32)));
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof PartitionPositionBounds))
                return false;
            PartitionPositionBounds that = (PartitionPositionBounds) o;
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
     * <p>
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
        if (newDescriptor.fileFor(Components.DATA).exists())
        {
            String msg = String.format("File %s already exists, can't move the file there", newDescriptor.fileFor(Components.DATA));
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        if (copyData)
        {
            try
            {
                logger.info("Hardlinking new SSTable {} to {}", oldDescriptor, newDescriptor);
                hardlink(oldDescriptor, newDescriptor, components);
            }
            catch (FSWriteError ex)
            {
                logger.warn("Unable to hardlink new SSTable {} to {}, falling back to copying", oldDescriptor, newDescriptor, ex);
                copy(oldDescriptor, newDescriptor, components);
            }
        }
        else
        {
            logger.info("Moving new SSTable {} to {}", oldDescriptor, newDescriptor);
            rename(oldDescriptor, newDescriptor, components);
        }

        SSTableReader reader;
        try
        {
            reader = open(cfs, newDescriptor, components, cfs.metadata);
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

    /**
     * @return the physical size on disk of all components for this SSTable in bytes
     */
    public long bytesOnDisk()
    {
        return bytesOnDisk(false);
    }

    /**
     * @return the total logical/uncompressed size in bytes of all components for this SSTable
     */
    public long logicalBytesOnDisk()
    {
        return bytesOnDisk(true);
    }

    private long bytesOnDisk(boolean logical)
    {
        long bytes = 0;
        for (Component component : components)
        {
            // Only the data file is compressable.
            bytes += logical && component == Components.DATA && compression
                     ? getCompressionMetadata().dataLength
                     : descriptor.fileFor(component).length();
        }
        return bytes;
    }

    @VisibleForTesting
    public void maybePersistSSTableReadMeter()
    {
        tidy.global.maybePersistSSTableReadMeter();
    }

    /**
     * Returns a new verifier for this sstable. Note that the reader must match the provided cfs.
     */
    public abstract IVerifier getVerifier(ColumnFamilyStore cfs,
                                          OutputHandler outputHandler,
                                          boolean isOffline,
                                          IVerifier.Options options);

    /**
     * A method to be called by {@link #getPosition(PartitionPosition, Operator, boolean, SSTableReadsListener)}
     * and {@link #getRowIndexEntry(PartitionPosition, Operator, boolean, SSTableReadsListener)} methods when
     * a searched key is found. It adds a trace message and notify the provided listener.
     */
    protected void notifySelected(SSTableReadsListener.SelectionReason reason, SSTableReadsListener localListener, Operator op, boolean updateStats, AbstractRowIndexEntry entry)
    {
        reason.trace(descriptor, entry);

        if (localListener != null)
            localListener.onSSTableSelected(this, reason);
    }

    /**
     * A method to be called by {@link #getPosition(PartitionPosition, Operator, boolean, SSTableReadsListener)}
     * and {@link #getRowIndexEntry(PartitionPosition, Operator, boolean, SSTableReadsListener)} methods when
     * a searched key is not found. It adds a trace message and notify the provided listener.
     */
    protected void notifySkipped(SSTableReadsListener.SkippingReason reason, SSTableReadsListener localListener, Operator op, boolean updateStats)
    {
        reason.trace(descriptor);

        if (localListener != null)
            localListener.onSSTableSkipped(this, reason);
    }

    /**
     * A builder of this sstable reader. It should be extended for each implementation of {@link SSTableReader} with
     * the implementation specific fields.
     *
     * @param <R> type of the reader the builder creates
     * @param <B> type of this builder
     */
    public abstract static class Builder<R extends SSTableReader, B extends Builder<R, B>> extends SSTable.Builder<R, B>
    {
        private long maxDataAge;
        private StatsMetadata statsMetadata;
        private OpenReason openReason;
        private SerializationHeader serializationHeader;
        private FileHandle dataFile;
        private DecoratedKey first;
        private DecoratedKey last;
        private boolean suspected;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        public B setMaxDataAge(long maxDataAge)
        {
            Preconditions.checkArgument(maxDataAge >= 0);
            this.maxDataAge = maxDataAge;
            return (B) this;
        }

        public B setStatsMetadata(StatsMetadata statsMetadata)
        {
            Preconditions.checkNotNull(statsMetadata);
            this.statsMetadata = statsMetadata;
            return (B) this;
        }

        public B setOpenReason(OpenReason openReason)
        {
            Preconditions.checkNotNull(openReason);
            this.openReason = openReason;
            return (B) this;
        }

        public B setSerializationHeader(SerializationHeader serializationHeader)
        {
            this.serializationHeader = serializationHeader;
            return (B) this;
        }

        public B setDataFile(FileHandle dataFile)
        {
            this.dataFile = dataFile;
            return (B) this;
        }

        public B setFirst(DecoratedKey first)
        {
            this.first = first != null ? first.retainable() : null;
            return (B) this;
        }

        public B setLast(DecoratedKey last)
        {
            this.last = last != null ? last.retainable() : null;
            return (B) this;
        }

        public B setSuspected(boolean suspected)
        {
            this.suspected = suspected;
            return (B) this;
        }

        public long getMaxDataAge()
        {
            return maxDataAge;
        }

        public StatsMetadata getStatsMetadata()
        {
            return statsMetadata;
        }

        public OpenReason getOpenReason()
        {
            return openReason;
        }

        public SerializationHeader getSerializationHeader()
        {
            return serializationHeader;
        }

        public FileHandle getDataFile()
        {
            return dataFile;
        }

        public DecoratedKey getFirst()
        {
            return first;
        }

        public DecoratedKey getLast()
        {
            return last;
        }

        public boolean isSuspected()
        {
            return suspected;
        }

        protected abstract R buildInternal(Owner owner);

        public R build(Owner owner, boolean validate, boolean online)
        {
            R reader = buildInternal(owner);

            try
            {
                if (isSuspected())
                    reader.markSuspect();

                reader.setup(online);

                if (validate)
                    reader.validate();
            }
            catch (RuntimeException | Error ex)
            {
                JVMStabilityInspector.inspectThrowable(ex);
                reader.selfRef().release();
                throw ex;
            }

            return reader;
        }
    }
}
