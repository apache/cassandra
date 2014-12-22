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
package org.apache.cassandra.io.sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedThrottledReader;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    private static final ScheduledThreadPoolExecutor syncExecutor = new ScheduledThreadPoolExecutor(1);
    private static final RateLimiter meterSyncThrottle = RateLimiter.create(100.0);

    /**
     * maxDataAge is a timestamp in local server time (e.g. System.currentTimeMilli) which represents an uppper bound
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

    // indexfile and datafile: might be null before a call to load()
    private SegmentedFile ifile;
    private SegmentedFile dfile;

    private IndexSummary indexSummary;
    private IFilter bf;

    private InstrumentingCache<KeyCacheKey, RowIndexEntry> keyCache;

    private final BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    private final AtomicInteger references = new AtomicInteger(1);
    // technically isCompacted is not necessary since it should never be unreferenced unless it is also compacted,
    // but it seems like a good extra layer of protection against reference counting bugs to not delete data based on that alone
    private final AtomicBoolean isCompacted = new AtomicBoolean(false);
    private final AtomicBoolean isSuspect = new AtomicBoolean(false);
    private final SSTableDeletingTask deletingTask;
    // not final since we need to be able to change level on a file.
    private volatile SSTableMetadata sstableMetadata;

    private final AtomicLong keyCacheHit = new AtomicLong(0);
    private final AtomicLong keyCacheRequest = new AtomicLong(0);

    @VisibleForTesting
    public RestorableMeter readMeter;
    private ScheduledFuture readMeterSyncFuture;

    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables, CFMetaData metadata)
    {
        long count = 0;

        for (SSTableReader sstable : sstables)
        {
            int indexKeyCount = sstable.getKeySampleSize();
            count = count + (indexKeyCount + 1) * sstable.indexSummary.getIndexInterval();
            if (logger.isDebugEnabled())
                logger.debug("index size for bloom filter calc for file  : " + sstable.getFilename() + "   : " + count);
        }

        return count;
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

    public static SSTableReader openNoValidation(Descriptor descriptor, Set<Component> components, CFMetaData metadata) throws IOException
    {
        return open(descriptor, components, metadata, StorageService.getPartitioner(), false);
    }

    public static SSTableReader openForBatch(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        SSTableMetadata sstableMetadata = openMetadata(descriptor, components, partitioner);
        SSTableReader sstable = new SSTableReader(descriptor,
                                                  components,
                                                  metadata,
                                                  partitioner,
                                                  System.currentTimeMillis(),
                                                  sstableMetadata);

        // special implementation of load to use non-pooled SegmentedFile builders
        SegmentedFile.Builder ibuilder = new BufferedSegmentedFile.Builder();
        SegmentedFile.Builder dbuilder = sstable.compression
                                       ? new CompressedSegmentedFile.Builder()
                                       : new BufferedSegmentedFile.Builder();
        if (!loadSummary(sstable, ibuilder, dbuilder, sstable.metadata))
            sstable.buildSummary(false, ibuilder, dbuilder, false);
        sstable.ifile = ibuilder.complete(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX));
        sstable.dfile = dbuilder.complete(sstable.descriptor.filenameFor(Component.DATA));

        sstable.bf = FilterFactory.AlwaysPresent;
        return sstable;
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        return open(descriptor, components, metadata, partitioner, true);
    }

    private static SSTableReader open(Descriptor descriptor,
                                      Set<Component> components,
                                      CFMetaData metadata,
                                      IPartitioner partitioner,
                                      boolean validate) throws IOException
    {
        long start = System.nanoTime();
        SSTableMetadata sstableMetadata = openMetadata(descriptor, components, partitioner);

        SSTableReader sstable = new SSTableReader(descriptor,
                                                  components,
                                                  metadata,
                                                  partitioner,
                                                  System.currentTimeMillis(),
                                                  sstableMetadata);

        sstable.load();

        if (validate)
            sstable.validate();

        logger.debug("INDEX LOAD TIME for {}: {} ms.", descriptor, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        if (sstable.getKeyCache() != null)
            logger.debug("key cache contains {}/{} keys", sstable.getKeyCache().size(), sstable.getKeyCache().getCapacity());

        return sstable;
    }

    private static SSTableMetadata openMetadata(Descriptor descriptor, Set<Component> components, IPartitioner partitioner) throws IOException
    {
        assert partitioner != null;
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA) : "Data component is missing for sstable" + descriptor;
        assert components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;

        logger.info("Opening {} ({} bytes)", descriptor, new File(descriptor.filenameFor(COMPONENT_DATA)).length());

        SSTableMetadata sstableMetadata = SSTableMetadata.serializer.deserialize(descriptor).left;

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = partitioner.getClass().getCanonicalName();
        if (sstableMetadata.partitioner != null && !partitionerName.equals(sstableMetadata.partitioner))
        {
            logger.error(String.format("Cannot open %s; partitioner %s does not match system partitioner %s.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                                       descriptor, sstableMetadata.partitioner, partitionerName));
            System.exit(1);
        }
        return sstableMetadata;
    }

    public static void logOpenException(Descriptor descriptor, IOException e)
    {
        if (e instanceof FileNotFoundException)
            logger.error("Missing sstable component in " + descriptor + "; skipped because of " + e.getMessage());
        else
            logger.error("Corrupt sstable " + descriptor + "; skipped", e);
    }

    public static Collection<SSTableReader> openAll(Set<Map.Entry<Descriptor, Set<Component>>> entries,
                                                      final CFMetaData metadata,
                                                      final IPartitioner partitioner)
    {
        final Collection<SSTableReader> sstables = new LinkedBlockingQueue<SSTableReader>();

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
                    catch (IOException ex)
                    {
                        logger.error("Corrupt sstable " + entry + "; skipped", ex);
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
    static SSTableReader internalOpen(Descriptor desc,
                                      Set<Component> components,
                                      CFMetaData metadata,
                                      IPartitioner partitioner,
                                      SegmentedFile ifile,
                                      SegmentedFile dfile,
                                      IndexSummary isummary,
                                      IFilter bf,
                                      long maxDataAge,
                                      SSTableMetadata sstableMetadata)
    {
        assert desc != null && partitioner != null && ifile != null && dfile != null && isummary != null && bf != null && sstableMetadata != null;
        return new SSTableReader(desc,
                                 components,
                                 metadata,
                                 partitioner,
                                 ifile, dfile,
                                 isummary,
                                 bf,
                                 maxDataAge,
                                 sstableMetadata);
    }


    private SSTableReader(final Descriptor desc,
                          Set<Component> components,
                          CFMetaData metadata,
                          IPartitioner partitioner,
                          long maxDataAge,
                          SSTableMetadata sstableMetadata)
    {
        super(desc, components, metadata, partitioner);
        this.sstableMetadata = sstableMetadata;
        this.maxDataAge = maxDataAge;

        deletingTask = new SSTableDeletingTask(this);

        // Don't track read rates for tables in the system keyspace and don't bother trying to load or persist
        // the read meter when in client mode
        if (Keyspace.SYSTEM_KS.equals(desc.ksname) || Config.isClientMode())
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

    private SSTableReader(Descriptor desc,
                          Set<Component> components,
                          CFMetaData metadata,
                          IPartitioner partitioner,
                          SegmentedFile ifile,
                          SegmentedFile dfile,
                          IndexSummary indexSummary,
                          IFilter bloomFilter,
                          long maxDataAge,
                          SSTableMetadata sstableMetadata)
    {
        this(desc, components, metadata, partitioner, maxDataAge, sstableMetadata);

        this.ifile = ifile;
        this.dfile = dfile;
        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
    }

    /**
     * Clean up all opened resources.
     *
     * @throws IOException
     */
    public void close() throws IOException
    {
        if (readMeterSyncFuture != null)
            readMeterSyncFuture.cancel(false);

        // Force finalizing mmapping if necessary
        ifile.cleanup();
        dfile.cleanup();
        // close the BF so it can be opened later.
        bf.close();
        indexSummary.close();
    }

    public void setTrackedBy(DataTracker tracker)
    {
        deletingTask.setTracker(tracker);
        // under normal operation we can do this at any time, but SSTR is also used outside C* proper,
        // e.g. by BulkLoader, which does not initialize the cache.  As a kludge, we set up the cache
        // here when we know we're being wired into the rest of the server infrastructure.
        keyCache = CacheService.instance.keyCache;
    }

    private void load() throws IOException
    {
        if (metadata.getBloomFilterFpChance() == 1.0)
        {
            // bf is disabled.
            load(false, true);
            bf = FilterFactory.AlwaysPresent;
        }
        else if (!components.contains(Component.FILTER))
        {
            // bf is enabled, but filter component is missing.
            load(true, true);
        }
        else if (descriptor.version.hasBloomFilterFPChance && sstableMetadata.bloomFilterFPChance != metadata.getBloomFilterFpChance())
        {
            // bf fp chance in sstable metadata and it has changed since compaction.
            load(true, true);
        }
        else
        {
            // bf is enabled, but fp chance isn't present in metadata (pre-ja) OR matches the currently configured value.
            load(false, true);
            loadBloomFilter();
        }
    }

    void loadBloomFilter() throws IOException
    {
        DataInputStream stream = null;
        try
        {
            stream = new DataInputStream(new BufferedInputStream(new FileInputStream(descriptor.filenameFor(Component.FILTER))));
            bf = FilterFactory.deserialize(stream, true);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
    }

    /**
     * Loads ifile, dfile and indexSummary, and optionally recreates the bloom filter.
     * @param saveSummaryIfCreated for bulk loading purposes, if the summary was absent and needed to be built, you can
     *                             avoid persisting it to disk by setting this to false
     */
    private void load(boolean recreateBloomFilter, boolean saveSummaryIfCreated) throws IOException
    {
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = compression
                                         ? SegmentedFile.getCompressedBuilder()
                                         : SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());

        boolean summaryLoaded = loadSummary(this, ibuilder, dbuilder, metadata);
        if (recreateBloomFilter || !summaryLoaded)
            buildSummary(recreateBloomFilter, ibuilder, dbuilder, summaryLoaded);

        ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
        dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));
        if (saveSummaryIfCreated && (recreateBloomFilter || !summaryLoaded)) // save summary information to disk
            saveSummary(this, ibuilder, dbuilder);
    }

     private void buildSummary(boolean recreateBloomFilter, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, boolean summaryLoaded) throws IOException
     {
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader primaryIndex = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));

        try
        {
            long indexSize = primaryIndex.length();
            long histogramCount = sstableMetadata.estimatedRowSize.count();
            long estimatedKeys = histogramCount > 0 && !sstableMetadata.estimatedRowSize.isOverflowed()
                               ? histogramCount
                               : estimateRowsFromIndex(primaryIndex); // statistics is supposed to be optional

            if (recreateBloomFilter)
                bf = FilterFactory.getFilter(estimatedKeys, metadata.getBloomFilterFpChance(), true);

            IndexSummaryBuilder summaryBuilder = null;
            if (!summaryLoaded)
                summaryBuilder = new IndexSummaryBuilder(estimatedKeys, metadata.getIndexInterval());

            long indexPosition;
            while ((indexPosition = primaryIndex.getFilePointer()) != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(primaryIndex);
                RowIndexEntry indexEntry = RowIndexEntry.serializer.deserialize(primaryIndex, descriptor.version);
                DecoratedKey decoratedKey = partitioner.decorateKey(key);
                if (first == null)
                    first = decoratedKey;
                last = decoratedKey;

                if (recreateBloomFilter)
                    bf.add(decoratedKey.key);

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
        finally
        {
            FileUtils.closeQuietly(primaryIndex);
        }

        first = getMinimalKey(first);
        last = getMinimalKey(last);
    }

    public static boolean loadSummary(SSTableReader reader, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, CFMetaData metadata)
    {
        File summariesFile = new File(reader.descriptor.filenameFor(Component.SUMMARY));
        if (!reader.descriptor.version.offHeapSummaries || !summariesFile.exists())
            return false;

        DataInputStream iStream = null;
        try
        {
            iStream = new DataInputStream(new FileInputStream(summariesFile));
            reader.indexSummary = IndexSummary.serializer.deserialize(iStream, reader.partitioner);
            if (reader.indexSummary.getIndexInterval() != metadata.getIndexInterval())
            {
                iStream.close();
                logger.debug("Cannot read the saved summary for {} because Index Interval changed from {} to {}.",
                             reader.toString(), reader.indexSummary.getIndexInterval(), metadata.getIndexInterval());
                FileUtils.deleteWithConfirm(summariesFile);
                return false;
            }
            reader.first = reader.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            reader.last = reader.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            ibuilder.deserializeBounds(iStream);
            dbuilder.deserializeBounds(iStream);
        }
        catch (IOException e)
        {
            logger.debug("Cannot deserialize SSTable Summary: ", e);
            // corrupted; delete it and fall back to creating a new summary
            FileUtils.closeQuietly(iStream);
            FileUtils.deleteWithConfirm(summariesFile);
            return false;
        }
        finally
        {
            FileUtils.closeQuietly(iStream);
        }

        return true;
    }

    public static void saveSummary(SSTableReader reader, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder)
    {
        File summariesFile = new File(reader.descriptor.filenameFor(Component.SUMMARY));
        if (summariesFile.exists())
            summariesFile.delete();

        DataOutputStream oStream = null;
        try
        {
            oStream = new DataOutputStream(new FileOutputStream(summariesFile));
            IndexSummary.serializer.serialize(reader.indexSummary, oStream);
            ByteBufferUtil.writeWithLength(reader.first.key, oStream);
            ByteBufferUtil.writeWithLength(reader.last.key, oStream);
            ibuilder.serializeBounds(oStream);
            dbuilder.serializeBounds(oStream);
        }
        catch (IOException e)
        {
            logger.debug("Cannot save SSTable Summary: ", e);

            // corrupted hence delete it and let it load it now.
            if (summariesFile.exists())
                summariesFile.delete();
        }
        finally
        {
            FileUtils.closeQuietly(oStream);
        }
    }

    public void releaseSummary() throws IOException
    {
        indexSummary.close();
        indexSummary = null;
    }

    private void validate()
    {
        if (this.first.compareTo(this.last) > 0)
            throw new IllegalStateException(String.format("SSTable first key %s > last key %s", this.first, this.last));
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    public long getIndexScanPosition(RowPosition key)
    {
        int index = indexSummary.binarySearch(key);
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return -1;
            return indexSummary.getPosition(greaterThan - 1);
        }
        else
        {
            return indexSummary.getPosition(index);
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
     * Returns the amount of memory in bytes used off heap by the index summary.
     * @return the amount of memory in bytes used off heap by the index summary
     */
    public long getIndexSummaryOffHeapSize()
    {
        return indexSummary.offHeapSize();
    }

    /**
     * @return An estimate of the number of keys in this SSTable.
     */
    public long estimatedKeys()
    {
        return ((long) indexSummary.size()) * indexSummary.getIndexInterval();
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
        return Math.max(1, sampleKeyCount * indexSummary.getIndexInterval());
    }

    /**
     * @return Approximately 1/INDEX_INTERVALth of the keys in this SSTable.
     */
    public int getKeySampleSize()
    {
        return indexSummary.size();
    }

    public byte[] getKeySample(int position)
    {
        return indexSummary.getKey(position);
    }

    private static List<Pair<Integer,Integer>> getSampleIndexesForRanges(IndexSummary summary, Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Integer,Integer>> positions = new ArrayList<Pair<Integer,Integer>>();

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
            positions.add(Pair.create(Integer.valueOf(left), Integer.valueOf(right)));
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
        List<Pair<Long,Long>> positions = new ArrayList<Pair<Long,Long>>();
        for (Range<Token> range : Range.normalize(ranges))
        {
            AbstractBounds<RowPosition> keyRange = range.toRowBounds();
            RowIndexEntry idxLeft = getPosition(keyRange.left, Operator.GT);
            long left = idxLeft == null ? -1 : idxLeft.position;
            if (left == -1)
                // left is past the end of the file
                continue;
            RowIndexEntry idxRight = getPosition(keyRange.right, Operator.GT);
            long right = idxRight == null ? -1 : idxRight.position;
            if (right == -1 || Range.isWrapAround(range.left, range.right))
                // right is past the end of the file, or it wraps
                right = uncompressedLength();
            if (left == right)
                // empty range
                continue;
            positions.add(Pair.create(Long.valueOf(left), Long.valueOf(right)));
        }
        return positions;
    }

    public void cacheKey(DecoratedKey key, RowIndexEntry info)
    {
        CFMetaData.Caching caching = metadata.getCaching();

        if (caching == CFMetaData.Caching.NONE
            || caching == CFMetaData.Caching.ROWS_ONLY
            || keyCache == null
            || keyCache.getCapacity() == 0)
        {
            return;
        }

        KeyCacheKey cacheKey = new KeyCacheKey(descriptor, key.key);
        logger.trace("Adding cache entry for {} -> {}", cacheKey, info);
        keyCache.put(cacheKey, info);
    }

    public void preheat(Map<DecoratedKey, RowIndexEntry> cachedKeys) throws IOException
    {
        RandomAccessFile f = new RandomAccessFile(getFilename(), "r");

        try
        {
            int fd = CLibrary.getfd(f.getFD());

            for (Map.Entry<DecoratedKey, RowIndexEntry> entry : cachedKeys.entrySet())
            {
                cacheKey(entry.getKey(), entry.getValue());

                // add to the cache but don't do actual preheating if we have it disabled in the config
                if (DatabaseDescriptor.shouldPreheatPageCache() && fd > 0)
                    CLibrary.preheatPage(fd, entry.getValue().position);
            }
        }
        finally
        {
            FileUtils.closeQuietly(f);
        }
    }

    public RowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return getCachedPosition(new KeyCacheKey(descriptor, key.key), updateStats);
    }

    private RowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        if (keyCache != null && keyCache.getCapacity() > 0) {
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
     * @see #getPosition(org.apache.cassandra.db.RowPosition, org.apache.cassandra.io.sstable.SSTableReader.Operator, boolean)
     */
    public RowIndexEntry getPosition(RowPosition key, Operator op)
    {
        return getPosition(key, op, true);
    }

    /**
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    public RowIndexEntry getPosition(RowPosition key, Operator op, boolean updateCacheAndStats)
    {
        // first, check bloom filter
        if (op == Operator.EQ)
        {
            assert key instanceof DecoratedKey; // EQ only make sense if the key is a valid row key
            if (!bf.isPresent(((DecoratedKey)key).key))
            {
                Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.generation);
                return null;
            }
        }

        // next, the key cache (only make sense for valid row key)
        if ((op == Operator.EQ || op == Operator.GE) && (key instanceof DecoratedKey))
        {
            DecoratedKey decoratedKey = (DecoratedKey)key;
            KeyCacheKey cacheKey = new KeyCacheKey(descriptor, decoratedKey.key);
            RowIndexEntry cachedPosition = getCachedPosition(cacheKey, updateCacheAndStats);
            if (cachedPosition != null)
            {
                Tracing.trace("Key cache hit for sstable {}", descriptor.generation);
                return cachedPosition;
            }
        }

        // next, see if the sampled index says it's impossible for the key to be present
        long sampledPosition = getIndexScanPosition(key);
        if (sampledPosition == -1)
        {
            if (op == Operator.EQ && updateCacheAndStats)
                bloomFilterTracker.addFalsePositive();
            // we matched the -1th position: if the operator might match forward, we'll start at the first
            // position. We however need to return the correct index entry for that first position.
            if (op.apply(1) >= 0)
            {
                sampledPosition = 0;
            }
            else
            {
                Tracing.trace("Partition summary allows skipping sstable {}", descriptor.generation);
                return null;
            }
        }

        // scan the on-disk index, starting at the nearest sampled position.
        // The check against IndexInterval is to be exit the loop in the EQ case when the key looked for is not present
        // (bloom filter false positive). But note that for non-EQ cases, we might need to check the first key of the
        // next index position because the searched key can be greater the last key of the index interval checked if it
        // is lesser than the first key of next interval (and in that case we must return the position of the first key
        // of the next interval).
        int i = 0;
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition);
        while (segments.hasNext() && i <= indexSummary.getIndexInterval())
        {
            FileDataInput in = segments.next();
            try
            {
                while (!in.isEOF() && i <= indexSummary.getIndexInterval())
                {
                    i++;

                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);

                    boolean opSatisfied; // did we find an appropriate position for the op requested
                    boolean exactMatch; // is the current position an exact match for the key, suitable for caching

                    // Compare raw keys if possible for performance, otherwise compare decorated keys.
                    if (op == Operator.EQ)
                    {
                        opSatisfied = exactMatch = indexKey.equals(((DecoratedKey) key).key);
                    }
                    else
                    {
                        DecoratedKey indexDecoratedKey = partitioner.decorateKey(indexKey);
                        int comparison = indexDecoratedKey.compareTo(key);
                        int v = op.apply(comparison);
                        opSatisfied = (v == 0);
                        exactMatch = (comparison == 0);
                        if (v < 0)
                        {
                            Tracing.trace("Partition index lookup allows skipping sstable {}", descriptor.generation);
                            return null;
                        }
                    }

                    if (opSatisfied)
                    {
                        // read data position from index entry
                        RowIndexEntry indexEntry = RowIndexEntry.serializer.deserialize(in, descriptor.version);
                        if (exactMatch && updateCacheAndStats)
                        {
                            assert key instanceof DecoratedKey; // key can be == to the index key only if it's a true row key
                            DecoratedKey decoratedKey = (DecoratedKey)key;

                            if (logger.isTraceEnabled())
                            {
                                // expensive sanity check!  see CASSANDRA-4687
                                FileDataInput fdi = dfile.getSegment(indexEntry.position);
                                DecoratedKey keyInDisk = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(fdi));
                                if (!keyInDisk.equals(key))
                                    throw new AssertionError(String.format("%s != %s in %s", keyInDisk, key, fdi.getPath()));
                                fdi.close();
                            }

                            // store exact match for the key
                            cacheKey(decoratedKey, indexEntry);
                        }
                        if (op == Operator.EQ && updateCacheAndStats)
                            bloomFilterTracker.addTruePositive();
                        Tracing.trace("Partition index with {} entries found for sstable {}", indexEntry.columnsIndex().size(), descriptor.generation);
                        return indexEntry;
                    }

                    RowIndexEntry.serializer.skip(in);
                }
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, in.getPath());
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }

        if (op == Operator.EQ && updateCacheAndStats)
            bloomFilterTracker.addFalsePositive();
        Tracing.trace("Partition index lookup complete (bloom filter false positive) for sstable {}", descriptor.generation);
        return null;
    }

    /**
     * Finds and returns the first key beyond a given token in this SSTable or null if no such key exists.
     */
    public DecoratedKey firstKeyBeyond(RowPosition token)
    {
        long sampledPosition = getIndexScanPosition(token);
        if (sampledPosition == -1)
            sampledPosition = 0;

        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition);
        while (segments.hasNext())
        {
            FileDataInput in = segments.next();
            try
            {
                while (!in.isEOF())
                {
                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                    DecoratedKey indexDecoratedKey = partitioner.decorateKey(indexKey);
                    if (indexDecoratedKey.compareTo(token) > 0)
                        return indexDecoratedKey;

                    RowIndexEntry.serializer.skip(in);
                }
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, in.getPath());
            }
            finally
            {
                FileUtils.closeQuietly(in);
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

    public boolean acquireReference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
                return true;
        }
    }

    /**
     * Release reference to this SSTableReader.
     * If there is no one referring to this SSTable, and is marked as compacted,
     * all resources are cleaned up and files are deleted eventually.
     */
    public void releaseReference()
    {
        if (references.decrementAndGet() == 0 && isCompacted.get())
        {
            /**
             * Make OS a favour and suggest (using fadvice call) that we
             * don't want to see pages of this SSTable in memory anymore.
             *
             * NOTE: We can't use madvice in java because it requires address of
             * the mapping, so instead we always open a file and run fadvice(fd, 0, 0) on it
             */
            dropPageCache();

            FileUtils.closeQuietly(this);
            deletingTask.schedule();
        }
        assert references.get() >= 0 : "Reference counter " +  references.get() + " for " + dfile.path;
    }

    /**
     * Mark the sstable as obsolete, i.e., compacted into newer sstables.
     *
     * When calling this function, the caller must ensure that the SSTableReader is not referenced anywhere
     * except for threads holding a reference.
     *
     * @return true if the this is the first time the file was marked obsolete.  Calling this
     * multiple times is usually buggy (see exceptions in DataTracker.unmarkCompacting and removeOldSSTablesSize).
     */
    public boolean markObsolete()
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + getFilename() + " compacted");

        return !isCompacted.getAndSet(true);
    }

    public boolean isMarkedCompacted()
    {
        return isCompacted.get();
    }

    public void markSuspect()
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + getFilename() + " as a suspect for blacklisting.");

        isSuspect.getAndSet(true);
    }

    public boolean isMarkedSuspect()
    {
        return isSuspect.get();
    }

    /**
     *
     * @param dataRange filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public SSTableScanner getScanner(DataRange dataRange)
    {
        return new SSTableScanner(this, dataRange, null);
    }

    /**
     * I/O SSTableScanner
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public SSTableScanner getScanner()
    {
        return getScanner((RateLimiter) null);
    }

    public SSTableScanner getScanner(RateLimiter limiter)
    {
        return new SSTableScanner(this, DataRange.allData(partitioner), limiter);
    }

    /**
     * Direct I/O SSTableScanner over a defined range of tokens.
     *
     * @param range the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ICompactionScanner getScanner(Range<Token> range, RateLimiter limiter)
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
    public ICompactionScanner getScanner(Collection<Range<Token>> ranges, RateLimiter limiter)
    {
        // We want to avoid allocating a SSTableScanner if the range don't overlap the sstable (#5249)
        List<Pair<Long, Long>> positions = getPositionsForRanges(Range.normalize(ranges));
        if (positions.isEmpty())
            return new EmptyCompactionScanner(getFilename());
        else
            return new SSTableScanner(this, ranges, limiter);
    }

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
            public int apply(int comparison) { return comparison >= 0 ? 0 : -comparison; }
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

    public ReplayPosition getReplayPosition()
    {
        return sstableMetadata.replayPosition;
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
            return SSTableMetadata.serializer.deserialize(descriptor).right;
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
        this.sstableMetadata = SSTableMetadata.serializer.deserialize(descriptor).left;
    }

    public SSTableMetadata getSSTableMetadata()
    {
        return sstableMetadata;
    }

    public RandomAccessReader openDataReader(RateLimiter limiter)
    {
        assert limiter != null;
        return compression
               ? CompressedThrottledReader.open(getFilename(), getCompressionMetadata(), limiter)
               : ThrottledReader.open(new File(getFilename()), limiter);
    }

    public RandomAccessReader openDataReader()
    {
        return compression
               ? CompressedRandomAccessReader.open(getFilename(), getCompressionMetadata())
               : RandomAccessReader.open(new File(getFilename()));
    }

    public RandomAccessReader openIndexReader()
    {
        return RandomAccessReader.open(new File(getIndexFilename()));
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
     * @param sstables
     * @return true if all desired references were acquired.  Otherwise, it will unreference any partial acquisition, and return false.
     */
    public static boolean acquireReferences(Iterable<SSTableReader> sstables)
    {
        SSTableReader failed = null;
        for (SSTableReader sstable : sstables)
        {
            if (!sstable.acquireReference())
            {
                failed = sstable;
                break;
            }
        }

        if (failed == null)
            return true;

        for (SSTableReader sstable : sstables)
        {
            if (sstable == failed)
                break;
            sstable.releaseReference();
        }
        return false;
    }

    public static void releaseReferences(Iterable<SSTableReader> sstables)
    {
        for (SSTableReader sstable : sstables)
        {
            sstable.releaseReference();
        }
    }

    private void dropPageCache()
    {
        dropPageCache(dfile.path);
        dropPageCache(ifile.path);
    }

    private void dropPageCache(String filePath)
    {
        RandomAccessFile file = null;

        try
        {
            file = new RandomAccessFile(filePath, "r");

            int fd = CLibrary.getfd(file.getFD());

            if (fd > 0)
            {
                if (logger.isDebugEnabled())
                    logger.debug(String.format("Dropping page cache of file %s.", filePath));

                CLibrary.trySkipCache(fd, 0, 0);
            }
        }
        catch (IOException e)
        {
            // we don't care if cache cleanup fails
        }
        finally
        {
            FileUtils.closeQuietly(file);
        }
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

    protected class EmptyCompactionScanner implements ICompactionScanner
    {
        private final String filename;

        public EmptyCompactionScanner(String filename)
        {
            this.filename = filename;
        }

        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public String getBackingFiles()
        {
            return filename;
        }

        public boolean hasNext()
        {
            return false;
        }

        public OnDiskAtomIterator next()
        {
            return null;
        }

        public void close() throws IOException { }

        public void remove() { }
    }

    public static class SizeComparator implements Comparator<SSTableReader>
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return Longs.compare(o1.onDiskLength(), o2.onDiskLength());
        }
    }
}
