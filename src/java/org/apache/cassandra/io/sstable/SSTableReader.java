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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    // guesstimated size of INDEX_INTERVAL index entries
    private static final int INDEX_FILE_BUFFER_BYTES = 16 * DatabaseDescriptor.getIndexInterval();

    /**
     * maxDataAge is a timestamp in local server time (e.g. System.currentTimeMilli) which represents an uppper bound
     * to the newest piece of data stored in the sstable. In other words, this sstable does not contain items created
     * later than maxDataAge.
     *
     * The field is not serialized to disk, so relying on it for more than what truncate does is not advised.
     *
     * When a new sstable is flushed, maxDataAge is set to the time of creation.
     * When a sstable is created from compaction, maxDataAge is set to max of all merged tables.
     *
     * The age is in milliseconds since epoc and is local to this host.
     */
    public final long maxDataAge;

    // indexfile and datafile: might be null before a call to load()
    private SegmentedFile ifile;
    private SegmentedFile dfile;

    private IndexSummary indexSummary;
    private Filter bf;

    private InstrumentingCache<KeyCacheKey, RowIndexEntry> keyCache;

    private final BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    private final AtomicInteger references = new AtomicInteger(1);
    // technically isCompacted is not necessary since it should never be unreferenced unless it is also compacted,
    // but it seems like a good extra layer of protection against reference counting bugs to not delete data based on that alone
    private final AtomicBoolean isCompacted = new AtomicBoolean(false);
    private final AtomicBoolean isSuspect = new AtomicBoolean(false);
    private final SSTableDeletingTask deletingTask;

    private final SSTableMetadata sstableMetadata;

    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = 0;

        for (SSTableReader sstable : sstables)
        {
            int indexKeyCount = sstable.getKeySamples().size();
            count = count + (indexKeyCount + 1) * DatabaseDescriptor.getIndexInterval();
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
        assert partitioner != null;
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA);
        assert components.contains(Component.PRIMARY_INDEX);

        long start = System.currentTimeMillis();
        logger.info("Opening {} ({} bytes)", descriptor, new File(descriptor.filenameFor(COMPONENT_DATA)).length());

        SSTableMetadata sstableMetadata = components.contains(Component.STATS)
                                        ? SSTableMetadata.serializer.deserialize(descriptor)
                                        : SSTableMetadata.createDefaultInstance();

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = partitioner.getClass().getCanonicalName();
        if (sstableMetadata.partitioner != null && !partitionerName.equals(sstableMetadata.partitioner))
        {
            logger.warn("Changing paritioner on a existing cluster can cause data loose, Please verify your partitioner in cassandra.yaml");
            logger.error(String.format("Cannot open %s because partitioner does not match %s != %s",descriptor, sstableMetadata.partitioner, partitionerName));
            System.exit(1);
        }

        SSTableReader sstable = new SSTableReader(descriptor,
                                                  components,
                                                  metadata,
                                                  partitioner,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  System.currentTimeMillis(),
                                                  sstableMetadata);
        // versions before 'c' encoded keys as utf-16 before hashing to the filter
        if (descriptor.version.hasStringsInBloomFilter)
        {
            sstable.load(true);
        }
        else
        {
            sstable.load(false);
            sstable.loadBloomFilter();
        }

        if (validate)
            sstable.validate();

        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for " + descriptor + ": " + (System.currentTimeMillis() - start) + " ms.");

        if (logger.isDebugEnabled() && sstable.getKeyCache() != null)
            logger.debug(String.format("key cache contains %s/%s keys", sstable.getKeyCache().size(), sstable.getKeyCache().getCapacity()));

        return sstable;
    }

    public static void logOpenException(Descriptor descriptor, IOException e)
    {
        if (e instanceof FileNotFoundException)
            logger.error("Missing sstable component in " + descriptor + "; skipped because of " + e.getMessage());
        else
            logger.error("Corrupt sstable " + descriptor + "; skipped", e);
    }

    public static Collection<SSTableReader> batchOpen(Set<Map.Entry<Descriptor, Set<Component>>> entries,
                                                      final CFMetaData metadata,
                                                      final IPartitioner partitioner)
    {
        final Collection<SSTableReader> sstables = new LinkedBlockingQueue<SSTableReader>();

        ExecutorService executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("SSTableBatchOpen", Runtime.getRuntime().availableProcessors());
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
                                      Filter bf,
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

    private SSTableReader(Descriptor desc,
                          Set<Component> components,
                          CFMetaData metadata,
                          IPartitioner partitioner,
                          SegmentedFile ifile,
                          SegmentedFile dfile,
                          IndexSummary indexSummary,
                          Filter bloomFilter,
                          long maxDataAge,
                          SSTableMetadata sstableMetadata)
    {
        super(desc, components, metadata, partitioner);
        this.sstableMetadata = sstableMetadata;
        this.maxDataAge = maxDataAge;

        this.ifile = ifile;
        this.dfile = dfile;
        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
        this.deletingTask = new SSTableDeletingTask(this);
    }

    public void setTrackedBy(DataTracker tracker)
    {
        keyCache = CacheService.instance.keyCache;
        deletingTask.setTracker(tracker);
    }

    void loadBloomFilter() throws IOException
    {
        if (!components.contains(Component.FILTER))
        {
            bf = FilterFactory.emptyFilter();
            return;
        }

        DataInputStream stream = null;
        try
        {
            stream = new DataInputStream(new BufferedInputStream(new FileInputStream(descriptor.filenameFor(Component.FILTER))));
            bf = FilterFactory.deserialize(stream, descriptor.version.filterType);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
    }

    /**
     * Loads ifile, dfile and indexSummary, and optionally recreates the bloom filter.
     */
    private void load(boolean recreatebloom) throws IOException
    {
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = compression
                                          ? SegmentedFile.getCompressedBuilder()
                                          : SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader primaryIndex = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), true);

        // try to load summaries from the disk and check if we need
        // to read primary index because we should re-create a BloomFilter or pre-load KeyCache
        final boolean summaryLoaded = loadSummary(this, ibuilder, dbuilder);
        final boolean readIndex = recreatebloom || !summaryLoaded;
        try
        {
            long indexSize = primaryIndex.length();
            long histogramCount = sstableMetadata.estimatedRowSize.count();
            long estimatedKeys = histogramCount > 0 && !sstableMetadata.estimatedRowSize.isOverflowed()
                               ? histogramCount
                               : estimateRowsFromIndex(primaryIndex); // statistics is supposed to be optional
            if (recreatebloom)
                bf = LegacyBloomFilter.getFilter(estimatedKeys, 15);

            if (!summaryLoaded)
                indexSummary = new IndexSummary(estimatedKeys);

            long indexPosition;
            while (readIndex && (indexPosition = primaryIndex.getFilePointer()) != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(primaryIndex);
                RowIndexEntry indexEntry = RowIndexEntry.serializer.deserialize(primaryIndex, descriptor.version);
                DecoratedKey decoratedKey = decodeKey(partitioner, descriptor, key);
                if (first == null)
                    first = decoratedKey;
                last = decoratedKey;

                if (recreatebloom)
                    bf.add(decoratedKey.key);

                // if summary was already read from disk we don't want to re-populate it using primary index
                if (!summaryLoaded)
                {
                    indexSummary.maybeAddEntry(decoratedKey, indexPosition);
                    ibuilder.addPotentialBoundary(indexPosition);
                    dbuilder.addPotentialBoundary(indexEntry.position);
                }
            }
        }
        finally
        {
            FileUtils.closeQuietly(primaryIndex);
        }
        first = getMinimalKey(first);
        last = getMinimalKey(last);
        // finalize the load.
        indexSummary.complete();
        // finalize the state of the reader
        ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
        dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));
        if (readIndex) // save summary information to disk
            saveSummary(this, ibuilder, dbuilder);
    }

    public static boolean loadSummary(SSTableReader reader, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder)
    {
        File summariesFile = new File(reader.descriptor.filenameFor(Component.SUMMARY));
        if (!summariesFile.exists())
            return false;

        DataInputStream iStream = null;
        try
        {
            iStream = new DataInputStream(new FileInputStream(summariesFile));
            reader.indexSummary = IndexSummary.serializer.deserialize(iStream, reader.partitioner);
            reader.first = decodeKey(reader.partitioner, reader.descriptor, ByteBufferUtil.readWithLength(iStream));
            reader.last = decodeKey(reader.partitioner, reader.descriptor, ByteBufferUtil.readWithLength(iStream));
            ibuilder.deserializeBounds(iStream);
            dbuilder.deserializeBounds(iStream);
        }
        catch (IOException e)
        {
            logger.debug("Cannot deserialize SSTable Summary: ", e);
            // corrupted hence delete it and let it load it now.
            if (summariesFile.exists())
                summariesFile.delete();

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

    private void validate()
    {
        if (this.first.compareTo(this.last) > 0)
            throw new IllegalStateException(String.format("SSTable first key %s > last key %s", this.first, this.last));
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    public long getIndexScanPosition(RowPosition key)
    {
        assert indexSummary.getKeys() != null && indexSummary.getKeys().size() > 0;
        int index = Collections.binarySearch(indexSummary.getKeys(), key);
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

        return ((CompressedSegmentedFile)dfile).metadata;
    }

    /**
     * For testing purposes only.
     */
    public void forceFilterFailures()
    {
        bf = LegacyBloomFilter.alwaysMatchingBloomFilter();
    }

    public Filter getBloomFilter()
    {
      return bf;
    }

    public long getBloomFilterSerializedSize()
    {
        return FilterFactory.serializedSize(bf, descriptor.version.filterType);
    }

    /**
     * @return An estimate of the number of keys in this SSTable.
     */
    public long estimatedKeys()
    {
        return indexSummary.getKeys().size() * DatabaseDescriptor.getIndexInterval();
    }

    /**
     * @param ranges
     * @return An estimate of the number of keys for given ranges in this SSTable.
     */
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        long sampleKeyCount = 0;
        List<Pair<Integer, Integer>> sampleIndexes = getSampleIndexesForRanges(indexSummary.getKeys(), ranges);
        for (Pair<Integer, Integer> sampleIndexRange : sampleIndexes)
            sampleKeyCount += (sampleIndexRange.right - sampleIndexRange.left + 1);
        return Math.max(1, sampleKeyCount * DatabaseDescriptor.getIndexInterval());
    }

    /**
     * @return Approximately 1/INDEX_INTERVALth of the keys in this SSTable.
     */
    public Collection<DecoratedKey> getKeySamples()
    {
        return indexSummary.getKeys();
    }

    private static List<Pair<Integer,Integer>> getSampleIndexesForRanges(List<DecoratedKey> samples, Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Integer,Integer>> positions = new ArrayList<Pair<Integer,Integer>>();
        if (samples.isEmpty())
            return positions;

        for (Range<Token> range : Range.normalize(ranges))
        {
            RowPosition leftPosition = range.left.maxKeyBound();
            RowPosition rightPosition = range.right.maxKeyBound();

            int left = Collections.binarySearch(samples, leftPosition);
            if (left < 0)
                left = (left + 1) * -1;
            else
                // left range are start exclusive
                left = left + 1;
            if (left == samples.size())
                // left is past the end of the sampling
                continue;

            int right = Range.isWrapAround(range.left, range.right)
                      ? samples.size() - 1
                      : Collections.binarySearch(samples, rightPosition);
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
            positions.add(new Pair(Integer.valueOf(left), Integer.valueOf(right)));
        }
        return positions;
    }

    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        final List<DecoratedKey> samples = indexSummary.getKeys();

        final List<Pair<Integer, Integer>> indexRanges = getSampleIndexesForRanges(samples, Collections.singletonList(range));

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
                        RowPosition k = samples.get(idx++);
                        // the index should only contain valid row key, we only allow RowPosition in KeyPosition for search purposes
                        assert k instanceof DecoratedKey;
                        return (DecoratedKey)k;
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
            positions.add(new Pair(Long.valueOf(left), Long.valueOf(right)));
        }
        return positions;
    }

    public void cacheKey(DecoratedKey key, RowIndexEntry info)
    {
        CFMetaData.Caching caching = metadata.getCaching();

        if (keyCache == null
                || caching == CFMetaData.Caching.NONE
                || caching == CFMetaData.Caching.ROWS_ONLY
                || keyCache.getCapacity() == 0)
            return;

        // avoid keeping a permanent reference to the original key buffer
        keyCache.put(new KeyCacheKey(descriptor, ByteBufferUtil.clone(key.key)), info);
    }

    public RowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return getCachedPosition(new KeyCacheKey(descriptor, key.key), updateStats);
    }

    private RowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        if (keyCache != null && keyCache.getCapacity() > 0)
            return updateStats ? keyCache.get(unifiedKey) : keyCache.getInternal(unifiedKey);
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
                return null;
        }

        // next, the key cache (only make sense for valid row key)
        if ((op == Operator.EQ || op == Operator.GE) && (key instanceof DecoratedKey))
        {
            DecoratedKey decoratedKey = (DecoratedKey)key;
            RowIndexEntry cachedPosition = getCachedPosition(new KeyCacheKey(descriptor, decoratedKey.key), updateCacheAndStats);
            if (cachedPosition != null)
                return cachedPosition;
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
                sampledPosition = 0;
            else
                return null;
        }

        // scan the on-disk index, starting at the nearest sampled position
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition, INDEX_FILE_BUFFER_BYTES);
        while (segments.hasNext())
        {
            FileDataInput input = segments.next();
            try
            {
                while (!input.isEOF())
                {
                    // read key & data position from index entry
                    DecoratedKey indexDecoratedKey = decodeKey(partitioner, descriptor, ByteBufferUtil.readWithShortLength(input));
                    int comparison = indexDecoratedKey.compareTo(key);
                    int v = op.apply(comparison);
                    if (v == 0)
                    {
                        RowIndexEntry indexEntry = RowIndexEntry.serializer.deserialize(input, descriptor.version);
                        if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0 && updateCacheAndStats)
                        {
                            assert key instanceof DecoratedKey; // key can be == to the index key only if it's a true row key
                            DecoratedKey decoratedKey = (DecoratedKey)key;
                            // store exact match for the key
                            cacheKey(decoratedKey, indexEntry);
                        }
                        if (op == Operator.EQ && updateCacheAndStats)
                            bloomFilterTracker.addTruePositive();
                        return indexEntry;
                    }
                    if (v < 0)
                    {
                        if (op == Operator.EQ && updateCacheAndStats)
                            bloomFilterTracker.addFalsePositive();
                        return null;
                    }
                    RowIndexEntry.serializer.skip(input, descriptor.version);
                }
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, input.getPath());
            }
            finally
            {
                FileUtils.closeQuietly(input);
            }
        }

        if (op == Operator.EQ && updateCacheAndStats)
            bloomFilterTracker.addFalsePositive();
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

    public void releaseReference()
    {
        if (references.decrementAndGet() == 0 && isCompacted.get())
        {
            // Force finalizing mmapping if necessary
            ifile.cleanup();
            dfile.cleanup();

            deletingTask.schedule();
        }
        assert references.get() >= 0 : "Reference counter " +  references.get() + " for " + dfile.path;
    }

    /**
     * Mark the sstable as compacted.
     * When calling this function, the caller must ensure that the SSTableReader is not referenced anywhere
     * except for threads holding a reference.
     *
     * @return true if the this is the first time the file was marked compacted.  With rare exceptions
     * (see DataTracker.unmarkCompacted) calling this multiple times would be buggy.
     */
    public boolean markCompacted()
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + getFilename() + " compacted");

        return !isCompacted.getAndSet(true);
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
     * @param filter filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public SSTableScanner getScanner(QueryFilter filter)
    {
        return new SSTableScanner(this, filter);
    }

   /**
    * Direct I/O SSTableScanner
    * @return A Scanner for seeking over the rows of the SSTable.
    */
    public SSTableScanner getDirectScanner()
    {
        return new SSTableScanner(this, true);
    }

   /**
    * Direct I/O SSTableScanner over a defined range of tokens.
    *
    * @param range the range of keys to cover
    * @return A Scanner for seeking over the rows of the SSTable.
    */
    public SSTableScanner getDirectScanner(Range<Token> range)
    {
        if (range == null)
            return getDirectScanner();
        return new SSTableBoundedScanner(this, true, range);
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

    public static long readRowSize(DataInput in, Descriptor d) throws IOException
    {
        if (d.version.hasIntRowSize)
            return in.readInt();
        return in.readLong();
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
     * Conditionally use the deprecated 'IPartitioner.convertFromDiskFormat' method.
     */
    public static DecoratedKey decodeKey(IPartitioner p, Descriptor d, ByteBuffer bytes)
    {
        if (d.version.hasEncodedKeys)
            return p.convertFromDiskFormat(bytes);
        return p.decorateKey(bytes);
    }

    public DecoratedKey decodeKey(ByteBuffer bytes)
    {
        return decodeKey(partitioner, descriptor, bytes);
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

    public double getCompressionRatio()
    {
        return sstableMetadata.compressionRatio;
    }

    public ReplayPosition getReplayPosition()
    {
        return sstableMetadata.replayPosition;
    }

    public long getMaxTimestamp()
    {
        return sstableMetadata.maxTimestamp;
    }

    public Set<Integer> getAncestors()
    {
        return sstableMetadata.ancestors;
    }

    public RandomAccessReader openDataReader(boolean skipIOCache)
    {
        return compression
               ? CompressedRandomAccessReader.open(getFilename(), getCompressionMetadata(), skipIOCache)
               : RandomAccessReader.open(new File(getFilename()), skipIOCache);
    }

    public RandomAccessReader openIndexReader(boolean skipIOCache)
    {
        return RandomAccessReader.open(new File(getIndexFilename()), skipIOCache);
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
}
