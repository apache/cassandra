/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.io.sstable;

import java.io.*;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable implements Comparable<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    // guesstimated size of INDEX_INTERVAL index entries
    private static final int INDEX_FILE_BUFFER_BYTES = 16 * DatabaseDescriptor.getIndexInterval();

    // `finalizers` is required to keep the PhantomReferences alive after the enclosing SSTR is itself
    // unreferenced.  otherwise they will never get enqueued.
    private static final Set<Reference<SSTableReader>> finalizers = new HashSet<Reference<SSTableReader>>();
    private static final ReferenceQueue<SSTableReader> finalizerQueue = new ReferenceQueue<SSTableReader>()
    {{
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    SSTableDeletingReference r;
                    try
                    {
                        r = (SSTableDeletingReference) finalizerQueue.remove();
                        finalizers.remove(r);
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                    try
                    {
                        r.cleanup();
                    }
                    catch (IOException e)
                    {
                        logger.error("Error deleting " + r.desc, e);
                    }
                }
            }
        };
        new Thread(runnable, "SSTABLE-DELETER").start();
    }};

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

    private InstrumentingCache<Pair<Descriptor, DecoratedKey>, Long> keyCache;

    private BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    private volatile SSTableDeletingReference phantomReference;

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

    public static SSTableReader open(Descriptor desc) throws IOException
    {
        return open(desc, DatabaseDescriptor.getCFMetaData(desc.ksname, desc.cfname));
    }

    public static SSTableReader open(Descriptor desc, CFMetaData metadata) throws IOException
    {
        return open(desc, componentsFor(desc, false), metadata, StorageService.getPartitioner());
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        return open(descriptor, components, Collections.<DecoratedKey>emptySet(), null, metadata, partitioner);
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, Set<DecoratedKey> savedKeys, DataTracker tracker, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        assert partitioner != null;
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA);
        assert components.contains(Component.PRIMARY_INDEX);

        long start = System.currentTimeMillis();
        logger.info("Opening " + descriptor);

        EstimatedHistogram rowSizes;
        EstimatedHistogram columnCounts;
        File statsFile = new File(descriptor.filenameFor(SSTable.COMPONENT_STATS));
        ReplayPosition rp = ReplayPosition.NONE;
        if (components.contains(Component.STATS) && statsFile.exists())
        {
            DataInputStream dis = null;
            try
            {
                logger.debug("Load metadata for {}", descriptor);
                dis = new DataInputStream(new BufferedInputStream(new FileInputStream(statsFile)));
                rowSizes = EstimatedHistogram.serializer.deserialize(dis);
                columnCounts = EstimatedHistogram.serializer.deserialize(dis);
                if (descriptor.hasReplayPosition())
                    rp = ReplayPosition.serializer.deserialize(dis);
            }
            finally
            {
                FileUtils.closeQuietly(dis);
            }
        }
        else
        {
            logger.debug("No statistics for {}", descriptor);
            rowSizes = SSTable.defaultRowHistogram();
            columnCounts = SSTable.defaultColumnHistogram();
        }

        SSTableReader sstable = new SSTableReader(descriptor, components, metadata, rp, partitioner, null, null, null, null, System.currentTimeMillis(), rowSizes, columnCounts);
        sstable.setTrackedBy(tracker);

        // versions before 'c' encoded keys as utf-16 before hashing to the filter
        if (descriptor.hasStringsInBloomFilter)
        {
            sstable.load(true, savedKeys);
        }
        else
        {
            sstable.load(false, savedKeys);
            sstable.loadBloomFilter();
        }
        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for " + descriptor + ": " + (System.currentTimeMillis() - start) + " ms.");

        if (logger.isDebugEnabled() && sstable.getKeyCache() != null)
            logger.debug(String.format("key cache contains %s/%s keys", sstable.getKeyCache().size(), sstable.getKeyCache().getCapacity()));

        return sstable;
    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    static SSTableReader internalOpen(Descriptor desc, Set<Component> components, CFMetaData metadata, ReplayPosition replayPosition, IPartitioner partitioner, SegmentedFile ifile, SegmentedFile dfile, IndexSummary isummary, Filter bf, long maxDataAge, EstimatedHistogram rowsize,
                                      EstimatedHistogram columncount) throws IOException
    {
        assert desc != null && partitioner != null && ifile != null && dfile != null && isummary != null && bf != null;
        return new SSTableReader(desc, components, metadata, replayPosition, partitioner, ifile, dfile, isummary, bf, maxDataAge, rowsize, columncount);
    }

    private SSTableReader(Descriptor desc,
                          Set<Component> components,
                          CFMetaData metadata,
                          ReplayPosition replayPosition,
                          IPartitioner partitioner,
                          SegmentedFile ifile,
                          SegmentedFile dfile,
                          IndexSummary indexSummary,
                          Filter bloomFilter,
                          long maxDataAge,
                          EstimatedHistogram rowSizes,
                          EstimatedHistogram columnCounts)
    throws IOException
    {
        super(desc, components, metadata, replayPosition, partitioner, rowSizes, columnCounts);
        this.maxDataAge = maxDataAge;

        this.ifile = ifile;
        this.dfile = dfile;
        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
    }

    public void setTrackedBy(DataTracker tracker)
    {
        if (tracker != null)
        {
            phantomReference = new SSTableDeletingReference(tracker, this, finalizerQueue);
            finalizers.add(phantomReference);
            keyCache = tracker.getKeyCache();
        }
    }

    void loadBloomFilter() throws IOException
    {
        if (!components.contains(Component.FILTER))
        {
            bf = BloomFilter.emptyFilter();
            return;
        }

        DataInputStream stream = null;
        try
        {
            stream = new DataInputStream(new BufferedInputStream(new FileInputStream(descriptor.filenameFor(Component.FILTER))));
            if (descriptor.usesOldBloomFilter)
            {
                bf = LegacyBloomFilter.serializer().deserialize(stream, 0); // version means nothing.
            }
            else
            {
                bf = BloomFilter.serializer().deserialize(stream);
            }
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
    }

    /**
     * Loads ifile, dfile and indexSummary, and optionally recreates the bloom filter.
     */
    private void load(boolean recreatebloom, Set<DecoratedKey> keysToLoadInCache) throws IOException
    {
        boolean cacheLoading = keyCache != null && !keysToLoadInCache.isEmpty();
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)),
                                                                      "r",
                                                                      BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE,
                                                                      true);
        try
        {
            if (keyCache != null && keyCache.getCapacity() - keyCache.size() < keysToLoadInCache.size())
                keyCache.updateCapacity(keyCache.size() + keysToLoadInCache.size());

            long indexSize = input.length();
            long estimatedKeys = SSTable.estimateRowsFromIndex(input);
            indexSummary = new IndexSummary(estimatedKeys);
            if (recreatebloom)
                // estimate key count based on index length
                bf = LegacyBloomFilter.getFilter(estimatedKeys, 15);
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                boolean shouldAddEntry = indexSummary.shouldAddEntry();
                ByteBuffer key = (shouldAddEntry || cacheLoading || recreatebloom)
                             ? ByteBufferUtil.readWithShortLength(input)
                             : ByteBufferUtil.skipShortLength(input);
                long dataPosition = input.readLong();
                if (key != null)
                {
                    DecoratedKey decoratedKey = decodeKey(partitioner, descriptor, key);
                    if (recreatebloom)
                        bf.add(decoratedKey.key);
                    if (shouldAddEntry)
                        indexSummary.addEntry(decoratedKey, indexPosition);
                    if (cacheLoading && keysToLoadInCache.contains(decoratedKey))
                        cacheKey(decoratedKey, dataPosition);
                }

                indexSummary.incrementRowid();
                ibuilder.addPotentialBoundary(indexPosition);
                dbuilder.addPotentialBoundary(dataPosition);
            }
            indexSummary.complete();
        }
        finally
        {
            FileUtils.closeQuietly(input);
        }

        // finalize the state of the reader
        ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
        dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    private IndexSummary.KeyPosition getIndexScanPosition(DecoratedKey decoratedKey)
    {
        assert indexSummary.getIndexPositions() != null && indexSummary.getIndexPositions().size() > 0;
        int index = Collections.binarySearch(indexSummary.getIndexPositions(), new IndexSummary.KeyPosition(decoratedKey, -1));
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return null;
            return indexSummary.getIndexPositions().get(greaterThan - 1);
        }
        else
        {
            return indexSummary.getIndexPositions().get(index);
        }
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

    /**
     * @return An estimate of the number of keys in this SSTable.
     */
    public long estimatedKeys()
    {
        return indexSummary.getIndexPositions().size() * DatabaseDescriptor.getIndexInterval();
    }

    /**
     * @return Approximately 1/INDEX_INTERVALth of the keys in this SSTable.
     */
    public Collection<DecoratedKey> getKeySamples()
    {
        return Collections2.transform(indexSummary.getIndexPositions(),
                                      new Function<IndexSummary.KeyPosition, DecoratedKey>(){
                                          public DecoratedKey apply(IndexSummary.KeyPosition kp)
                                          {
                                              return kp.key;
                                          }
                                      });
    }

    private static List<Pair<Integer,Integer>> getSampleIndexesForRanges(List<IndexSummary.KeyPosition> samples, Collection<Range> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Integer,Integer>> positions = new ArrayList<Pair<Integer,Integer>>();
        if (samples.isEmpty())
            return positions;

        for (AbstractBounds range : AbstractBounds.normalize(ranges))
        {
            DecoratedKey leftKey = new DecoratedKey(range.left, null);
            DecoratedKey rightKey = new DecoratedKey(range.right, null);

            int left = Collections.binarySearch(samples, new IndexSummary.KeyPosition(leftKey, -1));
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
                      : Collections.binarySearch(samples, new IndexSummary.KeyPosition(rightKey, -1));
            if (right < 0)
            {
                // range are end inclusive so we use the previous index from what binarySearch give us
                // since that will be the last index we will return
                right = (right + 1) * -1;
                if (right > 0)
                    right--;
            }

            if (left >= right)
                // empty range
                continue;
            positions.add(new Pair(Integer.valueOf(left), Integer.valueOf(right)));
        }
        return positions;
    }

    public Iterable<DecoratedKey> getKeySamples(final Range range)
    {
        final List<IndexSummary.KeyPosition> samples = indexSummary.getIndexPositions();

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
                        return samples.get(idx++).key;
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
    public List<Pair<Long,Long>> getPositionsForRanges(Collection<Range> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Long,Long>> positions = new ArrayList<Pair<Long,Long>>();
        for (AbstractBounds range : AbstractBounds.normalize(ranges))
        {
            long left = getPosition(new DecoratedKey(range.left, null), Operator.GT);
            if (left == -1)
                // left is past the end of the file
                continue;
            long right = getPosition(new DecoratedKey(range.right, null), Operator.GT);
            if (right == -1 || Range.isWrapAround(range.left, range.right))
                // right is past the end of the file, or it wraps
                right = length();
            if (left == right)
                // empty range
                continue;
            positions.add(new Pair(Long.valueOf(left), Long.valueOf(right)));
        }
        return positions;
    }

    public void cacheKey(DecoratedKey key, Long info)
    {
        assert key.key != null;
        // avoid keeping a permanent reference to the original key buffer
        DecoratedKey copiedKey = new DecoratedKey(key.token, ByteBufferUtil.clone(key.key));
        keyCache.put(new Pair<Descriptor, DecoratedKey>(descriptor, copiedKey), info);
    }

    public Long getCachedPosition(DecoratedKey key)
    {
        return getCachedPosition(new Pair<Descriptor, DecoratedKey>(descriptor, key));
    }

    private Long getCachedPosition(Pair<Descriptor, DecoratedKey> unifiedKey)
    {
        if (keyCache != null && keyCache.getCapacity() > 0)
            return keyCache.get(unifiedKey);
        return null;
    }

    /**
     * @param decoratedKey The key to apply as the rhs to the given Operator.
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @return The position in the data file to find the key, or -1 if the key is not present
     */
    public long getPosition(DecoratedKey decoratedKey, Operator op)
    {
        // first, check bloom filter
        if (op == Operator.EQ)
        {
            assert decoratedKey.key != null; // null is ok for GE scans
            if (!bf.isPresent(decoratedKey.key))
                return -1;
        }

        // next, the key cache
        if (op == Operator.EQ || op == Operator.GE)
        {
            Pair<Descriptor, DecoratedKey> unifiedKey = new Pair<Descriptor, DecoratedKey>(descriptor, decoratedKey);
            Long cachedPosition = getCachedPosition(unifiedKey);
            if (cachedPosition != null)
                return cachedPosition;
        }

        // next, see if the sampled index says it's impossible for the key to be present
        IndexSummary.KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            if (op == Operator.EQ)
                bloomFilterTracker.addFalsePositive();
            // we matched the -1th position: if the operator might match forward, return the 0th position
            return op.apply(1) >= 0 ? 0 : -1;
        }

        // scan the on-disk index, starting at the nearest sampled position
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition.indexPosition, INDEX_FILE_BUFFER_BYTES);
        while (segments.hasNext())
        {
            FileDataInput input = segments.next();
            try
            {
                while (!input.isEOF())
                {
                    // read key & data position from index entry
                    DecoratedKey indexDecoratedKey = decodeKey(partitioner, descriptor, ByteBufferUtil.readWithShortLength(input));
                    long dataPosition = input.readLong();

                    int comparison = indexDecoratedKey.compareTo(decoratedKey);
                    int v = op.apply(comparison);
                    if (v == 0)
                    {
                        if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0)
                        {
                            // store exact match for the key
                            if (decoratedKey.key != null)
                                cacheKey(decoratedKey, dataPosition);
                        }
                        if (op == Operator.EQ)
                            bloomFilterTracker.addTruePositive();
                        return dataPosition;
                    }
                    if (v < 0)
                    {
                        if (op == Operator.EQ)
                            bloomFilterTracker.addFalsePositive();
                        return -1;
                    }
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                FileUtils.closeQuietly(input);
            }
        }

        if (op == Operator.EQ)
            bloomFilterTracker.addFalsePositive();
        return -1;
    }

    /**
     * @return The length in bytes of the data file for this SSTable.
     */
    public long length()
    {
        return dfile.length;
    }

    public void markCompacted()
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + getFilename() + " compacted");
        try
        {
            if (!new File(descriptor.filenameFor(Component.COMPACTED_MARKER)).createNewFile())
                throw new IOException("Unable to create compaction marker");
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        phantomReference.deleteOnCleanup();
    }

    /**
     * @param bufferSize Buffer size in bytes for this Scanner.
     * @param filter filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public SSTableScanner getScanner(int bufferSize, QueryFilter filter)
    {
        return new SSTableScanner(this, filter, bufferSize);
    }

   /**
    * Direct I/O SSTableScanner
    * @param bufferSize Buffer size in bytes for this Scanner.
    * @return A Scanner for seeking over the rows of the SSTable.
    */
    public SSTableScanner getDirectScanner(int bufferSize)
    {
        return new SSTableScanner(this, bufferSize, true);
    }

   /**
    * Direct I/O SSTableScanner over a defined range of tokens.
    * @param bufferSize Buffer size in bytes for this Scanner.
    * @param range the range of keys to cover
    * @return A Scanner for seeking over the rows of the SSTable.
    */
    public SSTableScanner getDirectScanner(int bufferSize, Range range)
    {
        return new SSTableBoundedScanner(this, bufferSize, true, range);
    }

    public FileDataInput getFileDataInput(DecoratedKey decoratedKey, int bufferSize)
    {
        long position = getPosition(decoratedKey, Operator.EQ);
        if (position < 0)
            return null;

        return dfile.getSegment(position, bufferSize);
    }


    public int compareTo(SSTableReader o)
    {
        return descriptor.generation - o.descriptor.generation;
    }

    public AbstractType getColumnComparator()
    {
        return metadata.comparator;
    }

    public ColumnFamily createColumnFamily()
    {
        return ColumnFamily.create(metadata);
    }

    public IColumnSerializer getColumnSerializer()
    {
        return metadata.cfType == ColumnFamilyType.Standard
               ? Column.serializer()
               : SuperColumn.serializer(metadata.subcolumnComparator);
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
        if (d.hasIntRowSize)
            return in.readInt();
        return in.readLong();
    }

    public void createLinks(String snapshotDirectoryPath) throws IOException
    {
        for (Component component : components)
        {
            File sourceFile = new File(descriptor.filenameFor(component));
            File targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
            CLibrary.createHardLink(sourceFile, targetLink);
        }
    }

    /**
     * Conditionally use the deprecated 'IPartitioner.convertFromDiskFormat' method.
     */
    public static DecoratedKey decodeKey(IPartitioner p, Descriptor d, ByteBuffer bytes)
    {
        if (d.hasEncodedKeys)
            return p.convertFromDiskFormat(bytes);
        return p.decorateKey(bytes);
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

    public InstrumentingCache<Pair<Descriptor,DecoratedKey>, Long> getKeyCache()
    {
        return keyCache;
    }
}
