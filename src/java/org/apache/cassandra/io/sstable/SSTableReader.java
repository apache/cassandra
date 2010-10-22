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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

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
    private BloomFilter bf;

    private InstrumentedCache<Pair<Descriptor,DecoratedKey>, Long> keyCache;

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
        Set<Component> components = SSTable.componentsFor(desc);
        return open(desc, components, DatabaseDescriptor.getCFMetaData(desc.ksname, desc.cfname), StorageService.getPartitioner());
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        return open(descriptor, components, Collections.<DecoratedKey>emptySet(), null, metadata, partitioner);
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, Set<DecoratedKey> savedKeys, SSTableTracker tracker, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        assert partitioner != null;

        long start = System.currentTimeMillis();
        logger.info("Sampling index for " + descriptor);

        EstimatedHistogram rowSizes;
        EstimatedHistogram columnCounts;
        File statsFile = new File(descriptor.filenameFor(SSTable.COMPONENT_STATS));
        if (statsFile.exists())
        {
            logger.debug("Load statistics for {}", descriptor);
            DataInputStream dis = new DataInputStream(new FileInputStream(statsFile));
            rowSizes = EstimatedHistogram.serializer.deserialize(dis);
            columnCounts = EstimatedHistogram.serializer.deserialize(dis);
            dis.close();
        }
        else
        {
            logger.debug("No statistics for {}", descriptor);
            rowSizes = SSTable.defaultRowHistogram();
            columnCounts = SSTable.defaultColumnHistogram();
        }

        SSTableReader sstable = new SSTableReader(descriptor, components, metadata, partitioner, null, null, null, null, System.currentTimeMillis(), rowSizes, columnCounts);
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
            logger.debug(String.format("key cache contains %s/%s keys", sstable.getKeyCache().getSize(), sstable.getKeyCache().getCapacity()));

        return sstable;
    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    static SSTableReader internalOpen(Descriptor desc, Set<Component> components, CFMetaData metadata, IPartitioner partitioner, SegmentedFile ifile, SegmentedFile dfile, IndexSummary isummary, BloomFilter bf, long maxDataAge, EstimatedHistogram rowsize,
                                      EstimatedHistogram columncount) throws IOException
    {
        assert desc != null && partitioner != null && ifile != null && dfile != null && isummary != null && bf != null;
        return new SSTableReader(desc, components, metadata, partitioner, ifile, dfile, isummary, bf, maxDataAge, rowsize, columncount);
    }

    private SSTableReader(Descriptor desc,
                          Set<Component> components,
                          CFMetaData metadata,
                          IPartitioner partitioner,
                          SegmentedFile ifile,
                          SegmentedFile dfile,
                          IndexSummary indexSummary,
                          BloomFilter bloomFilter,
                          long maxDataAge,
                          EstimatedHistogram rowSizes,
                          EstimatedHistogram columnCounts)
    throws IOException
    {
        super(desc, components, metadata, partitioner, rowSizes, columnCounts);
        this.maxDataAge = maxDataAge;

        this.ifile = ifile;
        this.dfile = dfile;
        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
    }

    public void setTrackedBy(SSTableTracker tracker)
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
        DataInputStream stream = new DataInputStream(new FileInputStream(descriptor.filenameFor(Component.FILTER)));
        try
        {
            bf = BloomFilter.serializer().deserialize(stream);
        }
        finally
        {
            stream.close();
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
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(descriptor.filenameFor(Component.PRIMARY_INDEX), "r");
        try
        {
            if (keyCache != null && keyCache.getCapacity() - keyCache.getSize() < keysToLoadInCache.size())
                keyCache.updateCapacity(keyCache.getSize() + keysToLoadInCache.size());

            long indexSize = input.length();
            long estimatedKeys = SSTable.estimateRowsFromIndex(input);
            indexSummary = new IndexSummary(estimatedKeys);
            if (recreatebloom)
                // estimate key count based on index length
                bf = BloomFilter.getFilter(estimatedKeys, 15);
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                boolean shouldAddEntry = indexSummary.shouldAddEntry();
                ByteBuffer key = (ByteBuffer) ((shouldAddEntry || cacheLoading || recreatebloom)
                             ? FBUtilities.readShortByteArray(input)
                             : FBUtilities.skipShortByteArray(input));
                long dataPosition = input.readLong();
                if (key != null)
                {
                    DecoratedKey decoratedKey = decodeKey(partitioner, descriptor, key);
                    if (recreatebloom)
                        bf.add(decoratedKey.key);
                    if (shouldAddEntry)
                        indexSummary.addEntry(decoratedKey, indexPosition);
                    if (cacheLoading && keysToLoadInCache.contains(decoratedKey))
                        keyCache.put(new Pair(descriptor, decoratedKey), dataPosition);
                }

                indexSummary.incrementRowid();
                ibuilder.addPotentialBoundary(indexPosition);
                dbuilder.addPotentialBoundary(dataPosition);
            }
            indexSummary.complete();
        }
        finally
        {
            input.close();
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
        bf = BloomFilter.alwaysMatchingBloomFilter();
    }

    public BloomFilter getBloomFilter()
    {
      return bf;
    }

    /**
     * @return The key cache: for monitoring purposes.
     */
    public InstrumentedCache getKeyCache()
    {
        return keyCache;
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

    /**
     * @param decoratedKey The key to apply as the rhs to the given Operator.
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @return The position in the data file to find the key, or -1 if the key is not present
     */
    public long getPosition(DecoratedKey decoratedKey, Operator op)
    {
        // first, check bloom filter
        if (op == Operator.EQ && !bf.isPresent(decoratedKey.key))
            return -1;

        // next, the key cache
        Pair<Descriptor, DecoratedKey> unifiedKey = new Pair<Descriptor, DecoratedKey>(descriptor, decoratedKey);
        if (keyCache != null && keyCache.getCapacity() > 0)
        {
            Long cachedPosition = keyCache.get(unifiedKey);
            if (cachedPosition != null)
            {
                return cachedPosition;
            }
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
                    DecoratedKey indexDecoratedKey = decodeKey(partitioner, descriptor, FBUtilities.readShortByteArray(input));
                    long dataPosition = input.readLong();

                    int comparison = indexDecoratedKey.compareTo(decoratedKey);
                    int v = op.apply(comparison);
                    if (v == 0)
                    {
                        if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0)
                        {
                            if (op == Operator.EQ)
                                bloomFilterTracker.addTruePositive();
                            // store exact match for the key
                            keyCache.put(unifiedKey, Long.valueOf(dataPosition));
                        }
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
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    logger.error("error closing file", e);
                }
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
        components.add(Component.COMPACTED_MARKER);
        phantomReference.deleteOnCleanup();
    }

    /**
     * @param bufferSize Buffer size in bytes for this Scanner.
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public SSTableScanner getScanner(int bufferSize)
    {
        return new SSTableScanner(this, bufferSize);
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

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        return metadata.cfType == ColumnFamilyType.Standard
               ? Column.serializer()
               : SuperColumn.serializer(getColumnComparator());
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
}
