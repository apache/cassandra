/**
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

package org.apache.cassandra.io;

import java.io.*;
import java.util.*;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.Reference;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;

import org.apache.log4j.Logger;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.MappedFileDataInput;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable implements Comparable<SSTableReader>
{
    private static final Logger logger = Logger.getLogger(SSTableReader.class);

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
                    SSTableDeletingReference r = null;
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
                        logger.error("Error deleting " + r.path, e);
                    }
                }
            }
        };
        new Thread(runnable, "SSTABLE-DELETER").start();
    }};
    // in a perfect world, BUFFER_SIZE would be final, but we need to test with a smaller size to stay sane.
    static long BUFFER_SIZE = Integer.MAX_VALUE;

    public static int indexInterval()
    {
        return IndexSummary.INDEX_INTERVAL;
    }

    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = 0;

        for (SSTableReader sstable : sstables)
        {
            int indexKeyCount = sstable.getIndexPositions().size();
            count = count + (indexKeyCount + 1) * IndexSummary.INDEX_INTERVAL;
            if (logger.isDebugEnabled())
                logger.debug("index size for bloom filter calc for file  : " + sstable.getFilename() + "   : " + count);
        }

        return count;
    }

    public static SSTableReader open(String dataFileName) throws IOException
    {
        return open(dataFileName, StorageService.getPartitioner());
    }

    /** public, but only for tests */
    public static SSTableReader open(String dataFileName, IPartitioner partitioner) throws IOException
    {
        assert partitioner != null;

        long start = System.currentTimeMillis();
        SSTableReader sstable = new SSTableReader(dataFileName, partitioner);
        logger.info("Sampling index for " + dataFileName);
        sstable.loadIndexFile();
        sstable.loadBloomFilter();

        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for "  + dataFileName + ": " + (System.currentTimeMillis() - start) + " ms.");

        return sstable;
    }

    private volatile SSTableDeletingReference phantomReference;
    // jvm can only map up to 2GB at a time, so we split index/data into segments of that size when using mmap i/o
    private final MappedByteBuffer[] indexBuffers;
    private final MappedByteBuffer[] buffers;

    private InstrumentedCache<Pair<String, DecoratedKey>, PositionSize> keyCache;

    private BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    SSTableReader(String filename, IPartitioner partitioner, IndexSummary indexSummary, BloomFilter bloomFilter)
    throws IOException
    {
        super(filename, partitioner);

        if (DatabaseDescriptor.getIndexAccessMode() == DatabaseDescriptor.DiskAccessMode.mmap)
        {
            long indexLength = new File(indexFilename()).length();
            int bufferCount = 1 + (int) (indexLength / BUFFER_SIZE);
            indexBuffers = new MappedByteBuffer[bufferCount];
            long remaining = indexLength;
            for (int i = 0; i < bufferCount; i++)
            {
                indexBuffers[i] = mmap(indexFilename(), i * BUFFER_SIZE, (int) Math.min(remaining, BUFFER_SIZE));
                remaining -= BUFFER_SIZE;
            }
        }
        else
        {
            assert DatabaseDescriptor.getIndexAccessMode() == DatabaseDescriptor.DiskAccessMode.standard;
            indexBuffers = null;
        }

        if (DatabaseDescriptor.getDiskAccessMode() == DatabaseDescriptor.DiskAccessMode.mmap)
        {
            int bufferCount = 1 + (int) (new File(path).length() / BUFFER_SIZE);
            buffers = new MappedByteBuffer[bufferCount];
            long remaining = length();
            for (int i = 0; i < bufferCount; i++)
            {
                buffers[i] = mmap(path, i * BUFFER_SIZE, (int) Math.min(remaining, BUFFER_SIZE));
                remaining -= BUFFER_SIZE;
            }
        }
        else
        {
            assert DatabaseDescriptor.getDiskAccessMode() == DatabaseDescriptor.DiskAccessMode.standard;
            buffers = null;
        }

        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
    }

    public void setTrackedBy(SSTableTracker tracker)
    {
        phantomReference = new SSTableDeletingReference(tracker, this, finalizerQueue);
        finalizers.add(phantomReference);
        // TODO keyCache should never be null in live Cassandra, but only setting it here
        // means it can be during tests, so we have to do otherwise-unnecessary != null checks
        keyCache = tracker.getKeyCache();
    }

    private static MappedByteBuffer mmap(String filename, long start, int size) throws IOException
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile(filename, "r");
        }
        catch (FileNotFoundException e)
        {
            throw new IOError(e);
        }

        try
        {
            return raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size);
        }
        finally
        {
            raf.close();
        }
    }

    private SSTableReader(String filename, IPartitioner partitioner) throws IOException
    {
        this(filename, partitioner, null, null);
    }

    public List<IndexSummary.KeyPosition> getIndexPositions()
    {
        return indexSummary.getIndexPositions();
    }

    public long estimatedKeys()
    {
        return indexSummary.getIndexPositions().size() * IndexSummary.INDEX_INTERVAL;
    }

    void loadBloomFilter() throws IOException
    {
        DataInputStream stream = new DataInputStream(new FileInputStream(filterFilename()));
        try
        {
            bf = BloomFilter.serializer().deserialize(stream);
        }
        finally
        {
            stream.close();
        }
    }

    void loadIndexFile() throws IOException
    {
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        // any entries that do, we force into the in-memory sample so key lookup can always bsearch within
        // a single mmapped segment.
        indexSummary = new IndexSummary();
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        try
        {
            long indexSize = input.length();
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                {
                    break;
                }
                DecoratedKey decoratedKey = partitioner.convertFromDiskFormat(input.readUTF());
                long dataPosition = input.readLong();
                long nextIndexPosition = input.getFilePointer();
                // read the next index entry to see how big the row is
                long nextDataPosition;
                if (input.isEOF())
                {
                    nextDataPosition = length();
                }
                else
                {
                    input.readUTF();
                    nextDataPosition = input.readLong();
                    input.seek(nextIndexPosition);
                }
                indexSummary.maybeAddEntry(decoratedKey, dataPosition, nextDataPosition - dataPosition, indexPosition, nextIndexPosition);
            }
            indexSummary.complete();
        }
        finally
        {
            input.close();
        }
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
     * returns the position in the data file to find the given key, or -1 if the key is not present
     */
    public PositionSize getPosition(DecoratedKey decoratedKey) throws IOException
    {
        // first, check bloom filter
        if (!bf.isPresent(partitioner.convertToDiskFormat(decoratedKey)))
            return null;

        // next, the key cache
        Pair<String, DecoratedKey> unifiedKey = new Pair<String, DecoratedKey>(path, decoratedKey);
        if (keyCache != null && keyCache.getCapacity() > 0)
        {
            PositionSize cachedPosition = keyCache.get(unifiedKey);
            if (cachedPosition != null)
            {
                return cachedPosition;
            }
        }

        // next, see if the sampled index says it's impossible for the key to be present
        IndexSummary.KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            bloomFilterTracker.addFalsePositive();
            return null;
        }

        // get either a buffered or a mmap'd input for the on-disk index
        long p = sampledPosition.indexPosition;
        FileDataInput input;
        if (indexBuffers == null)
        {
            input = new BufferedRandomAccessFile(indexFilename(), "r");
            ((BufferedRandomAccessFile)input).seek(p);
        }
        else
        {
            input = indexInputAt(p);
        }

        // scan the on-disk index, starting at the nearest sampled position
        try
        {
            int i = 0;
            do
            {
                // handle exact sampled index hit
                IndexSummary.KeyPosition kp = indexSummary.getSpannedIndexPosition(input.getAbsolutePosition());
                if (kp != null && kp.key.equals(decoratedKey))
                {
                    bloomFilterTracker.addTruePositive();
                    return indexSummary.getSpannedDataPosition(kp);
                }
                // if using mmapped i/o, skip to the next mmap buffer if necessary
                if (input.isEOF() || kp != null)
                {
                    if (indexBuffers == null) // not mmap-ing, just one index input
                        break;

                    FileDataInput oldInput = input;
                    if (kp == null)
                    {
                        input = indexInputAt(input.getAbsolutePosition());
                    }
                    else
                    {
                        long nextUnspannedPostion = input.getAbsolutePosition()
                                                    + 2 + FBUtilities.encodedUTF8Length(StorageService.getPartitioner().convertToDiskFormat(kp.key))
                                                    + 8;
                        input = indexInputAt(nextUnspannedPostion);
                    }
                    oldInput.close();
                    if (input == null)
                        break;

                    continue;
                }

                // read key & data position from index entry
                DecoratedKey indexDecoratedKey = partitioner.convertFromDiskFormat(input.readUTF());
                long dataPosition = input.readLong();

                int v = indexDecoratedKey.compareTo(decoratedKey);
                if (v == 0)
                {
                    PositionSize info = getDataPositionSize(input, dataPosition);
                    if (keyCache != null && keyCache.getCapacity() > 0)
                        keyCache.put(unifiedKey, info);
                    bloomFilterTracker.addTruePositive();
                    return info;
                }
                if (v > 0)
                {
                    bloomFilterTracker.addFalsePositive();
                    return null;
                }
            } while  (++i < IndexSummary.INDEX_INTERVAL);
        }
        finally
        {
            if (input != null)
                input.close();
        }
        bloomFilterTracker.addFalsePositive();
        return null;
    }

    private FileDataInput indexInputAt(long indexPosition)
    {
        if (indexPosition > indexSummary.getLastIndexPosition())
            return null;
        int bufferIndex = bufferIndex(indexPosition);
        return new MappedFileDataInput(indexBuffers[bufferIndex], indexFilename(), BUFFER_SIZE * bufferIndex, (int)(indexPosition % BUFFER_SIZE));
    }

    private PositionSize getDataPositionSize(FileDataInput input, long dataPosition) throws IOException
    {
        // if we've reached the end of the index, then the row size is "the rest of the data file"
        if (input.isEOF())
            return new PositionSize(dataPosition, length() - dataPosition);

        // otherwise, row size is the start of the next row (in next index entry), minus the start of this one.
        long nextIndexPosition = input.getAbsolutePosition();
        // if next index entry would span mmap boundary, get the next row position from the summary instead
        PositionSize nextPositionSize = indexSummary.getSpannedDataPosition(nextIndexPosition);
        if (nextPositionSize != null)
            return new PositionSize(dataPosition, nextPositionSize.position - dataPosition);

        // read next entry directly
        int utflen = input.readUnsignedShort();
        if (utflen != input.skipBytes(utflen))
            throw new EOFException();
        return new PositionSize(dataPosition, input.readLong() - dataPosition);
    }

    /** like getPosition, but if key is not found will return the location of the first key _greater_ than the desired one, or -1 if no such key exists. */
    public long getNearestPosition(DecoratedKey decoratedKey) throws IOException
    {
        IndexSummary.KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            return 0;
        }

        // can't use a MappedFileDataInput here, since we might cross a segment boundary while scanning
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(path), "r");
        input.seek(sampledPosition.indexPosition);
        try
        {
            while (true)
            {
                DecoratedKey indexDecoratedKey;
                try
                {
                    indexDecoratedKey = partitioner.convertFromDiskFormat(input.readUTF());
                }
                catch (EOFException e)
                {
                    return -1;
                }
                long position = input.readLong();
                int v = indexDecoratedKey.compareTo(decoratedKey);
                if (v >= 0)
                    return position;
            }
        }
        finally
        {
            input.close();
        }
    }

    public long length()
    {
        return new File(path).length();
    }

    public int compareTo(SSTableReader o)
    {
        return ColumnFamilyStore.getGenerationFromFileName(path) - ColumnFamilyStore.getGenerationFromFileName(o.path);
    }

    public void markCompacted() throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + path + " compacted");
        if (!new File(compactedFilename()).createNewFile())
        {
            throw new IOException("Unable to create compaction marker");
        }
        phantomReference.deleteOnCleanup();
    }

    /** obviously only for testing */
    public void forceBloomFilterFailures()
    {
        bf = BloomFilter.alwaysMatchingBloomFilter();
    }

    public IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public SSTableScanner getScanner(int bufferSize) throws IOException
    {
        return new SSTableScanner(this, bufferSize);
    }

    public FileDataInput getFileDataInput(DecoratedKey decoratedKey, int bufferSize) throws IOException
    {
        PositionSize info = getPosition(decoratedKey);
        if (info == null)
            return null;

        if (buffers == null || (bufferIndex(info.position) != bufferIndex(info.position + info.size)))
        {
            BufferedRandomAccessFile file = new BufferedRandomAccessFile(path, "r", bufferSize);
            file.seek(info.position);
            return file;
        }
        return new MappedFileDataInput(buffers[bufferIndex(info.position)], path, BUFFER_SIZE * (info.position / BUFFER_SIZE), (int) (info.position % BUFFER_SIZE));
    }

    static int bufferIndex(long position)
    {
        return (int) (position / BUFFER_SIZE);
    }

    public AbstractType getColumnComparator()
    {
        return DatabaseDescriptor.getComparator(getTableName(), getColumnFamilyName());
    }

    public ColumnFamily makeColumnFamily()
    {
        return ColumnFamily.create(getTableName(), getColumnFamilyName());
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        return DatabaseDescriptor.getColumnFamilyType(getTableName(), getColumnFamilyName()).equals("Standard")
               ? Column.serializer()
               : SuperColumn.serializer(getColumnComparator());
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
