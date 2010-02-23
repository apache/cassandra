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

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.MappedFileDataInput;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

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
    private static final long BUFFER_SIZE = Integer.MAX_VALUE;

    public static int indexInterval()
    {
        return INDEX_INTERVAL;
    }

    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = 0;

        for (SSTableReader sstable : sstables)
        {
            int indexKeyCount = sstable.getIndexPositions().size();
            count = count + (indexKeyCount + 1) * INDEX_INTERVAL;
            if (logger.isDebugEnabled())
                logger.debug("index size for bloom filter calc for file  : " + sstable.getFilename() + "   : " + count);
        }

        return count;
    }

    public static SSTableReader open(String dataFileName) throws IOException
    {
        return open(dataFileName,
                    StorageService.getPartitioner(),
                    DatabaseDescriptor.getKeysCachedFraction(parseTableName(dataFileName), parseColumnFamilyName(dataFileName)));
    }

    public static SSTableReader open(String dataFileName, IPartitioner partitioner, double keysCacheFraction) throws IOException
    {
        assert partitioner != null;

        long start = System.currentTimeMillis();
        SSTableReader sstable = new SSTableReader(dataFileName, partitioner);
        logger.info("Sampling index for " + dataFileName);
        sstable.loadIndexFile();
        sstable.loadBloomFilter();
        if (keysCacheFraction > 0)
        {
            sstable.keyCache = createKeyCache((int)((sstable.getIndexPositions().size() + 1) * INDEX_INTERVAL * keysCacheFraction));
        }
        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for "  + dataFileName + ": " + (System.currentTimeMillis() - start) + " ms.");

        return sstable;
    }

    private volatile SSTableDeletingReference phantomReference;
    // jvm can only map up to 2GB at a time, so we split index/data into segments of that size when using mmap i/o
    private final MappedByteBuffer[] indexBuffers;
    private final MappedByteBuffer[] buffers;


    public static InstrumentedCache<DecoratedKey, PositionSize> createKeyCache(int size)
    {
        return new InstrumentedCache<DecoratedKey, PositionSize>(size);
    }

    private InstrumentedCache<DecoratedKey, PositionSize> keyCache;

    SSTableReader(String filename,
                  IPartitioner partitioner,
                  List<KeyPosition> indexPositions, Map<KeyPosition, PositionSize> spannedIndexDataPositions,
                  BloomFilter bloomFilter,
                  InstrumentedCache<DecoratedKey, PositionSize> keyCache)
            throws IOException
    {
        super(filename, partitioner);
        assert keyCache != null;

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

        this.indexPositions = indexPositions;
        this.spannedIndexDataPositions = spannedIndexDataPositions;
        this.bf = bloomFilter;
        this.keyCache = keyCache;
    }

    public void addFinalizingReference(SSTableTracker tracker)
    {
        phantomReference = new SSTableDeletingReference(tracker, this, finalizerQueue);
        finalizers.add(phantomReference);
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
        this(filename, partitioner, null, null, null, SSTableReader.createKeyCache(0));
    }

    public List<KeyPosition> getIndexPositions()
    {
        return indexPositions;
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
        indexPositions = new ArrayList<KeyPosition>();
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        // any entries that do, we force into the in-memory sample so key lookup can always bsearch within
        // a single mmapped segment.
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        try
        {
            int i = 0;
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
                boolean spannedEntry = bufferIndex(indexPosition) != bufferIndex(nextIndexPosition);
                if (i++ % INDEX_INTERVAL == 0 || spannedEntry)
                {
                    KeyPosition info;
                    info = new KeyPosition(decoratedKey, indexPosition);
                    indexPositions.add(info);

                    if (spannedEntry)
                    {
                        if (spannedIndexDataPositions == null)
                        {
                            spannedIndexDataPositions = new HashMap<KeyPosition, PositionSize>();
                        }
                        // read the next index entry to see how big the row is corresponding to the current, mmap-segment-spanning one
                        input.readUTF();
                        long nextDataPosition = input.readLong();
                        input.seek(nextIndexPosition);
                        spannedIndexDataPositions.put(info, new PositionSize(dataPosition, nextDataPosition - dataPosition));
                    }
                }
            }
        }
        finally
        {
            input.close();
        }
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    private KeyPosition getIndexScanPosition(DecoratedKey decoratedKey)
    {
        assert indexPositions != null && indexPositions.size() > 0;
        int index = Collections.binarySearch(indexPositions, new KeyPosition(decoratedKey, -1));
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return null;
            return indexPositions.get(greaterThan - 1);
        }
        else
        {
            return indexPositions.get(index);
        }
    }

    /**
     * returns the position in the data file to find the given key, or -1 if the key is not present
     */
    public PositionSize getPosition(DecoratedKey decoratedKey) throws IOException
    {
        if (!bf.isPresent(partitioner.convertToDiskFormat(decoratedKey)))
            return null;
        if (keyCache.getCapacity() > 0)
        {
            PositionSize cachedPosition = keyCache.get(decoratedKey);
            if (cachedPosition != null)
            {
                return cachedPosition;
            }
        }
        KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            return null;
        }
        if (spannedIndexDataPositions != null)
        {
            PositionSize info = spannedIndexDataPositions.get(sampledPosition);
            if (info != null)
                return info;
        }

        long p = sampledPosition.position;
        FileDataInput input;
        if (indexBuffers == null)
        {
            input = new BufferedRandomAccessFile(indexFilename(), "r");
            ((BufferedRandomAccessFile)input).seek(p);
        }
        else
        {
            input = new MappedFileDataInput(indexBuffers[bufferIndex(p)], indexFilename(), (int)(p % BUFFER_SIZE));
        }
        try
        {
            int i = 0;
            do
            {
                DecoratedKey indexDecoratedKey;
                try
                {
                    indexDecoratedKey = partitioner.convertFromDiskFormat(input.readUTF());
                }
                catch (EOFException e)
                {
                    return null;
                }
                long position = input.readLong();
                int v = indexDecoratedKey.compareTo(decoratedKey);
                if (v == 0)
                {
                    PositionSize info;
                    if (!input.isEOF())
                    {
                        int utflen = input.readUnsignedShort();
                        if (utflen != input.skipBytes(utflen))
                            throw new EOFException();
                        info = new PositionSize(position, input.readLong() - position);
                    }
                    else
                    {
                        info = new PositionSize(position, length() - position);
                    }
                    if (keyCache.getCapacity() > 0)
                        keyCache.put(decoratedKey, info);
                    return info;
                }
                if (v > 0)
                    return null;
            } while  (++i < INDEX_INTERVAL);
        }
        finally
        {
            input.close();
        }
        return null;
    }

    /** like getPosition, but if key is not found will return the location of the first key _greater_ than the desired one, or -1 if no such key exists. */
    public long getNearestPosition(DecoratedKey decoratedKey) throws IOException
    {
        KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            return 0;
        }

        // can't use a MappedFileDataInput here, since we might cross a segment boundary while scanning
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(path), "r");
        input.seek(sampledPosition.position);
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
        return new MappedFileDataInput(buffers[bufferIndex(info.position)], path, (int) (info.position % BUFFER_SIZE));
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

    public InstrumentedCache<DecoratedKey, PositionSize> getKeyCache()
    {
        return keyCache;
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        return DatabaseDescriptor.getColumnFamilyType(getTableName(), getColumnFamilyName()).equals("Standard")
               ? Column.serializer()
               : SuperColumn.serializer(getColumnComparator());
    }
}

class FileSSTableMap
{
    private final Map<String, SSTableReader> map = new NonBlockingHashMap<String, SSTableReader>();

    public SSTableReader get(String filename)
    {
        try
        {
            return map.get(new File(filename).getCanonicalPath());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public SSTableReader put(String filename, SSTableReader value)
    {
        try
        {
            return map.put(new File(filename).getCanonicalPath(), value);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Collection<SSTableReader> values()
    {
        return map.values();
    }

    public void clear()
    {
        map.clear();
    }

    public void remove(String filename) throws IOException
    {
        map.remove(new File(filename).getCanonicalPath());
    }

    @Override
    public String toString()
    {
        return "FileSSTableMap {" + StringUtils.join(map.keySet(), ", ") + "}";
    }
}
