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
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;

import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable implements Comparable<SSTableReader>
{
    private static final Logger logger = Logger.getLogger(SSTableReader.class);

    private static final FileSSTableMap openedFiles = new FileSSTableMap();
    
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
                    FileDeletingReference r = null;
                    try
                    {
                        r = (FileDeletingReference) finalizerQueue.remove();
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

    public static int indexInterval()
    {
        return INDEX_INTERVAL;
    }

    public static long getApproximateKeyCount()
    {
        return getApproximateKeyCount(openedFiles.values());
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

    /**
     * Get all indexed keys in any SSTable for our primary range
     * TODO add option to include keys from one or more other ranges
     */
    public static List<DecoratedKey> getIndexedDecoratedKeys()
    {
        Range range = StorageService.instance().getLocalPrimaryRange();
        List<DecoratedKey> indexedKeys = new ArrayList<DecoratedKey>();
        
        for (SSTableReader sstable : openedFiles.values())
        {
            for (KeyPosition kp : sstable.getIndexPositions())
            {
                if (range.contains(kp.key.token))
                {
                    indexedKeys.add(kp.key);
                }
            }
        }
        Collections.sort(indexedKeys);

        return indexedKeys;
    }

    public static SSTableReader open(String dataFileName) throws IOException
    {
        return open(dataFileName, StorageService.getPartitioner(), DatabaseDescriptor.getKeysCachedFraction(parseTableName(dataFileName)));
    }

    public static SSTableReader open(String dataFileName, IPartitioner partitioner, double cacheFraction) throws IOException
    {
        assert partitioner != null;
        assert openedFiles.get(dataFileName) == null;

        long start = System.currentTimeMillis();
        SSTableReader sstable = new SSTableReader(dataFileName, partitioner);
        sstable.loadIndexFile();
        sstable.loadBloomFilter();
        if (cacheFraction > 0)
        {
            sstable.keyCache = createKeyCache((int)((sstable.getIndexPositions().size() + 1) * INDEX_INTERVAL * cacheFraction));
        }
        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for "  + dataFileName + ": " + (System.currentTimeMillis() - start) + " ms.");

        return sstable;
    }

    FileDeletingReference phantomReference;

    public static ConcurrentLinkedHashMap<DecoratedKey, Long> createKeyCache(int size)
    {
        return ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, size);
    }

    private ConcurrentLinkedHashMap<DecoratedKey, Long> keyCache;

    SSTableReader(String filename, IPartitioner partitioner, List<KeyPosition> indexPositions, BloomFilter bloomFilter, ConcurrentLinkedHashMap<DecoratedKey, Long> keyCache)
    {
        super(filename, partitioner);
        this.indexPositions = indexPositions;
        this.bf = bloomFilter;
        phantomReference = new FileDeletingReference(this, finalizerQueue);
        finalizers.add(phantomReference);
        openedFiles.put(filename, this);
        this.keyCache = keyCache;
    }

    private SSTableReader(String filename, IPartitioner partitioner)
    {
        this(filename, partitioner, null, null, null);
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
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        try
        {
            indexPositions = new ArrayList<KeyPosition>();

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
                input.readLong();
                if (i++ % INDEX_INTERVAL == 0)
                {
                    indexPositions.add(new KeyPosition(decoratedKey, indexPosition));
                }
            }
        }
        finally
        {
            input.close();
        }
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    private long getIndexScanPosition(DecoratedKey decoratedKey, IPartitioner partitioner)
    {
        assert indexPositions != null && indexPositions.size() > 0;
        int index = Collections.binarySearch(indexPositions, new KeyPosition(decoratedKey, -1));
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return -1;
            return indexPositions.get(greaterThan - 1).position;
        }
        else
        {
            return indexPositions.get(index).position;
        }
    }

    /**
     * returns the position in the data file to find the given key, or -1 if the key is not present
     */
    public long getPosition(DecoratedKey decoratedKey) throws IOException
    {
        if (!bf.isPresent(partitioner.convertToDiskFormat(decoratedKey)))
            return -1;
        if (keyCache != null)
        {
            Long cachedPosition = keyCache.get(decoratedKey);
            if (cachedPosition != null)
            {
                return cachedPosition;
            }
        }
        long start = getIndexScanPosition(decoratedKey, partitioner);
        if (start < 0)
        {
            return -1;
        }

        // TODO mmap the index file?
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(path), "r");
        input.seek(start);
        int i = 0;
        try
        {
            do
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
                int v = partitioner.getDecoratedKeyComparator().compare(indexDecoratedKey, decoratedKey);
                if (v == 0)
                {
                    if (keyCache != null)
                        keyCache.put(decoratedKey, position);
                    return position;
                }
                if (v > 0)
                    return -1;
            } while  (++i < INDEX_INTERVAL);
        }
        finally
        {
            input.close();
        }
        return -1;
    }

    /** like getPosition, but if key is not found will return the location of the first key _greater_ than the desired one, or -1 if no such key exists. */
    public long getNearestPosition(DecoratedKey decoratedKey) throws IOException
    {
        long start = getIndexScanPosition(decoratedKey, partitioner);
        if (start < 0)
        {
            return 0;
        }
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(path), "r");
        input.seek(start);
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
                int v = partitioner.getDecoratedKeyComparator().compare(indexDecoratedKey, decoratedKey);
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
        openedFiles.remove(path);
        new File(compactedFilename()).createNewFile();
        phantomReference.deleteOnCleanup();
    }

    /** obviously only for testing */
    public void forceBloomFilterFailures()
    {
        bf = BloomFilter.alwaysMatchingBloomFilter();
    }

    static void reopenUnsafe() throws IOException // testing only
    {
        Collection<SSTableReader> sstables = new ArrayList<SSTableReader>(openedFiles.values());
        openedFiles.clear();
        for (SSTableReader sstable : sstables)
        {
            SSTableReader.open(sstable.path, sstable.partitioner, 0.01);
        }
    }

    public IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public SSTableScanner getScanner() throws IOException
    {
        return new SSTableScanner(this);
    }

    public String getTableName()
    {
        return parseTableName(path);
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

class FileDeletingReference extends PhantomReference<SSTableReader>
{
    public final String path;
    private boolean deleteOnCleanup;

    FileDeletingReference(SSTableReader referent, ReferenceQueue<? super SSTableReader> q)
    {
        super(referent, q);
        this.path = referent.path;
    }

    public void deleteOnCleanup()
    {
        deleteOnCleanup = true;
    }

    public void cleanup() throws IOException
    {
        if (deleteOnCleanup)
        {
            SSTable.delete(path);
        }
    }
}
