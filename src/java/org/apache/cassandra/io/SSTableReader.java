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

import org.apache.log4j.Logger;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not use open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable
{
    private static final Logger logger = Logger.getLogger(SSTableReader.class);

    private static final FileSSTableMap openedFiles = new FileSSTableMap();

    public static int indexInterval()
    {
        return INDEX_INTERVAL;
    }

    // todo can we refactor to take list of sstables?
    public static int getApproximateKeyCount(List<String> dataFiles)
    {
        int count = 0;

        for (String dataFileName : dataFiles)
        {
            SSTableReader sstable = openedFiles.get(dataFileName);
            assert sstable != null;
            int indexKeyCount = sstable.getIndexPositions().size();
            count = count + (indexKeyCount + 1) * INDEX_INTERVAL;
            if (logger.isDebugEnabled())
                logger.debug("index size for bloom filter calc for file  : " + dataFileName + "   : " + count);
        }

        return count;
    }

    /**
     * Get all indexed keys in the SSTable.
     */
    public static List<String> getIndexedKeys()
    {
        List<String> indexedKeys = new ArrayList<String>();

        for (SSTableReader sstable : openedFiles.values())
        {
            for (KeyPosition kp : sstable.getIndexPositions())
            {
                indexedKeys.add(kp.key);
            }
        }
        Collections.sort(indexedKeys);

        return indexedKeys;
    }

    public static SSTableReader open(String dataFileName) throws IOException
    {
        return open(dataFileName, StorageService.getPartitioner());
    }

    public static synchronized SSTableReader open(String dataFileName, IPartitioner partitioner) throws IOException
    {
        assert partitioner != null;
        assert openedFiles.get(dataFileName) == null;

        long start = System.currentTimeMillis();
        SSTableReader sstable = new SSTableReader(dataFileName, partitioner);
        sstable.loadIndexFile();
        sstable.loadBloomFilter();
        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for "  + dataFileName + ": " + (System.currentTimeMillis() - start) + " ms.");

        return sstable;
    }

    @Deprecated // move away from get() towards using the SSTR objects CFS knows about
    public static SSTableReader get(String dataFileName)
    {
        SSTableReader sstable = openedFiles.get(dataFileName);
        assert sstable != null;
        return sstable;
    }

    SSTableReader(String filename, IPartitioner partitioner, List<KeyPosition> indexPositions, BloomFilter bloomFilter)
    {
        super(filename, partitioner);
        this.indexPositions = indexPositions;
        this.bf = bloomFilter;
        openedFiles.put(filename, this);
    }

    private SSTableReader(String filename, IPartitioner partitioner)
    {
        super(filename, partitioner);
    }

    public List<KeyPosition> getIndexPositions()
    {
        return indexPositions;
    }

    private void loadBloomFilter() throws IOException
    {
        DataInputStream stream = new DataInputStream(new FileInputStream(filterFilename()));
        bf = BloomFilter.serializer().deserialize(stream);
    }

    private void loadIndexFile() throws IOException
    {
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
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
            String decoratedKey = input.readUTF();
            input.readLong();
            if (i++ % INDEX_INTERVAL == 0)
            {
                indexPositions.add(new KeyPosition(decoratedKey, indexPosition));
            }
        }
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    private long getIndexScanPosition(String decoratedKey, IPartitioner partitioner)
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
    public long getPosition(String decoratedKey) throws IOException
    {
        if (!bf.isPresent(decoratedKey))
            return -1;
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
                String indexDecoratedKey;
                try
                {
                    indexDecoratedKey = input.readUTF();
                }
                catch (EOFException e)
                {
                    return -1;
                }
                long position = input.readLong();
                int v = partitioner.getDecoratedKeyComparator().compare(indexDecoratedKey, decoratedKey);
                if (v == 0)
                {
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
    public long getNearestPosition(String decoratedKey) throws IOException
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
                String indexDecoratedKey;
                try
                {
                    indexDecoratedKey = input.readUTF();
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

    public void delete() throws IOException
    {
        FileUtils.deleteWithConfirm(new File(path));
        FileUtils.deleteWithConfirm(new File(indexFilename(path)));
        FileUtils.deleteWithConfirm(new File(filterFilename(path)));
        openedFiles.remove(path);
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
            SSTableReader.open(sstable.path, sstable.partitioner);
        }
    }

    public IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public FileStruct getFileStruct() throws IOException
    {
        return new FileStruct(this);
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
}
