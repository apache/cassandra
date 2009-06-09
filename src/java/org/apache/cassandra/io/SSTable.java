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
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.SequenceFile.ColumnGroupReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;

/**
 * This class is built on top of the SequenceFile. It stores
 * data on disk in sorted fashion. However the sorting is upto
 * the application. This class expects keys to be handed to it
 * in sorted order.
 *
 * A separate index file is maintained as well, containing the
 * SSTable keys and the offset into the SSTable at which they are found.
 * Every 1/indexInterval key is read into memory when the SSTable is opened.
 *
 * Finally, a bloom filter file is also kept for the keys in each SSTable.
 */

public class SSTable
{
    private static Logger logger_ = Logger.getLogger(SSTable.class);
    /* use this as a monitor to lock when loading index. */
    private static Object indexLoadLock_ = new Object();
    /* Every 128th key is an index. */
    private static final int indexInterval_ = 128;
    /* Required extension for temporary files created during compactions. */
    public static final String temporaryFile_ = "tmp";
    /*
     * This map has the SSTable as key and a BloomFilter as value. This
     * BloomFilter will tell us if a key/column pair is in the SSTable.
     * If not we can avoid scanning it.
     */
    private static Map<String, BloomFilter> bfs_ = new Hashtable<String, BloomFilter>();


    public static int indexInterval()
    {
        return indexInterval_;
    }

    /*
     * Maintains a list of KeyPosition objects per SSTable file loaded.
     * We do this so that we don't read the index file into memory multiple
     * times.
     *
     * The positions here refer to positions _in the index file_, not in
     * the sstable file directly.  So looking up a data location is a two
     * step process: use this map to find the location of the original index
     * entry, then read the data file at the location given by the index.
    */
    static IndexMap indexMetadataMap_ = new IndexMap();

    /**
     * This method deletes both the specified data file
     * and the associated index file
     *
     * @param dataFile - data file associated with the SSTable
     */
    public static void delete(String dataFile) throws IOException
    {
        /* remove the cached index table from memory */
        indexMetadataMap_.remove(dataFile);
        /* Delete the checksum file associated with this data file */
        ChecksumManager.onFileDelete(dataFile);

        deleteWithConfirm(new File(dataFile));
        deleteWithConfirm(new File(indexFilename(dataFile)));
        deleteWithConfirm(new File(filterFilename(dataFile)));
    }

    private static void deleteWithConfirm(File file) throws IOException
    {
        assert file.exists() : "attempted to delete non-existing file " + file.getName();
        if (!file.delete())
        {
            throw new IOException("Failed to delete " + file.getName());
        }
    }

    public static int getApproximateKeyCount(List<String> dataFiles)
    {
        int count = 0;

        for (String dataFile : dataFiles)
        {
            List<KeyPosition> index = indexMetadataMap_.get(dataFile);
            if (index != null)
            {
                int indexKeyCount = index.size();
                count = count + (indexKeyCount + 1) * indexInterval_;
                logger_.debug("index size for bloom filter calc for file  : " + dataFile + "   : " + count);
            }
        }

        return count;
    }

    /**
     * Get all indexed keys in the SSTable.
     */
    public static List<String> getIndexedKeys()
    {
        Set<String> indexFiles = indexMetadataMap_.keySet();
        List<KeyPosition> positions = new ArrayList<KeyPosition>();

        for (String indexFile : indexFiles)
        {
            positions.addAll(indexMetadataMap_.get(indexFile));
        }

        List<String> indexedKeys = new ArrayList<String>();
        for (KeyPosition kp : positions)
        {
            indexedKeys.add(kp.key);
        }

        Collections.sort(indexedKeys);
        return indexedKeys;
    }

    /*
     * Intialize the index files and also cache the Bloom Filters
     * associated with these files. Also caches the file handles 
     * associated with these files.
    */
    public static void onStart(List<String> filenames) throws IOException
    {
        for (String filename : filenames)
        {
            try
            {
                new SSTable(filename, StorageService.getPartitioner());
            }
            catch (IOException ex)
            {
                logger_.info("Deleting corrupted file " + filename);
                FileUtils.delete(filename);
                logger_.warn(LogUtil.throwableToString(ex));
            }
        }
    }

    /*
     * Stores the Bloom Filter associated with the given file.
    */
    public static void storeBloomFilter(String filename, BloomFilter bf)
    {
        bfs_.put(filename, bf);
    }

    /*
     * Removes the bloom filter associated with the specified file.
    */
    public static void removeAssociatedBloomFilter(String filename)
    {
        bfs_.remove(filename);
    }

    /*
     * Determines if the given key is in the specified file. If the
     * key is not present then we skip processing this file.
    */
    public static boolean isKeyInFile(String clientKey, String filename)
    {
        boolean bVal = false;
        BloomFilter bf = bfs_.get(filename);
        if (bf != null)
        {
            bVal = bf.isPresent(clientKey);
        }
        return bVal;
    }

    String dataFile_;
    private long keysWritten;
    private IFileWriter dataWriter_;
    private BufferedRandomAccessFile indexRAF_;
    private String lastWrittenKey_;
    private IPartitioner partitioner_;

    /**
     * This ctor basically gets passed in the full path name
     * of the data file associated with this SSTable. Use this
     * ctor to read the data in this file.
     */
    public SSTable(String dataFileName, IPartitioner partitioner) throws IOException
    {
        dataFile_ = dataFileName;
        partitioner_ = partitioner;
        /*
         * this is to prevent multiple threads from
         * loading the same index files multiple times
         * into memory.
        */
        synchronized (indexLoadLock_)
        {
            if (indexMetadataMap_.get(dataFile_) == null)
            {
                long start = System.currentTimeMillis();
                loadIndexFile();
                loadBloomFilter();
                logger_.debug("INDEX LOAD TIME: " + (System.currentTimeMillis() - start) + " ms.");
            }
        }
    }

    /**
     * This ctor is used for writing data into the SSTable. Use this
     * version for non DB writes to the SSTable.
     */
    public SSTable(String directory, String filename, IPartitioner partitioner) throws IOException
    {
        dataFile_ = directory + System.getProperty("file.separator") + filename + "-Data.db";
        partitioner_ = partitioner;
        dataWriter_ = SequenceFile.bufferedWriter(dataFile_, 4 * 1024 * 1024);
        indexRAF_ = new BufferedRandomAccessFile(indexFilename(), "rw", 1024 * 1024);
    }

    private void loadBloomFilter() throws IOException
    {
        assert bfs_.get(dataFile_) == null;
        DataInputStream stream = new DataInputStream(new FileInputStream(filterFilename()));
        bfs_.put(dataFile_, BloomFilter.serializer().deserialize(stream));
    }

    private void loadIndexFile() throws IOException
    {
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        ArrayList<KeyPosition> indexEntries = new ArrayList<KeyPosition>();

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
            if (i++ % indexInterval_ == 0)
            {
                indexEntries.add(new KeyPosition(decoratedKey, indexPosition, partitioner_));
            }
        }

        indexMetadataMap_.put(dataFile_, indexEntries);
    }

    private static String indexFilename(String dataFile)
    {
        String[] parts = dataFile.split("-");
        parts[parts.length - 1] = "Index.db";
        return StringUtils.join(parts, "-");
    }
    private String indexFilename()
    {
        return indexFilename(dataFile_);
    }

    private static String filterFilename(String dataFile)
    {
        String[] parts = dataFile.split("-");
        parts[parts.length - 1] = "Filter.db";
        return StringUtils.join(parts, "-");
    }
    private String filterFilename()
    {
        return filterFilename(dataFile_);
    }


    private String getFile(String name) throws IOException
    {
        File file = new File(name);
        if (file.exists())
        {
            return file.getAbsolutePath();
        }
        throw new IOException("File " + name + " was not found on disk.");
    }

    public String getDataFileLocation() throws IOException
    {
        return getFile(dataFile_);
    }

    private long beforeAppend(String decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        Comparator<String> c = partitioner_.getDecoratedKeyComparator();
        if (lastWrittenKey_ != null && c.compare(lastWrittenKey_, decoratedKey) > 0)
        {
            logger_.info("Last written key : " + lastWrittenKey_);
            logger_.info("Current key : " + decoratedKey);
            logger_.info("Writing into file " + dataFile_);
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey_ == null) ? 0 : dataWriter_.getCurrentPosition();
    }

    private void afterAppend(String decoratedKey, long position) throws IOException
    {
        lastWrittenKey_ = decoratedKey;
        long indexPosition = indexRAF_.getFilePointer();
        indexRAF_.writeUTF(decoratedKey);
        indexRAF_.writeLong(position);
        logger_.trace("wrote " + decoratedKey + " at " + position);

        if (keysWritten++ % indexInterval_ != 0)
            return;
        List<KeyPosition> indexEntries = SSTable.indexMetadataMap_.get(dataFile_);
        if (indexEntries == null)
        {
            indexEntries = new ArrayList<KeyPosition>();
            SSTable.indexMetadataMap_.put(dataFile_, indexEntries);
        }
        indexEntries.add(new KeyPosition(decoratedKey, indexPosition, partitioner_));
        logger_.trace("wrote index of " + decoratedKey + " at " + indexPosition);
    }

    // TODO make this take a DataOutputStream and wrap the byte[] version to combine them
    public void append(String decoratedKey, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataWriter_.append(decoratedKey, buffer);
        afterAppend(decoratedKey, currentPosition);
    }

    public void append(String decoratedKey, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataWriter_.append(decoratedKey, value);
        afterAppend(decoratedKey, currentPosition);
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    private static long getIndexScanPosition(String decoratedKey, IFileReader dataReader, IPartitioner partitioner)
    {
        List<KeyPosition> positions = indexMetadataMap_.get(dataReader.getFileName());
        assert positions != null && positions.size() > 0;
        int index = Collections.binarySearch(positions, new KeyPosition(decoratedKey, -1, partitioner));
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return -1;
            return positions.get(greaterThan - 1).position;
        }
        else
        {
            return positions.get(index).position;
        }
    }

    /**
     * returns the position in the data file to find the given key, or -1 if the key is not present
     */
    /* TODO having this static means we have to keep re-opening the index file, which sucks.  Need to move towards
       greater encapsulation. */
    public static long getPosition(String decoratedKey, IFileReader dataReader, IPartitioner partitioner) throws IOException
    {
        long start = getIndexScanPosition(decoratedKey, dataReader, partitioner);
        if (start < 0)
        {
            return -1;
        }
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(dataReader.getFileName()), "r");
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
                    return position;
                if (v > 0)
                    return -1;
            } while  (++i < indexInterval_);
        }
        finally
        {
            input.close();
        }
        return -1;
    }

    /** like getPosition, but if key is not found will return the location of the first key _greater_ than the desired one, or -1 if no such key exists. */
    public static long getNearestPosition(String decoratedKey, IFileReader dataReader, IPartitioner partitioner) throws IOException
    {
        long start = getIndexScanPosition(decoratedKey, dataReader, partitioner);
        if (start < 0)
        {
            return 0;
        }
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(dataReader.getFileName()), "r");
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

    public DataInputBuffer next(final String clientKey, String cfName, List<String> columnNames) throws IOException
    {
        return next(clientKey, cfName, columnNames, null);
    }

    public DataInputBuffer next(final String clientKey, String cfName, List<String> columnNames, IndexHelper.TimeRange timeRange) throws IOException
    {
        IFileReader dataReader = null;
        try
        {
            dataReader = SequenceFile.reader(dataFile_);
            String decoratedKey = partitioner_.decorateKey(clientKey);
            long position = getPosition(decoratedKey, dataReader, partitioner_);

            DataOutputBuffer bufOut = new DataOutputBuffer();
            DataInputBuffer bufIn = new DataInputBuffer();
            long bytesRead = dataReader.next(decoratedKey, bufOut, cfName, columnNames, timeRange, position);
            if (bytesRead != -1L)
            {
                if (bufOut.getLength() > 0)
                {
                    bufIn.reset(bufOut.getData(), bufOut.getLength());
                    /* read the key even though we do not use it */
                    bufIn.readUTF();
                    bufIn.readInt();
                }
            }
            return bufIn;
        }
        finally
        {
            if (dataReader != null)
            {
                dataReader.close();
            }
        }
    }

    public DataInputBuffer next(String clientKey, String columnFamilyColumn) throws IOException
    {
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamilyColumn);
        String columnFamilyName = values[0];
        List<String> cnNames = (values.length == 1) ? null : Arrays.asList(values[1]);
        return next(clientKey, columnFamilyName, cnNames);
    }

    public void close(BloomFilter bf) throws IOException
    {
        // bloom filter
        FileOutputStream fos = new FileOutputStream(filterFilename());
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();

        // index
        indexRAF_.getChannel().force(true);
        indexRAF_.close();

        // main data
        dataWriter_.close(); // calls force
    }
    
    private static String rename(String tmpFilename)
    {
        String filename = tmpFilename.replace("-" + temporaryFile_, "");
        new File(tmpFilename).renameTo(new File(filename));
        return filename;
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public void closeRename(BloomFilter bf) throws IOException
    {
    	close(bf);

        String oldDataFileName = dataFile_;
        rename(indexFilename());
        rename(filterFilename());
        dataFile_ = rename(dataFile_); // important to do this last since index & filter file names are derived from it

    	List<KeyPosition> positions = SSTable.indexMetadataMap_.remove(oldDataFileName);
    	SSTable.indexMetadataMap_.put(dataFile_, positions);
    }

    /**
     * obtain a BlockReader for the getColumnSlice call.
     */
    public ColumnGroupReader getColumnGroupReader(String key, String cfName, String startColumn, boolean isAscending) throws IOException
    {
        ColumnGroupReader reader = null;
        IFileReader dataReader = SequenceFile.reader(dataFile_);

        try
        {
            /* Morph key into actual key based on the partition type. */
            String decoratedKey = partitioner_.decorateKey(key);
            long position = getPosition(decoratedKey, dataReader, partitioner_);
            reader = new ColumnGroupReader(dataFile_, decoratedKey, cfName, startColumn, isAscending, position);
        }
        finally
        {
            if (dataReader != null)
                dataReader.close();
        }
        return reader;
    }

    /** obviously only for testing */
    public static void forceBloomFilterFailures(String filename)
    {
        SSTable.bfs_.put(filename, BloomFilter.alwaysMatchingBloomFilter());
    }
}


/*
 * This abstraction provides LRU symantics for the keys that are
 * "touched". Currently it holds the offset of the key in a data
 * file. May change to hold a reference to a IFileReader which
 * memory maps the key and its associated data on a touch.
*/
class TouchedKeyCache extends LinkedHashMap<String, Long>
{
    private final int capacity_;

    TouchedKeyCache(int capacity)
    {
        super(capacity + 1, 1.1f, true);
        capacity_ = capacity;
    }

    protected boolean removeEldestEntry(Map.Entry<String, Long> entry)
    {
        return (size() > capacity_);
    }
}

/**
 * This is a simple container for the index Key and its corresponding position
 * in the data file. Binary search is performed on a list of these objects
 * to lookup keys within the SSTable data file.
 *
 * All keys are decorated.
 */
class KeyPosition implements Comparable<KeyPosition>
{
    public final String key;
    public final long position;
    private final IPartitioner partitioner; // TODO rip out the static uses of KP so we can just use the parent SSTable's partitioner, when necessary

    public KeyPosition(String key, long position, IPartitioner partitioner)
    {
        this.key = key;
        this.position = position;
        this.partitioner = partitioner;
    }

    public int compareTo(KeyPosition kp)
    {
        return partitioner.getDecoratedKeyComparator().compare(key, kp.key);
    }

    public String toString()
    {
        return key + ":" + position;
    }
}

/**
 * wraps a Map to ensure that all filenames used as keys are cannonicalized.
 * (Note that cannonical paths are cached by the JDK so the performance hit is negligible.)
 */
class IndexMap
{
    private final Hashtable<String, List<KeyPosition>> hashtable = new Hashtable<String, List<KeyPosition>>();

    private String cannonicalize(String filename)
    {
        try
        {
            return new File(filename).getCanonicalPath();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public List<KeyPosition> get(String filename)
    {
        return hashtable.get(cannonicalize(filename));
    }

    public List<KeyPosition> put(String filename, List<KeyPosition> value)
    {
        return hashtable.put(cannonicalize(filename), value);
    }

    public void clear()
    {
        hashtable.clear();
    }

    public Set<String> keySet()
    {
        return hashtable.keySet();
    }

    public List<KeyPosition> remove(String filename)
    {
        return hashtable.remove(cannonicalize(filename));
    }
}
