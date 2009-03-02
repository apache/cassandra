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
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.PartitionerType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BasicUtilities;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;

/**
 * This class is built on top of the SequenceFile. It stores
 * data on disk in sorted fashion. However the sorting is upto
 * the application. This class expects keys to be handed to it
 * in sorted order. SSTable is broken up into blocks where each
 * block contains 128 keys. At the end of the file  the block 
 * index is written which contains the offsets to the keys in the
 * block. SSTable also maintains an index file to which every 128th 
 * key is written with a pointer to the block index which is the block 
 * that actually contains the key. This index file is then read and 
 * maintained in memory. SSTable is append only and immutable. SSTable
 * on disk looks as follows:
 * 
 *                 -------------------------
 *                 |------------------------|<-------|
 *                 |                        |        |  BLOCK-INDEX PTR
 *                 |                        |        |
 *                 |------------------------|--------
 *                 |------------------------|<-------|
 *                 |                        |        |
 *                 |                        |        |  BLOCK-INDEX PTR 
 *                 |                        |        |
 *                 |------------------------|---------
 *                 |------------------------|<--------|
 *                 |                        |         |
 *                 |                        |         |
 *                 |                        |         | BLOCK-INDEX PTR
 *                 |                        |         |
 *                 |------------------------|         |
 *                 |------------------------|----------
 *                 |------------------------|-----------------> BLOOM-FILTER
 * version-info <--|----------|-------------|-------> relative offset to last block index.
 *                 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SSTable
{
    private static Logger logger_ = Logger.getLogger(SSTable.class);
    /* use this as a monitor to lock when loading index. */
    private static Object indexLoadLock_ = new Object();
    /* Every 128th key is an index. */
    private static final int indexInterval_ = 128;
    /* Key associated with block index written to disk */
    public static final String blockIndexKey_ = "BLOCK-INDEX";
    /* Position in SSTable after the first Block Index */
    private static long positionAfterFirstBlockIndex_ = 0L;
    /* Required extension for temporary files created during compactions. */
    public static final String temporaryFile_ = "tmp";
    /* Use this long as a 64 bit entity to turn on some bits for various settings */
    private static final long version_ = 0L;
    /*
     * This map has the SSTable as key and a BloomFilter as value. This
     * BloomFilter will tell us if a key/column pair is in the SSTable.
     * If not we can avoid scanning it.
     */
    private static Map<String, BloomFilter> bfs_ = new Hashtable<String, BloomFilter>();
    /* Maintains a touched set of keys */
    private static LinkedHashMap<String, Long> touchCache_ = new TouchedKeyCache(DatabaseDescriptor.getTouchKeyCacheSize());        
    
    /**
     * This class holds the position of a key in a block
     * and the size of the data associated with this key. 
    */
    protected static class BlockMetadata
    {
        protected static final BlockMetadata NULL = new BlockMetadata(-1L, -1L);
        
        long position_;
        long size_;
        
        BlockMetadata(long position, long size)
        {
            position_ = position;
            size_ = size;
        }
    }
    
    /*
     * This abstraction provides LRU symantics for the keys that are 
     * "touched". Currently it holds the offset of the key in a data
     * file. May change to hold a reference to a IFileReader which
     * memory maps the key and its associated data on a touch.
    */
    private static class TouchedKeyCache extends LinkedHashMap<String, Long>
    {
        private final int capacity_;
        
        TouchedKeyCache(int capacity)
        {
            super(capacity + 1, 1.1f, true);
            capacity_ = capacity;
        }
        
        protected boolean removeEldestEntry(Map.Entry<String, Long> entry)
        {
            return ( size() > capacity_ );
        }
    }
    
    /**
     * This compares two strings and does it in reverse
     * order.
     * 
     * @author alakshman
     *
     */
    private static class OrderPreservingPartitionerComparator implements Comparator<String>
    {
        public int compare(String c1, String c2) 
        {
            return c2.compareTo(c1);
        } 
    }

    /**
     * This class compares two BigInteger's passes in
     * as strings and does so in reverse order.
     * @author alakshman
     *
     */
    private static class RandomPartitionerComparator implements Comparator<String>
    {
        public int compare(String c1, String c2) 
        {
            BigInteger b1 = new BigInteger(c1);
            BigInteger b2 = new BigInteger(c2);
            return b2.compareTo(b1);
        } 
    }
    
    /**
     * This is a simple container for the index Key and its corresponding position
     * in the data file. Binary search is performed on a list of these objects
     * to lookup keys within the SSTable data file.
    */
    public static class KeyPositionInfo implements Comparable<KeyPositionInfo>
    {
        private String key_;
        private long position_;

        public KeyPositionInfo(String key)
        {
            key_ = key;            
        }

        public KeyPositionInfo(String key, long position)
        {
            this(key);
            position_ = position;
        }

        public String key()
        {
            return key_;
        }

        public long position()
        {
            return position_;
        }

        public int compareTo(KeyPositionInfo kPosInfo)
        {
            int value = 0;
            PartitionerType pType = StorageService.getPartitionerType();
            switch( pType )
            {
                case OPHF:
                    value = key_.compareTo(kPosInfo.key_);                    
                    break;
                    
                default:
                    BigInteger b = new BigInteger(key_);
                    value = b.compareTo( new BigInteger(kPosInfo.key_) );
                    break;
            }
            return value;
        }

        public String toString()
        {
        	return key_ + ":" + position_;
        }
    }
    
    public static int indexInterval()
    {
    	return indexInterval_;
    }
    
    /*
     * Maintains a list of KeyPositionInfo objects per SSTable file loaded.
     * We do this so that we don't read the index file into memory multiple
     * times.
    */
    private static Map<String, List<KeyPositionInfo>> indexMetadataMap_ = new Hashtable<String, List<KeyPositionInfo>>();
    
    /** 
     * This method deletes both the specified data file
     * and the associated index file
     *
     * @param dataFile - data file associated with the SSTable
    */
    public static void delete(String dataFile)
    {        
        /* remove the cached index table from memory */
        indexMetadataMap_.remove(dataFile);
        
        File file = new File(dataFile);
        if ( file.exists() )
        {
            /* delete the data file */
			if (file.delete())
			{			    
			    logger_.info("** Deleted " + file.getName() + " **");
			}
			else
			{			  
			    logger_.error("Failed to delete " + file.getName());
			}
        }
    }

    public static int getApproximateKeyCount( List<String> dataFiles)
    {
    	int count = 0 ;

    	for(String dataFile : dataFiles )
    	{    		
    		List<KeyPositionInfo> index = indexMetadataMap_.get(dataFile);
    		if (index != null )
    		{
    			int indexKeyCount = index.size();
    			count = count + (indexKeyCount+1) * indexInterval_ ;
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
        List<KeyPositionInfo> keyPositionInfos = new ArrayList<KeyPositionInfo>();

        for ( String indexFile : indexFiles )
        {
            keyPositionInfos.addAll( indexMetadataMap_.get(indexFile) );
        }

        List<String> indexedKeys = new ArrayList<String>();
        for ( KeyPositionInfo keyPositionInfo : keyPositionInfos )
        {
            indexedKeys.add(keyPositionInfo.key_);
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
        for ( String filename : filenames )
        {
            SSTable ssTable = null;
            try
            {
                ssTable = new SSTable(filename);                                
            }
            catch ( IOException ex )
            {
                logger_.info("Deleting corrupted file " + filename);
                FileUtils.delete(filename);
                logger_.warn(LogUtil.throwableToString(ex));
            }
            finally
            {
                if ( ssTable != null )
                {
                    ssTable.close();
                }
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
    public static boolean isKeyInFile(String key, String filename)
    {
        boolean bVal = false;
        BloomFilter bf = bfs_.get(filename);
        if ( bf != null )
        {
            bVal = bf.isPresent(key);
        }
        return bVal;
    }

    private String dataFile_;    
    private IFileWriter dataWriter_;
    private String lastWrittenKey_;    
    private long firstBlockPosition_ = 0L;    
    private int indexKeysWritten_ = 0;
    /* Holds the keys and their respective positions of the current block index */
    private SortedMap<String, BlockMetadata> blockIndex_;    
    /* Holds all the block indicies for this SSTable */
    private List<SortedMap<String, BlockMetadata>> blockIndexes_;
    
    /**
     * This ctor basically gets passed in the full path name
     * of the data file associated with this SSTable. Use this
     * ctor to read the data in this file.
    */
    public SSTable(String dataFileName) throws IOException
    {        
        dataFile_ = dataFileName;
        init();
    }

    /**
     * This ctor is used for writing data into the SSTable. Use this
     * version for non DB writes to the SSTable.
    */
    public SSTable(String directory, String filename) throws IOException
    {  
        dataFile_ = directory + System.getProperty("file.separator") + filename + "-Data.db";                
        blockIndex_ = new TreeMap<String, BlockMetadata>(Collections.reverseOrder());
        blockIndexes_ = new ArrayList<SortedMap<String, BlockMetadata>>();        
        // dataWriter_ = SequenceFile.writer(dataFile_);
        dataWriter_ = SequenceFile.bufferedWriter(dataFile_, 4*1024*1024);
        SSTable.positionAfterFirstBlockIndex_ = dataWriter_.getCurrentPosition(); 
    } 
    
    private void initBlockIndex()
    {
        initBlockIndex(StorageService.getPartitionerType());
    }
    
    private void initBlockIndex(PartitionerType pType)
    {
        switch ( pType )
        {
            case OPHF: 
                blockIndex_ = new TreeMap<String, BlockMetadata>( new SSTable.OrderPreservingPartitionerComparator() );                
               break;
               
            default:
                blockIndex_ = new TreeMap<String, BlockMetadata>( new SSTable.RandomPartitionerComparator() );
                break;
        }
    }
    
    /**
     * This ctor is used for DB writes into the SSTable. Use this
     * version to write to the SSTable.
    */
    public SSTable(String directory, String filename, PartitionerType pType) throws IOException
    {        
        dataFile_ = directory + System.getProperty("file.separator") + filename + "-Data.db";  
        dataWriter_ = SequenceFile.bufferedWriter(dataFile_, 4*1024*1024);        
        SSTable.positionAfterFirstBlockIndex_ = dataWriter_.getCurrentPosition(); 
        /* set up the block index based on partition type */
        initBlockIndex(pType);
        blockIndexes_ = new ArrayList<SortedMap<String, BlockMetadata>>();
    }
    
    private void loadBloomFilter(IFileReader indexReader, long size) throws IOException
    {        
        /* read the position of the bloom filter */
        indexReader.seek(size - 8);
        byte[] bytes = new byte[8];
        long currentPosition = indexReader.getCurrentPosition();
        indexReader.readDirect(bytes);
        long position = BasicUtilities.byteArrayToLong(bytes);
        /* seek to the position of the bloom filter */
        indexReader.seek(currentPosition - position);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        /* read the bloom filter from disk */
        indexReader.next(bufOut);   
        bufOut.close();
        bufIn.reset(bufOut.getData(), bufOut.getLength());
        String key = bufIn.readUTF();
        if ( key.equals(SequenceFile.marker_) )
        {
            /*
             * We are now reading the serialized Bloom Filter. We read
             * the length and then pass the bufIn to the serializer of
             * the BloomFilter. We then store the Bloom filter in the
             * map. However if the Bloom Filter already exists then we
             * need not read the rest of the file.
            */
            bufIn.readInt();
            if ( bfs_.get(dataFile_) == null )
                bfs_.put(dataFile_, BloomFilter.serializer().deserialize(bufIn));
        }
    }
    
    private void loadIndexFile() throws IOException
    {    
        IFileReader indexReader = null;
        /* Read all block indexes to maintain an index in memory */
        try
        {            
            indexReader = SequenceFile.bufferedReader(dataFile_, 4*1024*1024);
            long size = indexReader.getEOF();
            
            /* load the bloom filter into memory */
            loadBloomFilter(indexReader, size);
            /* read the position of the last block index */
            byte[] bytes = new byte[8];
            /* seek to the position to read the relative position of the first block index */
            indexReader.seek(size - 16L);
            /* the beginning of the first block index */
            long currentPosition = indexReader.getCurrentPosition();
            indexReader.readDirect(bytes);
            long firstBlockIndexPosition = BasicUtilities.byteArrayToLong(bytes);  
            List<KeyPositionInfo> keyPositionInfos = new ArrayList<KeyPositionInfo>();
            indexMetadataMap_.put(dataFile_, keyPositionInfos);
            DataOutputBuffer bufOut = new DataOutputBuffer();
            DataInputBuffer bufIn = new DataInputBuffer();        
            
            long nextPosition = currentPosition - firstBlockIndexPosition;
            indexReader.seek(nextPosition);
            /* read the block indexes from the end of the file till we hit the first one. */
            while ( nextPosition > 0 )
            {
                bufOut.reset();
                /* position @ the current block index being processed */
                currentPosition = indexReader.getCurrentPosition();
                long bytesRead = indexReader.next(bufOut);
                if ( bytesRead != -1 )
                {
                    bufIn.reset(bufOut.getData(), bufOut.getLength());
                    /* read the block key. */
                    String blockIndexKey = bufIn.readUTF();
                    if ( !blockIndexKey.equals(SSTable.blockIndexKey_) )
                    {
                    	logger_.debug(" Done reading the block indexes, Index has been created");
                    	break;
                    }
                    /* read the size of the block index */
                    bufIn.readInt();                    
                    /* Number of keys in the block. */
                    int keys = bufIn.readInt();
                    String largestKeyInBlock = null;
                    for ( int i = 0; i < keys; ++i )
                    {
                        String keyInBlock = bufIn.readUTF();
                        if ( i == 0 )
                        {
                            largestKeyInBlock = keyInBlock;
                            /* relative offset in the block for the key*/
                            bufIn.readLong();
                            /* size of data associated with the key */
                            bufIn.readLong();
                            /* load the actual position of the block index into the index map */
                            keyPositionInfos.add( new KeyPositionInfo(largestKeyInBlock, currentPosition) );
                        }
                        else
                        {
                            /*
                             * This is not the key we are looking for. So read its position
                             * and the size of the data associated with it. This was stored
                             * as the BlockMetadata.
                            */
                            bufIn.readLong();
                            bufIn.readLong();
                        }
                    }
                }
            }
            bufIn.close();
            bufOut.close();
            Collections.sort(keyPositionInfos);
        }        
        finally
        {
            if ( indexReader != null )
            {
                indexReader.close();
            }
        }        
    }

    private void init() throws IOException
    {        
        /*
         * this is to prevent multiple threads from
         * loading the same index files multiple times
         * into memory.
        */
        synchronized( indexLoadLock_ )
        {
            if ( indexMetadataMap_.get(dataFile_) == null )
            {
                long start = System.currentTimeMillis();
                loadIndexFile();
                logger_.debug("INDEX LOAD TIME: " + (System.currentTimeMillis() - start) + " ms.");                
            }
        }
    }

    private String getFile(String name) throws IOException
    {
        File file = new File(name);
        if ( file.exists() )
            return file.getAbsolutePath();
        throw new IOException("File " + name + " was not found on disk.");
    }

    public String getDataFileLocation() throws IOException
    {
        return getFile(dataFile_);
    }

    public long lastModified()
    {
        return dataWriter_.lastModified();
    }
    
    /*
     * Seeks to the specified key on disk.
    */
    public void touch(String key, boolean fData) throws IOException
    {
        if ( touchCache_.containsKey(key) )
            return;
        
        IFileReader dataReader = SequenceFile.reader(dataFile_); 
        try
        {
        	/* Morph the key */
        	key = morphKey(key);
            Coordinate fileCoordinate = getCoordinates(key, dataReader);
            /* Get offset of key from block Index */
            dataReader.seek(fileCoordinate.end_);
            BlockMetadata blockMetadata = dataReader.getBlockMetadata(key);
            if ( blockMetadata.position_ != -1L )
            {
                touchCache_.put(dataFile_ + ":" + key, blockMetadata.position_);                  
            } 
            
            if ( fData )
            {
                /* Read the data associated with this key and pull it into the Buffer Cache */
                if ( blockMetadata.position_ != -1L )
                {
                    dataReader.seek(blockMetadata.position_);
                    DataOutputBuffer bufOut = new DataOutputBuffer();
                    dataReader.next(bufOut);
                    bufOut.reset();
                    logger_.debug("Finished the touch of the key to pull it into buffer cache.");
                }
            }
        }
        finally
        {
            if ( dataReader != null )
                dataReader.close();
        }
    }

    private long beforeAppend(String key) throws IOException
    {
    	if(key == null )
            throw new IOException("Keys must not be null.");
        if ( lastWrittenKey_ != null && key.compareTo(lastWrittenKey_) <= 0 )
        {
            logger_.info("Last written key : " + lastWrittenKey_);
            logger_.info("Current key : " + key);
            logger_.info("Writing into file " + dataFile_);
            throw new IOException("Keys must be written in ascending order.");
        }
        long currentPosition = (lastWrittenKey_ == null) ? SSTable.positionAfterFirstBlockIndex_ : dataWriter_.getCurrentPosition();
        return currentPosition;
    }
    
    private long beforeAppend(BigInteger hash) throws IOException
    {
        if(hash == null )
            throw new IOException("Keys must not be null.");
        if ( lastWrittenKey_ != null )
        {
            BigInteger previousKey = new BigInteger(lastWrittenKey_);
            if ( hash.compareTo(previousKey) <= 0 )
            {
                logger_.info("Last written key : " + previousKey);
                logger_.info("Current key : " + hash);
                logger_.info("Writing into file " + dataFile_);
                throw new IOException("Keys must be written in ascending order.");
            }
        }
        long currentPosition = (lastWrittenKey_ == null) ? SSTable.positionAfterFirstBlockIndex_ : dataWriter_.getCurrentPosition();
        return currentPosition;
    }

    private void afterAppend(String key, long position, long size) throws IOException
    {
        ++indexKeysWritten_;
        lastWrittenKey_ = key;
        blockIndex_.put(key, new BlockMetadata(position, size));
        if ( indexKeysWritten_ == indexInterval_ )
        {
        	blockIndexes_.add(blockIndex_);
        	blockIndex_ = new TreeMap<String, BlockMetadata>(Collections.reverseOrder());
            indexKeysWritten_ = 0;
        }                
    }
    
    private void afterAppend(BigInteger hash, long position, long size) throws IOException
    {
        ++indexKeysWritten_;
        String key = hash.toString();
        lastWrittenKey_ = key;
        blockIndex_.put(key, new BlockMetadata(position, size));
        if ( indexKeysWritten_ == indexInterval_ )
        {
            blockIndexes_.add(blockIndex_);
            initBlockIndex();
            indexKeysWritten_ = 0;
        }                
    }
    
    /**
     * Dumps all the block indicies for this SSTable
     * at the end of the file.
     * @throws IOException
     */
    private void dumpBlockIndexes() throws IOException
    {    	
        long position = dataWriter_.getCurrentPosition();
        firstBlockPosition_ = position;
    	for( SortedMap<String, BlockMetadata> block : blockIndexes_ )
    	{
    		dumpBlockIndex( block );
    	}  	
    }    
    
    private void dumpBlockIndex( SortedMap<String, BlockMetadata> blockIndex) throws IOException
    {
        /* Block Index is empty so bail. */
        if ( blockIndex.size() == 0 )
            return;
        
        DataOutputBuffer bufOut = new DataOutputBuffer();
        /* 
         * Record the position where we start writing the block index. This is will be
         * used as the position of the lastWrittenKey in the block in the index file
        */
        long position = dataWriter_.getCurrentPosition();
        Set<String> keys = blockIndex.keySet();                
        /* Number of keys in this block */
        bufOut.writeInt(keys.size());
        for ( String key : keys )
        {            
            bufOut.writeUTF(key);
            BlockMetadata blockMetadata = blockIndex.get(key);
            /* position of the key as a relative offset */
            bufOut.writeLong(position - blockMetadata.position_);
            bufOut.writeLong(blockMetadata.size_);
        }
        /* Write out the block index. */
        dataWriter_.append(SSTable.blockIndexKey_, bufOut);
        /* Load this index into the in memory index map */
        List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.get(dataFile_);
        if ( keyPositionInfos == null )
        {
        	keyPositionInfos = new ArrayList<KeyPositionInfo>();
        	SSTable.indexMetadataMap_.put(dataFile_, keyPositionInfos);
        }
        
        keyPositionInfos.add(new KeyPositionInfo(blockIndex.firstKey(), position));
        /*
        try
        {
            keyPositionInfos.add(new KeyPositionInfo(blockIndex.firstKey(), position));
        }
        catch(Exception ex)
        {
            Set<String> keysInBlock = blockIndex.keySet();
            for( String keyInBlock : keysInBlock )
            {
                logger_.warn("BLOCK KEY: " + keyInBlock);
            }
            logger_.warn(LogUtil.throwableToString(ex));
        }
        */
        blockIndex.clear();        
    }

    public void append(String key, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(key);
        dataWriter_.append(key, buffer);
        afterAppend(key, currentPosition, buffer.getLength());
    }
    
    public void append(String key, BigInteger hash, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(hash);
        /* Use as key - hash + ":" + key */
        dataWriter_.append(hash + ":" + key, buffer);
        afterAppend(hash, currentPosition, buffer.getLength());
    }

    public void append(String key, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(key);
        dataWriter_.append(key, value);
        afterAppend(key, currentPosition, value.length );
    }
    
    public void append(String key, BigInteger hash, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(hash);
        /* Use as key - hash + ":" + key */
        dataWriter_.append(hash + ":" + key, value);
        afterAppend(hash, currentPosition, value.length);
    }

    private Coordinate getCoordinates(String key, IFileReader dataReader) throws IOException
    {
    	List<KeyPositionInfo> indexInfo = indexMetadataMap_.get(dataFile_);
    	int size = (indexInfo == null) ? 0 : indexInfo.size();
    	long start = 0L;
    	long end = dataReader.getEOF();
        if ( size > 0 )
        {
            int index = Collections.binarySearch(indexInfo, new KeyPositionInfo(key));
            if ( index < 0 )
            {
                /*
                 * We are here which means that the requested
                 * key is not an index.
                */
                index = (++index)*(-1);
                /*
                 * This means key is not present at all. Hence
                 * a scan is in order.
                */
                start = (index == 0) ? 0 : indexInfo.get(index - 1).position();
                if ( index < size )
                {
                    end = indexInfo.get(index).position();
                }
                else
                {
                    /* This is the Block Index in the file. */
                    end = start;
                }
            }
            else
            {
                /*
                 * If we are here that means the key is in the index file
                 * and we can retrieve it w/o a scan. In reality we would
                 * like to have a retreive(key, fromPosition) but for now
                 * we use scan(start, start + 1) - a hack.
                */
                start = indexInfo.get(index).position();                
                end = start;
            }
        }
        else
        {
            /*
             * We are here which means there are less than
             * 128 keys in the system and hence our only recourse
             * is a linear scan from start to finish. Automatically
             * use memory mapping since we have a huge file and very
             * few keys.
            */
            end = dataReader.getEOF();
        }  
        
        return new Coordinate(start, end);
    }
    
    /**
     * Convert the application key into the appropriate application
     * key based on the partition type.
     * 
     * @param key the application key
     * @return the appropriate key based on partition mechanism
    */
    private String morphKey(String key)
    {
        String internalKey = key;
        PartitionerType pType = StorageService.getPartitionerType();
        switch ( pType )
        {
            case OPHF:
                break;
                
            default:
                internalKey = FBUtilities.hash(key).toString();
                break;
        }
        return internalKey;
    }
    
    public DataInputBuffer next(String key, String cf, List<String> cNames) throws IOException
    {
    	DataInputBuffer bufIn = null;        
        IFileReader dataReader = null;
        try
        {
            dataReader = SequenceFile.reader(dataFile_);
            /* Morph key into actual key based on the partition type. */ 
            key = morphKey(key);
            Coordinate fileCoordinate = getCoordinates(key, dataReader);    
            /*
             * we have the position we have to read from in order to get the
             * column family, get the column family and column(s) needed.
            */          
            bufIn = getData(dataReader, key, cf, cNames, fileCoordinate);
        }
        finally
        {
            if ( dataReader != null )
            {
                dataReader.close();
            }
        }
        return bufIn;
    }
    
    public DataInputBuffer next(String key, String columnName) throws IOException
    {
        DataInputBuffer bufIn = null;
        IFileReader dataReader = null;
        try
        {
            dataReader = SequenceFile.reader(dataFile_);
            /* Morph key into actual key based on the partition type. */ 
            key = morphKey(key);
            Coordinate fileCoordinate = getCoordinates(key, dataReader);
            /*
             * we have the position we have to read from in order to get the
             * column family, get the column family and column(s) needed.
            */            
            bufIn = getData(dataReader, key, columnName, fileCoordinate);
        }
        finally
        {
            if ( dataReader != null )
            {
                dataReader.close();
            }
        }
        return bufIn;
    }
    
    public DataInputBuffer next(String key, String columnName, IndexHelper.TimeRange timeRange) throws IOException
    {
        DataInputBuffer bufIn = null;
        IFileReader dataReader = null;
        try
        {
            dataReader = SequenceFile.reader(dataFile_);
            /* Morph key into actual key based on the partition type. */ 
            key = morphKey(key);
            Coordinate fileCoordinate = getCoordinates(key, dataReader);
            /*
             * we have the position we have to read from in order to get the
             * column family, get the column family and column(s) needed.
            */  
            bufIn = getData(dataReader, key, columnName, timeRange, fileCoordinate);
        }
        finally
        {
            if ( dataReader != null )
            {
                dataReader.close();
            }
        }
        return bufIn;
    }
    
    long getSeekPosition(String key, long start)
    {
        Long seekStart = touchCache_.get(dataFile_ + ":" + key);
        if( seekStart != null)
        {
            return seekStart;
        }
        return start;
    }
        
    /*
     * Get the data for the key from the position passed in. 
    */
    private DataInputBuffer getData(IFileReader dataReader, String key, String column, Coordinate section) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        
        long bytesRead = dataReader.next(key, bufOut, column, section);
        if ( bytesRead != -1L )
        {
            if ( bufOut.getLength() > 0 )
            {                              
                bufIn.reset(bufOut.getData(), bufOut.getLength());            
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();            
            }
        }
        
        return bufIn;
    }
    
    private DataInputBuffer getData(IFileReader dataReader, String key, String cf, List<String> columns, Coordinate section) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
                  
        long bytesRead = dataReader.next(key, bufOut, cf, columns, section);
        if ( bytesRead != -1L )
        {
            if ( bufOut.getLength() > 0 )
            {                     
                bufIn.reset(bufOut.getData(), bufOut.getLength());             
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();            
            }        
        }
        return bufIn;
    }
    
    /*
     * Get the data for the key from the position passed in. 
    */
    private DataInputBuffer getData(IFileReader dataReader, String key, String column, IndexHelper.TimeRange timeRange, Coordinate section) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
                
        try
        {
            dataReader.next(key, bufOut, column, timeRange, section);
            if ( bufOut.getLength() > 0 )
            {                              
                bufIn.reset(bufOut.getData(), bufOut.getLength());            
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();            
            }
        }
        catch ( IOException ex )
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        return bufIn;
    }
    
    /*
     * Given a key we are interested in this method gets the
     * closest index before the key on disk.
     *
     *  param @ key - key we are interested in.
     *  return position of the closest index before the key
     *  on disk or -1 if this key is not on disk.
    */
    private long getClosestIndexPositionToKeyOnDisk(String key)
    {
        long position = -1L;
        List<KeyPositionInfo> indexInfo = indexMetadataMap_.get(dataFile_);
        int size = indexInfo.size();
        int index = Collections.binarySearch(indexInfo, new KeyPositionInfo(key));
        if ( index < 0 )
        {
            /*
             * We are here which means that the requested
             * key is not an index.
            */
            index = (++index)*(-1);
            /* this means key is not present at all */
            if ( index >= size )
                return position;
            /* a scan is in order. */
            position = (index == 0) ? 0 : indexInfo.get(index - 1).position();
        }
        else
        {
            /*
             * If we are here that means the key is in the index file
             * and we can retrieve it w/o a scan. In reality we would
             * like to have a retreive(key, fromPosition) but for now
             * we use scan(start, start + 1) - a hack.
            */
            position = indexInfo.get(index).position();
        }
        return position;
    }

    public void close() throws IOException
    {
        close( new byte[0], 0 );
    }

    public void close(BloomFilter bf) throws IOException
    {
        /* Any remnants in the blockIndex should be added to the dump */
    	blockIndexes_.add(blockIndex_);
    	dumpBlockIndexes();
        
    	/* reset the buffer and serialize the Bloom Filter. */
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        bufOut.close();

        close(bufOut.getData(), bufOut.getLength());
        // byte[] bytes = new byte[bufOut.getLength()];        
        // System.arraycopy(bufOut.getData(), 0, bytes, 0, bufOut.getLength());
        // close(bytes, bytes.length);             
    }

    /**
     * Renames a temporary SSTable file to a valid data and index file
     */
    public void closeRename(BloomFilter bf) throws IOException
    {
    	close(bf);
        String tmpDataFile = dataFile_;
    	String dataFileName = dataFile_.replace("-" + temporaryFile_,"");    	
    	File dataFile = new File(dataFile_);
    	dataFile.renameTo(new File(dataFileName));    	    	
    	dataFile_ = dataFileName;        
    	/* Now repair the in memory index associated with the old name */
    	List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.remove(tmpDataFile);    	    	  	    	
    	SSTable.indexMetadataMap_.put(dataFile_, keyPositionInfos);
    }
    
    public void closeRename(BloomFilter bf, List<String> files) throws IOException
    {
        close( bf);
        String tmpDataFile = dataFile_;
        String dataFileName = dataFile_.replace("-" + temporaryFile_,"");
        File dataFile = new File(dataFile_);
        dataFile.renameTo(new File(dataFileName));
        dataFile_ = dataFileName;
        /* Now repair the in memory index associated with the old name */
        List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.remove(tmpDataFile);                         
        SSTable.indexMetadataMap_.put(dataFile_, keyPositionInfos);
        if ( files != null )
        {            
            files.add(dataFile_);
        }
    }
    
    private void close(byte[] footer, int size) throws IOException
    {
        /*
         * Write the bloom filter for this SSTable.
         * Then write three longs one which is a version
         * and one which is a pointer to the last written
         * block index and the last one is the position of
         * the Bloom Filter.
         */
        if ( dataWriter_ != null )
        {            
            long bloomFilterPosition = dataWriter_.getCurrentPosition();
            dataWriter_.close(footer, size);
            /* write the version field into the SSTable */           
            dataWriter_.writeDirect(BasicUtilities.longToByteArray(version_));
            /* write the relative position of the first block index from current position */
            long blockPosition = dataWriter_.getCurrentPosition() - firstBlockPosition_;
            dataWriter_.writeDirect(BasicUtilities.longToByteArray(blockPosition));
            
            /* write the position of the bloom filter */
            long bloomFilterRelativePosition = dataWriter_.getCurrentPosition() - bloomFilterPosition;
            dataWriter_.writeDirect(BasicUtilities.longToByteArray(bloomFilterRelativePosition));            
            dataWriter_.close();
        }
    } 
}
