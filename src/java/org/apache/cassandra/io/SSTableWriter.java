package org.apache.cassandra.io;
/*
 * 
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
 * 
 */


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;
import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    private long keysWritten;
    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;
    private DecoratedKey lastWrittenKey;
    private BloomFilter bf;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));
        bf = BloomFilter.getFilter(keyCount, 15);
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + path);
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition, int dataSize) throws IOException
    {
        String diskKey = partitioner.convertToDiskFormat(decoratedKey);
        bf.add(diskKey);
        lastWrittenKey = decoratedKey;
        long indexPosition = indexFile.getFilePointer();
        indexFile.writeUTF(diskKey);
        indexFile.writeLong(dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + decoratedKey + " at " + indexPosition);

        boolean spannedEntry = SSTableReader.bufferIndex(indexPosition) != SSTableReader.bufferIndex(indexFile.getFilePointer());
        if (keysWritten++ % INDEX_INTERVAL == 0 || spannedEntry)
        {
            if (indexPositions == null)
            {
                indexPositions = new ArrayList<KeyPosition>();
            }
            KeyPosition info = new KeyPosition(decoratedKey, indexPosition);
            indexPositions.add(info);

            if (spannedEntry)
            {
                if (spannedIndexDataPositions == null)
                {
                    spannedIndexDataPositions = new HashMap<KeyPosition, PositionSize>();
                }
                spannedIndexDataPositions.put(info, new PositionSize(dataPosition, dataSize));
            }
        }
    }

    // TODO make this take a DataOutputStream and wrap the byte[] version to combine them
    public void append(DecoratedKey decoratedKey, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        int length = buffer.getLength();
        assert length > 0;
        dataFile.writeInt(length);
        dataFile.write(buffer.getData(), 0, length);
        afterAppend(decoratedKey, currentPosition, length);
    }

    public void append(DecoratedKey decoratedKey, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        assert value.length > 0;
        dataFile.writeInt(value.length);
        dataFile.write(value);
        afterAppend(decoratedKey, currentPosition, value.length);
    }

    public SSTableReader closeAndOpenReader(double cacheFraction) throws IOException
    {
        return closeAndOpenReader(cacheFraction, false);
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader(double cacheFraction, boolean temporary) throws IOException
    {
        // bloom filter
        FileOutputStream fos = new FileOutputStream(filterFilename());
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();

        // index
        indexFile.getChannel().force(true);
        indexFile.close();

        // main data
        dataFile.close(); // calls force

        if (!temporary)
        {
            rename(indexFilename());
            rename(filterFilename());
            path = rename(path); // important to do this last since index & filter file names are derived from it
        }

        InstrumentedCache<DecoratedKey, PositionSize> keyCache = SSTableReader.createKeyCache((int)(cacheFraction * keysWritten));
        return new SSTableReader(path, partitioner, indexPositions, spannedIndexDataPositions, bf, keyCache);
    }

    static String rename(String tmpFilename)
    {
        String filename = tmpFilename.replace("-" + SSTable.TEMPFILE_MARKER, "");
        try
        {
            FBUtilities.renameWithConfirm(tmpFilename, filename);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return filename;
    }

    public static SSTableReader renameAndOpen(String dataFileName) throws IOException
    {
        SSTableWriter.rename(indexFilename(dataFileName));
        SSTableWriter.rename(filterFilename(dataFileName));
        dataFileName = SSTableWriter.rename(dataFileName);
        return SSTableReader.open(dataFileName,
                                  StorageService.getPartitioner(),
                                  DatabaseDescriptor.getKeysCachedFraction(parseTableName(dataFileName), parseColumnFamilyName(dataFileName)));
    }

}
