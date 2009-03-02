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

package org.apache.cassandra.tools;

import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.utils.BasicUtilities;
import org.apache.cassandra.utils.BloomFilter;

import org.apache.cassandra.io.*;
import org.apache.cassandra.utils.*;

public class IndexBuilder
{
    private static final int bufferSize_ = 64*1024;

    public static void main(String[] args)
    {
        if ( args.length != 1 )
        {
            System.out.println("Usage : java com.facebook.infrastructure.tools.IndexBuilder <full path to the data file>");
            System.exit(1);
        }

        try
        {
	        int blockCount = getBlockCount(args[0]);
	        System.out.println("Number of keys in the data file : " + (blockCount + 1)*SSTable.indexInterval());
	        buildIndex(args[0], blockCount);
        }
        catch(Throwable t)
        {
        	System.err.println("Exception: " + t.getMessage());
        	t.printStackTrace(System.err);
        }
    }

    private static int getBlockCount(String dataFile) throws IOException
    {
        IFileReader dataReader = SequenceFile.bufferedReader(dataFile, bufferSize_);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        int blockCount = 0;

        try
        {
            while ( !dataReader.isEOF() )
            {
                bufOut.reset();
                dataReader.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* Key just read */
                String key = bufIn.readUTF();
                if ( key.equals(SSTable.blockIndexKey_) )
                {
                    ++blockCount;
                }
            }
        }
        finally
        {
            dataReader.close();
        }
        return blockCount;
    }

    private static void buildIndex(String dataFile, int blockCount) throws IOException
    {
        String indexFile = dataFile.replace("-Data.", "-Index.");
        final int bufferSize = 64*1024;

        IFileWriter indexWriter = SequenceFile.bufferedWriter(indexFile, bufferSize);
        IFileReader dataReader = SequenceFile.bufferedReader(dataFile, bufferSize);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        /* BloomFilter of all data in the data file */
        BloomFilter bf = new BloomFilter((SSTable.indexInterval() + 1)*blockCount, 8);

        try
        {
            while ( !dataReader.isEOF() )
            {
                bufOut.reset();
                /* Record the position of the key. */
                long blockIndexOffset = dataReader.getCurrentPosition();
                dataReader.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* Key just read */
                String key = bufIn.readUTF();
                if ( key.equals(SSTable.blockIndexKey_) )
                {
                    /* Ignore the size of the data associated with the block index */
                    bufIn.readInt();
                    /* Number of keys in the block. */
                    int blockSize = bufIn.readInt();
                    /* Largest key in the block */
                    String largestKey = null;

                    /*
                     * Read the keys in this block and find the largest key in
                     * this block. This is the key that gets written into the
                     * index file.
                    */
                    for ( int i = 0; i < blockSize; ++i )
                    {
                        String currentKey = bufIn.readUTF();
                        bf.fill(currentKey);
                        if ( largestKey == null )
                        {
                            largestKey = currentKey;
                        }
                        else
                        {
                            if ( currentKey.compareTo(largestKey) > 0 )
                            {
                                /* record this key */
                                largestKey = currentKey;
                            }
                        }
                        /* read the position of the key and the size of key data and throws it away. */
                        bufIn.readLong();
                        bufIn.readLong();
                    }

                    /*
                     * Write into the index file the largest key in the block
                     * and the offset of the block index in the data file.
                    */
                    indexWriter.append(largestKey, BasicUtilities.longToByteArray(blockIndexOffset));
                }
            }
        }
        finally
        {
            dataReader.close();
            /* Cache the bloom filter */
            SSTable.storeBloomFilter(dataFile, bf);
            /* Write the bloom filter into the index file */
            bufOut.reset();
            BloomFilter.serializer().serialize(bf, bufOut);
            byte[] bytes = new byte[bufOut.getLength()];
            System.arraycopy(bufOut.getData(), 0, bytes, 0, bytes.length);
            indexWriter.close(bytes, bytes.length);
            bufOut.close();
        }
    }

}
