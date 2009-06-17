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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.io.SSTable.KeyPositionInfo;
import org.apache.cassandra.utils.BasicUtilities;


public class KeyExtracter
{
    private static final int bufferSize_ = 64*1024;

    public static void main(String[] args) throws Throwable
    {
        if ( args.length != 3 )
        {
            System.out.println("Usage : java com.facebook.infrastructure.tools.IndexBuilder <key to extract> <data file> <output file>");
            System.exit(1);
        }
		String keyToExtract = args[0];
		String dataFile = args[1];
		String outputFile = args[2];

        extractKeyIntoFile(keyToExtract, dataFile, outputFile);
    }

    public static boolean extractKeyIntoFile(String keyToExtract, String dataFile, String outputFile) throws IOException
    {
		IFileReader dataReader = SequenceFile.bufferedReader(dataFile, bufferSize_);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();

    	try
    	{
            while ( !dataReader.isEOF() )
            {
                bufOut.reset();
                dataReader.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* Key just read */
                String key = bufIn.readUTF();
                /* check if we want this key */
                if ( key.equals(keyToExtract) )
                {
                	int keySize = bufIn.readInt();
                	byte[] keyData = new byte[keySize];
                	bufIn.read(keyData, 0, keySize);

                	/* write the key data into a file */
                    RandomAccessFile raf = new RandomAccessFile(outputFile, "rw");                	
                	raf.writeUTF(key);
                	raf.writeInt(keySize);
                	raf.write(keyData);
                    dumpBlockIndex(keyToExtract, 0L, keySize, raf);
                    raf.close();
                    return true;
                }
            }
        }
        finally
        {
            dataReader.close();
        }

        return false;
    }
    
    private static void dumpBlockIndex(String key, long position, long size, RandomAccessFile raf) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();                       
        /* Number of keys in this block */
        bufOut.writeInt(1);
        bufOut.writeUTF(key);
        bufOut.writeLong(position);
        bufOut.writeLong(size);
        
        /* Write out the block index. */
        raf.writeUTF(SSTable.blockIndexKey_);
        raf.writeInt(bufOut.getLength());
        raf.write(bufOut.getData(), 0, bufOut.getLength());
    }
}
