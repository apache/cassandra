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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.utils.BloomFilter;


public class ThreadListBuilder
{
    private final static int bufSize_ = 64*1024*1024;
    private final static int count_ = 128*1024*1024;
    
    public static void main(String[] args) throws Throwable
    {
        if ( args.length != 2 )
        {
            System.out.println("Usage : java com.facebook.infrastructure.tools.ThreadListBuilder <directory containing files to be processed> <directory to dump the bloom filter in.>");
            System.exit(1);
        }
        
        File directory = new File(args[0]);
        File[] files = directory.listFiles();
        List<DataOutputBuffer> buffers = new ArrayList<DataOutputBuffer>();    
        BloomFilter bf = new BloomFilter(count_, 8);        
        int keyCount = 0;
        
        /* Process the list of files. */
        for ( File file : files )
        {
            System.out.println("Processing file " + file);
            BufferedReader bufReader = new BufferedReader( new InputStreamReader( new FileInputStream(file) ), ThreadListBuilder.bufSize_ );
            String line = null;
            
            while ( (line = bufReader.readLine()) != null )
            {
                /* After accumulating count_ keys reset the bloom filter. */
                if ( keyCount > 0 && keyCount % count_ == 0 )
                {                       
                    DataOutputBuffer bufOut = new DataOutputBuffer();
                    BloomFilter.serializer().serialize(bf, bufOut);
                    System.out.println("Finished serializing the bloom filter");
                    buffers.add(bufOut);
                    bf = new BloomFilter(count_, 8);
                }
                line = line.trim();                
                bf.add(line);
                ++keyCount;
            }
        }
        
        /* Add the bloom filter assuming the last one was left out */
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        buffers.add(bufOut);
        
        
        int size = buffers.size();
        for ( int i = 0; i < size; ++i )
        {
            DataOutputBuffer buffer = buffers.get(i);
            String file = args[1] + System.getProperty("file.separator") + "Bloom-Filter-" + i + ".dat";
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.write(buffer.getData(), 0, buffer.getLength());
            raf.close();
            buffer.close();
        }
        System.out.println("Done writing the bloom filter to disk");
    }
}
