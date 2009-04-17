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

package org.apache.cassandra.test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.utils.BloomFilter;


public class SSTableTest
{
    private static void rawSSTableWrite() throws Throwable
    {
        SSTable ssTable = new SSTable("C:\\Engagements\\Cassandra", "Table-Test-1");
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter bf = new BloomFilter(1000, 8);
        byte[] bytes = new byte[64*1024];
        Random random = new Random();
        for ( int i = 100; i < 1000; ++i )
        {
            String key = Integer.toString(i);
            ColumnFamily cf = new ColumnFamily("Test", "Standard");
            bufOut.reset();           
            // random.nextBytes(bytes);
            cf.addColumn("C", "Avinash Lakshman is a good man".getBytes(), i);
            ColumnFamily.serializerWithIndexes().serialize(cf, bufOut);
            ssTable.append(key, bufOut);            
            bf.add(key);
        }
        ssTable.close(bf);
    }

    private static void readSSTable() throws Throwable
    {
        SSTable ssTable = new SSTable("C:\\Engagements\\Cassandra\\Table-Test-1-Data.db");  
        for ( int i = 100; i < 1000; ++i )
        {
            String key = Integer.toString(i);            
            DataInputBuffer bufIn = ssTable.next(key, "Test:C");
            ColumnFamily cf = ColumnFamily.serializer().deserialize(bufIn);
            if ( cf != null )
            {            
                System.out.println("KEY:" + key);
                System.out.println(cf.name());
                Collection<IColumn> columns = cf.getAllColumns();
                for ( IColumn column : columns )
                {
                    System.out.println(column.name());
                }
            }
            else
            {
                System.out.println("CF doesn't exist for key " + key);
            }                             
        }
    }
    
    public static void main(String[] args) throws Throwable
    {
        BloomFilter bf = new BloomFilter(1024*1024, 15);
        for ( int i = 0; i < 1024*1024; ++i )
        {
            bf.add(Integer.toString(i));
        }
        
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        FileOutputStream fos = new FileOutputStream("C:\\Engagements\\bf.dat", true);
        fos.write(bufOut.getData(), 0, bufOut.getLength());
        fos.close();
        
        FileInputStream fis = new FileInputStream("C:\\Engagements\\bf.dat");
        byte[] bytes = new byte[fis.available()];
        fis.read(bytes);
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bytes, bytes.length );
        BloomFilter bf2 = BloomFilter.serializer().deserialize(bufIn);
        
        int count = 0;
        for ( int i = 0; i < 1024*1024; ++i )
        {
            if ( bf.isPresent(Integer.toString(i)) )
                ++count;
        }
        System.out.println(count);
        
        //DatabaseDescriptor.init();
        //hashSSTableWrite();
        //rawSSTableWrite();
        //readSSTable();
    } 
}
