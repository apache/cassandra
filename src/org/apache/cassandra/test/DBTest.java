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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Scanner;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.mapreduce.SequentialScanner;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.LogUtil;


public class DBTest
{
    private static void doWrites() throws Throwable
    {         
        for ( int i = 0; i < 512*1024; ++i )
        {
            String key = Integer.toString(i);
            RowMutation rm = new RowMutation("Mailbox", key);
            String value = "Data for key " + key;
            rm.add("Test:" + "Column", value.getBytes(), i);                
            rm.apply();
        }
        System.out.println("Write done");
    }
    
    private static void doReads() throws Throwable
    {
        Table table = Table.open("Mailbox");
        for ( int i = 100; i < 1000; ++i )
        {        
            String key = Integer.toString(i);
            Row row = table.getRow(key, "Test");
            System.out.println( row.getColumnFamily("Test") );
            System.out.println("Row read done");            
            ColumnFamily cf = table.get(key, "Test");                                  
            if (cf == null)
                System.out.println("KEY " + key + " is missing");
            else
            {
                Collection<IColumn> superColumns = cf.getAllColumns();                
                System.out.println("Success ...");
            }
        }
        System.out.println("Read done ...");  
    }
    
    private static void doRead(String key) throws Throwable
    {
        Table table = Table.open("Mailbox");
        Row row = table.getRow(key, "Test");    
        ColumnFamily cf = table.get(key, "Test");                                  
        if (cf == null)
            System.out.println("KEY " + key + " is missing");
        else
        {
            Collection<IColumn> columns = cf.getAllColumns();                
            for ( IColumn column : columns )
            {
                System.out.println(column.name());
                System.out.println( new String( column.value() ) );
            }
        }
        System.out.println("Read done ...");
    }
    
    private static void doScannerTest() throws Throwable
    {
        Scanner scanner = new Scanner("Mailbox");
        scanner.fetch(Integer.toString(105), "MailboxMailList0");
        
        while ( scanner.hasNext() )
        {
            System.out.println(scanner.next().name());
        }             
    }
    
    private static void doSequentialScannerTest() throws Throwable
    {
        SequentialScanner scanner = new SequentialScanner("Mailbox");
        while ( scanner.hasNext() )
        {
            Row row = scanner.next();  
            System.out.println( row.getColumnFamily("Test") );
            System.out.println( row.getColumnFamily("Test2") );
        }
    }
    
    public static void doTest()
    {
        String host = "insearch00";
        String host2 = "insearch0";
        Set<EndPoint> allNodes = new HashSet<EndPoint>();
        for ( int i = 1; i <= 3; ++i )
        {
            if ( i < 10 )
                allNodes.add( new EndPoint(host + i + ".sf2p.facebook.com", 7000) );
            else
                allNodes.add( new EndPoint(host2 + i + ".sf2p.facebook.com", 7000) );
        }
        
        for ( int i = 1; i <= 2; ++i )
        {
            if ( i < 10 )
                allNodes.add( new EndPoint(host + i + ".ash1.facebook.com", 7000) );
            else
                allNodes.add( new EndPoint(host2 + i + ".ash1.facebook.com", 7000) );
        }
        
        TestChoice t = new TestChoice(allNodes);
        t.assignReplicas();
    }
    
    public static void main(String[] args) throws Throwable
    {
        /*
        SSTable ssTable = new SSTable("C:\\Engagements\\", "Sample-Bf");
        BloomFilter bf = new BloomFilter(512*1024, 15);
        for ( int i = 0; i < 512*1024; ++i )
        {
            bf.fill( Integer.toString(i) );
        }        
        ssTable.close(bf);
        */
        /*
        IFileWriter writer = SequenceFile.bufferedWriter("C:\\Engagements\\Sample-Bf-Data.db", 4*1024*1024);
        BloomFilter bf = new BloomFilter(512*1024, 15);
        for ( int i = 0; i < 512*1024; ++i )
        {
            bf.fill( Integer.toString(i) );
        }
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        bufOut.close();
        writer.close(bufOut.getData(), bufOut.getLength());
        writer.close();        
        
        IFileReader reader = SequenceFile.bufferedReader("C:\\Engagements\\Sample-Bf-Data.db", 4*1024*1024);
        //DataOutputBuffer bufOut = new DataOutputBuffer();
        bufOut.reset();
        reader.next(bufOut);
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bufOut.getData(), bufOut.getLength());
        bufIn.readUTF();
        bufIn.readInt();
        BloomFilter bf2 = BloomFilter.serializer().deserialize(bufIn);
        int count = 0;
        for ( int i = 0; i < 512*1024; ++i )
        {
            if ( !bf2.isPresent(Integer.toString(i)) )
                ++count;
        }
        System.out.println(count);
        reader.close();
        */
        //LogUtil.init();
        //StorageService.instance().start(); 
        //doWrites();
        //doRead("543");
        
        DatabaseDescriptor.init();
        DBTest.doTest();
    }
}
