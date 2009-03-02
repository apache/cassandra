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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKind;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ContinuationContext;
import org.apache.cassandra.concurrent.ContinuationStage;
import org.apache.cassandra.concurrent.ContinuationsExecutor;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.IStage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.continuations.Suspendable;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadMessage;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.mapreduce.SequentialScanner;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDeliveryTask;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.commons.javaflow.Continuation;
import org.apache.log4j.Logger;


public class TestRunner
{
    private static EndPoint to_ = new EndPoint("tdsearch001.sf2p.facebook.com", 7000);
    
    private static void doWrite() throws Throwable
    {

        Table table = Table.open("Mailbox");  
        Random random = new Random();
        byte[] bytes = new byte[1024];
        for (int i = 1001; i <= 1130; ++i)
        {
            String key = Integer.toString(i);
            RowMutation rm = new RowMutation("Mailbox", key);
            random.nextBytes(bytes);
            for ( int j = 0; j < 1; ++j )
            {
                for ( int k = 0; k < 1; ++k )
                {                    
                    rm.add("MailboxMailData0:SuperColumn-" + j + ":Column-" + k, bytes, k);                    
                }
            }
            rm.apply();
        }
        System.out.println("Write done");
    }
    
    private static void doRead() throws Throwable
    {
        Table table = Table.open("Mailbox");
        String key = "511055962";
                
        /*
        List<String> list = new ArrayList<String>();
        list.add("SuperColumn-0");
        Row row = table.getRow(key, "MailboxMailList0", list);
        System.out.println(row);
        */
        
        ColumnFamily cf = table.get(key, "MailboxMailData0");
        try
        {
            Collection<IColumn> columns = cf.getAllColumns();            
            for ( IColumn column : columns )
            {                
                System.out.println(column.name());                
                Collection<IColumn> subColumns = column.getSubColumns();
                for ( IColumn subColumn : subColumns )
                {
                    System.out.println(subColumn);
                }                            
            }
        }
        catch ( Throwable th )
        {
            th.printStackTrace();
        }
    }
    
    private static void doDeletes()
    {
        
    }
    
            
    public static void main(String[] args) throws Throwable
    {  
        /*
        String name = "/var/cassandra/test.dat";
        FileInputStream f = new FileInputStream(name);
        File file = new File("/var/cassandra");
        Path path = file.toPath();
        WatchService watcher = FileSystems.getDefault().newWatchService(); 
        Thread thread = new Thread( new WatchKeyMonitor(watcher) );
        thread.start();
        
        WatchKey wKey = path.register( watcher, StandardWatchEventKind.ENTRY_DELETE );          
        file = new File(name);
        file.delete();
        
        Thread.sleep(3000);
        System.out.println("Closing the stream ...");
        f.close();
        */
        
        /*
        LogUtil.init();
        StorageService s = StorageService.instance();
        s.start();    
        doRead();
        */
        /*
        FileOutputStream fos = new FileOutputStream("C:\\Engagements\\Test.dat", true);
        SequentialScanner scanner = new SequentialScanner("Mailbox");            
        int count = 0;
        while ( scanner.hasNext() )
        {
            Row row = scanner.next();  
            String value = row.key() + System.getProperty("line.separator");
            fos.write( value.getBytes() );
           
            Map<String, ColumnFamily> cfs = row.getColumnFamilies();
            Set<String> keys = cfs.keySet();
            
            for ( String key : keys )
            {
                System.out.println(row.getColumnFamily(key));
            }           
        }          
        fos.close();
        System.out.println("Done ...");
        */
        /*
        ExecutorService es = new DebuggableThreadPoolExecutor(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl("TEST"));
        es.execute(new TestImpl());
        */
        /*
        LogUtil.init();
        StorageService s = StorageService.instance();
        s.start();
        */
        /*
        ReadMessage readMessage = new ReadMessage("Mailbox", args[1], "Test");
        Message message = ReadMessage.makeReadMessage(readMessage);
        Runnable task = new MessageDeliveryTask(message);
               
        ExecutorService es = new ContinuationsExecutor(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl("TEST"));
        int end = Integer.parseInt(args[0]);
        for ( int i = 0; i < end; ++i )
        {
            es.execute(task);
        }
        */
        
        /*
        if ( args[0].equals("S") )
        {  
            ExecutorService es = new ContinuationsExecutor(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>() );               
            es.execute( new Scanner() );
        }
        */
        /*
        DataOutputBuffer bufOut = new DataOutputBuffer();
        String value = "Avinash Lakshman";
        for ( int i = 0; i < 100; ++i )
        {
            bufOut.writeUTF(Integer.toString(i));
            bufOut.writeInt(value.length());
            bufOut.write(value.getBytes());                        
        }
        
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bufOut.getData(), bufOut.getLength());
        DataOutputBuffer buffer = new DataOutputBuffer();        
        IFileWriter writer = SequenceFile.aioWriter("C:\\Engagements\\test.dat", 64*1024);
        SortedMap<String, Integer> offsets = getOffsets(bufIn);
        Set<String> keys = offsets.keySet();                                                      
        for ( String key : keys )
        {
            bufIn.setPosition(offsets.get(key));
            buffer.reset();
            buffer.write(bufIn, bufIn.readInt());
            writer.append(key, buffer);            
        }
        writer.close();
        */
    }
}

@Suspendable
class Scanner implements Runnable
{   
    private static final Logger logger_ = Logger.getLogger(Scanner.class);
    
    public void run()
    {        
        try
        {            
            SequentialScanner scanner = new SequentialScanner("Mailbox");            
            
            while ( scanner.hasNext() )
            {
                Row row = scanner.next();    
                logger_.debug(row.key());
            }            
        }
        catch ( IOException ex )
        {
            ex.printStackTrace();
        }        
    }
}

class Test 
{
    public static void goo()
    {
        System.out.println("I am goo()");
    }
}
