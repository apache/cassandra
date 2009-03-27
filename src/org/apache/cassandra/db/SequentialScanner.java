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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.log4j.Logger;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.continuations.Suspendable;
import org.apache.cassandra.db.ColumnFamilyNotDefinedException;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.FileStruct;
import org.apache.cassandra.db.IScanner;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;

/**
 * This class is used to scan through all the keys in disk
 * in Iterator style. Usage is as follows:
 * SequentialScanner scanner = new SequentialScanner("table");
 * 
 * while ( scanner.hasNext() )
 * {
 *     Row row = scanner.next();
 *     // Do something with the row
 * }
 * 
 * @author alakshman
 *
 */

public class SequentialScanner implements IScanner<Row>
{   
    private static Logger logger_ = Logger.getLogger( SequentialScanner.class );
    private final static int bufSize_ = 1024*1024;
    
    /* Table over which we want to perform a sequential scan. */
    private String table_;     
    private Queue<FileStruct> fsQueue_ = new PriorityQueue<FileStruct>();
    
    public SequentialScanner(String table) throws IOException
    {
        table_ = table;    
        List<String> allFiles = Table.open(table_).getAllSSTablesOnDisk();
        
        for (String file : allFiles)
        {                                         
            FileStruct fs = new FileStruct(file, SequentialScanner.bufSize_);             
            fsQueue_.add(fs);               
        }
    }
    
    /**
     * Determines if there is anything more to be 
     * scanned.
     * @return true if more elements are remanining
     * else false.
     */
    public boolean hasNext() throws IOException
    {
        boolean hasNext = ( fsQueue_.size() > 0 ) ? true : false;
        return hasNext;
    }
    
    /**
     * Returns the next row associated with the smallest key
     * on disk.
     * 
     * @return row of the next smallest key on disk.
     */
    public Row next() throws IOException
    {
        if ( fsQueue_.size() == 0 )
            throw new IllegalStateException("Nothing in the stream to scan.");
    
        Row row = null;          
        FileStruct fs = fsQueue_.poll();   
        
        // Process the key only if it is in the primary range and not a block index.
        if ( StorageService.instance().isPrimary(fs.getKey()) && !fs.getKey().equals(SSTable.blockIndexKey_) )
        {            
            row = Table.open(table_).get(fs.getKey());                
        }
        
        doCorrections(fs.getKey());        
        long bytesRead = fs.advance();
        if ( bytesRead != -1L )
            fsQueue_.add(fs);
        return row;
    }
    
    /**
     * This method advances the pointer in the file struct
     * in the even the same key occurs in multiple files.
     * 
     * @param key key we are interested in.
     * @throws IOException
     */
    private void doCorrections(String key) throws IOException
    { 
        List<FileStruct> lfs = new ArrayList<FileStruct>();
        Iterator<FileStruct> it = fsQueue_.iterator();
        
        while ( it.hasNext() )
        {
            FileStruct fs = it.next();
            /* 
             * We encountered a key that is greater 
             * than the key we are currently serving 
             * so scram. 
            */
            if ( fs.getKey().compareTo(key) != 0 )
            {                
                break;
            }
            else
            {                
                lfs.add(fs);
            }
        }
        
        for ( FileStruct fs : lfs )
        {
            /* discard duplicate entries. */
            fsQueue_.poll();
            long bytesRead = fs.advance();
            if ( bytesRead != -1L )
            {                
                fsQueue_.add(fs);
            }
        }
    }
    
    public void close() throws IOException
    {
        if ( fsQueue_.size() > 0 )
        {            
            for ( int i = 0; i < fsQueue_.size(); ++i )
            {
                FileStruct fs = fsQueue_.poll();
                fs.close();
            }
        }
    }
    
    public void fetch(String key, String cf) throws IOException, ColumnFamilyNotDefinedException
    {
        throw new UnsupportedOperationException("This operation does not make sense in the SequentialScanner");
    }
}
