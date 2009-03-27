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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.io.SSTable;

import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class BinaryMemtable implements MemtableMBean
{
    private static Logger logger_ = Logger.getLogger( Memtable.class );
    private int threshold_ = 512*1024*1024;
    private AtomicInteger currentSize_ = new AtomicInteger(0);

    /* Table and ColumnFamily name are used to determine the ColumnFamilyStore */
    private String table_;
    private String cfName_;
    private boolean isFrozen_ = false;
    private Map<String, byte[]> columnFamilies_ = new NonBlockingHashMap<String, byte[]>();
    /* Lock and Condition for notifying new clients about Memtable switches */
    Lock lock_ = new ReentrantLock();
    Condition condition_;

    BinaryMemtable(String table, String cfName) throws IOException
    {
        condition_ = lock_.newCondition();
        table_ = table;
        cfName_ = cfName;
    }

    public int getMemtableThreshold()
    {
        return currentSize_.get();
    }

    void resolveSize(int oldSize, int newSize)
    {
        currentSize_.addAndGet(newSize - oldSize);
    }


    boolean isThresholdViolated()
    {
        if (currentSize_.get() >= threshold_ || columnFamilies_.size() > 50000)
        {
            logger_.debug("CURRENT SIZE:" + currentSize_.get());
        	return true;
        }
        return false;
    }

    String getColumnFamily()
    {
    	return cfName_;
    }

    /*
     * This version is used by the external clients to put data into
     * the memtable. This version will respect the threshold and flush
     * the memtable to disk when the size exceeds the threshold.
    */
    void put(String key, byte[] buffer) throws IOException
    {
        if (isThresholdViolated() )
        {
            lock_.lock();
            try
            {
                ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
                if (!isFrozen_)
                {
                    isFrozen_ = true;
                    BinaryMemtableManager.instance().submit(cfStore.getColumnFamilyName(), this);
                    cfStore.switchBinaryMemtable(key, buffer);
                }
                else
                {
                    cfStore.applyBinary(key, buffer);
                }
            }
            finally
            {
                lock_.unlock();
            }
        }
        else
        {
            resolve(key, buffer);
        }
    }

    private void resolve(String key, byte[] buffer)
    {
            columnFamilies_.put(key, buffer);
            currentSize_.addAndGet(buffer.length + key.length());
    }


    /*
     * 
    */
    void flush() throws IOException
    {
        if ( columnFamilies_.size() == 0 )
            return;
        ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
        String directory = DatabaseDescriptor.getDataFileLocation();
        String filename = cfStore.getNextFileName();

        /*
         * Use the SSTable to write the contents of the TreeMap
         * to disk.
        */
        SSTable ssTable = new SSTable(directory, filename);
        List<String> keys = new ArrayList<String>( columnFamilies_.keySet() );
        Collections.sort(keys);        
        /* Use this BloomFilter to decide if a key exists in a SSTable */
        BloomFilter bf = new BloomFilter(keys.size(), 8);
        for ( String key : keys )
        {           
            byte[] bytes = columnFamilies_.get(key);
            if ( bytes.length > 0 )
            {            	
                /* Now write the key and value to disk */
                ssTable.append(key, bytes);
                bf.fill(key);
            }
        }
        ssTable.close(bf);
        cfStore.storeLocation( ssTable.getDataFileLocation(), bf );
        columnFamilies_.clear();       
    }
}
