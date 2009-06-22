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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.*;


/**
 * There are two ways hinted data gets delivered to the intended nodes.
 *
 * runHints() runs periodically and pushes the hinted data on this node to
 * every intended node.
 *
 * runDelieverHints() is called when some other node starts up (potentially
 * from a failure) and delivers the hinted data just to that node.
 */
public class HintedHandOffManager
{
    private static HintedHandOffManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(HintedHandOffManager.class);
    public static final String key_ = "HintedHandOffKey";
    final static long intervalInMins_ = 60;
    private ScheduledExecutorService executor_ = new DebuggableScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("HINTED-HANDOFF-POOL"));


    public static HintedHandOffManager instance()
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new HintedHandOffManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }

    private static boolean sendMessage(String endpointAddress, String tableName, String key) throws DigestMismatchException, TimeoutException, IOException, InvalidRequestException
    {
        EndPoint endPoint = new EndPoint(endpointAddress, DatabaseDescriptor.getStoragePort());
        if (!FailureDetector.instance().isAlive(endPoint))
        {
            return false;
        }

        Table table = Table.open(tableName);
        Row row = table.get(key);
        Row purgedRow = new Row(tableName,key);
        for (ColumnFamily cf : row.getColumnFamilies())
        {
            purgedRow.addColumnFamily(ColumnFamilyStore.removeDeleted(cf));
        }
        RowMutation rm = new RowMutation(tableName, purgedRow);
        Message message = rm.makeRowMutationMessage();
        QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(1, new WriteResponseResolver());
        MessagingService.getMessagingInstance().sendRR(message, new EndPoint[]{ endPoint }, quorumResponseHandler);

        return quorumResponseHandler.get();
    }

    private static void deleteEndPoint(String endpointAddress, String tableName, String key, long timestamp) throws Exception
    {
        RowMutation rm = new RowMutation(tableName, key_);
        rm.delete(Table.hints_ + ":" + key + ":" + endpointAddress, timestamp);
        rm.apply();
    }

    private static void deleteHintedData(String tableName, String key) throws Exception
    {
        // delete the row from Application CFs: find the largest timestamp in any of
        // the data columns, and delete the entire CF with that value for the tombstone.

        // Note that we delete all data associated with the key: this may be more than
        // we sent earlier in sendMessage, since HH is not serialized with writes.
        // This is sub-optimal but okay, since HH is just an effort to make a recovering
        // node more consistent than it would have been; we can rely on the other
        // consistency mechanisms to finish the job in this corner case.
        RowMutation rm = new RowMutation(tableName, key_);
        Table table = Table.open(tableName);
        Row row = table.get(key); // not necessary to do removeDeleted here
        Collection<ColumnFamily> cfs = row.getColumnFamilies();
        for (ColumnFamily cf : cfs)
        {
            Set<IColumn> columns = cf.getAllColumns();
            long maxTS = Long.MIN_VALUE;
            if (!cf.isSuper())
            {
                for (IColumn col : columns)
                    maxTS = Math.max(maxTS, col.timestamp());
            }
            else
            {
                for (IColumn col : columns)
                {
                    maxTS = Math.max(maxTS, col.timestamp());
                    Collection<IColumn> subColumns = col.getSubColumns();
                    for (IColumn subCol : subColumns)
                        maxTS = Math.max(maxTS, subCol.timestamp());
                }
            }
            rm.delete(cf.name(), maxTS);
        }
        rm.apply();
    }

    private static void deliverAllHints(ColumnFamilyStore columnFamilyStore)
    {
        logger_.debug("Started hinted handoff of " + columnFamilyStore.columnFamily_);

        // 1. Scan through all the keys that we need to handoff
        // 2. For each key read the list of recepients and send
        // 3. Delete that recepient from the key if write was successful
        // 4. If all writes were success for a given key we can even delete the key .
        // 5. Now force a flush
        // 6. Do major compaction to clean up all deletes etc.
        // 7. I guess we r done
        for ( String tableName:DatabaseDescriptor.getTables() )
        {
            try
            {
                Table table = Table.open(tableName);
                ColumnFamily hintColumnFamily = ColumnFamilyStore.removeDeleted(table.get(key_, Table.hints_), Integer.MAX_VALUE);
                if (hintColumnFamily == null)
                {
                    columnFamilyStore.forceFlush();
                    return;
                }
                Collection<IColumn> keys = hintColumnFamily.getAllColumns();
                if (keys == null)
                {
                    return;
                }

                for (IColumn keyColumn : keys)
                {
                    Collection<IColumn> endpoints = keyColumn.getSubColumns();
                    int deleted = 0;
                    for (IColumn endpoint : endpoints)
                    {
                        if (sendMessage(endpoint.name(), tableName, keyColumn.name()))
                        {
                            deleteEndPoint(endpoint.name(), tableName, keyColumn.name(), keyColumn.timestamp());
                            deleted++;
                        }
                    }
                    if (deleted == endpoints.size())
                    {
                        deleteHintedData(tableName, keyColumn.name());
                    }
                }
                columnFamilyStore.forceFlush();
                columnFamilyStore.forceCompaction(null, null, 0, null);
            }
            catch (Exception ex)
            {
                logger_.error("error delivering hints", ex);
            }
            finally
            {
                logger_.debug("Finished hinted handoff of " + columnFamilyStore.columnFamily_);
            }
        }
    }

    private static void deliverHintsToEndpoint(EndPoint endPoint)
    {
        logger_.debug("Started hinted handoff for endPoint " + endPoint.getHost());

        // 1. Scan through all the keys that we need to handoff
        // 2. For each key read the list of recepients if teh endpoint matches send
        // 3. Delete that recepient from the key if write was successful
        for ( String tableName:DatabaseDescriptor.getTables() )
        {
            try
            {
                Table table = Table.open(tableName);
                ColumnFamily hintedColumnFamily = table.get(key_, Table.hints_);
                if (hintedColumnFamily == null)
                {
                    return;
                }
                Collection<IColumn> keys = hintedColumnFamily.getAllColumns();
                if (keys == null)
                {
                    return;
                }

                for (IColumn keyColumn : keys)
                {
                    Collection<IColumn> endpoints = keyColumn.getSubColumns();
                    for (IColumn endpoint : endpoints)
                    {
                        if (endpoint.name().equals(endPoint.getHost()) && sendMessage(endpoint.name(), null, keyColumn.name()))
                        {
                            deleteEndPoint(endpoint.name(), tableName, keyColumn.name(), keyColumn.timestamp());
                            if (endpoints.size() == 1)
                            {
                                deleteHintedData(tableName, keyColumn.name());
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger_.error("Error delivering hints", ex);
            }
            finally
            {
                logger_.debug("Finished hinted handoff for endpoint " + endPoint.getHost());
            }            
        }
    }

    public void submit(final ColumnFamilyStore columnFamilyStore)
    {
        Runnable r = new Runnable()
        {
            public void run()
            {
                deliverAllHints(columnFamilyStore);
            }
        };
    	executor_.scheduleWithFixedDelay(r, HintedHandOffManager.intervalInMins_, HintedHandOffManager.intervalInMins_, TimeUnit.MINUTES);
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void deliverHints(final EndPoint to)
    {
        Runnable r = new Runnable()
        {
            public void run()
            {
                deliverHintsToEndpoint(to);
            }
        };
    	executor_.submit(r);
    }
}
