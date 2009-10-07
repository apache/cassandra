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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.*;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;


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
    final static long INTERVAL_IN_MS = 3600 * 1000;
    private ExecutorService executor_ = new DebuggableThreadPoolExecutor("HINTED-HANDOFF-POOL");
    Timer timer = new Timer("HINTED-HANDOFF-TIMER");
    public static final String HINTS_CF = "HintsColumnFamily";


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
        MessagingService.instance().sendRR(message, new EndPoint[]{ endPoint }, quorumResponseHandler);

        return quorumResponseHandler.get();
    }

    private static void deleteEndPoint(byte[] endpointAddress, String tableName, byte[] key, long timestamp) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tableName);
        rm.delete(new QueryPath(HINTS_CF, key, endpointAddress), timestamp);
        rm.apply();
    }

    private static void deleteHintedData(String tableName, String key) throws IOException
    {
        // delete the row from Application CFs: find the largest timestamp in any of
        // the data columns, and delete the entire CF with that value for the tombstone.

        // Note that we delete all data associated with the key: this may be more than
        // we sent earlier in sendMessage, since HH is not serialized with writes.
        // This is sub-optimal but okay, since HH is just an effort to make a recovering
        // node more consistent than it would have been; we can rely on the other
        // consistency mechanisms to finish the job in this corner case.
        RowMutation rm = new RowMutation(tableName, key);
        Table table = Table.open(tableName);
        Row row = table.get(key); // not necessary to do removeDeleted here
        Collection<ColumnFamily> cfs = row.getColumnFamilies();
        for (ColumnFamily cf : cfs)
        {
            long maxTS = Long.MIN_VALUE;
            if (!cf.isSuper())
            {
                for (IColumn col : cf.getSortedColumns())
                    maxTS = Math.max(maxTS, col.timestamp());
            }
            else
            {
                for (IColumn col : cf.getSortedColumns())
                {
                    maxTS = Math.max(maxTS, col.timestamp());
                    Collection<IColumn> subColumns = col.getSubColumns();
                    for (IColumn subCol : subColumns)
                        maxTS = Math.max(maxTS, subCol.timestamp());
                }
            }
            rm.delete(new QueryPath(cf.name()), maxTS);
        }
        rm.apply();
    }

    /** hintStore must be the hints columnfamily from the system table */
    private static void deliverAllHints(ColumnFamilyStore hintStore) throws DigestMismatchException, IOException, InvalidRequestException, TimeoutException
    {
        if (logger_.isDebugEnabled())
          logger_.debug("Started deliverAllHints");

        // 1. Scan through all the keys that we need to handoff
        // 2. For each key read the list of recipients and send
        // 3. Delete that recipient from the key if write was successful
        // 4. If all writes were success for a given key we can even delete the key .
        // 5. Now force a flush
        // 6. Do major compaction to clean up all deletes etc.
        // 7. I guess we are done
        for (String tableName : DatabaseDescriptor.getTables())
        {
            ColumnFamily hintColumnFamily = ColumnFamilyStore.removeDeleted(hintStore.getColumnFamily(new IdentityQueryFilter(tableName, new QueryPath(HINTS_CF))), Integer.MAX_VALUE);
            if (hintColumnFamily == null)
            {
                continue;
            }
            Collection<IColumn> keys = hintColumnFamily.getSortedColumns();

            for (IColumn keyColumn : keys)
            {
                Collection<IColumn> endpoints = keyColumn.getSubColumns();
                String keyStr = new String(keyColumn.name(), "UTF-8");
                int deleted = 0;
                for (IColumn endpoint : endpoints)
                {
                    String endpointStr = new String(endpoint.name(), "UTF-8");
                    if (sendMessage(endpointStr, tableName, keyStr))
                    {
                        deleteEndPoint(endpoint.name(), tableName, keyColumn.name(), keyColumn.timestamp());
                        deleted++;
                    }
                }
                if (deleted == endpoints.size())
                {
                    deleteHintedData(tableName, keyStr);
                }
            }
        }
        hintStore.forceFlush();
        hintStore.doMajorCompaction(0);

        if (logger_.isDebugEnabled())
          logger_.debug("Finished deliverAllHints");
    }

    private static void deliverHintsToEndpoint(EndPoint endPoint) throws IOException, DigestMismatchException, InvalidRequestException, TimeoutException
    {
        if (logger_.isDebugEnabled())
          logger_.debug("Started hinted handoff for endPoint " + endPoint.getHost());

        String targetEPBytes = endPoint.getHost();
        // 1. Scan through all the keys that we need to handoff
        // 2. For each key read the list of recipients if the endpoint matches send
        // 3. Delete that recipient from the key if write was successful
        Table systemTable = Table.open(Table.SYSTEM_TABLE);
        for (String tableName : DatabaseDescriptor.getTables())
        {
            ColumnFamily hintedColumnFamily = systemTable.get(tableName, HINTS_CF);
            if (hintedColumnFamily == null)
            {
                continue;
            }
            Collection<IColumn> keys = hintedColumnFamily.getSortedColumns();

            for (IColumn keyColumn : keys)
            {
                String keyStr = new String(keyColumn.name(), "UTF-8");
                Collection<IColumn> endpoints = keyColumn.getSubColumns();
                for (IColumn hintEndPoint : endpoints)
                {
                    if (hintEndPoint.name().equals(targetEPBytes) && sendMessage(endPoint.getHost(), null, keyStr))
                    {
                        deleteEndPoint(hintEndPoint.name(), tableName, keyColumn.name(), keyColumn.timestamp());
                        if (endpoints.size() == 1)
                        {
                            deleteHintedData(tableName, keyStr);
                        }
                    }
                }
            }
        }

        if (logger_.isDebugEnabled())
          logger_.debug("Finished hinted handoff for endpoint " + endPoint.getHost());
    }

    public void scheduleHandoffsFor(final ColumnFamilyStore columnFamilyStore)
    {
        final Runnable r = new Runnable()
        {
            public void run()
            {
                try
                {
                    deliverAllHints(columnFamilyStore);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
        timer.schedule(new TimerTask()
        {
            public void run()
            {
                executor_.execute(r);
            }
        }, INTERVAL_IN_MS, INTERVAL_IN_MS);
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
                try
                {
                    deliverHintsToEndpoint(to);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    	executor_.submit(r);
    }
}
