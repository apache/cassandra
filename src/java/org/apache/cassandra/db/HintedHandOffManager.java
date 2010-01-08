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
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;

import java.net.InetAddress;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.*;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.WrappedRunnable;


/**
 * For each table (keyspace), there is a row in the system hints CF.
 * SuperColumns in that row are keys for which we have hinted data.
 * Subcolumns names within that supercolumn are host IPs. Subcolumn values are always empty.
 * Instead, we store the row data "normally" in the application table it belongs in.
 *
 * So when we deliver hints we look up endpoints that need data delivered
 * on a per-key basis, then read that entire row out and send it over.
 * (TODO handle rows that have incrementally grown too large for a single message.)
 *
 * HHM never deletes the row from Application tables; there is no way to distinguish that
 * from hinted tombstones!  instead, rely on cleanup compactions to remove data
 * that doesn't belong on this node.  (Cleanup compactions may be started manually
 * -- on a per node basis -- with "nodeprobe cleanup.")
 *
 * TODO this avoids our hint rows from growing excessively large by offloading the
 * message data into application tables.  But, this means that cleanup compactions
 * will nuke HH data.  Probably better would be to store the RowMutation messages
 * in a HHData (non-super) CF, modifying the above to store a UUID value in the
 * HH subcolumn value, which we use as a key to a [standard] HHData system CF
 * that would contain the message bytes.
 *
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
    private static volatile HintedHandOffManager instance_;
    private static final Lock lock_ = new ReentrantLock();
    private static final Logger logger_ = Logger.getLogger(HintedHandOffManager.class);
    final static long INTERVAL_IN_MS = 3600 * 1000; // check for ability to deliver hints this often
    public static final String HINTS_CF = "HintsColumnFamily";
    private static final int PAGE_SIZE = 10000;

    private final ExecutorService executor_ = new JMXEnabledThreadPoolExecutor("HINTED-HANDOFF-POOL");


    public static HintedHandOffManager instance()
    {
        if (instance_ == null)
        {
            lock_.lock();
            try
            {
                if (instance_ == null)
                    instance_ = new HintedHandOffManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }

    public HintedHandOffManager()
    {
        new Thread(new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (true)
                {
                    Thread.sleep(INTERVAL_IN_MS);
                    deliverAllHints();
                }
            }
        }, "Hint delivery").start();
    }

    private static boolean sendMessage(InetAddress endPoint, String tableName, String key) throws IOException
    {
        if (!Gossiper.instance().isKnownEndpoint(endPoint))
        {
            logger_.warn("Hints found for endpoint " + endPoint + " which is not part of the gossip network.  discarding.");
            return true;
        }
        if (!FailureDetector.instance().isAlive(endPoint))
        {
            return false;
        }

        Table table = Table.open(tableName);
        RowMutation rm = new RowMutation(tableName, key);
        for (ColumnFamilyStore cfstore : table.getColumnFamilyStores().values())
        {
            ColumnFamily cf = cfstore.getColumnFamily(new IdentityQueryFilter(key, new QueryPath(cfstore.getColumnFamilyName())));
            if (cf != null)
                rm.add(cf);
        }
        Message message = rm.makeRowMutationMessage();
        WriteResponseHandler responseHandler = new WriteResponseHandler(1);
        MessagingService.instance().sendRR(message, new InetAddress[] { endPoint }, responseHandler);

        try
        {
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            return false;
        }
        return true;
    }

    private static void deleteEndPoint(byte[] endpointAddress, String tableName, byte[] key, long timestamp) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tableName);
        rm.delete(new QueryPath(HINTS_CF, key, endpointAddress), timestamp);
        rm.apply();
    }

    private static void deleteHintKey(String tableName, byte[] key) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tableName);
        rm.delete(new QueryPath(HINTS_CF, key, null), System.currentTimeMillis());
        rm.apply();
    }

    /** hintStore must be the hints columnfamily from the system table */
    private static void deliverAllHints() throws DigestMismatchException, IOException, InvalidRequestException, TimeoutException
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
        ColumnFamilyStore hintStore = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(HINTS_CF);
        for (String tableName : DatabaseDescriptor.getTables())
        {
            byte[] startColumn = ArrayUtils.EMPTY_BYTE_ARRAY;
            while (true)
            {
                QueryFilter filter = new SliceQueryFilter(tableName, new QueryPath(HINTS_CF), startColumn, ArrayUtils.EMPTY_BYTE_ARRAY, false, PAGE_SIZE);
                ColumnFamily hintColumnFamily = ColumnFamilyStore.removeDeleted(hintStore.getColumnFamily(filter), Integer.MAX_VALUE);
                if (hintColumnFamily == null)
                    break;
                Collection<IColumn> keys = hintColumnFamily.getSortedColumns();

                for (IColumn keyColumn : keys)
                {
                    Collection<IColumn> endpoints = keyColumn.getSubColumns();
                    String keyStr = new String(keyColumn.name(), "UTF-8");
                    int deleted = 0;
                    for (IColumn endpoint : endpoints)
                    {
                        if (sendMessage(InetAddress.getByAddress(endpoint.name()), tableName, keyStr))
                        {
                            deleteEndPoint(endpoint.name(), tableName, keyColumn.name(), System.currentTimeMillis());
                            deleted++;
                        }
                    }
                    if (deleted == endpoints.size())
                    {
                        deleteHintKey(tableName, keyColumn.name());
                    }

                    startColumn = keyColumn.name(); // repeating the last as the first is fine since we just deleted it
                }
            }
        }
        hintStore.forceFlush();
        try
        {
            CompactionManager.instance.submitMajor(hintStore).get();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        if (logger_.isDebugEnabled())
          logger_.debug("Finished deliverAllHints");
    }

    private static void deliverHintsToEndpoint(InetAddress endPoint) throws IOException, DigestMismatchException, InvalidRequestException, TimeoutException
    {
        if (logger_.isDebugEnabled())
          logger_.debug("Started hinted handoff for endPoint " + endPoint);

        byte[] targetEPBytes = endPoint.getAddress();
        // 1. Scan through all the keys that we need to handoff
        // 2. For each key read the list of recipients if the endpoint matches send
        // 3. Delete that recipient from the key if write was successful
        ColumnFamilyStore hintStore = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(HINTS_CF);
        for (String tableName : DatabaseDescriptor.getTables())
        {
            byte[] startColumn = ArrayUtils.EMPTY_BYTE_ARRAY;
            while (true)
            {
                QueryFilter filter = new SliceQueryFilter(tableName, new QueryPath(HINTS_CF), startColumn, ArrayUtils.EMPTY_BYTE_ARRAY, false, PAGE_SIZE);
                ColumnFamily hintColumnFamily = ColumnFamilyStore.removeDeleted(hintStore.getColumnFamily(filter), Integer.MAX_VALUE);
                if (hintColumnFamily == null)
                    break;
                Collection<IColumn> keys = hintColumnFamily.getSortedColumns();

                for (IColumn keyColumn : keys)
                {
                    String keyStr = new String(keyColumn.name(), "UTF-8");
                    Collection<IColumn> endpoints = keyColumn.getSubColumns();
                    for (IColumn hintEndPoint : endpoints)
                    {
                        if (Arrays.equals(hintEndPoint.name(), targetEPBytes) && sendMessage(endPoint, tableName, keyStr))
                        {
                            if (endpoints.size() == 1)
                                deleteHintKey(tableName, keyColumn.name());
                            else
                                deleteEndPoint(hintEndPoint.name(), tableName, keyColumn.name(), System.currentTimeMillis());
                            break;
                        }
                    }

                    startColumn = keyColumn.name(); // repeating the last as the first is fine since we just deleted it
                }
            }
        }

        if (logger_.isDebugEnabled())
          logger_.debug("Finished hinted handoff for endpoint " + endPoint);
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void deliverHints(final InetAddress to)
    {
        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                deliverHintsToEndpoint(to);
            }
        };
    	executor_.submit(r);
    }
}
