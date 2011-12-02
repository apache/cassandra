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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.cliffc.high_scale_lib.NonBlockingHashSet;



/**
 * For each endpoint for which we have hints, there is a row in the system hints CF.
 * The key for this row is ByteBuffer.wrap(string), i.e. "127.0.0.1".
 * (We have to use String keys for compatibility with OPP.)
 * SuperColumns in these rows are the mutations to replay, with uuid names:
 *
 *  <dest ip>: {              // key
 *    <uuid>: {               // supercolumn
 *      mutation: <mutation>  // subcolumn
 *      version: <mutation serialization version>
 *      table: <table of hinted mutation>
 *      key: <key of hinted mutation>
 *    }
 *  }
 *
 * When FailureDetector signals that a node that was down is back up, we page through
 * the hinted mutations and send them over one at a time, waiting for
 * hinted_handoff_throttle_delay in between each.
 *
 * deliverHints is also exposed to JMX so it can be run manually if FD ever misses
 * its cue somehow.
 */

public class HintedHandOffManager implements HintedHandOffManagerMBean
{
    public static final HintedHandOffManager instance = new HintedHandOffManager();
    public static final String HINTS_CF = "HintsColumnFamily";

    private static final Logger logger_ = LoggerFactory.getLogger(HintedHandOffManager.class);
    private static final int PAGE_SIZE = 1024;
    private static final int LARGE_NUMBER = 65536; // 64k nodes ought to be enough for anybody.

    // in 0.8, subcolumns were KS-CF bytestrings, and the data was stored in the "normal" storage there.
    // (so replay always consisted of sending an entire row,
    // no matter how little was part of the mutation that created the hint.)
    private static final String SEPARATOR_08 = "-";

    private final NonBlockingHashSet<InetAddress> queuedDeliveries = new NonBlockingHashSet<InetAddress>();

    private final ExecutorService executor_ = new JMXEnabledThreadPoolExecutor("HintedHandoff", Thread.MIN_PRIORITY);

    public HintedHandOffManager()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=HintedHandoffManager"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    public void registerMBean()
    {
        logger_.debug("Created HHOM instance, registered MBean.");
    }

    private static boolean sendMutation(InetAddress endpoint, RowMutation mutation) throws IOException
    {
        IWriteResponseHandler responseHandler = WriteResponseHandler.create(endpoint);
        MessagingService.instance().sendRR(mutation, endpoint, responseHandler);

        try
        {
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            return false;
        }

        try
        {
            Thread.sleep(DatabaseDescriptor.getHintedHandoffThrottleDelay());
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        return true;
    }

    private static void deleteHint(ByteBuffer tokenBytes, ByteBuffer hintId, long timestamp) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tokenBytes);
        rm.delete(new QueryPath(HINTS_CF, hintId), timestamp);
        rm.applyUnsafe(); // don't bother with commitlog since we're going to flush as soon as we're done with delivery
    }

    public void deleteHintsForEndpoint(final String ipOrHostname)
    {
        try
        {
            InetAddress endpoint = InetAddress.getByName(ipOrHostname);
            deleteHintsForEndpoint(endpoint);
        }
        catch (UnknownHostException e)
        {
            logger_.warn("Unable to find "+ipOrHostname+", not a hostname or ipaddr of a node?:");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void deleteHintsForEndpoint(final InetAddress endpoint)
    {
        if (!StorageService.instance.getTokenMetadata().isMember(endpoint))
            return;
        Token<?> token = StorageService.instance.getTokenMetadata().getToken(endpoint);
        ByteBuffer tokenBytes = StorageService.getPartitioner().getTokenFactory().toByteArray(token);
        final ColumnFamilyStore hintStore = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(HINTS_CF);
        final RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tokenBytes);
        rm.delete(new QueryPath(HINTS_CF), System.currentTimeMillis());

        // execute asynchronously to avoid blocking caller (which may be processing gossip)
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    logger_.info("Deleting any stored hints for " + endpoint);
                    rm.apply();
                    hintStore.forceFlush();
                    CompactionManager.instance.submitMaximal(hintStore, Integer.MAX_VALUE);
                }
                catch (Exception e)
                {
                    logger_.warn("Could not delete hints for " + endpoint + ": " + e);
                }
            }
        };
        StorageService.optionalTasks.execute(runnable);
    }

    private static boolean pagingFinished(ColumnFamily hintColumnFamily, ByteBuffer startColumn)
    {
        // done if no hints found or the start column (same as last column processed in previous iteration) is the only one
        return hintColumnFamily == null
               || (hintColumnFamily.getSortedColumns().size() == 1 && hintColumnFamily.getColumn(startColumn) != null);
    }

    private int waitForSchemaAgreement(InetAddress endpoint) throws InterruptedException
    {
        Gossiper gossiper = Gossiper.instance;
        int waited = 0;
        // first, wait for schema to be gossiped.
        while (gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA) == null) {
            Thread.sleep(1000);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new RuntimeException("Didin't receive gossiped schema from " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        waited = 0;
        // then wait for the correct schema version.
        // usually we use DD.getDefsVersion, which checks the local schema uuid as stored in the system table.
        // here we check the one in gossip instead; this serves as a canary to warn us if we introduce a bug that
        // causes the two to diverge (see CASSANDRA-2946)
        while (!gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA).value.equals(
                gossiper.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress()).getApplicationState(ApplicationState.SCHEMA).value))
        {
            Thread.sleep(1000);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new RuntimeException("Could not reach schema agreement with " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        logger_.debug("schema for {} matches local schema", endpoint);
        return waited;
    }
            
    private void deliverHintsToEndpoint(InetAddress endpoint) throws IOException, DigestMismatchException, InvalidRequestException, TimeoutException, InterruptedException
    {
        ColumnFamilyStore hintStore = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(HINTS_CF);
        try
        {
            if (hintStore.isEmpty())
                return; // nothing to do, don't confuse users by logging a no-op handoff

            logger_.debug("Checking remote({}) schema before delivering hints", endpoint);
            int waited = waitForSchemaAgreement(endpoint);
            // sleep a random amount to stagger handoff delivery from different replicas.
            // (if we had to wait, then gossiper randomness took care of that for us already.)
            if (waited == 0) {
                // use a 'rounded' sleep interval because of a strange bug with windows: CASSANDRA-3375
                int sleep = FBUtilities.threadLocalRandom().nextInt(2000) * 30;
                logger_.debug("Sleeping {}ms to stagger hint delivery", sleep);
                Thread.sleep(sleep);
            }

            if (!FailureDetector.instance.isAlive(endpoint))
            {
                logger_.info("Endpoint {} died before hint delivery, aborting", endpoint);
                return;
            }
        }
        finally
        {
            queuedDeliveries.remove(endpoint);
        }

        // 1. Get the key of the endpoint we need to handoff
        // 2. For each column, deserialize the mutation and send it to the endpoint
        // 3. Delete the subcolumn if the write was successful
        // 4. Force a flush
        // 5. Do major compaction to clean up all deletes etc.

        // find the hints for the node using its token.
        Token<?> token = StorageService.instance.getTokenMetadata().getToken(endpoint);
        logger_.info("Started hinted handoff for token: {} with IP: {}", token, endpoint);
        ByteBuffer tokenBytes = StorageService.getPartitioner().getTokenFactory().toByteArray(token);
        DecoratedKey<?> epkey =  StorageService.getPartitioner().decorateKey(tokenBytes);
        int rowsReplayed = 0;
        ByteBuffer startColumn = ByteBufferUtil.EMPTY_BYTE_BUFFER;

        delivery:
        while (true)
        {
            QueryFilter filter = QueryFilter.getSliceFilter(epkey, new QueryPath(HINTS_CF), startColumn, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, PAGE_SIZE);
            ColumnFamily hintColumnFamily = ColumnFamilyStore.removeDeleted(hintStore.getColumnFamily(filter), Integer.MAX_VALUE);
            if (pagingFinished(hintColumnFamily, startColumn))
                break;

            page:
            for (IColumn hint : hintColumnFamily.getSortedColumns())
            {
                startColumn = hint.name();
                for (IColumn subColumn : hint.getSubColumns())
                {
                    // both 0.8 and 1.0 column names are UTF8 strings, so this check is safe
                    if (ByteBufferUtil.string(subColumn.name()).contains(SEPARATOR_08))
                    {
                        logger_.debug("0.8-style hint found.  This should have been taken care of by purgeIncompatibleHints");
                        deleteHint(tokenBytes, hint.name(), hint.maxTimestamp());
                        continue page;
                    }
                }

                IColumn versionColumn = hint.getSubColumn(ByteBufferUtil.bytes("version"));
                IColumn tableColumn = hint.getSubColumn(ByteBufferUtil.bytes("table"));
                IColumn keyColumn = hint.getSubColumn(ByteBufferUtil.bytes("key"));
                IColumn mutationColumn = hint.getSubColumn(ByteBufferUtil.bytes("mutation"));
                assert versionColumn != null;
                assert tableColumn != null;
                assert keyColumn != null;
                assert mutationColumn != null;
                DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(mutationColumn.value()));
                RowMutation rm = RowMutation.serializer().deserialize(in, ByteBufferUtil.toInt(versionColumn.value()));

                if (sendMutation(endpoint, rm))
                {
                    deleteHint(tokenBytes, hint.name(), hint.maxTimestamp());
                    rowsReplayed++;
                }
                else
                {
                    logger_.info("Could not complete hinted handoff to " + endpoint);
                    break delivery;
                }
            }
        }

        if (rowsReplayed > 0)
        {
            hintStore.forceFlush();
            try
            {
                CompactionManager.instance.submitMaximal(hintStore, Integer.MAX_VALUE).get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        logger_.info(String.format("Finished hinted handoff of %s rows to endpoint %s",
                                   rowsReplayed, endpoint));
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void deliverHints(final InetAddress to)
    {
        logger_.debug("deliverHints to {}", to);
        if (!queuedDeliveries.add(to))
            return;

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                deliverHintsToEndpoint(to);
            }
        };
    	executor_.execute(r);
    }

    public void deliverHints(String to) throws UnknownHostException
    {
        deliverHints(InetAddress.getByName(to));
    }

    public List<String> listEndpointsPendingHints()
    {
        List<Row> rows = getHintsSlice(1);

        // Extract the keys as strings to be reported.
        LinkedList<String> result = new LinkedList<String>();
        for (Row r : rows)
        {
            if (r.cf != null) //ignore removed rows
                result.addFirst(new String(r.key.key.array()));
        }
        return result;
    }

    public Map<String, Integer> countPendingHints()
    {
        List<Row> rows = getHintsSlice(Integer.MAX_VALUE);

        Map<String, Integer> result = new HashMap<String, Integer>();
        for (Row r : rows)
        {
            if (r.cf != null) //ignore removed rows
                result.put(new String(r.key.key.array()), r.cf.getColumnCount());
        }
        return result;
    }

    private List<Row> getHintsSlice(int column_count)
    {
        // ColumnParent for HintsCF...
        ColumnParent parent = new ColumnParent(HINTS_CF);

        // Get count # of columns...
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]).setFinish(new byte[0]);
        sliceRange.setCount(column_count);
        predicate.setSlice_range(sliceRange);

        // From keys "" to ""...
        IPartitioner<?> partitioner = StorageService.getPartitioner();
        ByteBuffer empty = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        Range range = new Range(partitioner.getToken(empty), partitioner.getToken(empty));

        // Get a bunch of rows!
        List<Row> rows;
        try
        {
            rows = StorageProxy.getRangeSlice(new RangeSliceCommand("system", parent, predicate, range, LARGE_NUMBER), ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {
            logger_.info("HintsCF getEPPendingHints timed out.");
            throw new RuntimeException(e);
        }
        return rows;
    }
}
