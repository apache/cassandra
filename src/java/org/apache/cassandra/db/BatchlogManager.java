/*
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
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

public class BatchlogManager implements BatchlogManagerMBean
{
    private static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INTERVAL = 60 * 1000; // milliseconds
    private static final int PAGE_SIZE = 128; // same as HHOM, for now, w/out using any heuristics. TODO: set based on avg batch size.

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();

    private final AtomicLong totalBatchesReplayed = new AtomicLong();
    private final AtomicBoolean isReplaying = new AtomicBoolean();

    public static final ScheduledExecutorService batchlogTasks = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");

    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws ExecutionException, InterruptedException
            {
                replayAllFailedBatches();
            }
        };

        batchlogTasks.scheduleWithFixedDelay(runnable, StorageService.RING_DELAY, REPLAY_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public int countAllBatches()
    {
        return (int) process("SELECT count(*) FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.BATCHLOG_CF).one().getLong("count");
    }

    public long getTotalBatchesReplayed()
    {
        return totalBatchesReplayed.longValue();
    }

    public void forceBatchlogReplay()
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws ExecutionException, InterruptedException
            {
                replayAllFailedBatches();
            }
        };
        batchlogTasks.execute(runnable);
    }

    public static RowMutation getBatchlogMutationFor(Collection<RowMutation> mutations, UUID uuid)
    {
        return getBatchlogMutationFor(mutations, uuid, FBUtilities.timestampMicros());
    }

    @VisibleForTesting
    static RowMutation getBatchlogMutationFor(Collection<RowMutation> mutations, UUID uuid, long now)
    {
        ByteBuffer writtenAt = LongType.instance.decompose(now / 1000);
        ByteBuffer data = serializeRowMutations(mutations);

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(CFMetaData.BatchlogCf);
        cf.addColumn(new Column(columnName(""), ByteBufferUtil.EMPTY_BYTE_BUFFER, now));
        cf.addColumn(new Column(columnName("data"), data, now));
        cf.addColumn(new Column(columnName("written_at"), writtenAt, now));

        return new RowMutation(Keyspace.SYSTEM_KS, UUIDType.instance.decompose(uuid), cf);
    }

    private static ByteBuffer serializeRowMutations(Collection<RowMutation> mutations)
    {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        try
        {
            out.writeInt(mutations.size());
            for (RowMutation rm : mutations)
                RowMutation.serializer.serialize(rm, out, MessagingService.VERSION_12);
        }
        catch (IOException e)
        {
            throw new AssertionError(); // cannot happen.
        }

        return ByteBuffer.wrap(bos.toByteArray());
    }

    @VisibleForTesting
    void replayAllFailedBatches() throws ExecutionException, InterruptedException
    {
        if (!isReplaying.compareAndSet(false, true))
            return;

        logger.debug("Started replayAllFailedBatches");

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        int throttleInKB = DatabaseDescriptor.getBatchlogReplayThrottleInKB() / StorageService.instance.getTokenMetadata().getAllEndpoints().size();
        RateLimiter rateLimiter = RateLimiter.create(throttleInKB == 0 ? Double.MAX_VALUE : throttleInKB * 1024);

        try
        {
            UntypedResultSet page = process("SELECT id, data, written_at, version FROM %s.%s LIMIT %d",
                                            Keyspace.SYSTEM_KS,
                                            SystemKeyspace.BATCHLOG_CF,
                                            PAGE_SIZE);

            while (!page.isEmpty())
            {
                UUID id = processBatchlogPage(page, rateLimiter);

                if (page.size() < PAGE_SIZE)
                    break; // we've exhausted the batchlog, next query would be empty.

                page = process("SELECT id, data, written_at, version FROM %s.%s WHERE token(id) > token(%s) LIMIT %d",
                               Keyspace.SYSTEM_KS,
                               SystemKeyspace.BATCHLOG_CF,
                               id,
                               PAGE_SIZE);
            }

            cleanup();
        }
        finally
        {
            isReplaying.set(false);
        }

        logger.debug("Finished replayAllFailedBatches");
    }

    // returns the UUID of the last seen batch
    private UUID processBatchlogPage(UntypedResultSet page, RateLimiter rateLimiter)
    {
        UUID id = null;
        for (UntypedResultSet.Row row : page)
        {
            id = row.getUUID("id");
            long writtenAt = row.getLong("written_at");
            int version = row.has("version") ? row.getInt("version") : MessagingService.VERSION_12;
            // enough time for the actual write + batchlog entry mutation delivery (two separate requests).
            long timeout = DatabaseDescriptor.getWriteRpcTimeout() * 2; // enough time for the actual write + BM removal mutation
            if (System.currentTimeMillis() < writtenAt + timeout)
                continue; // not ready to replay yet, might still get a deletion.
            replayBatch(id, row.getBytes("data"), writtenAt, version, rateLimiter);
        }
        return id;
    }

    private void replayBatch(UUID id, ByteBuffer data, long writtenAt, int version, RateLimiter rateLimiter)
    {
        logger.debug("Replaying batch {}", id);

        try
        {
            replaySerializedMutations(data, writtenAt, version, rateLimiter);
        }
        catch (IOException e)
        {
            logger.warn("Skipped batch replay of {} due to {}", id, e);
        }

        deleteBatch(id);

        totalBatchesReplayed.incrementAndGet();
    }

    private void deleteBatch(UUID id)
    {
        RowMutation mutation = new RowMutation(Keyspace.SYSTEM_KS, UUIDType.instance.decompose(id));
        mutation.delete(SystemKeyspace.BATCHLOG_CF, FBUtilities.timestampMicros());
        mutation.apply();
    }

    private void replaySerializedMutations(ByteBuffer data, long writtenAt, int version, RateLimiter rateLimiter) throws IOException
    {
        DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(data));
        int size = in.readInt();
        for (int i = 0; i < size; i++)
            replaySerializedMutation(RowMutation.serializer.deserialize(in, version), writtenAt, version, rateLimiter);
    }

    /*
     * We try to deliver the mutations to the replicas ourselves if they are alive and only resort to writing hints
     * when a replica is down or a write request times out.
     */
    private void replaySerializedMutation(RowMutation mutation, long writtenAt, int version, RateLimiter rateLimiter)
    {
        int ttl = calculateHintTTL(mutation, writtenAt);
        if (ttl <= 0)
            return; // the mutation isn't safe to replay.

        Set<InetAddress> liveEndpoints = new HashSet<>();
        String ks = mutation.getKeyspaceName();
        Token<?> tk = StorageService.getPartitioner().getToken(mutation.key());
        int mutationSize = (int) RowMutation.serializer.serializedSize(mutation, version);

        for (InetAddress endpoint : Iterables.concat(StorageService.instance.getNaturalEndpoints(ks, tk),
                                                     StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, ks)))
        {
            rateLimiter.acquire(mutationSize);
            if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                mutation.apply();
            else if (FailureDetector.instance.isAlive(endpoint))
                liveEndpoints.add(endpoint); // will try delivering directly instead of writing a hint.
            else
                StorageProxy.writeHintForMutation(mutation, ttl, endpoint);
        }

        if (!liveEndpoints.isEmpty())
            attemptDirectDelivery(mutation, writtenAt, liveEndpoints);
    }

    private void attemptDirectDelivery(RowMutation mutation, long writtenAt, Set<InetAddress> endpoints)
    {
        List<WriteResponseHandler> handlers = Lists.newArrayList();
        final CopyOnWriteArraySet<InetAddress> undelivered = new CopyOnWriteArraySet<>(endpoints);
        for (final InetAddress ep : endpoints)
        {
            Runnable callback = new Runnable()
            {
                public void run()
                {
                    undelivered.remove(ep);
                }
            };
            WriteResponseHandler handler = new WriteResponseHandler(ep, WriteType.UNLOGGED_BATCH, callback);
            MessagingService.instance().sendRR(mutation.createMessage(), ep, handler);
            handlers.add(handler);
        }

        // Wait for all the requests to complete.
        for (WriteResponseHandler handler : handlers)
        {
            try
            {
                handler.get();
            }
            catch (WriteTimeoutException e)
            {
                logger.debug("Timed out replaying a batched mutation to a node, will write a hint");
            }
        }

        if (!undelivered.isEmpty())
        {
            int ttl = calculateHintTTL(mutation, writtenAt); // recalculate ttl
            if (ttl > 0)
                for (InetAddress endpoint : undelivered)
                    StorageProxy.writeHintForMutation(mutation, ttl, endpoint);
        }
    }

    // calculate ttl for the mutation's hint (and reduce ttl by the time the mutation spent in the batchlog).
    // this ensures that deletes aren't "undone" by an old batch replay.
    private int calculateHintTTL(RowMutation mutation, long writtenAt)
    {
        return (int) ((HintedHandOffManager.calculateHintTTL(mutation) * 1000 - (System.currentTimeMillis() - writtenAt)) / 1000);
    }

    private static ByteBuffer columnName(String name)
    {
        return CFMetaData.BatchlogCf.getCfDef().getColumnNameBuilder().add(UTF8Type.instance.decompose(name)).build();
    }

    // force flush + compaction to reclaim space from the replayed batches
    private void cleanup() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.BATCHLOG_CF);
        cfs.forceBlockingFlush();
        Collection<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstr : cfs.getSSTables())
            descriptors.add(sstr.descriptor);
        if (!descriptors.isEmpty()) // don't pollute the logs if there is nothing to compact.
            CompactionManager.instance.submitUserDefined(cfs, descriptors, Integer.MAX_VALUE).get();
    }

    private static UntypedResultSet process(String format, Object... args)
    {
        return QueryProcessor.processInternal(String.format(format, args));
    }
}
