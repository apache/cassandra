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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

public class BatchlogManager implements BatchlogManagerMBean
{
    private static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final int VERSION = MessagingService.VERSION_12;
    private static final long TIMEOUT = 2 * DatabaseDescriptor.getWriteRpcTimeout();

    private static final ByteBuffer WRITTEN_AT = columnName("written_at");
    private static final ByteBuffer DATA = columnName("data");

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);

    public static final BatchlogManager instance = new BatchlogManager();

    private final AtomicLong totalBatchesReplayed = new AtomicLong();
    private final AtomicBoolean isReplaying = new AtomicBoolean();

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
        StorageService.optionalTasks.scheduleWithFixedDelay(runnable,
                                                            StorageService.RING_DELAY,
                                                            10 * 60 * 1000,
                                                            TimeUnit.MILLISECONDS);
    }

    public int countAllBatches()
    {
        int count = 0;

        for (Row row : getRangeSlice(new NamesQueryFilter(ImmutableSortedSet.<ByteBuffer>of())))
        {
            if (row.cf != null && !row.cf.isMarkedForDelete())
                count++;
        }

        return count;
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
        StorageService.optionalTasks.execute(runnable);
    }

    public static RowMutation getBatchlogMutationFor(Collection<RowMutation> mutations, UUID uuid)
    {
        long timestamp = FBUtilities.timestampMicros();
        ByteBuffer writtenAt = LongType.instance.decompose(timestamp / 1000);
        ByteBuffer data = serializeRowMutations(mutations);

        ColumnFamily cf = ColumnFamily.create(CFMetaData.BatchlogCf);
        cf.addColumn(new Column(WRITTEN_AT, writtenAt, timestamp));
        cf.addColumn(new Column(DATA, data, timestamp));
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, UUIDType.instance.decompose(uuid));
        rm.add(cf);

        return rm;
    }

    private static ByteBuffer serializeRowMutations(Collection<RowMutation> mutations)
    {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try
        {
            dos.writeInt(mutations.size());
            for (RowMutation rm : mutations)
                RowMutation.serializer.serialize(rm, dos, VERSION);
        }
        catch (IOException e)
        {
            throw new AssertionError(); // cannot happen.
        }

        return ByteBuffer.wrap(bos.toByteArray());
    }

    private void replayAllFailedBatches() throws ExecutionException, InterruptedException
    {
        if (!isReplaying.compareAndSet(false, true))
            return;

        try
        {
            logger.debug("Started replayAllFailedBatches");

            for (Row row : getRangeSlice(new NamesQueryFilter(WRITTEN_AT)))
            {
                if (row.cf == null || row.cf.isMarkedForDelete())
                    continue;

                IColumn writtenAt = row.cf.getColumn(WRITTEN_AT);
                if (writtenAt == null || System.currentTimeMillis() > LongType.instance.compose(writtenAt.value()) + TIMEOUT)
                    replayBatch(row.key);
            }

            cleanup();
        }
        finally
        {
            isReplaying.set(false);
        }

        logger.debug("Finished replayAllFailedBatches");
    }

    private void replayBatch(DecoratedKey key)
    {
        UUID uuid = UUIDType.instance.compose(key.key);

        logger.debug("Replaying batch {}", uuid);

        ColumnFamilyStore store = Table.open(Table.SYSTEM_KS).getColumnFamilyStore(SystemTable.BATCHLOG_CF);
        QueryFilter filter = QueryFilter.getIdentityFilter(key, new QueryPath(SystemTable.BATCHLOG_CF));
        ColumnFamily batch = store.getColumnFamily(filter);

        if (batch == null || batch.isMarkedForDelete())
            return;

        IColumn dataColumn = batch.getColumn(DATA);
        try
        {
            if (dataColumn != null)
                writeHintsForSerializedMutations(dataColumn.value());
        }
        catch (IOException e)
        {
            logger.warn("Skipped batch replay of {} due to {}", uuid, e);
        }

        deleteBatch(key);
        totalBatchesReplayed.incrementAndGet();
    }

    private static void writeHintsForSerializedMutations(ByteBuffer data) throws IOException
    {
        DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(data));
        int size = in.readInt();
        for (int i = 0; i < size; i++)
            writeHintsForMutation(RowMutation.serializer.deserialize(in, VERSION));
    }

    private static void writeHintsForMutation(RowMutation mutation) throws IOException
    {
        String table = mutation.getTable();
        Token tk = StorageService.getPartitioner().getToken(mutation.key());
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);
        for (InetAddress target : Iterables.concat(naturalEndpoints, pendingEndpoints))
        {
            if (target.equals(FBUtilities.getBroadcastAddress()))
                mutation.apply();
            else
                StorageProxy.writeHintForMutation(mutation, target);
        }
    }

    private static void deleteBatch(DecoratedKey key)
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, key.key);
        rm.delete(new QueryPath(SystemTable.BATCHLOG_CF), FBUtilities.timestampMicros());
        rm.apply();
    }

    private static ByteBuffer columnName(String name)
    {
        ByteBuffer raw = UTF8Type.instance.decompose(name);
        return CFMetaData.BatchlogCf.getCfDef().getColumnNameBuilder().add(raw).build();
    }

    private static List<Row> getRangeSlice(IDiskAtomFilter columnFilter)
    {
        ColumnFamilyStore store = Table.open(Table.SYSTEM_KS).getColumnFamilyStore(SystemTable.BATCHLOG_CF);
        IPartitioner partitioner = StorageService.getPartitioner();
        RowPosition minPosition = partitioner.getMinimumToken().minKeyBound();
        AbstractBounds<RowPosition> range = new Range<RowPosition>(minPosition, minPosition, partitioner);
        return store.getRangeSlice(null, range, Integer.MAX_VALUE, columnFilter, null);
    }

    /** force flush + compaction to reclaim space from replayed batches */
    private void cleanup() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Table.open(Table.SYSTEM_KS).getColumnFamilyStore(SystemTable.BATCHLOG_CF);
        cfs.forceBlockingFlush();
        Collection<Descriptor> descriptors = new ArrayList<Descriptor>();
        for (SSTableReader sstr : cfs.getSSTables())
            descriptors.add(sstr.descriptor);
        if (!descriptors.isEmpty()) // don't pollute the logs if there is nothing to compact.
            CompactionManager.instance.submitUserDefined(cfs, descriptors, Integer.MAX_VALUE).get();
    }
}
