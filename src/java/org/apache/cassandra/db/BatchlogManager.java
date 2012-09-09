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
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableSortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class BatchlogManager implements BatchlogManagerMBean
{
    private static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final int VERSION = MessagingService.VERSION_12;
    private static final long TIMEOUT = 2 * DatabaseDescriptor.getRpcTimeout();

    private static final ByteBuffer COORDINATOR = columnName("coordinator");
    private static final ByteBuffer WRITTEN_AT = columnName("written_at");
    private static final ByteBuffer DATA = columnName("data");
    private static final SortedSet<ByteBuffer> META = ImmutableSortedSet.of(COORDINATOR, WRITTEN_AT);

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);

    public static final BatchlogManager instance = new BatchlogManager();

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

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                replayAllFailedBatches();
            }
        };
        StorageService.optionalTasks.scheduleWithFixedDelay(runnable,
                                                            StorageService.RING_DELAY,
                                                            10 * 60 * 1000,
                                                            TimeUnit.MILLISECONDS);
    }

    public static RowMutation getBatchlogMutationFor(Collection<RowMutation> mutations, UUID uuid)
    {
        long timestamp = FBUtilities.timestampMicros();
        ByteBuffer coordinator = InetAddressType.instance.decompose(FBUtilities.getBroadcastAddress());
        ByteBuffer writtenAt = LongType.instance.decompose(timestamp / 1000);
        ByteBuffer data = serializeRowMutations(mutations);

        ColumnFamily cf = ColumnFamily.create(CFMetaData.BatchlogCF);
        cf.addColumn(new Column(COORDINATOR, coordinator, timestamp));
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

    private static void replayAllFailedBatches()
    {
        if (logger.isDebugEnabled())
            logger.debug("Started replayAllFailedBatches");

        ColumnFamilyStore store = Table.open(Table.SYSTEM_KS).getColumnFamilyStore(SystemTable.BATCHLOG_CF);

        if (store.isEmpty())
            return;

        IPartitioner partitioner = StorageService.getPartitioner();
        RowPosition minPosition = partitioner.getMinimumToken().minKeyBound();
        AbstractBounds<RowPosition> range = new Range<RowPosition>(minPosition, minPosition, partitioner);

        List<Row> rows = store.getRangeSlice(null, range, Integer.MAX_VALUE, new NamesQueryFilter(META), null);

        for (Row row : rows)
        {
            if (row.cf.isMarkedForDelete())
                continue;

            IColumn coordinatorColumn = row.cf.getColumn(COORDINATOR);
            IColumn writtenAtColumn = row.cf.getColumn(WRITTEN_AT);

            if (coordinatorColumn == null || writtenAtColumn == null)
            {
                replayBatch(row.key);
                continue;
            }

            InetAddress coordinator = InetAddressType.instance.compose(coordinatorColumn.value());
            long writtenAt = LongType.instance.compose(writtenAtColumn.value());
            // if the batch is new and its coordinator is alive - give it a chance to complete naturally.
            if (System.currentTimeMillis() < writtenAt + TIMEOUT && FailureDetector.instance.isAlive(coordinator))
                continue;

            replayBatch(row.key);
        }

        if (logger.isDebugEnabled())
            logger.debug("Finished replayAllFailedBatches");
    }

    private static void replayBatch(DecoratedKey key)
    {
        UUID uuid = UUIDType.instance.compose(key.key);

        if (logger.isDebugEnabled())
            logger.debug("Replaying batch {}", uuid);

        ColumnFamilyStore store = Table.open(Table.SYSTEM_KS).getColumnFamilyStore(SystemTable.BATCHLOG_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(key, new QueryPath(SystemTable.BATCHLOG_CF), DATA);
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
        for (InetAddress target : StorageProxy.getWriteEndpoints(mutation.getTable(), mutation.key()))
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
        return CFMetaData.BatchlogCF.getCfDef().getColumnNameBuilder().add(raw).build();
    }
}
