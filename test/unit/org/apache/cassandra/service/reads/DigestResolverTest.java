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

package org.apache.cassandra.service.reads;

import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.DigestMismatchException;
import org.apache.cassandra.service.DigestResolver;
import org.apache.cassandra.service.ReadCallback;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertTrue;

public class DigestResolverTest
{
    public static final String KEYSPACE1 = "DigestResolverTest";
    public static final String CF_STANDARD = "Standard1";

    private static Keyspace ks;
    private static CFMetaData cfm;

    private static final InetAddressAndPort EP1;
    private static final InetAddressAndPort EP2;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.0.0.1");
            EP2 = InetAddressAndPort.getByName("127.0.0.2");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        CFMetaData.Builder builder1 = CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD)
                                                        .addPartitionKey("key", BytesType.instance)
                                                        .addClusteringColumn("col1", AsciiType.instance)
                                                        .addRegularColumn("c1", AsciiType.instance);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(2), builder1.build());

        ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CF_STANDARD);
        cfm = cfs.metadata;
    }
    
    /**
     * This test makes a time-boxed effort to reproduce the issue found in CASSANDRA-16883.
     */
    @Test
    public void multiThreadedNoRepairNeededReadCallback() throws DigestMismatchException
    {
        DecoratedKey dk = Util.dk("key1");
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, FBUtilities.nowInSeconds(), dk);
        BufferCell cell = BufferCell.live(cfm.partitionColumns().regulars.getSimple(0), 1000, bytes("1"));
        PartitionUpdate response = PartitionUpdate.singleRowUpdate(cfm, dk, BTreeRow.singleCellRow(cfm.comparator.make("1"), cell));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        long endTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);

        try
        {
            while (System.nanoTime() < endTime)
            {
                final DigestResolver resolver = new DigestResolver(ks, command, ConsistencyLevel.ONE, 2);
                final ReadCallback callback = new ReadCallback(resolver, ConsistencyLevel.ONE, command, ImmutableList.of(EP1.address, EP2.address), System.nanoTime());
                
                final CountDownLatch startlatch = new CountDownLatch(2);

                pool.execute(() ->
                             {
                                 startlatch.countDown();
                                 waitForLatch(startlatch);
                                 callback.response(ReadResponse.createDataResponse(iter(response), command));
                             });

                pool.execute(() ->
                             {
                                 startlatch.countDown();
                                 waitForLatch(startlatch);
                                 callback.response(ReadResponse.createDataResponse(iter(response), command));
                             });

                callback.awaitResults();
                assertTrue(resolver.isDataPresent());
                
                try (PartitionIterator result = resolver.resolve())
                {
                    assertTrue(result.hasNext());
                }
            }
        }
        finally
        {
            pool.shutdown();
        }
    }

    public UnfilteredPartitionIterator iter(PartitionUpdate update)
    {
        return new SingletonUnfilteredPartitionIterator(update.unfilteredIterator(), false);
    }

    private void waitForLatch(CountDownLatch startlatch)
    {
        try
        {
            startlatch.await();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
