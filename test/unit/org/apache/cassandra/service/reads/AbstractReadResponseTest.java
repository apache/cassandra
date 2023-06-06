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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.BufferClusteringBoundary;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.Verb.READ_REQ;

/**
 * Base class for testing various components which deal with read responses
 */
@Ignore
public abstract class AbstractReadResponseTest
{
    public static final String KEYSPACE1 = "DataResolverTest";
    public static final String KEYSPACE3 = "DataResolverTest3";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COLLECTION = "Collection1";

    public static Keyspace ks;
    public static Keyspace ks3;
    public static ColumnFamilyStore cfs;
    public static ColumnFamilyStore cfs2;
    public static ColumnFamilyStore cfs3;
    public static TableMetadata cfm;
    public static TableMetadata cfm2;
    public static TableMetadata cfm3;
    public static ColumnMetadata m;

    public static DecoratedKey dk;
    static long nowInSec;

    static final InetAddressAndPort EP1;
    static final InetAddressAndPort EP2;
    static final InetAddressAndPort EP3;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.0.0.1");
            EP2 = InetAddressAndPort.getByName("127.0.0.2");
            EP3 = InetAddressAndPort.getByName("127.0.0.3");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        TableMetadata.Builder builder1 =
        TableMetadata.builder(KEYSPACE1, CF_STANDARD)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addClusteringColumn("col1", AsciiType.instance)
                     .addRegularColumn("c1", AsciiType.instance)
                     .addRegularColumn("c2", AsciiType.instance)
                     .addRegularColumn("one", AsciiType.instance)
                     .addRegularColumn("two", AsciiType.instance);

        TableMetadata.Builder builder3 =
        TableMetadata.builder(KEYSPACE3, CF_STANDARD)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addClusteringColumn("col1", AsciiType.instance)
                     .addRegularColumn("c1", AsciiType.instance)
                     .addRegularColumn("c2", AsciiType.instance)
                     .addRegularColumn("one", AsciiType.instance)
                     .addRegularColumn("two", AsciiType.instance);

        TableMetadata.Builder builder2 =
        TableMetadata.builder(KEYSPACE1, CF_COLLECTION)
                     .addPartitionKeyColumn("k", ByteType.instance)
                     .addRegularColumn("m", MapType.getInstance(IntegerType.instance, IntegerType.instance, true));

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(2), builder1, builder2);
        SchemaLoader.createKeyspace(KEYSPACE3, KeyspaceParams.simple(4), builder3);

        ks = Keyspace.open(KEYSPACE1);
        cfs = ks.getColumnFamilyStore(CF_STANDARD);
        cfm = cfs.metadata();
        cfs2 = ks.getColumnFamilyStore(CF_COLLECTION);
        cfm2 = cfs2.metadata();
        ks3 = Keyspace.open(KEYSPACE3);
        cfs3 = ks3.getColumnFamilyStore(CF_STANDARD);
        cfm3 = cfs3.metadata();
        m = cfm2.getColumn(new ColumnIdentifier("m", false));
    }

    @Before
    public void setUp() throws Exception
    {
        dk = Util.dk("key1");
        nowInSec = FBUtilities.nowInSeconds();
    }

    static void assertPartitionsEqual(RowIterator l, RowIterator r)
    {
        try (RowIterator left = l; RowIterator right = r)
        {
            Assert.assertTrue(Util.sameContent(left, right));
        }
    }

    static void assertPartitionsEqual(UnfilteredRowIterator left, UnfilteredRowIterator right)
    {
        Assert.assertTrue(Util.sameContent(left, right));
    }

    static void assertPartitionsEqual(UnfilteredPartitionIterator left, UnfilteredPartitionIterator right)
    {
        while (left.hasNext())
        {
            Assert.assertTrue(right.hasNext());
            assertPartitionsEqual(left.next(), right.next());
        }
        Assert.assertFalse(right.hasNext());
    }

    static void assertPartitionsEqual(PartitionIterator l, PartitionIterator r)
    {
        try (PartitionIterator left = l; PartitionIterator right = r)
        {
            while (left.hasNext())
            {
                Assert.assertTrue(right.hasNext());
                assertPartitionsEqual(left.next(), right.next());
            }
            Assert.assertFalse(right.hasNext());
        }
    }

    static void consume(PartitionIterator i)
    {
        try (PartitionIterator iterator = i)
        {
            while (iterator.hasNext())
            {
                try (RowIterator rows = iterator.next())
                {
                    while (rows.hasNext())
                        rows.next();
                }
            }
        }
    }

    static PartitionIterator filter(UnfilteredPartitionIterator iter)
    {
        return UnfilteredPartitionIterators.filter(iter, nowInSec);
    }

    static DecoratedKey dk(String k)
    {
        return cfs.decorateKey(ByteBufferUtil.bytes(k));
    }

    static DecoratedKey dk(int k)
    {
        return dk(Integer.toString(k));
    }


    static Message<ReadResponse> response(ReadCommand command,
                                            InetAddressAndPort from,
                                            UnfilteredPartitionIterator data,
                                            boolean isDigestResponse,
                                            int fromVersion,
                                            ByteBuffer repairedDataDigest,
                                            boolean hasPendingRepair)
    {
        ReadResponse response = isDigestResponse
                                ? ReadResponse.createDigestResponse(data, command)
                                : ReadResponse.createRemoteDataResponse(data, repairedDataDigest, hasPendingRepair, command, fromVersion);
        return Message.builder(READ_REQ, response)
                      .from(from)
                      .build();
    }

    static Message<ReadResponse> response(InetAddressAndPort from,
                                            UnfilteredPartitionIterator partitionIterator,
                                            ByteBuffer repairedDigest,
                                            boolean hasPendingRepair,
                                            ReadCommand cmd)
    {
        return response(cmd, from, partitionIterator, false, MessagingService.current_version, repairedDigest, hasPendingRepair);
    }

    static Message<ReadResponse> response(ReadCommand command, InetAddressAndPort from, UnfilteredPartitionIterator data, boolean isDigestResponse)
    {
        return response(command, from, data, false, MessagingService.current_version, ByteBufferUtil.EMPTY_BYTE_BUFFER, isDigestResponse);
    }

    static Message<ReadResponse> response(ReadCommand command, InetAddressAndPort from, UnfilteredPartitionIterator data)
    {
        return response(command, from, data, false, MessagingService.current_version, ByteBufferUtil.EMPTY_BYTE_BUFFER, false);
    }

    public RangeTombstone tombstone(Object start, Object end, long markedForDeleteAt, long localDeletionTime)
    {
        return tombstone(start, true, end, true, markedForDeleteAt, localDeletionTime);
    }

    public RangeTombstone tombstone(Object start, boolean inclusiveStart, Object end, boolean inclusiveEnd, long markedForDeleteAt, long localDeletionTime)
    {
        ClusteringBound<?> startBound = rtBound(start, true, inclusiveStart);
        ClusteringBound<?> endBound = rtBound(end, false, inclusiveEnd);
        return new RangeTombstone(Slice.make(startBound, endBound), DeletionTime.build(markedForDeleteAt, localDeletionTime));
    }

    public ClusteringBound<?> rtBound(Object value, boolean isStart, boolean inclusive)
    {
        ClusteringBound.Kind kind = isStart
                                    ? (inclusive ? ClusteringPrefix.Kind.INCL_START_BOUND : ClusteringPrefix.Kind.EXCL_START_BOUND)
                                    : (inclusive ? ClusteringPrefix.Kind.INCL_END_BOUND : ClusteringPrefix.Kind.EXCL_END_BOUND);

        return BufferClusteringBound.create(kind, cfm.comparator.make(value).getBufferArray());
    }

    public ClusteringBoundary rtBoundary(Object value, boolean inclusiveOnEnd)
    {
        ClusteringBound.Kind kind = inclusiveOnEnd
                                    ? ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY
                                    : ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY;
        return BufferClusteringBoundary.create(kind, cfm.comparator.make(value).getBufferArray());
    }

    public RangeTombstoneBoundMarker marker(Object value, boolean isStart, boolean inclusive, long markedForDeleteAt, long localDeletionTime)
    {
        return new RangeTombstoneBoundMarker(rtBound(value, isStart, inclusive), DeletionTime.build(markedForDeleteAt, localDeletionTime));
    }

    public RangeTombstoneBoundaryMarker boundary(Object value, boolean inclusiveOnEnd, long markedForDeleteAt1, long localDeletionTime1, long markedForDeleteAt2, long localDeletionTime2)
    {
        return new RangeTombstoneBoundaryMarker(rtBoundary(value, inclusiveOnEnd),
                                                DeletionTime.build(markedForDeleteAt1, localDeletionTime1),
                                                DeletionTime.build(markedForDeleteAt2, localDeletionTime2));
    }

    public UnfilteredPartitionIterator fullPartitionDelete(TableMetadata table, DecoratedKey dk, long timestamp, long nowInSec)
    {
        return new SingletonUnfilteredPartitionIterator(PartitionUpdate.fullPartitionDelete(table, dk, timestamp, nowInSec).unfilteredIterator());
    }

    public UnfilteredPartitionIterator iter(PartitionUpdate update)
    {
        return new SingletonUnfilteredPartitionIterator(update.unfilteredIterator());
    }

    public UnfilteredPartitionIterator iter(DecoratedKey key, Unfiltered... unfiltereds)
    {
        SortedSet<Unfiltered> s = new TreeSet<>(cfm.comparator);
        Collections.addAll(s, unfiltereds);
        final Iterator<Unfiltered> iterator = s.iterator();

        UnfilteredRowIterator rowIter = new AbstractUnfilteredRowIterator(cfm,
                                                                          key,
                                                                          DeletionTime.LIVE,
                                                                          cfm.regularAndStaticColumns(),
                                                                          Rows.EMPTY_STATIC_ROW,
                                                                          false,
                                                                          EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                return iterator.hasNext() ? iterator.next() : endOfData();
            }
        };
        return new SingletonUnfilteredPartitionIterator(rowIter);
    }
}
