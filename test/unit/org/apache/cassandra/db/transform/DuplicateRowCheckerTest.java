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

package org.apache.cassandra.db.transform;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.junit.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.DIAGNOSTIC_SNAPSHOT_INTERVAL_NANOS;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DuplicateRowCheckerTest extends CQLTester
{
    ColumnFamilyStore cfs;
    TableMetadata metadata;
    static HashMap<InetAddressAndPort, Message<?>> sentMessages;

    @BeforeClass
    public static void setupMessaging()
    {
        sentMessages = new HashMap<>();
        MessagingService.instance().outboundSink.add((message, to) -> { sentMessages.put(to, message); return false;});
    }

    @Before
    public void setup() throws Throwable
    {
        DatabaseDescriptor.setSnapshotOnDuplicateRowDetection(true);
        DIAGNOSTIC_SNAPSHOT_INTERVAL_NANOS.setLong(0);
        // Create a table and insert some data. The actual rows read in the test will be synthetic
        // but this creates an sstable on disk to be snapshotted.
        createTable("CREATE TABLE %s (pk text, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2))");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (pk, ck1, ck2, v) values (?, ?, ?, ?)", "key", i, i, i);
        flush();

        metadata = getCurrentColumnFamilyStore().metadata();
        cfs = getCurrentColumnFamilyStore();
        sentMessages.clear();
    }

    @Test
    public void noDuplicates()
    {
        // no duplicates
        iterate(iter(metadata,
                     false,
                     makeRow(metadata, 0, 0),
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 2)));
        assertCommandIssued(sentMessages, false);
    }

    @Test
    public void singleDuplicateForward()
    {

        iterate(iter(metadata,
                     false,
                     makeRow(metadata, 0, 0),
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 1)));
        assertCommandIssued(sentMessages, true);
    }

    @Test
    public void singleDuplicateReverse()
    {
        iterate(iter(metadata,
                     true,
                     makeRow(metadata, 0, 0),
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 1)));
        assertCommandIssued(sentMessages, true);
    }

    @Test
    public void multipleContiguousForward()
    {
        iterate(iter(metadata,
                     false,
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 1)));
        assertCommandIssued(sentMessages, true);
    }

    @Test
    public void multipleContiguousReverse()
    {
        iterate(iter(metadata,
                     true,
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 1)));
        assertCommandIssued(sentMessages, true);
    }

    @Test
    public void multipleDisjointForward()
    {
        iterate(iter(metadata,
                     false,
                     makeRow(metadata, 0, 0),
                     makeRow(metadata, 0, 0),
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 2),
                     makeRow(metadata, 0, 2)));
        assertCommandIssued(sentMessages, true);
    }

    @Test
    public void multipleDisjointReverse()
    {
        iterate(iter(metadata,
                     true,
                     makeRow(metadata, 0, 0),
                     makeRow(metadata, 0, 0),
                     makeRow(metadata, 0, 1),
                     makeRow(metadata, 0, 2),
                     makeRow(metadata, 0, 2)));
        assertCommandIssued(sentMessages, true);
    }

    public static void assertCommandIssued(HashMap<InetAddressAndPort, Message<?>> sent, boolean isExpected)
    {
        assertEquals(isExpected, !sent.isEmpty());
        if (isExpected)
        {
            assertEquals(1, sent.size());
            assertTrue(sent.containsKey(FBUtilities.getBroadcastAddressAndPort()));
            SnapshotCommand command = (SnapshotCommand) sent.get(FBUtilities.getBroadcastAddressAndPort()).payload;
            assertTrue(command.snapshot_name.startsWith(DiagnosticSnapshotService.DUPLICATE_ROWS_DETECTED_SNAPSHOT_PREFIX));
        }
    }

    private void iterate(UnfilteredPartitionIterator iter)
    {
        try (PartitionIterator partitions = applyChecker(iter))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    partition.forEachRemaining(u -> {});
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }

    public static Row makeRow(TableMetadata metadata, Object... clusteringValues)
    {
        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);

        return BTreeRow.noCellLiveRow(Clustering.make(clusteringByteBuffers), LivenessInfo.create(0, 0));
    }

    public static UnfilteredRowIterator partition(TableMetadata metadata,
                                                  DecoratedKey key,
                                                  boolean isReversedOrder,
                                                  Unfiltered... unfiltereds)
    {
        Iterator<Unfiltered> iterator = Iterators.forArray(unfiltereds);
        return new AbstractUnfilteredRowIterator(metadata,
                                                 key,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 isReversedOrder,
                                                 EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                return iterator.hasNext() ? iterator.next() : endOfData();
            }
        };
    }

    private static PartitionIterator applyChecker(UnfilteredPartitionIterator unfiltered)
    {
        long nowInSecs = 0;
        return DuplicateRowChecker.duringRead(FilteredPartitions.filter(unfiltered, nowInSecs),
                                              Collections.singletonList(FBUtilities.getBroadcastAddressAndPort()));
    }

    public static UnfilteredPartitionIterator iter(TableMetadata metadata, boolean isReversedOrder, Unfiltered... unfiltereds)
    {
        DecoratedKey key = metadata.partitioner.decorateKey(bytes("key"));
        UnfilteredRowIterator rowIter = partition(metadata, key, isReversedOrder, unfiltereds);
        return new SingletonUnfilteredPartitionIterator(rowIter);
    }
}
