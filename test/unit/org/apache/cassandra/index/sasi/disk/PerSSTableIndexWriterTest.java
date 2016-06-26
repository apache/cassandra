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
package org.apache.cassandra.index.sasi.disk;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sasi.KeyFetcher;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.util.concurrent.Futures;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PerSSTableIndexWriterTest extends SchemaLoader
{
    private static final String KS_NAME = "sasi";
    private static final String CF_NAME = "test_cf";

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        SchemaLoader.loadSchema();
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(KS_NAME,
                                                                     KeyspaceParams.simpleTransient(1),
                                                                     Tables.of(SchemaLoader.sasiCFMD(KS_NAME, CF_NAME))));
    }

    private static final ClusteringComparator CLUSTERING_COMPARATOR = new ClusteringComparator(LongType.instance);

    @Test
    public void testPartialIndexWrites() throws Exception
    {
        final int maxKeys = 100000, numParts = 4, partSize = maxKeys / numParts;
        final String keyFormat = "key%06d";
        final long timestamp = System.currentTimeMillis();

        ColumnFamilyStore cfs = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        ColumnDefinition column = cfs.metadata.getColumnDefinition(UTF8Type.instance.decompose("age"));

        SASIIndex sasi = (SASIIndex) cfs.indexManager.getIndexByName("age");

        File directory = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor descriptor = Descriptor.fromFilename(cfs.getSSTablePath(directory));
        PerSSTableIndexWriter indexWriter = (PerSSTableIndexWriter) sasi.getFlushObserver(descriptor, OperationType.FLUSH);

        SortedMap<RowKey, Row> expectedKeys = new TreeMap<>();

        for (int i = 0; i < maxKeys; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.format(keyFormat, i));
            Clustering clustering = Clustering.make(ByteBufferUtil.bytes(i * 1L));
            expectedKeys.put(new RowKey(cfs.metadata.partitioner.decorateKey(key), clustering, CLUSTERING_COMPARATOR),
                             BTreeRow.singleCellRow(clustering,
                                                    BufferCell.live(column, timestamp, Int32Type.instance.decompose(i))));
        }

        indexWriter.begin();

        Iterator<Map.Entry<RowKey, Row>> keyIterator = expectedKeys.entrySet().iterator();
        long position = 0;

        Set<String> segments = new HashSet<>();
        outer:
        for (;;)
        {
            for (int i = 0; i < partSize; i++)
            {
                if (!keyIterator.hasNext())
                    break outer;

                Map.Entry<RowKey, Row> key = keyIterator.next();

                indexWriter.startPartition(key.getKey().decoratedKey, position);
                indexWriter.nextUnfilteredCluster(key.getValue(), position);
                position++;
            }

            PerSSTableIndexWriter.Index index = indexWriter.getIndex(column);

            OnDiskIndex segment = index.scheduleSegmentFlush(false).call();
            index.segments.add(Futures.immediateFuture(segment));
            segments.add(segment.getIndexPath());
        }

        for (String segment : segments)
            Assert.assertTrue(new File(segment).exists());

        String indexFile = indexWriter.indexes.get(column).filename(true);

        // final flush
        indexWriter.complete();

        for (String segment : segments)
            Assert.assertFalse(new File(segment).exists());

        OnDiskIndex index = new OnDiskIndex(new File(indexFile), Int32Type.instance, new FakeKeyFetcher(cfs, keyFormat));

        Assert.assertEquals(0, UTF8Type.instance.compare(index.minKey(), ByteBufferUtil.bytes(String.format(keyFormat, 0))));
        Assert.assertEquals(0, UTF8Type.instance.compare(index.maxKey(), ByteBufferUtil.bytes(String.format(keyFormat, maxKeys - 1))));

        Set<RowKey> actualKeys = new HashSet<>();
        int count = 0;
        for (OnDiskIndex.DataTerm term : index)
        {
            RangeIterator<Long, Token> tokens = term.getTokens();

            while (tokens.hasNext())
            {
                for (RowKey key : tokens.next())
                    actualKeys.add(key);
            }

            Assert.assertEquals(count++, (int) Int32Type.instance.compose(term.getTerm()));
        }

        Assert.assertEquals(expectedKeys.size(), actualKeys.size());
        for (RowKey key : expectedKeys.keySet())
            Assert.assertTrue("Key was not present : " + key, actualKeys.contains(key));

        FileUtils.closeQuietly(index);
    }

    @Test
    public void testSparse() throws Exception
    {
        final String columnName = "timestamp";

        ColumnFamilyStore cfs = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        ColumnDefinition column = cfs.metadata.getColumnDefinition(UTF8Type.instance.decompose(columnName));

        SASIIndex sasi = (SASIIndex) cfs.indexManager.getIndexByName(columnName);

        File directory = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor descriptor = Descriptor.fromFilename(cfs.getSSTablePath(directory));
        PerSSTableIndexWriter indexWriter = (PerSSTableIndexWriter) sasi.getFlushObserver(descriptor, OperationType.FLUSH);

        final long now = System.currentTimeMillis();

        indexWriter.begin();
        indexWriter.indexes.put(column, indexWriter.newIndex(sasi.getIndex()));

        populateSegment(cfs.metadata, indexWriter.getIndex(column), new HashMap<Long, KeyOffsets>()
        {{
            put(now,     new KeyOffsets() {{ put(0, 0); put(1, 1); }});
            put(now + 1, new KeyOffsets() {{ put(2, 2); put(3, 3); }});
            put(now + 2, new KeyOffsets() {{
                put(4, 4); put(5, 5); put(6, 6);
                put(7, 7); put(8, 8); put(9, 9);
            }});
        }});

        Callable<OnDiskIndex> segmentBuilder = indexWriter.getIndex(column).scheduleSegmentFlush(false);

        Assert.assertNull(segmentBuilder.call());

        PerSSTableIndexWriter.Index index = indexWriter.getIndex(column);
        Random random = ThreadLocalRandom.current();

        Supplier<KeyOffsets> offsetSupplier = () -> new KeyOffsets() {{
            put(random.nextInt(), random.nextInt());
            put(random.nextInt(), random.nextInt());
            put(random.nextInt(), random.nextInt());
        }};

        Set<String> segments = new HashSet<>();
        // now let's test multiple correct segments with yield incorrect final segment
        for (int i = 0; i < 3; i++)
        {
            populateSegment(cfs.metadata, index, new HashMap<Long, KeyOffsets>()
            {{
                put(now,     offsetSupplier.get());
                put(now + 1, offsetSupplier.get());
                put(now + 2, offsetSupplier.get());
            }});

            try
            {
                // flush each of the new segments, they should all succeed
                OnDiskIndex segment = index.scheduleSegmentFlush(false).call();
                index.segments.add(Futures.immediateFuture(segment));
                segments.add(segment.getIndexPath());
            }
            catch (Exception | FSError e)
            {
                e.printStackTrace();
                Assert.fail();
            }
        }

        // make sure that all of the segments are present of the filesystem
        for (String segment : segments)
            Assert.assertTrue(new File(segment).exists());

        indexWriter.complete();

        // make sure that individual segments have been cleaned up
        for (String segment : segments)
            Assert.assertFalse(new File(segment).exists());

        // and combined index doesn't exist either
        Assert.assertFalse(new File(index.outputFile).exists());
    }

    private static void populateSegment(CFMetaData metadata, PerSSTableIndexWriter.Index index, Map<Long, KeyOffsets> data)
    {
        for (Map.Entry<Long, KeyOffsets> value : data.entrySet())
        {
            ByteBuffer term = LongType.instance.decompose(value.getKey());
            for (LongObjectCursor<long[]> cursor : value.getValue())
            {
                ByteBuffer key = ByteBufferUtil.bytes(String.format("key%06d", cursor.key));
                for (long rowOffset : cursor.value)
                {
                    index.add(term,
                              metadata.partitioner.decorateKey(key),
                              ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE - 1),
                              ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE - 1));
                }
            }
        }
    }

    private final class FakeKeyFetcher implements KeyFetcher
    {
        private final ColumnFamilyStore cfs;
        private final String keyFormat;

        public FakeKeyFetcher(ColumnFamilyStore cfs, String keyFormat)
        {
            this.cfs = cfs;
            this.keyFormat = keyFormat;
        }

        public DecoratedKey getPartitionKey(long keyPosition)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.format(keyFormat, keyPosition));
            return cfs.metadata.partitioner.decorateKey(key);
        }

        public Clustering getClustering(long offset)
        {
            return Clustering.make(ByteBufferUtil.bytes(offset));
        }

        public RowKey getRowKey(long partitionOffset, long rowOffset)
        {
            return new RowKey(getPartitionKey(partitionOffset), getClustering(rowOffset), CLUSTERING_COMPARATOR);
        }
    }

    public IPartitioner getPartitioner()
    {
        return Murmur3Partitioner.instance;
    }

}
