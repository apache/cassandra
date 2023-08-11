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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;

public class PerSSTableIndexWriterTest extends SchemaLoader
{
    private static final String KS_NAME = "sasi";
    private static final String CF_NAME = "test_cf";

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        CASSANDRA_CONFIG.setString("cassandra-murmur.yaml");
        SchemaLoader.loadSchema();
        SchemaTestUtil.announceNewKeyspace(KeyspaceMetadata.create(KS_NAME,
                                                                   KeyspaceParams.simpleTransient(1),
                                                                   Tables.of(SchemaLoader.sasiCFMD(KS_NAME, CF_NAME).build())));
    }

    @Test
    public void testPartialIndexWrites() throws Exception
    {
        final int maxKeys = 100000, numParts = 4, partSize = maxKeys / numParts;
        final String keyFormat = "key%06d";
        final long timestamp = System.currentTimeMillis();

        ColumnFamilyStore cfs = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        ColumnMetadata column = cfs.metadata().getColumn(UTF8Type.instance.decompose("age"));

        SASIIndex sasi = (SASIIndex) cfs.indexManager.getIndexByName(cfs.name + "_age");

        File directory = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor descriptor = cfs.newSSTableDescriptor(directory);
        PerSSTableIndexWriter indexWriter = (PerSSTableIndexWriter) sasi.getFlushObserver(descriptor, LifecycleTransaction.offline(OperationType.FLUSH));

        SortedMap<DecoratedKey, Row> expectedKeys = new TreeMap<>(DecoratedKey.comparator);

        for (int i = 0; i < maxKeys; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.format(keyFormat, i));
            expectedKeys.put(cfs.metadata().partitioner.decorateKey(key),
                             BTreeRow.singleCellRow(Clustering.EMPTY,
                                                    BufferCell.live(column, timestamp, Int32Type.instance.decompose(i))));
        }

        indexWriter.begin();

        Iterator<Map.Entry<DecoratedKey, Row>> keyIterator = expectedKeys.entrySet().iterator();
        long position = 0;

        Set<String> segments = new HashSet<>();
        outer:
        for (;;)
        {
            for (int i = 0; i < partSize; i++)
            {
                if (!keyIterator.hasNext())
                    break outer;

                Map.Entry<DecoratedKey, Row> key = keyIterator.next();


                indexWriter.startPartition(key.getKey(), position, position);
                position++;
                indexWriter.nextUnfilteredCluster(key.getValue());
            }

            PerSSTableIndexWriter.Index index = indexWriter.getIndex(column);

            OnDiskIndex segment = index.scheduleSegmentFlush(false).call();
            index.segments.add(Futures.immediateFuture(segment));
            segments.add(segment.getIndexPath());
        }

        for (String segment : segments)
            Assert.assertTrue(new File(segment).exists());

        File indexFile = indexWriter.indexes.get(column).file(true);

        // final flush
        indexWriter.complete();

        for (String segment : segments)
            Assert.assertFalse(new File(segment).exists());

        OnDiskIndex index = new OnDiskIndex(indexFile, Int32Type.instance, keyPosition -> {
            ByteBuffer key = ByteBufferUtil.bytes(String.format(keyFormat, keyPosition));
            return cfs.metadata().partitioner.decorateKey(key);
        });

        Assert.assertEquals(0, UTF8Type.instance.compare(index.minKey(), ByteBufferUtil.bytes(String.format(keyFormat, 0))));
        Assert.assertEquals(0, UTF8Type.instance.compare(index.maxKey(), ByteBufferUtil.bytes(String.format(keyFormat, maxKeys - 1))));

        Set<DecoratedKey> actualKeys = new HashSet<>();
        int count = 0;
        for (OnDiskIndex.DataTerm term : index)
        {
            RangeIterator<Long, Token> tokens = term.getTokens();

            while (tokens.hasNext())
            {
                for (DecoratedKey key : tokens.next())
                    actualKeys.add(key);
            }

            Assert.assertEquals(count++, (int) Int32Type.instance.compose(term.getTerm()));
        }

        Assert.assertEquals(expectedKeys.size(), actualKeys.size());
        for (DecoratedKey key : expectedKeys.keySet())
            Assert.assertTrue(actualKeys.contains(key));

        FileUtils.closeQuietly(index);
    }

    @Test
    public void testSparse() throws Exception
    {
        final String columnName = "timestamp";

        ColumnFamilyStore cfs = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        ColumnMetadata column = cfs.metadata().getColumn(UTF8Type.instance.decompose(columnName));

        SASIIndex sasi = (SASIIndex) cfs.indexManager.getIndexByName(cfs.name + "_" + columnName);

        File directory = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor descriptor = cfs.newSSTableDescriptor(directory);
        PerSSTableIndexWriter indexWriter = (PerSSTableIndexWriter) sasi.getFlushObserver(descriptor, LifecycleTransaction.offline(OperationType.FLUSH));

        final long now = System.currentTimeMillis();

        indexWriter.begin();
        indexWriter.indexes.put(column, indexWriter.newIndex(sasi.getIndex()));

        populateSegment(cfs.metadata(), indexWriter.getIndex(column), new HashMap<Long, Set<Integer>>()
        {{
            put(now,     new HashSet<>(Arrays.asList(0, 1)));
            put(now + 1, new HashSet<>(Arrays.asList(2, 3)));
            put(now + 2, new HashSet<>(Arrays.asList(4, 5, 6, 7, 8, 9)));
        }});

        Callable<OnDiskIndex> segmentBuilder = indexWriter.getIndex(column).scheduleSegmentFlush(false);

        Assert.assertNull(segmentBuilder.call());

        PerSSTableIndexWriter.Index index = indexWriter.getIndex(column);
        Random random = ThreadLocalRandom.current();

        Set<String> segments = new HashSet<>();
        // now let's test multiple correct segments with yield incorrect final segment
        for (int i = 0; i < 3; i++)
        {
            populateSegment(cfs.metadata(), index, new HashMap<Long, Set<Integer>>()
            {{
                put(now,     new HashSet<>(Arrays.asList(random.nextInt(), random.nextInt(), random.nextInt())));
                put(now + 1, new HashSet<>(Arrays.asList(random.nextInt(), random.nextInt(), random.nextInt())));
                put(now + 2, new HashSet<>(Arrays.asList(random.nextInt(), random.nextInt(), random.nextInt())));
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
        Assert.assertFalse(index.outputFile.exists());
    }

    private static void populateSegment(TableMetadata metadata, PerSSTableIndexWriter.Index index, Map<Long, Set<Integer>> data)
    {
        for (Map.Entry<Long, Set<Integer>> value : data.entrySet())
        {
            ByteBuffer term = LongType.instance.decompose(value.getKey());
            for (Integer keyPos : value.getValue())
            {
                ByteBuffer key = ByteBufferUtil.bytes(String.format("key%06d", keyPos));
                index.add(term, metadata.partitioner.decorateKey(key), ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE - 1));
            }
        }
    }
}
