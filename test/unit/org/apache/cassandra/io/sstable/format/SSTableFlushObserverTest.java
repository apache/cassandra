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
package org.apache.cassandra.io.sstable.format;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import junit.framework.Assert;
import org.junit.Test;

public class SSTableFlushObserverTest
{
    private static final String KS_NAME = "test";
    private static final String CF_NAME = "flush_observer";

    @Test
    public void testFlushObserver()
    {
        CFMetaData cfm = CFMetaData.Builder.create(KS_NAME, CF_NAME)
                                           .addPartitionKey("id", UTF8Type.instance)
                                           .addRegularColumn("first_name", UTF8Type.instance)
                                           .addRegularColumn("age", Int32Type.instance)
                                           .addRegularColumn("height", LongType.instance)
                                           .build();

        LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.COMPACTION);
        FlushObserver observer = new FlushObserver();

        String sstableDirectory = DatabaseDescriptor.getAllDataFileLocations()[0];
        File directory = new File(sstableDirectory + File.pathSeparator + KS_NAME + File.pathSeparator + CF_NAME);
        directory.deleteOnExit();

        if (!directory.exists() && !directory.mkdirs())
            throw new FSWriteError(new IOException("failed to create tmp directory"), directory.getAbsolutePath());

        SSTableFormat.Type sstableFormat = DatabaseDescriptor.getSSTableFormat();

        BigTableWriter writer = new BigTableWriter(new Descriptor(sstableFormat.info.getLatestVersion().version,
                                                                  directory,
                                                                  KS_NAME, CF_NAME,
                                                                  0,
                                                                  sstableFormat),
                                                   10L, 0L, cfm,
                                                   new MetadataCollector(cfm.comparator).sstableLevel(0),
                                                   new SerializationHeader(true, cfm, cfm.partitionColumns(), EncodingStats.NO_STATS),
                                                   Collections.singletonList(observer),
                                                   transaction);

        SSTableReader reader = null;
        Multimap<ByteBuffer, Cell> expected = ArrayListMultimap.create();

        try
        {
            final long now = System.currentTimeMillis();

            ByteBuffer key = UTF8Type.instance.fromString("key1");
            expected.putAll(key, Arrays.asList(BufferCell.live(getColumn(cfm, "first_name"), now,UTF8Type.instance.fromString("jack")),
                                               BufferCell.live(getColumn(cfm, "age"), now, Int32Type.instance.decompose(27)),
                                               BufferCell.live(getColumn(cfm, "height"), now, LongType.instance.decompose(183L))));

            writer.append(new RowIterator(cfm, key.duplicate(), Collections.singletonList(buildRow(expected.get(key)))));

            key = UTF8Type.instance.fromString("key2");
            expected.putAll(key, Arrays.asList(BufferCell.live(getColumn(cfm, "first_name"), now,UTF8Type.instance.fromString("jim")),
                                               BufferCell.live(getColumn(cfm, "age"), now, Int32Type.instance.decompose(30)),
                                               BufferCell.live(getColumn(cfm, "height"), now, LongType.instance.decompose(180L))));

            writer.append(new RowIterator(cfm, key, Collections.singletonList(buildRow(expected.get(key)))));

            key = UTF8Type.instance.fromString("key3");
            expected.putAll(key, Arrays.asList(BufferCell.live(getColumn(cfm, "first_name"), now,UTF8Type.instance.fromString("ken")),
                                               BufferCell.live(getColumn(cfm, "age"), now, Int32Type.instance.decompose(30)),
                                               BufferCell.live(getColumn(cfm, "height"), now, LongType.instance.decompose(178L))));

            writer.append(new RowIterator(cfm, key, Collections.singletonList(buildRow(expected.get(key)))));

            reader = writer.finish(true);
        }
        finally
        {
            FileUtils.closeQuietly(writer);
        }

        Assert.assertTrue(observer.isComplete);
        Assert.assertEquals(expected.size(), observer.rows.size());

        for (Pair<ByteBuffer, Long> e : observer.rows.keySet())
        {
            ByteBuffer key = e.left;
            Long indexPosition = e.right;

            try (FileDataInput index = reader.ifile.createReader(indexPosition))
            {
                ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(index);
                Assert.assertEquals(0, UTF8Type.instance.compare(key, indexKey));
            }
            catch (IOException ex)
            {
                throw new FSReadError(ex, reader.getIndexFilename());
            }

            Assert.assertEquals(expected.get(key), observer.rows.get(e));
        }
    }

    private static class RowIterator extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Unfiltered> rows;

        public RowIterator(CFMetaData cfm, ByteBuffer key, Collection<Unfiltered> content)
        {
            super(cfm,
                  DatabaseDescriptor.getPartitioner().decorateKey(key),
                  DeletionTime.LIVE,
                  cfm.partitionColumns(),
                  BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING),
                  false,
                  EncodingStats.NO_STATS);

            rows = content.iterator();
        }

        @Override
        protected Unfiltered computeNext()
        {
            return rows.hasNext() ? rows.next() : endOfData();
        }
    }

    private static class FlushObserver implements SSTableFlushObserver
    {
        private final Multimap<Pair<ByteBuffer, Long>, Cell> rows = ArrayListMultimap.create();
        private Pair<ByteBuffer, Long> currentKey;
        private boolean isComplete;

        @Override
        public void begin()
        {}

        @Override
        public void startPartition(DecoratedKey key, long indexPosition)
        {
            currentKey = Pair.create(key.getKey(), indexPosition);
        }

        @Override
        public void nextUnfilteredCluster(Unfiltered row)
        {
            if (row.isRow())
                ((Row) row).forEach((c) -> rows.put(currentKey, (Cell) c));
        }

        @Override
        public void complete()
        {
            isComplete = true;
        }
    }

    private static Row buildRow(Collection<Cell> cells)
    {
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(Clustering.EMPTY);
        cells.forEach(rowBuilder::addCell);
        return rowBuilder.build();
    }

    private static ColumnDefinition getColumn(CFMetaData cfm, String name)
    {
        return cfm.getColumnDefinition(UTF8Type.instance.fromString(name));
    }
}
