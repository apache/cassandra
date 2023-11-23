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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Transactional.AbstractTransactional.State;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SSTableFlushObserverTest
{

    private TableMetadata cfm;
    private File directory;
    private SSTableFormat<?, ?> sstableFormat;
    private static Supplier<SSTableId> idGen;

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        idGen = SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty());
    }

    private static final String KS_NAME = "test";
    private static final String CF_NAME = "flush_observer";

    @Before
    public void setUp() throws Exception
    {
        cfm = TableMetadata.builder(KS_NAME, CF_NAME)
                     .addPartitionKeyColumn("id", UTF8Type.instance)
                     .addRegularColumn("first_name", UTF8Type.instance)
                     .addRegularColumn("age", Int32Type.instance)
                     .addRegularColumn("height", LongType.instance)
                     .build();
        String sstableDirectory = DatabaseDescriptor.getAllDataFileLocations()[0];
        directory = new File(sstableDirectory + File.pathSeparator() + KS_NAME + File.pathSeparator() + CF_NAME);
        directory.deleteOnExit();

        if (!directory.exists() && !directory.tryCreateDirectories())
            throw new FSWriteError(new IOException("failed to create tmp directory"), directory.absolutePath());

        sstableFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @Test
    public void testFlushObserver() throws Exception
    {
        try (LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.COMPACTION))
        {
            FlushObserver observer = new FlushObserver();

            Descriptor descriptor = new Descriptor(sstableFormat.getLatestVersion(),
                                                   directory,
                                                   cfm.keyspace,
                                                   cfm.name,
                                                   idGen.get());
            Index.Group indexGroup = mock(Index.Group.class);
            when(indexGroup.getFlushObserver(any(), any(), any())).thenReturn(observer);
            SSTableWriter.Builder<?, ?> writerBuilder = descriptor.getFormat().getWriterFactory().builder(descriptor)
                                                                  .setKeyCount(10)
                                                                  .setTableMetadataRef(TableMetadataRef.forOfflineTools(cfm))
                                                                  .setMetadataCollector(new MetadataCollector(cfm.comparator).sstableLevel(0))
                                                                  .setSerializationHeader(new SerializationHeader(true, cfm, cfm.regularAndStaticColumns(), EncodingStats.NO_STATS))
                                                                  .setSecondaryIndexGroups(Collections.singleton(indexGroup))
                                                                  .addDefaultComponents(Collections.emptySet());
            assertThat(observer.beginCalled).isFalse();
            assertThat(observer.isComplete).isFalse();

            SSTableWriter writer = writerBuilder.build(transaction, null);

            assertThat(observer.beginCalled).isTrue();
            assertThat(observer.isComplete).isFalse();

            SSTableReader reader;
            Multimap<ByteBuffer, Cell<?>> expected = ArrayListMultimap.create();

            try
            {
                final long now = System.currentTimeMillis();

                ByteBuffer key = UTF8Type.instance.fromString("key1");
                expected.putAll(key, Arrays.asList(BufferCell.live(getColumn(cfm, "age"), now, Int32Type.instance.decompose(27)),
                                                   BufferCell.live(getColumn(cfm, "first_name"), now, UTF8Type.instance.fromString("jack")),
                                                   BufferCell.live(getColumn(cfm, "height"), now, LongType.instance.decompose(183L))));

                writer.append(new RowIterator(cfm, key.duplicate(), Collections.singletonList(buildRow(expected.get(key)))));

                key = UTF8Type.instance.fromString("key2");
                expected.putAll(key, Arrays.asList(BufferCell.live(getColumn(cfm, "age"), now, Int32Type.instance.decompose(30)),
                                                   BufferCell.live(getColumn(cfm, "first_name"), now, UTF8Type.instance.fromString("jim")),
                                                   BufferCell.live(getColumn(cfm, "height"), now, LongType.instance.decompose(180L))));

                writer.append(new RowIterator(cfm, key, Collections.singletonList(buildRow(expected.get(key)))));

                key = UTF8Type.instance.fromString("key3");
                expected.putAll(key, Arrays.asList(BufferCell.live(getColumn(cfm, "age"), now, Int32Type.instance.decompose(30)),
                                                   BufferCell.live(getColumn(cfm, "first_name"), now, UTF8Type.instance.fromString("ken")),
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

            for (Triple<ByteBuffer, Long, Long> e : observer.rows.keySet())
            {
                ByteBuffer key = e.getLeft();
                long indexPosition = e.getRight();

                DecoratedKey indexKey = reader.keyAtPositionFromSecondaryIndex(indexPosition);
                Assert.assertEquals(0, UTF8Type.instance.compare(key, indexKey.getKey()));
                Assert.assertEquals(expected.get(key), observer.rows.get(e));
            }
        }
    }

    @Test
    public void testFailedInitialization() throws Exception
    {
        try (LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.COMPACTION))
        {
            FlushObserver observer1 = new FlushObserver();
            FlushObserver observer2 = new FlushObserver();
            Index.Group indexGroup1 = mock(Index.Group.class);
            Index.Group indexGroup2 = mock(Index.Group.class);
            when(indexGroup1.getFlushObserver(any(), any(), any())).thenReturn(observer1);
            when(indexGroup2.getFlushObserver(any(), any(), any())).thenReturn(observer2);
            observer2.failOnBegin = true;

            Descriptor descriptor = new Descriptor(sstableFormat.getLatestVersion(),
                                                   directory,
                                                   cfm.keyspace,
                                                   cfm.name,
                                                   idGen.get());
            assertThatRuntimeException().isThrownBy(() -> descriptor.getFormat().getWriterFactory().builder(descriptor)
                                                                    .setKeyCount(10)
                                                                    .setTableMetadataRef(TableMetadataRef.forOfflineTools(cfm))
                                                                    .setMetadataCollector(new MetadataCollector(cfm.comparator).sstableLevel(0))
                                                                    .setSerializationHeader(new SerializationHeader(true, cfm, cfm.regularAndStaticColumns(), EncodingStats.NO_STATS))
                                                                    .setSecondaryIndexGroups(List.of(indexGroup1, indexGroup2))
                                                                    .addDefaultComponents(Collections.emptySet())
                                                                    .build(transaction, null)
            ).withMessage("Failed to initialize");

            assertThat(observer1.beginCalled).isTrue();
            assertThat(observer1.isComplete).isFalse();
            assertThat(observer1.abortCalled).isTrue();

            assertThat(observer2.beginCalled).isTrue();
            assertThat(observer2.isComplete).isFalse();
            assertThat(observer2.abortCalled).isFalse();

            assertThat(transaction.state()).isEqualTo(State.IN_PROGRESS);
            assertThat(transaction.originals()).isEmpty();
        }
    }

    private static class RowIterator extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Unfiltered> rows;

        public RowIterator(TableMetadata cfm, ByteBuffer key, Collection<Unfiltered> content)
        {
            super(cfm,
                  DatabaseDescriptor.getPartitioner().decorateKey(key),
                  DeletionTime.LIVE,
                  cfm.regularAndStaticColumns(),
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
        private final Multimap<Triple<ByteBuffer, Long, Long>, Cell<?>> rows = ArrayListMultimap.create();
        private final Multimap<Triple<ByteBuffer, Long, Long>, Cell<?>> staticRows = ArrayListMultimap.create();

        private Triple<ByteBuffer, Long, Long> currentKey;
        private boolean isComplete;
        private boolean beginCalled;
        private boolean failOnBegin;
        private boolean abortCalled;

        @Override
        public void begin()
        {
            beginCalled = true;
            if (failOnBegin)
                throw new RuntimeException("Failed to initialize");
        }

        @Override
        public void startPartition(DecoratedKey key, long dataPosition, long indexPosition)
        {
            currentKey = ImmutableTriple.of(key.getKey(), dataPosition, indexPosition);
        }

        @Override
        public void nextUnfilteredCluster(Unfiltered row)
        {
            if (row.isRow())
                ((Row) row).forEach((c) -> rows.put(currentKey, (Cell<?>) c));
        }

        @Override
        public void complete()
        {
            isComplete = true;
        }

        @Override
        public void staticRow(Row staticRow)
        {
            staticRow.forEach((c) -> staticRows.put(currentKey, (Cell<?>) c));
        }

        @Override
        public void abort(Throwable accumulate)
        {
            abortCalled = true;
        }
    }

    private static Row buildRow(Collection<Cell<?>> cells)
    {
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(Clustering.EMPTY);
        cells.forEach(rowBuilder::addCell);
        return rowBuilder.build();
    }

    private static ColumnMetadata getColumn(TableMetadata cfm, String name)
    {
        return cfm.getColumn(UTF8Type.instance.fromString(name));
    }
}
