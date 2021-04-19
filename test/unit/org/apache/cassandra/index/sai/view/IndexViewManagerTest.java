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
package org.apache.cassandra.index.sai.view;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class IndexViewManagerTest extends SAITester
{
    private static final int CONCURRENT_UPDATES = 100;

    @BeforeClass
    public static void setupVersionBarrier()
    {
        requireNetwork();
    }

    @Test
    public void testUpdateFromFlush() throws Throwable
    {
        createTable("CREATE TABLE %S (k INT PRIMARY KEY, v INT)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        ColumnContext columnContext = columnIndex(getCurrentColumnFamilyStore(), indexName);
        View initialView = columnContext.getView();

        execute("INSERT INTO %s(k, v) VALUES (1, 10)");
        execute("INSERT INTO %s(k, v) VALUES (2, 20)");
        flush();

        View updatedView = columnContext.getView();
        assertNotEquals(initialView, updatedView);
        assertEquals(1, updatedView.getIndexes().size());
    }

    @Test
    public void testUpdateFromCompaction() throws Throwable
    {
        createTable("CREATE TABLE %S (k INT PRIMARY KEY, v INT)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        ColumnContext columnContext = columnIndex(store, indexName);
        store.disableAutoCompaction();

        execute("INSERT INTO %s(k, v) VALUES (1, 10)");
        execute("INSERT INTO %s(k, v) VALUES (2, 20)");
        flush();

        execute("INSERT INTO %s(k, v) VALUES (3, 30)");
        execute("INSERT INTO %s(k, v) VALUES (4, 40)");
        flush();

        View initialView = columnContext.getView();
        assertEquals(2, initialView.getIndexes().size());

        CompactionManager.instance.performMaximal(store, false);

        View updatedView = columnContext.getView();
        assertNotEquals(initialView, updatedView);
        assertEquals(1, updatedView.getIndexes().size());
    }

    /**
     * Tests concurrent sstable updates from flush and compaction, see CASSANDRA-14207.
     */
    @Test
    public void testConcurrentUpdate() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %S (k INT PRIMARY KEY, v INT)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        ColumnContext columnContext = columnIndex(store, indexName);
        Path tmpDir = Files.createTempDirectory("IndexViewManagerTest");
        store.disableAutoCompaction();

        List<Descriptor> descriptors = new ArrayList<>();
        // create sstable 1 from flush
        execute("INSERT INTO %s(k, v) VALUES (1, 10)");
        execute("INSERT INTO %s(k, v) VALUES (2, 20)");
        flush();

        // create sstable 2 from flush
        execute("INSERT INTO %s(k, v) VALUES (3, 30)");
        execute("INSERT INTO %s(k, v) VALUES (4, 40)");
        flush();

        // save sstables 1 and 2 and create sstable 3 from compaction
        assertEquals(2, store.getLiveSSTables().size());
        store.getLiveSSTables().forEach(reader -> copySSTable(reader, tmpDir));
        getCurrentColumnFamilyStore().getLiveSSTables().stream().map(t -> t.descriptor).forEach(descriptors::add);
        CompactionManager.instance.performMaximal(store, false);

        // create sstable 4 from flush
        execute("INSERT INTO %s(k, v) VALUES (5, 50)");
        execute("INSERT INTO %s(k, v) VALUES (6, 60)");
        flush();

        // save sstables 3 and 4
        store.getLiveSSTables().forEach(reader -> copySSTable(reader, tmpDir));
        getCurrentColumnFamilyStore().getLiveSSTables().stream().map(t -> t.descriptor).forEach(descriptors::add);

        List<SSTableReader> sstables = descriptors.stream()
                                                .map(desc -> new Descriptor(tmpDir.toFile(), KEYSPACE, tableName, desc.id))
                                                .map(desc -> desc.getFormat().getReaderFactory().open(desc))
                                                .collect(Collectors.toList());
        assertThat(sstables).hasSize(4);

        List<SSTableReader> none = Collections.emptyList();
        List<SSTableReader> initial = sstables.stream().limit(2).collect(Collectors.toList());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        for (int i = 0; i < CONCURRENT_UPDATES; i++)
        {
            // mock the initial view indexes to track the number of releases
            List<SSTableContext> initialContexts = sstables.stream().limit(2).map(SSTableContext::create).collect(Collectors.toList());
            List<SSTableIndex> initialIndexes = new ArrayList<>();

            for (SSTableContext initialContext : initialContexts)
            {
                MockSSTableIndex mockSSTableIndex = new MockSSTableIndex(initialContext, columnContext);
                initialIndexes.add(mockSSTableIndex);
            }

            IndexViewManager tracker = new IndexViewManager(columnContext, initialIndexes);
            View initialView = tracker.getView();
            assertEquals(2, initialView.size());

            List<SSTableContext> compacted = sstables.stream().skip(2).limit(1).map(SSTableContext::create).collect(Collectors.toList());
            List<SSTableContext> flushed = sstables.stream().skip(3).limit(1).map(SSTableContext::create).collect(Collectors.toList());

            // concurrently update from both flush and compaction
            Future<?> compaction = executor.submit(() -> tracker.update(initial, compacted, true, false));
            Future<?> flush = executor.submit(() -> tracker.update(none, flushed, true, false));

            FBUtilities.waitOnFutures(Arrays.asList(compaction, flush));

            View updatedView = tracker.getView();
            assertNotEquals(initialView, updatedView);
            assertEquals(2, updatedView.getIndexes().size());

            for (SSTableIndex index : initialIndexes)
            {
                assertEquals(1, ((MockSSTableIndex) index).releaseCount);
            }

            // release original SSTableContext objects.
            // shared copies are already released when compacted and flushed are added.
            initialContexts.forEach(SSTableContext::close);
            initialContexts.forEach(group -> assertTrue(group.isCleanedUp()));

            // release compacted and flushed SSTableContext original and shared copies
            compacted.forEach(SSTableContext::close);
            flushed.forEach(SSTableContext::close);
            tracker.getView().getIndexes().forEach(SSTableIndex::release);
            compacted.forEach(group -> assertTrue(group.isCleanedUp()));
            flushed.forEach(group -> assertTrue(group.isCleanedUp()));
        }
        sstables.forEach(sstable -> sstable.selfRef().release());
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    private ColumnContext columnIndex(ColumnFamilyStore store, String indexName)
    {
        assert store.indexManager != null;
        StorageAttachedIndex sai = (StorageAttachedIndex) store.indexManager.getIndexByName(indexName);
        return sai.getContext();
    }

    public static class MockSSTableIndex extends SSTableIndex
    {
        int releaseCount = 0;

        MockSSTableIndex(SSTableContext group, ColumnContext context) throws IOException
        {
            super(group, context, IndexComponents.create(context.getIndexName(), group.descriptor(), CryptoUtils.getCompressionParams(group.sstable())));
        }

        @Override
        public void release()
        {
            super.release();
            releaseCount++;
        }
    }

    private static void copySSTable(SSTableReader table, Path destDir)
    {
        for (Component component : SSTable.componentsFor(table.descriptor))
        {
            Path src = table.descriptor.fileFor(component).toPath();
            Path dst = destDir.resolve(src.getFileName());
            try
            {
                Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
