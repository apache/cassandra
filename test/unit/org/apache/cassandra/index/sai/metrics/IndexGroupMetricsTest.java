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
package org.apache.cassandra.index.sai.metrics;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SSTableContextManager;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class IndexGroupMetricsTest extends AbstractMetricsTest
{
    @Before
    public void setup() throws Exception
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();
    }

    @Test
    public void verifyIndexGroupMetrics() throws Throwable
    {
        // create first index
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        // no open files
        assertEquals(0, getOpenIndexFiles());
        assertEquals(0, getDiskUsage());

        int sstables = 10;
        for (int i = 0; i < sstables; i++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
            flush();
        }

        // with 10 sstable
        int indexopenFileCountWithOnlyNumeric = getOpenIndexFiles();
        assertEquals(sstables * (Version.LATEST.onDiskFormat().openFilesPerSSTableIndex(false) +
                                 Version.LATEST.onDiskFormat().openFilesPerColumnIndex()),
                     indexopenFileCountWithOnlyNumeric);

        long diskUsageWithOnlyNumeric = getDiskUsage();
        assertNotEquals(0, diskUsageWithOnlyNumeric);

        // compaction should reduce open files
        compact();

        assertEquals(Version.LATEST.onDiskFormat().openFilesPerSSTableIndex(false) +
                     Version.LATEST.onDiskFormat().openFilesPerColumnIndex(),
                     getOpenIndexFiles());

        // drop last index, no open index files
        dropIndex("DROP INDEX %s." + v1IndexName);
        assertNull(getCurrentIndexGroup());
    }

    /**
     * {@code UnindexedSSTables} counts live sstables with no per-sstable index components, which are absent from every
     * index view: their rows are readable but match no index predicate, and nothing throws or logs on the read path.
     * This covers the whole of an index build over a pre-existing sstable, which is the window in which that is the
     * expected state.
     */
    @Test
    public void unindexedSSTablesGaugeIsNonZeroWhileAnSSTableHasNoIndexComponents() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        // An sstable that exists before any index does, so the build has to visit it. Until it does, the sstable is
        // live with no index components at all.
        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
        flush();
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());

        Injections.Barrier pauseBeforeWritingComponents = pauseInIndexBuild("pauseBeforeWritingComponents");
        Injections.inject(pauseBeforeWritingComponents);

        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        // The build has read the sstable and is about to write its components, so this is mid-window by construction
        // rather than by timing.
        waitForAssert(() -> assertEquals(1, pauseBeforeWritingComponents.getCount()));
        assertEquals("an sstable with no index components must be counted for the whole of the build",
                     1, getUnindexedSSTables());

        pauseBeforeWritingComponents.countDown();
        waitForTableIndexesQueryable();

        waitForAssert(() -> assertEquals("the gauge must clear once the components have been written",
                                         0, getUnindexedSSTables()));
    }

    /**
     * A full rebuild deletes the per-sstable components of an sstable that stays live and only rewrites them at the
     * end, so for the whole of the rebuild -- minutes or hours for a large sstable, and forever if the rebuild dies --
     * that sstable answers no index predicate. Releasing the record at the start of that window instead of keeping it
     * left the gauge reading 0 for exactly as long as query results were incomplete, which is the one time it needed
     * to read anything else.
     */
    @Test
    public void unindexedSSTablesGaugeStaysNonZeroForTheWholeOfARebuild() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        IndexIdentifier index = createIndexIdentifier(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));

        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
        flush();
        waitForTableIndexesQueryable();
        assertEquals("a fully built index must report nothing unindexed", 0, getUnindexedSSTables());

        Injections.Barrier pauseMidRebuild = pauseInIndexBuild("pauseMidRebuild");
        Injections.inject(pauseMidRebuild);

        // rebuildIndexes blocks until the build finishes, so the rebuild gets its own thread and this one observes.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> rebuild = executor.submit(() -> rebuildIndexes(index.indexName));

            waitForAssert(() -> assertEquals(1, pauseMidRebuild.getCount()));
            assertEquals("the components are gone and not yet rewritten, so the gauge must say so",
                         1, getUnindexedSSTables());

            pauseMidRebuild.countDown();
            rebuild.get(1, TimeUnit.MINUTES);
        }
        finally
        {
            executor.shutdownNow();
        }

        waitForAssert(() -> assertEquals("the gauge must clear once the rebuild has rewritten the components",
                                         0, getUnindexedSSTables()));
    }

    /**
     * Two properties of the record itself.
     * <p>
     * {@code clear()} drops it along with the contexts, because the readers it names are no longer tracked: leaving
     * them behind would over-report, and would let a released sstable's descriptor turn up in the warning.
     * <p>
     * {@code incompleteSSTables()} hands back a copy. {@code warnOnUnindexedSSTables} tests the set and then reads an
     * example descriptor out of it, while {@code release} empties it from paths holding no lock that warning takes --
     * so over a live view that pair is {@code isEmpty()} followed by {@code iterator().next()} on an empty iterator,
     * i.e. a {@link java.util.NoSuchElementException} out of a notification handler, which fails the flush that sent
     * the notification.
     */
    @Test
    public void unindexedSSTablesRecordIsClearedWithTheContextsAndHandedOutAsASnapshot() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
        flush();
        waitForTableIndexesQueryable();
        assertEquals(0, getUnindexedSSTables());

        // What a build that died leaves behind: the completion marker missing, which is the only thing
        // SSTableContextManager.update tests, so the reload leaves this sstable out of the index view.
        corruptIndexComponent(IndexComponent.GROUP_COMPLETION_MARKER, CorruptionType.REMOVED);
        reloadSSTableIndex();
        assertEquals("an sstable with no completion marker is absent from every index view",
                     1, getUnindexedSSTables());

        SSTableContextManager contextManager = getCurrentIndexGroup().sstableContextManager();
        Set<SSTableReader> snapshot = incompleteSSTables(contextManager);
        assertEquals(1, snapshot.size());

        contextManager.clear();

        assertEquals("clear() must forget the unindexed records along with the contexts", 0, getUnindexedSSTables());
        assertEquals("incompleteSSTables() must return a copy, or a caller that tests it and then reads from it races" +
                     " with removal", 1, snapshot.size());
    }

    /**
     * An unindexed sstable that leaves the live set stops being one. Otherwise the gauge would be a standing false
     * alarm for a table that has long since compacted the sstable away -- and the warning would go on naming a
     * descriptor whose reader has been released.
     */
    @Test
    public void unindexedSSTablesGaugeDropsAnSSTableThatIsCompactedAway() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
        flush();
        waitForTableIndexesQueryable();

        corruptIndexComponent(IndexComponent.GROUP_COMPLETION_MARKER, CorruptionType.REMOVED);
        reloadSSTableIndex();
        assertEquals(1, getUnindexedSSTables());

        // A second, fully indexed sstable, so the compaction below has something to merge and the count has to come
        // from the first one rather than from there being no sstables at all.
        execute("INSERT INTO %s (id1, v1) VALUES ('1', 1)");
        flush();
        assertEquals("an sstable that does have its components must not be counted", 1, getUnindexedSSTables());

        compact();

        waitForAssert(() -> assertEquals("a compacted-away sstable must stop being counted", 0, getUnindexedSSTables()));
    }

    private static Injections.Barrier pauseInIndexBuild(String name)
    {
        // At the entry of completeSSTable: the per-sstable components have been deleted (by shouldWritePerSSTableFiles,
        // for an initial build as much as a rebuild) and are not written until indexWriter.complete() inside it.
        return Injections.newBarrier(name, 2, false)
                         .add(InvokePointBuilder.newInvokePoint()
                                                .onClass(StorageAttachedIndexBuilder.class)
                                                .onMethod("completeSSTable"))
                         .build();
    }

    /**
     * {@code SSTableContextManager#incompleteSSTables} is package-private and this test is not in that package. Making
     * it {@code @VisibleForTesting public} would let this go.
     */
    @SuppressWarnings("unchecked")
    private static Set<SSTableReader> incompleteSSTables(SSTableContextManager contextManager) throws Exception
    {
        Method method = SSTableContextManager.class.getDeclaredMethod("incompleteSSTables");
        method.setAccessible(true);
        return (Set<SSTableReader>) method.invoke(contextManager);
    }

    protected int getOpenIndexFiles()
    {
        return (int) getMetricValue(objectNameNoIndex("OpenIndexFiles", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }

    protected long getDiskUsage()
    {
        return (long) getMetricValue(objectNameNoIndex("DiskUsedBytes", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }

    protected int getUnindexedSSTables()
    {
        return (int) getMetricValue(objectNameNoIndex("UnindexedSSTables", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }
}
