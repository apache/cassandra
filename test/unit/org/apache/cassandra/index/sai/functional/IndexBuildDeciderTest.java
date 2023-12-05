/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.functional;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.ReadFailureException;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.IndexBuildDecider;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContextManager;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.v1.MemtableIndexWriter;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.awaitility.Awaitility;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexBuildDeciderTest extends SAITester
{
    static final Injections.Counter flushWithMemtableIndexWriterCount =
    Injections.newCounter("flushWithMemtableIndexWriterCount")
              .add(newInvokePoint().onClass(MemtableIndexWriter.class).onMethod("<init>"))
              .build();

    @BeforeClass
    public static void init()
    {
        System.setProperty("cassandra.custom_index_build_decider", IndexBuildDeciderWithoutInitialBuild.class.getName());
    }

    @AfterClass
    public static void teardown()
    {
        System.clearProperty("cassandra.custom_index_build_decider");
    }

    @Before
    public void before() throws Throwable
    {
        Injections.inject(flushWithMemtableIndexWriterCount);
    }

    @After
    public void after()
    {
        flushWithMemtableIndexWriterCount.reset();
        Injections.deleteAll();
    }

    @Test
    public void testNoInitialBuildWithSAI() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        // populate one sstable
        int rowCount = 10;
        for (int j = 0; j < rowCount / 2; j++)
            execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", String.valueOf(j), j);
        flush();

        SSTableReader initialSSTable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        int initialSSTableFileCount = sstableFileCount(initialSSTable);

        // populate memtable
        for (int j = rowCount / 2; j < rowCount; j++)
            execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", String.valueOf(j), j);

        // create index: it's not queryable because IndexBuildDeciderWithoutInitialBuild skipped the initial build and
        // didn't consider the index queryable because there was already one sstable
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        Awaitility.await("Index is not queryable")
                  .pollDelay(5, TimeUnit.SECONDS)
                  .until(() -> !isIndexQueryable());
        assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE v1>=0")).isInstanceOf(ReadFailureException.class);

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        SSTableContextManager sstableContext = group.sstableContextManager();

        // given there was no index build, the initial sstable has no index files
        assertEquals(initialSSTableFileCount, sstableFileCount(initialSSTable));
        assertFalse(sstableContext.contains(initialSSTable));

        // creating the index caused the memtable to be flushed: any existing memtables before the index is created are
        // flushed with the SSTableIndexWriter
        assertEquals(2, getCurrentColumnFamilyStore().getLiveSSTables().size());
        assertEquals(0, flushWithMemtableIndexWriterCount.get());

        // check the second sstable flushed at index creation is now indexed:
        SSTableReader secondSSTable = getCurrentColumnFamilyStore().getLiveSSTables().stream().filter(s -> s != initialSSTable).findFirst().get();
        assertEquals(initialSSTableFileCount + numericIndexFileCount(), sstableFileCount(secondSSTable));
        assertTrue(sstableContext.contains(secondSSTable));

        // SAI#canFlushFromMemtableIndex should be true
        StorageAttachedIndex sai = (StorageAttachedIndex) group.getIndexes().iterator().next();
        assertTrue(sai.canFlushFromMemtableIndex());

        // flush another memtable: it should be flushed with MemtableIndexWriter
        execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", String.valueOf(0), 0);
        flush();
        assertEquals(1, flushWithMemtableIndexWriterCount.get());
        SSTableReader thirdSStable = getCurrentColumnFamilyStore().getLiveSSTables().stream().filter(s -> s != initialSSTable && s != secondSSTable).findFirst().get();
        assertEquals(initialSSTableFileCount + numericIndexFileCount(), sstableFileCount(thirdSStable));
        assertTrue(sstableContext.contains(thirdSStable));
    }

    private int sstableFileCount(SSTableReader secondSSTable)
    {
        Path sstableDir = secondSSTable.descriptor.directory.toPath();
        String prefix = sstableDir + "/" + secondSSTable.descriptor.filenamePart();
        return FileUtils.listPaths(sstableDir, path -> path.toString().startsWith(prefix)).size();
    }

    private int numericIndexFileCount()
    {
        IndexContext context = createIndexContext("v1", Int32Type.instance);
        return V2OnDiskFormat.instance.perIndexComponents(context).size()
               + V2OnDiskFormat.instance.perSSTableComponents().size();
    }

    public static class IndexBuildDeciderWithoutInitialBuild implements IndexBuildDecider
    {
        @Override
        public Decision onInitialBuild()
        {
            return Decision.NONE;
        }

        @Override
        public boolean isIndexQueryableAfterInitialBuild(ColumnFamilyStore cfs)
        {
            return cfs.getLiveSSTables().isEmpty();
        }
    }
}
