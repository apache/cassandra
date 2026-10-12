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

package org.apache.cassandra.db.compaction;

import java.util.concurrent.atomic.AtomicLong;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.service.CacheService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Key cache migration during compaction looks a key up only in the originals whose key cache is enabled.
 * With caching keys NONE no lookup is made, on either path. BIG format only.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "count key cache lookups",
        targetClass = "org.apache.cassandra.io.sstable.keycache.KeyCache",
        targetMethod = "get",
        targetLocation = "AT ENTRY",
        condition = "org.apache.cassandra.db.compaction.KeyCacheMigrationProbeTest.counting",
        action = "org.apache.cassandra.db.compaction.KeyCacheMigrationProbeTest.lookups.incrementAndGet()")
public class KeyCacheMigrationProbeTest extends CQLTester
{
    public static final AtomicLong lookups = new AtomicLong();
    public static volatile boolean counting;

    private SSTableFormat<?, ?> originalFormat;
    private boolean originalMigrate;
    private boolean originalCursor;
    private long originalKeyCacheCapacity;

    @Before
    public void selectBigAndEnableMigration()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        originalMigrate = DatabaseDescriptor.shouldMigrateKeycacheOnCompaction();
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(true);
        originalCursor = DatabaseDescriptor.cursorCompactionEnabled();
        originalKeyCacheCapacity = CacheService.instance.keyCache.getCapacity();
        if (originalKeyCacheCapacity == 0)
            CacheService.instance.keyCache.setCapacity(1L << 20);
    }

    @After
    public void restore()
    {
        counting = false;
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(originalMigrate);
        DatabaseDescriptor.setCursorCompactionEnabled(originalCursor);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        CacheService.instance.keyCache.setCapacity(originalKeyCacheCapacity);
    }

    @Test
    public void keysNoneMakesNoLookupOnCursorPath() throws Throwable
    {
        assertEquals("a compaction of a table with caching keys NONE looked up the key cache",
                     0, lookupsDuringCompaction("NONE", true));
    }

    @Test
    public void keysNoneMakesNoLookupOnIteratorPath() throws Throwable
    {
        assertEquals("a compaction of a table with caching keys NONE looked up the key cache",
                     0, lookupsDuringCompaction("NONE", false));
    }

    /** The same compaction with keys ALL does look up, or the zero results above would prove nothing. */
    @Test
    public void keysAllLooksUpOnCursorPath() throws Throwable
    {
        assertTrue(lookupsDuringCompaction("ALL", true) > 0);
    }

    @Test
    public void keysAllLooksUpOnIteratorPath() throws Throwable
    {
        assertTrue(lookupsDuringCompaction("ALL", false) > 0);
    }

    private long lookupsDuringCompaction(String keys, boolean cursor) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v int) WITH caching = {'keys': '" + keys + "'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 100; pk++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, round);
            flush();
        }
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        lookups.set(0);
        counting = true;
        try
        {
            cfs.forceMajorCompaction();
        }
        finally
        {
            counting = false;
        }
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
        return lookups.get();
    }
}
