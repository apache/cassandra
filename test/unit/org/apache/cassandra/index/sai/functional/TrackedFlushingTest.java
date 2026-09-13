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

package org.apache.cassandra.index.sai.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;

import static org.junit.Assert.assertEquals;

/**
 * SAI on a table whose keyspace is migrated to mutation tracking while it is taking writes. The migration splits the
 * live memtable generation per {@link org.apache.cassandra.db.LogDomain}, so one generation flushes into one sstable
 * per domain, and each output needs an index covering the rows that sstable holds.
 *
 * Every test asserts the unindexed count as well as the indexed one, so a failure says whether the rows or only their
 * index entries were lost.
 */
public class TrackedFlushingTest extends SAITester
{
    @Before
    public void startTracking()
    {
        MutationJournal.start();
        MutationTrackingService.start();
    }

    @After
    public void stopTracking() throws InterruptedException
    {
        MutationTrackingService.shutdown();
    }

    @Test
    public void indexCoversRowsWrittenAfterTheSplit()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        migrateToTracked();
        insert(0, 10);

        assertGenerationSplit();
        assertEquals("precondition: the memtable index answers before the flush", 10, indexedCount());

        flush();

        assertEquals("rows are present after the flush", 10, unindexedCount());
        assertEquals("rows are indexed after the flush", 10, indexedCount());
    }

    @Test
    public void indexCoversRowsWrittenEitherSideOfTheSplit()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        insert(0, 5);
        migrateToTracked();
        insert(5, 10);

        assertGenerationSplit();
        assertEquals("precondition: the memtable index answers before the flush", 10, indexedCount());

        flush();

        assertOneSSTablePerDomain();
        assertEquals("rows are present after the flush", 10, unindexedCount());
        assertEquals("rows are indexed after the flush", 10, indexedCount());
    }

    private void migrateToTracked()
    {
        schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication_type = 'tracked'");
    }

    private void insert(int fromInclusive, int toExclusive)
    {
        for (int i = fromInclusive; i < toExclusive; i++)
            execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", Integer.toString(i), i);
    }

    private int indexedCount()
    {
        return executeNet("SELECT id1 FROM %s WHERE v1 >= 0").all().size();
    }

    private int unindexedCount()
    {
        return executeNet("SELECT id1 FROM %s").all().size();
    }

    private void assertGenerationSplit()
    {
        Memtable current = getCurrentColumnFamilyStore().getTracker().getView().getCurrentMemtable();
        assertEquals("precondition: the generation must hold both log domains", 2, current.flushSources().size());
    }

    /**
     * A journal-derived sstable carries coordinator log offsets and no commit log interval, and a commit-log-derived
     * one carries the reverse. Asserted so that a passing test cannot mean the generation never split.
     */
    private void assertOneSSTablePerDomain()
    {
        int fromCommitLog = 0;
        int fromJournal = 0;
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            if (sstable.getSSTableMetadata().coordinatorLogOffsets.isEmpty())
                fromCommitLog++;
            else
                fromJournal++;
        }
        assertEquals("precondition: one sstable flushed from the commit log", 1, fromCommitLog);
        assertEquals("precondition: one sstable flushed from the journal", 1, fromJournal);
    }
}
