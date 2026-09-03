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

    /**
     * The vector path writes its index through {@code MemtableIndex.writeDirect} rather than a row-mapping merge, and
     * pre-creates the index on switch, so it reaches the flush by a different route than a literal index.
     */
    @Test
    public void vectorIndexCoversRowsWrittenAfterTheSplit()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'sai'");

        migrateToTracked();
        insertVectors(0, 10);

        assertGenerationSplit();
        assertEquals("precondition: the memtable index answers before the flush", 10, annCount());

        flush();

        assertEquals("rows are present after the flush", 10, unindexedCount());
        assertEquals("rows are indexed after the flush", 10, annCount());
    }

    @Test
    public void vectorIndexCoversRowsWrittenEitherSideOfTheSplit()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'sai'");

        insertVectors(0, 5);
        migrateToTracked();
        insertVectors(5, 10);

        assertGenerationSplit();
        assertEquals("precondition: the memtable index answers before the flush", 10, annCount());

        flush();

        assertOneSSTablePerDomain();
        assertEquals("rows are present after the flush", 10, unindexedCount());
        assertEquals("rows are indexed after the flush", 10, annCount());
    }

    /**
     * Overwriting a row already resident in the same source reaches {@code MemtableIndexManager.update}, which asserts
     * for a vector index that the source already has an index.
     */
    @Test
    public void vectorIndexSurvivesOverwriteInASplitGeneration()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'sai'");

        insertVectors(0, 5);
        migrateToTracked();
        insertVectors(5, 10);
        insertVectors(5, 10);

        assertGenerationSplit();

        flush();

        assertEquals("rows are present after the flush", 10, unindexedCount());
        assertEquals("rows are indexed after the flush", 10, annCount());
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

    private void insertVectors(int fromInclusive, int toExclusive)
    {
        for (int i = fromInclusive; i < toExclusive; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, vector(1.0f + i, 2.0f + i, 3.0f + i));
    }

    private int annCount()
    {
        return execute("SELECT k FROM %s ORDER BY v ANN OF [1.0, 2.0, 3.0] LIMIT 100").size();
    }

    private int indexedCount()
    {
        return execute("SELECT id1 FROM %s WHERE v1 >= 0").size();
    }

    private int unindexedCount()
    {
        return execute("SELECT * FROM %s").size();
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
