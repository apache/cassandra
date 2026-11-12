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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertEquals;

public class UCSOverlapMetricsTest extends CQLTester
{
    @Test
    public void testOverlappingFlushes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10'}");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        writeOverlappingFlushes(3);

        // small flushes all land in L0
        assertEquals(3, cfs.getCompactionStrategyManager().getUCSMaxOverlapL0());
        assertEquals(3, (int) cfs.metric.ucsMaxOverlapL0.getValue());
        assertEquals(3, cfs.getUCSMaxOverlapL0());
    }

    @Test
    public void testNonOverlappingFlushes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10'}");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // each sstable holds a single, different key
        for (int pk = 0; pk < 3; pk++)
        {
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, pk);
            flush();
        }

        assertEquals(3, cfs.getLiveSSTables().size());
        assertEquals(1, cfs.getUCSMaxOverlapL0());
    }

    @Test
    public void testEmptyUCSTable()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertEquals(0, (int) cfs.metric.ucsMaxOverlapL0.getValue());
    }

    @Test
    public void testNonUCSTable()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH compaction = {'class': 'LeveledCompactionStrategy'}");
        execute("INSERT INTO %s (pk, v) VALUES (0, 0)");
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertEquals(0, (int) cfs.metric.ucsMaxOverlapL0.getValue());
    }

    // each flush covers the same 100 keys, so every sstable overlaps every other
    private void writeOverlappingFlushes(int flushes)
    {
        for (int flush = 0; flush < flushes; flush++)
        {
            for (int pk = 0; pk < 100; pk++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, flush);
            flush();
        }
    }
}
