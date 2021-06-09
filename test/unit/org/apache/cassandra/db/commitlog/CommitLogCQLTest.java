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

package org.apache.cassandra.db.commitlog;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

public class CommitLogCQLTest extends CQLTester
{
    @Test
    public void testTruncateSegmentDiscard() throws Throwable
    {
        String otherTable = createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");

        createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");
        execute("INSERT INTO %s (idx, data) VALUES (?, ?)", 15, Integer.toString(15));
        flush();

        // We write something in different table to advance the commit log position. Current table remains clean.
        executeFormattedQuery(String.format("INSERT INTO %s.%s (idx, data) VALUES (?, ?)", keyspace(), otherTable), 16, Integer.toString(16));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assert cfs.getTracker().getView().getCurrentMemtable().isClean();
        // Calling switchMemtable directly applies Flush even though memtable is empty. This can happen with some races
        // (flush with recycling by segment manager). It should still tell commitlog that the memtable's region is clean.
        // CASSANDRA-12436
        cfs.switchMemtable(UNIT_TESTS);

        execute("INSERT INTO %s (idx, data) VALUES (?, ?)", 15, Integer.toString(17));

        Collection<CommitLogSegment> active = new ArrayList<>(CommitLog.instance.segmentManager.getActiveSegments());
        CommitLog.instance.forceRecycleAllSegments();

        // If one of the previous segments remains, it wasn't clean.
        active.retainAll(CommitLog.instance.segmentManager.getActiveSegments());
        assert active.isEmpty();
    }
}
