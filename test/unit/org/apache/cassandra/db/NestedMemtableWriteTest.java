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

package org.apache.cassandra.db;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21019: a write made from within an already-started mutation on the same write context is a
 * nested write, and {@link ColumnFamilyStore#apply} must route it to {@code Memtable.put}, which does not
 * wait for pool room, rather than to {@code Memtable.checkSpaceAndPut}, which does. Every other write must
 * still wait, including the writes an index build makes, which the {@code updateIndexes} flag cannot tell
 * apart from a nested one.
 */
public class NestedMemtableWriteTest extends CQLTester
{
    @Test
    public void contextReportsOneMemtableWriteAtATime()
    {
        CassandraWriteContext context = new CassandraWriteContext(new OpOrder().start(), CommitLogPosition.NONE);

        assertFalse("a fresh context applies nothing", context.isApplyingToMemtable());
        assertTrue("the first write is the outermost one", context.enterMemtableWrite());
        assertTrue(context.isApplyingToMemtable());
        assertFalse("a write inside the outermost one is nested", context.enterMemtableWrite());
        assertTrue("a nested write must not clear the mark", context.isApplyingToMemtable());

        context.exitMemtableWrite();
        assertFalse("the mark is cleared once the outermost write completes", context.isApplyingToMemtable());
        assertTrue("the next write is again the outermost one", context.enterMemtableWrite());

        context.exitMemtableWrite();
        context.close();
    }

    @Test
    public void writeTimeIndexUpdateIsNested() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING '" + ContextRecordingIndex.class.getName() + "'");

        ContextRecordingIndex.applying.clear();
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");

        assertEquals("the indexer callback runs inside the enclosing mutation",
                     singletonList(Boolean.TRUE), ContextRecordingIndex.applying);
    }

    @Test
    public void indexBuildIsNotNested() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING '" + ContextRecordingIndex.class.getName() + "'");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        execute("INSERT INTO %s (k, v) VALUES (2, 2)");
        flush(); // the build reads sstables, not memtables

        ContextRecordingIndex.applying.clear();
        getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(ImmutableSet.of(index));

        assertEquals("the build indexes every row", 2, ContextRecordingIndex.applying.size());
        assertFalse("an index build is a top-level write and must still wait for room",
                    ContextRecordingIndex.applying.contains(Boolean.TRUE));
    }

    /** Records, for every indexed row, whether the write context was already applying to a memtable. */
    public static class ContextRecordingIndex extends StubIndex
    {
        static final List<Boolean> applying = new CopyOnWriteArrayList<>();

        public ContextRecordingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public boolean dependsOn(ColumnMetadata column)
        {
            return true;
        }

        @Override
        public boolean shouldBuildBlocking()
        {
            return true; // so that rebuildIndexesBlocking() selects this index
        }

        @Override
        public Indexer indexerFor(DecoratedKey key,
                                  RegularAndStaticColumns columns,
                                  long nowInSec,
                                  WriteContext ctx,
                                  IndexTransaction.Type transactionType,
                                  Memtable memtable)
        {
            return new Indexer()
            {
                @Override
                public void insertRow(Row row)
                {
                    applying.add(CassandraWriteContext.fromContext(ctx).isApplyingToMemtable());
                }
            };
        }
    }
}
