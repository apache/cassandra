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

package org.apache.cassandra.db.partitions;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.btree.BTree;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a grow/reset op mix on one partition's {@code set<text>} column with strictly
 * increasing timestamps, then asserts the memtable's on/off-heap ownership never goes negative.
 */
public class SetCellAccountingTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(SetCellAccountingTest.class);

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            Config conf = (Config) confField.get(null);
            conf.memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
        CQLTester.prepareServer();
    }

    private ColumnFamilyStore createTestTable()
    {
        createTable("CREATE TABLE %s (" +
                    "    name text PRIMARY KEY," +
                    "    last_contact timestamp," +
                    "    namespace text," +
                    "    partitioner text," +
                    "    properties text," +
                    "    state text," +
                    "    seed_hosts set<text>)");
        return getCurrentColumnFamilyStore();
    }

    @Test
    public void largeSetGrowShrinkKeepOwnsNonNegative()
    {
        ColumnFamilyStore cfs = createTestTable();

        final int setSize = BTree.MAX_KEYS + 1;
        final int ops = 10_000;
        final AtomicLong ts = new AtomicLong(1_000_000L);

        for (int op = 0; op < ops; op++)
        {
            long t = ts.incrementAndGet();
            String cql;
            if (op % 2 == 0)
            {
                cql = "UPDATE %s USING TIMESTAMP " + t + " SET seed_hosts = seed_hosts + " +
                      rangeSet(0, setSize) + " WHERE name = 'test'";
            }
            else
            {
                cql = "UPDATE %s USING TIMESTAMP " + t + " SET seed_hosts = " +
                      rangeSet(0, setSize / 2) + " WHERE name = 'test'";
            }


            QueryProcessor.executeInternal(formatQuery(cql));
            UntypedResultSet rs = QueryProcessor.executeInternal(
            formatQuery("SELECT seed_hosts FROM %s WHERE name = 'test'"));
            Set<String> seeds = (rs == null || rs.isEmpty() || !rs.one().has("seed_hosts"))
                                ? null
                                : rs.one().getSet("seed_hosts", UTF8Type.instance);

            if (op % 100 == 0)
                logger.info("== op=" + op +
                            ", seedsSize= " + (seeds != null ? seeds.size() : "0") +
                            ", heapSize= " + ownsOnHeapNow(cfs) +
                            ", offheapSize= " + ownsOffHeapNow(cfs) +
                            ", seed_hosts=" + seeds);
            assertOwnsNonNegative(cfs, "after op=" + op);

        }
        cfs.forceBlockingFlush();
        assertOwnsNonNegative(cfs, "after flush");
    }

    private static long ownsOnHeapNow(ColumnFamilyStore cfs)
    {
        return cfs.getTracker().getView().getCurrentMemtable().getAllocator().onHeap().owns();
    }

    private static long ownsOffHeapNow(ColumnFamilyStore cfs)
    {
        return cfs.getTracker().getView().getCurrentMemtable().getAllocator().offHeap().owns();
    }


    /**
     * {@code {'e00000','e00001',...}} for indices [from,to).
     */
    private static String rangeSet(int from, int to)
    {
        if (to <= from) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = from; i < to; i++)
        {
            if (i > from) sb.append(',');
            sb.append('\'').append(elemName(i)).append('\'');
        }
        return sb.append('}').toString();
    }

    private static String elemName(int i)
    {
        return 'e' + String.format("%05d", i);
    }

    private static void assertOwnsNonNegative(ColumnFamilyStore cfs, String step)
    {
        for (Memtable mt : cfs.getTracker().getView().getAllMemtables())
        {
            assertThat(mt.getAllocator().onHeap().owns())
            .as("ON-heap owns went NEGATIVE [" + step + "]")
            .isGreaterThanOrEqualTo(0L);
            assertThat(mt.getAllocator().offHeap().owns())
            .as("OFF-heap owns went NEGATIVE [" + step + "]")
            .isGreaterThanOrEqualTo(0L);
        }
    }
}
