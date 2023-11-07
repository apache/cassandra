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

package org.apache.cassandra.io.sstable.format;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests initially implemented for CASSANDRA-18932 and CASSANDRA-18993. They verify the behaviour of querying data
 * from a row with a clustering column condition, where the last item covered by the column index block is a range
 * tombstone boundary marker.
 * <p/>
 * The column index is actually a part of primary index in BIG format and row index in BTI format.
 */
@RunWith(BMUnitRunner.class)
public class ColumnIndexTest extends CQLTester
{
    private static final AtomicBoolean rtbmLastInIndexBlock = new AtomicBoolean(false);

    public static void setRTBMLastInIndexBlock()
    {
        rtbmLastInIndexBlock.set(true);
    }

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        CQLTester.setUpClass();
        requireNetwork();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        rtbmLastInIndexBlock.set(false);
    }

    @Test
    @BMRules(rules = { @BMRule(name = "force_add_index_block_after_range_tombstone_boundary_marker_big",
                               targetClass = "org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter",
                               targetMethod = "addUnfiltered",
                               targetLocation = "AT EXIT",
                               condition = "$1 instanceof org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker",
                               action = "$this.addIndexBlock()"),
                       @BMRule(name = "force_add_index_block_after_range_tombstone_boundary_marker_bti",
                               targetClass = "org.apache.cassandra.io.sstable.format.bti.BtiFormatPartitionWriter",
                               targetMethod = "addUnfiltered",
                               targetLocation = "AT EXIT",
                               condition = "$1 instanceof org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker",
                               action = "$this.addIndexBlock()"),
    })
    public void testBoundaryMarkerAtTheEndOfIndexBlock() throws Throwable
    {
        int expectedRows = 10;
        IntFunction<String> fmt = (i) -> String.format("%05d", i);

        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c ASC)");

        for (int i = 0; i < expectedRows; i++)
        {
            // delete (0; 1] and then insert 1 will make a range tombstone boundary marker (0; 0]
            execute("DELETE FROM %s WHERE k = ? AND c > ? AND c <= ?", 0, i - 1, i);
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, i, fmt.apply(i));
        }

        flush();

        ResultSet r = executeNetWithPaging("SELECT * FROM %s", 1);
        Iterator<Row> iter = r.iterator();
        int n = 0;
        while (iter.hasNext())
        {
            Row row = iter.next();
            int c = row.getInt("c");
            String v = row.getString("v");
            logger.info("Read row: " + row);
            assertThat(v).isEqualTo(fmt.apply(c));
            n++;
        }
        assertThat(n).isEqualTo(expectedRows);
    }

    @Test
    @BMRules(rules = { @BMRule(name = "test_big",
                               targetClass = "org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter",
                               targetMethod = "addUnfiltered",
                               targetLocation = "AT INVOKE addIndexBlock ALL",
                               condition = "$1 instanceof org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker",
                               action = "org.apache.cassandra.io.sstable.format.ColumnIndexTest.setRTBMLastInIndexBlock()"),
                       @BMRule(name = "test_bti",
                               targetClass = "org.apache.cassandra.io.sstable.format.bti.BtiFormatPartitionWriter",
                               targetMethod = "addUnfiltered",
                               targetLocation = "AT INVOKE addIndexBlock ALL",
                               condition = "$1 instanceof org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker",
                               action = "org.apache.cassandra.io.sstable.format.ColumnIndexTest.setRTBMLastInIndexBlock()") })
    public void silentDataLossTest() throws Throwable
    {
        rtbmLastInIndexBlock.set(false);

        String tab1 = createTable("CREATE TABLE %s (pk1 bigint, ck1 bigint, v1 ascii, PRIMARY KEY (pk1, ck1))");
        String tab2 = createTable("CREATE TABLE %s (pk1 bigint, ck1 bigint, v1 ascii, PRIMARY KEY (pk1, ck1))");

        for (int size = 0; size < 1000; size += 10)
        {
            long pk = size;
            String longString = StringUtils.repeat("a", size);

            for (String tbl : new String[]{ tab1, tab2 })
            {
                execute(String.format("INSERT INTO %s.%s (pk1, ck1, v1) VALUES (?, ?, ?)", keyspace(), tbl), pk, 0L, longString);
                execute(String.format("INSERT INTO %s.%s (pk1, ck1) VALUES (?, ?)", keyspace(), tbl), pk, 1L);
                execute(String.format("DELETE FROM %s.%s WHERE pk1 = ? AND ck1 > ? AND ck1 < ?", keyspace(), tbl), pk, 1L, 5L);
                execute(String.format("INSERT INTO %s.%s (pk1, ck1, v1) VALUES (?, ?, ?)", keyspace(), tbl), pk, 2L, longString);
                execute(String.format("DELETE FROM %s.%s WHERE pk1 = ? AND ck1 > ? AND ck1 < ?", keyspace(), tbl), pk, 2L, 5L);
            }
            flush(keyspace(), tab1);
            List<Row> r1 = executeNetWithPaging(String.format("SELECT * FROM %s.%s WHERE pk1 = ?", keyspace(), tab1), 1, pk).all();
            List<Row> r2 = executeNetWithPaging(String.format("SELECT * FROM %s.%s WHERE pk1 = ?", keyspace(), tab2), 1, pk).all();

            assertThat(r1).usingElementComparator(CQLTester::compareNetRows)
                          .describedAs("Rows for pk=%s", pk)
                          .isEqualTo(r2);
            assertThat(r1).hasSize(3);
        }

        assertThat(rtbmLastInIndexBlock.get()).isTrue();
    }
}
