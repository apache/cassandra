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

package org.apache.cassandra.db.virtual;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.bouncycastle.util.encoders.Hex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PrimaryIdTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private PrimaryIdTable primaryIdTable;
    private String table;
    private AtomicInteger scanned;

    private final boolean useBtiFormat;
    @Parameterized.Parameters(name = "Use BtiFormat = {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
            {false}, {true}
        });
    }

    public PrimaryIdTableTest(boolean useBtiFormat) {
        this.useBtiFormat = useBtiFormat;
    }

    @Before
    public void before()
    {
        if (useBtiFormat) {
            DatabaseDescriptor.setSelectedSSTableFormat(new BtiFormat.BtiFormatFactory().getInstance(Collections.emptyMap()));
        }
        primaryIdTable = new PrimaryIdTable(KS_NAME);
        scanned = new AtomicInteger();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(primaryIdTable)));

        table = createTable("CREATE TABLE %s (key blob PRIMARY KEY, value blob)");

        for (int i = -10; i < 1000; i++)
        {
            ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(i);
            ByteBuffer value = ByteBuffer.wrap(new byte[1]);
            execute("INSERT INTO %s (key, value) VALUES (?, ?)", key, value);
        }
        Util.flushTable(KEYSPACE, table);
        primaryIdTable.readListener.add(unused -> scanned.incrementAndGet());
    }

    @Test
    public void testPrimaryIdTable()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ?",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1010, all.size());
        for (int i = -10; i < 1000; i++)
        {
            Row row = all.get(i + 10);
            assertEquals(BigInteger.valueOf(i), row.get("token_value", BigInteger.class));
        }
        // 1010 + 100 for the 1 per 10 page, +1 for the last
        assertEquals(1111, scanned.get());
    }

    @Test
    public void testTokenValueGreaterThanZero()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value > 0",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(999, all.size());
        for (int i = 1; i < 1000; i++)
        {
            Row row = all.get(i - 1);
            assertEquals(BigInteger.valueOf(i), row.get("token_value", BigInteger.class));
        }
        assertEquals(1099, scanned.get());
    }

    @Test
    public void testTokenValueGreaterThanNegativeFive()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value > -5",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1004, all.size());
        for (int i = -4; i < 1000; i++)
        {
            Row row = all.get(i + 4);
            assertEquals(BigInteger.valueOf(i), row.get("token_value", BigInteger.class));
        }
        // 1004 + 100 for the 1 per 10 page, +1 for the last
        assertEquals(1105, scanned.get());
    }

    @Test
    public void testTokenValueLessThanOrEqualToFive()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value <= 5",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(16, all.size());
        for (int i = -10; i <= 5; i++)
        {
            Row row = all.get(i + 10);
            assertEquals(BigInteger.valueOf(i), row.get("token_value", BigInteger.class));
        }
        assertEquals(18, scanned.get());
    }

    @Test
    public void testTokenValueEqualToZero()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value = 0",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
        Row row = all.get(0);
        assertEquals(BigInteger.valueOf(0), row.get("token_value", BigInteger.class));
        assertEquals(2, scanned.get());
    }

    @Test
    public void testTokenValueBounds()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value > 0 AND token_value < 15",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(14, all.size());

        for (int i = 0; i < 14; i++)
        {
            Row row = all.get(i);
            assertEquals(BigInteger.valueOf(i + 1), row.get("token_value", BigInteger.class));
        }
        // 0->10 = 11, 10->16 = 7
        assertEquals(18, scanned.get());
    }

    @Test
    public void testTokenValueBoundsWithKey()
    {
        ByteBuffer ten = Murmur3Partitioner.LongToken.keyForToken(10);
        String key = Hex.toHexString(ten.array());
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value > 0 AND token_value < 15 AND key = ?",
                                            10, KEYSPACE, table, key);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
        Row row = all.get(0);
        assertEquals(BigInteger.valueOf(10), row.get("token_value", BigInteger.class));
        assertEquals(2, scanned.get());
    }

    @Test
    public void testByKey()
    {
        ByteBuffer ten = Murmur3Partitioner.LongToken.keyForToken(10);
        String key = Hex.toHexString(ten.array());
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND key = ?",
                                            10, KEYSPACE, table, key);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
        Row row = all.get(0);
        assertEquals(BigInteger.valueOf(10), row.get("token_value", BigInteger.class));
        assertEquals(2, scanned.get());
    }

    @Test
    public void testIgnoreSStableOutOfRange()
    {
        ByteBuffer twok = Murmur3Partitioner.LongToken.keyForToken(2000);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", twok, ByteBuffer.wrap(new byte[1]));
        Util.flushTable(KEYSPACE, table);
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value > 1500",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
        Row row = all.get(0);
        assertEquals(BigInteger.valueOf(2000), row.get("token_value", BigInteger.class));
        assertEquals(1L, row.get("sstables", Long.class).longValue());
        assertEquals(1, scanned.get());
    }

    @Test
    public void testNoResults()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value < -1000",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(0, all.size());
        assertEquals(0, scanned.get()); // sstables shouldn't even of been touched
    }

    @Test(expected = InvalidQueryException.class)
    public void testNonExistantKeyspace()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = 'non_existent' AND table_name = ?",
                                            10, table);
        List<Row> all = rs.all();
        assertEquals(0, all.size());
        assertEquals(0, scanned.get());
    }

    @Test
    public void testNoResultsWithSSTables()
    {
        ByteBuffer o1 = Murmur3Partitioner.LongToken.keyForToken(10000);
        ByteBuffer o2 = Murmur3Partitioner.LongToken.keyForToken(10002);
        ByteBuffer value = ByteBuffer.wrap(new byte[10]);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", o1, value);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", o2, value);
        Util.flushTable(KEYSPACE, table);

        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ? AND token_value = 10001",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(0, all.size());
        assertEquals(1, scanned.get());
    }

    @Test
    public void testSameKeyInMultipleSSTables()
    {
        String table = createTable("CREATE TABLE %s (key blob PRIMARY KEY, value blob)");

        ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(1);
        ByteBuffer value = ByteBuffer.wrap(new byte[10]);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", key, value);
        Util.flushTable(KEYSPACE, table);
        value = ByteBuffer.wrap(new byte[100]);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", key, value);
        Util.flushTable(KEYSPACE, table);

        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.primary_ids WHERE keyspace_name = ? AND table_name = ?",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
        Row row = all.get(0);
        assertEquals(BigInteger.valueOf(1), row.get("token_value", BigInteger.class));
        long size = row.get("size_estimate", Long.class);
        assertTrue(size >= 110 && size < 200);
        assertEquals(2L, row.get("sstables", Long.class).longValue());
        assertEquals(2, scanned.get());
    }
}
