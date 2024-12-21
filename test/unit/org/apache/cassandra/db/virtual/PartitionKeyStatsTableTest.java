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
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import org.junit.runners.Parameterized.Parameters;

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
public class PartitionKeyStatsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private String table;
    private AtomicInteger scanned;

    private final boolean useBtiFormat;

    @Parameters(name = "Use BtiFormat = {0}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{ { false }, { true } });
    }

    public PartitionKeyStatsTableTest(boolean useBtiFormat)
    {
        this.useBtiFormat = useBtiFormat;
    }

    @Before
    public void before()
    {
        if (useBtiFormat)
            DatabaseDescriptor.setSelectedSSTableFormat(new BtiFormat.BtiFormatFactory().getInstance(Collections.emptyMap()));

        PartitionKeyStatsTable primaryIdTable = new PartitionKeyStatsTable(KS_NAME);
        scanned = new AtomicInteger();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(primaryIdTable)));

        table = createTable("CREATE TABLE %s (key blob PRIMARY KEY, value blob)");

        ByteBuffer value = ByteBuffer.wrap(new byte[1]);
        for (int i = -10; i < 1000; i++)
        {
            ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(i);
            execute("INSERT INTO %s (key, value) VALUES (?, ?)", key, value);
        }
        Util.flushTable(KEYSPACE, table);
        primaryIdTable.readListener.add(unused -> scanned.incrementAndGet());
    }

    @Test
    public void testPrimaryIdTable()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ?",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1010, all.size());
        assertResults(all, -10, 1000);
        // 1010 + 100 for the 1 per 10 page, +1 for the last
        assertEquals(1111, scanned.get());
    }

    @Test
    public void testTokenValueGreaterThanZero()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value > 0",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(999, all.size());
        assertResults(all, 1, 1000);
        assertEquals(1099, scanned.get());
    }

    @Test
    public void testTokenValueGreaterThanNegativeFive()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value > -5",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1004, all.size());
        assertResults(all, -4, 1000);
        // 1004 + 100 for the 1 per 10 page, +1 for the last
        assertEquals(1105, scanned.get());
    }

    @Test
    public void testTokenValueLessThanOrEqualToFive()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value <= 5",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(16, all.size());
        assertResults(all, -10, 5);
        assertEquals(18, scanned.get());
    }

    @Test
    public void testTokenValueEqualToZero()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value = 0",
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
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value > 0 AND token_value < 15",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(14, all.size());
        assertResults(all, 1, 14);
        // 0->10 = 11, 10->16 = 7
        assertEquals(18, scanned.get());
    }

    @Test
    public void testTokenValueBoundsWithBetween()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value BETWEEN 0 AND 15",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(16, all.size());
        assertResults(all, 0, 15);
        assertEquals(18, scanned.get());
    }

    @Test
    public void testTokenValueBoundsWithIn()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value IN (1,3,6)",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(3, all.size());
        assertEquals(BigInteger.valueOf(1), all.get(0).get("token_value", BigInteger.class));
        assertEquals(BigInteger.valueOf(3), all.get(1).get("token_value", BigInteger.class));
        assertEquals(BigInteger.valueOf(6), all.get(2).get("token_value", BigInteger.class));
        assertEquals(7, scanned.get());
    }

    @Test
    public void testTokenValueBoundsWithKey()
    {
        ByteBuffer ten = Murmur3Partitioner.LongToken.keyForToken(10);
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value > 0 AND token_value < 15 AND key = ?",
                                            10, KEYSPACE, table, Hex.toHexString(ten.array()));
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
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND key = ?",
                                            10, KEYSPACE, table, Hex.toHexString(ten.array()));
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
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value > 1500",
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
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value < -1000",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(0, all.size());
        assertEquals(0, scanned.get()); // sstables shouldn't even of been touched
    }

    @Test(expected = InvalidQueryException.class)
    public void testNonExistantKeyspace()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = 'non_existent' AND table_name = ?",
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

        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value = 10001",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(0, all.size());
        assertEquals(1, scanned.get());
    }

    @Test
    public void testPrimaryIdTableDuplicates()
    {
        // 0xc25f118f072d6ba5cab7fb1468ace617 hashes to 1563004846366
        ByteBuffer dup = Murmur3Partitioner.LongToken.keyForToken(1563004846366L);
        // -19, 68, -61 (0xed44c3) hashes to 1563004846366
        ByteBuffer dup2 = ByteBuffer.wrap(new byte[]{ -19, 68, -61 });
        ByteBuffer value = ByteBuffer.wrap(new byte[10]);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", dup, value);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", dup2, value);
        Util.flushTable(KEYSPACE, table);

        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND token_value = 1563004846366",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(2, all.size());
        assertEquals(BigInteger.valueOf(1563004846366L), all.get(0).get("token_value", BigInteger.class));
        assertEquals(BigInteger.valueOf(1563004846366L), all.get(1).get("token_value", BigInteger.class));
        assertEquals("c25f118f072d6ba5cab7fb1468ace617", all.get(0).getString("key"));
        assertEquals("ed44c3", all.get(1).getString("key"));
        assertEquals(2, scanned.get());
    }

    @Test
    public void testCompositeType() throws UnknownHostException
    {
        String table = createTable("CREATE TABLE %s (key text, keytwo inet, value text, primary key ((key, keytwo)))");

        execute("INSERT INTO %s (key, keytwo, value) VALUES (?, ?, ?)", "testkey", InetAddress.getByName("127.0.0.1"), "value");
        Util.flushTable(KEYSPACE, table);

        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND key = 'testkey:127.0.0.1'",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
    }

    @Test
    public void testTextType()
    {
        String table = createTable("CREATE TABLE %s (key text PRIMARY KEY, value text)");

        execute("INSERT INTO %s (key, value) VALUES (?, ?)", "testkey", "value");
        Util.flushTable(KEYSPACE, table);

        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ? AND key = 'testkey'",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
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

        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_key_statistics WHERE keyspace_name = ? AND table_name = ?",
                                            10, KEYSPACE, table);
        List<Row> all = rs.all();
        assertEquals(1, all.size());
        Row row = all.get(0);
        assertEquals(BigInteger.valueOf(1), row.get("token_value", BigInteger.class));
        long size = row.get("size_estimate", Long.class);
        // providing a range since with timestamp delta vint encoding worried this may drift with time or in wierd
        // VMs so just want to make sure it's in the right ballpark
        assertTrue(size >= 110 && size < 200);
        assertEquals(2L, row.get("sstables", Long.class).longValue());
        assertEquals(2, scanned.get());
    }

    private static void assertResults(List<Row> all, int start, int end)
    {
        for (int i = start, offset = 0; i < end; i++, offset++)
        {
            Row row = all.get(offset);
        }
    }
}
