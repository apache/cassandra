/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.db.marshal;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBuilder;
import org.apache.cassandra.db.marshal.datetime.DateRangeCodec;
import org.hamcrest.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class DateRangeIntegrationTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        CodecRegistry.DEFAULT_INSTANCE.register(DateRangeCodec.instance);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testDateRangeAsPrimaryKey() throws Throwable
    {
        String keyspace = randomKeyspace();
        executeNet(String.format("CREATE KEYSPACE %s WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}", keyspace));
        executeNet("USE " + keyspace);
        executeNet("CREATE TABLE dr (k 'DateRangeType' PRIMARY KEY, v int)");
        executeNet("INSERT INTO dr (k, v) VALUES ('[2010-12-03 TO 2010-12-04]', 1)");
        executeNet("INSERT INTO dr (k, v) VALUES ('[2015-12-03T10:15:30.001Z TO 2016-01-01T00:05:11.967Z]', 2)");

        ResultSet results = executeNet(String.format("SELECT * FROM %s.dr", keyspace));
        List<Row> rows = results.all();

        assertEquals(2, rows.size());
        DateRange expected = dateRange("2010-12-03T00:00:00.000Z", Precision.DAY, "2010-12-04T23:59:59.999Z", Precision.DAY);
        assertEquals(expected, rows.get(0).get("k", DateRange.class));
        expected = dateRange("2015-12-03T10:15:30.001Z", Precision.MILLISECOND, "2016-01-01T00:05:11.967Z", Precision.MILLISECOND);
        assertEquals(expected, rows.get(1).get("k", DateRange.class));

        results = executeNet("SELECT * FROM dr WHERE k = '[2015-12-03T10:15:30.001Z TO 2016-01-01T00:05:11.967]'");
        rows = results.all();

        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0).getInt("v"));


        flush(keyspace, "dr");

        results = executeNet("SELECT * FROM dr");
        rows = results.all();

        assertEquals(2, rows.size());
        expected = dateRange("2015-12-03T10:15:30.001Z", Precision.MILLISECOND, "2016-01-01T00:05:11.967Z", Precision.MILLISECOND);
        assertEquals(expected, rows.get(1).get("k", DateRange.class));
    }

    @Test
    public void testCreateDateRange() throws Throwable
    {
        String keyspace = randomKeyspace();
        executeNet(String.format("CREATE KEYSPACE %s WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}", keyspace));
        executeNet("USE " + keyspace);
        executeNet("CREATE TABLE dr (k int PRIMARY KEY, v 'DateRangeType')");
        executeNet("INSERT INTO dr (k, v) VALUES (1, '[2000-01-01T10:15:30.301Z TO *]')");
        executeNet("INSERT INTO dr (k, v) VALUES (2, '[2000-02 TO 2000-03]')");
        executeNet("INSERT INTO dr (k, v) VALUES (3, '[* TO 2020]')");
        executeNet("INSERT INTO dr (k, v) VALUES (4, null)");
        executeNet("INSERT INTO dr (k) VALUES (5)");

        ResultSet results = executeNet("SELECT * FROM dr");
        List<Row> rows = results.all();
        assertEquals(5, rows.size());
        DateRange dateRange = rows.get(4).get("v", DateRange.class);
        assertNotNull(dateRange);
        DateRange expected = DateRangeBuilder.dateRange()
                .withUnboundedLowerBound()
                .withUpperBound("2020-12-31T23:59:59.999Z", Precision.YEAR)
                .build();
        assertEquals(expected, dateRange);
    }

    @Test
    public void testInvalidDateRangeOrder() throws Throwable
    {
        String keyspace = randomKeyspace();
        executeNet(String.format("CREATE KEYSPACE %s WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}", keyspace));
        executeNet("USE " + keyspace);
        executeNet("CREATE TABLE dr (k int PRIMARY KEY, v 'DateRangeType')");

        expectedException.expect(InvalidQueryException.class);
        expectedException.expectMessage("Wrong order: 2020-01-01T10:15:30.009Z TO 2010-01-01T00:05:11.031Z");
        expectedException.expectMessage("Could not parse date range: [2020-01-01T10:15:30.009Z TO 2010-01-01T00:05:11.031Z]");
        executeNet("INSERT INTO dr (k, v) VALUES (1, '[2020-01-01T10:15:30.009Z TO 2010-01-01T00:05:11.031Z]')");
    }

    @Test
    public void testDateRangeInTuples() throws Throwable
    {
        String keyspace = randomKeyspace();
        executeNet(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '1'}", keyspace));
        executeNet("USE " + keyspace);
        executeNet("CREATE TYPE IF NOT EXISTS test_udt (i int, range 'DateRangeType')");
        executeNet("CREATE TABLE dr (k int PRIMARY KEY, u test_udt, uf frozen<test_udt>, t tuple<'DateRangeType', int>, tf frozen<tuple<'DateRangeType', int>>)");

        executeNet("INSERT INTO dr (k, u, uf, t, tf) VALUES (" +
                "1, " +
                "{i: 10, range: '[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]'}, " +
                "{i: 20, range: '[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]'}, " +
                "('[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]', 30), " +
                "('[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]', 40))");

        DateRange expected = dateRange("2000-01-01T10:15:30.003Z", Precision.MILLISECOND, "2020-01-01T10:15:30.001Z", Precision.MILLISECOND);
        ResultSet results = executeNet("SELECT * FROM dr");
        List<Row> rows = results.all();
        assertEquals(1, rows.size());

        UDTValue u = rows.get(0).get("u", UDTValue.class);
        DateRange dateRange = u.get("range", DateRange.class);
        assertEquals(expected, dateRange);
        assertEquals(10, u.getInt("i"));

        u = rows.get(0).get("uf", UDTValue.class);
        dateRange = u.get("range", DateRange.class);
        assertEquals(expected, dateRange);
        assertEquals(20, u.getInt("i"));

        TupleValue t = rows.get(0).get("t", TupleValue.class);
        dateRange = t.get(0, DateRange.class);
        assertEquals(expected, dateRange);
        assertEquals(30, t.getInt(1));

        t = rows.get(0).get("tf", TupleValue.class);
        dateRange = t.get(0, DateRange.class);
        assertEquals(expected, dateRange);
        assertEquals(40, t.getInt(1));
    }

    @Test
    public void testDateRangeInCollections() throws Throwable
    {
        String keyspace = randomKeyspace();
        executeNet(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '1'}", keyspace));
        executeNet("USE " + keyspace);
        executeNet("CREATE TABLE dr (k int PRIMARY KEY, l list<'DateRangeType'>, s set<'DateRangeType'>, dr2i map<'DateRangeType', int>, i2dr map<int, 'DateRangeType'>)");

        executeNet("INSERT INTO dr (k, l, s, i2dr, dr2i) VALUES (" +
                "1, " +
                "['[2000-01-01T10:15:30.001Z TO 2020]', '[2010-01-01T10:15:30.001Z TO 2020]', '2001-01-02'], " +
                "{'[2000-01-01T10:15:30.001Z TO 2020]', '[2000-01-01T10:15:30.001Z TO 2020]', '[2010-01-01T10:15:30.001Z TO 2020]'}, " +
                "{1: '[2000-01-01T10:15:30.001Z TO 2020]', 2: '[2010-01-01T10:15:30.001Z TO 2020]'}, " +
                "{'[2000-01-01T10:15:30.001Z TO 2020]': 1, '[2010-01-01T10:15:30.001Z TO 2020]': 2})");

        ResultSet results = executeNet("SELECT * FROM dr");
        List<Row> rows = results.all();
        assertEquals(1, rows.size());

        List<DateRange> drList = rows.get(0).getList("l", DateRange.class);
        assertEquals(3, drList.size());
        assertEquals(dateRange("2000-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR), drList.get(0));
        assertEquals(dateRange("2010-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR), drList.get(1));
        assertEquals(DateRangeBuilder.dateRange().withLowerBound("2001-01-02T00:00:00.000Z", Precision.DAY).build(), drList.get(2));

        Set<DateRange> drSet = rows.get(0).getSet("s", DateRange.class);
        assertEquals(2, drSet.size());
        assertEquals(
                Sets.newHashSet(
                        dateRange("2000-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR),
                        dateRange("2010-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR)),
                drSet);

        Map<DateRange, Integer> dr2i = rows.get(0).getMap("dr2i", DateRange.class, Integer.class);
        assertEquals(2, dr2i.size());
        assertEquals(1, (int) dr2i.get(dateRange("2000-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR)));
        assertEquals(2, (int) dr2i.get(dateRange("2010-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR)));

        Map<Integer, DateRange> i2dr = rows.get(0).getMap("i2dr", Integer.class, DateRange.class);
        assertEquals(2, i2dr.size());
        assertEquals(dateRange("2000-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR), i2dr.get(1));
        assertEquals(dateRange("2010-01-01T10:15:30.001Z", Precision.MILLISECOND, "2020-12-31T23:59:59.999Z", Precision.YEAR), i2dr.get(2));
    }

    @Test
    public void testPreparedStatementsWithDateRange() throws Throwable
    {
        String keyspace = randomKeyspace();
        executeNet(String.format("CREATE KEYSPACE %s WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}", keyspace));
        executeNet("USE " + keyspace);
        executeNet("CREATE TABLE dr (k int PRIMARY KEY, v 'DateRangeType')");

        Session session = sessionNet();
        PreparedStatement statement = session.prepare(String.format("INSERT INTO %s.dr (k,v) VALUES(?,?)", keyspace));

        DateRange dateRange = dateRange("2007-12-03T00:00:00.000Z", Precision.DAY, "2007-12-17T00:00:00.000Z", Precision.MONTH);
        session.execute(statement.bind(1, dateRange));

        ResultSet results = executeNet("SELECT * FROM dr");
        List<Row> rows = results.all();
        assertEquals(1, rows.size());

        DateRange actual = rows.get(0).get("v", DateRange.class);
        assertEquals(Precision.DAY, actual.getLowerBound().getPrecision());
        assertEquals(Precision.MONTH, actual.getUpperBound().getPrecision());
        assertEquals("[2007-12-03 TO 2007-12]", actual.formatToSolrString());

        results = executeNet("SELECT JSON * FROM dr");
        assertThat(results.all().get(0).toString(), Matchers.containsString("\"v\": \"[2007-12-03 TO 2007-12]\""));
    }

    @Test
    public void testSemanticallyEquivalentDateRanges() throws Throwable
    {
        String keyspace = randomKeyspace();
        executeNet(String.format("CREATE KEYSPACE %s WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}", keyspace));
        executeNet("USE " + keyspace);
        executeNet("CREATE TABLE dr (k int, c0 'DateRangeType', PRIMARY KEY (k, c0))");

        executeNet("INSERT INTO dr (k, c0) VALUES (1, '2016-01-01')");
        executeNet("INSERT INTO dr (k, c0) VALUES (1, '[2016-01-01 TO 2016-01-01]')");
        executeNet("INSERT INTO dr (k, c0) VALUES (1, '[2016-01-01T00:00:00.000Z TO 2016-01-01]')");
        executeNet("INSERT INTO dr (k, c0) VALUES (1, '[2016-01-01T00:00:00.000Z TO 2016-01-01:23:59:59.999Z]')");
        executeNet("INSERT INTO dr (k, c0) VALUES (1, '[2016-01-01 TO 2016-01-01:23:59:59.999Z]')");

        ResultSet results = executeNet("SELECT * FROM dr");
        assertEquals(5, results.all().size());

        results = executeNet("SELECT * FROM dr WHERE c0 = '2016-01-01' ALLOW FILTERING");
        assertEquals(1, results.all().size());

        results = executeNet("SELECT * FROM dr WHERE k = 1 AND c0 = '[2016-01-01T00:00:00.000Z TO 2016-01-01:23:59:59.999Z]'");
        assertEquals(1, results.all().size());
    }

    private String randomKeyspace()
    {
        return "ks" + System.nanoTime();
    }

    private DateRange dateRange(String lowerBound, Precision lowerBoundPrecision, String upperBound, Precision upperBoundPrecision)
    {
        return DateRangeBuilder.dateRange()
                .withLowerBound(lowerBound, lowerBoundPrecision)
                .withUpperBound(upperBound, upperBoundPrecision)
                .build();
    }
}
