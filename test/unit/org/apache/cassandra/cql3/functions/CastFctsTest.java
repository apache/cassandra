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
package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

public class CastFctsTest extends CQLTester
{
    @Test
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b text, c double)");

        assertInvalidSyntaxMessage("no viable alternative at input '(' (... b, c) VALUES ([CAST](...)",
                                   "INSERT INTO %s (a, b, c) VALUES (CAST(? AS int), ?, ?)", 1.6, "test", 6.3);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (..." + KEYSPACE + "." + currentTable()
                + " SET c = [cast](...)",
                                   "UPDATE %s SET c = cast(? as double) WHERE a = ?", 1, 1);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (...= ? WHERE a = [CAST] (...)",
                                   "UPDATE %s SET c = ? WHERE a = CAST (? AS INT)", 1, 2.0);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (..." + KEYSPACE + "." + currentTable()
                + " WHERE a = [CAST] (...)",
                                   "DELETE FROM %s WHERE a = CAST (? AS INT)", 1, 2.0);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (..." + KEYSPACE + "." + currentTable()
                + " WHERE a = [CAST] (...)",
                                   "SELECT * FROM %s WHERE a = CAST (? AS INT)", 1, 2.0);

        assertInvalidMessage("a cannot be cast to boolean", "SELECT CAST(a AS boolean) FROM %s");
    }

    @Test
    public void testNumericCastsInSelectionClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a tinyint primary key,"
                                   + " b smallint,"
                                   + " c int,"
                                   + " d bigint,"
                                   + " e float,"
                                   + " f double,"
                                   + " g decimal,"
                                   + " h varint,"
                                   + " i int)");

        execute("INSERT INTO %s (a, b, c, d, e, f, g, h) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (byte) 1, (short) 2, 3, 4L, 5.2F, 6.3, BigDecimal.valueOf(6.3), BigInteger.valueOf(4));

        assertColumnNames(execute("SELECT CAST(b AS int), CAST(c AS int), CAST(d AS double) FROM %s"),
                          "cast(b as int)",
                          "c",
                          "cast(d as double)");

        assertRows(execute("SELECT CAST(a AS tinyint), " +
                "CAST(b AS tinyint), " +
                "CAST(c AS tinyint), " +
                "CAST(d AS tinyint), " +
                "CAST(e AS tinyint), " +
                "CAST(f AS tinyint), " +
                "CAST(g AS tinyint), " +
                "CAST(h AS tinyint), " +
                "CAST(i AS tinyint) FROM %s"),
                   row((byte) 1, (byte) 2, (byte) 3, (byte) 4L, (byte) 5, (byte) 6, (byte) 6, (byte) 4, null));

        assertRows(execute("SELECT CAST(a AS smallint), " +
                "CAST(b AS smallint), " +
                "CAST(c AS smallint), " +
                "CAST(d AS smallint), " +
                "CAST(e AS smallint), " +
                "CAST(f AS smallint), " +
                "CAST(g AS smallint), " +
                "CAST(h AS smallint), " +
                "CAST(i AS smallint) FROM %s"),
                   row((short) 1, (short) 2, (short) 3, (short) 4L, (short) 5, (short) 6, (short) 6, (short) 4, null));

        assertRows(execute("SELECT CAST(a AS int), " +
                "CAST(b AS int), " +
                "CAST(c AS int), " +
                "CAST(d AS int), " +
                "CAST(e AS int), " +
                "CAST(f AS int), " +
                "CAST(g AS int), " +
                "CAST(h AS int), " +
                "CAST(i AS int) FROM %s"),
                   row(1, 2, 3, 4, 5, 6, 6, 4, null));

        assertRows(execute("SELECT CAST(a AS bigint), " +
                "CAST(b AS bigint), " +
                "CAST(c AS bigint), " +
                "CAST(d AS bigint), " +
                "CAST(e AS bigint), " +
                "CAST(f AS bigint), " +
                "CAST(g AS bigint), " +
                "CAST(h AS bigint), " +
                "CAST(i AS bigint) FROM %s"),
                   row(1L, 2L, 3L, 4L, 5L, 6L, 6L, 4L, null));

        assertRows(execute("SELECT CAST(a AS float), " +
                "CAST(b AS float), " +
                "CAST(c AS float), " +
                "CAST(d AS float), " +
                "CAST(e AS float), " +
                "CAST(f AS float), " +
                "CAST(g AS float), " +
                "CAST(h AS float), " +
                "CAST(i AS float) FROM %s"),
                   row(1.0F, 2.0F, 3.0F, 4.0F, 5.2F, 6.3F, 6.3F, 4.0F, null));

        assertRows(execute("SELECT CAST(a AS double), " +
                "CAST(b AS double), " +
                "CAST(c AS double), " +
                "CAST(d AS double), " +
                "CAST(e AS double), " +
                "CAST(f AS double), " +
                "CAST(g AS double), " +
                "CAST(h AS double), " +
                "CAST(i AS double) FROM %s"),
                   row(1.0, 2.0, 3.0, 4.0, (double) 5.2F, 6.3, 6.3, 4.0, null));

        assertRows(execute("SELECT CAST(a AS decimal), " +
                "CAST(b AS decimal), " +
                "CAST(c AS decimal), " +
                "CAST(d AS decimal), " +
                "CAST(e AS decimal), " +
                "CAST(f AS decimal), " +
                "CAST(g AS decimal), " +
                "CAST(h AS decimal), " +
                "CAST(i AS decimal) FROM %s"),
                   row(BigDecimal.valueOf(1.0),
                       BigDecimal.valueOf(2.0),
                       BigDecimal.valueOf(3.0),
                       BigDecimal.valueOf(4.0),
                       BigDecimal.valueOf(5.2F),
                       BigDecimal.valueOf(6.3),
                       BigDecimal.valueOf(6.3),
                       BigDecimal.valueOf(4.0),
                       null));

        assertRows(execute("SELECT CAST(a AS ascii), " +
                "CAST(b AS ascii), " +
                "CAST(c AS ascii), " +
                "CAST(d AS ascii), " +
                "CAST(e AS ascii), " +
                "CAST(f AS ascii), " +
                "CAST(g AS ascii), " +
                "CAST(h AS ascii), " +
                "CAST(i AS ascii) FROM %s"),
                   row("1",
                       "2",
                       "3",
                       "4",
                       "5.2",
                       "6.3",
                       "6.3",
                       "4",
                       null));

        assertRows(execute("SELECT CAST(a AS text), " +
                "CAST(b AS text), " +
                "CAST(c AS text), " +
                "CAST(d AS text), " +
                "CAST(e AS text), " +
                "CAST(f AS text), " +
                "CAST(g AS text), " +
                "CAST(h AS text), " +
                "CAST(i AS text) FROM %s"),
                   row("1",
                       "2",
                       "3",
                       "4",
                       "5.2",
                       "6.3",
                       "6.3",
                       "4",
                       null));
    }

    @Test
    public void testTimeCastsInSelectionClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a timeuuid primary key, b timestamp, c date, d time)");

        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        DateTime date = DateTimeFormat.forPattern("yyyy-MM-dd")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21");

        long timeInMillis = dateTime.getMillis();

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, '2015-05-21 11:03:02+00', '2015-05-21', '11:03:02')",
                UUIDGen.getTimeUUID(timeInMillis));

        assertRows(execute("SELECT CAST(a AS timestamp), " +
                           "CAST(b AS timestamp), " +
                           "CAST(c AS timestamp) FROM %s"),
                   row(new Date(dateTime.getMillis()), new Date(dateTime.getMillis()), new Date(date.getMillis())));

        int timeInMillisToDay = SimpleDateSerializer.timeInMillisToDay(date.getMillis());
        assertRows(execute("SELECT CAST(a AS date), " +
                           "CAST(b AS date), " +
                           "CAST(c AS date) FROM %s"),
                   row(timeInMillisToDay, timeInMillisToDay, timeInMillisToDay));

        assertRows(execute("SELECT CAST(b AS text), " +
                           "CAST(c AS text), " +
                           "CAST(d AS text) FROM %s"),
                   row("2015-05-21T11:03:02.000Z", "2015-05-21", "11:03:02.000000000"));
    }

    @Test
    public void testOtherTypeCastsInSelectionClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a ascii primary key,"
                                   + " b inet,"
                                   + " c boolean)");

        execute("INSERT INTO %s (a, b, c) VALUES (?, '127.0.0.1', ?)",
                "test", true);

        assertRows(execute("SELECT CAST(a AS text), " +
                "CAST(b AS text), " +
                "CAST(c AS text) FROM %s"),
                   row("test", "127.0.0.1", "true"));
    }

    @Test
    public void testCastsWithReverseOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (a int,"
                                   + " b smallint,"
                                   + " c double,"
                                   + " primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC);");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)",
                1, (short) 2, 6.3);

        assertRows(execute("SELECT CAST(a AS tinyint), " +
                "CAST(b AS tinyint), " +
                "CAST(c AS tinyint) FROM %s"),
                   row((byte) 1, (byte) 2, (byte) 6));

        assertRows(execute("SELECT CAST(CAST(a AS tinyint) AS smallint), " +
                "CAST(CAST(b AS tinyint) AS smallint), " +
                "CAST(CAST(c AS tinyint) AS smallint) FROM %s"),
                   row((short) 1, (short) 2, (short) 6));

        assertRows(execute("SELECT CAST(CAST(CAST(a AS tinyint) AS double) AS text), " +
                "CAST(CAST(CAST(b AS tinyint) AS double) AS text), " +
                "CAST(CAST(CAST(c AS tinyint) AS double) AS text) FROM %s"),
                   row("1.0", "2.0", "6.0"));

        String f = createFunction(KEYSPACE, "int",
                                  "CREATE FUNCTION %s(val int) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS double " +
                                          "LANGUAGE java " +
                                          "AS 'return (double)val;'");

        assertRows(execute("SELECT " + f + "(CAST(b AS int)) FROM %s"),
                   row((double) 2));

        assertRows(execute("SELECT CAST(" + f + "(CAST(b AS int)) AS text) FROM %s"),
                   row("2.0"));
    }

    @Test
    public void testCounterCastsInSelectionClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b counter)");

        execute("UPDATE %s SET b = b + 2 WHERE a = 1");

        assertRows(execute("SELECT CAST(b AS tinyint), " +
                "CAST(b AS smallint), " +
                "CAST(b AS int), " +
                "CAST(b AS bigint), " +
                "CAST(b AS float), " +
                "CAST(b AS double), " +
                "CAST(b AS decimal), " +
                "CAST(b AS ascii), " +
                "CAST(b AS text) FROM %s"),
                   row((byte) 2, (short) 2, 2, 2L, 2.0F, 2.0, BigDecimal.valueOf(2.0), "2", "2"));
    }
}
