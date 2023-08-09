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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.TimeUUID;

import org.junit.Test;

public class CastFctsTest extends CQLTester
{
    @Test
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b text, c double)");

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
                   row(BigDecimal.valueOf(1),
                       BigDecimal.valueOf(2),
                       BigDecimal.valueOf(3),
                       BigDecimal.valueOf(4),
                       new BigDecimal("5.2"),
                       BigDecimal.valueOf(6.3),
                       BigDecimal.valueOf(6.3),
                       BigDecimal.valueOf(4),
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
    public void testNoLossOfPrecisionForCastToDecimal() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, bigint_clmn bigint, varint_clmn varint)");
        execute("INSERT INTO %s(k, bigint_clmn, varint_clmn) VALUES(2, 9223372036854775807, 1234567890123456789)");

        assertRows(execute("SELECT CAST(bigint_clmn AS decimal), CAST(varint_clmn AS decimal) FROM %s"),
                   row(BigDecimal.valueOf(9223372036854775807L), BigDecimal.valueOf(1234567890123456789L)));
    }

    @Test
    public void testTimeCastsInSelectionClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a timeuuid primary key, b timestamp, c date, d time)");

        final String yearMonthDay = "2015-05-21";
        final LocalDate localDate = LocalDate.of(2015, 5, 21);
        ZonedDateTime date = localDate.atStartOfDay(ZoneOffset.UTC);

        ZonedDateTime dateTime = ZonedDateTime.of(localDate, LocalTime.of(11,3,2), ZoneOffset.UTC);

        long timeInMillis = dateTime.toInstant().toEpochMilli();

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, '" + yearMonthDay + " 11:03:02+00', '2015-05-21', '11:03:02')",
                TimeUUID.Generator.atUnixMillis(timeInMillis));

        assertRows(execute("SELECT CAST(a AS timestamp), " +
                           "CAST(b AS timestamp), " +
                           "CAST(c AS timestamp) FROM %s"),
                   row(Date.from(dateTime.toInstant()), Date.from(dateTime.toInstant()), Date.from(date.toInstant())));

        int timeInMillisToDay = SimpleDateSerializer.timeInMillisToDay(date.toInstant().toEpochMilli());
        assertRows(execute("SELECT CAST(a AS date), " +
                           "CAST(b AS date), " +
                           "CAST(c AS date) FROM %s"),
                   row(timeInMillisToDay, timeInMillisToDay, timeInMillisToDay));

        assertRows(execute("SELECT CAST(b AS text), " +
                           "CAST(c AS text), " +
                           "CAST(d AS text) FROM %s"),
                   row(yearMonthDay + "T11:03:02.000Z", yearMonthDay, "11:03:02.000000000"));
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
                   row((byte) 2, (short) 2, 2, 2L, 2.0F, 2.0, BigDecimal.valueOf(2), "2", "2"));
    }

    /**
     * Verifies that the {@code CAST} function can be used in the values of {@code INSERT INTO} statements.
     */
    @Test
    public void testCastsInInsertIntoValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        // Simple cast
        execute("INSERT INTO %s (k, v) VALUES (1, CAST(1.3 AS int))");
        assertRows(execute("SELECT v FROM %s"), row(1));

        // Nested casts
        execute("INSERT INTO %s (k, v) VALUES (1, CAST(CAST(CAST(2.3 AS int) AS float) AS int))");
        assertRows(execute("SELECT v FROM %s"), row(2));

        // Cast of placeholder with type hint
        execute("INSERT INTO %s (k, v) VALUES (1, CAST((float) ? AS int))", 3.4f);
        assertRows(execute("SELECT v FROM %s"), row(3));

        // Cast of placeholder without type hint
        assertInvalidRequestMessage("Ambiguous call to function system.cast_as_int",
                                    "INSERT INTO %s (k, v) VALUES (1, CAST(? AS int))", 3.4f);

        // Type hint of cast
        execute("INSERT INTO %s (k, v) VALUES (1, (int) CAST(4.9 AS int))");
        assertRows(execute("SELECT v FROM %s"), row(4));

        // Function of cast
        execute(String.format("INSERT INTO %%s (k, v) VALUES (1, %s(CAST(5 AS float)))", floatToInt()));
        assertRows(execute("SELECT v FROM %s"), row(5));

        // Cast of function
        execute(String.format("INSERT INTO %%s (k, v) VALUES (1, CAST(%s(6) AS int))", intToFloat()));
        assertRows(execute("SELECT v FROM %s"), row(6));
    }

    /**
     * Verifies that the {@code CAST} function can be used in the values of {@code UPDATE} statements.
     */
    @Test
    public void testCastsInUpdateValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        // Simple cast
        execute("UPDATE %s SET v = CAST(1.3 AS int) WHERE k = 1");
        assertRows(execute("SELECT v FROM %s"), row(1));

        // Nested casts
        execute("UPDATE %s SET v = CAST(CAST(CAST(2.3 AS int) AS float) AS int) WHERE k = 1");
        assertRows(execute("SELECT v FROM %s"), row(2));

        // Cast of placeholder with type hint
        execute("UPDATE %s SET v = CAST((float) ? AS int) WHERE k = 1", 3.4f);
        assertRows(execute("SELECT v FROM %s"), row(3));

        // Cast of placeholder without type hint
        assertInvalidRequestMessage("Ambiguous call to function system.cast_as_int",
                                    "UPDATE %s SET v = CAST(? AS int) WHERE k = 1", 3.4f);

        // Type hint of cast
        execute("UPDATE %s SET v = (int) CAST(4.9 AS int) WHERE k = 1");
        assertRows(execute("SELECT v FROM %s"), row(4));

        // Function of cast
        execute(String.format("UPDATE %%s SET v = %s(CAST(5 AS float)) WHERE k = 1", floatToInt()));
        assertRows(execute("SELECT v FROM %s"), row(5));

        // Cast of function
        execute(String.format("UPDATE %%s SET v = CAST(%s(6) AS int) WHERE k = 1", intToFloat()));
        assertRows(execute("SELECT v FROM %s"), row(6));
    }

    /**
     * Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code UPDATE} statements.
     */
    @Test
    public void testCastsInUpdateWhereClause() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        for (int i = 1; i <= 6; i++)
        {
            execute("INSERT INTO %s (k) VALUES (?)", i);
        }

        // Simple cast
        execute("UPDATE %s SET v = ? WHERE k = CAST(1.3 AS int)", 1);
        assertRows(execute("SELECT v FROM %s WHERE k = ?", 1), row(1));

        // Nested casts
        execute("UPDATE %s SET v = ? WHERE k = CAST(CAST(CAST(2.3 AS int) AS float) AS int)", 2);
        assertRows(execute("SELECT v FROM %s WHERE k = ?", 2), row(2));

        // Cast of placeholder with type hint
        execute("UPDATE %s SET v = ? WHERE k = CAST((float) ? AS int)", 3, 3.4f);
        assertRows(execute("SELECT v FROM %s WHERE k = ?", 3), row(3));

        // Cast of placeholder without type hint
        assertInvalidRequestMessage("Ambiguous call to function system.cast_as_int",
                                    "UPDATE %s SET v = ? WHERE k = CAST(? AS int)", 3, 3.4f);

        // Type hint of cast
        execute("UPDATE %s SET v = ? WHERE k = (int) CAST(4.9 AS int)", 4);
        assertRows(execute("SELECT v FROM %s WHERE k = ?", 4), row(4));

        // Function of cast
        execute(String.format("UPDATE %%s SET v = ? WHERE k = %s(CAST(5 AS float))", floatToInt()), 5);
        assertRows(execute("SELECT v FROM %s WHERE k = ?", 5), row(5));

        // Cast of function
        execute(String.format("UPDATE %%s SET v = ? WHERE k = CAST(%s(6) AS int)", intToFloat()), 6);
        assertRows(execute("SELECT v FROM %s WHERE k = ?", 6), row(6));
    }

    /**
     * Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code SELECT} statements.
     */
    @Test
    public void testCastsInSelectWhereClause() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");

        for (int i = 1; i <= 6; i++)
        {
            execute("INSERT INTO %s (k) VALUES (?)", i);
        }

        // Simple cast
        assertRows(execute("SELECT k FROM %s WHERE k = CAST(1.3 AS int)"), row(1));

        // Nested casts
        assertRows(execute("SELECT k FROM %s WHERE k = CAST(CAST(CAST(2.3 AS int) AS float) AS int)"), row(2));

        // Cast of placeholder with type hint
        assertRows(execute("SELECT k FROM %s WHERE k = CAST((float) ? AS int)", 3.4f), row(3));

        // Cast of placeholder without type hint
        assertInvalidRequestMessage("Ambiguous call to function system.cast_as_int",
                                    "SELECT k FROM %s WHERE k = CAST(? AS int)", 3.4f);

        // Type hint of cast
        assertRows(execute("SELECT k FROM %s WHERE k = (int) CAST(4.9 AS int)"), row(4));

        // Function of cast
        assertRows(execute(String.format("SELECT k FROM %%s WHERE k = %s(CAST(5 AS float))", floatToInt())), row(5));

        // Cast of function
        assertRows(execute(String.format("SELECT k FROM %%s WHERE k = CAST(%s(6) AS int)", intToFloat())), row(6));
    }

    /**
     * Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code DELETE} statements.
     */
    @Test
    public void testCastsInDeleteWhereClause() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");

        for (int i = 1; i <= 6; i++)
        {
            execute("INSERT INTO %s (k) VALUES (?)", i);
        }

        // Simple cast
        execute("DELETE FROM %s WHERE k = CAST(1.3 AS int)");
        assertEmpty(execute("SELECT * FROM %s WHERE k = ?", 1));

        // Nested casts
        execute("DELETE FROM %s WHERE k = CAST(CAST(CAST(2.3 AS int) AS float) AS int)");
        assertEmpty(execute("SELECT * FROM %s WHERE k = ?", 2));

        // Cast of placeholder with type hint
        execute("DELETE FROM %s WHERE k = CAST((float) ? AS int)", 3.4f);
        assertEmpty(execute("SELECT * FROM %s WHERE k = ?", 3));

        // Cast of placeholder without type hint
        assertInvalidRequestMessage("Ambiguous call to function system.cast_as_int",
                                    "DELETE FROM %s WHERE k = CAST(? AS int)", 3.4f);

        // Type hint of cast
        execute("DELETE FROM %s WHERE k = (int) CAST(4.9 AS int)");
        assertEmpty(execute("SELECT * FROM %s WHERE k = ?", 4));

        // Function of cast
        execute(String.format("DELETE FROM %%s WHERE k = %s(CAST(5 AS float))", floatToInt()));
        assertEmpty(execute("SELECT * FROM %s WHERE k = ?", 5));

        // Cast of function
        execute(String.format("DELETE FROM %%s WHERE k = CAST(%s(6) AS int)", intToFloat()));
        assertEmpty(execute("SELECT * FROM %s WHERE k = ?", 6));
    }

    /**
     * Creates a CQL function that casts an {@code int} argument into a {@code float}.
     *
     * @return the name of the created function
     */
    private String floatToInt() throws Throwable
    {
        return createFunction(KEYSPACE,
                              "int, int",
                              "CREATE FUNCTION IF NOT EXISTS %s (x float) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS int " +
                              "LANGUAGE java " +
                              "AS 'return Float.valueOf(x).intValue();'");
    }

    /**
     * Creates a CQL function that casts a {@code float} argument into an {@code int}.
     *
     * @return the name of the created function
     */
    private String intToFloat() throws Throwable
    {
        return createFunction(KEYSPACE,
                              "int, int",
                              "CREATE FUNCTION IF NOT EXISTS %s (x int) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS float " +
                              "LANGUAGE java " +
                              "AS 'return (float) x;'");
    }

    /**
     * Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code CREATE MATERIALIZED
     * VIEW} statements.
     */
    @Test
    public void testCastsInCreateViewWhereClause() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        String viewName = keyspace() + ".mv_with_cast";
        execute(String.format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s" +
                              "   WHERE k < CAST(3.14 AS int) AND v IS NOT NULL" +
                              "   PRIMARY KEY (v, k)", viewName));

        // start storage service so MV writes are applied
        StorageService.instance.initServer();

        execute("INSERT INTO %s (k, v) VALUES (1, 10)");
        execute("INSERT INTO %s (k, v) VALUES (2, 20)");
        execute("INSERT INTO %s (k, v) VALUES (3, 30)");

        assertRows(execute(String.format("SELECT * FROM %s", viewName)), row(10, 1), row(20, 2));

        execute("DROP MATERIALIZED VIEW " + viewName);
    }
}
