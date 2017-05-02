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
package org.apache.cassandra.cql3.validation.operations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.time.DateUtils;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.TurboFilterList;
import ch.qos.logback.classic.turbo.ReconfigureOnChangeFilter;
import ch.qos.logback.classic.turbo.TurboFilter;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AggregationTest extends CQLTester
{
    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidThrowMessage("Cannot drop non existing aggregate", InvalidRequestException.class, "DROP AGGREGATE " + KEYSPACE + ".aggr_does_not_exist");
        assertInvalidThrowMessage("Cannot drop non existing aggregate", InvalidRequestException.class, "DROP AGGREGATE " + KEYSPACE + ".aggr_does_not_exist(int,text)");
        assertInvalidThrowMessage("Cannot drop non existing aggregate", InvalidRequestException.class, "DROP AGGREGATE keyspace_does_not_exist.aggr_does_not_exist");
        assertInvalidThrowMessage("Cannot drop non existing aggregate", InvalidRequestException.class, "DROP AGGREGATE keyspace_does_not_exist.aggr_does_not_exist(int,text)");

        execute("DROP AGGREGATE IF EXISTS " + KEYSPACE + ".aggr_does_not_exist");
        execute("DROP AGGREGATE IF EXISTS " + KEYSPACE + ".aggr_does_not_exist(int,text)");
        execute("DROP AGGREGATE IF EXISTS keyspace_does_not_exist.aggr_does_not_exist");
        execute("DROP AGGREGATE IF EXISTS keyspace_does_not_exist.aggr_does_not_exist(int,text)");
    }

    @Test
    public void testFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c double, d decimal, e smallint, f tinyint, primary key (a, b))");

        // Test with empty table
        assertColumnNames(execute("SELECT COUNT(*) FROM %s"), "count");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(0L));
        assertColumnNames(execute("SELECT max(b), min(b), sum(b), avg(b)," +
                                  "max(c), sum(c), avg(c)," +
                                  "sum(d), avg(d)," +
                                  "max(e), min(e), sum(e), avg(e)," +
                                  "max(f), min(f), sum(f), avg(f) FROM %s"),
                          "system.max(b)", "system.min(b)", "system.sum(b)", "system.avg(b)",
                          "system.max(c)", "system.sum(c)", "system.avg(c)",
                          "system.sum(d)", "system.avg(d)",
                          "system.max(e)", "system.min(e)", "system.sum(e)", "system.avg(e)",
                          "system.max(f)", "system.min(f)", "system.sum(f)", "system.avg(f)");
        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b)," +
                           "max(c), sum(c), avg(c)," +
                           "sum(d), avg(d)," +
                           "max(e), min(e), sum(e), avg(e)," +
                           "max(f), min(f), sum(f), avg(f) FROM %s"),
                   row(null, null, 0, 0, null, 0.0, 0.0, new BigDecimal("0"), new BigDecimal("0"),
                       null, null, (short)0, (short)0,
                       null, null, (byte)0, (byte)0));

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 11.5, 11.5, 1, 1)");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 2, 9.5, 1.5, 2, 2)");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 3, 9.0, 2.0, 3, 3)");

        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c), sum(d), avg(d)," +
                           "max(e), min(e), sum(e), avg(e)," +
                           "max(f), min(f), sum(f), avg(f)" +
                           " FROM %s"),
                   row(3, 1, 6, 2, 11.5, 30.0, 10.0, new BigDecimal("15.0"), new BigDecimal("5.0"),
                       (short)3, (short)1, (short)6, (short)2,
                       (byte)3, (byte)1, (byte)6, (byte)2));

        execute("INSERT INTO %s (a, b, d) VALUES (1, 5, 1.0)");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(b), count(c), count(e), count(f) FROM %s"), row(4L, 3L, 3L, 3L));
        // Makes sure that LIMIT does not affect the result of aggregates
        assertRows(execute("SELECT COUNT(b), count(c), count(e), count(f) FROM %s LIMIT 2"), row(4L, 3L, 3L, 3L));
        assertRows(execute("SELECT COUNT(b), count(c), count(e), count(f) FROM %s WHERE a = 1 LIMIT 2"),
                   row(4L, 3L, 3L, 3L));
        assertRows(execute("SELECT AVG(CAST(b AS double)) FROM %s"), row(11.0/4));
    }

    @Test
    public void testCountStarFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        // Test with empty table
        assertColumnNames(execute("SELECT COUNT(*) FROM %s"), "count");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(0L));
        assertColumnNames(execute("SELECT COUNT(1) FROM %s"), "count");
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(0L));
        assertColumnNames(execute("SELECT COUNT(*), COUNT(*) FROM %s"), "count", "count");
        assertRows(execute("SELECT COUNT(*), COUNT(*) FROM %s"), row(0L, 0L));

        // Test with alias
        assertColumnNames(execute("SELECT COUNT(*) as myCount FROM %s"), "mycount");
        assertRows(execute("SELECT COUNT(*) as myCount FROM %s"), row(0L));
        assertColumnNames(execute("SELECT COUNT(1) as myCount FROM %s"), "mycount");
        assertRows(execute("SELECT COUNT(1) as myCount FROM %s"), row(0L));

        // Test with other aggregates
        assertColumnNames(execute("SELECT COUNT(*), max(b), b FROM %s"), "count", "system.max(b)", "b");
        assertRows(execute("SELECT COUNT(*), max(b), b  FROM %s"), row(0L, null, null));
        assertColumnNames(execute("SELECT COUNT(1), max(b), b FROM %s"), "count", "system.max(b)", "b");
        assertRows(execute("SELECT COUNT(1), max(b), b  FROM %s"), row(0L, null, null));

        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 11.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 9.0)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 5, 1.0)");

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(4L));
        assertRows(execute("SELECT max(b), b, COUNT(*) FROM %s"), row(5, 1, 4L));
        assertRows(execute("SELECT max(b), COUNT(1), b FROM %s"), row(5, 4L, 1));
        // Makes sure that LIMIT does not affect the result of aggregates
        assertRows(execute("SELECT max(b), COUNT(1), b FROM %s LIMIT 2"), row(5, 4L, 1));
        assertRows(execute("SELECT max(b), COUNT(1), b FROM %s WHERE a = 1 LIMIT 2"), row(5, 4L, 1));
    }

    @Test
    public void testAggregateWithColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key (a, b))");

        // Test with empty table
        assertColumnNames(execute("SELECT count(b), max(b) as max, b, c as first FROM %s"),
                          "system.count(b)", "max", "b", "first");
        assertRows(execute("SELECT count(b), max(b) as max, b, c as first FROM %s"),
                           row(0L, null, null, null));

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, null)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 4, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 8, 12)");

        assertRows(execute("SELECT count(b), max(b) as max, b, c as first FROM %s"),
                   row(3L, 8, 2, null));
    }

    @Test
    public void testAggregateOnCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b counter, primary key (a))");

        // Test with empty table
        assertColumnNames(execute("SELECT count(b), max(b) as max, b FROM %s"),
                          "system.count(b)", "max", "b");
        assertRows(execute("SELECT count(b), max(b) as max, b FROM %s"),
                   row(0L, null, null));

        execute("UPDATE %s SET b = b + 1 WHERE a = 1");
        execute("UPDATE %s SET b = b + 1 WHERE a = 1");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));
        flush();
        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));

        execute("UPDATE %s SET b = b + 2 WHERE a = 1");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 4L, 4L, 4L, 4L));

        execute("UPDATE %s SET b = b - 2 WHERE a = 1");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));
        flush();
        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));

        execute("UPDATE %s SET b = b + 1 WHERE a = 2");
        execute("UPDATE %s SET b = b + 1 WHERE a = 2");
        execute("UPDATE %s SET b = b + 2 WHERE a = 2");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(2L, 4L, 2L, 3L, 6L));
    }

    @Test
    public void testAggregateWithUdtFields() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (x int)");
        createTable("CREATE TABLE %s (a int primary key, b frozen<" + myType + ">, c frozen<" + myType + ">)");

        // Test with empty table
        assertColumnNames(execute("SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                          "system.count(b.x)", "max", "b.x", "first");
        assertRows(execute("SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                           row(0L, null, null, null));

        execute("INSERT INTO %s (a, b, c) VALUES (1, {x:2}, null)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, {x:4}, {x:6})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, {x:8}, {x:12})");

        assertRows(execute("SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                   row(3L, 8, 2, null));

        assertInvalidMessage("Invalid field selection: system.max(b) of type blob is not a user type",
                             "SELECT max(b).x as max FROM %s");
    }

    @Test
    public void testAggregateWithFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b double, c double, primary key(a, b))");

        String copySign = createFunction(KEYSPACE,
                                         "double, double",
                                         "CREATE OR REPLACE FUNCTION %s(magnitude double, sign double) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS double " +
                                         "LANGUAGE JAVA " +
                                         "AS 'return Double.valueOf(Math.copySign(magnitude, sign));';");

        // Test with empty table
        assertColumnNames(execute("SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s"),
                          "system.count(b)", "max", copySign + "(b, c)", "first");
        assertRows(execute("SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s"),
                           row(0L, null, null, null));

        execute("INSERT INTO %s (a, b, c) VALUES (0, -1.2, 2.1)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1.3, -3.4)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1.4, 1.2)");

        assertRows(execute("SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s"),
                   row(3L, 1.4, 1.2, -2.1));

        execute("INSERT INTO %s (a, b, c) VALUES (1, -1.2, null)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1.3, -3.4)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1.4, 1.2)");
        assertRows(execute("SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s WHERE a = 1"),
                   row(3L, 1.4, null, null));
    }

    @Test
    public void testAggregateWithWriteTimeOrTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int, c int)");

        // Test with empty table
        assertColumnNames(execute("SELECT count(writetime(b)), min(ttl(b)) as min, writetime(b), ttl(c) as first FROM %s"),
                          "system.count(writetime(b))", "min", "writetime(b)", "first");
        assertRows(execute("SELECT count(writetime(b)), min(ttl(b)) as min, writetime(b), ttl(c) as first FROM %s"),
                           row(0L, null, null, null));

        long today = System.currentTimeMillis() * 1000;
        long yesterday = today - (DateUtils.MILLIS_PER_DAY * 1000);

        final int secondsPerMinute = 60;
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, null) USING TTL " + (20 * secondsPerMinute));
        execute("INSERT INTO %s (a, b, c) VALUES (2, 4, 6) USING TTL " + (10 * secondsPerMinute));
        execute("INSERT INTO %s (a, b, c) VALUES (4, 8, 12) USING TIMESTAMP " + yesterday );

        assertRows(execute("SELECT count(writetime(b)), count(ttl(b)) FROM %s"),
                   row(3L, 2L));

        UntypedResultSet resultSet = execute("SELECT min(ttl(b)), ttl(b) FROM %s");
        assertEquals(1, resultSet.size());
        Row row = resultSet.one();
        assertTrue(row.getInt("ttl(b)") > (10 * secondsPerMinute));
        assertTrue(row.getInt("system.min(ttl(b))") <= (10 * secondsPerMinute));

        resultSet = execute("SELECT min(writetime(b)), writetime(b) FROM %s");
        assertEquals(1, resultSet.size());
        row = resultSet.one();

        assertTrue(row.getLong("writetime(b)") >= today);
        assertTrue(row.getLong("system.min(writetime(b))") == yesterday);
    }

    @Test
    public void testFunctionsWithCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int , b int, c double, primary key(a, b) ) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 11.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 9.0)");

        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c) FROM %s"),
                   row(3, 1, 6, 2, 11.5, 30.0, 10.0));

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(3L));
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(3L));
        assertRows(execute("SELECT COUNT(*) FROM %s WHERE a = 1 AND b > 1"), row(2L));
        assertRows(execute("SELECT COUNT(1) FROM %s WHERE a = 1 AND b > 1"), row(2L));
        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c) FROM %s WHERE a = 1 AND b > 1"),
                   row(3, 2, 5, 2, 9.5, 18.5, 9.25));
    }

    @Test
    public void testInvalidCalls() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key (a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 8)");

        assertInvalidSyntax("SELECT max(b), max(c) FROM %s WHERE max(a) = 1");
        assertInvalidMessage("aggregate functions cannot be used as arguments of aggregate functions", "SELECT max(sum(c)) FROM %s");
    }

    @Test
    public void testReversedType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 8)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, 7)");

        assertRows(execute("SELECT max(c), min(c), avg(c) FROM %s WHERE a = 1 AND b > 1"), row(9, 7, 8));
    }

    @Test
    public void testNestedFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b timeuuid, c double, d double)");

        String copySign = createFunction(KEYSPACE,
                                         "double, double",
                                         "CREATE OR REPLACE FUNCTION %s(magnitude double, sign double) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS double " +
                                         "LANGUAGE JAVA " +
                                         "AS 'return Double.valueOf(Math.copySign(magnitude, sign));';");

        assertColumnNames(execute("SELECT max(a), max(toUnixTimestamp(b)) FROM %s"), "system.max(a)", "system.max(system.tounixtimestamp(b))");
        assertRows(execute("SELECT max(a), max(toUnixTimestamp(b)) FROM %s"), row(null, null));
        assertColumnNames(execute("SELECT max(a), toUnixTimestamp(max(b)) FROM %s"), "system.max(a)", "system.tounixtimestamp(system.max(b))");
        assertRows(execute("SELECT max(a), toUnixTimestamp(max(b)) FROM %s"), row(null, null));

        assertColumnNames(execute("SELECT max(" + copySign + "(c, d)) FROM %s"), "system.max(" + copySign + "(c, d))");
        assertRows(execute("SELECT max(" + copySign + "(c, d)) FROM %s"), row((Object) null));

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, maxTimeuuid('2011-02-03 04:05:00+0000'), -1.2, 2.1)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, maxTimeuuid('2011-02-03 04:06:00+0000'), 1.3, -3.4)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (3, maxTimeuuid('2011-02-03 04:10:00+0000'), 1.4, 1.2)");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = format.parse("2011-02-03 04:10:00");
        date = DateUtils.truncate(date, Calendar.MILLISECOND);

        assertRows(execute("SELECT max(a), max(toUnixTimestamp(b)) FROM %s"), row(3, date.getTime()));
        assertRows(execute("SELECT max(a), toUnixTimestamp(max(b)) FROM %s"), row(3, date.getTime()));

        assertRows(execute("SELECT " + copySign + "(max(c), min(c)) FROM %s"), row(-1.4));
        assertRows(execute("SELECT " + copySign + "(c, d) FROM %s"), row(1.2), row(-1.3), row(1.4));
        assertRows(execute("SELECT max(" + copySign + "(c, d)) FROM %s"), row(1.4));
        assertRows(execute("SELECT " + copySign + "(c, max(c)) FROM %s"), row(1.2));
        assertRows(execute("SELECT " + copySign + "(max(c), c) FROM %s"), row(-1.4));;
    }

    @Test
    public void testSchemaChange() throws Throwable
    {
        String f = createFunction(KEYSPACE,
                                  "double, double",
                                  "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS double " +
                                  "LANGUAGE javascript " +
                                  "AS '\"string\";';");

        createFunctionOverload(f,
                               "double, double",
                               "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS int " +
                               "LANGUAGE javascript " +
                               "AS '\"string\";';");

        String a = createAggregate(KEYSPACE,
                                   "double",
                                   "CREATE OR REPLACE AGGREGATE %s(double) " +
                                   "SFUNC " + shortFunctionName(f) + " " +
                                   "STYPE double " +
                                   "INITCOND 0");

        assertLastSchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.AGGREGATE,
                               KEYSPACE, parseFunctionName(a).name,
                               "double");

        schemaChange("CREATE OR REPLACE AGGREGATE " + a + "(double) " +
                     "SFUNC " + shortFunctionName(f) + " " +
                     "STYPE double " +
                     "INITCOND 0");

        assertLastSchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.AGGREGATE,
                               KEYSPACE, parseFunctionName(a).name,
                               "double");

        createAggregateOverload(a,
                                "int",
                                "CREATE OR REPLACE AGGREGATE %s(int) " +
                                "SFUNC " + shortFunctionName(f) + " " +
                                "STYPE int " +
                                "INITCOND 0");

        assertLastSchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.AGGREGATE,
                               KEYSPACE, parseFunctionName(a).name,
                               "int");

        schemaChange("DROP AGGREGATE " + a + "(double)");

        assertLastSchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.AGGREGATE,
                               KEYSPACE, parseFunctionName(a).name,
                               "double");
    }

    @Test
    public void testDropStatements() throws Throwable
    {
        String f = createFunction(KEYSPACE,
                                  "double, double",
                                  "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS double " +
                                  "LANGUAGE javascript " +
                                  "AS '\"string\";';");

        createFunctionOverload(f,
                               "double, double",
                               "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS int " +
                               "LANGUAGE javascript " +
                               "AS '\"string\";';");

        // DROP AGGREGATE must not succeed against a scalar
        assertInvalidMessage("matches multiple function definitions", "DROP AGGREGATE " + f);
        assertInvalidMessage("non existing", "DROP AGGREGATE " + f + "(double, double)");

        String a = createAggregate(KEYSPACE,
                                   "double",
                                   "CREATE OR REPLACE AGGREGATE %s(double) " +
                                   "SFUNC " + shortFunctionName(f) + " " +
                                   "STYPE double " +
                                   "INITCOND 0");
        createAggregateOverload(a,
                                "int",
                                "CREATE OR REPLACE AGGREGATE %s(int) " +
                                "SFUNC " + shortFunctionName(f) + " " +
                                "STYPE int " +
                                "INITCOND 0");

        // DROP FUNCTION must not succeed against an aggregate
        assertInvalidMessage("matches multiple function definitions", "DROP FUNCTION " + a);
        assertInvalidMessage("non existing function", "DROP FUNCTION " + a + "(double)");

        // ambigious
        assertInvalidMessage("matches multiple function definitions", "DROP AGGREGATE " + a);
        assertInvalidMessage("matches multiple function definitions", "DROP AGGREGATE IF EXISTS " + a);

        execute("DROP AGGREGATE IF EXISTS " + KEYSPACE + ".non_existing");
        execute("DROP AGGREGATE IF EXISTS " + a + "(int, text)");

        execute("DROP AGGREGATE " + a + "(double)");

        execute("DROP AGGREGATE IF EXISTS " + a + "(double)");
    }

    @Test
    public void testDropReferenced() throws Throwable
    {
        String f = createFunction(KEYSPACE,
                                  "double, double",
                                  "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS double " +
                                  "LANGUAGE javascript " +
                                  "AS '\"string\";';");

        String a = createAggregate(KEYSPACE,
                                   "double",
                                   "CREATE OR REPLACE AGGREGATE %s(double) " +
                                   "SFUNC " + shortFunctionName(f) + " " +
                                   "STYPE double " +
                                   "INITCOND 0");

        // DROP FUNCTION must not succeed because the function is still referenced by the aggregate
        assertInvalidMessage("still referenced by", "DROP FUNCTION " + f);

        execute("DROP AGGREGATE " + a + "(double)");
    }

    @Test
    public void testJavaAggregateNoInit() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return a.toString();'");

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int " +
                                   "FINALFUNC " + shortFunctionName(fFinal));

        // 1 + 2 + 3 = 6
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row("6"));

        execute("DROP AGGREGATE " + a + "(int)");

        assertInvalidMessage("Unknown function", "SELECT " + a + "(b) FROM %s");
    }

    @Test
    public void testJavaAggregateNullInitcond() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return a.toString();'");

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int " +
                                   "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                   "INITCOND null");

        // 1 + 2 + 3 = 6
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row("6"));

        execute("DROP AGGREGATE " + a + "(int)");

        assertInvalidMessage("Unknown function", "SELECT " + a + "(b) FROM %s");
    }

    @Test
    public void testJavaAggregateInvalidInitcond() throws Throwable
    {
        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return a.toString();'");

        assertInvalidMessage("Invalid STRING constant (foobar)",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + " " +
                             "INITCOND 'foobar'");
    }

    @Test
    public void testJavaAggregateIncompatibleTypes() throws Throwable
    {
        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return a.toString();'");

        String fState2 = createFunction(KEYSPACE,
                                        "int, int",
                                        "CREATE FUNCTION %s(a double, b double) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS double " +
                                        "LANGUAGE java " +
                                        "AS 'return Double.valueOf((a!=null?a.doubleValue():0d) + b.doubleValue());'");

        String fFinal2 = createFunction(KEYSPACE,
                                        "int",
                                        "CREATE FUNCTION %s(a double) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS text " +
                                        "LANGUAGE java " +
                                        "AS 'return a.toString();'");

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal));
        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal));
        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal));
        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int");
        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double");

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState2) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal));

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal2));
    }

    @Test
    public void testJavaAggregateNonExistingFuncs() throws Throwable
    {
        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return a.toString();'");

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + "_not_there " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal));

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + "_not_there");

        execute("CREATE AGGREGATE " + KEYSPACE + ".aggrInvalid(int)" +
                "SFUNC " + shortFunctionName(fState) + " " +
                "STYPE int " +
                "FINALFUNC " + shortFunctionName(fFinal));
        execute("DROP AGGREGATE " + KEYSPACE + ".aggrInvalid(int)");
    }

    @Test
    public void testJavaAggregateFailingFuncs() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'throw new RuntimeException(\"thrown to unit test - not a bug\");'");

        String fStateOK = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf(42);'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'throw new RuntimeException(\"thrown to unit test - not a bug\");'");

        String fFinalOK = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return \"foobar\";'");

        String a0 = createAggregate(KEYSPACE,
                                    "int",
                                    "CREATE AGGREGATE %s(int) " +
                                    "SFUNC " + shortFunctionName(fState) + " " +
                                    "STYPE int " +
                                    "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                    "INITCOND null");
        String a1 = createAggregate(KEYSPACE,
                                    "int",
                                    "CREATE AGGREGATE %s(int) " +
                                    "SFUNC " + shortFunctionName(fStateOK) + " " +
                                    "STYPE int " +
                                    "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                    "INITCOND null");
        String a2 = createAggregate(KEYSPACE,
                                    "int",
                                    "CREATE AGGREGATE %s(int) " +
                                    "SFUNC " + shortFunctionName(fStateOK) + " " +
                                    "STYPE int " +
                                    "FINALFUNC " + shortFunctionName(fFinalOK) + " " +
                                    "INITCOND null");

        assertInvalidThrowMessage("java.lang.RuntimeException", FunctionExecutionException.class, "SELECT " + a0 + "(b) FROM %s");
        assertInvalidThrowMessage("java.lang.RuntimeException", FunctionExecutionException.class, "SELECT " + a1 + "(b) FROM %s");
        assertRows(execute("SELECT " + a2 + "(b) FROM %s"), row("foobar"));
    }

    @Test
    public void testJavaAggregateWithoutStateOrFinal() throws Throwable
    {
        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".jSumFooNE1(int) " +
                             "SFUNC jSumFooNEstate " +
                             "STYPE int");

        String f = createFunction(KEYSPACE,
                                  "int, int",
                                  "CREATE FUNCTION %s(a int, b int) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS int " +
                                  "LANGUAGE java " +
                                  "AS 'return Integer.valueOf(a + b);'");

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".jSumFooNE2(int) " +
                             "SFUNC " + shortFunctionName(f) + " " +
                             "STYPE int " +
                             "FINALFUNC jSumFooNEfinal");

        execute("DROP FUNCTION " + f + "(int, int)");
    }

    @Test
    public void testJavaAggregate() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return a.toString();'");

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int " +
                                   "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                   "INITCOND 42");

        assertRows(execute("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, shortFunctionName(a)),
                   row("42"));

        // 42 + 1 + 2 + 3 = 48
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row("48"));

        execute("DROP AGGREGATE " + a + "(int)");

        execute("DROP FUNCTION " + fFinal + "(int)");
        execute("DROP FUNCTION " + fState + "(int, int)");

        assertInvalidMessage("Unknown function", "SELECT " + a + "(b) FROM %s");
    }

    @Test
    public void testJavaAggregateSimple() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String a = createAggregate(KEYSPACE,
                                   "int, int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int");

        // 1 + 2 + 3 = 6
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row(6));

        execute("DROP AGGREGATE " + a + "(int)");

        execute("DROP FUNCTION " + fState + "(int, int)");

        assertInvalidMessage("Unknown function", "SELECT " + a + "(b) FROM %s");
    }

    @Test
    public void testJavaAggregateEmpty() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String a = createAggregate(KEYSPACE,
                                   "int, int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int");

        assertRows(execute("SELECT " + a + "(b) FROM %s"), row(new Object[]{null}));
    }

    @Test
    public void testJavaAggregateStateEmpty() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b uuid)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(state map<uuid, int>, type uuid) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS map<uuid, int> " +
                                       "LANGUAGE java " +
                                       "AS 'return state;'");

        String a = createAggregate(KEYSPACE,
                                   "int, int",
                                   "CREATE AGGREGATE %s(uuid) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE map<uuid, int> " +
                                   "INITCOND {}");

        assertRows(execute("SELECT " + a + "(b) FROM %s"), row(Collections.emptyMap()));
    }

    @Test
    public void testJavaAggregateComplex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        // build an average aggregation function using
        // tuple<bigint,int> as state
        // double as finaltype

        String fState = createFunction(KEYSPACE,
                                       "tuple<bigint, int>, int",
                                       "CREATE FUNCTION %s(a tuple<bigint, int>, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS tuple<bigint, int> " +
                                       "LANGUAGE java " +
                                       "AS '" +
                                       "a.setLong(0, a.getLong(0) + b.intValue());" +
                                       "a.setInt(1, a.getInt(1) + 1);" +
                                       "return a;" +
                                       "'");

        String fFinal = createFunction(KEYSPACE,
                                       "tuple<bigint, int>",
                                       "CREATE FUNCTION %s(a tuple<bigint, int>) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS double " +
                                       "LANGUAGE java " +
                                       "AS '" +
                                       "double r = a.getLong(0);" +
                                       "r /= a.getInt(1);" +
                                       "return Double.valueOf(r);" +
                                       "'");

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE tuple<bigint, int> "+
                                   "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                   "INITCOND (0, 0)");

        assertRows(execute("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, shortFunctionName(a)),
                   row("(0, 0)"));

        // 1 + 2 + 3 = 6 / 3 = 2
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row(2d));

    }

    @Test
    public void testJavascriptAggregate() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE javascript " +
                                       "AS 'a + b;'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE javascript " +
                                       "AS '\"\"+a'");

        String a = createFunction(KEYSPACE,
                                  "int",
                                  "CREATE AGGREGATE %s(int) " +
                                  "SFUNC " + shortFunctionName(fState) + " " +
                                  "STYPE int " +
                                  "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                  "INITCOND 42");

        // 42 + 1 + 2 + 3 = 48
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row("48"));

        execute("DROP AGGREGATE " + a + "(int)");

        execute("DROP FUNCTION " + fFinal + "(int)");
        execute("DROP FUNCTION " + fState + "(int, int)");

        assertInvalidMessage("Unknown function", "SELECT " + a + "(b) FROM %s");
    }

    @Test
    public void testJavascriptAggregateSimple() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE javascript " +
                                       "AS 'a + b;'");

        String a = createAggregate(KEYSPACE,
                                   "int, int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int ");

        // 1 + 2 + 3 = 6
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row(6));

        execute("DROP AGGREGATE " + a + "(int)");

        execute("DROP FUNCTION " + fState + "(int, int)");

        assertInvalidMessage("Unknown function", "SELECT " + a + "(b) FROM %s");
    }

    @Test
    public void testFunctionDropPreparedStatement() throws Throwable
    {
        String otherKS = "cqltest_foo";

        execute("CREATE KEYSPACE IF NOT EXISTS " + otherKS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        try
        {
            execute("CREATE TABLE " + otherKS + ".jsdp (a int primary key, b int)");

            String fState = createFunction(otherKS,
                                           "int, int",
                                           "CREATE FUNCTION %s(a int, b int) " +
                                           "CALLED ON NULL INPUT " +
                                           "RETURNS int " +
                                           "LANGUAGE javascript " +
                                           "AS 'a + b;'");

            String a = createAggregate(otherKS,
                                       "int",
                                       "CREATE AGGREGATE %s(int) " +
                                       "SFUNC " + shortFunctionName(fState) + " " +
                                       "STYPE int");

            ResultMessage.Prepared prepared = QueryProcessor.prepare("SELECT " + a + "(b) FROM " + otherKS + ".jsdp", ClientState.forInternalCalls(), false);
            assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

            execute("DROP AGGREGATE " + a + "(int)");
            assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));

            //

            execute("CREATE AGGREGATE " + a + "(int) " +
                    "SFUNC " + shortFunctionName(fState) + " " +
                    "STYPE int");

            prepared = QueryProcessor.prepare("SELECT " + a + "(b) FROM " + otherKS + ".jsdp", ClientState.forInternalCalls(), false);
            assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

            execute("DROP KEYSPACE " + otherKS + ";");

            assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));
        }
        finally
        {
            execute("DROP KEYSPACE IF EXISTS " + otherKS + ";");
        }
    }

    @Test
    public void testAggregatesReferencedInAggregates() throws Throwable
    {

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE javascript " +
                                       "AS 'a + b;'");

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int ");

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggInv(int) " +
                             "SFUNC " + shortFunctionName(a) + " " +
                             "STYPE int ");

        assertInvalidMessage("does not exist or is not a scalar function",
                             "CREATE AGGREGATE " + KEYSPACE + ".aggInv(int) " +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(a));
    }

    @Test
    public void testCalledOnNullInput() throws Throwable
    {
        String fStateNonNull = createFunction(KEYSPACE,
                                              "int, int",
                                              "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS int " +
                                              "LANGUAGE java\n" +
                                              "AS 'return Integer.valueOf(state + val);';");
        String fStateNull = createFunction(KEYSPACE,
                                           "int, int",
                                           "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                                           "CALLED ON NULL INPUT " +
                                           "RETURNS int " +
                                           "LANGUAGE java\n" +
                                           "AS 'return Integer.valueOf(" +
                                           "   (state != null ? state.intValue() : 0) " +
                                           "   + (val != null ? val.intValue() : 0));';");
        String fStateAlwaysNull = createFunction(KEYSPACE,
                                           "int, int",
                                           "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                                           "CALLED ON NULL INPUT " +
                                           "RETURNS int " +
                                           "LANGUAGE java\n" +
                                           "AS 'return null;';");
        String fFinalNonNull = createFunction(KEYSPACE,
                                              "int",
                                              "CREATE OR REPLACE FUNCTION %s(state int) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS int " +
                                              "LANGUAGE java\n" +
                                              "AS 'return Integer.valueOf(state);';");
        String fFinalNull = createFunction(KEYSPACE,
                                           "int",
                                           "CREATE OR REPLACE FUNCTION %s(state int) " +
                                           "CALLED ON NULL INPUT " +
                                           "RETURNS int " +
                                           "LANGUAGE java\n" +
                                           "AS 'return state;';");

        assertInvalid("CREATE AGGREGATE " + KEYSPACE + ".invAggr(int) " +
                      "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                      "STYPE int");
        assertInvalid("CREATE AGGREGATE " + KEYSPACE + ".invAggr(int) " +
                      "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                      "STYPE int " +
                      "FINALFUNC " + shortFunctionName(fFinalNonNull));

        String aStateNull = createAggregate(KEYSPACE,
                                               "int",
                                               "CREATE AGGREGATE %s(int) " +
                                               "SFUNC " + shortFunctionName(fStateNull) + " " +
                                               "STYPE int");
        String aStateNullFinalNull = createAggregate(KEYSPACE,
                                                        "int",
                                                        "CREATE AGGREGATE %s(int) " +
                                                        "SFUNC " + shortFunctionName(fStateNull) + " " +
                                                        "STYPE int " +
                                                        "FINALFUNC " + shortFunctionName(fFinalNull));
        String aStateNullFinalNonNull = createAggregate(KEYSPACE,
                                                        "int",
                                                        "CREATE AGGREGATE %s(int) " +
                                                        "SFUNC " + shortFunctionName(fStateNull) + " " +
                                                        "STYPE int " +
                                                        "FINALFUNC " + shortFunctionName(fFinalNonNull));
        String aStateNonNull = createAggregate(KEYSPACE,
                                               "int",
                                               "CREATE AGGREGATE %s(int) " +
                                               "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                                               "STYPE int " +
                                               "INITCOND 0");
        String aStateNonNullFinalNull = createAggregate(KEYSPACE,
                                                        "int",
                                                        "CREATE AGGREGATE %s(int) " +
                                                        "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                                                        "STYPE int " +
                                                        "FINALFUNC " + shortFunctionName(fFinalNull) + " " +
                                                        "INITCOND 0");
        String aStateNonNullFinalNonNull = createAggregate(KEYSPACE,
                                                           "int",
                                                           "CREATE AGGREGATE %s(int) " +
                                                           "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                                                           "STYPE int " +
                                                           "FINALFUNC " + shortFunctionName(fFinalNonNull) + " " +
                                                           "INITCOND 0");
        String aStateAlwaysNullFinalNull = createAggregate(KEYSPACE,
                                                           "int",
                                                           "CREATE AGGREGATE %s(int) " +
                                                           "SFUNC " + shortFunctionName(fStateAlwaysNull) + " " +
                                                           "STYPE int " +
                                                           "FINALFUNC " + shortFunctionName(fFinalNull));
        String aStateAlwaysNullFinalNonNull = createAggregate(KEYSPACE,
                                                           "int",
                                                           "CREATE AGGREGATE %s(int) " +
                                                           "SFUNC " + shortFunctionName(fStateAlwaysNull) + " " +
                                                           "STYPE int " +
                                                           "FINALFUNC " + shortFunctionName(fFinalNonNull));

        createTable("CREATE TABLE %s (key int PRIMARY KEY, i int)");

        execute("INSERT INTO %s (key, i) VALUES (0, null)");
        execute("INSERT INTO %s (key, i) VALUES (1, 1)");
        execute("INSERT INTO %s (key, i) VALUES (2, 2)");
        execute("INSERT INTO %s (key, i) VALUES (3, 3)");

        assertRows(execute("SELECT " + aStateNull + "(i) FROM %s WHERE key = 0"), row(0));
        assertRows(execute("SELECT " + aStateNullFinalNull + "(i) FROM %s WHERE key = 0"), row(0));
        assertRows(execute("SELECT " + aStateNullFinalNonNull + "(i) FROM %s WHERE key = 0"), row(0));
        assertRows(execute("SELECT " + aStateNonNull + "(i) FROM %s WHERE key = 0"), row(0));
        assertRows(execute("SELECT " + aStateNonNullFinalNull + "(i) FROM %s WHERE key = 0"), row(0));
        assertRows(execute("SELECT " + aStateNonNullFinalNonNull + "(i) FROM %s WHERE key = 0"), row(0));
        assertRows(execute("SELECT " + aStateAlwaysNullFinalNull + "(i) FROM %s WHERE key = 0"), row(new Object[]{null}));
        assertRows(execute("SELECT " + aStateAlwaysNullFinalNonNull + "(i) FROM %s WHERE key = 0"), row(new Object[]{null}));

        assertRows(execute("SELECT " + aStateNull + "(i) FROM %s WHERE key = 1"), row(1));
        assertRows(execute("SELECT " + aStateNullFinalNull + "(i) FROM %s WHERE key = 1"), row(1));
        assertRows(execute("SELECT " + aStateNullFinalNonNull + "(i) FROM %s WHERE key = 1"), row(1));
        assertRows(execute("SELECT " + aStateNonNull + "(i) FROM %s WHERE key = 1"), row(1));
        assertRows(execute("SELECT " + aStateNonNullFinalNull + "(i) FROM %s WHERE key = 1"), row(1));
        assertRows(execute("SELECT " + aStateNonNullFinalNonNull + "(i) FROM %s WHERE key = 1"), row(1));
        assertRows(execute("SELECT " + aStateAlwaysNullFinalNull + "(i) FROM %s WHERE key = 1"), row(new Object[]{null}));
        assertRows(execute("SELECT " + aStateAlwaysNullFinalNonNull + "(i) FROM %s WHERE key = 1"), row(new Object[]{null}));

        assertRows(execute("SELECT " + aStateNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6));
        assertRows(execute("SELECT " + aStateNullFinalNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6));
        assertRows(execute("SELECT " + aStateNullFinalNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6));
        assertRows(execute("SELECT " + aStateNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6));
        assertRows(execute("SELECT " + aStateNonNullFinalNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6));
        assertRows(execute("SELECT " + aStateNonNullFinalNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6));
        assertRows(execute("SELECT " + aStateAlwaysNullFinalNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(new Object[]{null}));
        assertRows(execute("SELECT " + aStateAlwaysNullFinalNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(new Object[]{null}));
    }

    @Test
    public void testBrokenAggregate() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val int)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1);

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE javascript " +
                                       "AS 'a + b;'");

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int ");

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace());
        UDAggregate f = (UDAggregate) ksm.functions.get(parseFunctionName(a)).iterator().next();

        UDAggregate broken = UDAggregate.createBroken(f.name(),
                                                      f.argTypes(),
                                                      f.returnType(),
                                                      null,
                                                      new InvalidRequestException("foo bar is broken"));

        Schema.instance.setKeyspaceMetadata(ksm.withSwapped(ksm.functions.without(f.name(), f.argTypes()).with(broken)));

        assertInvalidThrowMessage("foo bar is broken", InvalidRequestException.class,
                                  "SELECT " + a + "(val) FROM %s");
    }

    @Test
    public void testWrongStateType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val int)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1);

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS double " +
                                       "LANGUAGE java " +
                                       "AS 'return Double.valueOf(1.0);'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf(1);';");

        assertInvalidMessage("return type must be the same as the first argument type - check STYPE, argument and return types",
                             "CREATE AGGREGATE %s(int) " +
                             "SFUNC " + shortFunctionName(fState) + ' ' +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + ' ' +
                             "INITCOND 1");
    }

    @Test
    public void testWrongKeyspace() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (txt text, i int)");
        String type = KEYSPACE + '.' + typeName;

        String fState = createFunction(KEYSPACE_PER_TEST,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS double " +
                                       "LANGUAGE java " +
                                       "AS 'return Double.valueOf(1.0);'");

        String fFinal = createFunction(KEYSPACE_PER_TEST,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf(1);';");

        String fStateWrong = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS double " +
                                       "LANGUAGE java " +
                                       "AS 'return Double.valueOf(1.0);'");

        String fFinalWrong = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf(1);';");

        assertInvalidMessage(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; user types can only be used in the keyspace they are defined in",
                                           KEYSPACE_PER_TEST, KEYSPACE),
                             "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                             "SFUNC " + shortFunctionName(fState) + ' ' +
                             "STYPE " + type + " " +
                             "FINALFUNC " + shortFunctionName(fFinal) + ' ' +
                             "INITCOND 1");

        assertInvalidMessage("mismatched input", // specifying a function using "keyspace.functionname" is a syntax error
                             "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                             "SFUNC " + fStateWrong + ' ' +
                             "STYPE " + type + " " +
                             "FINALFUNC " + shortFunctionName(fFinal) + ' ' +
                             "INITCOND 1");

        assertInvalidMessage("expecting EOF", // specifying a function using "keyspace.functionname" is a syntax error
                             "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                             "SFUNC " + shortFunctionName(fState) + ' ' +
                             "STYPE " + type + " " +
                             "FINALFUNC " + fFinalWrong + ' ' +
                             "INITCOND 1");

        assertInvalidMessage("expecting EOF", // specifying a function using "keyspace.functionname" is a syntax error
                             "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                             "SFUNC " + shortFunctionName(fState) + ' ' +
                             "STYPE " + type + ' ' +
                             "FINALFUNC " + SchemaConstants.SYSTEM_KEYSPACE_NAME + ".min " +
                             "INITCOND 1");
    }

    @Test
    public void testFunctionWithFrozenSetType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<set<int>>)");
        createIndex("CREATE INDEX ON %s (FULL(b))");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, set());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, set(1, 2, 3));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, set(4, 5, 6));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, set(7, 8, 9));

        String fState = createFunction(KEYSPACE,
                                       "set<int>",
                                       "CREATE FUNCTION %s (state set<int>, values set<int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS set<int> " +
                                       "LANGUAGE java\n" +
                                       "AS 'return values;';");

        String fFinal = createFunction(KEYSPACE,
                                       "set<int>",
                                       "CREATE FUNCTION %s(state set<int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS set<int> " +
                                       "LANGUAGE java " +
                                       "AS 'return state;'");

        assertInvalidMessage("The function state type should not be frozen",
                             "CREATE AGGREGATE %s(set<int>) " +
                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                             "STYPE frozen<set<int>> " +
                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "set<int>",
                                             "CREATE AGGREGATE %s(set<int>) " +
                                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                                             "STYPE set<int> " +
                                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                                             "INITCOND null");

        assertRows(execute("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, shortFunctionName(aggregation)),
                   row((Object) null));

        assertRows(execute("SELECT " + aggregation + "(b) FROM %s"),
                   row(set(7, 8, 9)));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP AGGREGATE %s (frozen<set<int>>);");
    }

    @Test
    public void testFunctionWithFrozenListType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<list<int>>)");
        createIndex("CREATE INDEX ON %s (FULL(b))");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, list());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, list(1, 2, 3));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, list(4, 5, 6));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, list(7, 8, 9));

        String fState = createFunction(KEYSPACE,
                                       "list<int>",
                                       "CREATE FUNCTION %s (state list<int>, values list<int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS list<int> " +
                                       "LANGUAGE java\n" +
                                       "AS 'return values;';");

        String fFinal = createFunction(KEYSPACE,
                                       "list<int>",
                                       "CREATE FUNCTION %s(state list<int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS list<int> " +
                                       "LANGUAGE java " +
                                       "AS 'return state;'");

        assertInvalidMessage("The function state type should not be frozen",
                             "CREATE AGGREGATE %s(list<int>) " +
                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                             "STYPE frozen<list<int>> " +
                             "FINALFUNC " + parseFunctionName(fFinal).name + " " +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "list<int>",
                                             "CREATE AGGREGATE %s(list<int>) " +
                                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                                             "STYPE list<int> " +
                                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                                             "INITCOND null");

        assertRows(execute("SELECT " + aggregation + "(b) FROM %s"),
                   row(list(7, 8, 9)));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP AGGREGATE %s (frozen<list<int>>);");
    }

    @Test
    public void testFunctionWithFrozenMapType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<map<int, int>>)");
        createIndex("CREATE INDEX ON %s (FULL(b))");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, map());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, map(1, 2, 3, 4));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, map(4, 5, 6, 7));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, map(7, 8, 9, 10));

        String fState = createFunction(KEYSPACE,
                                       "map<int, int>",
                                       "CREATE FUNCTION %s (state map<int, int>, values map<int, int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS map<int, int> " +
                                       "LANGUAGE java\n" +
                                       "AS 'return values;';");

        String fFinal = createFunction(KEYSPACE,
                                       "map<int, int>",
                                       "CREATE FUNCTION %s(state map<int, int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS map<int, int> " +
                                       "LANGUAGE java " +
                                       "AS 'return state;'");

        assertInvalidMessage("The function state type should not be frozen",
                             "CREATE AGGREGATE %s(map<int, int>) " +
                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                             "STYPE frozen<map<int, int>> " +
                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "map<int, int>",
                                             "CREATE AGGREGATE %s(map<int, int>) " +
                                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                                             "STYPE map<int, int> " +
                                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                                             "INITCOND null");

        assertRows(execute("SELECT " + aggregation + "(b) FROM %s"),
                   row(map(7, 8, 9, 10)));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP AGGREGATE %s (frozen<map<int, int>>);");
    }

    @Test
    public void testFunctionWithFrozenTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<tuple<int, int>>)");
        createIndex("CREATE INDEX ON %s (b)");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, tuple());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, tuple(1, 2));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, tuple(4, 5));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, tuple(7, 8));

        String fState = createFunction(KEYSPACE,
                                       "tuple<int, int>",
                                       "CREATE FUNCTION %s (state tuple<int, int>, values tuple<int, int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS tuple<int, int> " +
                                       "LANGUAGE java\n" +
                                       "AS 'return values;';");

        String fFinal = createFunction(KEYSPACE,
                                       "tuple<int, int>",
                                       "CREATE FUNCTION %s(state tuple<int, int>) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS tuple<int, int> " +
                                       "LANGUAGE java " +
                                       "AS 'return state;'");

        assertInvalidMessage("The function state type should not be frozen",
                             "CREATE AGGREGATE %s(tuple<int, int>) " +
                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                             "STYPE frozen<tuple<int, int>> " +
                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "tuple<int, int>",
                                             "CREATE AGGREGATE %s(tuple<int, int>) " +
                                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                                             "STYPE tuple<int, int> " +
                                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                                             "INITCOND null");

        assertRows(execute("SELECT " + aggregation + "(b) FROM %s"),
                   row(tuple(7, 8)));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP AGGREGATE %s (frozen<tuple<int, int>>);");
    }

    @Test
    public void testFunctionWithFrozenUDFType() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + myType + ">)");
        createIndex("CREATE INDEX ON %s (b)");

        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 0, 1);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 1, 2);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 2, 4);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 3, 7);

        String fState = createFunction(KEYSPACE,
                                       myType + ',' + myType,
                                       "CREATE FUNCTION %s (state " + myType + ", values " + myType + ") " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS " + myType + " " +
                                       "LANGUAGE java\n" +
                                       "AS 'return values;';");

        String fFinal = createFunction(KEYSPACE,
                                       myType,
                                       "CREATE FUNCTION %s(state " + myType + ") " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS " + myType + " " +
                                       "LANGUAGE java " +
                                       "AS 'return state;'");

        assertInvalidMessage("The function state type should not be frozen",
                             "CREATE AGGREGATE %s(" + myType + ") " +
                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                             "STYPE frozen<" + myType + "> " +
                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             myType,
                                             "CREATE AGGREGATE %s(" + myType + ") " +
                                             "SFUNC " + parseFunctionName(fState).name + ' ' +
                                             "STYPE " + myType + ' ' +
                                             "FINALFUNC " + parseFunctionName(fFinal).name + ' ' +
                                             "INITCOND null");

        assertRows(execute("SELECT " + aggregation + "(b).f FROM %s"),
                   row(7));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP AGGREGATE %s (frozen<" + myType + ">);");
    }

    @Test
    public void testEmptyValues() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b text)");
        execute("INSERT INTO %s (a, b) VALUES (1, '')");
        execute("INSERT INTO %s (a, b) VALUES (2, '')");
        execute("INSERT INTO %s (a, b) VALUES (3, '')");

        String fCON = createFunction(KEYSPACE,
                                     "text, text",
                                     "CREATE FUNCTION %s(a text, b text) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS text " +
                                     "LANGUAGE java " +
                                     "AS 'return a + \"x\" + b + \"y\";'");

        String fCONf = createFunction(KEYSPACE,
                                     "text",
                                     "CREATE FUNCTION %s(a text) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS text " +
                                     "LANGUAGE java " +
                                     "AS 'return \"fin\" + a;'");

        String aCON = createAggregate(KEYSPACE,
                                      "text",
                                      "CREATE AGGREGATE %s(text) " +
                                      "SFUNC " + shortFunctionName(fCON) + ' ' +
                                      "STYPE text " +
                                      "FINALFUNC " + shortFunctionName(fCONf) + ' ' +
                                      "INITCOND ''");

        assertRows(execute("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, shortFunctionName(aCON)),
                   row("''"));

        String fRNON = createFunction(KEYSPACE,
                                      "text, text",
                                      "CREATE FUNCTION %s(a text, b text) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java " +
                                      "AS 'return a + \"x\" + b + \"y\";'");

        String fRNONf = createFunction(KEYSPACE,
                                      "text",
                                      "CREATE FUNCTION %s(a text) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java " +
                                      "AS 'return \"fin\" + a;'");

        String aRNON = createAggregate(KEYSPACE,
                                      "text",
                                      "CREATE AGGREGATE %s(text) " +
                                      "SFUNC " + shortFunctionName(fRNON) + ' ' +
                                      "STYPE text " +
                                      "FINALFUNC " + shortFunctionName(fRNONf) + ' ' +
                                      "INITCOND ''");

        assertRows(execute("SELECT " + aCON + "(b) FROM %s"), row("finxyxyxy"));
        assertRows(execute("SELECT " + aRNON + "(b) FROM %s"), row("finxyxyxy"));

        createTable("CREATE TABLE %s (a int primary key, b text)");
        execute("INSERT INTO %s (a, b) VALUES (1, null)");
        execute("INSERT INTO %s (a, b) VALUES (2, null)");
        execute("INSERT INTO %s (a, b) VALUES (3, null)");

        assertRows(execute("SELECT " + aCON + "(b) FROM %s"), row("finxnullyxnullyxnully"));
        assertRows(execute("SELECT " + aRNON + "(b) FROM %s"), row("fin"));

    }

    @Test
    public void testEmptyListAndNullInitcond() throws Throwable
    {
        String f = createFunction(KEYSPACE,
                                      "list, int",
                                      "CREATE FUNCTION %s(s list<text>, i int) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS list<text> " +
                                      "LANGUAGE java " +
                                      "AS 'if (i != null) s.add(String.valueOf(i)); return s;'");

        String a = createAggregate(KEYSPACE,
                                       "int",
                                       "CREATE AGGREGATE %s(int) " +
                                       "SFUNC " + shortFunctionName(f) + ' ' +
                                       "STYPE list<text> " +
                                       "INITCOND [  ]");

        assertRows(execute("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, shortFunctionName(a)),
                   row("[]"));

        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, null)");
        execute("INSERT INTO %s (a, b) VALUES (3, 2)");
        assertRows(execute("SELECT " + a + "(b) FROM %s"), row(Arrays.asList("1", "2")));
    }

    @Test
    public void testLogbackReload() throws Throwable
    {
        // see https://issues.apache.org/jira/browse/CASSANDRA-11033

        // make logback's scan interval 1ms - boilerplate, but necessary for this test
        configureLogbackScanPeriod(1L);
        try
        {

            createTable("CREATE TABLE %s (" +
                        "   year int PRIMARY KEY," +
                        "   country text," +
                        "   title text)");

            String[] countries = Locale.getISOCountries();
            ThreadLocalRandom rand = ThreadLocalRandom.current();
            for (int i = 0; i < 10000; i++)
            {
                execute("INSERT INTO %s (year, country, title) VALUES (1980,?,?)",
                        countries[rand.nextInt(countries.length)],
                        "title-" + i);
            }

            String albumCountByCountry = createFunction(KEYSPACE,
                                                        "map<text,bigint>,text,text",
                                                        "CREATE FUNCTION IF NOT EXISTS %s(state map<text,bigint>,country text, album_title text)\n" +
                                                        " RETURNS NULL ON NULL INPUT\n" +
                                                        " RETURNS map<text,bigint>\n" +
                                                        " LANGUAGE java\n" +
                                                        " AS $$\n" +
                                                        "   if(state.containsKey(country)) {\n" +
                                                        "       Long newCount = (Long)state.get(country) + 1;\n" +
                                                        "       state.put(country, newCount);\n" +
                                                        "   } else {\n" +
                                                        "       state.put(country, 1L);\n" +
                                                        "   }\n" +
                                                        "   return state;\n" +
                                                        " $$;");

            String releasesByCountry = createAggregate(KEYSPACE,
                                                       "text, text",
                                                       " CREATE AGGREGATE IF NOT EXISTS %s(text, text)\n" +
                                                       " SFUNC " + shortFunctionName(albumCountByCountry) + '\n' +
                                                       " STYPE map<text,bigint>\n" +
                                                       " INITCOND { };");

            long tEnd = System.currentTimeMillis() + 150;
            while (System.currentTimeMillis() < tEnd)
            {
                execute("SELECT " + releasesByCountry + "(country,title) FROM %s WHERE year=1980");
            }
        }
        finally
        {
            configureLogbackScanPeriod(60000L);
        }
    }

    private static void configureLogbackScanPeriod(long millis)
    {
        Logger l = LoggerFactory.getLogger(AggregationTest.class);
        ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) l;
        LoggerContext ctx = logbackLogger.getLoggerContext();
        TurboFilterList turboFilterList = ctx.getTurboFilterList();
        boolean done = false;
        for (TurboFilter turboFilter : turboFilterList)
        {
            if (turboFilter instanceof ReconfigureOnChangeFilter)
            {
                ReconfigureOnChangeFilter reconfigureFilter = (ReconfigureOnChangeFilter) turboFilter;
                reconfigureFilter.setContext(ctx);
                reconfigureFilter.setRefreshPeriod(millis);
                reconfigureFilter.stop();
                reconfigureFilter.start(); // start() sets the next check timestammp
                done = true;
                break;
            }
        }
        assertTrue("ReconfigureOnChangeFilter not in logback's turbo-filter list - do that by adding scan=\"true\" to logback-test.xml's configuration element", done);
    }

    @Test
    public void testOrReplaceOptionals() throws Throwable
    {
        String fState = createFunction(KEYSPACE,
                                       "list<text>, int",
                                       "CREATE FUNCTION %s(s list<text>, i int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS list<text> " +
                                       "LANGUAGE java " +
                                       "AS 'if (i != null) s.add(String.valueOf(i)); return s;'");

        String fFinal = shortFunctionName(createFunction(KEYSPACE,
                                                         "list<text>",
                                                         "CREATE FUNCTION %s(s list<text>) " +
                                                         "CALLED ON NULL INPUT " +
                                                         "RETURNS list<text> " +
                                                         "LANGUAGE java " +
                                                         "AS 'return s;'"));

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + ' ' +
                                   "STYPE list<text> ");

        checkOptionals(a, null, null);

        String ddlPrefix = "CREATE OR REPLACE AGGREGATE " + a + "(int) " +
                           "SFUNC " + shortFunctionName(fState) + ' ' +
                           "STYPE list<text> ";

        // Test replacing INITCOND
        execute(ddlPrefix + "INITCOND [  ] ");
        checkOptionals(a, null, "[]");

        execute(ddlPrefix);
        checkOptionals(a, null, null);

        execute(ddlPrefix + "INITCOND [  ] ");
        checkOptionals(a, null, "[]");

        execute(ddlPrefix + "INITCOND null");
        checkOptionals(a, null, null);

        // Test replacing FINALFUNC
        execute(ddlPrefix + "FINALFUNC " + shortFunctionName(fFinal) + ' ');
        checkOptionals(a, shortFunctionName(fFinal), null);

        execute(ddlPrefix);
        checkOptionals(a, null, null);
    }

    private void checkOptionals(String aggregateName, String finalFunc, String initCond) throws Throwable
    {
        assertRows(execute("SELECT final_func, initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, shortFunctionName(aggregateName)),
                   row(finalFunc, initCond));
    }

    @Test
    public void testCustomTypeInitcond() throws Throwable
    {
        try
        {
            String type = "DynamicCompositeType(s => UTF8Type, i => Int32Type)";

            executeNet(ProtocolVersion.CURRENT,
                       "CREATE FUNCTION " + KEYSPACE + ".f11064(i 'DynamicCompositeType(s => UTF8Type, i => Int32Type)')\n" +
                       "RETURNS NULL ON NULL INPUT\n" +
                       "RETURNS '" + type + "'\n" +
                       "LANGUAGE java\n" +
                       "AS 'return i;'");

            // create aggregate using the 'composite syntax' for composite types
            executeNet(ProtocolVersion.CURRENT,
                       "CREATE AGGREGATE " + KEYSPACE + ".a11064()\n" +
                       "SFUNC f11064 " +
                       "STYPE '" + type + "'\n" +
                       "INITCOND 's@foo:i@32'");

            AbstractType<?> compositeType = TypeParser.parse(type);
            ByteBuffer compositeTypeValue = compositeType.fromString("s@foo:i@32");
            String compositeTypeString = compositeType.asCQL3Type().toCQLLiteral(compositeTypeValue, ProtocolVersion.CURRENT);
            // ensure that the composite type is serialized using the 'blob syntax'
            assertTrue(compositeTypeString.startsWith("0x"));

            // ensure that the composite type is 'serialized' using the 'blob syntax' in the schema
            assertRows(execute("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, "a11064"),
                       row(compositeTypeString));

            // create aggregate using the 'blob syntax' for composite types
            executeNet(ProtocolVersion.CURRENT,
                       "CREATE AGGREGATE " + KEYSPACE + ".a11064_2()\n" +
                       "SFUNC f11064 " +
                       "STYPE '" + type + "'\n" +
                       "INITCOND " + compositeTypeString);

            // ensure that the composite type is 'serialized' using the 'blob syntax' in the schema
            assertRows(execute("SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", KEYSPACE, "a11064_2"),
                       row(compositeTypeString));
        }
        finally
        {
            try
            {
                execute("DROP AGGREGATE " + KEYSPACE + ".a11064_2");
            }
            catch (Exception ignore)
            {
            }
            try
            {
                execute("DROP AGGREGATE " + KEYSPACE + ".a11064");
            }
            catch (Exception ignore)
            {
            }
            try
            {
                execute("DROP FUNCTION " + KEYSPACE + ".f11064");
            }
            catch (Exception ignore)
            {
            }
        }
    }

    @Test
    public void testArithmeticCorrectness() throws Throwable
    {
        createTable("create table %s (bucket int primary key, val decimal)");
        execute("insert into %s (bucket, val) values (1, 0.25)");
        execute("insert into %s (bucket, val) values (2, 0.25)");
        execute("insert into %s (bucket, val) values (3, 0.5);");

        BigDecimal a = new BigDecimal("0.25");
        a = a.add(new BigDecimal("0.25"));
        a = a.add(new BigDecimal("0.5"));
        a = a.divide(new BigDecimal(3), RoundingMode.HALF_EVEN);

        assertRows(execute("select avg(val) from %s where bucket in (1, 2, 3);"),
                   row(a));
    }

    @Test
    public void testAggregatesWithoutOverflow() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 tinyint, v2 smallint, v3 int, v4 bigint, v5 varint)");
        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    (byte) ((Byte.MAX_VALUE / 3) + i), (short) ((Short.MAX_VALUE / 3) + i), (Integer.MAX_VALUE / 3) + i, (Long.MAX_VALUE / 3) + i,
                    BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(i)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row((byte) ((Byte.MAX_VALUE / 3) + 2), (short) ((Short.MAX_VALUE / 3) + 2), (Integer.MAX_VALUE / 3) + 2, (Long.MAX_VALUE / 3) + 2,
                       BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(2))));

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i + 3,
                    (byte) (100 + i), (short) (100 + i), 100 + i, 100L + i, BigInteger.valueOf(100 + i));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (4, 5, 6);"),
                   row((byte) 102, (short) 102, 102, 102L, BigInteger.valueOf(102)));
    }

    @Test
    public void testAggregateOverflow() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 tinyint, v2 smallint, v3 int, v4 bigint, v5 varint)");
        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row(Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2))));

        execute("truncate %s");

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE, BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row(Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE, BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2))));

    }

    @Test
    public void testDoubleAggregatesPrecision() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double, v3 decimal)");

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", i,
                    Float.MAX_VALUE, Double.MAX_VALUE, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3) from %s where bucket in (1, 2, 3);"),
                   row(Float.MAX_VALUE, Double.MAX_VALUE, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(2))));

        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 4, (float) 100.10, 100.10, BigDecimal.valueOf(100.10));
        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 5, (float) 110.11, 110.11, BigDecimal.valueOf(110.11));
        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 6, (float) 120.12, 120.12, BigDecimal.valueOf(120.12));

        assertRows(execute("select avg(v1), avg(v2), avg(v3) from %s where bucket in (4, 5, 6);"),
                   row((float) 110.11, 110.11, BigDecimal.valueOf(110.11)));
    }

    @Test
    public void testNan() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double)");

        for (int i = 1; i <= 10; i++)
            if (i != 5)
                execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", i, (float) i, (double) i);

        execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", 5, Float.NaN, Double.NaN);

        assertRows(execute("select avg(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                   row(Float.NaN, Double.NaN));
        assertRows(execute("select sum(v1), sum(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                   row(Float.NaN, Double.NaN));
    }

    @Test
    public void testInfinity() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double)");
        for (boolean positive: new boolean[] { true, false})
        {
            final float FLOAT_INFINITY = positive ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
            final double DOUBLE_INFINITY = positive ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;

            for (int i = 1; i <= 10; i++)
                if (i != 5)
                    execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", i, (float) i, (double) i);

            execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", 5, FLOAT_INFINITY, DOUBLE_INFINITY);

            assertRows(execute("select avg(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                       row(FLOAT_INFINITY, DOUBLE_INFINITY));
            assertRows(execute("select sum(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                       row(FLOAT_INFINITY, DOUBLE_INFINITY));

            execute("truncate %s");
        }
    }

    @Test
    public void testSumPrecision() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double, v3 decimal)");

        for (int i = 1; i <= 17; i++)
            execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", i, (float) (i / 10.0), i / 10.0, BigDecimal.valueOf(i / 10.0));

        assertRows(execute("select sum(v1), sum(v2), sum(v3) from %s;"),
                   row((float) 15.3, 15.3, BigDecimal.valueOf(15.3)));
    }
}
