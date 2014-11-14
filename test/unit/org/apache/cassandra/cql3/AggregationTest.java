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
package org.apache.cassandra.cql3;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

public class AggregationTest extends CQLTester
{
    @Test
    public void testFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c double, d decimal, primary key (a, b))");

        // Test with empty table
        assertColumnNames(execute("SELECT COUNT(*) FROM %s"), "count");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(0L));
        assertColumnNames(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c), sum(d), avg(d) FROM %s"),
                          "system.max(b)", "system.min(b)", "system.sum(b)", "system.avg(b)" , "system.max(c)", "system.sum(c)", "system.avg(c)", "system.sum(d)", "system.avg(d)");
        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c), sum(d), avg(d) FROM %s"),
                   row(null, null, 0, 0, null, 0.0, 0.0, new BigDecimal("0"), new BigDecimal("0")));

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 1, 11.5, 11.5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 9.5, 1.5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 3, 9.0, 2.0)");

        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c), sum(d), avg(d) FROM %s"),
                   row(3, 1, 6, 2, 11.5, 30.0, 10.0, new BigDecimal("15.0"), new BigDecimal("5.0")));

        execute("INSERT INTO %s (a, b, d) VALUES (1, 5, 1.0)");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(b), count(c) FROM %s"), row(4L, 3L));
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
        assertInvalid("SELECT max(b), c FROM %s");
        assertInvalid("SELECT b, max(c) FROM %s");
        assertInvalid("SELECT max(sum(c)) FROM %s");
        assertInvalidSyntax("SELECT COUNT(2) FROM %s");
    }

    @Test
    public void testNestedFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b timeuuid, c double, d double)");

        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".copySign(magnitude double, sign double) RETURNS double LANGUAGE JAVA\n" +
                "AS 'return Double.valueOf(Math.copySign(magnitude.doubleValue(), sign.doubleValue()));';");

        assertColumnNames(execute("SELECT max(a), max(unixTimestampOf(b)) FROM %s"), "system.max(a)", "system.max(system.unixtimestampof(b))");
        assertRows(execute("SELECT max(a), max(unixTimestampOf(b)) FROM %s"), row(null, null));
        assertColumnNames(execute("SELECT max(a), unixTimestampOf(max(b)) FROM %s"), "system.max(a)", "system.unixtimestampof(system.max(b))");
        assertRows(execute("SELECT max(a), unixTimestampOf(max(b)) FROM %s"), row(null, null));

        assertColumnNames(execute("SELECT max(copySign(c, d)) FROM %s"), "system.max("+KEYSPACE+".copysign(c, d))");
        assertRows(execute("SELECT max(copySign(c, d)) FROM %s"), row((Object) null));

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, maxTimeuuid('2011-02-03 04:05:00+0000'), -1.2, 2.1)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, maxTimeuuid('2011-02-03 04:06:00+0000'), 1.3, -3.4)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (3, maxTimeuuid('2011-02-03 04:10:00+0000'), 1.4, 1.2)");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = format.parse("2011-02-03 04:10:00");
        date = DateUtils.truncate(date, Calendar.MILLISECOND);

        assertRows(execute("SELECT max(a), max(unixTimestampOf(b)) FROM %s"), row(3, date.getTime()));
        assertRows(execute("SELECT max(a), unixTimestampOf(max(b)) FROM %s"), row(3, date.getTime()));

        assertRows(execute("SELECT copySign(max(c), min(c)) FROM %s"), row(-1.4));
        assertRows(execute("SELECT copySign(c, d) FROM %s"), row(1.2), row(-1.3), row(1.4));
        assertRows(execute("SELECT max(copySign(c, d)) FROM %s"), row(1.4));
        assertInvalid("SELECT copySign(c, max(c)) FROM %s");
        assertInvalid("SELECT copySign(max(c), c) FROM %s");
    }
}
