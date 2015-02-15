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
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

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
                          "system.max(b)", "system.min(b)", "system.sum(b)", "system.avg(b)", "system.max(c)", "system.sum(c)", "system.avg(c)", "system.sum(d)", "system.avg(d)");
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
        assertInvalidMessage("only aggregates or no aggregate", "SELECT max(b), c FROM %s");
        assertInvalidMessage("only aggregates or no aggregate", "SELECT b, max(c) FROM %s");
        assertInvalidMessage("aggregate functions cannot be used as arguments of aggregate functions", "SELECT max(sum(c)) FROM %s");
        assertInvalidSyntax("SELECT COUNT(2) FROM %s");
    }

    @Test
    public void testNestedFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b timeuuid, c double, d double)");

        String copySign = createFunction(KEYSPACE,
                                         "double, double",
                                         "CREATE OR REPLACE FUNCTION %s(magnitude double, sign double) " +
                                         "RETURNS double " +
                                         "LANGUAGE JAVA " +
                                         "AS 'return Double.valueOf(Math.copySign(magnitude.doubleValue(), sign.doubleValue()));';");

        assertColumnNames(execute("SELECT max(a), max(unixTimestampOf(b)) FROM %s"), "system.max(a)", "system.max(system.unixtimestampof(b))");
        assertRows(execute("SELECT max(a), max(unixTimestampOf(b)) FROM %s"), row(null, null));
        assertColumnNames(execute("SELECT max(a), unixTimestampOf(max(b)) FROM %s"), "system.max(a)", "system.unixtimestampof(system.max(b))");
        assertRows(execute("SELECT max(a), unixTimestampOf(max(b)) FROM %s"), row(null, null));

        assertColumnNames(execute("SELECT max(" + copySign + "(c, d)) FROM %s"), "system.max(" + copySign + "(c, d))");
        assertRows(execute("SELECT max(" + copySign + "(c, d)) FROM %s"), row((Object) null));

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, maxTimeuuid('2011-02-03 04:05:00+0000'), -1.2, 2.1)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, maxTimeuuid('2011-02-03 04:06:00+0000'), 1.3, -3.4)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (3, maxTimeuuid('2011-02-03 04:10:00+0000'), 1.4, 1.2)");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = format.parse("2011-02-03 04:10:00");
        date = DateUtils.truncate(date, Calendar.MILLISECOND);

        assertRows(execute("SELECT max(a), max(unixTimestampOf(b)) FROM %s"), row(3, date.getTime()));
        assertRows(execute("SELECT max(a), unixTimestampOf(max(b)) FROM %s"), row(3, date.getTime()));

        assertRows(execute("SELECT " + copySign + "(max(c), min(c)) FROM %s"), row(-1.4));
        assertRows(execute("SELECT " + copySign + "(c, d) FROM %s"), row(1.2), row(-1.3), row(1.4));
        assertRows(execute("SELECT max(" + copySign + "(c, d)) FROM %s"), row(1.4));
        assertInvalidMessage("must be either all aggregates or no aggregates", "SELECT " + copySign + "(c, max(c)) FROM %s");
        assertInvalidMessage("must be either all aggregates or no aggregates", "SELECT " + copySign + "(max(c), c) FROM %s");
    }

    @Test
    public void testSchemaChange() throws Throwable
    {
        String f = createFunction(KEYSPACE,
                                  "double, double",
                                  "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                  "RETURNS double " +
                                  "LANGUAGE javascript " +
                                  "AS '\"string\";';");

        createFunctionOverload(f,
                               "double, double",
                               "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                               "RETURNS int " +
                               "LANGUAGE javascript " +
                               "AS '\"string\";';");

        String a = createAggregate(KEYSPACE,
                                   "double",
                                   "CREATE OR REPLACE AGGREGATE %s(double) " +
                                   "SFUNC " + shortFunctionName(f) + " " +
                                   "STYPE double");

        assertLastSchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.AGGREGATE,
                               KEYSPACE, parseFunctionName(a).name,
                               "double");

        schemaChange("CREATE OR REPLACE AGGREGATE " + a + "(double) " +
                     "SFUNC " + shortFunctionName(f) + " " +
                     "STYPE double");

        assertLastSchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.AGGREGATE,
                               KEYSPACE, parseFunctionName(a).name,
                               "double");

        createAggregateOverload(a,
                                "int",
                                "CREATE OR REPLACE AGGREGATE %s(int) " +
                                "SFUNC " + shortFunctionName(f) + " " +
                                "STYPE int");

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
                                  "RETURNS double " +
                                  "LANGUAGE javascript " +
                                  "AS '\"string\";';");

        createFunctionOverload(f,
                               "double, double",
                               "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
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
                                   "STYPE double");
        createAggregateOverload(a,
                                "int",
                                "CREATE OR REPLACE AGGREGATE %s(int) " +
                                "SFUNC " + shortFunctionName(f) + " " +
                                "STYPE int");

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
                                  "RETURNS double " +
                                  "LANGUAGE javascript " +
                                  "AS '\"string\";';");

        String a = createAggregate(KEYSPACE,
                                   "double",
                                   "CREATE OR REPLACE AGGREGATE %s(double) " +
                                   "SFUNC " + shortFunctionName(f) + " " +
                                   "STYPE double");

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
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
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
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
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
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
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
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return a.toString();'");

        String fState2 = createFunction(KEYSPACE,
                                        "int, int",
                                        "CREATE FUNCTION %s(a double, b double) " +
                                        "RETURNS double " +
                                        "LANGUAGE java " +
                                        "AS 'return Double.valueOf((a!=null?a.doubleValue():0d) + b.doubleValue());'");

        String fFinal2 = createFunction(KEYSPACE,
                                        "int",
                                        "CREATE FUNCTION %s(a double) " +
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
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
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
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'throw new RuntimeException();'");

        String fStateOK = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf(42);'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'throw new RuntimeException();'");

        String fFinalOK = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
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
                                  "RETURNS int " +
                                  "LANGUAGE java " +
                                  "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

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
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
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
                                       "frozen<tuple<bigint, int>>, int",
                                       "CREATE FUNCTION %s(a frozen<tuple<bigint, int>>, b int) " +
                                       "RETURNS frozen<tuple<bigint, int>> " +
                                       "LANGUAGE java " +
                                       "AS '" +
                                       "a.setLong(0, a.getLong(0) + b.intValue());" +
                                       "a.setInt(1, a.getInt(1) + 1);" +
                                       "return a;" +
                                       "'");

        String fFinal = createFunction(KEYSPACE,
                                       "frozen<tuple<bigint, int>>",
                                       "CREATE FUNCTION %s(a frozen<tuple<bigint, int>>) " +
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
                                   "STYPE frozen<tuple<bigint, int>> "+
                                   "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                   "INITCOND (0, 0)");

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
                                       "RETURNS int " +
                                       "LANGUAGE javascript " +
                                       "AS 'a + b;'");

        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s(a int) " +
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
                                           "RETURNS int " +
                                           "LANGUAGE javascript " +
                                           "AS 'a + b;'");

            String a = createAggregate(otherKS,
                                       "int",
                                       "CREATE AGGREGATE %s(int) " +
                                       "SFUNC " + shortFunctionName(fState) + " " +
                                       "STYPE int");

            ResultMessage.Prepared prepared = QueryProcessor.prepare("SELECT " + a + "(b) FROM " + otherKS + ".jsdp", ClientState.forInternalCalls(), false);
            Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

            execute("DROP AGGREGATE " + a + "(int)");
            Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));

            //

            execute("CREATE AGGREGATE " + a + "(int) " +
                    "SFUNC " + shortFunctionName(fState) + " " +
                    "STYPE int");

            prepared = QueryProcessor.prepare("SELECT " + a + "(b) FROM " + otherKS + ".jsdp", ClientState.forInternalCalls(), false);
            Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

            execute("DROP KEYSPACE " + otherKS + ";");

            Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));
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
    public void testBrokenAggregate() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val int)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1);

        String fState = createFunction(KEYSPACE,
                                       "int, int",
                                       "CREATE FUNCTION %s(a int, b int) " +
                                       "RETURNS int " +
                                       "LANGUAGE javascript " +
                                       "AS 'a + b;'");

        String a = createAggregate(KEYSPACE,
                                   "int",
                                   "CREATE AGGREGATE %s(int) " +
                                   "SFUNC " + shortFunctionName(fState) + " " +
                                   "STYPE int ");

        UDAggregate f = (UDAggregate) Functions.find(parseFunctionName(a)).get(0);

        Functions.replaceFunction(UDAggregate.createBroken(f.name(), f.argTypes(), f.returnType(),
                                                           null, new InvalidRequestException("foo bar is broken")));

        assertInvalidThrowMessage("foo bar is broken", InvalidRequestException.class,
                                  "SELECT " + a + "(val) FROM %s");
    }

}
