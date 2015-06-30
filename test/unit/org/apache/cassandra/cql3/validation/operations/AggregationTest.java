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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.Int32Serializer;
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
        assertInvalidMessage("must be either all aggregates or no aggregates", "SELECT " + copySign + "(c, max(c)) FROM %s");
        assertInvalidMessage("must be either all aggregates or no aggregates", "SELECT " + copySign + "(max(c), c) FROM %s");
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

        KSMetaData ksm = Schema.instance.getKSMetaData(keyspace());
        UDAggregate f = (UDAggregate) ksm.functions.get(parseFunctionName(a)).iterator().next();

        UDAggregate broken = UDAggregate.createBroken(f.name(),
                                                      f.argTypes(),
                                                      f.returnType(),
                                                      null,
                                                      new InvalidRequestException("foo bar is broken"));

        Schema.instance.setKeyspaceDefinition(ksm.cloneWith(ksm.functions.without(f.name(), f.argTypes()).with(broken)));

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

        assertInvalidMessage(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; user types can only be used in the keyspace they are defined in",
                                           KEYSPACE_PER_TEST, KEYSPACE),
                             "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                             "SFUNC " + fStateWrong + ' ' +
                             "STYPE " + type + " " +
                             "FINALFUNC " + shortFunctionName(fFinal) + ' ' +
                             "INITCOND 1");

        assertInvalidMessage(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; user types can only be used in the keyspace they are defined in",
                                           KEYSPACE_PER_TEST, KEYSPACE),
                             "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                             "SFUNC " + shortFunctionName(fState) + ' ' +
                             "STYPE " + type + " " +
                             "FINALFUNC " + fFinalWrong + ' ' +
                             "INITCOND 1");
    }

    @Test
    public void testSystemKeyspace() throws Throwable
    {
        String fState = createFunction(KEYSPACE,
                                       "text, text",
                                       "CREATE FUNCTION %s(a text, b text) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return \"foobar\";'");

        createAggregate(KEYSPACE,
                        "text",
                        "CREATE AGGREGATE %s(text) " +
                        "SFUNC " + shortFunctionName(fState) + ' ' +
                        "STYPE text " +
                        "FINALFUNC system.varcharasblob " +
                        "INITCOND 'foobar'");
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
                             "SFUNC " + fState + " " +
                             "STYPE frozen<set<int>> " +
                             "FINALFUNC " + fFinal + " " +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "set<int>",
                                             "CREATE AGGREGATE %s(set<int>) " +
                                             "SFUNC " + fState + " " +
                                             "STYPE set<int> " +
                                             "FINALFUNC " + fFinal + " " +
                                             "INITCOND null");

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
                             "SFUNC " + fState + " " +
                             "STYPE frozen<list<int>> " +
                             "FINALFUNC " + fFinal + " " +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "list<int>",
                                             "CREATE AGGREGATE %s(list<int>) " +
                                             "SFUNC " + fState + " " +
                                             "STYPE list<int> " +
                                             "FINALFUNC " + fFinal + " " +
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
                             "SFUNC " + fState + " " +
                             "STYPE frozen<map<int, int>> " +
                             "FINALFUNC " + fFinal + " " +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "map<int, int>",
                                             "CREATE AGGREGATE %s(map<int, int>) " +
                                             "SFUNC " + fState + " " +
                                             "STYPE map<int, int> " +
                                             "FINALFUNC " + fFinal + " " +
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
                             "SFUNC " + fState + " " +
                             "STYPE frozen<tuple<int, int>> " +
                             "FINALFUNC " + fFinal + " " +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             "tuple<int, int>",
                                             "CREATE AGGREGATE %s(tuple<int, int>) " +
                                             "SFUNC " + fState + " " +
                                             "STYPE tuple<int, int> " +
                                             "FINALFUNC " + fFinal + " " +
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
                                       "tuple<int, int>",
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
                             "SFUNC " + fState + " " +
                             "STYPE frozen<" + myType + "> " +
                             "FINALFUNC " + fFinal + " " +
                             "INITCOND null");

        String aggregation = createAggregate(KEYSPACE,
                                             myType,
                                             "CREATE AGGREGATE %s(" + myType + ") " +
                                             "SFUNC " + fState + " " +
                                             "STYPE " + myType + " " +
                                             "FINALFUNC " + fFinal + " " +
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

        String fRNON = createFunction(KEYSPACE,
                                      "text",
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
                                      "int",
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
    public void testSystemKsFuncs() throws Throwable
    {

        String fAdder = createFunction(KEYSPACE,
                                      "int, int",
                                      "CREATE FUNCTION %s(a int, b int) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE java " +
                                      "AS 'return (a != null ? a : 0) + (b != null ? b : 0);'");

        String aAggr = createAggregate(KEYSPACE,
                                      "int",
                                      "CREATE AGGREGATE %s(int) " +
                                      "SFUNC " + shortFunctionName(fAdder) + ' ' +
                                      "STYPE int " +
                                      "FINALFUNC intasblob");

        createTable("CREATE TABLE %s (a int primary key, b int)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 2)");
        execute("INSERT INTO %s (a, b) VALUES (3, 3)");

        assertRows(execute("SELECT " + aAggr + "(b) FROM %s"), row(Int32Serializer.instance.serialize(6)));

    }
}
