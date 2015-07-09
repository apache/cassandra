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
package org.apache.cassandra.cql3.validation.entities;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class UFTest extends CQLTester
{
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

        assertLastSchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "double", "double");

        createFunctionOverload(f,
                               "double, double",
                               "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS int " +
                               "LANGUAGE javascript " +
                               "AS '\"string\";';");

        assertLastSchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "int", "int");

        schemaChange("CREATE OR REPLACE FUNCTION " + f + "(state int, val int) " +
                     "RETURNS NULL ON NULL INPUT " +
                     "RETURNS int " +
                     "LANGUAGE javascript " +
                     "AS '\"string\";';");

        assertLastSchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "int", "int");

        schemaChange("DROP FUNCTION " + f + "(double, double)");

        assertLastSchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "double", "double");
    }

    @Test
    public void testFunctionDropOnKeyspaceDrop() throws Throwable
    {
        String fSin = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE FUNCTION %s ( input double ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS double " +
                                     "LANGUAGE java " +
                                     "AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        FunctionName fSinName = parseFunctionName(fSin);

        Assert.assertEquals(1, Schema.instance.getFunctions(parseFunctionName(fSin)).size());

        assertRows(execute("SELECT function_name, language FROM system_schema.functions WHERE keyspace_name=?", KEYSPACE_PER_TEST),
                   row(fSinName.name, "java"));

        dropPerTestKeyspace();

        assertRows(execute("SELECT function_name, language FROM system_schema.functions WHERE keyspace_name=?", KEYSPACE_PER_TEST));

        Assert.assertEquals(0, Schema.instance.getFunctions(fSinName).size());
    }

    @Test
    public void testFunctionDropPreparedStatement() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, d double)");

        String fSin = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE FUNCTION %s ( input double ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS double " +
                                     "LANGUAGE java " +
                                     "AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        FunctionName fSinName = parseFunctionName(fSin);

        Assert.assertEquals(1, Schema.instance.getFunctions(parseFunctionName(fSin)).size());

        // create a pairs of Select and Inserts. One statement in each pair uses the function so when we
        // drop it those statements should be removed from the cache in QueryProcessor. The other statements
        // should be unaffected.

        ResultMessage.Prepared preparedSelect1 = QueryProcessor.prepare(
                                                                       String.format("SELECT key, %s(d) FROM %s.%s", fSin, KEYSPACE, currentTable()),
                                                                       ClientState.forInternalCalls(), false);
        ResultMessage.Prepared preparedSelect2 = QueryProcessor.prepare(
                                                    String.format("SELECT key FROM %s.%s", KEYSPACE, currentTable()),
                                                    ClientState.forInternalCalls(), false);
        ResultMessage.Prepared preparedInsert1 = QueryProcessor.prepare(
                                                      String.format("INSERT INTO %s.%s (key, d) VALUES (?, %s(?))", KEYSPACE, currentTable(), fSin),
                                                      ClientState.forInternalCalls(), false);
        ResultMessage.Prepared preparedInsert2 = QueryProcessor.prepare(
                                                      String.format("INSERT INTO %s.%s (key, d) VALUES (?, ?)", KEYSPACE, currentTable()),
                                                      ClientState.forInternalCalls(), false);

        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedSelect1.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedSelect2.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedInsert1.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedInsert2.statementId));

        execute("DROP FUNCTION " + fSin + "(double);");

        // the statements which use the dropped function should be removed from cache, with the others remaining
        Assert.assertNull(QueryProcessor.instance.getPrepared(preparedSelect1.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedSelect2.statementId));
        Assert.assertNull(QueryProcessor.instance.getPrepared(preparedInsert1.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedInsert2.statementId));

        execute("CREATE FUNCTION " + fSin + " ( input double ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS double " +
                "LANGUAGE java " +
                "AS 'return Double.valueOf(Math.sin(input));'");

        Assert.assertEquals(1, Schema.instance.getFunctions(fSinName).size());

        preparedSelect1= QueryProcessor.prepare(
                                         String.format("SELECT key, %s(d) FROM %s.%s", fSin, KEYSPACE, currentTable()),
                                         ClientState.forInternalCalls(), false);
        preparedInsert1 = QueryProcessor.prepare(
                                         String.format("INSERT INTO %s.%s (key, d) VALUES (?, %s(?))", KEYSPACE, currentTable(), fSin),
                                         ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedSelect1.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedInsert1.statementId));

        dropPerTestKeyspace();

        // again, only the 2 statements referencing the function should be removed from cache
        // this time because the statements select from tables in KEYSPACE, only the function
        // is scoped to KEYSPACE_PER_TEST
        Assert.assertNull(QueryProcessor.instance.getPrepared(preparedSelect1.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedSelect2.statementId));
        Assert.assertNull(QueryProcessor.instance.getPrepared(preparedInsert1.statementId));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(preparedInsert2.statementId));
    }

    @Test
    public void testDropFunctionDropsPreparedStatementsWithDelayedValues() throws Throwable
    {
        // test that dropping a function removes stmts which use
        // it to provide a DelayedValue collection from the
        // cache in QueryProcessor
        checkDelayedValuesCorrectlyIdentifyFunctionsInUse(false);
    }

    @Test
    public void testDropKeyspaceContainingFunctionDropsPreparedStatementsWithDelayedValues() throws Throwable
    {
        // test that dropping a function removes stmts which use
        // it to provide a DelayedValue collection from the
        // cache in QueryProcessor
        checkDelayedValuesCorrectlyIdentifyFunctionsInUse(true);
    }

    private ResultMessage.Prepared prepareStatementWithDelayedValue(CollectionType.Kind kind, String function)
    {
        String collectionType;
        String literalArgs;
        switch (kind)
        {
            case LIST:
                collectionType = "list<double>";
                literalArgs = String.format("[%s(0.0)]", function);
                break;
            case SET:
                collectionType = "set<double>";
                literalArgs = String.format("{%s(0.0)}", function);
                break;
            case MAP:
                collectionType = "map<double, double>";
                literalArgs = String.format("{%s(0.0):0.0}", function);
                break;
            default:
                Assert.fail("Unsupported collection type " + kind);
                collectionType = null;
                literalArgs = null;
        }

        createTable("CREATE TABLE %s (" +
                    " key int PRIMARY KEY," +
                    " val " + collectionType + ')');

        ResultMessage.Prepared prepared = QueryProcessor.prepare(
                                                                String.format("INSERT INTO %s.%s (key, val) VALUES (?, %s)",
                                                                             KEYSPACE,
                                                                             currentTable(),
                                                                             literalArgs),
                                                                ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));
        return prepared;
    }

    private ResultMessage.Prepared prepareStatementWithDelayedValueTuple(String function)
    {
        createTable("CREATE TABLE %s (" +
                    " key int PRIMARY KEY," +
                    " val tuple<double> )");

        ResultMessage.Prepared prepared = QueryProcessor.prepare(
                                                                String.format("INSERT INTO %s.%s (key, val) VALUES (?, (%s(0.0)))",
                                                                             KEYSPACE,
                                                                             currentTable(),
                                                                             function),
                                                                ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));
        return prepared;
    }

    public void checkDelayedValuesCorrectlyIdentifyFunctionsInUse(boolean dropKeyspace) throws Throwable
    {
        // prepare a statement which doesn't use any function for a control
        createTable("CREATE TABLE %s (" +
                    " key int PRIMARY KEY," +
                    " val double)");
        ResultMessage.Prepared control = QueryProcessor.prepare(
                                                               String.format("INSERT INTO %s.%s (key, val) VALUES (?, ?)",
                                                                            KEYSPACE,
                                                                            currentTable()),
                                                               ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(control.statementId));

        // a function that we'll drop and verify that statements which use it to
        // provide a DelayedValue are removed from the cache in QueryProcessor
        String function = createFunction(KEYSPACE_PER_TEST, "double",
                                        "CREATE FUNCTION %s ( input double ) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS double " +
                                        "LANGUAGE javascript " +
                                        "AS 'input'");
        Assert.assertEquals(1, Schema.instance.getFunctions(parseFunctionName(function)).size());

        List<ResultMessage.Prepared> prepared = new ArrayList<>();
        // prepare statements which use the function to provide a DelayedValue
        prepared.add(prepareStatementWithDelayedValue(CollectionType.Kind.LIST, function));
        prepared.add(prepareStatementWithDelayedValue(CollectionType.Kind.SET, function));
        prepared.add(prepareStatementWithDelayedValue(CollectionType.Kind.MAP, function));
        prepared.add(prepareStatementWithDelayedValueTuple(function));

        // what to drop - the function is scoped to the per-test keyspace, but the prepared statements
        // select from the per-fixture keyspace. So if we drop the per-test keyspace, the function
        // should be removed along with the statements that reference it. The control statement should
        // remain present in the cache. Likewise, if we actually drop the function itself the control
        // statement should not be removed, but the others should be
        if (dropKeyspace)
            dropPerTestKeyspace();
        else
            execute("DROP FUNCTION " + function);

        Assert.assertNotNull(QueryProcessor.instance.getPrepared(control.statementId));
        for (ResultMessage.Prepared removed : prepared)
            Assert.assertNull(QueryProcessor.instance.getPrepared(removed.statementId));
    }

    @Test
    public void testFunctionCreationAndDrop() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, d double)");

        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 3, 3d);

        // simple creation
        String fSin = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE FUNCTION %s ( input double ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS double " +
                                     "LANGUAGE java " +
                                     "AS 'return Math.sin(input);'");
        // check we can't recreate the same function
        assertInvalidMessage("already exists",
                             "CREATE FUNCTION " + fSin + " ( input double ) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        // but that it doesn't comply with "IF NOT EXISTS"
        execute("CREATE FUNCTION IF NOT EXISTS " + fSin + " ( input double ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                "LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        // Validate that it works as expected
        assertRows(execute("SELECT key, " + fSin + "(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Replace the method with incompatible return type
        assertInvalidMessage("the new return type text is not compatible with the return type double of existing function",
                             "CREATE OR REPLACE FUNCTION " + fSin + " ( input double ) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS text " +
                             "LANGUAGE java AS 'return Double.valueOf(42d);'");

        // proper replacement
        execute("CREATE OR REPLACE FUNCTION " + fSin + " ( input double ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                "LANGUAGE java AS 'return Double.valueOf(42d);'");

        // Validate the method as been replaced
        assertRows(execute("SELECT key, " + fSin + "(d) FROM %s"),
            row(1, 42.0),
            row(2, 42.0),
            row(3, 42.0)
        );

        // same function but other keyspace
        String fSin2 = createFunction(KEYSPACE, "double",
                                      "CREATE FUNCTION %s ( input double ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE java " +
                                      "AS 'return Math.sin(input);'");
        assertRows(execute("SELECT key, " + fSin2 + "(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Drop
        execute("DROP FUNCTION " + fSin);
        execute("DROP FUNCTION " + fSin2);

        // Drop unexisting function
        assertInvalidMessage("Cannot drop non existing function", "DROP FUNCTION " + fSin);
        // but don't complain with "IF EXISTS"
        execute("DROP FUNCTION IF EXISTS " + fSin);

        // can't drop native functions
        assertInvalidMessage("system keyspace is not user-modifiable", "DROP FUNCTION totimestamp");
        assertInvalidMessage("system keyspace is not user-modifiable", "DROP FUNCTION uuid");

        // sin() no longer exists
        assertInvalidMessage("Unknown function", "SELECT key, sin(d) FROM %s");
    }

    @Test
    public void testFunctionExecution() throws Throwable
    {
        createTable("CREATE TABLE %s (v text PRIMARY KEY)");

        execute("INSERT INTO %s(v) VALUES (?)", "aaa");

        String fRepeat = createFunction(KEYSPACE_PER_TEST, "text,int",
                                        "CREATE FUNCTION %s(v text, n int) " +
                                        "RETURNS NULL ON NULL INPUT " +
                                        "RETURNS text " +
                                        "LANGUAGE java " +
                                        "AS 'StringBuilder sb = new StringBuilder();\n" +
                                        "    for (int i = 0; i < n; i++)\n" +
                                        "        sb.append(v);\n" +
                                        "    return sb.toString();'");

        assertRows(execute("SELECT v FROM %s WHERE v=" + fRepeat + "(?, ?)", "a", 3), row("aaa"));
        assertEmpty(execute("SELECT v FROM %s WHERE v=" + fRepeat + "(?, ?)", "a", 2));
    }

    @Test
    public void testFunctionOverloading() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", "f2", 1);

        String fOverload = createFunction(KEYSPACE_PER_TEST, "varchar",
                                          "CREATE FUNCTION %s ( input varchar ) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS text " +
                                          "LANGUAGE java " +
                                          "AS 'return \"f1\";'");
        createFunctionOverload(fOverload,
                               "int",
                               "CREATE OR REPLACE FUNCTION %s(i int) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"f2\";'");
        createFunctionOverload(fOverload,
                               "text,text",
                               "CREATE OR REPLACE FUNCTION %s(v1 text, v2 text) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"f3\";'");
        createFunctionOverload(fOverload,
                               "ascii",
                               "CREATE OR REPLACE FUNCTION %s(v ascii) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"f1\";'");

        // text == varchar, so this should be considered as a duplicate
        assertInvalidMessage("already exists",
                             "CREATE FUNCTION " + fOverload + "(v varchar) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS text " +
                             "LANGUAGE java AS 'return \"f1\";'");

        assertRows(execute("SELECT " + fOverload + "(k), " + fOverload + "(v), " + fOverload + "(k, k) FROM %s"),
            row("f1", "f2", "f3")
        );

        forcePreparedValues();
        // This shouldn't work if we use preparation since there no way to know which overload to use
        assertInvalidMessage("Ambiguous call to function", "SELECT v FROM %s WHERE k = " + fOverload + "(?)", "foo");
        stopForcingPreparedValues();

        // but those should since we specifically cast
        assertEmpty(execute("SELECT v FROM %s WHERE k = " + fOverload + "((text)?)", "foo"));
        assertRows(execute("SELECT v FROM %s WHERE k = " + fOverload + "((int)?)", 3), row(1));
        assertEmpty(execute("SELECT v FROM %s WHERE k = " + fOverload + "((ascii)?)", "foo"));
        // And since varchar == text, this should work too
        assertEmpty(execute("SELECT v FROM %s WHERE k = " + fOverload + "((varchar)?)", "foo"));

        // no such functions exist...
        assertInvalidMessage("non existing function", "DROP FUNCTION " + fOverload + "(boolean)");
        assertInvalidMessage("non existing function", "DROP FUNCTION " + fOverload + "(bigint)");

        // 'overloaded' has multiple overloads - so it has to fail (CASSANDRA-7812)
        assertInvalidMessage("matches multiple function definitions", "DROP FUNCTION " + fOverload);
        execute("DROP FUNCTION " + fOverload + "(varchar)");
        assertInvalidMessage("none of its type signatures match", "SELECT v FROM %s WHERE k = " + fOverload + "((text)?)", "foo");
        execute("DROP FUNCTION " + fOverload + "(text, text)");
        assertInvalidMessage("none of its type signatures match", "SELECT v FROM %s WHERE k = " + fOverload + "((text)?,(text)?)", "foo", "bar");
        execute("DROP FUNCTION " + fOverload + "(ascii)");
        assertInvalidMessage("cannot be passed as argument 0 of function", "SELECT v FROM %s WHERE k = " + fOverload + "((ascii)?)", "foo");
        // single-int-overload must still work
        assertRows(execute("SELECT v FROM %s WHERE k = " + fOverload + "((int)?)", 3), row(1));
        // overloaded has just one overload now - so the following DROP FUNCTION is not ambigious (CASSANDRA-7812)
        execute("DROP FUNCTION " + fOverload);
    }

    @Test
    public void testCreateOrReplaceJavaFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                "CREATE FUNCTION %s( input double ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                "LANGUAGE java " +
                "AS '\n" +
                "  // parameter val is of type java.lang.Double\n" +
                "  /* return type is of type java.lang.Double */\n" +
                "  if (input == null) {\n" +
                "    return null;\n" +
                "  }\n" +
                "  return Math.sin( input );\n" +
                "';");

        // just check created function
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );

        execute("CREATE OR REPLACE FUNCTION " + fName + "( input double ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                "LANGUAGE java\n" +
                "AS '\n" +
                "  return input;\n" +
                "';");

        // check if replaced function returns correct result
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, 1d),
                   row(2, 2d, 2d),
                   row(3, 3d, 3d)
        );
    }

    @Test
    public void testJavaFunctionNoParameters() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = "\n  return 1L;\n";

        String fName = createFunction(KEYSPACE, "",
                                      "CREATE OR REPLACE FUNCTION %s() " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS bigint " +
                                      "LANGUAGE JAVA\n" +
                                      "AS '" +functionBody + "';");

        assertRows(execute("SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                           KEYSPACE, parseFunctionName(fName).name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "() FROM %s"),
                   row(1, 1d, 1L),
                   row(2, 2d, 1L),
                   row(3, 3d, 1L)
        );
    }

    @Test
    public void testJavaFunctionInvalidBodies() throws Throwable
    {
        try
        {
            execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".jfinv() " +
                    "RETURNS NULL ON NULL INPUT " +
                    "RETURNS bigint " +
                    "LANGUAGE JAVA\n" +
                    "AS '\n" +
                    "foobarbaz" +
                    "\n';");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Java source compilation failed"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("insert \";\" to complete BlockStatements"));
        }

        try
        {
            execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".jfinv() " +
                    "RETURNS NULL ON NULL INPUT " +
                    "RETURNS bigint " +
                    "LANGUAGE JAVA\n" +
                    "AS '\n" +
                    "foobarbaz;" +
                    "\n';");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Java source compilation failed"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("foobarbaz cannot be resolved to a type"));
        }
    }

    @Test
    public void testJavaFunctionInvalidReturn() throws Throwable
    {
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION jfir(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return 1L;';");
    }

    @Test
    public void testJavaFunctionArgumentTypeMismatch() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val bigint)");

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double)" +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA " +
                                      "AS 'return Double.valueOf(val);';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3L);
        assertInvalidMessage("val cannot be passed as argument 0 of function",
                             "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testJavaFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (val == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  return Math.sin(val);\n";

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA " +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testFunctionInTargetKeyspace() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("CREATE TABLE " + KEYSPACE_PER_TEST + ".second_tab (key int primary key, val double)");

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA " +
                                      "AS 'return Double.valueOf(val);';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertInvalidMessage("Unknown function",
                             "SELECT key, val, " + parseFunctionName(fName).name + "(val) FROM %s");

        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM " + KEYSPACE_PER_TEST + ".second_tab"),
                   row(1, 1d, 1d),
                   row(2, 2d, 2d),
                   row(3, 3d, 3d)
        );
    }

    @Test
    public void testFunctionWithReservedName() throws Throwable
    {
        execute("CREATE TABLE " + KEYSPACE_PER_TEST + ".second_tab (key int primary key, val double)");

        String fName = createFunction(KEYSPACE_PER_TEST, "",
                                      "CREATE OR REPLACE FUNCTION %s() " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS timestamp " +
                                      "LANGUAGE JAVA " +
                                      "AS 'return null;';");

        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 3, 3d);

        // ensure that system now() is executed
        UntypedResultSet rows = execute("SELECT key, val, now() FROM " + KEYSPACE_PER_TEST + ".second_tab");
        Assert.assertEquals(3, rows.size());
        UntypedResultSet.Row row = rows.iterator().next();
        Date ts = row.getTimestamp(row.getColumns().get(2).name.toString());
        Assert.assertNotNull(ts);

        // ensure that KEYSPACE_PER_TEST's now() is executed
        rows = execute("SELECT key, val, " + fName + "() FROM " + KEYSPACE_PER_TEST + ".second_tab");
        Assert.assertEquals(3, rows.size());
        row = rows.iterator().next();
        Assert.assertFalse(row.has(row.getColumns().get(2).name.toString()));
    }

    @Test
    public void testFunctionInSystemKS() throws Throwable
    {
        execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".totimestamp(val timeuuid) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS timestamp " +
                "LANGUAGE JAVA\n" +

                "AS 'return null;';");

        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION system.jnft(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION system.totimestamp(val timeuuid) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS timestamp " +
                             "LANGUAGE JAVA\n" +

                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "DROP FUNCTION system.now");

        // KS for executeInternal() is system
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION jnft(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION totimestamp(val timeuuid) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS timestamp " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "DROP FUNCTION now");
    }

    @Test
    public void testFunctionNonExistingKeyspace() throws Throwable
    {
        assertInvalidMessage("to non existing keyspace",
                             "CREATE OR REPLACE FUNCTION this_ks_does_not_exist.jnft(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
    }

    @Test
    public void testFunctionAfterOnDropKeyspace() throws Throwable
    {
        dropPerTestKeyspace();

        assertInvalidMessage("to non existing keyspace",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE_PER_TEST + ".jnft(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
    }

    @Test
    public void testJavaKeyspaceFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (val == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  return Math.sin( val );\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE OR REPLACE FUNCTION %s(val double) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS double " +
                                     "LANGUAGE JAVA " +
                                     "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testJavaRuntimeException() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  throw new RuntimeException(\"oh no!\");\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA\n" +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        // function throws a RuntimeException which is wrapped by FunctionExecutionException
        assertInvalidThrowMessage("java.lang.RuntimeException: oh no", FunctionExecutionException.class,
                                  "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testJavaDollarQuotedFunction() throws Throwable
    {
        String functionBody = '\n' +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (input == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  return \"'\"+Math.sin(input)+'\\\'';\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE FUNCTION %s( input double ) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java\n" +
                                      "AS $$" + functionBody + "$$;");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));
    }

    @Test
    public void testJavaSimpleCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>)");

        String fList = createFunction(KEYSPACE_PER_TEST, "list<double>",
                                     "CREATE FUNCTION %s( lst list<double> ) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS list<double> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return lst;$$;");
        String fSet = createFunction(KEYSPACE_PER_TEST, "set<text>",
                                     "CREATE FUNCTION %s( st set<text> ) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS set<text> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return st;$$;");
        String fMap = createFunction(KEYSPACE_PER_TEST, "map<int, boolean>",
                                     "CREATE FUNCTION %s( mp map<int, boolean> ) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS map<int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return mp;$$;");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        execute("INSERT INTO %s (key, lst, st, mp) VALUES (1, ?, ?, ?)", list, set, map);

        assertRows(execute("SELECT " + fList + "(lst), " + fSet + "(st), " + fMap + "(mp) FROM %s WHERE key = 1"),
                   row(list, set, map));

        // same test - but via native protocol
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fList + "(lst), " + fSet + "(st), " + fMap + "(mp) FROM %s WHERE key = 1"),
                          row(list, set, map));
    }

    @Test
    public void testWrongKeyspace() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (txt text, i int)");
        String type = KEYSPACE + '.' + typeName;

        assertInvalidMessage(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; user types can only be used in the keyspace they are defined in",
                                           KEYSPACE_PER_TEST, KEYSPACE),
                             "CREATE FUNCTION " + KEYSPACE_PER_TEST + ".test_wrong_ks( val int ) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS " + type + " " +
                             "LANGUAGE java\n" +
                             "AS $$return val;$$;");

        assertInvalidMessage(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; user types can only be used in the keyspace they are defined in",
                                           KEYSPACE_PER_TEST, KEYSPACE),
                             "CREATE FUNCTION " + KEYSPACE_PER_TEST + ".test_wrong_ks( val " + type + " ) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS int " +
                             "LANGUAGE java\n" +
                             "AS $$return val;$$;");
    }

    @Test
    public void testComplexNullValues() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>," +
                    "tup frozen<tuple<double, text, int, boolean>>, udt frozen<" + type + ">)");

        String fList = createFunction(KEYSPACE, "list<double>",
                                      "CREATE FUNCTION %s( coll list<double> ) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS list<double> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return coll;$$;");
        String fSet = createFunction(KEYSPACE, "set<text>",
                                     "CREATE FUNCTION %s( coll set<text> ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS set<text> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return coll;$$;");
        String fMap = createFunction(KEYSPACE, "map<int, boolean>",
                                     "CREATE FUNCTION %s( coll map<int, boolean> ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS map<int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return coll;$$;");
        String fTup = createFunction(KEYSPACE, "tuple<double, text, int, boolean>",
                                     "CREATE FUNCTION %s( val tuple<double, text, int, boolean> ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS tuple<double, text, int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return val;$$;");
        String fUdt = createFunction(KEYSPACE, type,
                                     "CREATE FUNCTION %s( val " + type + " ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS " + type + " " +
                                     "LANGUAGE java\n" +
                                     "AS $$return val;$$;");
        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);
        Object t = tuple(1d, "one", 42, false);

        execute("INSERT INTO %s (key, lst, st, mp, tup, udt) VALUES (1, ?, ?, ?, ?, {txt: 'one', i:1})", list, set, map, t);
        execute("INSERT INTO %s (key, lst, st, mp, tup, udt) VALUES (2, ?, ?, ?, ?, null)", null, null, null, null);

        execute("SELECT " +
                fList + "(lst), " +
                fSet + "(st), " +
                fMap + "(mp), " +
                fTup + "(tup), " +
                fUdt + "(udt) FROM %s WHERE key = 1");
        UntypedResultSet.Row row = execute("SELECT " +
                                           fList + "(lst) as l, " +
                                           fSet + "(st) as s, " +
                                           fMap + "(mp) as m, " +
                                           fTup + "(tup) as t, " +
                                           fUdt + "(udt) as u " +
                                           "FROM %s WHERE key = 1").one();
        Assert.assertNotNull(row.getBytes("l"));
        Assert.assertNotNull(row.getBytes("s"));
        Assert.assertNotNull(row.getBytes("m"));
        Assert.assertNotNull(row.getBytes("t"));
        Assert.assertNotNull(row.getBytes("u"));
        row = execute("SELECT " +
                      fList + "(lst) as l, " +
                      fSet + "(st) as s, " +
                      fMap + "(mp) as m, " +
                      fTup + "(tup) as t, " +
                      fUdt + "(udt) as u " +
                      "FROM %s WHERE key = 2").one();
        Assert.assertNull(row.getBytes("l"));
        Assert.assertNull(row.getBytes("s"));
        Assert.assertNull(row.getBytes("m"));
        Assert.assertNull(row.getBytes("t"));
        Assert.assertNull(row.getBytes("u"));

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            Row r = executeNet(version, "SELECT " +
                                        fList + "(lst) as l, " +
                                        fSet + "(st) as s, " +
                                        fMap + "(mp) as m, " +
                                        fTup + "(tup) as t, " +
                                        fUdt + "(udt) as u " +
                                        "FROM %s WHERE key = 1").one();
            Assert.assertNotNull(r.getBytesUnsafe("l"));
            Assert.assertNotNull(r.getBytesUnsafe("s"));
            Assert.assertNotNull(r.getBytesUnsafe("m"));
            Assert.assertNotNull(r.getBytesUnsafe("t"));
            Assert.assertNotNull(r.getBytesUnsafe("u"));
            r = executeNet(version, "SELECT " +
                                    fList + "(lst) as l, " +
                                    fSet + "(st) as s, " +
                                    fMap + "(mp) as m, " +
                                    fTup + "(tup) as t, " +
                                    fUdt + "(udt) as u " +
                                    "FROM %s WHERE key = 2").one();
            Assert.assertNull(r.getBytesUnsafe("l"));
            Assert.assertNull(r.getBytesUnsafe("s"));
            Assert.assertNull(r.getBytesUnsafe("m"));
            Assert.assertNull(r.getBytesUnsafe("t"));
            Assert.assertNull(r.getBytesUnsafe("u"));
        }
    }

    @Test
    public void testJavaTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, tup frozen<tuple<double, text, int, boolean>>)");

        String fName = createFunction(KEYSPACE, "tuple<double, text, int, boolean>",
                                     "CREATE FUNCTION %s( tup tuple<double, text, int, boolean> ) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS tuple<double, text, int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return tup;$$;");

        Object t = tuple(1d, "foo", 2, true);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT tup FROM %s WHERE key = 1"),
                   row(t));

        assertRows(execute("SELECT " + fName + "(tup) FROM %s WHERE key = 1"),
                   row(t));
    }

    @Test
    public void testJavaTupleTypeCollection() throws Throwable
    {
        String tupleTypeDef = "tuple<double, list<double>, set<text>, map<int, boolean>>";

        createTable("CREATE TABLE %s (key int primary key, tup frozen<" + tupleTypeDef + ">)");

        String fTup0 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS " + tupleTypeDef + ' ' +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       tup;$$;");
        String fTup1 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       Double.valueOf(tup.getDouble(0));$$;");
        String fTup2 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                                      "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS list<double> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return " +
                                      "       tup.getList(1, Double.class);$$;");
        String fTup3 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS set<text> " +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       tup.getSet(2, String.class);$$;");
        String fTup4 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS map<int, boolean> " +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       tup.getMap(3, Integer.class, Boolean.class);$$;");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        Object t = tuple(1d, list, set, map);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT " + fTup0 + "(tup) FROM %s WHERE key = 1"),
                   row(t));
        assertRows(execute("SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                   row(1d));
        assertRows(execute("SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                   row(list));
        assertRows(execute("SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                   row(set));
        assertRows(execute("SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                   row(map));

        TupleType tType = TupleType.of(DataType.cdouble(),
                                       DataType.list(DataType.cdouble()),
                                       DataType.set(DataType.text()),
                                       DataType.map(DataType.cint(), DataType.cboolean()));
        TupleValue tup = tType.newValue(1d, list, set, map);
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup0 + "(tup) FROM %s WHERE key = 1"),
                          row(tup));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                          row(1d));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                          row(list));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                          row(set));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                          row(map));
        }
    }

    @Test
    public void testJavaUserTypeWithUse() throws Throwable
    {
        String type = createType("CREATE TYPE %s (txt text, i int)");
        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + KEYSPACE + '.' + type + ">)");
        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            executeNet(version, "USE " + KEYSPACE);

            executeNet(version,
                       "CREATE FUNCTION f_use1( udt " + type + " ) " +
                       "RETURNS NULL ON NULL INPUT " +
                       "RETURNS " + type + " " +
                       "LANGUAGE java " +
                       "AS $$return " +
                       "     udt;$$;");
            try
            {
                List<Row> rowsNet = executeNet(version, "SELECT f_use1(udt) FROM %s WHERE key = 1").all();
                Assert.assertEquals(1, rowsNet.size());
                UDTValue udtVal = rowsNet.get(0).getUDTValue(0);
                Assert.assertEquals("one", udtVal.getString("txt"));
                Assert.assertEquals(1, udtVal.getInt("i"));
            }
            finally
            {
                executeNet(version, "DROP FUNCTION f_use1");
            }
        }
    }

    @Test
    public void testJavaUserType() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fUdt0 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS " + type + " " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     udt;$$;");
        String fUdt1 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + ") " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     udt.getString(\"txt\");$$;");
        String fUdt2 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + ") " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     Integer.valueOf(udt.getInt(\"i\"));$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        UntypedResultSet rows = execute("SELECT " + fUdt0 + "(udt) FROM %s WHERE key = 1");
        Assert.assertEquals(1, rows.size());
        assertRows(execute("SELECT " + fUdt1 + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fUdt2 + "(udt) FROM %s WHERE key = 1"),
                   row(1));

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            List<Row> rowsNet = executeNet(version, "SELECT " + fUdt0 + "(udt) FROM %s WHERE key = 1").all();
            Assert.assertEquals(1, rowsNet.size());
            UDTValue udtVal = rowsNet.get(0).getUDTValue(0);
            Assert.assertEquals("one", udtVal.getString("txt"));
            Assert.assertEquals(1, udtVal.getInt("i"));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fUdt1 + "(udt) FROM %s WHERE key = 1"),
                          row("one"));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fUdt2 + "(udt) FROM %s WHERE key = 1"),
                          row(1));
        }
    }

    @Test
    public void testUserTypeDrop() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     Integer.valueOf(udt.getInt(\"i\"));$$;");

        FunctionName fNameName = parseFunctionName(fName);

        Assert.assertEquals(1, Schema.instance.getFunctions(fNameName).size());

        ResultMessage.Prepared prepared = QueryProcessor.prepare(String.format("SELECT key, %s(udt) FROM %s.%s", fName, KEYSPACE, currentTable()),
                                                                 ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        // UT still referenced by table
        assertInvalidMessage("Cannot drop user type", "DROP TYPE " + type);

        execute("DROP TABLE %s");

        // UT still referenced by UDF
        assertInvalidMessage("as it is still used by function", "DROP TYPE " + type);

        Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        // function stays
        Assert.assertEquals(1, Schema.instance.getFunctions(fNameName).size());
    }

    @Test
    public void testJavaUserTypeRenameField() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java\n" +
                                      "AS $$return udt.getString(\"txt\");$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        assertRows(execute("SELECT " + fName + "(udt) FROM %s WHERE key = 1"),
                   row("one"));

        execute("ALTER TYPE " + type + " RENAME txt TO str");

        assertInvalidMessage("txt is not a field defined in this UDT",
                             "SELECT " + fName + "(udt) FROM %s WHERE key = 1");

        execute("ALTER TYPE " + type + " RENAME str TO txt");

        assertRows(execute("SELECT " + fName + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
    }

    @Test
    public void testJavaUserTypeAddFieldWithReplace() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName1replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + ") " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS text " +
                                              "LANGUAGE java\n" +
                                              "AS $$return udt.getString(\"txt\");$$;");
        String fName2replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "CALLED ON NULL INPUT " +
                                              "RETURNS int " +
                                              "LANGUAGE java\n" +
                                              "AS $$return Integer.valueOf(udt.getInt(\"i\"));$$;");
        String fName3replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "CALLED ON NULL INPUT " +
                                              "RETURNS double " +
                                              "LANGUAGE java\n" +
                                              "AS $$return Double.valueOf(udt.getDouble(\"added\"));$$;");
        String fName4replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS " + type + " " +
                                              "LANGUAGE java\n" +
                                              "AS $$return udt;$$;");

        String fName1noReplace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS text " +
                                              "LANGUAGE java\n" +
                                              "AS $$return udt.getString(\"txt\");$$;");
        String fName2noReplace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "CALLED ON NULL INPUT " +
                                              "RETURNS int " +
                                              "LANGUAGE java\n" +
                                              "AS $$return Integer.valueOf(udt.getInt(\"i\"));$$;");
        String fName3noReplace = createFunction(KEYSPACE, type,
                                                "CREATE FUNCTION %s( udt " + type + " ) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS double " +
                                                "LANGUAGE java\n" +
                                                "AS $$return Double.valueOf(udt.getDouble(\"added\"));$$;");
        String fName4noReplace = createFunction(KEYSPACE, type,
                                                "CREATE FUNCTION %s( udt " + type + " ) " +
                                                "RETURNS NULL ON NULL INPUT " +
                                                "RETURNS " + type + " " +
                                                "LANGUAGE java\n" +
                                                "AS $$return udt;$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        assertRows(execute("SELECT " + fName1replace + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fName2replace + "(udt) FROM %s WHERE key = 1"),
                   row(1));

        // add field

        execute("ALTER TYPE " + type + " ADD added double");

        execute("INSERT INTO %s (key, udt) VALUES (2, {txt: 'two', i:2, added: 2})");

        // note: type references of functions remain at the state _before_ the type mutation
        // means we need to recreate the functions

        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS text " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     udt.getString(\"txt\");$$;",
                              fName1replace, type));
        Assert.assertEquals(1, Schema.instance.getFunctions(parseFunctionName(fName1replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS int " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     Integer.valueOf(udt.getInt(\"i\"));$$;",
                              fName2replace, type));
        Assert.assertEquals(1, Schema.instance.getFunctions(parseFunctionName(fName2replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS double " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     Double.valueOf(udt.getDouble(\"added\"));$$;",
                              fName3replace, type));
        Assert.assertEquals(1, Schema.instance.getFunctions(parseFunctionName(fName3replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS %s " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     udt;$$;",
                              fName4replace, type, type));
        Assert.assertEquals(1, Schema.instance.getFunctions(parseFunctionName(fName4replace)).size());

        assertRows(execute("SELECT " + fName1replace + "(udt) FROM %s WHERE key = 2"),
                   row("two"));
        assertRows(execute("SELECT " + fName2replace + "(udt) FROM %s WHERE key = 2"),
                   row(2));
        assertRows(execute("SELECT " + fName3replace + "(udt) FROM %s WHERE key = 2"),
                   row(2d));
        assertRows(execute("SELECT " + fName3replace + "(udt) FROM %s WHERE key = 1"),
                   row(0d));

        // un-replaced functions will work since the user type has changed
        // and the UDF has exchanged the user type reference

        assertRows(execute("SELECT " + fName1noReplace + "(udt) FROM %s WHERE key = 2"),
                   row("two"));
        assertRows(execute("SELECT " + fName2noReplace + "(udt) FROM %s WHERE key = 2"),
                   row(2));
        assertRows(execute("SELECT " + fName3noReplace + "(udt) FROM %s WHERE key = 2"),
                   row(2d));
        assertRows(execute("SELECT " + fName3noReplace + "(udt) FROM %s WHERE key = 1"),
                   row(0d));

        execute("DROP FUNCTION " + fName1replace);
        execute("DROP FUNCTION " + fName2replace);
        execute("DROP FUNCTION " + fName3replace);
        execute("DROP FUNCTION " + fName4replace);
        execute("DROP FUNCTION " + fName1noReplace);
        execute("DROP FUNCTION " + fName2noReplace);
        execute("DROP FUNCTION " + fName3noReplace);
        execute("DROP FUNCTION " + fName4noReplace);
    }

    @Test
    public void testJavaUTCollections() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable(String.format("CREATE TABLE %%s " +
                                  "(key int primary key, lst list<frozen<%s>>, st set<frozen<%s>>, mp map<int, frozen<%s>>)",
                                  type, type, type));

        String fName1 = createFunction(KEYSPACE, "list<frozen<" + type + ">>",
                              "CREATE FUNCTION %s( lst list<frozen<" + type + ">> ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS text " +
                              "LANGUAGE java\n" +
                              "AS $$" +
                              "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)lst.get(1);" +
                              "     return udtVal.getString(\"txt\");$$;");
        String fName2 = createFunction(KEYSPACE, "set<frozen<" + type + ">>",
                                       "CREATE FUNCTION %s( st set<frozen<" + type + ">> ) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java\n" +
                                       "AS $$" +
                                       "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)st.iterator().next();" +
                                       "     return udtVal.getString(\"txt\");$$;");
        String fName3 = createFunction(KEYSPACE, "map<int, frozen<" + type + ">>",
                              "CREATE FUNCTION %s( mp map<int, frozen<" + type + ">> ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS text " +
                              "LANGUAGE java\n" +
                              "AS $$" +
                              "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)mp.get(Integer.valueOf(3));" +
                              "     return udtVal.getString(\"txt\");$$;");

        execute("INSERT INTO %s (key, lst, st, mp) values (1, " +
                "[ {txt: 'one', i:1}, {txt: 'three', i:1}, {txt: 'one', i:1} ] , " +
                "{ {txt: 'one', i:1}, {txt: 'three', i:3}, {txt: 'two', i:2} }, " +
                "{ 1: {txt: 'one', i:1}, 2: {txt: 'one', i:3}, 3: {txt: 'two', i:2} })");

        assertRows(execute("SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                   row("three", "one", "two"));

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                          row("three", "one", "two"));
    }

    @Test
    public void testJavascriptSimpleCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>)");

        String fName1 = createFunction(KEYSPACE_PER_TEST, "list<double>",
                "CREATE FUNCTION %s( lst list<double> ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS list<double> " +
                "LANGUAGE javascript\n" +
                "AS 'lst;';");
        String fName2 = createFunction(KEYSPACE_PER_TEST, "set<text>",
                "CREATE FUNCTION %s( st set<text> ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS set<text> " +
                "LANGUAGE javascript\n" +
                "AS 'st;';");
        String fName3 = createFunction(KEYSPACE_PER_TEST, "map<int, boolean>",
                "CREATE FUNCTION %s( mp map<int, boolean> ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS map<int, boolean> " +
                "LANGUAGE javascript\n" +
                "AS 'mp;';");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        execute("INSERT INTO %s (key, lst, st, mp) VALUES (1, ?, ?, ?)", list, set, map);

        assertRows(execute("SELECT lst, st, mp FROM %s WHERE key = 1"),
                   row(list, set, map));

        assertRows(execute("SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                   row(list, set, map));

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                          row(list, set, map));
    }

    @Test
    public void testJavascriptTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, tup frozen<tuple<double, text, int, boolean>>)");

        String fName = createFunction(KEYSPACE_PER_TEST, "tuple<double, text, int, boolean>",
                "CREATE FUNCTION %s( tup tuple<double, text, int, boolean> ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS tuple<double, text, int, boolean> " +
                "LANGUAGE javascript\n" +
                "AS $$tup;$$;");

        Object t = tuple(1d, "foo", 2, true);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT tup FROM %s WHERE key = 1"),
                   row(t));

        assertRows(execute("SELECT " + fName + "(tup) FROM %s WHERE key = 1"),
                   row(t));
    }

    @Test
    public void testJavascriptTupleTypeCollection() throws Throwable
    {
        String tupleTypeDef = "tuple<double, list<double>, set<text>, map<int, boolean>>";
        createTable("CREATE TABLE %s (key int primary key, tup frozen<" + tupleTypeDef + ">)");

        String fTup1 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS tuple<double, list<double>, set<text>, map<int, boolean>> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup;$$;");
        String fTup2 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS double " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup.getDouble(0);$$;");
        String fTup3 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS list<double> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup.getList(1, java.lang.Class.forName(\"java.lang.Double\"));$$;");
        String fTup4 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS set<text> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup.getSet(2, java.lang.Class.forName(\"java.lang.String\"));$$;");
        String fTup5 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS map<int, boolean> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup.getMap(3, java.lang.Class.forName(\"java.lang.Integer\"), java.lang.Class.forName(\"java.lang.Boolean\"));$$;");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        Object t = tuple(1d, list, set, map);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                   row(t));
        assertRows(execute("SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                   row(1d));
        assertRows(execute("SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                   row(list));
        assertRows(execute("SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                   row(set));
        assertRows(execute("SELECT " + fTup5 + "(tup) FROM %s WHERE key = 1"),
                   row(map));

        // same test - but via native protocol
        TupleType tType = TupleType.of(DataType.cdouble(),
                                       DataType.list(DataType.cdouble()),
                                       DataType.set(DataType.text()),
                                       DataType.map(DataType.cint(), DataType.cboolean()));
        TupleValue tup = tType.newValue(1d, list, set, map);
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                          row(tup));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                          row(1d));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                          row(list));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                          row(set));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup5 + "(tup) FROM %s WHERE key = 1"),
                          row(map));
        }
    }

    @Test
    public void testJavascriptUserType() throws Throwable
    {
        String type = createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fUdt1 = createFunction(KEYSPACE, type,
                              "CREATE FUNCTION %s( udt " + type + " ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS " + type + " " +
                              "LANGUAGE javascript\n" +
                              "AS $$" +
                              "     udt;$$;");
        String fUdt2 = createFunction(KEYSPACE, type,
                              "CREATE FUNCTION %s( udt " + type + " ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS text " +
                              "LANGUAGE javascript\n" +
                              "AS $$" +
                              "     udt.getString(\"txt\");$$;");
        String fUdt3 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE javascript\n" +
                                      "AS $$" +
                                      "     udt.getInt(\"i\");$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        UntypedResultSet rows = execute("SELECT " + fUdt1 + "(udt) FROM %s WHERE key = 1");
        Assert.assertEquals(1, rows.size());
        assertRows(execute("SELECT " + fUdt2 + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fUdt3 + "(udt) FROM %s WHERE key = 1"),
                   row(1));
    }

    @Test
    public void testJavascriptUTCollections() throws Throwable
    {
        String type = createType("CREATE TYPE %s (txt text, i int)");

        createTable(String.format("CREATE TABLE %%s " +
                                  "(key int primary key, lst list<frozen<%s>>, st set<frozen<%s>>, mp map<int, frozen<%s>>)",
                                  type, type, type));

        String fName = createFunction(KEYSPACE, "list<frozen<" + type + ">>",
                                      "CREATE FUNCTION %s( lst list<frozen<" + type + ">> ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE javascript\n" +
                                      "AS $$" +
                                      "        lst.get(1).getString(\"txt\");$$;");
        createFunctionOverload(fName, "set<frozen<" + type + ">>",
                               "CREATE FUNCTION %s( st set<frozen<" + type + ">> ) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE javascript\n" +
                               "AS $$" +
                               "        st.iterator().next().getString(\"txt\");$$;");
        createFunctionOverload(fName, "map<int, frozen<" + type + ">>",
                               "CREATE FUNCTION %s( mp map<int, frozen<" + type + ">> ) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE javascript\n" +
                               "AS $$" +
                               "        mp.get(java.lang.Integer.valueOf(3)).getString(\"txt\");$$;");

        execute("INSERT INTO %s (key, lst, st, mp) values (1, " +
                // list<frozen<UDT>>
                "[ {txt: 'one', i:1}, {txt: 'three', i:1}, {txt: 'one', i:1} ] , " +
                // set<frozen<UDT>>
                "{ {txt: 'one', i:1}, {txt: 'three', i:3}, {txt: 'two', i:2} }, " +
                // map<int, frozen<UDT>>
                "{ 1: {txt: 'one', i:1}, 2: {txt: 'one', i:3}, 3: {txt: 'two', i:2} })");

        assertRows(execute("SELECT " + fName + "(lst) FROM %s WHERE key = 1"),
                   row("three"));
        assertRows(execute("SELECT " + fName + "(st) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fName + "(mp) FROM %s WHERE key = 1"),
                   row("two"));

        String cqlSelect = "SELECT " + fName + "(lst), " + fName + "(st), " + fName + "(mp) FROM %s WHERE key = 1";
        assertRows(execute(cqlSelect),
                   row("three", "one", "two"));

        // same test - but via native protocol
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, cqlSelect),
                          row("three", "one", "two"));
    }

    @Test
    public void testJavascriptFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  Math.sin(val);\n";

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE javascript\n" +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("javascript", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testJavascriptBadReturnType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE javascript\n" +
                                      "AS '\"string\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ClassCastException
        assertInvalidMessage("Invalid value for CQL type double", "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testJavascriptThrow() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String fName = createFunction(KEYSPACE, "double",
                       "CREATE OR REPLACE FUNCTION %s(val double) " +
                       "RETURNS NULL ON NULL INPUT " +
                       "RETURNS double " +
                       "LANGUAGE javascript\n" +
                       "AS 'throw \"fool\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ScriptException
        assertInvalidThrowMessage("fool", FunctionExecutionException.class,
                                  "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testDuplicateArgNames() throws Throwable
    {
        assertInvalidMessage("duplicate argument names for given function",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double, val text) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS text " +
                             "LANGUAGE javascript\n" +
                             "AS '\"foo bar\";';");
    }

    @Test
    public void testJavascriptCompileFailure() throws Throwable
    {
        assertInvalidMessage("Failed to compile function 'cql_test_keyspace.scrinv'",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE javascript\n" +
                             "AS 'foo bar';");
    }

    @Test
    public void testScriptInvalidLanguage() throws Throwable
    {
        assertInvalidMessage("Invalid language 'artificial_intelligence' for function 'cql_test_keyspace.scrinv'",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE artificial_intelligence\n" +
                             "AS 'question for 42?';");
    }

    @Test
    public void testScriptReturnTypeCasting() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);

        Object[][] variations = {
                                new Object[]    {   "true",     "boolean",  true    },
                                new Object[]    {   "false",    "boolean",  false   },
                                new Object[]    {   "100",      "tinyint",  (byte)100 },
                                new Object[]    {   "100.",     "tinyint",  (byte)100 },
                                new Object[]    {   "100",      "smallint", (short)100 },
                                new Object[]    {   "100.",     "smallint", (short)100 },
                                new Object[]    {   "100",      "int",      100     },
                                new Object[]    {   "100.",     "int",      100     },
                                new Object[]    {   "100",      "double",   100d    },
                                new Object[]    {   "100.",     "double",   100d    },
                                new Object[]    {   "100",      "bigint",   100L    },
                                new Object[]    {   "100.",     "bigint",   100L    },
                                new Object[]    {   "100",      "varint",   BigInteger.valueOf(100L)    },
                                new Object[]    {   "100.",     "varint",   BigInteger.valueOf(100L)    },
                                new Object[]    {   "parseInt(\"100\");", "decimal",  BigDecimal.valueOf(100d)    },
                                new Object[]    {   "100.",     "decimal",  BigDecimal.valueOf(100d)    },
                                };

        for (Object[] variation : variations)
        {
            Object functionBody = variation[0];
            Object returnType = variation[1];
            Object expectedResult = variation[2];

            String fName = createFunction(KEYSPACE, "double",
                                          "CREATE OR REPLACE FUNCTION %s(val double) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS " +returnType + ' ' +
                                          "LANGUAGE javascript " +
                                          "AS '" + functionBody + ";';");
            assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                       row(1, 1d, expectedResult));
        }
    }

    @Test
    public void testScriptParamReturnTypes() throws Throwable
    {
        UUID ruuid = UUID.randomUUID();
        UUID tuuid = UUIDGen.getTimeUUID();

        createTable("CREATE TABLE %s (key int primary key, " +
                    "tival tinyint, sival smallint, ival int, lval bigint, fval float, dval double, vval varint, ddval decimal, " +
                    "timval time, dtval date, tsval timestamp, uval uuid, tuval timeuuid)");
        execute("INSERT INTO %s (key, tival, sival, ival, lval, fval, dval, vval, ddval, timval, dtval, tsval, uval, tuval) VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 1,
                (byte)1, (short)1, 1, 1L, 1f, 1d, BigInteger.valueOf(1L), BigDecimal.valueOf(1d), 1L, Integer.MAX_VALUE, new Date(1), ruuid, tuuid);

        Object[][] variations = {
                                new Object[] {  "tinyint",  "tival",    (byte)1,                (byte)2  },
                                new Object[] {  "smallint", "sival",    (short)1,               (short)2  },
                                new Object[] {  "int",      "ival",     1,                      2  },
                                new Object[] {  "bigint",   "lval",     1L,                     2L  },
                                new Object[] {  "float",    "fval",     1f,                     2f  },
                                new Object[] {  "double",   "dval",     1d,                     2d  },
                                new Object[] {  "varint",   "vval",     BigInteger.valueOf(1L), BigInteger.valueOf(2L)  },
                                new Object[] {  "decimal",  "ddval",    BigDecimal.valueOf(1d), BigDecimal.valueOf(2d)  },
                                new Object[] {  "time",     "timval",   1L,                     2L  },
                                };

        for (Object[] variation : variations)
        {
            Object type = variation[0];
            Object col = variation[1];
            Object expected1 = variation[2];
            Object expected2 = variation[3];
            String fName = createFunction(KEYSPACE, type.toString(),
                           "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS " + type + ' ' +
                           "LANGUAGE javascript " +
                           "AS 'val+1;';");
            assertRows(execute("SELECT key, " + col + ", " + fName + '(' + col + ") FROM %s"),
                       row(1, expected1, expected2));
        }

        variations = new Object[][] {
                     new Object[] {  "timestamp","tsval",    new Date(1),            new Date(1)  },
                     new Object[] {  "uuid",     "uval",     ruuid,                  ruuid  },
                     new Object[] {  "timeuuid", "tuval",    tuuid,                  tuuid  },
                     new Object[] {  "date",     "dtval",    Integer.MAX_VALUE,      Integer.MAX_VALUE },
        };

        for (Object[] variation : variations)
        {
            Object type = variation[0];
            Object col = variation[1];
            Object expected1 = variation[2];
            Object expected2 = variation[3];
            String fName = createFunction(KEYSPACE, type.toString(),
                                          "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS " + type + ' ' +
                                          "LANGUAGE javascript " +
                                          "AS 'val;';");
            assertRows(execute("SELECT key, " + col + ", " + fName + '(' + col + ") FROM %s"),
                       row(1, expected1, expected2));
        }
    }

    static class TypesTestDef
    {
        final String udfType;
        final String tableType;
        final String columnName;
        final Object referenceValue;

        String fCheckArgAndReturn;

        String fCalledOnNull;
        String fReturnsNullOnNull;

        TypesTestDef(String udfType, String tableType, String columnName, Object referenceValue)
        {
            this.udfType = udfType;
            this.tableType = tableType;
            this.columnName = columnName;
            this.referenceValue = referenceValue;
        }
    }

    @Test
    public void testTypesWithAndWithoutNulls() throws Throwable
    {
        // test various combinations of types against UDFs with CALLED ON NULL or RETURNS NULL ON NULL

        String type = createType("CREATE TYPE %s (txt text, i int)");

        TypesTestDef[] typeDefs =
        {
        //                udf type,            table type,                 column, reference value
        new TypesTestDef("timestamp", "timestamp", "ts", new Date()),
        new TypesTestDef("date", "date", "dt", 12345),
        new TypesTestDef("time", "time", "tim", 12345L),
        new TypesTestDef("uuid", "uuid", "uu", UUID.randomUUID()),
        new TypesTestDef("timeuuid", "timeuuid", "tu", UUIDGen.getTimeUUID()),
        new TypesTestDef("tinyint", "tinyint", "ti", (byte) 42),
        new TypesTestDef("smallint", "smallint", "si", (short) 43),
        new TypesTestDef("int", "int", "i", 44),
        new TypesTestDef("bigint", "bigint", "b", 45L),
        new TypesTestDef("float", "float", "f", 46f),
        new TypesTestDef("double", "double", "d", 47d),
        new TypesTestDef("boolean", "boolean", "x", true),
        new TypesTestDef("ascii", "ascii", "a", "tqbfjutld"),
        new TypesTestDef("text", "text", "t", "k\u00f6lsche jung"),
        //new TypesTestDef(type,                 "frozen<" + type + '>',     "u",    null),
        new TypesTestDef("tuple<int, text>", "frozen<tuple<int, text>>", "tup", tuple(1, "foo"))
        };

        String createTableDDL = "CREATE TABLE %s (key int PRIMARY KEY";
        String insertDML = "INSERT INTO %s (key";
        List<Object> values = new ArrayList<>();
        for (TypesTestDef typeDef : typeDefs)
        {
            createTableDDL += ", " + typeDef.columnName + ' ' + typeDef.tableType;
            insertDML += ", " + typeDef.columnName;
            String typeName = typeDef.udfType;
            typeDef.fCheckArgAndReturn = createFunction(KEYSPACE,
                                                        typeName,
                                                        "CREATE OR REPLACE FUNCTION %s(val " + typeName + ") " +
                                                        "CALLED ON NULL INPUT " +
                                                        "RETURNS " + typeName + ' ' +
                                                        "LANGUAGE java\n" +
                                                        "AS 'return val;';");
            typeDef.fCalledOnNull = createFunction(KEYSPACE,
                                                   typeName,
                                                   "CREATE OR REPLACE FUNCTION %s(val " + typeName + ") " +
                                                   "CALLED ON NULL INPUT " +
                                                   "RETURNS text " +
                                                   "LANGUAGE java\n" +
                                                   "AS 'return \"called\";';");
            typeDef.fReturnsNullOnNull = createFunction(KEYSPACE,
                                                        typeName,
                                                        "CREATE OR REPLACE FUNCTION %s(val " + typeName + ") " +
                                                        "RETURNS NULL ON NULL INPUT " +
                                                        "RETURNS text " +
                                                        "LANGUAGE java\n" +
                                                        "AS 'return \"called\";';");
            values.add(typeDef.referenceValue);
        }

        createTableDDL += ')';
        createTable(createTableDDL);

        insertDML += ") VALUES (1";
        for (TypesTestDef ignored : typeDefs)
            insertDML += ", ?";
        insertDML += ')';

        execute(insertDML, values.toArray());

        // second row with null values
        for (int i = 0; i < values.size(); i++)
            values.set(i, null);
        execute(insertDML.replace('1', '2'), values.toArray());

        // check argument input + return
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fCheckArgAndReturn + '(' + typeDef.columnName + ") FROM %s WHERE key = 1"),
                       row(new Object[]{ typeDef.referenceValue }));
        }

        // check for CALLED ON NULL INPUT with non-null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fCalledOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 1"),
                       row(new Object[]{ "called" }));
        }

        // check for CALLED ON NULL INPUT with null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fCalledOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 2"),
                       row(new Object[]{ "called" }));
        }

        // check for RETURNS NULL ON NULL INPUT with non-null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fReturnsNullOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 1"),
                       row(new Object[]{ "called" }));
        }

        // check for RETURNS NULL ON NULL INPUT with null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fReturnsNullOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 2"),
                       row(new Object[]{ null }));
        }

    }

    @Test
    public void testReplaceAllowNulls() throws Throwable
    {
        String fNulls = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE OR REPLACE FUNCTION %s(val int) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java\n" +
                                       "AS 'return \"foo bar\";';");
        String fNoNulls = createFunction(KEYSPACE,
                                         "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE java\n" +
                                         "AS 'return \"foo bar\";';");

        assertInvalid("CREATE OR REPLACE FUNCTION " + fNulls + "(val int) " +
                      "RETURNS NULL ON NULL INPUT " +
                      "RETURNS text " +
                      "LANGUAGE java\n" +
                      "AS 'return \"foo bar\";';");
        assertInvalid("CREATE OR REPLACE FUNCTION " + fNoNulls + "(val int) " +
                      "CALLED ON NULL INPUT " +
                      "RETURNS text " +
                      "LANGUAGE java\n" +
                      "AS 'return \"foo bar\";';");

        execute("CREATE OR REPLACE FUNCTION " + fNulls + "(val int) " +
                "CALLED ON NULL INPUT " +
                "RETURNS text " +
                "LANGUAGE java\n" +
                "AS 'return \"foo bar\";';");
        execute("CREATE OR REPLACE FUNCTION " + fNoNulls + "(val int) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS text " +
                "LANGUAGE java\n" +
                "AS 'return \"foo bar\";';");
    }

    @Test
    public void testBrokenFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1d);

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA\n" +
                                      "AS 'throw new RuntimeException();';");

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(KEYSPACE_PER_TEST);
        UDFunction f = (UDFunction) ksm.functions.get(parseFunctionName(fName)).iterator().next();

        UDFunction broken = UDFunction.createBrokenFunction(f.name(),
                                                            f.argNames(),
                                                            f.argTypes(),
                                                            f.returnType(),
                                                            true,
                                                            "java",
                                                            f.body(),
                                                            new InvalidRequestException("foo bar is broken"));
        Schema.instance.setKeyspaceMetadata(ksm.withSwapped(ksm.functions.without(f.name(), f.argTypes()).with(broken)));

        assertInvalidThrowMessage("foo bar is broken", InvalidRequestException.class,
                                  "SELECT key, " + fName + "(dval) FROM %s");
    }

    @Test
    public void testFunctionExecutionExceptionNet() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1d);

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA\n" +
                                      "AS 'throw new RuntimeException();'");

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            try
            {
                assertRowsNet(version,
                              executeNet(version, "SELECT " + fName + "(dval) FROM %s WHERE key = 1"));
                Assert.fail();
            }
            catch (com.datastax.driver.core.exceptions.FunctionExecutionException fee)
            {
                // Java driver neither throws FunctionExecutionException nor does it set the exception code correctly
                Assert.assertTrue(version >= Server.VERSION_4);
            }
            catch (InvalidQueryException e)
            {
                Assert.assertTrue(version < Server.VERSION_4);
            }
        }
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

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenSetArg(values frozen<set<int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS int " +
                             "LANGUAGE java\n" +
                             "AS 'int sum = 0; for (Object value : values) {sum += value;} return sum;';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values set<int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<set<int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "set<int>",
                                             "CREATE FUNCTION %s (values set<int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS int " +
                                             "LANGUAGE java\n" +
                                             "AS 'int sum = 0; for (Object value : values) {sum += ((Integer) value);} return sum;';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, 0));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, 6));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, 15));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, 24));

        functionName = createFunction(KEYSPACE,
                                      "set<int>",
                                             "CREATE FUNCTION %s (values set<int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS set<int> " +
                                             "LANGUAGE java\n" +
                                             "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", set(1, 2, 3)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<set<int>>);");
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

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<list<int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS int " +
                             "LANGUAGE java\n" +
                             "AS 'int sum = 0; for (Object value : values) {sum += value;} return sum;';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values list<int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<list<int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "list<int>",
                                             "CREATE FUNCTION %s (values list<int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS int " +
                                             "LANGUAGE java\n" +
                                             "AS 'int sum = 0; for (Object value : values) {sum += ((Integer) value);} return sum;';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, 0));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, 6));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, 15));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, 24));

        functionName = createFunction(KEYSPACE,
                                      "list<int>",
                                      "CREATE FUNCTION %s (values list<int>) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS list<int> " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", set(1, 2, 3)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<list<int>>);");
    }

    @Test
    public void testFunctionWithFrozenMapType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<map<int, int>>)");
        createIndex("CREATE INDEX ON %s (FULL(b))");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, map());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, map(1, 1, 2, 2, 3, 3));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, map(4, 4, 5, 5, 6, 6));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, map(7, 7, 8, 8, 9, 9));

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<map<int, int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS int " +
                             "LANGUAGE java\n" +
                             "AS 'int sum = 0; for (Object value : values.values()) {sum += value;} return sum;';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values map<int, int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<map<int, int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "map<int, int>",
                                             "CREATE FUNCTION %s (values map<int, int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS int " +
                                             "LANGUAGE java\n" +
                                             "AS 'int sum = 0; for (Object value : values.values()) {sum += ((Integer) value);} return sum;';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, 0));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, 6));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, 15));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, 24));

        functionName = createFunction(KEYSPACE,
                                      "map<int, int>",
                                      "CREATE FUNCTION %s (values map<int, int>) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS map<int, int> " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", map(1, 1, 2, 2, 3, 3)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<map<int, int>>);");
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

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<tuple<int, int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS text " +
                             "LANGUAGE java\n" +
                             "AS 'return values.toString();';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values tuple<int, int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<tuple<int, int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "tuple<int, int>",
                                             "CREATE FUNCTION %s (values tuple<int, int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS text " +
                                             "LANGUAGE java\n" +
                                             "AS 'return values.toString();';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, "(null, null)"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, "(1, 2)"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, "(4, 5)"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, "(7, 8)"));

        functionName = createFunction(KEYSPACE,
                                      "tuple<int, int>",
                                      "CREATE FUNCTION %s (values tuple<int, int>) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS tuple<int, int> " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", tuple(1, 2)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<tuple<int, int>>);");
    }

    @Test
    public void testFunctionWithFrozenUDType() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + myType + ">)");
        createIndex("CREATE INDEX ON %s (b)");

        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 0, 0);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 1, 1);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 2, 4);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 3, 7);

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<" + myType + ">) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS text " +
                             "LANGUAGE java\n" +
                             "AS 'return values.toString();';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values " + myType + ") " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<" + myType + "> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             myType,
                                             "CREATE FUNCTION %s (values " + myType + ") " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS text " +
                                             "LANGUAGE java\n" +
                                             "AS 'return values.toString();';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, "{f:0}"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, "{f:1}"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, "{f:4}"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, "{f:7}"));

        functionName = createFunction(KEYSPACE,
                                      myType,
                                      "CREATE FUNCTION %s (values " + myType + ") " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS " + myType + " " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "({f: ?})", 1),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<" + myType + ">);");
    }

    @Test
    public void testEmptyString() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, sval text, aval ascii, bval blob, empty_int int)");
        execute("INSERT INTO %s (key, sval, aval, bval, empty_int) VALUES (?, ?, ?, ?, blobAsInt(0x))", 1, "", "", ByteBuffer.allocate(0));

        String fNameSRC = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameSCC = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return \"\";'");

        String fNameSRN = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameSCN = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return \"\";'");

        String fNameBRC = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameBCC = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return ByteBuffer.allocate(0);'");

        String fNameBRN = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameBCN = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return ByteBuffer.allocate(0);'");

        String fNameIRC = createFunction(KEYSPACE_PER_TEST, "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameICC = createFunction(KEYSPACE_PER_TEST, "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return 0;'");

        String fNameIRN = createFunction(KEYSPACE_PER_TEST, "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameICN = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return 0;'");

        assertRows(execute("SELECT " + fNameSRC + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSRN + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCC + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCN + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSRC + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSRN + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCC + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCN + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameBRC + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameBRN + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameBCC + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameBCN + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameIRC + "(empty_int) FROM %s"), row(new Object[]{null}));
        assertRows(execute("SELECT " + fNameIRN + "(empty_int) FROM %s"), row(new Object[]{null}));
        assertRows(execute("SELECT " + fNameICC + "(empty_int) FROM %s"), row(0));
        assertRows(execute("SELECT " + fNameICN + "(empty_int) FROM %s"), row(new Object[]{null}));
    }
}
