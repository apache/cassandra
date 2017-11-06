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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.JavaBasedUDFunction;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

public class UFTest extends CQLTester
{
    @Test
    public void testJavaSourceName()
    {
        Assert.assertEquals("String", JavaBasedUDFunction.javaSourceName(TypeToken.of(String.class)));
        Assert.assertEquals("java.util.Map<Integer, String>", JavaBasedUDFunction.javaSourceName(TypeTokens.mapOf(Integer.class, String.class)));
        Assert.assertEquals("com.datastax.driver.core.UDTValue", JavaBasedUDFunction.javaSourceName(TypeToken.of(UDTValue.class)));
        Assert.assertEquals("java.util.Set<com.datastax.driver.core.UDTValue>", JavaBasedUDFunction.javaSourceName(TypeTokens.setOf(UDTValue.class)));
    }

    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidThrowMessage("Cannot drop non existing function", InvalidRequestException.class, "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist");
        assertInvalidThrowMessage("Cannot drop non existing function", InvalidRequestException.class, "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist(int,text)");
        assertInvalidThrowMessage("Cannot drop non existing function", InvalidRequestException.class, "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist");
        assertInvalidThrowMessage("Cannot drop non existing function", InvalidRequestException.class, "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist(int,text)");

        execute("DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist");
        execute("DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist(int,text)");
        execute("DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist");
        execute("DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist(int,text)");
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

    private void checkDelayedValuesCorrectlyIdentifyFunctionsInUse(boolean dropKeyspace) throws Throwable
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
    public void testFunctionExecutionWithReversedTypeAsOutput() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v text, PRIMARY KEY(k, v)) WITH CLUSTERING ORDER BY (v DESC)");

        String fRepeat = createFunction(KEYSPACE_PER_TEST, "text",
                                        "CREATE FUNCTION %s(v text) " +
                                        "RETURNS NULL ON NULL INPUT " +
                                        "RETURNS text " +
                                        "LANGUAGE java " +
                                        "AS 'return v + v;'");

        execute("INSERT INTO %s(k, v) VALUES (?, " + fRepeat + "(?))", 1, "a");
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
        assertInvalidMessage("Keyspace this_ks_does_not_exist doesn't exist",
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

        assertInvalidMessage("Keyspace " + KEYSPACE_PER_TEST + " doesn't exist",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE_PER_TEST + ".jnft(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
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

        for (ProtocolVersion version : PROTOCOL_VERSIONS)
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
                Assert.assertTrue(version.isGreaterOrEqualTo(ProtocolVersion.V4));
            }
            catch (InvalidQueryException e)
            {
                Assert.assertTrue(version.isSmallerThan(ProtocolVersion.V4));
            }
        }
    }

    @Test
    public void testArgumentGenerics() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, sval text, aval ascii, bval blob, empty_int int)");

        String typeName = createType("CREATE TYPE %s (txt text, i int)");

        createFunction(KEYSPACE, "map<text,bigint>,list<text>",
                       "CREATE FUNCTION IF NOT EXISTS %s(state map<text,bigint>, styles list<text>)\n" +
                       "  RETURNS NULL ON NULL INPUT\n" +
                       "  RETURNS map<text,bigint>\n" +
                       "  LANGUAGE java\n" +
                       "  AS $$\n" +
                       "    for (String style : styles) {\n" +
                       "      if (state.containsKey(style)) {\n" +
                       "        state.put(style, state.get(style) + 1L);\n" +
                       "      } else {\n" +
                       "        state.put(style, 1L);\n" +
                       "      }\n" +
                       "    }\n" +
                       "    return state;\n" +
                       "  $$");

        createFunction(KEYSPACE, "text",
                                  "CREATE OR REPLACE FUNCTION %s("                 +
                                  "  listText list<text>,"                         +
                                  "  setText set<text>,"                           +
                                  "  mapTextInt map<text, int>,"                   +
                                  "  mapListTextSetInt map<frozen<list<text>>, frozen<set<int>>>," +
                                  "  mapTextTuple map<text, frozen<tuple<int, text>>>," +
                                  "  mapTextType map<text, frozen<" + typeName + ">>" +
                                  ") "                                             +
                                  "CALLED ON NULL INPUT "                          +
                                  "RETURNS map<frozen<list<text>>, frozen<set<int>>> " +
                                  "LANGUAGE JAVA\n"                                +
                                  "AS $$" +
                                  "     for (String s : listtext) {};" +
                                  "     for (String s : settext) {};" +
                                  "     for (String s : maptextint.keySet()) {};" +
                                  "     for (Integer s : maptextint.values()) {};" +
                                  "     for (java.util.List<String> l : maplisttextsetint.keySet()) {};" +
                                  "     for (java.util.Set<Integer> s : maplisttextsetint.values()) {};" +
                                  "     for (com.datastax.driver.core.TupleValue t : maptexttuple.values()) {};" +
                                  "     for (com.datastax.driver.core.UDTValue u : maptexttype.values()) {};" +
                                  "     return maplisttextsetint;" +
                                  "$$");
    }

    @Test
    public void testArgAndReturnTypes() throws Throwable
    {

        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");
        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'foo', i: 42})");

        // Java UDFs

        String f = createFunction(KEYSPACE, "int",
                                  "CREATE OR REPLACE FUNCTION %s(val int) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS " + type + ' ' +
                                  "LANGUAGE JAVA\n" +
                                  "AS 'return udfContext.newReturnUDTValue();';");

        assertRows(execute("SELECT " + f + "(key) FROM %s"),
                   row(userType("txt", null, "i", null)));

        f = createFunction(KEYSPACE, "int",
                           "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS " + type + ' ' +
                           "LANGUAGE JAVA\n" +
                           "AS $$" +
                           "   com.datastax.driver.core.UDTValue udt = udfContext.newArgUDTValue(\"val\");" +
                           "   udt.setString(\"txt\", \"baz\");" +
                           "   udt.setInt(\"i\", 88);" +
                           "   return udt;" +
                           "$$;");

        assertRows(execute("SELECT " + f + "(udt) FROM %s"),
                   row(userType("txt", "baz", "i", 88)));

        f = createFunction(KEYSPACE, "int",
                           "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS tuple<text, int>" +
                           "LANGUAGE JAVA\n" +
                           "AS $$" +
                           "   com.datastax.driver.core.TupleValue tv = udfContext.newReturnTupleValue();" +
                           "   tv.setString(0, \"baz\");" +
                           "   tv.setInt(1, 88);" +
                           "   return tv;" +
                           "$$;");

        assertRows(execute("SELECT " + f + "(udt) FROM %s"),
                   row(tuple("baz", 88)));

        // JavaScript UDFs

        f = createFunction(KEYSPACE, "int",
                           "CREATE OR REPLACE FUNCTION %s(val int) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS " + type + ' ' +
                           "LANGUAGE JAVASCRIPT\n" +
                           "AS $$" +
                           "   udt = udfContext.newReturnUDTValue();" +
                           "   udt;" +
                           "$$;");

        assertRows(execute("SELECT " + f + "(key) FROM %s"),
                   row(userType("txt", null, "i", null)));

        f = createFunction(KEYSPACE, "int",
                           "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS " + type + ' ' +
                           "LANGUAGE JAVASCRIPT\n" +
                           "AS $$" +
                           "   udt = udfContext.newArgUDTValue(0);" +
                           "   udt.setString(\"txt\", \"baz\");" +
                           "   udt.setInt(\"i\", 88);" +
                           "   udt;" +
                           "$$;");

        assertRows(execute("SELECT " + f + "(udt) FROM %s"),
                   row(userType("txt", "baz", "i", 88)));

        f = createFunction(KEYSPACE, "int",
                           "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS tuple<text, int>" +
                           "LANGUAGE JAVASCRIPT\n" +
                           "AS $$" +
                           "   tv = udfContext.newReturnTupleValue();" +
                           "   tv.setString(0, \"baz\");" +
                           "   tv.setInt(1, 88);" +
                           "   tv;" +
                           "$$;");

        assertRows(execute("SELECT " + f + "(udt) FROM %s"),
                   row(tuple("baz", 88)));

        createFunction(KEYSPACE, "map",
                       "CREATE FUNCTION %s(my_map map<text, text>)\n" +
                       "         CALLED ON NULL INPUT\n" +
                       "         RETURNS text\n" +
                       "         LANGUAGE java\n" +
                       "         AS $$\n" +
                       "             String buffer = \"\";\n" +
                       "             for(java.util.Map.Entry<String, String> entry: my_map.entrySet()) {\n" +
                       "                 buffer = buffer + entry.getKey() + \": \" + entry.getValue() + \", \";\n" +
                       "             }\n" +
                       "             return buffer;\n" +
                       "         $$;\n");
    }

    @Test
    public void testImportJavaUtil() throws Throwable
    {
        createFunction(KEYSPACE, "list<text>",
                "CREATE OR REPLACE FUNCTION %s(listText list<text>) "                                             +
                        "CALLED ON NULL INPUT "                          +
                        "RETURNS set<text> " +
                        "LANGUAGE JAVA\n"                                +
                        "AS $$\n" +
                        "     Set<String> set = new HashSet<String>(); " +
                        "     for (String s : listtext) {" +
                        "            set.add(s);" +
                        "     }" +
                        "     return set;" +
                        "$$");

    }

    @Test
    public void testAnyUserTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, sval text)");
        execute("INSERT INTO %s (key, sval) VALUES (1, 'foo')");

        String udt = createType("CREATE TYPE %s (a int, b text, c bigint)");

        String fUdt = createFunction(KEYSPACE, "text",
                                     "CREATE OR REPLACE FUNCTION %s(arg text) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS " + udt + " " +
                                     "LANGUAGE JAVA\n" +
                                     "AS $$\n" +
                                     "    UDTValue udt = udfContext.newUDTValue(\"" + udt + "\");" +
                                     "    udt.setInt(\"a\", 42);" +
                                     "    udt.setString(\"b\", \"42\");" +
                                     "    udt.setLong(\"c\", 4242);" +
                                     "    return udt;" +
                                     "$$");

        assertRows(execute("SELECT " + fUdt + "(sval) FROM %s"),
                   row(userType("a", 42, "b", "42", "c", 4242L)));

        String fTup = createFunction(KEYSPACE, "text",
                                     "CREATE OR REPLACE FUNCTION %s(arg text) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS tuple<int, " + udt + "> " +
                                     "LANGUAGE JAVA\n" +
                                     "AS $$\n" +
                                     "    UDTValue udt = udfContext.newUDTValue(\"" + udt + "\");" +
                                     "    udt.setInt(\"a\", 42);" +
                                     "    udt.setString(\"b\", \"42\");" +
                                     "    udt.setLong(\"c\", 4242);" +
                                     "    TupleValue tup = udfContext.newTupleValue(\"tuple<int," + udt + ">\");" +
                                     "    tup.setInt(0, 88);" +
                                     "    tup.setUDTValue(1, udt);" +
                                     "    return tup;" +
                                     "$$");

        assertRows(execute("SELECT " + fTup + "(sval) FROM %s"),
                   row(tuple(88, userType("a", 42, "b", "42", "c", 4242L))));
    }

    @Test(expected = SyntaxException.class)
    public void testEmptyFunctionName() throws Throwable
    {
        execute("CREATE FUNCTION IF NOT EXISTS " + KEYSPACE + ".\"\" (arg int)\n" +
                "  RETURNS NULL ON NULL INPUT\n" +
                "  RETURNS int\n" +
                "  LANGUAGE java\n" +
                "  AS $$\n" +
                "    return a;\n" +
                "  $$");
    }

    @Test(expected = SyntaxException.class)
    public void testEmptyArgName() throws Throwable
    {
        execute("CREATE FUNCTION IF NOT EXISTS " + KEYSPACE + ".myfn (\"\" int)\n" +
                "  RETURNS NULL ON NULL INPUT\n" +
                "  RETURNS int\n" +
                "  LANGUAGE java\n" +
                "  AS $$\n" +
                "    return a;\n" +
                "  $$");
    }
}
