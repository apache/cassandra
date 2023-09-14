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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UFTest extends CQLTester
{
    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidThrowMessage(String.format("Function '%s.func_does_not_exist' doesn't exist", KEYSPACE),
                                  InvalidRequestException.class,
                                  "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist");

        assertInvalidThrowMessage(String.format("Function '%s.func_does_not_exist(int, text)' doesn't exist", KEYSPACE),
                                  InvalidRequestException.class,
                                  "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist(int, text)");

        assertInvalidThrowMessage("Function 'keyspace_does_not_exist.func_does_not_exist' doesn't exist",
                                  InvalidRequestException.class,
                                  "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist");

        assertInvalidThrowMessage("Function 'keyspace_does_not_exist.func_does_not_exist(int, text)' doesn't exist",
                                  InvalidRequestException.class,
                                  "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist(int, text)");

        execute("DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist");
        execute("DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist(int,text)");
        execute("DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist");
        execute("DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist(int,text)");
    }

    @Test
    public void testSchemaChange()
    {
        String f = createFunctionName(KEYSPACE);
        String functionName = shortFunctionName(f);
        registerFunction(f, "double, double");

        assertSchemaChange("CREATE OR REPLACE FUNCTION " + f + "(state double, val double)" +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           "LANGUAGE java " +
                           "AS ' return Double.valueOf(Math.max(state, val)); ';",
                           Change.CREATED,
                           Target.FUNCTION,
                           KEYSPACE, functionName,
                           "double", "double");

        registerFunction(f, "int, int");

        assertSchemaChange("CREATE OR REPLACE FUNCTION " + f + "(state int, val int) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS int " +
                           "LANGUAGE java " +
                           "AS ' return Integer.valueOf(Math.max(state, val));';",
                           Change.CREATED,
                           Target.FUNCTION,
                           KEYSPACE, functionName,
                           "int", "int");

        assertSchemaChange("CREATE OR REPLACE FUNCTION " + f + "(state int, val int) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS int " +
                           "LANGUAGE java " +
                           "AS ' return Integer.valueOf(Math.min(state, val));';",
                           Change.UPDATED,
                           Target.FUNCTION,
                           KEYSPACE, functionName,
                           "int", "int");

        assertSchemaChange("DROP FUNCTION " + f + "(double, double)",
                           Change.DROPPED, Target.FUNCTION,
                           KEYSPACE, functionName,
                           "double", "double");

        // The function with nested tuple should be created without throwing InvalidRequestException. See CASSANDRA-15857
        String fl = createFunctionName(KEYSPACE);
        registerFunction(fl, "list<tuple<int, int>>, double");

        assertSchemaChange("CREATE OR REPLACE FUNCTION " + fl + "(state list<tuple<int, int>>, val double) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           "LANGUAGE java " +
                           "AS ' return val;';",
                           Change.CREATED, Target.FUNCTION,
                           KEYSPACE, shortFunctionName(fl),
                           "list<tuple<int, int>>", "double");
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

        Assert.assertEquals(1, Schema.instance.getUserFunctions(parseFunctionName(fSin)).size());

        assertRows(execute("SELECT function_name, language FROM system_schema.functions WHERE keyspace_name=?", KEYSPACE_PER_TEST),
                   row(fSinName.name, "java"));

        dropPerTestKeyspace();

        assertRows(execute("SELECT function_name, language FROM system_schema.functions WHERE keyspace_name=?", KEYSPACE_PER_TEST));

        Assert.assertEquals(0, Schema.instance.getUserFunctions(fSinName).size());
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

        Assert.assertEquals(1, Schema.instance.getUserFunctions(parseFunctionName(fSin)).size());

        // create a pairs of Select and Inserts. One statement in each pair uses the function so when we
        // drop it those statements should be removed from the cache in QueryProcessor. The other statements
        // should be unaffected.

        ResultMessage.Prepared preparedSelect1 = QueryProcessor.instance.prepare(
                                                                       String.format("SELECT key, %s(d) FROM %s.%s", fSin, KEYSPACE, currentTable()),
                                                                       ClientState.forInternalCalls());
        ResultMessage.Prepared preparedSelect2 = QueryProcessor.instance.prepare(
                                                    String.format("SELECT key FROM %s.%s", KEYSPACE, currentTable()),
                                                    ClientState.forInternalCalls());
        ResultMessage.Prepared preparedInsert1 = QueryProcessor.instance.prepare(
                                                      String.format("INSERT INTO %s.%s (key, d) VALUES (?, %s(?))", KEYSPACE, currentTable(), fSin),
                                                      ClientState.forInternalCalls());
        ResultMessage.Prepared preparedInsert2 = QueryProcessor.instance.prepare(
                                                      String.format("INSERT INTO %s.%s (key, d) VALUES (?, ?)", KEYSPACE, currentTable()),
                                                      ClientState.forInternalCalls());

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

        Assert.assertEquals(1, Schema.instance.getUserFunctions(fSinName).size());

        preparedSelect1= QueryProcessor.instance.prepare(
                                         String.format("SELECT key, %s(d) FROM %s.%s", fSin, KEYSPACE, currentTable()),
                                         ClientState.forInternalCalls());
        preparedInsert1 = QueryProcessor.instance.prepare(
                                         String.format("INSERT INTO %s.%s (key, d) VALUES (?, %s(?))", KEYSPACE, currentTable(), fSin),
                                         ClientState.forInternalCalls());
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

        ResultMessage.Prepared prepared = QueryProcessor.instance.prepare(
                                                                String.format("INSERT INTO %s.%s (key, val) VALUES (?, %s)",
                                                                             KEYSPACE,
                                                                             currentTable(),
                                                                             literalArgs),
                                                                ClientState.forInternalCalls());
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));
        return prepared;
    }

    private ResultMessage.Prepared prepareStatementWithDelayedValueTuple(String function)
    {
        createTable("CREATE TABLE %s (" +
                    " key int PRIMARY KEY," +
                    " val tuple<double> )");

        ResultMessage.Prepared prepared = QueryProcessor.instance.prepare(
                                                                String.format("INSERT INTO %s.%s (key, val) VALUES (?, (%s(0.0)))",
                                                                             KEYSPACE,
                                                                             currentTable(),
                                                                             function),
                                                                ClientState.forInternalCalls());
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));
        return prepared;
    }

    private void checkDelayedValuesCorrectlyIdentifyFunctionsInUse(boolean dropKeyspace) throws Throwable
    {
        // prepare a statement which doesn't use any function for a control
        createTable("CREATE TABLE %s (" +
                    " key int PRIMARY KEY," +
                    " val double)");
        ResultMessage.Prepared control = QueryProcessor.instance.prepare(
                                                               String.format("INSERT INTO %s.%s (key, val) VALUES (?, ?)",
                                                                            KEYSPACE,
                                                                            currentTable()),
                                                               ClientState.forInternalCalls());
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(control.statementId));

        // a function that we'll drop and verify that statements which use it to
        // provide a DelayedValue are removed from the cache in QueryProcessor
        String function = createFunction(KEYSPACE_PER_TEST, "double",
                                        "CREATE FUNCTION %s ( input double ) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS double " +
                                        "LANGUAGE java " +
                                        "AS 'return Double.valueOf(Math.log(input.doubleValue()));'");
        Assert.assertEquals(1, Schema.instance.getUserFunctions(parseFunctionName(function)).size());

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
                             "LANGUAGE java AS 'return \"42d\";'");

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
        assertInvalidMessage(String.format("Function '%s' doesn't exist", fSin), "DROP FUNCTION " + fSin);
        // but don't complain with "IF EXISTS"
        execute("DROP FUNCTION IF EXISTS " + fSin);

        // can't drop native functions
        assertInvalidMessage("System keyspace 'system' is not user-modifiable", "DROP FUNCTION to_timestamp");
        assertInvalidMessage("System keyspace 'system' is not user-modifiable", "DROP FUNCTION uuid");

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
        assertInvalidMessage(String.format("Function '%s(boolean)' doesn't exist", fOverload), "DROP FUNCTION " + fOverload + "(boolean)");
        assertInvalidMessage(String.format("Function '%s(bigint)' doesn't exist", fOverload), "DROP FUNCTION " + fOverload + "(bigint)");

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
        execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".to_timestamp(val timeuuid) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS timestamp " +
                "LANGUAGE JAVA\n" +

                "AS 'return null;';");

        assertInvalidMessage("System keyspace 'system' is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION system.jnft(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("System keyspace 'system' is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION system.to_timestamp(val timeuuid) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS timestamp " +
                             "LANGUAGE JAVA\n" +

                             "AS 'return null;';");
        assertInvalidMessage("System keyspace 'system' is not user-modifiable",
                             "DROP FUNCTION system.now");

        // KS for executeLocally() is system
        assertInvalidMessage("System keyspace 'system' is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION jnft(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("System keyspace 'system' is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION to_timestamp(val timeuuid) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS timestamp " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("System keyspace 'system' is not user-modifiable",
                             "DROP FUNCTION now");
    }

    @Test
    public void testFunctionNonExistingKeyspace() throws Throwable
    {
        assertInvalidMessage("Keyspace 'this_ks_does_not_exist' doesn't exist",
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

        assertInvalidMessage("Keyspace '" + KEYSPACE_PER_TEST + "' doesn't exist",
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

        Assert.assertEquals(1, Schema.instance.getUserFunctions(fNameName).size());

        ResultMessage.Prepared prepared = QueryProcessor.instance.prepare(String.format("SELECT key, %s(udt) FROM %s.%s", fName, KEYSPACE, currentTable()),
                                                                 ClientState.forInternalCalls());
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        // UT still referenced by table
        assertInvalidMessage("Cannot drop user type", "DROP TYPE " + type);

        execute("DROP TABLE %s");

        // UT still referenced by UDF
        assertInvalidMessage("as it is still used by function", "DROP TYPE " + type);

        Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        // function stays
        Assert.assertEquals(1, Schema.instance.getUserFunctions(fNameName).size());
    }

    @Test
    public void testDuplicateArgNames() throws Throwable
    {
        assertInvalidMessage("Duplicate argument names for given function",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(input double, input int) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE java AS 'return Math.max(input, input)';");
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

        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE_PER_TEST);
        UDFunction f = (UDFunction) ksm.userFunctions.get(parseFunctionName(fName)).iterator().next();

        UDFunction broken = UDFunction.createBrokenFunction(f.name(),
                                                            f.argNames(),
                                                            f.argTypes(),
                                                            f.returnType(),
                                                            true,
                                                            "java",
                                                            f.body(),
                                                            new InvalidRequestException("foo bar is broken"));
        SchemaTestUtil.addOrUpdateKeyspace(ksm.withSwapped(ksm.userFunctions.without(f.name(), f.argTypes()).with(broken)), false);

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
    public void testEmptyString() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, sval text, aval ascii, bval blob, empty_int int)");
        execute("INSERT INTO %s (key, sval, aval, bval, empty_int) VALUES (?, ?, ?, ?, blob_as_int(0x))", 1, "", "", ByteBuffer.allocate(0));

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

        String fNameICN = createFunction(KEYSPACE_PER_TEST, "int",
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
        assertRows(execute("SELECT " + fNameIRC + "(empty_int) FROM %s"), row(new Object[]{ null }));
        assertRows(execute("SELECT " + fNameIRN + "(empty_int) FROM %s"), row(new Object[]{ null }));
        assertRows(execute("SELECT " + fNameICC + "(empty_int) FROM %s"), row(0));
        assertRows(execute("SELECT " + fNameICN + "(empty_int) FROM %s"), row(new Object[]{ null }));
    }

    @Test
    public void testRejectInvalidFunctionNamesOnCreation()
    {
        for (String funcName : Arrays.asList("my/fancy/func", "my_other[fancy]func"))
        {
            assertThatThrownBy(() -> {
                createFunctionOverload(String.format("%s.\"%s\"", KEYSPACE_PER_TEST, funcName), "int",
                                       "CREATE OR REPLACE FUNCTION %s(val int) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE JAVA\n" +
                                       "AS 'return val;'");
            }).hasRootCauseInstanceOf(InvalidRequestException.class)
              .hasRootCauseMessage("Function name '%s' is invalid", funcName);
        }
    }
}
