/*
 *
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
package org.apache.cassandra.cql3.selection;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SelectionColumnMappingTest extends CQLTester
{
    private static final ColumnDefinition NULL_DEF = null;
    String tableName;
    String typeName;
    UserType userType;
    String functionName;

    @BeforeClass
    public static void setUpClass()     // overrides CQLTester.setUpClass()
    {
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);

        prepareServer();
    }

    @Test
    public void testSelectionColumnMapping() throws Throwable
    {
        // Organised as a single test to avoid the overhead of
        // table creation for each variant

        typeName = createType("CREATE TYPE %s (f1 int, f2 text)");
        tableName = createTable("CREATE TABLE %s (" +
                                " k int PRIMARY KEY," +
                                " v1 int," +
                                " v2 ascii," +
                                " v3 frozen<" + typeName + ">)");
        userType = Schema.instance.getKSMetaData(KEYSPACE).types.get(ByteBufferUtil.bytes(typeName)).get().freeze();
        functionName = createFunction(KEYSPACE, "int, ascii",
                                      "CREATE FUNCTION %s (i int, a ascii) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE java " +
                                      "AS 'return Integer.valueOf(i);'");
        execute("INSERT INTO %s (k, v1 ,v2, v3) VALUES (1, 1, 'foo', {f1:1, f2:'bar'})");

        testSimpleTypes();
        testWildcard();
        testSimpleTypesWithAliases();
        testUserTypes();
        testUserTypesWithAliases();
        testWritetimeAndTTL();
        testWritetimeAndTTLWithAliases();
        testFunction();
        testNoArgFunction();
        testUserDefinedFunction();
        testOverloadedFunction();
        testFunctionWithAlias();
        testNoArgumentFunction();
        testNestedFunctions();
        testNestedFunctionsWithArguments();
        testCount();
        testDuplicateFunctionsWithoutAliases();
        testDuplicateFunctionsWithAliases();
        testSelectDistinct();
        testMultipleAliasesOnSameColumn();
        testMixedColumnTypes();
        testMultipleUnaliasedSelectionOfSameColumn();
        testUserDefinedAggregate();
    }

    @Test
    public void testMultipleArgumentFunction() throws Throwable
    {
        // demonstrate behaviour of token() with composite partition key
        tableName = createTable("CREATE TABLE %s (a int, b text, PRIMARY KEY ((a, b)))");
        ColumnSpecification tokenSpec = columnSpecification("system.token(a, b)", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(tokenSpec, columnDefinitions("a", "b"));
        // we don't use verify like with the other tests because this query will produce no results
        SelectStatement statement = getSelect("SELECT token(a,b) FROM %s");
        verifyColumnMapping(expected, statement);
        statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
    }

    private void testSimpleTypes() throws Throwable
    {
        // simple column identifiers without aliases are represented in
        // ResultSet.Metadata by the underlying ColumnDefinition
        ColumnSpecification kSpec = columnSpecification("k", Int32Type.instance);
        ColumnSpecification v1Spec = columnSpecification("v1", Int32Type.instance);
        ColumnSpecification v2Spec = columnSpecification("v2", AsciiType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(kSpec, columnDefinition("k"))
                                                                .addMapping(v1Spec, columnDefinition("v1"))
                                                                .addMapping(v2Spec, columnDefinition("v2"));

        verify(expected, "SELECT k, v1, v2 FROM %s");
    }

    private void testWildcard() throws Throwable
    {
        // Wildcard select represents each column in the table with a ColumnDefinition
        // in the ResultSet metadata
        ColumnDefinition kSpec = columnDefinition("k");
        ColumnDefinition v1Spec = columnDefinition("v1");
        ColumnDefinition v2Spec = columnDefinition("v2");
        ColumnDefinition v3Spec = columnDefinition("v3");
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(kSpec, columnDefinition("k"))
                                                                .addMapping(v1Spec, columnDefinition("v1"))
                                                                .addMapping(v2Spec, columnDefinition("v2"))
                                                                .addMapping(v3Spec, columnDefinition("v3"));

        verify(expected, "SELECT * FROM %s");
    }

    private void testSimpleTypesWithAliases() throws Throwable
    {
        // simple column identifiers with aliases are represented in ResultSet.Metadata
        // by a ColumnSpecification based on the underlying ColumnDefinition
        ColumnSpecification kSpec = columnSpecification("k_alias", Int32Type.instance);
        ColumnSpecification v1Spec = columnSpecification("v1_alias", Int32Type.instance);
        ColumnSpecification v2Spec = columnSpecification("v2_alias", AsciiType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(kSpec, columnDefinition("k"))
                                                                .addMapping(v1Spec, columnDefinition("v1"))
                                                                .addMapping(v2Spec, columnDefinition("v2"));
        verify(expected, "SELECT k AS k_alias, v1 AS v1_alias, v2 AS v2_alias FROM %s");
    }

    private void testUserTypes() throws Throwable
    {
        // User type fields are represented in ResultSet.Metadata by a
        // ColumnSpecification denoting the name and type of the particular field
        ColumnSpecification f1Spec = columnSpecification("v3.f1", Int32Type.instance);
        ColumnSpecification f2Spec = columnSpecification("v3.f2", UTF8Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(f1Spec, columnDefinition("v3"))
                                                                .addMapping(f2Spec, columnDefinition("v3"));

        verify(expected, "SELECT v3.f1, v3.f2 FROM %s");
    }

    private void testUserTypesWithAliases() throws Throwable
    {
        // User type fields with aliases are represented in ResultSet.Metadata
        // by a ColumnSpecification with the alias name and the type of the actual field
        ColumnSpecification f1Spec = columnSpecification("f1_alias", Int32Type.instance);
        ColumnSpecification f2Spec = columnSpecification("f2_alias", UTF8Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(f1Spec, columnDefinition("v3"))
                                                                .addMapping(f2Spec, columnDefinition("v3"));

        verify(expected, "SELECT v3.f1 AS f1_alias, v3.f2 AS f2_alias FROM %s");
    }

    private void testWritetimeAndTTL() throws Throwable
    {
        // writetime and ttl are represented in ResultSet.Metadata by a ColumnSpecification
        // with the function name plus argument and a long or int type respectively
        ColumnSpecification wtSpec = columnSpecification("writetime(v1)", LongType.instance);
        ColumnSpecification ttlSpec = columnSpecification("ttl(v2)", Int32Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(wtSpec, columnDefinition("v1"))
                                                                .addMapping(ttlSpec, columnDefinition("v2"));

        verify(expected, "SELECT writetime(v1), ttl(v2) FROM %s");
    }

    private void testWritetimeAndTTLWithAliases() throws Throwable
    {
        // writetime and ttl with aliases are represented in ResultSet.Metadata
        // by a ColumnSpecification with the alias name and the appropriate numeric type
        ColumnSpecification wtSpec = columnSpecification("wt_alias", LongType.instance);
        ColumnSpecification ttlSpec = columnSpecification("ttl_alias", Int32Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(wtSpec, columnDefinition("v1"))
                                                                .addMapping(ttlSpec, columnDefinition("v2"));

        verify(expected, "SELECT writetime(v1) AS wt_alias, ttl(v2) AS ttl_alias FROM %s");
    }

    private void testFunction() throws Throwable
    {
        // a function such as intasblob(<col>) is represented in ResultSet.Metadata
        // by a ColumnSpecification with the function name plus args and the type set
        // to the function's return type
        ColumnSpecification fnSpec = columnSpecification("system.intasblob(v1)", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec, columnDefinition("v1"));

        verify(expected, "SELECT intasblob(v1) FROM %s");
    }

    private void testNoArgFunction() throws Throwable
    {
        // a no-arg function such as now() is represented in ResultSet.Metadata
        // but has no mapping to any underlying column
        ColumnSpecification fnSpec = columnSpecification("system.now()", TimeUUIDType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping().addMapping(fnSpec, NULL_DEF);

        verify(expected, "SELECT now() FROM %s");
    }

    private void testOverloadedFunction() throws Throwable
    {
        String fnName = createFunction(KEYSPACE, "int",
                                       "CREATE FUNCTION %s (input int) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return \"Hello World\";'");
        createFunctionOverload(fnName, "text",
                               "CREATE FUNCTION %s (input text) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"Hello World\";'");

        createFunctionOverload(fnName, "int, text",
                               "CREATE FUNCTION %s (input1 int, input2 text) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"Hello World\";'");
        ColumnSpecification fnSpec1 = columnSpecification(fnName + "(v1)", UTF8Type.instance);
        ColumnSpecification fnSpec2 = columnSpecification(fnName + "(v2)", UTF8Type.instance);
        ColumnSpecification fnSpec3 = columnSpecification(fnName + "(v1, v2)", UTF8Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec1, columnDefinition("v1"))
                                                                .addMapping(fnSpec2, columnDefinition("v2"))
                                                                .addMapping(fnSpec3, columnDefinitions("v1", "v2"));

        verify(expected, String.format("SELECT %1$s(v1), %1$s(v2), %1$s(v1, v2) FROM %%s", fnName));
    }

    private void testCount() throws Throwable
    {
        // SELECT COUNT does not necessarily include any mappings, but it must always return
        // a singleton list from getColumnSpecifications() in order for the ResultSet.Metadata
        // to be constructed correctly:
        // * COUNT(*) / COUNT(1) do not generate any mappings, as no specific columns are referenced
        // * COUNT(foo) does generate a mapping from the 'system.count' column spec to foo
        ColumnSpecification count = columnSpecification("count", LongType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping().addMapping(count, NULL_DEF);

        verify(expected, "SELECT COUNT(*) FROM %s");
        verify(expected, "SELECT COUNT(1) FROM %s");

        ColumnSpecification aliased = columnSpecification("count_alias", LongType.instance);
        expected = SelectionColumnMapping.newMapping().addMapping(aliased, NULL_DEF);

        verify(expected, "SELECT COUNT(*) AS count_alias FROM %s");
        verify(expected, "SELECT COUNT(1) AS count_alias FROM %s");

        ColumnSpecification countV1 = columnSpecification("system.count(v1)", LongType.instance);
        expected = SelectionColumnMapping.newMapping().addMapping(countV1, columnDefinition("v1"));
        verify(expected, "SELECT COUNT(v1) FROM %s");

        ColumnSpecification countV1Alias = columnSpecification("count_v1", LongType.instance);
        expected = SelectionColumnMapping.newMapping().addMapping(countV1Alias, columnDefinition("v1"));
        verify(expected, "SELECT COUNT(v1) AS count_v1 FROM %s");
    }

    private void testUserDefinedFunction() throws Throwable
    {
        // UDFs are basically represented in the same way as system functions
        String functionCall = String.format("%s(v1, v2)", functionName);
        ColumnSpecification fnSpec = columnSpecification(functionCall, Int32Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec, columnDefinitions("v1", "v2"));
        verify(expected, "SELECT " + functionCall + " FROM %s");
    }

    private void testFunctionWithAlias() throws Throwable
    {
        // a function with an alias is represented in ResultSet.Metadata by a
        // ColumnSpecification with the alias and the type set to the function's
        // return type
        ColumnSpecification fnSpec = columnSpecification("fn_alias", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec, columnDefinition("v1"));

        verify(expected, "SELECT intasblob(v1) AS fn_alias FROM %s");
    }

    public void testNoArgumentFunction() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("system.now()",
                                                                                          TimeUUIDType.instance),
                                                                      NULL_DEF);
        verify(expected, "SELECT now() FROM %s");
    }

    public void testNestedFunctionsWithArguments() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("system.blobasint(system.intasblob(v1))",
                                                                                          Int32Type.instance),
                                                                      columnDefinition("v1"));
        verify(expected, "SELECT blobasint(intasblob(v1)) FROM %s");
    }

    public void testNestedFunctions() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("system.tounixtimestamp(system.now())",
                                                                                          LongType.instance),
                                                                      NULL_DEF);
        verify(expected, "SELECT tounixtimestamp(now()) FROM %s");
    }

    public void testDuplicateFunctionsWithoutAliases() throws Throwable
    {
        // where duplicate functions are present, the ColumnSpecification list will
        // contain an entry per-duplicate but the mappings will be deduplicated (i.e.
        // a single mapping k/v pair regardless of the number of duplicates)
        ColumnSpecification spec = columnSpecification("system.intasblob(v1)", BytesType.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(spec, columnDefinition("v1"))
                                                          .addMapping(spec, columnDefinition("v1"));
        verify(expected, "SELECT intasblob(v1), intasblob(v1) FROM %s");
    }

    public void testDuplicateFunctionsWithAliases() throws Throwable
    {
        // where duplicate functions are present with distinct aliases, they are
        // represented as any other set of distinct columns would be - an entry
        // in theColumnSpecification list and a separate k/v mapping for each
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("blob_1", BytesType.instance),
                                                                      columnDefinition("v1"))
                                                          .addMapping(columnSpecification("blob_2", BytesType.instance),
                                                                      columnDefinition("v1"));
        verify(expected, "SELECT intasblob(v1) AS blob_1, intasblob(v1) AS blob_2 FROM %s");
    }

    public void testSelectDistinct() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping().addMapping(columnSpecification("k",
                                                                                                       Int32Type.instance),
                                                                                   columnDefinition("k"));
        verify(expected, "SELECT DISTINCT k FROM %s");

    }

    private void testMultipleAliasesOnSameColumn() throws Throwable
    {
        // Multiple result columns derived from the same underlying column are
        // represented by ColumnSpecifications
        ColumnSpecification alias1 = columnSpecification("alias_1", Int32Type.instance);
        ColumnSpecification alias2 = columnSpecification("alias_2", Int32Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(alias1, columnDefinition("v1"))
                                                                .addMapping(alias2, columnDefinition("v1"));

        verify(expected, "SELECT v1 AS alias_1, v1 AS alias_2 FROM %s");
    }

    private void testMultipleUnaliasedSelectionOfSameColumn() throws Throwable
    {
        // simple column identifiers without aliases are represented in
        // ResultSet.Metadata by the underlying ColumnDefinition
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("v1", Int32Type.instance),
                                                                      columnDefinition("v1"))
                                                          .addMapping(columnSpecification("v1", Int32Type.instance),
                                                                      columnDefinition("v1"));

        verify(expected, "SELECT v1, v1 FROM %s");
    }

    private void testMixedColumnTypes() throws Throwable
    {
        ColumnSpecification kSpec = columnSpecification("k_alias", Int32Type.instance);
        ColumnSpecification v1Spec = columnSpecification("writetime(v1)", LongType.instance);
        ColumnSpecification v2Spec = columnSpecification("ttl_alias", Int32Type.instance);
        ColumnSpecification f1Spec = columnSpecification("v3.f1", Int32Type.instance);
        ColumnSpecification f2Spec = columnSpecification("f2_alias", UTF8Type.instance);
        ColumnSpecification f3Spec = columnSpecification("v3", userType);

        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(kSpec, columnDefinition("k"))
                                                                .addMapping(v1Spec, columnDefinition("v1"))
                                                                .addMapping(v2Spec, columnDefinition("v2"))
                                                                .addMapping(f1Spec, columnDefinition("v3"))
                                                                .addMapping(f2Spec, columnDefinition("v3"))
                                                                .addMapping(f3Spec, columnDefinition("v3"));


        verify(expected, "SELECT k AS k_alias," +
                         "       writetime(v1)," +
                         "       ttl(v2) as ttl_alias," +
                         "       v3.f1," +
                         "       v3.f2 AS f2_alias," +
                         "       v3" +
                         " FROM %s");
    }

    private void testUserDefinedAggregate() throws Throwable
    {
        String sFunc = parseFunctionName(createFunction(KEYSPACE, "int",
                                                        " CREATE FUNCTION %s (a int, b int)" +
                                                        " RETURNS NULL ON NULL INPUT" +
                                                        " RETURNS int" +
                                                        " LANGUAGE javascript" +
                                                        " AS 'a + b'")).name;

        String aFunc = createAggregate(KEYSPACE, "int, int",
                                       " CREATE AGGREGATE %s (int)" +
                                       " SFUNC " + sFunc +
                                       " STYPE int" +
                                       " INITCOND 0");

        String plusOne = createFunction(KEYSPACE, "int",
                                        " CREATE FUNCTION %s (a int)" +
                                        " RETURNS NULL ON NULL INPUT" +
                                        " RETURNS int" +
                                        " LANGUAGE javascript" +
                                        " AS 'a+1'");

        String sqFunc = createFunction(KEYSPACE, "int",
                                       " CREATE FUNCTION %s (a int)" +
                                       " RETURNS NULL ON NULL INPUT" +
                                       " RETURNS int" +
                                       " LANGUAGE javascript" +
                                       " AS 'a*a'");

        ColumnDefinition v1 = columnDefinition("v1");
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification(aFunc + "(v1)",
                                                                                          Int32Type.instance),
                                                                      v1);
        verify(expected, String.format("SELECT %s(v1) FROM %%s", aFunc));

        // aggregate with nested udfs as input
        String specName = String.format("%s(%s(%s(v1)))", aFunc, sqFunc, plusOne);
        expected = SelectionColumnMapping.newMapping().addMapping(columnSpecification(specName, Int32Type.instance),
                                                                  v1);
        verify(expected, String.format("SELECT %s FROM %%s", specName));
    }

    private void verify(SelectionColumns expected, String query) throws Throwable
    {
        SelectStatement statement = getSelect(query);
        verifyColumnMapping(expected, statement);
        checkExecution(statement, expected.getColumnSpecifications());
    }

    private void checkExecution(SelectStatement statement, List<ColumnSpecification> expectedResultColumns)
    throws RequestExecutionException, RequestValidationException
    {
        UntypedResultSet rs = UntypedResultSet.create(statement.executeInternal(QueryState.forInternalCalls(),
                                                                                QueryOptions.DEFAULT).result);

        assertEquals(expectedResultColumns, rs.one().getColumns());
    }

    private SelectStatement getSelect(String query) throws RequestValidationException
    {
        CQLStatement statement = QueryProcessor.getStatement(String.format(query, KEYSPACE + "." + tableName),
                                                             ClientState.forInternalCalls()).statement;
        assertTrue(statement instanceof SelectStatement);
        return (SelectStatement)statement;
    }

    private void verifyColumnMapping(SelectionColumns expected, SelectStatement select)
    {
        assertEquals(expected, select.getSelection().getColumnMapping());
    }

    private Iterable<ColumnDefinition> columnDefinitions(String...names)
    {
        List<ColumnDefinition> defs = new ArrayList<>();
        for (String n : names)
            defs.add(columnDefinition(n));
        return defs;
    }

    private ColumnDefinition columnDefinition(String name)
    {
        return Schema.instance.getCFMetaData(KEYSPACE, tableName)
                              .getColumnDefinition(new ColumnIdentifier(name, true));

    }

    private ColumnSpecification columnSpecification(String name, AbstractType<?> type)
    {
        return new ColumnSpecification(KEYSPACE,
                                       tableName,
                                       new ColumnIdentifier(name, true),
                                       type);
    }
}
