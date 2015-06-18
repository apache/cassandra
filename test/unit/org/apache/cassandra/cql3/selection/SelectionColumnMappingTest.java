package org.apache.cassandra.cql3.selection;

import java.util.Collections;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SelectionColumnMappingTest extends CQLTester
{
    String tableName;
    String typeName;
    UserType userType;
    String functionName;

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
        userType = Schema.instance.getKSMetaData(KEYSPACE).userTypes.getType(ByteBufferUtil.bytes(typeName));
        functionName = createFunction(KEYSPACE, "int, ascii",
                                      "CREATE FUNCTION %s (i int, a ascii) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE java " +
                                      "AS 'return Integer.valueOf(i);'");
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
        testMultipleAliasesOnSameColumn();
        testCount();
        testMixedColumnTypes();
    }

    @Test
    public void testMultipleArgumentFunction() throws Throwable
    {
        // demonstrate behaviour of token() with composite partition key
        tableName = createTable("CREATE TABLE %s (a int, b text, PRIMARY KEY ((a, b)))");
        ColumnSpecification tokenSpec = columnSpecification("system.token(a, b)", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(tokenSpec, columnDefinition("a"))
                                                                .addMapping(tokenSpec, columnDefinition("b"));

        assertEquals(expected, extractColumnMappingFromSelect("SELECT token(a,b) FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT k, v1, v2 FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT * FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT k AS k_alias, v1 AS v1_alias, v2 AS v2_alias FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT v3.f1, v3.f2 FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT v3.f1 AS f1_alias, v3.f2 AS f2_alias FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT writetime(v1), ttl(v2) FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT writetime(v1) AS wt_alias, ttl(v2) AS ttl_alias FROM %s"));
    }

    private void testFunction() throws Throwable
    {
        // a function such as intasblob(<col>) is represented in ResultSet.Metadata
        // by a ColumnSpecification with the function name plus args and the type set
        // to the function's return type
        ColumnSpecification fnSpec = columnSpecification("system.intasblob(v1)", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec, columnDefinition("v1"));

        assertEquals(expected, extractColumnMappingFromSelect("SELECT intasblob(v1) FROM %s"));
    }

    private void testNoArgFunction() throws Throwable
    {
        // a no-arg function such as now() is represented in ResultSet.Metadata
        // but has no mapping to any underlying column
        ColumnSpecification fnSpec = columnSpecification("system.now()", TimeUUIDType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping().addMapping(fnSpec, null);

        SelectionColumns actual = extractColumnMappingFromSelect("SELECT now() FROM %s");
        assertEquals(expected, actual);
        assertEquals(Collections.singletonList(fnSpec), actual.getColumnSpecifications());
        assertTrue(actual.getMappings().isEmpty());
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
                                                                .addMapping(fnSpec3, columnDefinition("v1"))
                                                                .addMapping(fnSpec3, columnDefinition("v2"));

        String select = String.format("SELECT %1$s(v1), %1$s(v2), %1$s(v1, v2) FROM %%s", fnName);
        SelectionColumns actual = extractColumnMappingFromSelect(select);

        assertEquals(expected, actual);
        assertEquals(ImmutableList.of(fnSpec1, fnSpec2, fnSpec3), actual.getColumnSpecifications());
    }

    private void testCount() throws Throwable
    {
        // SELECT COUNT does not necessarily include any mappings, but it must always return
        // a singleton list from getColumnSpecifications() in order for the ResultSet.Metadata
        // to be constructed correctly:
        // * COUNT(*) / COUNT(1) do not generate any mappings, as no specific columns are referenced
        // * COUNT(foo) does generate a mapping from the 'system.count' column spec to foo
        ColumnSpecification count = columnSpecification("count", LongType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(count, null);

        SelectionColumns actual = extractColumnMappingFromSelect("SELECT COUNT(*) FROM %s");
        assertEquals(expected, actual);
        assertEquals(Collections.singletonList(count), actual.getColumnSpecifications());
        assertTrue(actual.getMappings().isEmpty());

        actual = extractColumnMappingFromSelect("SELECT COUNT(1) FROM %s");
        assertEquals(expected, actual);
        assertEquals(Collections.singletonList(count), actual.getColumnSpecifications());
        assertTrue(actual.getMappings().isEmpty());

        ColumnSpecification countV1 = columnSpecification("system.count(v1)", LongType.instance);
        expected = SelectionColumnMapping.newMapping()
                                         .addMapping(countV1, columnDefinition("v1"));
        actual = extractColumnMappingFromSelect("SELECT COUNT(v1) FROM %s");
        assertEquals(expected, actual);
        assertEquals(Collections.singletonList(countV1), actual.getColumnSpecifications());
        assertFalse(actual.getMappings().isEmpty());
    }

    private void testUserDefinedFunction() throws Throwable
    {
        // UDFs are basically represented in the same way as system functions
        String functionCall = String.format("%s(v1, v2)", functionName);
        ColumnSpecification fnSpec = columnSpecification(functionCall, Int32Type.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec, columnDefinition("v1"))
                                                                .addMapping(fnSpec, columnDefinition("v2"));

        assertEquals(expected, extractColumnMappingFromSelect("SELECT " + functionCall + " FROM %s"));
    }

    private void testFunctionWithAlias() throws Throwable
    {
        // a function with an alias is represented in ResultSet.Metadata by a
        // ColumnSpecification with the alias and the type set to the function's
        // return type
        ColumnSpecification fnSpec = columnSpecification("fn_alias", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec, columnDefinition("v1"));

        assertEquals(expected, extractColumnMappingFromSelect("SELECT intasblob(v1) AS fn_alias FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT v1 AS alias_1, v1 AS alias_2 FROM %s"));
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

        assertEquals(expected, extractColumnMappingFromSelect("SELECT k AS k_alias," +
                                                              "       writetime(v1)," +
                                                              "       ttl(v2) as ttl_alias," +
                                                              "       v3.f1," +
                                                              "       v3.f2 AS f2_alias," +
                                                              "       v3" +
                                                              " FROM %s"));
    }

    private SelectionColumns extractColumnMappingFromSelect(String query) throws RequestValidationException
    {
        CQLStatement statement = QueryProcessor.getStatement(String.format(query, KEYSPACE + "." + tableName),
                                                             ClientState.forInternalCalls()).statement;
        assertTrue(statement instanceof SelectStatement);
        return ((SelectStatement)statement).getSelection().getColumnMapping();
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
