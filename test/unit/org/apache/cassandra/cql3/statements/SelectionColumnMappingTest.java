package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SelectionColumnMappingTest extends CQLTester
{
    String tableName;
    String typeName;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setPartitioner(new ByteOrderedPartitioner());
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
        testSimpleTypes();
        testWildcard();
        testSimpleTypesWithAliases();
        testUserTypes();
        testUserTypesWithAliases();
        testWritetimeAndTTL();
        testWritetimeAndTTLWithAliases();
        testFunction();
        testFunctionWithAlias();
        testMultipleAliasesOnSameColumn();
        testMixedColumnTypes();
    }

    @Test
    public void testMultipleArgumentFunction() throws Throwable
    {
        // token() is currently the only function which accepts multiple arguments
        tableName = createTable("CREATE TABLE %s (a int, b text, PRIMARY KEY ((a, b)))");
        ColumnSpecification tokenSpec = columnSpecification("token(a, b)", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(tokenSpec, columnDefinitions("a", "b"));

        assertEquals(expected, extractColumnMappingFromSelect("SELECT token(a,b) FROM %s"));
    }

    private void testSimpleTypes() throws Throwable
    {
        // simple column identifiers without aliases are represented in
        // ResultSet.Metadata by the underlying ColumnDefinition
        ColumnDefinition kDef = columnDefinition("k");
        ColumnDefinition v1Def = columnDefinition("v1");
        ColumnDefinition v2Def = columnDefinition("v2");
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(kDef, columnDefinition("k"))
                                                                .addMapping(v1Def, columnDefinition("v1"))
                                                                .addMapping(v2Def, columnDefinition("v2"));

        assertEquals(expected, extractColumnMappingFromSelect("SELECT k, v1, v2 FROM %s"));
    }

    private void testWildcard() throws Throwable
    {
        // Wildcard select should behave just as though we had
        // explicitly selected each column
        ColumnDefinition kDef = columnDefinition("k");
        ColumnDefinition v1Def = columnDefinition("v1");
        ColumnDefinition v2Def = columnDefinition("v2");
        ColumnDefinition v3Def = columnDefinition("v3");
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(kDef, columnDefinition("k"))
                                                                .addMapping(v1Def, columnDefinition("v1"))
                                                                .addMapping(v2Def, columnDefinition("v2"))
                                                                .addMapping(v3Def, columnDefinition("v3"));

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
        ColumnSpecification fnSpec = columnSpecification("intasblob(v1)", BytesType.instance);
        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(fnSpec, columnDefinition("v1"));

        assertEquals(expected, extractColumnMappingFromSelect("SELECT intasblob(v1) FROM %s"));
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

        SelectionColumnMapping expected = SelectionColumnMapping.newMapping()
                                                                .addMapping(kSpec, columnDefinition("k"))
                                                                .addMapping(v1Spec, columnDefinition("v1"))
                                                                .addMapping(v2Spec, columnDefinition("v2"))
                                                                .addMapping(f1Spec, columnDefinition("v3"))
                                                                .addMapping(f2Spec, columnDefinition("v3"))
                                                                .addMapping(columnDefinition("v3"), columnDefinition(
                                                                                                                    "v3"));

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

    private Iterable<ColumnDefinition> columnDefinitions(String...name)
    {
        List<ColumnDefinition> list = new ArrayList<>();
        for (String n : name)
            list.add(columnDefinition(n));
        return list;
    }

    private ColumnSpecification columnSpecification(String name, AbstractType<?> type)
    {
        return new ColumnSpecification(KEYSPACE,
                                       tableName,
                                       new ColumnIdentifier(name, true),
                                       type);
    }
}
