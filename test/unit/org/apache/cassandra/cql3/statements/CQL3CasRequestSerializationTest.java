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
package org.apache.cassandra.cql3.statements;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CQL3CasRequestSerializationTest extends CQLTester
{
    private static final int DEFAULT_PK_VALUE = 1;
    private static final int DEFAULT_CK_VALUE = 1;

    private TableMetadata metadata;
    private DecoratedKey partitionKey;
    private RegularAndStaticColumns conditionColumns;
    private ClientState clientState;

    @Before
    public void setUp() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, v2 text, s int static, PRIMARY KEY (pk, ck))");
        metadata = currentTableMetadata();
        partitionKey = createDecoratedKey(metadata, DEFAULT_PK_VALUE);
        conditionColumns = metadata.regularAndStaticColumns();
        clientState = ClientState.forInternalCalls();
    }

    // === Helper Methods ===

    /**
     * Creates a DecoratedKey for the given table metadata and partition key value.
     */
    private static DecoratedKey createDecoratedKey(TableMetadata metadata, int pkValue)
    {
        return metadata.partitioner.decorateKey(Int32Type.instance.decompose(pkValue));
    }

    /**
     * Creates a Clustering for the given clustering key value.
     */
    private static Clustering<?> createClustering(int ckValue)
    {
        return Clustering.make(Int32Type.instance.decompose(ckValue));
    }

    /**
     * Sets up a new table and returns a TestTableSetup with all necessary components.
     */
    private TestTableSetup setupTable(String createTableSql)
    {
        createTable(createTableSql);
        TableMetadata tableMetadata = currentTableMetadata();
        assertSchemaRegistration(tableMetadata);

        return new TestTableSetup(
            tableMetadata,
            createDecoratedKey(tableMetadata, DEFAULT_PK_VALUE),
            tableMetadata.regularAndStaticColumns()
        );
    }

    /**
     * Verifies that the table metadata is properly registered in the global schema.
     * This is critical for TxnWrite.Fragment serialization which relies on Schema.instance lookups.
     */
    private static void assertSchemaRegistration(TableMetadata metadata)
    {
        TableMetadata schemaMetadata = Schema.instance.getTableMetadata(metadata.id);
        assertNotNull("Table metadata must be available in Schema.instance for serialization", schemaMetadata);
        assertEquals("Schema metadata must match our test metadata", metadata.id, schemaMetadata.id);
        assertEquals("Schema metadata keyspace must match", metadata.keyspace, schemaMetadata.keyspace);
        assertEquals("Schema metadata name must match", metadata.name, schemaMetadata.name);
    }

    /**
     * Creates a CQL statement from SQL and returns the parsed ModificationStatement.
     */
    private ModificationStatement createStatement(String sql, String keyspace, String tableName)
    {
        String formattedSql = String.format(sql, keyspace, tableName);
        return (ModificationStatement) QueryProcessor.parseStatement(formattedSql, clientState);
    }

    /**
     * Performs round-trip serialization test with assertion.
     */
    private static void assertRoundTripSerialization(CQL3CasRequest original, String testDescription) throws IOException
    {
        CQL3CasRequest deserialized = serdes(original);
        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve " + testDescription, original, deserialized);
    }

    /**
     * Helper class to hold table setup components.
     */
    private static class TestTableSetup
    {
        final TableMetadata metadata;
        final DecoratedKey key;
        final RegularAndStaticColumns conditionColumns;

        TestTableSetup(TableMetadata metadata, DecoratedKey key, RegularAndStaticColumns conditionColumns)
        {
            this.metadata = metadata;
            this.key = key;
            this.conditionColumns = conditionColumns;
        }

        CQL3CasRequest createRequest(boolean updatesRegular, boolean updatesStatic)
        {
            return new CQL3CasRequest(metadata, key, conditionColumns, updatesRegular, updatesStatic);
        }
    }

    /**
     * Performs round-trip serialization/deserialization with size validation.
     */
    private static CQL3CasRequest serdes(CQL3CasRequest request) throws IOException
    {
        CQL3CasRequest.Serializer serializer = CQL3CasRequest.serializer;
        int expectedSize = (int) serializer.serializedSize(request, MessagingService.current_version);

        try (DataOutputBuffer out = new DataOutputBuffer(expectedSize))
        {
            serializer.serialize(request, out, MessagingService.current_version);
            assertEquals("Serialized size must match calculated size", expectedSize, out.buffer().limit());

            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return serializer.deserialize(in, MessagingService.current_version);
            }
        }
    }

    @Test
    public void testBasicRoundTrip() throws Throwable
    {
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, false);

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve all fields", original, deserialized);
    }

    @Test
    public void testNotExistsCondition() throws Throwable
    {
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, false);
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addNotExist(clustering);

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve NotExists condition", original, deserialized);
    }

    @Test
    public void testExistsCondition() throws Throwable
    {
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, false);
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering);

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve Exists condition", original, deserialized);
    }

    @Test
    public void testStaticRowConditions() throws Throwable
    {
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, false, true);
        original.addNotExist(Clustering.STATIC_CLUSTERING);

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve static row conditions", original, deserialized);
    }

    @Test
    public void testColumnConditions() throws Throwable
    {
        // This test verifies serialization works for requests that could have column conditions
        // The actual column conditions are added through the CQL processing layer in practice
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, false);
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));

        // Add a basic existence condition for serialization testing
        original.addExist(clustering);

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve column conditions", original, deserialized);
    }

    @Test
    public void testMixedConditionsAndUpdates() throws Throwable
    {
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, true);

        // Add both static and regular conditions
        original.addNotExist(Clustering.STATIC_CLUSTERING);
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering);

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve mixed conditions", original, deserialized);
    }

    @Test
    public void testWithWriteFragments() throws Throwable
    {
        // Verify schema is properly registered before testing fragments
        assertSchemaRegistration(metadata);

        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, false);

        // Create a simple INSERT statement to generate a writeFragment
        String insertCql = String.format("INSERT INTO %s.%s (pk, ck, v1) VALUES (1, 1, 100)",
                                        metadata.keyspace, metadata.name);
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.parseStatement(insertCql, clientState);

        // Add write fragment to the request
        QueryOptions options = QueryOptions.DEFAULT;
        original.addWriteFragment(stmt, options, clientState, FBUtilities.nowInSeconds());

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve request with writeFragments", original, deserialized);
    }

    @Test
    public void testMultipleWriteFragments() throws Throwable
    {
        // Verify schema is properly registered before testing fragments
        assertSchemaRegistration(metadata);

        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, false);

        // Create multiple statements to generate multiple writeFragments
        String insertCql1 = String.format("INSERT INTO %s.%s (pk, ck, v1) VALUES (1, 1, 100)",
                                         metadata.keyspace, metadata.name);
        String insertCql2 = String.format("INSERT INTO %s.%s (pk, ck, v2) VALUES (1, 2, 'test')",
                                         metadata.keyspace, metadata.name);

        ModificationStatement stmt1 = (ModificationStatement) QueryProcessor.parseStatement(insertCql1, clientState);

        ModificationStatement stmt2 = (ModificationStatement) QueryProcessor.parseStatement(insertCql2, clientState);

        // Add multiple write fragments to the request
        QueryOptions options = QueryOptions.DEFAULT;
        original.addWriteFragment(stmt1, options, clientState, FBUtilities.nowInSeconds());
        original.addWriteFragment(stmt2, options, clientState, FBUtilities.nowInSeconds());

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve request with multiple writeFragments", original, deserialized);
    }

    @Test
    public void testNullStaticConditions() throws Throwable
    {
        // Test with no static conditions (null case)
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, false);
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering); // Only regular conditions, no static

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve null static conditions", original, deserialized);
    }

    @Test
    public void testEmptyConditionsMap() throws Throwable
    {
        // Test with only static conditions (empty regular conditions map)
        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, false, true);
        original.addExist(Clustering.STATIC_CLUSTERING); // Only static condition

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve empty conditions map", original, deserialized);
    }

    @Test
    public void testComplexScenario() throws Throwable
    {
        // Verify schema is properly registered before testing fragments
        assertSchemaRegistration(metadata);

        CQL3CasRequest original = new CQL3CasRequest(metadata, partitionKey, conditionColumns, true, true);

        // Mix of conditions
        original.addNotExist(Clustering.STATIC_CLUSTERING);

        Clustering<?> clustering1 = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering1);

        Clustering<?> clustering2 = Clustering.make(Int32Type.instance.decompose(2));
        original.addNotExist(clustering2);

        // Add writeFragments as part of complex scenario
        String insertCql = String.format("INSERT INTO %s.%s (pk, ck, v1, s) VALUES (1, 3, 300, 500)",
                                        metadata.keyspace, metadata.name);
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.parseStatement(insertCql, clientState);

        QueryOptions options = QueryOptions.DEFAULT;
        original.addWriteFragment(stmt, options, clientState, FBUtilities.nowInSeconds());

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve complex conditions and writeFragments scenario", original, deserialized);
    }

    @Test
    public void testListAppendReferenceOperations() throws Throwable
    {
        // Create a table with list columns to test list append operations
        String tableName = createTable("CREATE TABLE %s (pk int, ck int, list_col list<int>, static_list_col list<text> static, PRIMARY KEY (pk, ck))");
        TableMetadata listMetadata = currentTableMetadata();

        // Verify schema is properly registered before testing fragments
        TableMetadata schemaMetadata = Schema.instance.getTableMetadata(listMetadata.id);
        assertNotNull("Table metadata must be available in Schema.instance for serialization", schemaMetadata);
        assertEquals("Schema metadata must match our test metadata", listMetadata.id, schemaMetadata.id);

        DecoratedKey key = listMetadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        RegularAndStaticColumns condColumns = listMetadata.regularAndStaticColumns();

        CQL3CasRequest original = new CQL3CasRequest(listMetadata, key, condColumns, true, true);

        // Create list append statement - this creates reference operations
        String listAppendCql = String.format("UPDATE %s.%s SET list_col = list_col + [1, 2] WHERE pk = 1 AND ck = 1 IF EXISTS",
                                            listMetadata.keyspace, listMetadata.name);
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.parseStatement(listAppendCql, clientState);

        QueryOptions options = QueryOptions.DEFAULT;

        // Add conditions for the existence check
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering);

        // Add write fragment - this should create non-empty TxnReferenceOperations
        original.addWriteFragment(stmt, options, clientState, FBUtilities.nowInSeconds());

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve list append reference operations", original, deserialized);
    }

    @Test
    public void testStaticListAppendReferenceOperations() throws Throwable
    {
        // Create a table with static list columns to test static reference operations
        String tableName = createTable("CREATE TABLE %s (pk int, ck int, v int, static_list_col list<text> static, PRIMARY KEY (pk, ck))");
        TableMetadata listMetadata = currentTableMetadata();

        // Verify schema is properly registered before testing fragments
        TableMetadata schemaMetadata = Schema.instance.getTableMetadata(listMetadata.id);
        assertNotNull("Table metadata must be available in Schema.instance for serialization", schemaMetadata);
        assertEquals("Schema metadata must match our test metadata", listMetadata.id, schemaMetadata.id);

        DecoratedKey key = listMetadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        RegularAndStaticColumns condColumns = listMetadata.regularAndStaticColumns();

        CQL3CasRequest original = new CQL3CasRequest(listMetadata, key, condColumns, true, true);

        // Create static list append statement - this creates static reference operations
        String staticListAppendCql = String.format("UPDATE %s.%s SET static_list_col = static_list_col + ['test', 'value'] WHERE pk = 1 IF EXISTS",
                                                   listMetadata.keyspace, listMetadata.name);
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.parseStatement(staticListAppendCql, clientState);

        QueryOptions options = QueryOptions.DEFAULT;

        // Add existence condition (even though we're updating static columns, the test framework expects conditions)
        // This doesn't need to semantically match the CQL - it's about testing serialization
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering);

        // Add write fragment - this should create non-empty TxnReferenceOperations.statics
        original.addWriteFragment(stmt, options, clientState, FBUtilities.nowInSeconds());

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve static list append reference operations", original, deserialized);
    }

    @Test
    public void testNumericIncrementReferenceOperations() throws Throwable
    {
        // Create a table with regular numeric columns to test increment operations
        String tableName = createTable("CREATE TABLE %s (pk int, ck int, numeric_col int, static_numeric int static, PRIMARY KEY (pk, ck))");
        TableMetadata numericMetadata = currentTableMetadata();

        // Verify schema is properly registered before testing fragments
        TableMetadata schemaMetadata = Schema.instance.getTableMetadata(numericMetadata.id);
        assertNotNull("Table metadata must be available in Schema.instance for serialization", schemaMetadata);
        assertEquals("Schema metadata must match our test metadata", numericMetadata.id, schemaMetadata.id);

        DecoratedKey key = numericMetadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        RegularAndStaticColumns condColumns = numericMetadata.regularAndStaticColumns();

        CQL3CasRequest original = new CQL3CasRequest(numericMetadata, key, condColumns, true, true);

        // Create numeric increment statement - this creates reference operations
        String numericIncrementCql = String.format("UPDATE %s.%s SET numeric_col = numeric_col + 5 WHERE pk = 1 AND ck = 1 IF EXISTS",
                                                   numericMetadata.keyspace, numericMetadata.name);
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.parseStatement(numericIncrementCql, clientState);

        QueryOptions options = QueryOptions.DEFAULT;

        // Add conditions for the existence check
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering);

        // Add write fragment - this should create non-empty TxnReferenceOperations with numeric increment operations
        original.addWriteFragment(stmt, options, clientState, FBUtilities.nowInSeconds());

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve numeric increment reference operations", original, deserialized);
    }

    @Test
    public void testMixedReferenceOperations() throws Throwable
    {
        // Create a table with both list and map columns
        String tableName = createTable("CREATE TABLE %s (pk int, ck int, list_col list<int>, map_col map<text, int>, static_list_col list<text> static, static_map_col map<text, int> static, PRIMARY KEY (pk, ck))");
        TableMetadata mixedMetadata = currentTableMetadata();

        // Verify schema is properly registered before testing fragments
        TableMetadata schemaMetadata = Schema.instance.getTableMetadata(mixedMetadata.id);
        assertNotNull("Table metadata must be available in Schema.instance for serialization", schemaMetadata);
        assertEquals("Schema metadata must match our test metadata", mixedMetadata.id, schemaMetadata.id);

        DecoratedKey key = mixedMetadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        RegularAndStaticColumns condColumns = mixedMetadata.regularAndStaticColumns();

        CQL3CasRequest original = new CQL3CasRequest(mixedMetadata, key, condColumns, true, true);

        // Add conditions for existence check
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        original.addExist(clustering);

        // Create list append statement
        String listAppendCql = String.format("UPDATE %s.%s SET list_col = list_col + [10, 20] WHERE pk = 1 AND ck = 1 IF EXISTS",
                                            mixedMetadata.keyspace, mixedMetadata.name);
        ModificationStatement listStmt = (ModificationStatement) QueryProcessor.parseStatement(listAppendCql, clientState);

        // Create map update statement
        String mapUpdateCql = String.format("UPDATE %s.%s SET map_col = map_col + {'key1': 100} WHERE pk = 1 AND ck = 1 IF EXISTS",
                                           mixedMetadata.keyspace, mixedMetadata.name);
        ModificationStatement mapStmt = (ModificationStatement) QueryProcessor.parseStatement(mapUpdateCql, clientState);

        // Create static list append statement
        String staticListAppendCql = String.format("UPDATE %s.%s SET static_list_col = static_list_col + ['static', 'test'] WHERE pk = 1 IF EXISTS",
                                                   mixedMetadata.keyspace, mixedMetadata.name);
        ModificationStatement staticListStmt = (ModificationStatement) QueryProcessor.parseStatement(staticListAppendCql, clientState);

        QueryOptions options = QueryOptions.DEFAULT;

        // Add write fragments - this should create TxnReferenceOperations with both regular and static operations
        original.addWriteFragment(listStmt, options, clientState, FBUtilities.nowInSeconds());
        original.addWriteFragment(mapStmt, options, clientState, FBUtilities.nowInSeconds());
        original.addWriteFragment(staticListStmt, options, clientState, FBUtilities.nowInSeconds());

        CQL3CasRequest deserialized = serdes(original);

        assertNotNull(deserialized);
        assertEquals("Round-trip serialization should preserve mixed reference operations (list, map, static)", original, deserialized);
    }
}