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
package org.apache.cassandra.index.sai.cql;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.restrictions.IndexRestrictions;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.schema.CreateIndexStatement.INVALID_CUSTOM_INDEX_TARGET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StorageAttachedIndexDDLTest extends SAITester
{
    private static final Injections.Counter saiCreationCounter = Injections.newCounter("IndexCreationCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("register"))
                                                                           .build();

    private static final Injection FAIL_INDEX_GC_TRANSACTION = Injections.newCustom("fail_index_gc_transaction")
                                                                         .add(InvokePointBuilder.newInvokePoint().onClass("org.apache.cassandra.index.SecondaryIndexManager$IndexGCTransaction")
                                                                                                .onMethod("<init>"))
                                                                         .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                                                         .build();

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();

        Injections.inject(saiCreationCounter, FAIL_INDEX_GC_TRANSACTION);

        saiCreationCounter.reset();
    }

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
    }

    @Test
    public void shouldFailUnsupportedType() throws Throwable
    {
        for (CQL3Type.Native cql3Type : CQL3Type.Native.values())
        {
            if (cql3Type == CQL3Type.Native.EMPTY)
                continue;

            String createTableTemplate = "CREATE TABLE %%s (id text PRIMARY KEY, %s %s)";
            createTable(String.format(createTableTemplate, cql3Type, cql3Type));

            if (StorageAttachedIndex.SUPPORTED_TYPES.contains(cql3Type))
            {
                try
                {
                    executeNet(String.format("CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'", cql3Type));
                }
                catch (RuntimeException e)
                {
                    fail("Index creation on supported type " + cql3Type + " should have succeeded.");
                }
            }
            else
                assertThatThrownBy(() -> executeNet(String.format("CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'", cql3Type)))
                        .isInstanceOf(InvalidQueryException.class);
        }
    }

    @Test
    public void shouldFailCreationOnPartitionKey()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(id) USING 'StorageAttachedIndex'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Cannot create secondary index on the only partition key column id");
    }

    @Test
    public void shouldFailCreationUsingMode()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING " +
                                            "'StorageAttachedIndex' WITH OPTIONS = { 'mode' : 'CONTAINS' }")).isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void shouldFailCreateWithUserType()
    {
        String typeName = createType("CREATE TYPE %s (a text, b int, c double)");
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val " + typeName + ")");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) " +
                                            "USING 'StorageAttachedIndex'")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldNotFailCreateWithTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val tuple<text, int, double>)");

        executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        TableMetadata metadata = currentTableMetadata();
        AbstractType<?> tuple = metadata.getColumn(ColumnIdentifier.getInterned("val", false)).type;
        assertFalse(tuple.isMultiCell());
        assertFalse(tuple.isCollection());
        assertTrue(tuple.isTuple());
    }

    @Test
    public void shouldFailCreateWithInvalidCharactersInColumnName()
    {
        String invalidColumn = "/invalid";
        createTable(String.format("CREATE TABLE %%s (id text PRIMARY KEY, \"%s\" text)", invalidColumn));

        assertThatThrownBy(() -> executeNet(String.format("CREATE CUSTOM INDEX ON %%s(\"%s\")" +
                                                          " USING 'StorageAttachedIndex'", invalidColumn)))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessage(String.format(INVALID_CUSTOM_INDEX_TARGET, invalidColumn, SchemaConstants.NAME_LENGTH));
    }

    @Test
    public void shouldCreateIndexIfExists()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(val) USING 'StorageAttachedIndex' ");

        createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(val) USING 'StorageAttachedIndex' ");

        assertEquals(1, saiCreationCounter.get());
    }

    @Test
    public void shouldBeCaseSensitiveByDefault() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Camel'").size());

        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'camel'").size());
    }

    @Test
    public void shouldBeNonNormalizedByDefault() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Cam\u00E1l'").size());

        // Both \u00E1 and \u0061\u0301 are visible as the character á, but without NFC normalization, they won't match.
        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'Cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldCreateIndexOnReversedType() throws Throwable
    {
        createTable("CREATE TABLE %s (id text, ck1 text, ck2 int, val text, PRIMARY KEY (id,ck1,ck2)) WITH CLUSTERING ORDER BY (ck1 desc, ck2 desc)");

        String indexNameCk1 = createIndex("CREATE CUSTOM INDEX ON %s(ck1) USING 'StorageAttachedIndex'");
        String indexNameCk2 = createIndex("CREATE CUSTOM INDEX ON %s(ck2) USING 'StorageAttachedIndex'");

        execute("insert into %s(id, ck1, ck2, val) values('1', '2', 3, '3')");
        execute("insert into %s(id, ck1, ck2, val) values('1', '3', 4, '4')");
        assertEquals(1, executeNet("SELECT * FROM %s WHERE ck1='3'").all().size());
        assertEquals(2, executeNet("SELECT * FROM %s WHERE ck2>=0").all().size());
        assertEquals(2, executeNet("SELECT * FROM %s WHERE ck2<=4").all().size());

        SecondaryIndexManager sim = getCurrentColumnFamilyStore().indexManager;
        StorageAttachedIndex index = (StorageAttachedIndex) sim.getIndexByName(indexNameCk1);
        IndexContext context = index.getIndexContext();
        assertTrue(context.isLiteral());
        assertTrue(context.getValidator() instanceof ReversedType);

        index = (StorageAttachedIndex) sim.getIndexByName(indexNameCk2);
        context = index.getIndexContext();
        assertFalse(context.isLiteral());
        assertTrue(context.getValidator() instanceof ReversedType);
    }

    @Test
    public void shouldCreateIndexWithAlias()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertEquals(1, saiCreationCounter.get());
    }

    /**
     * Verify SASI can be created and queries with SAI dependencies.
     * Not putting in {@link MixedIndexImplementationsTest} because it uses CQLTester which doesn't load SAI dependency.
     */
    @Test
    public void shouldCreateSASI() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v2) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS',\n" +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',\n" +
                    "'tokenization_enable_stemming': 'true',\n" +
                    "'tokenization_locale': 'en',\n" +
                    "'tokenization_skip_stop_words': 'true',\n" +
                    "'analyzed': 'true',\n" +
                    "'tokenization_normalize_lowercase': 'true'};");

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(1, rows.all().size());

        rows = executeNet("SELECT id1 FROM %s WHERE v2 like '0'");
        assertEquals(1, rows.all().size());
    }

    @Test
    public void shouldFailCreationOnMultipleColumns()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val1 text, val2 text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val1, val2) USING 'StorageAttachedIndex'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("storage-attached index cannot be created over multiple columns");
    }

    @Test
    public void shouldFailCreationMultipleIndexesOnSimpleColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v1 TEXT)");

        executeNet("CREATE CUSTOM INDEX index_1 ON %s(v1) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // same name
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX index_1 ON %s(v1) USING 'StorageAttachedIndex'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Index 'index_1' already exists");

        // different name, same option
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX index_2 ON %s(v1) USING 'StorageAttachedIndex'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Index index_2 is a duplicate of existing index index_1");

        execute("INSERT INTO %s (id, v1) VALUES(1, '1')");

        ResultSet rows = executeNet("SELECT id FROM %s WHERE v1 = '1'");
        assertEquals(1, rows.all().size());
    }

    @Test
    public void shouldRejectQueriesWithCustomExpressions()
    {
        createTable(CREATE_TABLE_TEMPLATE);

        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        assertThatThrownBy(() -> executeNet(String.format("SELECT * FROM %%s WHERE expr(%s, 0)", index)))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessage(String.format(IndexRestrictions.CUSTOM_EXPRESSION_NOT_SUPPORTED, index));
    }
}
