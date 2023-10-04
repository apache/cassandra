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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.restrictions.IndexRestrictions;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class StorageAttachedIndexDDLTest extends SAITester
{
    private static final Injections.Counter saiCreationCounter = Injections.newCounter("IndexCreationCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("register"))
                                                                           .build();

    private static final Injection failSAIInitialializaion = Injections.newCustom("fail_sai_initialization")
                                                                       .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                                                       .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                                                       .build();

    private static final Injection forceFlushPause = Injections.newPause("force_flush_pause", 30_000)
                                                               .add(InvokePointBuilder.newInvokePoint().onClass(ColumnFamilyStore.class).onMethod("forceBlockingFlush"))
                                                               .build();

    private static final Injection failPerIndexMetaCompletion = Injections.newCustom("fail_index_meta_completion")
                                                                          .add(InvokePointBuilder.newInvokePoint().onClass(SegmentBuilder.class).onMethod("flush"))
                                                                          .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                                                          .build();

    private static final Injection failPerSSTableTokenAdd = Injections.newCustom("fail_token_writer")
                                                                      .add(InvokePointBuilder.newInvokePoint().onClass(NumericValuesWriter.class).onMethod("add"))
                                                                      .add(ActionBuilder.newActionBuilder().actions().doThrow(IOException.class, Expression.quote("Injected failure!")))
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

        Injections.inject(saiCreationCounter, indexBuildCounter, FAIL_INDEX_GC_TRANSACTION);

        saiCreationCounter.reset();
        indexBuildCounter.reset();
    }

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
    }

    @Test
    public void shouldFailUnsupportedType()
    {
        for (CQL3Type.Native cql3Type : CQL3Type.Native.values())
        {
            if (cql3Type == CQL3Type.Native.EMPTY)
                continue;

            String createTableTemplate = "CREATE TABLE %%s (id text PRIMARY KEY, %s %s)";
            createTable(String.format(createTableTemplate, cql3Type, cql3Type));

            boolean supported = StorageAttachedIndex.SUPPORTED_TYPES.contains(cql3Type);

            try
            {
                executeNet(String.format("CREATE INDEX ON %%s(%s) USING 'sai'", cql3Type));
                assertTrue("Index creation on unsupported type " + cql3Type + " should have failed.", supported);
            }
            catch (RuntimeException e)
            {
                assertFalse("Index creation on supported type " + cql3Type + " should have succeeded.", supported);
                // InvalidConfigurationInQueryException is subclass of InvalidQueryException
                assertTrue(Throwables.isCausedBy(e, InvalidQueryException.class::isInstance));
            }
        }
    }

    @Test
    public void shouldFailCreationOnPartitionKey()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(id) USING 'sai'"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(String.format(CreateIndexStatement.ONLY_PARTITION_KEY, "id"));
    }

    @Test
    public void shouldFailCreationUsingMode()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(val) USING 'sai' " +
                                            "WITH OPTIONS = { 'mode' : 'CONTAINS' }")).isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void shouldFailCreateSpecifyingAnalyzerClass()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(val) " +
                                            "USING 'sai' " +
                                            "WITH OPTIONS = { 'analyzer_class' : 'org.apache.cassandra.index.sai.analyzer.NonTokenizingAnalyzer' }"))
        .isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void shouldFailCreateWithMisspelledOption()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(val) " +
                                            "USING 'sai' " +
                                            "WITH OPTIONS = { 'case-sensitive' : true }")).isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void shouldFailCaseSensitiveWithNonText()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");

        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(val) " +
                                            "USING 'sai' " +
                                            "WITH OPTIONS = { 'case_sensitive' : true }")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailOnNormalizeWithNonText()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");

        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(val) " +
                                            "USING 'sai' " +
                                            "WITH OPTIONS = { 'normalize' : true }")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailCreateWithUserType()
    {
        String typeName = createType("CREATE TYPE %s (a text, b int, c double)");
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val " + typeName + ')');

        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(val) " +
                                            "USING 'sai'")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldNotFailCreateWithTupleType()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val tuple<text, int, double>)");

        executeNet("CREATE INDEX ON %s(val) USING 'sai'");

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

        assertThatThrownBy(() -> executeNet(String.format("CREATE INDEX ON %%s(\"%s\")" +
                                                          " USING 'sai'", invalidColumn)))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage(String.format(CreateIndexStatement.INVALID_CUSTOM_INDEX_TARGET, invalidColumn, SchemaConstants.NAME_LENGTH));
    }

    @Test
    public void shouldCreateIndexIfExists()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        createIndex("CREATE INDEX IF NOT EXISTS ON %s(val) USING 'sai' ");
        createIndexAsync("CREATE INDEX IF NOT EXISTS ON %s(val) USING 'sai' ");

        assertEquals(1, saiCreationCounter.get());
    }

    @Test
    public void shouldCreateIndexCaseInsensitive()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val1 text, val2 text)");
        createIndex("CREATE INDEX mixed_case_val ON %s(val1) USING 'Sai' ");
        createIndex("CREATE INDEX upper_case_val ON %s(val2) USING 'SAI' ");

        assertEquals(2, saiCreationCounter.get());
    }

    @Test
    public void shouldCreateIndexWithClassName()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        createIndex("CREATE INDEX ON %s(val) USING 'StorageAttachedIndex' ");
        assertEquals(1, saiCreationCounter.get());
    }

    @Test
    public void shouldCreateIndexWithDefault()
    {
        DatabaseDescriptor.setDefaultSecondaryIndex(StorageAttachedIndex.NAME);
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        createIndex("CREATE INDEX ON %s(val)");
        assertEquals(1, saiCreationCounter.get());
    }

    @Test
    public void shouldFailWithDefaultIndexDisabled()
    {
        DatabaseDescriptor.setDefaultSecondaryIndex(StorageAttachedIndex.NAME);
        boolean original = DatabaseDescriptor.getDefaultSecondaryIndexEnabled();

        try
        {
            DatabaseDescriptor.setDefaultSecondaryIndexEnabled(false);
            createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
            assertThatThrownBy(() -> createIndex("CREATE INDEX ON %s(val)")).hasRootCauseInstanceOf(InvalidRequestException.class)
                                                                            .hasRootCauseMessage(CreateIndexStatement.MUST_SPECIFY_INDEX_IMPLEMENTATION);
            assertEquals(0, saiCreationCounter.get());
        }
        finally
        {
            DatabaseDescriptor.setDefaultSecondaryIndexEnabled(original);
        }
    }

    @Test
    public void shouldBeCaseSensitiveByDefault()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Camel'").size());

        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'camel'").size());
    }

    @Test
    public void shouldEnableCaseSensitiveSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'case_sensitive' : true }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Camel'").size());

        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'camel'").size());
    }

    @Test
    public void shouldEnableCaseInsensitiveSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'camel'").size());
    }

    @Test
    public void shouldBeNonNormalizedByDefault()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Cam\u00E1l'").size());

        // Both \u00E1 and \u0061\u0301 are visible as the character á, but without NFC normalization, they won't match.
        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'Cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableNonNormalizedSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'normalize' : false }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Cam\u00E1l'").size());

        // Both \u00E1 and \u0061\u0301 are visible as the character á, but without NFC normalization, they won't match.
        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'Cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableNormalizedSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'normalize' : true }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableNormalizedCaseInsensitiveSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'normalize' : true, 'case_sensitive' : false}");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableAsciiSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'ascii' : true, 'case_sensitive' : false}");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Éppinger')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'eppinger'").size());
    }

    @Test
    public void shouldRejectAnalysisOnPrimaryKeyColumns()
    {
        createTable("CREATE TABLE %s (k1 text, k2 text, c1 text, c2 text, PRIMARY KEY((k1, k2), c1, c2))");

        for (String column : Arrays.asList("k1", "k2", "c1", "c2"))
        {
            for (String enabled : Arrays.asList("true", "false"))
            {
                assertRejectsAnalysisOnPrimaryKeyColumns(column, ImmutableMap.of(NonTokenizingOptions.NORMALIZE, enabled));
                assertRejectsAnalysisOnPrimaryKeyColumns(column, ImmutableMap.of(NonTokenizingOptions.CASE_SENSITIVE, enabled));
                assertRejectsAnalysisOnPrimaryKeyColumns(column, ImmutableMap.of(NonTokenizingOptions.ASCII, enabled));
                assertRejectsAnalysisOnPrimaryKeyColumns(column, ImmutableMap.of(NonTokenizingOptions.NORMALIZE, enabled,
                                                                                 NonTokenizingOptions.CASE_SENSITIVE, enabled,
                                                                                 NonTokenizingOptions.ASCII, enabled));
            }
        }
    }

    private void assertRejectsAnalysisOnPrimaryKeyColumns(String column, Map<String, String> optionsMap)
    {
        String options = new CqlBuilder().append(optionsMap).toString();
        Assertions.assertThatThrownBy(() -> createIndex("CREATE INDEX ON %s(" + column + ") USING 'sai' WITH OPTIONS = " + options))
                  .hasRootCauseInstanceOf(InvalidRequestException.class)
                  .hasRootCauseMessage(StorageAttachedIndex.ANALYSIS_ON_KEY_COLUMNS_MESSAGE + options);
    }

    @Test
    public void shouldCreateIndexOnReversedType()
    {
        createTable("CREATE TABLE %s (id text, ck1 text, val text, PRIMARY KEY (id,ck1)) WITH CLUSTERING ORDER BY (ck1 desc)");

        String indexNameCk1 = createIndex("CREATE INDEX ON %s(ck1) USING 'sai'");

        execute("insert into %s(id, ck1, val) values('1', '2', '3')");
        execute("insert into %s(id, ck1, val) values('1', '3', '4')");
        assertEquals(1, executeNet("SELECT * FROM %s WHERE ck1='3'").all().size());

        flush();
        assertEquals(1, executeNet("SELECT * FROM %s WHERE ck1='2'").all().size());

        SecondaryIndexManager sim = getCurrentColumnFamilyStore().indexManager;
        StorageAttachedIndex index = (StorageAttachedIndex) sim.getIndexByName(indexNameCk1);
        IndexContext context = index.getIndexContext();
        assertTrue(context.isLiteral());
        assertTrue(context.getValidator() instanceof ReversedType);
    }

    @Test
    public void shouldCreateIndexWithFullClassName()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        assertEquals(1, saiCreationCounter.get());
    }

    /**
     * Verify SASI can be created and queries with SAI dependencies.
     * Not putting in {@link MixedIndexImplementationsTest} because it uses CQLTester which doesn't load SAI dependency.
     */
    @Test
    public void shouldCreateSASI()
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

        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(val1, val2) USING 'sai'"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("storage-attached index cannot be created over multiple columns");
    }

    @Test
    public void shouldFailCreationMultipleIndexesOnSimpleColumn()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v1 TEXT)");
        execute("INSERT INTO %s (id, v1) VALUES(1, '1')");
        flush();

        executeNet("CREATE INDEX index_1 ON %s(v1) USING 'sai'");
        waitForTableIndexesQueryable();

        // same name
        assertThatThrownBy(() -> executeNet("CREATE INDEX index_1 ON %s(v1) USING 'sai'"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(String.format(CreateIndexStatement.INDEX_ALREADY_EXISTS, "index_1"));

        // different name, same option
        assertThatThrownBy(() -> executeNet("CREATE INDEX index_2 ON %s(v1) USING 'sai'"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(String.format(CreateIndexStatement.INDEX_DUPLICATE_OF_EXISTING, "index_2", "index_1"));

        // different name, different option, same target.
        assertThatThrownBy(() -> executeNet("CREATE INDEX ON %s(v1) USING 'sai' WITH OPTIONS = { 'case_sensitive' : true }"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("Cannot create more than one storage-attached index on the same column: v1" );

        ResultSet rows = executeNet("SELECT id FROM %s WHERE v1 = '1'");
        assertEquals(1, rows.all().size());
    }

    @Test
    public void shouldIndexBuildingWithInMemoryData()
    {
        createTable(CREATE_TABLE_TEMPLATE);

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('" + i + "', " + i + ", '0')");

        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForTableIndexesQueryable();

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(rowCount, rows.all().size());
    }

    @Test
    public void shouldIndexExistingMemtableOnCreationWithConcurrentFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        Injections.Barrier delayInitializationTask =
        Injections.newBarrier("delayInitializationTask", 2, false)
                  .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild"))
                  .build();

        // Create the index, but do not allow the initial index build to begin:
        Injections.inject(delayInitializationTask);
        createIndexAsync("CREATE INDEX ON %s(val) USING 'sai'");

        // Flush the Memtable's contents, which will feed data to the index as the SSTable is written:
        flush();

        // Allow the initialization task, which builds the index, to continue:
        delayInitializationTask.countDown();

        waitForTableIndexesQueryable();

        ResultSet rows = executeNet("SELECT id FROM %s WHERE val = 'Camel'");
        assertEquals(1, rows.all().size());

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void shouldRejectQueriesBeforeIndexInitializationFinished() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('" + i + "', " + i + ", '0')");

        Injections.inject(forceFlushPause);
        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1>=0")).isInstanceOf(ReadFailureException.class);
    }

    @Test
    public void shouldRejectQueriesOnIndexInitializationFailure() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('" + i + "', " + i + ", '0')");
        flush();

        Injections.inject(failSAIInitialializaion);
        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForAssert(() -> assertEquals(1, indexBuildCounter.get()));
        waitForCompactions();

        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1>=0")).isInstanceOf(ReadFailureException.class);
    }

    @Test
    public void shouldReleaseIndexFilesAfterDroppingLastIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        String numericIndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String literalIndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        IndexContext numericIndexContext = createIndexContext(numericIndexName, Int32Type.instance);
        IndexContext literalIndexContext = createIndexContext(literalIndexName, UTF8Type.instance);
        verifyIndexFiles(numericIndexContext, literalIndexContext, 0, 0);

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        flush();
        verifyIndexFiles(numericIndexContext, literalIndexContext, 1, 1);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(1, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(1, rows.all().size());

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0')");
        flush();
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 2);
        verifySSTableIndexes(numericIndexName, 2, 2);
        verifySSTableIndexes(literalIndexName, 2, 2);
        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        dropIndex("DROP INDEX %s." + numericIndexName);
        verifyIndexFiles(numericIndexContext, literalIndexContext, 0, 2);
        verifySSTableIndexes(numericIndexName, 2, 0);
        verifySSTableIndexes(literalIndexName, 2, 2);
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('2', 2, '0')");
        flush();
        verifyIndexFiles(numericIndexContext, literalIndexContext, 0, 3);
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(3, rows.all().size());

        dropIndex("DROP INDEX %s." + literalIndexName);
        verifyIndexFiles(numericIndexContext, literalIndexContext, 0, 0);
        assertNull(getCurrentIndexGroup());

        assertEquals("Segment memory limiter should revert to zero on drop.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void shouldCreateIndexFilesAfterMultipleConcurrentIndexCreation()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        flush();
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();
        verifyNoIndexFiles();

        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);
        waitForTableIndexesQueryable();
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 2);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void shouldCreateIndexFilesAfterMultipleSequentialIndexCreation()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        flush();
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();
        verifyNoIndexFiles();

        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        waitForTableIndexesQueryable();
        verifyIndexFiles(numericIndexContext, null, 2, 0);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());

        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);
        waitForTableIndexesQueryable();
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 2);
        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void shouldReleaseIndexFilesAfterCompaction()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        flush();
        verifyIndexFiles(numericIndexContext, literalIndexContext, 1, 1);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(1, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(1, rows.all().size());

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 2);
        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        compact();
        waitForAssert(() -> verifyIndexFiles(numericIndexContext, literalIndexContext, 1, 1));

        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero after compaction.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void truncateWithBuiltIndexes()
    {
        verifyTruncateWithIndex(false);
    }

    @Test
    public void concurrentTruncateWithIndexBuilding()
    {
        verifyTruncateWithIndex(true);
    }

    private void verifyTruncateWithIndex(boolean concurrentTruncate)
    {
        createTable(CREATE_TABLE_TEMPLATE);

        if (!concurrentTruncate)
        {
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        }

        // create 100 rows, half in sstable and half in memtable
        int num = 100;
        for (int i = 0; i < num; i++)
        {
            if (i == num / 2)
                flush();
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('" + i + "', 0, '0');");
        }

        if (concurrentTruncate)
        {
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
            truncate(true);
            waitForTableIndexesQueryable();
        }
        else
        {
            truncate(true);
        }

        waitForAssert(this::verifyNoIndexFiles);

        // verify index-view-manager has been cleaned up
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 0);
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), 0);

        assertEquals("Segment memory limiter should revert to zero after truncate.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void verifyRebuildCorruptedFiles() throws Throwable
    {
        // prepare schema and data
        createTable(CREATE_TABLE_TEMPLATE);
        String numericIndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String stringIndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        for (CorruptionType corruptionType : CorruptionType.values())
        {
            verifyRebuildCorruptedFiles(numericIndexName, stringIndexName, corruptionType, false);
            verifyRebuildCorruptedFiles(numericIndexName, stringIndexName, corruptionType, true);
        }

        assertEquals("Segment memory limiter should revert to zero following rebuild.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    private void verifyRebuildCorruptedFiles(String numericIndexName,
                                             String stringIndexName,
                                             CorruptionType corruptionType,
                                             boolean rebuild) throws Throwable
    {
        IndexContext numericIndexContext = createIndexContext(numericIndexName, Int32Type.instance);
        IndexContext stringIndexContext = createIndexContext(stringIndexName, UTF8Type.instance);

        for (IndexComponent component : Version.LATEST.onDiskFormat().perSSTableIndexComponents(false))
            verifyRebuildIndexComponent(numericIndexContext, stringIndexContext, component, null, corruptionType, true, true, rebuild);

        for (IndexComponent component : Version.LATEST.onDiskFormat().perColumnIndexComponents(numericIndexContext))
            verifyRebuildIndexComponent(numericIndexContext, stringIndexContext, component, numericIndexContext, corruptionType, false, true, rebuild);

        for (IndexComponent component : Version.LATEST.onDiskFormat().perColumnIndexComponents(stringIndexContext))
            verifyRebuildIndexComponent(numericIndexContext, stringIndexContext, component, stringIndexContext, corruptionType, true, false, rebuild);
    }

    private void verifyRebuildIndexComponent(IndexContext numericIndexContext,
                                             IndexContext stringIndexContext,
                                             IndexComponent component,
                                             IndexContext corruptionContext,
                                             CorruptionType corruptionType,
                                             boolean failedStringIndex,
                                             boolean failedNumericIndex,
                                             boolean rebuild) throws Throwable
    {
        // The completion markers are valid if they exist on the file system, so we only need to test
        // their removal. If we are testing with encryption then we don't want to test any components
        // that are encryptable unless they have been removed because encrypted components aren't
        // checksum validated.

        if (component == IndexComponent.PARTITION_SIZES || component == IndexComponent.PARTITION_KEY_BLOCKS ||
            component == IndexComponent.PARTITION_KEY_BLOCK_OFFSETS || component == IndexComponent.CLUSTERING_KEY_BLOCKS ||
            component == IndexComponent.CLUSTERING_KEY_BLOCK_OFFSETS)
            return;

        if (((component == IndexComponent.GROUP_COMPLETION_MARKER) ||
             (component == IndexComponent.COLUMN_COMPLETION_MARKER)) &&
            (corruptionType != CorruptionType.REMOVED))
            return;

        int rowCount = 2;

        // initial verification
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1);
        verifySSTableIndexes(stringIndexContext.getIndexName(), 1);
        verifyIndexFiles(numericIndexContext, stringIndexContext, 1, 1, 1, 1, 1);
        assertTrue(verifyChecksum(numericIndexContext));
        assertTrue(verifyChecksum(numericIndexContext));

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(rowCount, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(rowCount, rows.all().size());

        // corrupt file
        if (corruptionContext != null)
            corruptIndexComponent(component, corruptionContext, corruptionType);
        else
            corruptIndexComponent(component, corruptionType);

        // If we are removing completion markers then the rest of the components should still have
        // valid checksums.
        boolean expectedNumericState = !failedNumericIndex || isBuildCompletionMarker(component);
        boolean expectedLiteralState = !failedStringIndex || isBuildCompletionMarker(component);

        assertEquals("Checksum verification for " + component + " should be " + expectedNumericState + " but was " + !expectedNumericState,
                     expectedNumericState, verifyChecksum(numericIndexContext));
        assertEquals(expectedLiteralState, verifyChecksum(stringIndexContext));

        if (rebuild)
        {
            rebuildIndexes(numericIndexContext.getIndexName(), stringIndexContext.getIndexName());
        }
        else
        {
            // Reload all SSTable indexes to manifest the corruption:
            reloadSSTableIndex();

            // Verify the index cannot be read:
            verifySSTableIndexes(numericIndexContext.getIndexName(), Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).contains(component) ? 0 : 1, failedNumericIndex ? 0 : 1);
            verifySSTableIndexes(stringIndexContext.getIndexName(), Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).contains(component) ? 0 : 1, failedStringIndex ? 0 : 1);

            try
            {
                // If the corruption is that a file is missing entirely, the index won't be marked non-queryable...
                rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
                assertEquals(failedNumericIndex ? 0 : rowCount, rows.all().size());
            }
            catch (ReadFailureException e)
            {
                // ...but most kind of corruption will result in the index being non-queryable.
            }

            try
            {
                // If the corruption is that a file is missing entirely, the index won't be marked non-queryable...
                rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
                assertEquals(failedStringIndex ? 0 : rowCount, rows.all().size());
            }
            catch (ReadFailureException e)
            {
                // ...but most kind of corruption will result in the index being non-queryable.
            }

            // Simulate the index repair that would occur on restart:
            runInitializationTask();
        }

        // verify indexes are recovered
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1);
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1);
        verifyIndexFiles(numericIndexContext, stringIndexContext, 1, 1, 1, 1, 1);

        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(rowCount, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(rowCount, rows.all().size());
    }

    @Test
    public void verifyCleanupFailedPerIndexFiles() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        flush();
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        // Inject failure
        Injections.inject(failPerIndexMetaCompletion);
        failPerIndexMetaCompletion.enable();

        try
        {
            // Create a new index, which will actuate a build compaction and fail, but leave the node running...
            IndexContext numericIndexContext = createIndexContext(createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
            // two index builders running in different compaction threads because of parallelised index initial build
            waitForAssert(() -> assertEquals(2, indexBuildCounter.get()));
            waitForCompactionsFinished();

            // Only token/offset files for the first SSTable in the compaction task should exist, while column-specific files are blown away:
            verifyIndexFiles(numericIndexContext, null, 2, 0, 0, 0, 0);

            assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
            assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
        }
        finally
        {
            failPerIndexMetaCompletion.disable();
        }
    }

    @Test
    public void verifyCleanupFailedPrimaryKeyFiles() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        flush();
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        // Inject failure
        Injections.inject(failPerSSTableTokenAdd);
        failPerSSTableTokenAdd.enable();

        try
        {
            // Create a new index, which will actuate a build compaction and fail, but leave the node running...
            createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
            // two index builders running in different compaction threads because of parallelised index initial build
            waitForAssert(() -> assertEquals(2, indexBuildCounter.get()));
            waitForAssert(() -> assertEquals(0, getCompactionTasks()));

            // SSTable-level token/offset file(s) should be removed, while column-specific files never existed:
            verifyNoIndexFiles();

            assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
            assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
        }
        finally
        {
            failPerSSTableTokenAdd.disable();
        }
    }

    @Test
    public void verifyFlushAndCompactEmptyIndex()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);

        // flush empty index
        execute("INSERT INTO %s (id1) VALUES ('0');");
        flush();

        execute("INSERT INTO %s (id1) VALUES ('1');");
        flush();

        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 0, 0, 2, 2);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(0, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(0, rows.all().size());

        // compact empty index
        compact();
        waitForAssert(() -> verifyIndexFiles(numericIndexContext, literalIndexContext, 1, 0, 0, 1, 1));

        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(0, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(0, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void verifyFlushAndCompactNonIndexableRows()
    {
        // valid row ids, but no valid indexable content
        Runnable populateData = () -> {
            try
            {
                execute("INSERT INTO %s (id1) VALUES ('0');");
                flush();

                execute("INSERT INTO %s (id1) VALUES ('1');");
                flush();
            }
            catch (Throwable e)
            {
                throw Throwables.unchecked(e);
            }
        };


        verifyFlushAndCompactEmptyIndexes(populateData);
    }

    @Test
    public void verifyFlushAndCompactTombstones()
    {
        // no valid row ids
        Runnable populateData = () -> {
            try
            {
                execute("DELETE FROM %s WHERE id1 = '0'");
                flush();

                execute("DELETE FROM %s WHERE id1 = '1'");
                flush();
            }
            catch (Throwable e)
            {
                throw Throwables.unchecked(e);
            }
        };

        verifyFlushAndCompactEmptyIndexes(populateData);
    }

    private void verifyFlushAndCompactEmptyIndexes(Runnable populateData)
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);


        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);
        waitForTableIndexesQueryable();

        populateData.run();
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 2, 0);
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), 2, 0);
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 0, 0, 2, 2);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(0, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(0, rows.all().size());

        // compact empty index
        compact();
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 1, 0);
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), 1, 0);
        waitForAssert(() -> verifyIndexFiles(numericIndexContext, literalIndexContext, 1, 0, 0, 1, 1));

        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(0, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(0, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void droppingIndexStopInitialIndexBuild() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        int num = 100;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        Injections.Barrier delayIndexBuilderCompletion = Injections.newBarrier("delayIndexBuilder", 2, false)
                                                                   .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                                                   .build();

        Injections.inject(delayIndexBuilderCompletion);
        String indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForAssert(() -> assertEquals(1, delayIndexBuilderCompletion.getCount()));

        dropIndex("DROP INDEX %s." + indexName);

        // let blocked builders to continue
        delayIndexBuilderCompletion.countDown();
        waitForCompactions();

        delayIndexBuilderCompletion.disable();

        assertNull(getCurrentIndexGroup());
        assertFalse("Expect index not built", SystemKeyspace.isIndexBuilt(KEYSPACE, indexName));

        // create index again, it should succeed
        indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForTableIndexesQueryable();
        verifySSTableIndexes(indexName, 1);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(num, rows.all().size());
    }

    @Test
    @SuppressWarnings("BusyWait")
    public void nodetoolStopInitialIndexBuild() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        // create 100 rows into 1 sstable
        int num = 100;
        int sstable = 1;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('" + i + "', 0, '0');");
        }
        flush();

        Injections.Barrier delayIndexBuilderCompletion = Injections.newBarrierAwait("delayIndexBuilder", 1, true)
                                                                   .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                                                   .build();

        Injections.inject(delayIndexBuilderCompletion);

        IndexContext numericIndexContext = createIndexContext(createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);

        waitForAssert(() -> assertTrue(getCompactionTasks() > 0), 1000, TimeUnit.MILLISECONDS);

        // Stop initial index build by interrupting active and pending compactions
        int attempt = 20;
        while (getCompactionTasks() > 0 && attempt > 0)
        {
            System.out.println("Attempt " + attempt + " at stopping the compaction tasks");

            // only interrupts active compactions, not pending compactions.
            CompactionManager.instance.stopCompaction(OperationType.INDEX_BUILD.name());
            // let blocked builder to continue, but still block pending builder threads
            delayIndexBuilderCompletion.reset();

            Thread.sleep(3000);
            attempt--;
        }
        if (getCompactionTasks() > 0)
            fail("Compaction tasks are not interrupted.");

        delayIndexBuilderCompletion.disable();

        // initial index builder should have stopped abruptly resulting in the index not being queryable
        verifyInitialIndexFailed(numericIndexContext.getIndexName());
        Assertions.assertThat(getNotQueryableIndexes()).isNotEmpty();

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (Index i : cfs.indexManager.listIndexes())
        {
            StorageAttachedIndex index = (StorageAttachedIndex) i;
            assertEquals(0, index.getIndexContext().getMemtableIndexManager().size());

            View view = index.getIndexContext().getView();
            assertTrue("Expect index build stopped", view.getIndexes().isEmpty());
        }

        assertEquals("Segment memory limiter should revert to zero on interrupted compactions.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());

        // rebuild index
        ColumnFamilyStore.rebuildSecondaryIndex(KEYSPACE, currentTable(), numericIndexContext.getIndexName());

        verifyIndexFiles(numericIndexContext, null, sstable, 0);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(num, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero following rebuild.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());

        assertTrue(verifyChecksum(numericIndexContext));
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

    @Test
    public void testInitialBuildParallelism()
    {
        Function<Long, SSTableReader> createMockSSTable = onDiskLength -> {
            SSTableReader reader = Mockito.mock(SSTableReader.class);
            when(reader.onDiskLength()).thenReturn(onDiskLength);
            return reader;
        };

        Function<List<SSTableReader>, List<Long>> toSize = sstables -> sstables.stream().map(SSTableReader::onDiskLength).collect(Collectors.toList());

        // total size = 55
        List<SSTableReader> sstables = LongStream.range(1, 11).boxed().map(createMockSSTable).collect(Collectors.toList());

        // avg = 55 == total size
        List<List<SSTableReader>> groups = StorageAttachedIndex.groupBySize(sstables, 1);
        Iterator<List<SSTableReader>> iterator = groups.iterator();
        assertEquals(1, groups.size());
        assertEquals(Arrays.asList(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L), toSize.apply(iterator.next())); // size = 55

        // avg = 27.5
        groups = StorageAttachedIndex.groupBySize(sstables, 2);
        iterator = groups.iterator();
        assertEquals(2, groups.size());
        assertEquals(Arrays.asList(10L, 9L, 8L, 7L), toSize.apply(iterator.next())); // size = 34
        assertEquals(Arrays.asList(6L, 5L, 4L, 3L, 2L, 1L), toSize.apply(iterator.next())); // size = 21

        // avg = 18.333
        groups = StorageAttachedIndex.groupBySize(sstables, 3);
        iterator = groups.iterator();
        assertEquals(3, groups.size());
        assertEquals(Arrays.asList(10L, 9L), toSize.apply(iterator.next())); // size = 19
        assertEquals(Arrays.asList(8L, 7L, 6L), toSize.apply(iterator.next())); // size = 21
        assertEquals(Arrays.asList(5L, 4L, 3L, 2L, 1L), toSize.apply(iterator.next())); // size = 15

        // avg = 11
        groups = StorageAttachedIndex.groupBySize(sstables, 5);
        iterator = groups.iterator();
        assertEquals(4, groups.size());
        assertEquals(Arrays.asList(10L, 9L), toSize.apply(iterator.next())); // size = 19
        assertEquals(Arrays.asList(8L, 7L), toSize.apply(iterator.next())); // size = 15
        assertEquals(Arrays.asList(6L, 5L), toSize.apply(iterator.next())); // size = 11
        assertEquals(Arrays.asList(4L, 3L, 2L, 1L), toSize.apply(iterator.next())); // size = 11

        // avg = 5.5
        groups = StorageAttachedIndex.groupBySize(sstables, 10);
        iterator = groups.iterator();
        assertEquals(7, groups.size());
        assertEquals(singletonList(10L), toSize.apply(iterator.next()));
        assertEquals(singletonList(9L), toSize.apply(iterator.next()));
        assertEquals(singletonList(8L), toSize.apply(iterator.next()));
        assertEquals(singletonList(7L), toSize.apply(iterator.next()));
        assertEquals(singletonList(6L), toSize.apply(iterator.next()));
        assertEquals(Arrays.asList(5L, 4L), toSize.apply(iterator.next()));
        assertEquals(Arrays.asList(3L, 2L, 1L), toSize.apply(iterator.next()));

        // avg = 2.75
        groups = StorageAttachedIndex.groupBySize(sstables, 20);
        iterator = groups.iterator();
        assertEquals(9, groups.size());
        assertEquals(singletonList(10L), toSize.apply(iterator.next()));
        assertEquals(singletonList(9L), toSize.apply(iterator.next()));
        assertEquals(singletonList(8L), toSize.apply(iterator.next()));
        assertEquals(singletonList(7L), toSize.apply(iterator.next()));
        assertEquals(singletonList(6L), toSize.apply(iterator.next()));
        assertEquals(singletonList(5L), toSize.apply(iterator.next()));
        assertEquals(singletonList(4L), toSize.apply(iterator.next()));
        assertEquals(singletonList(3L), toSize.apply(iterator.next()));
        assertEquals(Arrays.asList(2L, 1L), toSize.apply(iterator.next()));
    }

    private void assertZeroSegmentBuilderUsage()
    {
        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0, getColumnIndexBuildsInProgress());
    }
}
