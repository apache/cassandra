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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.restrictions.IndexRestrictions;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
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
    public void shouldFailUnsupportedType() throws Throwable
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
                executeNet(String.format("CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'", cql3Type));
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
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(id) USING 'StorageAttachedIndex'"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(String.format(CreateIndexStatement.ONLY_PARTITION_KEY, "id"));
    }

    @Test
    public void shouldFailCreationUsingMode()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING " +
                                            "'StorageAttachedIndex' WITH OPTIONS = { 'mode' : 'CONTAINS' }")).isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void shouldFailCreateSpecifyingAnalyzerClass()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) " +
                                            "USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = { 'analyzer_class' : 'org.apache.cassandra.index.sai.analyzer.NonTokenizingAnalyzer' }"))
        .isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void shouldFailCreateWithMisspelledOption()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) " +
                                            "USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = { 'case-sensitive' : true }")).isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void shouldFailCaseSensitiveWithNonText()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) " +
                                            "USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = { 'case_sensitive' : true }")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailOnNormalizeWithNonText()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) " +
                                            "USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = { 'normalize' : true }")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailCreateWithUserType()
    {
        String typeName = createType("CREATE TYPE %s (a text, b int, c double)");
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val " + typeName + ')');

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
        .hasMessage(String.format(CreateIndexStatement.INVALID_CUSTOM_INDEX_TARGET, invalidColumn, SchemaConstants.NAME_LENGTH));
    }

    @Test
    public void shouldCreateIndexIfExists()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(val) USING 'StorageAttachedIndex' ");

        createIndexAsync("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(val) USING 'StorageAttachedIndex' ");

        assertEquals(1, saiCreationCounter.get());
    }

    @Test
    public void shouldCreateIndexOnReversedType() throws Throwable
    {
        createTable("CREATE TABLE %s (id text, ck1 text, val text, PRIMARY KEY (id,ck1)) WITH CLUSTERING ORDER BY (ck1 desc)");

        String indexNameCk1 = createIndex("CREATE CUSTOM INDEX ON %s(ck1) USING 'StorageAttachedIndex'");

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

        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS',\n" +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',\n" +
                    "'tokenization_enable_stemming': 'true',\n" +
                    "'tokenization_locale': 'en',\n" +
                    "'tokenization_skip_stop_words': 'true',\n" +
                    "'analyzed': 'true',\n" +
                    "'tokenization_normalize_lowercase': 'true'};");

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0');");

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0' ALLOW FILTERING");
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
        execute("INSERT INTO %s (id, v1) VALUES(1, '1')");
        flush();

        executeNet("CREATE CUSTOM INDEX index_1 ON %s(v1) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        // same name
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX index_1 ON %s(v1) USING 'StorageAttachedIndex'"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(String.format(CreateIndexStatement.INDEX_ALREADY_EXISTS, "index_1"));

        // different name, same option
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX index_2 ON %s(v1) USING 'StorageAttachedIndex'"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(String.format(CreateIndexStatement.INDEX_DUPLICATE_OF_EXISTING, "index_2", "index_1"));

        // different name, different option, same target.
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : true }"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("Cannot create more than one storage-attached index on the same column: v1" );

        ResultSet rows = executeNet("SELECT id FROM %s WHERE v1 = '1'");
        assertEquals(1, rows.all().size());
    }

    @Test
    public void shouldIndexBuildingWithInMemoryData() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (id1, v1) VALUES ('" + i + "', '0')");

        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
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
        createIndexAsync("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

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
            execute("INSERT INTO %s (id1, v1) VALUES ('" + i + "', '0')");

        Injections.inject(forceFlushPause);
        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1='0'")).isInstanceOf(ReadFailureException.class);
    }

    @Test
    public void shouldRejectQueriesOnIndexInitializationFailure() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (id1, v1) VALUES ('" + i + "', '0')");
        flush();

        Injections.inject(failSAIInitialializaion);
        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForAssert(() -> assertEquals(1, indexBuildCounter.get()));
        waitForCompactions();

        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1='0'")).isInstanceOf(ReadFailureException.class);
    }

    @Test
    public void shouldReleaseIndexFilesAfterDroppingLastIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        String literalIndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        IndexContext literalIndexContext = createIndexContext(literalIndexName, UTF8Type.instance);
        verifyIndexFiles(literalIndexContext, 0);

        execute("INSERT INTO %s (id1, v1) VALUES ('0', '0')");
        flush();
        verifyIndexFiles(literalIndexContext, 1);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(1, rows.all().size());

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0')");
        flush();
        verifyIndexFiles(literalIndexContext, 2);
        verifySSTableIndexes(literalIndexName, 2, 2);
        rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(2, rows.all().size());

        dropIndex("DROP INDEX %s." + literalIndexName);
        verifyIndexFiles(literalIndexContext, 0);
        verifySSTableIndexes(literalIndexName, 0);

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void shouldCreateIndexFilesAfterMultipleConcurrentIndexCreation() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1) VALUES ('0', '0');");
        flush();
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0');");
        flush();
        verifyNoIndexFiles();

        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), UTF8Type.instance);
        verifyIndexFiles(literalIndexContext, 2);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(2, rows.all().size());

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void shouldCreateIndexFilesAfterMultipleSequentialIndexCreation() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1) VALUES ('0', '0');");
        flush();
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0');");
        flush();
        verifyNoIndexFiles();

        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), UTF8Type.instance);
        verifyIndexFiles(literalIndexContext, 2);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(2, rows.all().size());

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void shouldReleaseIndexFilesAfterCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), UTF8Type.instance);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1) VALUES ('0', '0');");
        flush();
        verifyIndexFiles(literalIndexContext, 1);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(1, rows.all().size());

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0');");
        flush();
        verifyIndexFiles(literalIndexContext, 2);
        rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(2, rows.all().size());

        compact();
        waitForAssert(() -> verifyIndexFiles(literalIndexContext, 1));

        rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(2, rows.all().size());

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void truncateWithBuiltIndexes() throws Throwable
    {
        verifyTruncateWithIndex(false);
    }

    @Test
    public void concurrentTruncateWithIndexBuilding() throws Throwable
    {
        verifyTruncateWithIndex(true);
    }

    private void verifyTruncateWithIndex(boolean concurrentTruncate) throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        if (!concurrentTruncate)
        {
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        }

        // create 100 rows, half in sstable and half in memtable
        int num = 100;
        for (int i = 0; i < num; i++)
        {
            if (i == num / 2)
                flush();
            execute("INSERT INTO %s (id1, v1) VALUES ('" + i + "', '0');");
        }

        if (concurrentTruncate)
        {
            createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
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

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void verifyRebuildCorruptedFiles() throws Throwable
    {
        // prepare schema and data
        createTable(CREATE_TABLE_TEMPLATE);
        String literalIndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        execute("INSERT INTO %s (id1, v1) VALUES ('0', '0');");
        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0');");
        flush();

        for (CorruptionType corruptionType : CorruptionType.values())
        {
            verifyRebuildCorruptedFiles(literalIndexName, corruptionType, false);
            verifyRebuildCorruptedFiles(literalIndexName, corruptionType, true);
        }

        assertZeroSegmentBuilderUsage();
    }

    private void verifyRebuildCorruptedFiles(String literalIndexName,
                                             CorruptionType corruptionType,
                                             boolean rebuild) throws Throwable
    {
        IndexContext LiteralIndexContext = createIndexContext(literalIndexName, UTF8Type.instance);

        for (IndexComponent component : Version.LATEST.onDiskFormat().perSSTableIndexComponents())
            verifyRebuildIndexComponent(LiteralIndexContext, component, null, corruptionType, rebuild);

        for (IndexComponent component : Version.LATEST.onDiskFormat().perColumnIndexComponents(LiteralIndexContext))
            verifyRebuildIndexComponent(LiteralIndexContext, component, LiteralIndexContext, corruptionType, rebuild);
    }

    private void verifyRebuildIndexComponent(IndexContext literalIndexContext,
                                             IndexComponent component,
                                             IndexContext corruptionContext,
                                             CorruptionType corruptionType,
                                             boolean rebuild) throws Throwable
    {
        // The completion markers are valid if they exist on the file system, so we only need to test
        // their removal. If we are testing with encryption then we don't want to test any components
        // that are encryptable unless they have been removed because encrypted components aren't
        // checksum validated.

        if (component == IndexComponent.PRIMARY_KEY_TRIE || component == IndexComponent.PRIMARY_KEY_BLOCKS || component == IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS)
            return;

        if (((component == IndexComponent.GROUP_COMPLETION_MARKER) ||
             (component == IndexComponent.COLUMN_COMPLETION_MARKER)) &&
            (corruptionType != CorruptionType.REMOVED))
            return;

        int rowCount = 2;

        // initial verification
        verifySSTableIndexes(literalIndexContext.getIndexName(), 1);
        verifyIndexFiles(literalIndexContext, 1, 1, 1);
        assertTrue(verifyChecksum(literalIndexContext));

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(rowCount, rows.all().size());

        // corrupt file
        if (corruptionContext != null)
            corruptIndexComponent(component, corruptionContext, corruptionType);
        else
            corruptIndexComponent(component, corruptionType);

        // If we are removing completion markers then the rest of the components should still have
        // valid checksums.
        boolean expectedLiteralState = isBuildCompletionMarker(component);

        assertEquals("Validation for " + component + " should be " + expectedLiteralState + " but was " + !expectedLiteralState,
                     expectedLiteralState,
                     validateComponents(literalIndexContext));

        assertEquals("Checksum verification for " + component + " should be " + expectedLiteralState + " but was " + !expectedLiteralState,
                     expectedLiteralState,
                     verifyChecksum(literalIndexContext));

        if (rebuild)
        {
            rebuildIndexes(literalIndexContext.getIndexName());
        }
        else
        {
            // Reload all SSTable indexes to manifest the corruption:
            reloadSSTableIndex();

            // Verify the index cannot be read:
            verifySSTableIndexes(literalIndexContext.getIndexName(), Version.LATEST.onDiskFormat().perSSTableIndexComponents().contains(component) ? 0 : 1, 0);

            try
            {
                // If the corruption is that a file is missing entirely, the index won't be marked non-queryable...
                rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
                assertEquals(0, rows.all().size());
            }
            catch (ReadFailureException e)
            {
                // ...but most kind of corruption will result in the index being non-queryable.
            }

            // Simulate the index repair that would occur on restart:
            runInitializationTask();
        }

        // verify indexes are recovered
        verifySSTableIndexes(literalIndexContext.getIndexName(), 1);
        verifyIndexFiles(literalIndexContext, 1, 1, 1);

        rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(rowCount, rows.all().size());
    }

    @Test
    public void verifyCleanupFailedPerIndexFiles() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id1, v1) VALUES ('0', '0');");
        flush();
        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0');");
        flush();

        // Inject failure
        Injections.inject(failPerIndexMetaCompletion);
        failPerIndexMetaCompletion.enable();

        try
        {
            // Create a new index, which will actuate a build compaction and fail, but leave the node running...
            IndexContext literalIndexContext = createIndexContext(createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1")), UTF8Type.instance);
            // two index builders running in different compaction threads because of parallelised index initial build
            waitForAssert(() -> assertEquals(2, indexBuildCounter.get()));
            waitForCompactionsFinished();

            // Only token/primary key files for the first SSTable in the compaction task should exist, while column-specific files are blown away:
            verifyIndexFiles(literalIndexContext, 2, 0, 0);

            Assertions.assertThat(getNotQueryableIndexes()).isNotEmpty();

            assertZeroSegmentBuilderUsage();
        }
        finally
        {
            failPerIndexMetaCompletion.disable();
        }
    }

    @Test
    public void verifyCleanupFailedTokenPrimaryKeyFiles() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id1, v1) VALUES ('0', '0');");
        flush();
        execute("INSERT INTO %s (id1, v1) VALUES ('1', '0');");
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

            Assertions.assertThat(getNotQueryableIndexes()).isNotEmpty();

            assertZeroSegmentBuilderUsage();
        }
        finally
        {
            failPerSSTableTokenAdd.disable();
        }
    }

    @Test
    public void verifyFlushAndCompactEmptyIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), UTF8Type.instance);

        // flush empty index
        execute("INSERT INTO %s (id1) VALUES ('0');");
        flush();

        execute("INSERT INTO %s (id1) VALUES ('1');");
        flush();

        verifyIndexFiles(literalIndexContext, 2, 0, 2);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(0, rows.all().size());

        // compact empty index
        compact();
        waitForAssert(() -> verifyIndexFiles(literalIndexContext, 1, 0, 1));

        rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(0, rows.all().size());

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void verifyFlushAndCompactNonIndexableRows() throws Throwable
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
    public void verifyFlushAndCompactTombstones() throws Throwable
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

    private void verifyFlushAndCompactEmptyIndexes(Runnable populateData) throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);


        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), UTF8Type.instance);

        populateData.run();
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 2, 0);
        verifyIndexFiles(literalIndexContext, 2, 0, 2);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(0, rows.all().size());

        // compact empty index
        compact();
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 1, 0);
        waitForAssert(() -> verifyIndexFiles(literalIndexContext, 1, 0, 1));

        rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(0, rows.all().size());

        assertZeroSegmentBuilderUsage();
    }

    @Test
    public void droppingIndexStopInitialIndexBuild() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        int num = 100;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES (?, '0')", Integer.toString(i));
        }
        flush();

        Injections.Barrier delayIndexBuilderCompletion = Injections.newBarrier("delayIndexBuilder", 2, false)
                                                                   .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                                                   .build();

        Injections.inject(delayIndexBuilderCompletion);
        String indexName = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForAssert(() -> assertEquals(1, delayIndexBuilderCompletion.getCount()));

        dropIndex("DROP INDEX %s." + indexName);

        // let blocked builders to continue
        delayIndexBuilderCompletion.countDown();
        waitForCompactions();

        delayIndexBuilderCompletion.disable();

        verifySSTableIndexes(indexName, 0);
        assertFalse("Expect index not built", SystemKeyspace.isIndexBuilt(KEYSPACE, indexName));

        // create index again, it should succeed
        indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        verifySSTableIndexes(indexName, 1);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
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
            execute("INSERT INTO %s (id1, v1) VALUES ('" + i + "', '0');");
        }
        flush();

        Injections.Barrier delayIndexBuilderCompletion = Injections.newBarrierAwait("delayIndexBuilder", 1, true)
                                                                   .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                                                   .build();

        Injections.inject(delayIndexBuilderCompletion);

        IndexContext literalIndexContext = createIndexContext(createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1")), UTF8Type.instance);

        waitForAssert(() -> assertTrue(getCompactionTasks() > 0), 1000, TimeUnit.MILLISECONDS);

        // Stop initial index build by interrupting active and pending compactions
        int attempt = 20;
        while (getCompactionTasks() > 0 && attempt > 0)
        {
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
        verifyInitialIndexFailed(literalIndexContext.getIndexName());
        Assertions.assertThat(getNotQueryableIndexes()).isNotEmpty();

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (Index i : cfs.indexManager.listIndexes())
        {
            StorageAttachedIndex index = (StorageAttachedIndex) i;
            assertEquals(0, index.getIndexContext().getMemtableIndexManager().size());

            View view = index.getIndexContext().getView();
            assertTrue("Expect index build stopped", view.getIndexes().isEmpty());
        }

        assertZeroSegmentBuilderUsage();

        // rebuild index
        ColumnFamilyStore.rebuildSecondaryIndex(KEYSPACE, currentTable(), literalIndexContext.getIndexName());

        verifyIndexFiles(literalIndexContext, sstable);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1='0'");
        assertEquals(num, rows.all().size());

        assertZeroSegmentBuilderUsage();

        assertTrue(verifyChecksum(literalIndexContext));
    }

    @Test
    public void shouldRejectQueriesWithCustomExpressions()
    {
        createTable(CREATE_TABLE_TEMPLATE);

        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        assertThatThrownBy(() -> executeNet(String.format("SELECT * FROM %%s WHERE expr(%s, '0')", index)))
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

    private void assertZeroSegmentBuilderUsage() throws Exception
    {
        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0, getColumnIndexBuildsInProgress());
    }
}
