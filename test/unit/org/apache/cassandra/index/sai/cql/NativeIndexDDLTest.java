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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.restrictions.IndexRestrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
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
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.SuppressLeakCheck;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@SuppressLeakCheck(jira="STAR-974")
public class NativeIndexDDLTest extends SAITester
{
    private static final Injections.Counter NDI_CREATION_COUNTER = Injections.newCounter("IndexCreationCounter")
                                                                             .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("register"))
                                                                             .build();

    private static final Injection failNDIInitialializaion = Injections.newCustom("fail_ndi_initialization")
                                                                       .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                                                       .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                                                       .build();

    private static final Injection failNumericIndexBuild = Injections.newCustom("fail_numeric_index_build")
                                                                       .add(InvokePointBuilder.newInvokePoint().onClass(SegmentBuilder.KDTreeSegmentBuilder.class).onMethod("addInternal"))
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

    private static final Injection pauseSAIWriterComplete = Injections.newPause("pause_sai_writer_complete", 10_000)
                                                                      .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete"))
                                                                      .build();

    @BeforeClass
    public static void init()
    {
        System.setProperty("cassandra.sai.validate_max_term_size_at_coordinator", Boolean.TRUE.toString());
    }

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();

        Injections.inject(NDI_CREATION_COUNTER, INDEX_BUILD_COUNTER, FAIL_INDEX_GC_TRANSACTION);

        NDI_CREATION_COUNTER.reset();
        INDEX_BUILD_COUNTER.reset();
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
                executeNet(String.format("CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'", cql3Type));
                assertTrue("Index creation on unsupported type " + cql3Type + " should have failed.", supported);
            }
            catch (RuntimeException e)
            {
                assertFalse("Index creation on supported type " + cql3Type + " should have succeeded.", supported);
                // InvalidConfigurationInQueryException is sub-class of InvalidQueryException
                assertTrue(Throwables.isCausedBy(e, InvalidQueryException.class));
            }
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
                                            "WITH OPTIONS = { 'case_sensitive' : false }")).isInstanceOf(InvalidQueryException.class);
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
    public void shouldNotFailCreateWithTupleType()
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
    public void shouldCreateIndexIfExists()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(val) USING 'StorageAttachedIndex' ");

        createIndexAsync("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(val) USING 'StorageAttachedIndex' ");

        assertEquals(1, NDI_CREATION_COUNTER.get());
    }

    @Test
    public void shouldBeCaseSensitiveByDefault()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Camel'").size());

        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'camel'").size());
    }

    @Test
    public void shouldEnableCaseSensitiveSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        // Case sensitive search is the default, and as such, it does not make the SAI qualify as "analyzed".
        // The queries below use '=' and not ':' because : is limited to analyzed indexes.
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : true }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Camel'").size());

        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'camel'").size());
    }

    @Test
    public void shouldEnableCaseInsensitiveSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : false }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Camel')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val : 'camel'").size());
        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'camel'").size());
    }

    @Test
    public void shouldBeNonNormalizedByDefault()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Cam\u00E1l'").size());

        // Both \u00E1 and \u0061\u0301 are visible as the character á, but without NFC normalization, they won't match.
        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'Cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableNonNormalizedSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        // Normalize search is disabled by default, and as such, it does not make the SAI qualify as "analyzed".
        // The queries below use '=' and not ':' because : is limited to analyzed indexes.
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = { 'normalize' : false }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'Cam\u00E1l'").size());

        // Both \u00E1 and \u0061\u0301 are visible as the character á, but without NFC normalization, they won't match.
        assertEquals(0, execute("SELECT id FROM %s WHERE val = 'Cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableNormalizedSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = { 'normalize' : true }");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val : 'Cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableNormalizedCaseInsensitiveSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = { 'normalize' : true, 'case_sensitive' : false}");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Cam\u00E1l')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val : 'cam\u0061\u0301l'").size());
    }

    @Test
    public void shouldEnableAsciiSearch()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = { 'ascii' : true, 'case_sensitive' : false}");

        execute("INSERT INTO %s (id, val) VALUES ('1', 'Éppinger')");

        assertEquals(1, execute("SELECT id FROM %s WHERE val : 'eppinger'").size());
    }

    @Test
    public void shouldCreateIndexOnReversedType()
    {
        createTable("CREATE TABLE %s (id text, ck1 text, ck2 int, val text, PRIMARY KEY (id,ck1,ck2)) WITH CLUSTERING ORDER BY (ck1 desc, ck2 desc)");

        String indexNameCk1 = createIndex("CREATE CUSTOM INDEX ON %s(ck1) USING 'StorageAttachedIndex'");
        String indexNameCk2 = createIndex("CREATE CUSTOM INDEX ON %s(ck2) USING 'StorageAttachedIndex'");

        execute("insert into %s(id, ck1, ck2, val) values('1', '2', 3, '3')");
        execute("insert into %s(id, ck1, ck2, val) values('1', '3', 4, '4')");
        assertEquals(1, executeNet("SELECT * FROM %s WHERE ck1='3'").all().size());
        assertEquals(2, executeNet("SELECT * FROM %s WHERE ck2>=0").all().size());
        assertEquals(2, executeNet("SELECT * FROM %s WHERE ck2<=4").all().size());

        flush();
        assertEquals(1, executeNet("SELECT * FROM %s WHERE ck1='2'").all().size());
        assertEquals(2, executeNet("SELECT * FROM %s WHERE ck2>=3").all().size());
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

        assertEquals(1, NDI_CREATION_COUNTER.get());
    }

    /**
     * Verify SASI can be created and queries with SAI dependencies.
     * Not putting in {@link MixedIndexImplementationsTest} because it uses CQLTester which doesn't load SAI dependency.
     */
    @Test
    public void shouldCreateSASI()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        // without options
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        Assertions.assertThatThrownBy(() -> execute("SELECT id1 FROM %s WHERE v1>=0"))
                .hasMessageContaining(String.format(StatementRestrictions.HAS_UNSUPPORTED_SASI_INDEX_RESTRICTION_MESSAGE, "v1"));
        dropIndex("DROP INDEX " + keyspace() + '.' + currentIndex());

        // with options
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS',\n" +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',\n" +
                    "'tokenization_enable_stemming': 'true',\n" +
                    "'tokenization_locale': 'en',\n" +
                    "'tokenization_skip_stop_words': 'true',\n" +
                    "'analyzed': 'true',\n" +
                    "'tokenization_normalize_lowercase': 'true'};");
        Assertions.assertThatThrownBy(() -> execute("SELECT id1 FROM %s WHERE v1>=0"))
                .hasMessageContaining(String.format(StatementRestrictions.HAS_UNSUPPORTED_SASI_INDEX_RESTRICTION_MESSAGE, "v1"));
    }

    @Test
    public void shouldCreateNumericIndexWithBkdPostingsSkipAndMinLeaves()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'bkd_postings_skip' : 3, 'bkd_postings_min_leaves' : 32}");

        assertEquals(1, NDI_CREATION_COUNTER.get());
    }

    @Test
    public void shouldCreateNumericIndexWithBkdPostingsSkipOnly()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'bkd_postings_skip' : 3}");

        assertEquals(1, NDI_CREATION_COUNTER.get());
    }

    @Test
    public void shouldCreateNumericIndexWithBkdPostingsMinLeavesOnly()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'bkd_postings_min_leaves': 32}");

        assertEquals(1, NDI_CREATION_COUNTER.get());
    }

    @Test
    public void shouldFailToCreateNumericIndexWithTooLowBkdPostingsSkip()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = {'bkd_postings_skip' : 0, 'bkd_postings_min_leaves' : 32}")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailToCreateNumericIndexWithTooLowBkdPostingsMinLeaves()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = {'bkd_postings_skip' : 3, 'bkd_postings_min_leaves' : 0}")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailToCreateStringIndexWithBkdPostingsSkip()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = {'bkd_postings_skip' : 3}")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailToCreateStringIndexWithBkdPostingsMinLeaves()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = {'bkd_postings_min_leaves' : 9}")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailToCreateInvalidBooleanOption()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = {'case_sensitive': 'NOTVALID'}")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void shouldFailToCreateEmptyBooleanOption()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' " +
                                            "WITH OPTIONS = {'case_sensitive': ''}")).isInstanceOf(InvalidQueryException.class);
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
    public void shouldFailCreationMultipleIndexesOnSimpleColumn()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v1 TEXT)");
        execute("INSERT INTO %s (id, v1) VALUES(1, '1')");
        flush();

        executeNet("CREATE CUSTOM INDEX index_1 ON %s(v1) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        // same name
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX index_1 ON %s(v1) USING 'StorageAttachedIndex'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Index 'index_1' already exists");

        // different name, same option
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX index_2 ON %s(v1) USING 'StorageAttachedIndex'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Index index_2 is a duplicate of existing index index_1");

        // different name, different option, same target.
        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : true }"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Cannot create duplicate storage-attached index on column: v1" );

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
        String indexName = createIndexAsync("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Flush the Memtable's contents, which will feed data to the index as the SSTable is written:
        flush();

        // Allow the initialization task, which builds the index, to continue:
        delayInitializationTask.countDown();

        waitForIndexQueryable(indexName);

        ResultSet rows = executeNet("SELECT id FROM %s WHERE val = 'Camel'");
        assertEquals(1, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
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

        Injections.inject(failNDIInitialializaion);
        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForAssert(() -> assertEquals(1, INDEX_BUILD_COUNTER.get()));
        waitForCompactions();

        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1>=0")).isInstanceOf(ReadFailureException.class);
    }

    @Test
    public void testAwaitInitialIndexBuilds() throws Throwable
    {
        // setup table and SAI
        createTable(CREATE_TABLE_TEMPLATE);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE);
        cfs.awaitInitialIndexBuilds(10, TimeUnit.SECONDS);  // no-ops: no initial index

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        flush();

        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForAssert(() -> assertEquals(1, INDEX_BUILD_COUNTER.get()));
        waitForCompactions();
        cfs.awaitInitialIndexBuilds(10, TimeUnit.SECONDS); // no-ops:  no initial index

        // create another CFS to simulate startup with SAI
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        TableMetadataRef metadataRef = Schema.instance.getTableMetadataRef(KEYSPACE, currentTable());
        ColumnFamilyStore recreatedCfs = ColumnFamilyStore.createColumnFamilyStore(keyspace, currentTable(), metadataRef, new Directories(metadataRef.get()), true, false, true);
        try
        {
            recreatedCfs.awaitInitialIndexBuilds(5, TimeUnit.SECONDS);
        }
        finally
        {
            recreatedCfs.unloadCf();
        }
    }

    @Test
    public void testAwaitInitialIndexBuildsWithFailure() throws Throwable
    {
        // setup table and SAI
        createTable(CREATE_TABLE_TEMPLATE);
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        flush();

        // inject SAI initialization task failure to fail building SAI files
        Injections.inject(failNDIInitialializaion);
        createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForAssert(() -> assertEquals(1, INDEX_BUILD_COUNTER.get()));
        waitForCompactions();
        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1>=0")).isInstanceOf(ReadFailureException.class);

        // create another CFS to simulate startup with SAI to trigger initial build task
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        TableMetadataRef metadataRef = Schema.instance.getTableMetadataRef(KEYSPACE, currentTable());
        ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(keyspace, currentTable(), metadataRef, new Directories(metadataRef.get()), true, false, true);
        try
        {
            assertThatThrownBy(() -> cfs.awaitInitialIndexBuilds(5, TimeUnit.SECONDS))
                    .isInstanceOf(RuntimeException.class)
                    .hasRootCauseMessage("Injected failure!");
        }
        finally
        {
            cfs.unloadCf();
        }
    }

    @Test
    public void testMaxTermSizeRejectionsAtWrite()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v text, m map<text, text>, frozen_m frozen<map<text, text>>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(m) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(full(frozen_m)) USING 'StorageAttachedIndex'");

        String largeTerm = UTF8Type.instance.compose(ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT / 2 + 1));
        assertThatThrownBy(() -> executeNet("INSERT INTO %s (k, v) VALUES (0, ?)", largeTerm))
        .hasMessage(String.format("Term of column v exceeds the byte limit for index. Term size 32.000KiB. Max allowed size %s.",
                                  FBUtilities.prettyPrintMemory(IndexContext.MAX_STRING_TERM_SIZE)))
        .isInstanceOf(InvalidQueryException.class);

        assertThatThrownBy(() -> executeNet("INSERT INTO %s (k, m) VALUES (0, {'key': '" + largeTerm + "'})"))
        .hasMessage(String.format("Term of column m exceeds the byte limit for index. Term size 32.000KiB. Max allowed size %s.",
                                  FBUtilities.prettyPrintMemory(IndexContext.MAX_STRING_TERM_SIZE)))
        .isInstanceOf(InvalidQueryException.class);

        assertThatThrownBy(() -> executeNet("INSERT INTO %s (k, frozen_m) VALUES (0, {'key': '" + largeTerm + "'})"))
        .hasMessage(String.format("Term of column frozen_m exceeds the byte limit for index. Term size 32.015KiB. Max allowed size %s.",
                                  FBUtilities.prettyPrintMemory(IndexContext.MAX_FROZEN_TERM_SIZE)))
        .isInstanceOf(InvalidQueryException.class);
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
        verifySSTableIndexes(numericIndexName, 0);
        verifySSTableIndexes(literalIndexName, 0);

        assertEquals("Segment memory limiter should revert to zero on drop.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void shouldCreateIndexFilesAfterMultipleConcurrentIndexCreation() throws Throwable
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
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 2);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void shouldCreateIndexFilesAfterMultipleSequentialIndexCreation() throws Throwable
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
        verifyIndexFiles(numericIndexContext, null, 2, 0);
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());

        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 2);
        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(2, rows.all().size());

        assertEquals("Segment memory limiter should revert to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
    }

    @Test
    public void shouldReleaseIndexFilesAfterCompaction() throws Throwable
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

        // Set up logging appender to capture log messages
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(StorageAttachedIndexBuilder.class);
        InMemoryAppender appender = new InMemoryAppender();
        logger.addAppender(appender);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.INFO);

        try
        {
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
                // Flush remaining rows to ensure we have SSTables for index builds
                flush();

                // Inject a delay at indexWriter.begin() which is called AFTER build setup
                // but BEFORE the indexing loop. This ensures builds are in a running state
                // and will check isStopRequested() when they continue after truncate.
                Injection slowDown = Injections.newPause("slowDownBuild", 5000)
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndexWriter.class)
                                                                      .onMethod("begin")
                                                                      .atEntry())
                                               .build();
                
                Injections.inject(slowDown);

                try
                {
                    createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
                    createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v2"));
                    
                    // Wait for index builds to be submitted to CompactionManager
                    waitForAssert(() -> assertTrue("Index build should be submitted",
                                                 INDEX_BUILD_COUNTER.get() > 0),
                                 5, TimeUnit.SECONDS);

                    // CRITICAL: Wait for builds to actually START processing partitions
                    // We need to ensure builds have entered the indexing loop and hit the first
                    // startPartition() call BEFORE we truncate. Otherwise truncate happens before
                    // builds reach the interruptible phase and they never see the stop request.
                    // The 10s delay per partition means once they start, they'll be running for a while.
                    waitForAssert(() -> assertTrue("Index builds should be running",
                                                   getCompactionTasks() > 0),
                                  10, TimeUnit.SECONDS);
                    
                    // Now truncate while builds are actively processing (stuck in the 10s delay)
                    // This ensures truncate happens AFTER builds have started, so they'll detect it
                    truncate(true);
                    
                    // Wait for all compactions/builds to complete after truncate
                    waitForCompactions();
                    
                    waitForTableIndexesQueryable();
                }
                finally
                {
                    Injections.deleteAll();
                }
            }
            else
            {
                truncate(true);
                
                // Verify NO stop logs when truncating with already-built indexes
                verifyIndexBuildLog(appender, Level.INFO, "Stop requested while building indexes", false);
                verifyIndexBuildLog(appender, Level.ERROR, "Stop requested while building initial indexes", false);
            }

            waitForAssert(this::verifyNoIndexFiles);

            // verify index-view-manager has been cleaned up
            verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 0);
            verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), 0);

            assertEquals("Segment memory limiter should revert to zero after truncate.", 0L, getSegmentBufferUsedBytes());
            assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());
        }
        finally
        {
            logger.setLevel(originalLevel);
            logger.detachAppender(appender);
        }
    }

    /**
     * Simulate SAI build error during index rebuild: index rebuild task should fail
     */
    @Test
    public void testIndexRebuildAborted() throws Throwable
    {
        // prepare schema and data
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(2, rows.all().size());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SecondaryIndexManager sim = cfs.getIndexManager();
        StorageAttachedIndex sai = (StorageAttachedIndex) sim.listIndexes().iterator().next();
        assertThat(sai.getIndexContext().getView().getIndexes()).hasSize(1);

        StorageAttachedIndexGroup saiGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(saiGroup.sstableContextManager().size()).isEqualTo(1);

        // rebuild index with byteman error
        Injections.inject(failNumericIndexBuild);

        // rebuild should fail
        assertThatThrownBy(() -> sim.buildIndexesBlocking(cfs.getLiveSSTables(), new HashSet<>(sim.listIndexes()), true))
                          .isInstanceOf(RuntimeException.class).hasMessageContaining("Injected failure");

        // index is no longer queryable
        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1>=0"))
        .isInstanceOf(ReadFailureException.class);
    }

    /**
     * Simulate SAI build error during compaction: compaction task should abort
     */
    @Test
    public void testIndexCompactionAborted() throws Throwable
    {
        // prepare schema and data
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('2', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('3', 1, '0');");
        flush();

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(4, rows.all().size());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(2);

        SecondaryIndexManager sim = cfs.getIndexManager();
        StorageAttachedIndex sai = (StorageAttachedIndex) sim.listIndexes().iterator().next();
        assertThat(sai.getIndexContext().getView().getIndexes()).hasSize(2);

        StorageAttachedIndexGroup saiGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(saiGroup.sstableContextManager().size()).isEqualTo(2);

        // rebuild index with byteman error
        Injections.inject(failNumericIndexBuild);

        // compaction task should fail
        assertThatThrownBy(cfs::forceMajorCompaction)
        .isInstanceOf(RuntimeException.class).hasMessageContaining("Injected failure");

        // verify sstables and indexes are the same
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        assertThat(saiGroup.sstableContextManager().size()).isEqualTo(2);
        assertThat(sai.getIndexContext().getView().getIndexes()).hasSize(2);

        // index is still queryable
        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(4, rows.all().size());
    }

    @Test
    public void testIndexCompactionWhenIndexIsDropped() throws Throwable
    {
        // prepare schema and data
        createTable(CREATE_TABLE_TEMPLATE);
        String indexName1 = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('2', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('3', 1, '0');");
        flush();

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(4, rows.all().size());

        // pause index writer completion
        Injections.inject(pauseSAIWriterComplete);
        pauseSAIWriterComplete.enable();

        try
        {

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertThat(cfs.getLiveSSTables()).hasSize(2);

            new TestWithConcurrentVerification(
            () -> cfs.forceMajorCompaction(),
            () -> {
                try
                {
                    // wait for compaction paused
                    FBUtilities.sleepQuietly(5000);

                    dropIndex("DROP INDEX %s." + indexName1);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }, -1 // run verification task once
            ).start();

            // compaction completed
            assertThat(cfs.getLiveSSTables()).hasSize(1);

            StorageAttachedIndexGroup saiGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
            assertThat(saiGroup.sstableContextManager().size()).isEqualTo(1);
        }
        finally
        {
            pauseSAIWriterComplete.disable();
        }
    }

    @Test
    public void testIndexCompactionWhenIndexIsUnloaded() throws Throwable
    {
        // prepare schema and data
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('2', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('3', 1, '0');");
        flush();

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(4, rows.all().size());

        // pause index writer completion
        Injections.inject(pauseSAIWriterComplete);
        pauseSAIWriterComplete.enable();

        try
        {
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertThat(cfs.getLiveSSTables()).hasSize(2);
            TestWithConcurrentVerification compactionTask = new TestWithConcurrentVerification(
            () -> cfs.forceMajorCompaction(),
            () -> {
                try
                {
                    // wait for compaction paused
                    FBUtilities.sleepQuietly(5000);

                    SecondaryIndexManager sim = cfs.getIndexManager();
                    sim.unloadAllIndexes();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }, -1 // run verification task once
            );

            assertThatThrownBy(compactionTask::start).hasMessageContaining("is unloaded");

            // compaction is aborted
            assertThat(cfs.getLiveSSTables()).hasSize(2);

            // indexes are unloaded
            StorageAttachedIndexGroup saiGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
            assertThat(saiGroup.sstableContextManager().size()).isEqualTo(0);
        }
        finally
        {
            pauseSAIWriterComplete.disable();
        }
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
        IndexContext numericIndexContext = getIndexContext(numericIndexName);
        IndexContext stringIndexContext = getIndexContext(stringIndexName);

        for (IndexComponentType component : Version.current(KEYSPACE).onDiskFormat()
                                                   .perSSTableComponentTypes(currentTableMetadata().hasClustering()))
            verifyRebuildIndexComponent(numericIndexContext, stringIndexContext, component, null, corruptionType, true, true, rebuild);

        for (IndexComponentType component : Version.current(KEYSPACE).onDiskFormat().perIndexComponentTypes(numericIndexContext))
            verifyRebuildIndexComponent(numericIndexContext, stringIndexContext, component, numericIndexContext, corruptionType, false, true, rebuild);

        for (IndexComponentType component : Version.current(KEYSPACE).onDiskFormat().perIndexComponentTypes(stringIndexContext))
            verifyRebuildIndexComponent(numericIndexContext, stringIndexContext, component, stringIndexContext, corruptionType, true, false, rebuild);
    }

    private void verifyRebuildIndexComponent(IndexContext numericIndexContext,
                                             IndexContext stringIndexContext,
                                             IndexComponentType component,
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
        if (((component == IndexComponentType.GROUP_COMPLETION_MARKER) ||
             (component == IndexComponentType.COLUMN_COMPLETION_MARKER)) &&
            (corruptionType != CorruptionType.REMOVED))
            return;

        // Skip per SSTable components for v2 primary key maps
        if (component == IndexComponentType.PRIMARY_KEY_TRIE || component == IndexComponentType.PRIMARY_KEY_BLOCKS || component == IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS)
            return;

        // Skip per SSTables components for v9 primary key maps
        if (component == IndexComponentType.ROW_TO_TOKEN || component == IndexComponentType.ROW_TO_PARTITION ||
            component == IndexComponentType.PARTITION_TO_SIZE || component == IndexComponentType.PARTITION_KEY_BLOCKS ||
            component == IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS || component == IndexComponentType.CLUSTERING_KEY_BLOCKS ||
            component == IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS)
            return;

        logger.info("CORRUPTING: {}, corruption type = {}", component, corruptionType);

        int rowCount = 2;

        // initial verification
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1);
        verifySSTableIndexes(stringIndexContext.getIndexName(), 1);
        verifyIndexComponentFiles(numericIndexContext, stringIndexContext);
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

        // Reload all SSTable indexes to manifest the corruption:
        reloadSSTableIndex();

        try
        {
            // If the corruption is that a file is missing entirely, the index won't be marked non-queryable...
            rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
            // If we corrupted the index (and it's still queryable), we get either 0 or 2, depending on whether
            // there is previous build of the index that gets automatically picked up. But mostly, we want to ensure
            // the index does work if it's not corrupted.
            if (!failedNumericIndex)
                assertEquals(rowCount, rows.all().size());

            //assertEquals(failedNumericIndex ? 0 : rowCount, rows.all().size());
        }
        catch (ReadFailureException e)
        {
            // ...but most kind of corruption will result in the index being non-queryable.
        }

        try
        {
            // If the corruption is that a file is missing entirely, the index won't be marked non-queryable...
            rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
            // Same as above
            if (!failedStringIndex)
                assertEquals(rowCount, rows.all().size());
        }
        catch (ReadFailureException e)
        {
            // ...but most kind of corruption will result in the index being non-queryable.
        }

        if (rebuild)
        {
            rebuildIndexes(numericIndexContext.getIndexName(), stringIndexContext.getIndexName());
        }
        else
        {
            // Simulate the index repair that would occur on restart:
            runInitializationTask();
        }

        // verify indexes are recovered
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1);
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1);
        verifyIndexComponentFiles(numericIndexContext, stringIndexContext);

        rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(rowCount, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(rowCount, rows.all().size());
    }

    /**
     * Tests that we can rebuild and reload in place to a newer version only after upgrading sstables.
     * More exhaustive testing is done in {@link CreateIndexWithNewVersionTest}.
     */
    @Test
    public void verifyCanRebuildAndReloadInPlaceToNewerVersionOnlyAfterSSTableUpgrade()
    {
        Version current = Version.current(KEYSPACE);
        try
        {
            SAIUtil.setCurrentVersion(Version.AA);

            // prepare schema and data
            createTable(CREATE_TABLE_TEMPLATE);
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

            int rowCount = 2;
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
            flush();

            // Sanity check first
            Runnable queryTester = () -> {
                ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
                assertEquals(rowCount, rows.all().size());
                rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
                assertEquals(rowCount, rows.all().size());
            };
            queryTester.run();
            verifySAIVersionInUse(Version.AA);

            SAIUtil.setCurrentVersion(current);

            // The queries should still work, but the version should not have changed.
            rebuildTableIndexes();
            reloadSSTableIndexInPlace();
            queryTester.run();
            verifySAIVersionInUse(Version.AA);

            // Now upgrade sstables and try again
            upgradeSSTables();
            queryTester.run();
            verifySAIVersionInUse(current);

            // Rebuild once again
            rebuildTableIndexes();
            reloadSSTableIndexInPlace();
            queryTester.run();
            verifySAIVersionInUse(current);
        }
        finally
        {
            // If we haven't failed, we should already have done this, but if we did fail ...
            SAIUtil.setCurrentVersion(current);
        }
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
            waitForAssert(() -> assertEquals(2, INDEX_BUILD_COUNTER.get()));
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
    public void verifyCleanupFailedTokenOffsetFiles() throws Throwable
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
            waitForAssert(() -> assertEquals(2, INDEX_BUILD_COUNTER.get()));
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
    public void verifyFlushAndCompactEmptyIndex() throws Throwable
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


        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);

        populateData.run();
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 2, 2);
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), 2, 2);
        verifyIndexFiles(numericIndexContext, literalIndexContext, 2, 0, 0, 2, 2);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(0, rows.all().size());
        rows = executeNet("SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(0, rows.all().size());

        // compact empty index
        compact();
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), 1, 1);
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), 1, 1);
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

        verifySSTableIndexes(indexName, 0);
        assertFalse("Expect index not built", SystemKeyspace.isIndexBuilt(KEYSPACE, indexName));

        // create index again, it should succeed
        indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        verifySSTableIndexes(indexName, 1);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(num, rows.all().size());
    }

    @Test
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

        IndexContext numericIndexContext = getIndexContext(createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1")));

        waitForAssert(() -> assertTrue(getCompactionTasks() > 0), 1000, TimeUnit.MILLISECONDS);

        // Stop initial index build by interrupting active and pending compactions
        int attempt = 20;
        while (getCompactionTasks() > 0 && attempt > 0)
        {
            logger.info("Attempt {} at stopping the compaction tasks", attempt);

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
        assertFalse(areAllTableIndexesQueryable());

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (Index i : cfs.indexManager.listIndexes())
        {
            StorageAttachedIndex index = (StorageAttachedIndex) i;
            assertTrue(index.getIndexContext().getLiveMemtables().isEmpty());

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
    public void testStopRequestedDuringIndexRebuild() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);

        // Create data and initial index
        int num = 100;
        for (int i = 0; i < num; i++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('" + i + "', 0, '0');");

        flush();

        String indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForTableIndexesQueryable();

        // Set up logging appender to capture log messages BEFORE starting rebuild
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(StorageAttachedIndexBuilder.class);
        InMemoryAppender appender = new InMemoryAppender();
        logger.addAppender(appender);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.INFO);

        try
        {
            // Set up barrier to delay index rebuild - use newBarrier instead of newBarrierAwait
            Injections.Barrier delayIndexRebuild = Injections.newBarrier("delayIndexRebuild", 2, false)
                                                             .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                                             .build();

            Injections.inject(delayIndexRebuild);

            // Start index rebuild in background (isInitialBuild=false)
            Thread rebuildThread = new Thread(() -> {
                try
                {
                    rebuildIndexes(indexName);
                }
                catch (Exception e)
                {
                    // Expected to be interrupted
                    NativeIndexDDLTest.logger.info("Rebuild thread caught exception: {}", e.getMessage());
                }
            });
            rebuildThread.start();

            // Wait for rebuild to reach the barrier
            waitForAssert(() -> assertEquals(1, delayIndexRebuild.getCount()), 5000, TimeUnit.MILLISECONDS);

            // Stop the rebuild (this should trigger the INFO log we check later)
            CompactionManager.instance.stopCompaction(OperationType.INDEX_BUILD.name());
            
            // Let the rebuild continue and hit the stop
            delayIndexRebuild.countDown();

            // Wait for rebuild thread to complete
            rebuildThread.join(10000);

            delayIndexRebuild.disable();

            // Add small delay to allow async logging to complete
            Thread.sleep(500);

            // Verify that the INFO log was generated
            verifyIndexBuildLog(appender, Level.INFO, "Stop requested while building indexes", true);
        }
        finally
        {
            logger.setLevel(originalLevel);
            logger.detachAppender(appender);
        }
    }


    private static class InMemoryAppender extends AppenderBase<ILoggingEvent>
    {
        private final List<ILoggingEvent> events = new ArrayList<>();

        private InMemoryAppender()
        {
            start();
        }

        @Override
        protected synchronized void append(ILoggingEvent event)
        {
            events.add(event);
        }

        public synchronized List<ILoggingEvent> getEventsForLevel(Level level)
        {
            List<ILoggingEvent> result = new ArrayList<>();
            for (ILoggingEvent event : events)
            {
                if (event.getLevel() == level)
                {
                    result.add(event);
                }
            }
            return result;
        }
    }

    /**
     * Helper to verify expected log messages from StorageAttachedIndexBuilder
     * @param appender the log appender that captured events
     * @param expectedLevel the log level to check (INFO, ERROR, etc.)
     * @param expectedMessageFragment the text fragment expected in the log message
     * @param shouldExist true if the log should exist, false if it should NOT exist
     */
    private void verifyIndexBuildLog(InMemoryAppender appender, Level expectedLevel, String expectedMessageFragment, boolean shouldExist)
    {
        List<ILoggingEvent> events = appender.getEventsForLevel(expectedLevel);
        boolean found = events.stream()
            .anyMatch(e -> e.getFormattedMessage().contains(expectedMessageFragment));
        
        if (shouldExist)
            assertTrue("Expected " + expectedLevel + " log containing '" + expectedMessageFragment + "'. Captured " +
                       events.size() + ' ' + expectedLevel + " events.", found);
        else
            assertFalse("Should NOT have " + expectedLevel + " log containing '" + expectedMessageFragment + '\'', found);
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


    @Test
    public void shouldRejectLargeStringTerms()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String insert = "INSERT INTO %s (k, v) VALUES (0, ?)";

        // insert a text term with the max possible size
        execute(insert, ByteBuffer.allocate(IndexContext.MAX_STRING_TERM_SIZE));

        // insert a text term over the max possible size
        assertThatThrownBy(() -> execute(insert, ByteBuffer.allocate(IndexContext.MAX_STRING_TERM_SIZE + 1)))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Term of column v exceeds the byte limit for index");
    }

    @Test
    public void shouldRejectLargeFrozenTerms()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<text>>)");
        createIndex("CREATE CUSTOM INDEX ON %s(full(v)) USING 'StorageAttachedIndex'");
        String insert = "INSERT INTO %s (k, v) VALUES (0, ?)";

        // insert a frozen term with the max possible size
        // list serialization uses 4 bytes for the collection size, and then 4 bytes per each item size
        String value1 = UTF8Type.instance.compose(ByteBuffer.allocate(IndexContext.MAX_FROZEN_TERM_SIZE / 2 - 8));
        String value2 = UTF8Type.instance.compose(ByteBuffer.allocate(IndexContext.MAX_FROZEN_TERM_SIZE / 2 - 4));
        execute(insert, Arrays.asList(value1, value2));

        // insert a frozen term over the max possible size
        assertThatThrownBy(() -> execute(insert, Arrays.asList(value1, value2, "x")))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Term of column v exceeds the byte limit for index");
    }

    @Test
    public void shouldRejectLargeAnalyzedTerms()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'" +
                    " WITH OPTIONS = {'index_analyzer': 'whitespace'}");
        String insert = "INSERT INTO %s (k, v) VALUES (0, ?)";


        // insert an analyzed column with terms of cumulating up to the max possible size
        String term = UTF8Type.instance.compose(ByteBuffer.allocate(IndexContext.MAX_ANALYZED_SIZE / 2));
        execute(insert, term + ' ' + term);

        // insert a frozen term over the max possible size
        assertThatThrownBy(() -> execute(insert, term + ' ' + term + ' ' + term))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Term's analyzed size for column v exceeds the cumulative limit for index");
    }

    @Test
    public void shouldRejectLargeVector()
    {
        String table = "CREATE TABLE %%s (k int PRIMARY KEY, v vector<float, %d>)";
        String index = "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}";
        String insert = "INSERT INTO %s (k, v) VALUES (0, ?)";

        // insert a vector term with the max possible size
        int dimensions = IndexContext.MAX_VECTOR_TERM_SIZE / Float.BYTES;
        createTable(String.format(table, dimensions));
        createIndex(index);
        execute(insert, vector(new float[dimensions]));

        // create a vector index producing terms over the max possible size
        createTable(String.format(table, dimensions + 1));
        // VSTODO: uncomment when https://github.com/riptano/VECTOR-SEARCH/issues/85 is solved
//        assertThatThrownBy(() -> execute(index))
//        .isInstanceOf(InvalidRequestException.class)
//        .hasMessageContaining("An index of vector<float, 4097> will produce terms of 16.004KiB, " +
//                              "exceeding the max vector term size of 16.000KiB. " +
//                              "That sets an implicit limit of 4096 dimensions for float vectors.");
    }
}
