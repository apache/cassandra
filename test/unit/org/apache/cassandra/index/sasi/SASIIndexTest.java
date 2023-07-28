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
package org.apache.cassandra.index.sasi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.DelimiterAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.NoOpAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.exceptions.TimeQuotaExceededException;
import org.apache.cassandra.index.sasi.memory.IndexMemtable;
import org.apache.cassandra.index.sasi.plan.QueryController;
import org.apache.cassandra.index.sasi.plan.SASIIndexSearcher;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryManager;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.db.ColumnFamilyStoreTest.getSnapshotManifestAndSchemaFileSizes;

public class SASIIndexTest
{
    private static final IPartitioner PARTITIONER;

    static {
        CASSANDRA_CONFIG.setString("cassandra-murmur.yaml");
        PARTITIONER = Murmur3Partitioner.instance;
    }

    private static final String KS_NAME = "sasi";
    private static final String CF_NAME = "test_cf";
    private static final String CLUSTERING_CF_NAME_1 = "clustering_test_cf_1";
    private static final String CLUSTERING_CF_NAME_2 = "clustering_test_cf_2";
    private static final String STATIC_CF_NAME = "static_sasi_test_cf";
    private static final String FTS_CF_NAME = "full_text_search_sasi_test_cf";

    private long timestamp;

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KS_NAME,
                                    KeyspaceParams.simpleTransient(1),
                                    SchemaLoader.sasiCFMD(KS_NAME, CF_NAME),
                                    SchemaLoader.clusteringSASICFMD(KS_NAME, CLUSTERING_CF_NAME_1),
                                    SchemaLoader.clusteringSASICFMD(KS_NAME, CLUSTERING_CF_NAME_2, "location"),
                                    SchemaLoader.staticSASICFMD(KS_NAME, STATIC_CF_NAME),
                                    SchemaLoader.fullTextSearchSASICFMD(KS_NAME, FTS_CF_NAME));
        stores().forEach(ColumnFamilyStore::disableAutoCompaction);
    }

    @Before
    public void cleanUp()
    {
        timestamp = 0;
        cleanupData();
    }

    @Test
    public void testSASIComponentsAddedToSnapshot() throws Throwable
    {
        String snapshotName = "sasi_test";
        Map<String, Pair<String, Integer>> data = new HashMap<>();
        Random r = new Random();

        for (int i = 0; i < 100; i++)
            data.put(UUID.randomUUID().toString(), Pair.create(UUID.randomUUID().toString(), r.nextInt()));

        ColumnFamilyStore store = loadData(data, true);

        Set<SSTableReader> ssTableReaders = store.getLiveSSTables();
        Set<Component> sasiComponents = new HashSet<>();

        for (Index index : store.indexManager.listIndexes())
            if (index instanceof SASIIndex)
                sasiComponents.add(((SASIIndex) index).getIndex().getComponent());

        Assert.assertFalse(sasiComponents.isEmpty());

        try
        {
            store.snapshot(snapshotName);

            // Compact to make true snapshot size != 0
            store.forceMajorCompaction();
            LifecycleTransaction.waitForDeletions();

            SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(store.getDirectories().getSnapshotManifestFile(snapshotName));

            Assert.assertFalse(ssTableReaders.isEmpty());
            Assert.assertFalse(manifest.files.isEmpty());
            Assert.assertEquals(ssTableReaders.size(), manifest.files.size());

            Map<Descriptor, Set<Component>> snapshotSSTables = store.getDirectories()
                                                                    .sstableLister(Directories.OnTxnErr.IGNORE)
                                                                    .snapshots(snapshotName)
                                                                    .list();

            long indexSize = 0;
            long tableSize = 0;

            for (SSTableReader sstable : ssTableReaders)
            {
                File snapshotDirectory = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName);
                Descriptor snapshotSSTable = new Descriptor(snapshotDirectory,
                                                            sstable.getKeyspaceName(),
                                                            sstable.getColumnFamilyName(),
                                                            sstable.descriptor.id,
                                                            sstable.descriptor.version.format);

                Set<Component> components = snapshotSSTables.get(snapshotSSTable);

                Assert.assertNotNull(components);
                Assert.assertTrue(components.containsAll(sasiComponents));

                for (Component c : components)
                {
                    long componentSize = snapshotSSTable.fileFor(c).length();
                    if (Component.Type.fromRepresentation(c.name, sstable.descriptor.version.format) == Components.Types.SECONDARY_INDEX)
                        indexSize += componentSize;
                    else
                        tableSize += componentSize;
                }
            }
            
            TableSnapshot details = store.listSnapshots().get(snapshotName);

            // check that SASI components are included in the computation of snapshot size
            long snapshotSize = tableSize + indexSize + getSnapshotManifestAndSchemaFileSizes(details);
            Assert.assertEquals(snapshotSize, details.computeTrueSizeBytes());
        }
        finally
        {
            store.clearSnapshot(snapshotName);
        }
    }

    @Test
    public void testSingleExpressionQueries() throws Exception
    {
        testSingleExpressionQueries(false);
        cleanupData();
        testSingleExpressionQueries(true);
    }

    private void testSingleExpressionQueries(boolean forceFlush)
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key1", Pair.create("Pavel", 14));
            put("key2", Pair.create("Pavel", 26));
            put("key3", Pair.create("Pavel", 27));
            put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRows(rows, "key1", "key2", "key3", "key4");

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("av")));
        assertRows(rows, "key1", "key2", "key3");

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("as")));
        assertRows(rows, "key4");

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("aw")));
        assertRowsSize(rows, 0);

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("avel")));
        assertRows(rows, "key1", "key2", "key3");

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("n")));
        assertRows(rows, "key4");

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(27)));
        assertRows(rows, "key3", "key4");

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(26)));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(13)));
        assertRowsSize(rows, 0);
    }

    @Test
    public void testEmptyTokenizedResults()
    {
        testEmptyTokenizedResults(false);
        cleanupData();
        testEmptyTokenizedResults(true);
    }

    private void testEmptyTokenizedResults(boolean forceFlush)
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("  ", 14));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        Set<String> rows= getIndexed(store, 10, buildExpression(UTF8Type.instance.decompose("first_name"), Operator.LIKE_MATCHES, UTF8Type.instance.decompose("doesntmatter")));
        assertRows(rows);
    }

    @Test
    public void testMultiExpressionQueries()
    {
        testMultiExpressionQueries(false);
        cleanupData();
        testMultiExpressionQueries(true);
    }

    public void testMultiExpressionQueries(boolean forceFlush)
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows;
        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(14)));
        assertRows(rows, "key2", "key3", "key4");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(27)));
        assertRows(rows, "key1", "key2");

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GT, Int32Type.instance.decompose(14)),
                         buildExpression(age, Operator.LT, Int32Type.instance.decompose(27)));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GT, Int32Type.instance.decompose(12)));
        assertRows(rows, "key1", "key2", "key3", "key4");

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GTE, Int32Type.instance.decompose(13)));
        assertRows(rows, "key1", "key2", "key3", "key4");

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GTE, Int32Type.instance.decompose(16)));
        assertRows(rows, "key2", "key3", "key4");


        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.LT, Int32Type.instance.decompose(30)));
        assertRows(rows, "key1", "key2", "key3", "key4");

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.LTE, Int32Type.instance.decompose(29)));
        assertRows(rows, "key1", "key2", "key3", "key4");

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.LTE, Int32Type.instance.decompose(25)));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("avel")),
                                     buildExpression(age, Operator.LTE, Int32Type.instance.decompose(25)));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("n")),
                                     buildExpression(age, Operator.LTE, Int32Type.instance.decompose(25)));
        Assert.assertTrue(rows.isEmpty());

    }

    @Test
    public void testCrossSSTableQueries()
    {
        testCrossSSTableQueries(false);
        cleanupData();
        testCrossSSTableQueries(true);

    }

    private void testCrossSSTableQueries(boolean forceFlush)
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", 43));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create("Josephine", 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
            }};

        loadData(part1, forceFlush); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
            }};

        loadData(part2, forceFlush);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", 28));
            }};

        ColumnFamilyStore store = loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows;
        rows = getIndexed(store, 10, buildExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("Fiona")),
                                     buildExpression(age, Operator.LT, Int32Type.instance.decompose(40)));

        assertRows(rows, "key6");


        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        assertRows(rows, "key0", "key11", "key12", "key13", "key14", "key3", "key4", "key6", "key7", "key8" );

        rows = getIndexed(store, 5,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        assertRowsSize(rows, 5);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GTE, Int32Type.instance.decompose(35)));

        assertRows(rows, "key0", "key11", "key12", "key13", "key4", "key6", "key7");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        assertRows(rows, "key14", "key3", "key8");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(27)),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        assertRows(rows, "key14");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(10)));

        assertRowsSize(rows, 10);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(50)));

        assertRowsSize(rows, 10);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ie")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(43)));

        assertRows(rows, "key1", "key10");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("a")));

        assertRows(rows, "key12", "key13", "key3", "key4", "key6");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(33)));

        assertRows(rows, "key3");
    }

    @Test
    public void testQueriesThatShouldBeTokenized()
    {
        testQueriesThatShouldBeTokenized(false);
        cleanupData();
        testQueriesThatShouldBeTokenized(true);
    }

    private void testQueriesThatShouldBeTokenized(boolean forceFlush)
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("If you can dream it, you can do it.", 43));
                put("key1", Pair.create("What you get by achieving your goals is not " +
                        "as important as what you become by achieving your goals, do it.", 33));
                put("key2", Pair.create("Keep your face always toward the sunshine " +
                        "- and shadows will fall behind you.", 43));
                put("key3", Pair.create("We can't help everyone, but everyone can " +
                        "help someone.", 27));
            }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                buildExpression(firstName, Operator.LIKE_CONTAINS,
                        UTF8Type.instance.decompose("What you get by achieving your goals")),
                buildExpression(age, Operator.GT, Int32Type.instance.decompose(32)));

        assertRows(rows, "key1");

        rows = getIndexed(store, 10,
                buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("do it.")));

        assertRows(rows, "key0", "key1");
    }

    @Test
    public void testPrefixSearchWithContainsMode()
    {
        testPrefixSearchWithContainsMode(false);
        cleanupData();
        testPrefixSearchWithContainsMode(true);
    }

    private void testPrefixSearchWithContainsMode(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(FTS_CF_NAME);

        executeCQL(FTS_CF_NAME, "INSERT INTO %s.%s (song_id, title, artist) VALUES(?, ?, ?)", UUID.fromString("1a4abbcd-b5de-4c69-a578-31231e01ff09"), "Poker Face", "Lady Gaga");
        executeCQL(FTS_CF_NAME, "INSERT INTO %s.%s (song_id, title, artist) VALUES(?, ?, ?)", UUID.fromString("9472a394-359b-4a06-b1d5-b6afce590598"), "Forgetting the Way Home", "Our Lady of Bells");
        executeCQL(FTS_CF_NAME, "INSERT INTO %s.%s (song_id, title, artist) VALUES(?, ?, ?)", UUID.fromString("4f8dc18e-54e6-4e16-b507-c5324b61523b"), "Zamki na piasku", "Lady Pank");
        executeCQL(FTS_CF_NAME, "INSERT INTO %s.%s (song_id, title, artist) VALUES(?, ?, ?)", UUID.fromString("eaf294fa-bad5-49d4-8f08-35ba3636a706"), "Koncertowa", "Lady Pank");


        if (forceFlush)
            Util.flush(store);

        final UntypedResultSet results = executeCQL(FTS_CF_NAME, "SELECT * FROM %s.%s WHERE artist LIKE 'lady%%'");
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
    }

    @Test
    public void testMultiExpressionQueriesWhereRowSplitBetweenSSTables()
    {
        testMultiExpressionQueriesWhereRowSplitBetweenSSTables(false);
        cleanupData();
        testMultiExpressionQueriesWhereRowSplitBetweenSSTables(true);
    }

    private void testMultiExpressionQueriesWhereRowSplitBetweenSSTables(boolean forceFlush)
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create(null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        loadData(part1, forceFlush); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create(null, 28));
        }};

        loadData(part2, forceFlush);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create(null, 43));
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", -1));
                put("key2", Pair.create("Josephine", -1));
        }};

        ColumnFamilyStore store = loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      buildExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("Fiona")),
                                      buildExpression(age, Operator.LT, Int32Type.instance.decompose(40)));

        assertRows(rows, "key6");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        assertRows(rows, "key0", "key11", "key12", "key13", "key14", "key3", "key4", "key6", "key7", "key8");

        rows = getIndexed(store, 5,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        assertRowsSize(rows, 5);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GTE, Int32Type.instance.decompose(35)));

        assertRows(rows, "key0", "key11", "key12", "key13", "key4", "key6", "key7");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        assertRows(rows, "key14", "key3", "key8");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(27)),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        assertRows(rows, "key14");

        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key12", Pair.create(null, 12));
                put("key14", Pair.create("Demario", 42));
                put("key2", Pair.create("Frank", -1));
        }};

        store = loadData(part4, forceFlush);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("Susana")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(13)),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(10)));
        assertRows(rows, "key12");

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("Demario")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(30)));
        assertRowsSize(rows, 0);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("Josephine")));
        assertRowsSize(rows, 0);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(10)));

        assertRowsSize(rows, 10);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(50)));

        assertRowsSize(rows, 10);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ie")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(43)));

        assertRows(rows, "key0", "key1", "key10");
    }

    @Test
    public void testPagination()
    {
        testPagination(false);
        cleanupData();
        testPagination(true);
    }

    private void testPagination(boolean forceFlush)
    {
        // split data into 3 distinct SSTables to test paging with overlapping token intervals.

        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        loadData(part2, forceFlush);
        loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<DecoratedKey> uniqueKeys = getPaged(store, 4,
                buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                buildExpression(age, Operator.GTE, Int32Type.instance.decompose(21)));


        List<String> expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test a single equals condition

        uniqueKeys = getPaged(store, 4, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key18");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test something which is smaller than a single page
        uniqueKeys = getPaged(store, 4,
                              buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                              buildExpression(age, Operator.EQ, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key22");
                add("key08");
                add("key07");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // the same but with the page size of 2 to test minimal pagination windows

        uniqueKeys = getPaged(store, 2,
                              buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                              buildExpression(age, Operator.EQ, Int32Type.instance.decompose(36)));

        Assert.assertEquals(expected, convert(uniqueKeys));

        // and last but not least, test age range query with pagination
        uniqueKeys = getPaged(store, 4,
                buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                buildExpression(age, Operator.GT, Int32Type.instance.decompose(20)),
                buildExpression(age, Operator.LTE, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key08");
                add("key07");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        Set<String> rows;

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' limit 10 ALLOW FILTERING;", KS_NAME, CF_NAME));
        assertRows(rows, "key03", "key04", "key09", "key13", "key14", "key16", "key20", "key22", "key24", "key25");

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' and token(id) >= token('key14') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        assertRows(rows, "key03", "key04", "key14", "key16", "key24");

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' and token(id) >= token('key14') and token(id) <= token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        assertRows(rows, "key14", "key16", "key24");

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' and age > 30 and token(id) >= token('key14') and token(id) <= token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        assertRows(rows, "key14");

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name like '%%ie' limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        assertRows(rows, "key07", "key20", "key24");

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name like '%%ie' AND token(id) > token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        assertRows(rows, "key07", "key24");
    }

    @Test
    public void testColumnNamesWithSlashes()
    {
        testColumnNamesWithSlashes(false);
        cleanupData();
        testColumnNamesWithSlashes(true);
    }

    private void testColumnNamesWithSlashes(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        Mutation.PartitionUpdateCollector rm1 = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey(AsciiType.instance.decompose("key1")));
        rm1.add(PartitionUpdate.singleRowUpdate(store.metadata(),
                                                rm1.key(),
                                                buildRow(buildCell(store.metadata(),
                                                                   UTF8Type.instance.decompose("/data/output/id"),
                                                                   AsciiType.instance.decompose("jason"),
                                                                   1000))));

        Mutation.PartitionUpdateCollector rm2 = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey(AsciiType.instance.decompose("key2")));
        rm2.add(PartitionUpdate.singleRowUpdate(store.metadata(),
                                                rm2.key(),
                                                buildRow(buildCell(store.metadata(),
                                                                   UTF8Type.instance.decompose("/data/output/id"),
                                                                   AsciiType.instance.decompose("pavel"),
                                                                   2000))));

        Mutation.PartitionUpdateCollector rm3 = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey(AsciiType.instance.decompose("key3")));
        rm3.add(PartitionUpdate.singleRowUpdate(store.metadata(),
                                                rm3.key(),
                                                buildRow(buildCell(store.metadata(),
                                                                   UTF8Type.instance.decompose("/data/output/id"),
                                                                   AsciiType.instance.decompose("Aleksey"),
                                                                   3000))));

        rm1.build().apply();
        rm2.build().apply();
        rm3.build().apply();

        if (forceFlush)
            Util.flush(store);

        final ByteBuffer dataOutputId = UTF8Type.instance.decompose("/data/output/id");

        Set<String> rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRows(rows, "key1", "key2");

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("A")));
        assertRows(rows, "key3");

        // doesn't really make sense to rebuild index for in-memory data
        if (!forceFlush)
            return;

        store.indexManager.invalidateAllIndexesBlocking();

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        // now let's trigger index rebuild and check if we got the data back
        store.indexManager.rebuildIndexesBlocking(Sets.newHashSet(store.name + "_data_output_id"));

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRows(rows, "key1", "key2");

        // also let's try to build an index for column which has no data to make sure that doesn't fail
        store.indexManager.rebuildIndexesBlocking(Sets.newHashSet(store.name + "_first_name"));
        store.indexManager.rebuildIndexesBlocking(Sets.newHashSet(store.name + "_data_output_id"));

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRows(rows, "key1", "key2");

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("el")));
        assertRows(rows, "key2");
    }

    @Test
    public void testInvalidate()
    {
        testInvalidate(false);
        cleanupData();
        testInvalidate(true);
    }

    private void testInvalidate(boolean forceFlush)
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create(null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRows(rows, "key0", "key3", "key4");

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(33)));
        assertRows(rows, "key1");

        store.indexManager.invalidateAllIndexesBlocking();

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(33)));
        Assert.assertTrue(rows.toString(), rows.isEmpty());


        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Fred", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create("Dino", 28));
        }};

        loadData(part2, forceFlush);

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRows(rows, "key6", "key7");

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(40)));
        assertRows(rows, "key9");
    }

    @Test
    public void testIndexRedistribution() throws IOException
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key01", Pair.create("a", 33));
            put("key02", Pair.create("a", 41));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key03", Pair.create("a", 22));
            put("key04", Pair.create("a", 45));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key05", Pair.create("a", 32));
            put("key06", Pair.create("a", 38));
        }};

        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key07", Pair.create("a", 36));
            put("key08", Pair.create("a", 36));
        }};

        Map<String, Pair<String, Integer>> part5 = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key09", Pair.create("a", 21));
            put("key10", Pair.create("a", 35));
        }};

        ColumnFamilyStore store = loadData(part1, true);
        loadData(part2, true);
        loadData(part3, true);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Set<String> rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 6);

        loadData(part4, true);
        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 8);

        loadData(part5, true);

        int minIndexInterval = store.metadata().params.minIndexInterval;
        try
        {
            redistributeSummaries(10, store, firstName, minIndexInterval * 2);
            redistributeSummaries(10, store, firstName, minIndexInterval * 4);
            redistributeSummaries(10, store, firstName, minIndexInterval * 8);
            redistributeSummaries(10, store, firstName, minIndexInterval * 16);
        } finally
        {
            setMinIndexInterval(minIndexInterval);
        }
    }

    private void redistributeSummaries(int expected, ColumnFamilyStore store, ByteBuffer firstName, int minIndexInterval) throws IOException
    {
        setMinIndexInterval(minIndexInterval);
        IndexSummaryManager.instance.redistributeSummaries();
        Util.flush(store);

        Set<String> rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), expected, rows.size());
    }

    private void setMinIndexInterval(int minIndexInterval) {
        QueryProcessor.executeOnceInternal(String.format("ALTER TABLE %s.%s WITH min_index_interval = %d;", KS_NAME, CF_NAME, minIndexInterval));
    }

    @Test
    public void testTruncate()
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1, true, 2);

        loadData(part2, true, 4);
        loadData(part3, true, 6);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Set<String> rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 16);

        // make sure we don't prematurely delete anything
        store.indexManager.truncateAllIndexesBlocking(1);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 16);

        store.indexManager.truncateAllIndexesBlocking(3);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 10);

        store.indexManager.truncateAllIndexesBlocking(5);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 6);

        store.indexManager.truncateAllIndexesBlocking(7);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 0);

        // add back in some data just to make sure it all still works
        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key40", Pair.create("Tianna", 31));
                put("key41", Pair.create("Dennis", 32));
        }};

        loadData(part4, true, 8);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRowsSize(rows, 1);
    }

    @Test
    public void testConcurrentMemtableReadsAndWrites() throws Exception
    {
        final ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        ExecutorService scheduler = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        final int writeCount = 10000;
        final AtomicInteger updates = new AtomicInteger(0);

        for (int i = 0; i < writeCount; i++)
        {
            final String key = "key" + i;
            final String firstName = "first_name#" + i;
            final String lastName = "last_name#" + i;
            final long timestamp = 1000 + i;

            scheduler.submit(() -> {
                try
                {
                    newMutation(key, firstName, lastName, 26, timestamp).apply();
                    Uninterruptibles.sleepUninterruptibly(5, MILLISECONDS); // back up a bit to do more reads
                }
                finally
                {
                    updates.incrementAndGet();
                }
            });
        }

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        int previousCount = 0;

        try
        {
            do
            {
                // this loop figures out if number of search results monotonically increasing
                // to make sure that concurrent updates don't interfere with reads, uses first_name and age
                // indexes to test correctness of both Trie and SkipList ColumnIndex implementations.

                Set<DecoratedKey> rows = getPaged(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                                  buildExpression(age, Operator.EQ, Int32Type.instance.decompose(26)));

                Assert.assertTrue(previousCount <= rows.size());
                previousCount = rows.size();
            }
            while (updates.get() < writeCount);

            // to make sure that after all of the writes are done we can read all "count" worth of rows
            Set<DecoratedKey> rows = getPaged(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                            buildExpression(age, Operator.EQ, Int32Type.instance.decompose(26)));
            Assert.assertEquals(writeCount, rows.size());
        } finally {
            scheduler.shutdownNow();
            Assert.assertTrue(scheduler.awaitTermination(1, TimeUnit.MINUTES));
        }
    }

    @Test
    public void testSameKeyInMemtableAndSSTables()
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Map<String, Pair<String, Integer>> data1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data1, true);

        Map<String, Pair<String, Integer>> data2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 28));
        }};

        loadData(data2, true);

        Map<String, Pair<String, Integer>> data3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 15));
                put("key4", Pair.create("Jason", 29));
        }};

        loadData(data3, false);

        Set<String> rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        assertRows(rows, "key1", "key2", "key3", "key4");


        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                      buildExpression(age, Operator.EQ, Int32Type.instance.decompose(15)));

        assertRows(rows, "key1");

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                      buildExpression(age, Operator.EQ, Int32Type.instance.decompose(29)));

        assertRows(rows, "key4");

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                      buildExpression(age, Operator.EQ, Int32Type.instance.decompose(27)));

        assertRows(rows, "key2", "key3");
    }

    @Test
    public void testUnicodeSupport()
    {
        testUnicodeSupport(false);
        cleanupData();
        testUnicodeSupport(true);
    }

    private void testUnicodeSupport(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment");

        Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key1"));
        update(rm, comment, UTF8Type.instance.decompose("ⓈⓅⒺⒸⒾⒶⓁ ⒞⒣⒜⒭⒮ and normal ones"), 1000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key2"));
        update(rm, comment, UTF8Type.instance.decompose("龍馭鬱"), 2000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key3"));
        update(rm, comment, UTF8Type.instance.decompose("インディアナ"), 3000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key4"));
        update(rm, comment, UTF8Type.instance.decompose("レストラン"), 4000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key5"));
        update(rm, comment, UTF8Type.instance.decompose("ベンジャミン ウエスト"), 5000);
        rm.build().apply();

        if (forceFlush)
            Util.flush(store);

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ⓈⓅⒺⒸⒾ")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("normal")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("鬱")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("馭鬱")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍馭鬱")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ベンジャミン")));
        assertRows(rows, "key5");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("レストラ")));
        assertRows(rows, "key4");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("インディ")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ベンジャミ")));
        assertRows(rows, "key5");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ン")));
        assertRows(rows, "key4", "key5");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("レストラン")));
        assertRows(rows, "key4");
    }

    @Test
    public void testUnicodeSuffixModeNoSplits()
    {
        testUnicodeSuffixModeNoSplits(false);
        cleanupData();
        testUnicodeSuffixModeNoSplits(true);
    }

    private void testUnicodeSuffixModeNoSplits(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment_suffix_split");

        Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key1"));
        update(rm, comment, UTF8Type.instance.decompose("龍馭鬱"), 1000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key2"));
        update(rm, comment, UTF8Type.instance.decompose("インディアナ"), 2000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key3"));
        update(rm, comment, UTF8Type.instance.decompose("レストラン"), 3000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key4"));
        update(rm, comment, UTF8Type.instance.decompose("ベンジャミン ウエスト"), 4000);
        rm.build().apply();

        if (forceFlush)
            Util.flush(store);

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("鬱")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("馭鬱")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍馭鬱")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ベンジャミン")));
        assertRows(rows, "key4");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("トラン")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ディア")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ジャミン")));
        assertRows(rows, "key4");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ン")));
        assertRows(rows, "key2", "key3", "key4");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ン")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("ベンジャミン ウエスト")));
        assertRows(rows, "key4");
    }

    @Test
    public void testThatTooBigValueIsRejected()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment_suffix_split");

        for (int i = 0; i < 10; i++)
        {
            byte[] randomBytes = new byte[ThreadLocalRandom.current().nextInt(OnDiskIndexBuilder.MAX_TERM_SIZE, 5 * OnDiskIndexBuilder.MAX_TERM_SIZE)];
            ThreadLocalRandom.current().nextBytes(randomBytes);

            final ByteBuffer bigValue = UTF8Type.instance.decompose(new String(randomBytes));

            Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key1"));
            update(rm, comment, bigValue, 1000 + i);
            rm.build().apply();

            Set<String> rows;

            rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, bigValue.duplicate()));
            Assert.assertEquals(0, rows.size());

            Util.flush(store);

            rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, bigValue.duplicate()));
            Assert.assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSearchTimeouts()
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Map<String, Pair<String, Integer>> data1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data1, true);

        RowFilter filter = RowFilter.create();
        filter.add(store.metadata().getColumn(firstName), Operator.LIKE_CONTAINS, AsciiType.instance.fromString("a"));

        ReadCommand command =
            PartitionRangeReadCommand.create(store.metadata(),
                                             FBUtilities.nowInSeconds(),
                                             ColumnFilter.all(store.metadata()),
                                             filter,
                                             DataLimits.NONE,
                                             DataRange.allData(store.metadata().partitioner));
        try
        {
            new SASIIndexSearcher(store, command, 0).search(ReadExecutionController.empty());
            Assert.fail();
        }
        catch (TimeQuotaExceededException e)
        {
            // correct behavior
        }
        catch (Exception e)
        {
            Assert.fail();
            e.printStackTrace();
        }

        // to make sure that query doesn't fail in normal conditions

        try (ReadExecutionController controller = command.executionController())
        {
            Set<String> rows = getKeys(new SASIIndexSearcher(store, command, DatabaseDescriptor.getRangeRpcTimeout(MILLISECONDS)).search(controller));
            assertRows(rows, "key1", "key2", "key3", "key4");
        }
    }

    @Test
    public void testLowerCaseAnalyzer()
    {
        testLowerCaseAnalyzer(false);
        cleanupData();
        testLowerCaseAnalyzer(true);
    }

    @Test
    public void testChinesePrefixSearch()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer fullName = UTF8Type.instance.decompose("/output/full-name/");

        Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key1"));
        update(rm, fullName, UTF8Type.instance.decompose("美加 八田"), 1000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key2"));
        update(rm, fullName, UTF8Type.instance.decompose("仁美 瀧澤"), 2000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key3"));
        update(rm, fullName, UTF8Type.instance.decompose("晃宏 高須"), 3000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key4"));
        update(rm, fullName, UTF8Type.instance.decompose("弘孝 大竹"), 4000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key5"));
        update(rm, fullName, UTF8Type.instance.decompose("満枝 榎本"), 5000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key6"));
        update(rm, fullName, UTF8Type.instance.decompose("飛鳥 上原"), 6000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key7"));
        update(rm, fullName, UTF8Type.instance.decompose("大輝 鎌田"), 7000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key8"));
        update(rm, fullName, UTF8Type.instance.decompose("利久 寺地"), 8000);
        rm.build().apply();

        Util.flush(store);


        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.EQ, UTF8Type.instance.decompose("美加 八田")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("美加")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.EQ, UTF8Type.instance.decompose("晃宏 高須")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("大輝")));
        assertRows(rows, "key7");
    }

    public void testLowerCaseAnalyzer(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("address");

        Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key1"));
        update(rm, comment, UTF8Type.instance.decompose("577 Rogahn Valleys Apt. 178"), 1000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key2"));
        update(rm, comment, UTF8Type.instance.decompose("89809 Beverly Course Suite 089"), 2000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key3"));
        update(rm, comment, UTF8Type.instance.decompose("165 clydie oval apt. 399"), 3000);
        rm.build().apply();

        if (forceFlush)
            Util.flush(store);

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 Rogahn Valleys")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 ROgAhn VallEYs")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 rogahn valleys")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 rogahn")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("57")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 Beverly Course")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 BEVERly COURSE")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 beverly course")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 Beverly")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("8980")));
        assertRows(rows, "key2");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 ClYdie OvAl APT. 399")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 Clydie Oval Apt. 399")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 clydie oval apt. 399")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 ClYdie OvA")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 ClYdi")));
        assertRows(rows, "key3");

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165")));
        assertRows(rows, "key3");
    }

    @Test
    public void testPrefixSSTableLookup()
    {
        // This test coverts particular case which interval lookup can return invalid results
        // when queried on the prefix e.g. "j".
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer name = UTF8Type.instance.decompose("first_name_prefix");

        Mutation.PartitionUpdateCollector rm;

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key1"));
        update(rm, name, UTF8Type.instance.decompose("Pavel"), 1000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key2"));
        update(rm, name, UTF8Type.instance.decompose("Jordan"), 2000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key3"));
        update(rm, name, UTF8Type.instance.decompose("Mikhail"), 3000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key4"));
        update(rm, name, UTF8Type.instance.decompose("Michael"), 4000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key5"));
        update(rm, name, UTF8Type.instance.decompose("Johnny"), 5000);
        rm.build().apply();

        // first flush would make interval for name - 'johnny' -> 'pavel'
        Util.flush(store);

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key6"));
        update(rm, name, UTF8Type.instance.decompose("Jason"), 6000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key7"));
        update(rm, name, UTF8Type.instance.decompose("Vijay"), 7000);
        rm.build().apply();

        rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey("key8")); // this name is going to be tokenized
        update(rm, name, UTF8Type.instance.decompose("Jean-Claude"), 8000);
        rm.build().apply();

        // this flush is going to produce range - 'jason' -> 'vijay'
        Util.flush(store);

        // make sure that overlap of the prefixes is properly handled across sstables
        // since simple interval tree lookup is not going to cover it, prefix lookup actually required.

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("J")));
        assertRows(rows, "key2", "key5", "key6", "key8");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("j")));
        assertRows(rows, "key2", "key5", "key6", "key8");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("m")));
        assertRows(rows, "key3", "key4");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("v")));
        assertRows(rows, "key7");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("p")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("j")),
                                     buildExpression(name, Operator.NEQ, UTF8Type.instance.decompose("joh")));
        assertRows(rows, "key2", "key6", "key8");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("pavel")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Pave")));
        Assert.assertTrue(rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Pavel")));
        assertRows(rows, "key1");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("JeAn")));
        assertRows(rows, "key8");

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("claUde")));
        assertRows(rows, "key8");

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Jean")));
        Assert.assertTrue(rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Jean-Claude")));
        assertRows(rows, "key8");
    }

    @Test
    public void testSettingIsLiteralOption()
    {

        // special type which is UTF-8 but is only on the inside
        AbstractType<?> stringType = new AbstractType<String>(AbstractType.ComparisonType.CUSTOM)
        {
            public ByteBuffer fromString(String source) throws MarshalException
            {
                return UTF8Type.instance.fromString(source);
            }

            public Term fromJSONObject(Object parsed) throws MarshalException
            {
                throw new UnsupportedOperationException();
            }

            public TypeSerializer<String> getSerializer()
            {
                return UTF8Type.instance.getSerializer();
            }

            public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
            {
                return UTF8Type.instance.compare(left, accessorL, right, accessorR);
            }
        };

        // first let's check that we get 'false' for 'isLiteral' if we don't set the option with special comparator
        ColumnMetadata columnA = ColumnMetadata.regularColumn(KS_NAME, CF_NAME, "special-A", stringType);

        ColumnIndex indexA = new ColumnIndex(UTF8Type.instance, columnA, IndexMetadata.fromSchemaMetadata("special-index-A", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
        }}));

        Assert.assertTrue(indexA.isIndexed());
        Assert.assertFalse(indexA.isLiteral());

        // now let's double-check that we do get 'true' when we set it
        ColumnMetadata columnB = ColumnMetadata.regularColumn(KS_NAME, CF_NAME, "special-B", stringType);

        ColumnIndex indexB = new ColumnIndex(UTF8Type.instance, columnB, IndexMetadata.fromSchemaMetadata("special-index-B", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put("is_literal", "true");
        }}));

        Assert.assertTrue(indexB.isIndexed());
        Assert.assertTrue(indexB.isLiteral());

        // and finally we should also get a 'true' if it's built-in UTF-8/ASCII comparator
        ColumnMetadata columnC = ColumnMetadata.regularColumn(KS_NAME, CF_NAME, "special-C", UTF8Type.instance);

        ColumnIndex indexC = new ColumnIndex(UTF8Type.instance, columnC, IndexMetadata.fromSchemaMetadata("special-index-C", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
        }}));

        Assert.assertTrue(indexC.isIndexed());
        Assert.assertTrue(indexC.isLiteral());

        ColumnMetadata columnD = ColumnMetadata.regularColumn(KS_NAME, CF_NAME, "special-D", AsciiType.instance);

        ColumnIndex indexD = new ColumnIndex(UTF8Type.instance, columnD, IndexMetadata.fromSchemaMetadata("special-index-D", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
        }}));

        Assert.assertTrue(indexD.isIndexed());
        Assert.assertTrue(indexD.isLiteral());

        // and option should supersedes the comparator type
        ColumnMetadata columnE = ColumnMetadata.regularColumn(KS_NAME, CF_NAME, "special-E", UTF8Type.instance);

        ColumnIndex indexE = new ColumnIndex(UTF8Type.instance, columnE, IndexMetadata.fromSchemaMetadata("special-index-E", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put("is_literal", "false");
        }}));

        Assert.assertTrue(indexE.isIndexed());
        Assert.assertFalse(indexE.isLiteral());

        // test frozen-collection
        ColumnMetadata columnF = ColumnMetadata.regularColumn(KS_NAME,
                                                              CF_NAME,
                                                              "special-F",
                                                              ListType.getInstance(UTF8Type.instance, false));

        ColumnIndex indexF = new ColumnIndex(UTF8Type.instance, columnF, IndexMetadata.fromSchemaMetadata("special-index-F", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
        }}));

        Assert.assertTrue(indexF.isIndexed());
        Assert.assertFalse(indexF.isLiteral());
    }

    @Test
    public void testClusteringIndexes()
    {
        testClusteringIndexes(false);
        cleanupData();
        testClusteringIndexes(true);
    }

    public void testClusteringIndexes(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME_1);

        executeCQL(CLUSTERING_CF_NAME_1, "INSERT INTO %s.%s (name, nickname, location, age, height, score) VALUES (?, ?, ?, ?, ?, ?)", "Pavel", "xedin", "US", 27, 183, 1.0);
        executeCQL(CLUSTERING_CF_NAME_1, "INSERT INTO %s.%s (name, nickname, location, age, height, score) VALUES (?, ?, ?, ?, ?, ?)", "Pavel", "xedin", "BY", 28, 182, 2.0);
        executeCQL(CLUSTERING_CF_NAME_1 ,"INSERT INTO %s.%s (name, nickname, location, age, height, score) VALUES (?, ?, ?, ?, ?, ?)", "Jordan", "jrwest", "US", 27, 182, 1.0);

        if (forceFlush)
            Util.flush(store);

        UntypedResultSet results;

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location = ? ALLOW FILTERING", "US");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE age >= ? AND height = ? ALLOW FILTERING", 27, 182);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE age = ? AND height = ? ALLOW FILTERING", 28, 182);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE age >= ? AND height = ? AND score >= ? ALLOW FILTERING", 27, 182, 1.0);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE age >= ? AND height = ? AND score = ? ALLOW FILTERING", 27, 182, 1.0);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location = ? AND age >= ? ALLOW FILTERING", "US", 27);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location = ? ALLOW FILTERING", "BY");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location LIKE 'U%%' ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location LIKE 'U%%' AND height >= 183 ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location LIKE 'US%%' ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location LIKE 'US' ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        try
        {
            executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location LIKE '%%U' ALLOW FILTERING");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage().contains("only supported"));
            // expected
        }

        try
        {
            executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location LIKE '%%' ALLOW FILTERING");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage().contains("empty"));
            // expected
        }

        try
        {
            executeCQL(CLUSTERING_CF_NAME_1 ,"SELECT * FROM %s.%s WHERE location LIKE '%%%%' ALLOW FILTERING");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage().contains("empty"));
            // expected
        }

        // check restrictions on non-indexed clustering columns when preceding columns are indexed
        store = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME_2);
        executeCQL(CLUSTERING_CF_NAME_2 ,"INSERT INTO %s.%s (name, nickname, location, age, height, score) VALUES (?, ?, ?, ?, ?, ?)", "Tony", "tony", "US", 43, 184, 2.0);
        executeCQL(CLUSTERING_CF_NAME_2 ,"INSERT INTO %s.%s (name, nickname, location, age, height, score) VALUES (?, ?, ?, ?, ?, ?)", "Christopher", "chis", "US", 27, 180, 1.0);

        if (forceFlush)
            Util.flush(store);

        results = executeCQL(CLUSTERING_CF_NAME_2 ,"SELECT * FROM %s.%s WHERE location LIKE 'US' AND age = 43 ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Tony", results.one().getString("name"));
    }

    @Test
    public void testStaticIndex()
    {
        testStaticIndex(false);
        cleanupData();
        testStaticIndex(true);
    }

    public void testStaticIndex(boolean shouldFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(STATIC_CF_NAME);

        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,sensor_type) VALUES(?, ?)", 1, "TEMPERATURE");
        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,date,value,variance) VALUES(?, ?, ?, ?)", 1, 20160401L, 24.46, 2);
        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,date,value,variance) VALUES(?, ?, ?, ?)", 1, 20160402L, 25.62, 5);
        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,date,value,variance) VALUES(?, ?, ?, ?)", 1, 20160403L, 24.96, 4);

        if (shouldFlush)
            Util.flush(store);

        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,sensor_type) VALUES(?, ?)", 2, "PRESSURE");
        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,date,value,variance) VALUES(?, ?, ?, ?)", 2, 20160401L, 1.03, 9);
        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,date,value,variance) VALUES(?, ?, ?, ?)", 2, 20160402L, 1.04, 7);
        executeCQL(STATIC_CF_NAME, "INSERT INTO %s.%s (sensor_id,date,value,variance) VALUES(?, ?, ?, ?)", 2, 20160403L, 1.01, 4);

        if (shouldFlush)
            Util.flush(store);

        UntypedResultSet results;

        // Prefix search on static column only
        results = executeCQL(STATIC_CF_NAME ,"SELECT * FROM %s.%s WHERE sensor_type LIKE 'temp%%'");
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        Iterator<UntypedResultSet.Row> iterator = results.iterator();

        UntypedResultSet.Row row1 = iterator.next();
        Assert.assertEquals(20160401L, row1.getLong("date"));
        Assert.assertEquals(24.46, row1.getDouble("value"), 0.0);
        Assert.assertEquals(2, row1.getInt("variance"));


        UntypedResultSet.Row row2 = iterator.next();
        Assert.assertEquals(20160402L, row2.getLong("date"));
        Assert.assertEquals(25.62, row2.getDouble("value"), 0.0);
        Assert.assertEquals(5, row2.getInt("variance"));

        UntypedResultSet.Row row3 = iterator.next();
        Assert.assertEquals(20160403L, row3.getLong("date"));
        Assert.assertEquals(24.96, row3.getDouble("value"), 0.0);
        Assert.assertEquals(4, row3.getInt("variance"));


        // Combined static and non static filtering
        results = executeCQL(STATIC_CF_NAME ,"SELECT * FROM %s.%s WHERE sensor_type=? AND value >= ? AND value <= ? AND variance=? ALLOW FILTERING",
                             "pressure", 1.02, 1.05, 7);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        row1 = results.one();
        Assert.assertEquals(20160402L, row1.getLong("date"));
        Assert.assertEquals(1.04, row1.getDouble("value"), 0.0);
        Assert.assertEquals(7, row1.getInt("variance"));

        // Only non statc columns filtering
        results = executeCQL(STATIC_CF_NAME ,"SELECT * FROM %s.%s WHERE value >= ? AND variance <= ? ALLOW FILTERING", 1.02, 7);
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());

        iterator = results.iterator();

        row1 = iterator.next();
        Assert.assertEquals("TEMPERATURE", row1.getString("sensor_type"));
        Assert.assertEquals(20160401L, row1.getLong("date"));
        Assert.assertEquals(24.46, row1.getDouble("value"), 0.0);
        Assert.assertEquals(2, row1.getInt("variance"));


        row2 = iterator.next();
        Assert.assertEquals("TEMPERATURE", row2.getString("sensor_type"));
        Assert.assertEquals(20160402L, row2.getLong("date"));
        Assert.assertEquals(25.62, row2.getDouble("value"), 0.0);
        Assert.assertEquals(5, row2.getInt("variance"));

        row3 = iterator.next();
        Assert.assertEquals("TEMPERATURE", row3.getString("sensor_type"));
        Assert.assertEquals(20160403L, row3.getLong("date"));
        Assert.assertEquals(24.96, row3.getDouble("value"), 0.0);
        Assert.assertEquals(4, row3.getInt("variance"));

        UntypedResultSet.Row row4 = iterator.next();
        Assert.assertEquals("PRESSURE", row4.getString("sensor_type"));
        Assert.assertEquals(20160402L, row4.getLong("date"));
        Assert.assertEquals(1.04, row4.getDouble("value"), 0.0);
        Assert.assertEquals(7, row4.getInt("variance"));
    }

    @Test
    public void testTableRebuild() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME_1);

        executeCQL(CLUSTERING_CF_NAME_1, "INSERT INTO %s.%s (name, nickname, location, age, height, score) VALUES (?, ?, ?, ?, ?, ?)", "Pavel", "xedin", "US", 27, 183, 1.0);
        executeCQL(CLUSTERING_CF_NAME_1, "INSERT INTO %s.%s (name, location, age, height, score) VALUES (?, ?, ?, ?, ?)", "Pavel", "BY", 28, 182, 2.0);
        executeCQL(CLUSTERING_CF_NAME_1, "INSERT INTO %s.%s (name, nickname, location, age, height, score) VALUES (?, ?, ?, ?, ?, ?)", "Jordan", "jrwest", "US", 27, 182, 1.0);

        Util.flush(store);

        SSTable ssTable = store.getSSTables(SSTableSet.LIVE).iterator().next();
        Path path = FileSystems.getDefault().getPath(ssTable.getFilename().replace("-Data", "-SI_" + CLUSTERING_CF_NAME_1 + "_age"));

        // Overwrite index file with garbage
        try(FileChannel fc = FileChannel.open(path, StandardOpenOption.WRITE))
        {
            fc.truncate(8).write(ByteBuffer.wrap("grabage".getBytes(StandardCharsets.UTF_8)));
        }

        long size1 = Files.readAttributes(path, BasicFileAttributes.class).size();

        // Trying to query the corrupted index file yields no results
        Assert.assertTrue(executeCQL(CLUSTERING_CF_NAME_1, "SELECT * FROM %s.%s WHERE age = 27 AND name = 'Pavel'").isEmpty());

        // Rebuld index
        store.rebuildSecondaryIndex(CLUSTERING_CF_NAME_1 + "_age");

        long size2 = Files.readAttributes(path, BasicFileAttributes.class).size();
        // Make sure that garbage was overwriten
        Assert.assertTrue(size2 > size1);

        // Make sure that indexes work for rebuit tables
        CQLTester.assertRows(executeCQL(CLUSTERING_CF_NAME_1, "SELECT * FROM %s.%s WHERE age = 27 AND name = 'Pavel'"),
                             CQLTester.row("Pavel", "US", 27, "xedin", 183, 1.0));
        CQLTester.assertRows(executeCQL(CLUSTERING_CF_NAME_1, "SELECT * FROM %s.%s WHERE age = 28"),
                             CQLTester.row("Pavel", "BY", 28, "xedin", 182, 2.0));
        CQLTester.assertRows(executeCQL(CLUSTERING_CF_NAME_1, "SELECT * FROM %s.%s WHERE score < 2.0 AND nickname = 'jrwest' ALLOW FILTERING"),
                             CQLTester.row("Jordan", "US", 27, "jrwest", 182, 1.0));
    }

    @Test
    public void testIndexRebuild()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME_1);

        executeCQL(CLUSTERING_CF_NAME_1, "INSERT INTO %s.%s (name, nickname) VALUES (?, ?)", "Alex", "ifesdjeen");

        Util.flush(store);

        for (Index index : store.indexManager.listIndexes())
        {
            SASIIndex idx = (SASIIndex) index;
            Assert.assertFalse(idx.getIndex().init(store.getLiveSSTables()).iterator().hasNext());
        }
    }

    @Test
    public void testInvalidIndexOptions()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        try
        {
            // unsupported partition key column
            SASIIndex.validateOptions(Collections.singletonMap("target", "id"), store.metadata());
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("partition key columns are not yet supported by SASI"));
        }

        try
        {
            // invalid index mode
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "address"); put("mode", "NORMAL"); }},
                                      store.metadata());
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("Incorrect index mode"));
        }

        try
        {
            // invalid SPARSE on the literal index
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "address"); put("mode", "SPARSE"); }},
                                      store.metadata());
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("non-literal"));
        }

        try
        {
            // invalid SPARSE on the explicitly literal index
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "height"); put("mode", "SPARSE"); put("is_literal", "true"); }},
                                      store.metadata());
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("non-literal"));
        }

        try
        {
            //  SPARSE with analyzer
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "height"); put("mode", "SPARSE"); put("analyzed", "true"); }},
                                      store.metadata());
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("doesn't support analyzers"));
        }
    }

    @Test
    public void testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes()
    {
        String containsTable = "sasi_like_contains_test";
        String prefixTable = "sasi_like_prefix_test";
        String analyzedPrefixTable = "sasi_like_analyzed_prefix_test";
        String tokenizedContainsTable = "sasi_like_analyzed_contains_test";

        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, containsTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, prefixTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, analyzedPrefixTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, tokenizedContainsTable));

        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s(v) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'CONTAINS', " +
                                                         "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', " +
                                                         "'case_sensitive': 'false' };",
                                                         KS_NAME, containsTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s(v) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'PREFIX' };", KS_NAME, prefixTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s(v) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'PREFIX', 'analyzed': 'true' };", KS_NAME, analyzedPrefixTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s(v) " +
                                                         "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = " +
                                                         "{ 'mode' : 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                                                         "'analyzed': 'true', 'tokenization_enable_stemming': 'true', 'tokenization_normalize_lowercase': 'true', " +
                                                         "'tokenization_locale': 'en' };",
                                                         KS_NAME, tokenizedContainsTable));

        testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes(containsTable, prefixTable, analyzedPrefixTable, tokenizedContainsTable, false);
        testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes(containsTable, prefixTable, analyzedPrefixTable, tokenizedContainsTable, true);
    }

    private void testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes(String containsTable,
                                                                    String prefixTable,
                                                                    String analyzedPrefixTable,
                                                                    String tokenizedContainsTable,
                                                                    boolean forceFlush)
    {
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?);", KS_NAME, containsTable), 0, "Pavel");
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?);", KS_NAME, prefixTable), 0, "Jean-Claude");
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?);", KS_NAME, analyzedPrefixTable), 0, "Jean-Claude");
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?);", KS_NAME, tokenizedContainsTable), 0, "Pavel");

        if (forceFlush)
        {
            Keyspace keyspace = Keyspace.open(KS_NAME);
            for (String table : Arrays.asList(containsTable, prefixTable, analyzedPrefixTable))
                Util.flushTable(keyspace, table);
        }

        UntypedResultSet results;

        // CONTAINS

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pav';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pav%%';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pavel';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Pav';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Pavel';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Pav';", KS_NAME, tokenizedContainsTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since CONTAINS + analyzed indexes only support LIKE
        }

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pav%%';", KS_NAME, tokenizedContainsTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since CONTAINS + analyzed only support LIKE
        }

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pav%%';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Pav';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Pav%%';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        // PREFIX

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Jean';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Jean-Claude';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jea';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jea%%';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Jea';", KS_NAME, prefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%'
        }

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Jea%%';", KS_NAME, prefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%'
        }

        // PREFIX + analyzer

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Jean';", KS_NAME, analyzedPrefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support EQ without tokenization
        }

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jean';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Claude';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jean-Claude';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jean%%';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Claude%%';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Jean';", KS_NAME, analyzedPrefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%' and LIKE '<term>'
        }

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Claude%%';", KS_NAME, analyzedPrefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%' and LIKE '<term>'
        }

        for (String table : Arrays.asList(containsTable, prefixTable, analyzedPrefixTable))
            QueryProcessor.executeOnceInternal(String.format("TRUNCATE TABLE %s.%s", KS_NAME, table));
    }

    @Test
    public void testConditionalsWithReversedType()
    {
        final String TABLE_NAME = "reversed_clustering";

        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk text, ck int, v int, PRIMARY KEY (pk, ck)) " +
                                                         "WITH CLUSTERING ORDER BY (ck DESC);", KS_NAME, TABLE_NAME));
        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX ON %s.%s (ck) USING 'org.apache.cassandra.index.sasi.SASIIndex'", KS_NAME, TABLE_NAME));
        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX ON %s.%s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex'", KS_NAME, TABLE_NAME));

        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES ('Alex', 1, 1);", KS_NAME, TABLE_NAME));
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES ('Alex', 2, 2);", KS_NAME, TABLE_NAME));
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES ('Alex', 3, 3);", KS_NAME, TABLE_NAME));
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES ('Tom', 1, 1);", KS_NAME, TABLE_NAME));
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES ('Tom', 2, 2);", KS_NAME, TABLE_NAME));
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES ('Tom', 3, 3);", KS_NAME, TABLE_NAME));

        UntypedResultSet resultSet = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE ck <= 2;", KS_NAME, TABLE_NAME));

        CQLTester.assertRowsIgnoringOrder(resultSet,
                                          CQLTester.row("Alex", 1, 1),
                                          CQLTester.row("Alex", 2, 2),
                                          CQLTester.row("Tom", 1, 1),
                                          CQLTester.row("Tom", 2, 2));

        resultSet = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE ck <= 2 AND v > 1 ALLOW FILTERING;", KS_NAME, TABLE_NAME));

        CQLTester.assertRowsIgnoringOrder(resultSet,
                                          CQLTester.row("Alex", 2, 2),
                                          CQLTester.row("Tom", 2, 2));

        resultSet = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE ck < 2;", KS_NAME, TABLE_NAME));
        CQLTester.assertRowsIgnoringOrder(resultSet,
                                          CQLTester.row("Alex", 1, 1),
                                          CQLTester.row("Tom", 1, 1));

        resultSet = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE ck >= 2;", KS_NAME, TABLE_NAME));
        CQLTester.assertRowsIgnoringOrder(resultSet,
                                          CQLTester.row("Alex", 2, 2),
                                          CQLTester.row("Alex", 3, 3),
                                          CQLTester.row("Tom", 2, 2),
                                          CQLTester.row("Tom", 3, 3));

        resultSet = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE ck >= 2 AND v < 3 ALLOW FILTERING;", KS_NAME, TABLE_NAME));
        CQLTester.assertRowsIgnoringOrder(resultSet,
                                          CQLTester.row("Alex", 2, 2),
                                          CQLTester.row("Tom", 2, 2));

        resultSet = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE ck > 2;", KS_NAME, TABLE_NAME));
        CQLTester.assertRowsIgnoringOrder(resultSet,
                                          CQLTester.row("Alex", 3, 3),
                                          CQLTester.row("Tom", 3, 3));
    }

    @Test
    public void testIndexMemtableSwitching() throws Exception
    {
        // write some data but don't flush
        ColumnFamilyStore store = loadData(new HashMap<String, Pair<String, Integer>>()
        {{
            put("key1", Pair.create("Pavel", 14));
        }}, false);

        ColumnIndex index = ((SASIIndex) store.indexManager.getIndexByName(store.name + "_first_name")).getIndex();
        IndexMemtable beforeFlushMemtable = index.getCurrentMemtable();

        PartitionRangeReadCommand command =
            PartitionRangeReadCommand.create(store.metadata(),
                                             FBUtilities.nowInSeconds(),
                                             ColumnFilter.all(store.metadata()),
                                             RowFilter.none(),
                                             DataLimits.NONE,
                                             DataRange.allData(store.getPartitioner()));

        QueryController controller = new QueryController(store, command, Integer.MAX_VALUE);
        org.apache.cassandra.index.sasi.plan.Expression expression =
                new org.apache.cassandra.index.sasi.plan.Expression(controller, index)
                                                    .add(Operator.LIKE_MATCHES, UTF8Type.instance.fromString("Pavel"));

        Assert.assertTrue(rangesSize(beforeFlushMemtable, expression) > 0);

        Util.flush(store);

        IndexMemtable afterFlushMemtable = index.getCurrentMemtable();

        Assert.assertNotSame(afterFlushMemtable, beforeFlushMemtable);
        Assert.assertEquals(rangesSize(afterFlushMemtable, expression), 0);
        Assert.assertEquals(0, index.getPendingMemtables().size());

        loadData(new HashMap<String, Pair<String, Integer>>()
        {{
            put("key2", Pair.create("Sam", 15));
        }}, false);

        expression = new org.apache.cassandra.index.sasi.plan.Expression(controller, index)
                        .add(Operator.LIKE_MATCHES, UTF8Type.instance.fromString("Sam"));

        beforeFlushMemtable = index.getCurrentMemtable();
        Assert.assertTrue(rangesSize(beforeFlushMemtable, expression) > 0);

        // let's emulate switching memtable and see if we can still read-data in "pending"
        index.switchMemtable(store.getTracker().getView().getCurrentMemtable());

        Assert.assertNotSame(index.getCurrentMemtable(), beforeFlushMemtable);
        Assert.assertEquals(1, index.getPendingMemtables().size());

        Assert.assertTrue(rangesSize(index, expression) > 0);

        // emulate "everything is flushed" notification
        index.discardMemtable(store.getTracker().getView().getCurrentMemtable());

        Assert.assertEquals(0, index.getPendingMemtables().size());
        Assert.assertEquals(rangesSize(index, expression), 0);

        // test discarding data from memtable
        loadData(new HashMap<String, Pair<String, Integer>>()
        {{
            put("key3", Pair.create("Jonathan", 16));
        }}, false);

        expression = new org.apache.cassandra.index.sasi.plan.Expression(controller, index)
                .add(Operator.LIKE_MATCHES, UTF8Type.instance.fromString("Jonathan"));

        Assert.assertTrue(rangesSize(index, expression) > 0);

        index.switchMemtable();
        Assert.assertEquals(rangesSize(index, expression), 0);
    }

    private static long rangesSize(IndexMemtable index, org.apache.cassandra.index.sasi.plan.Expression expression) throws IOException
    {
        try (RangeIterator<Long, Token> ranges = index.search(expression))
        {
            return ranges.getCount();
        }
    }

    private static long rangesSize(ColumnIndex index, org.apache.cassandra.index.sasi.plan.Expression expression) throws IOException
    {
        try (RangeIterator<Long, Token> ranges = index.searchMemtable(expression))
        {
            return ranges.getCount();
        }
    }

    @Test
    public void testAnalyzerValidation()
    {
        final String TABLE_NAME = "analyzer_validation";
        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE %s.%s (" +
                                                         "  pk text PRIMARY KEY, " +
                                                         "  ascii_v ascii, " +
                                                         "  bigint_v bigint, " +
                                                         "  blob_v blob, " +
                                                         "  boolean_v boolean, " +
                                                         "  date_v date, " +
                                                         "  decimal_v decimal, " +
                                                         "  double_v double, " +
                                                         "  float_v float, " +
                                                         "  inet_v inet, " +
                                                         "  int_v int, " +
                                                         "  smallint_v smallint, " +
                                                         "  text_v text, " +
                                                         "  time_v time, " +
                                                         "  timestamp_v timestamp, " +
                                                         "  timeuuid_v timeuuid, " +
                                                         "  tinyint_v tinyint, " +
                                                         "  uuid_v uuid, " +
                                                         "  varchar_v varchar, " +
                                                         "  varint_v varint" +
                                                         ");",
                                                         KS_NAME,
                                                         TABLE_NAME));

        Columns regulars = Schema.instance.getTableMetadata(KS_NAME, TABLE_NAME).regularColumns();
        List<String> allColumns = regulars.stream().map(ColumnMetadata::toString).collect(Collectors.toList());
        List<String> textColumns = Arrays.asList("text_v", "ascii_v", "varchar_v");

        new HashMap<Class<? extends AbstractAnalyzer>, List<String>>()
        {{
            put(StandardAnalyzer.class, textColumns);
            put(NonTokenizingAnalyzer.class, textColumns);
            put(DelimiterAnalyzer.class, textColumns);
            put(NoOpAnalyzer.class, allColumns);
        }}
        .forEach((analyzer, supportedColumns) -> {
            for (String column : allColumns)
            {
                String query = String.format("CREATE CUSTOM INDEX ON %s.%s(%s) " +
                                             "USING 'org.apache.cassandra.index.sasi.SASIIndex' " +
                                             "WITH OPTIONS = {'analyzer_class': '%s', 'mode':'PREFIX'};",
                                             KS_NAME, TABLE_NAME, column, analyzer.getName());

                if (supportedColumns.contains(column))
                {
                    QueryProcessor.executeOnceInternal(query);
                }
                else
                {
                    try
                    {
                        QueryProcessor.executeOnceInternal(query);
                        Assert.fail("Expected ConfigurationException");
                    }
                    catch (ConfigurationException e)
                    {
                        // expected
                        Assert.assertTrue("Unexpected error message " + e.getMessage(),
                                          e.getMessage().contains("does not support type"));
                    }
                }
            }
        });
    }

    @Test
    public void testIllegalArgumentsForAnalyzerShouldFail()
    {
        String baseTable = "illegal_argument_test";
        String indexName = "illegal_index";
        QueryProcessor.executeOnceInternal(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KS_NAME));
        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, baseTable));

        try
        {
            QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS %s ON %s.%s(v) " +
                            "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'CONTAINS', " +
                            "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', " +
                            "'case_sensitive': 'false'," +
                            "'normalize_uppercase': 'true'};",
                    indexName, KS_NAME, baseTable));

            Assert.fail("creation of index analyzer with illegal options should fail");
        }
        catch (ConfigurationException e)
        {
            //correct behaviour
            //confirm that it wasn't written to the schema
            String query = String.format("SELECT * FROM system_schema.indexes WHERE keyspace_name = '%s' " +
                                         "and table_name = '%s' and index_name = '%s';", KS_NAME, baseTable, indexName);
            Assertions.assertThat(QueryProcessor.executeOnceInternal(query)).isEmpty();

            Assert.assertEquals("case_sensitive option cannot be specified together with either normalize_lowercase or normalize_uppercase", e.getMessage());
        }
    }

    private ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, boolean forceFlush)
    {
        return loadData(data, forceFlush, ++timestamp);
    }

    private ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, boolean forceFlush, long timestamp)
    {
        for (Map.Entry<String, Pair<String, Integer>> e : data.entrySet())
            newMutation(e.getKey(), e.getValue().left, null, e.getValue().right, timestamp).apply();

        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        if (forceFlush)
            Util.flush(store);

        return store;
    }

    private static void cleanupData()
    {
        stores().forEach(ColumnFamilyStore::truncateBlocking);
    }

    private static Stream<ColumnFamilyStore> stores()
    {
        Keyspace ks = Keyspace.open(KS_NAME);
        return ks.getMetadata().tables.stream().map(t -> ks.getColumnFamilyStore(t.name));
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, int maxResults, Expression... expressions)
    {
        return getIndexed(store, ColumnFilter.all(store.metadata()), maxResults, expressions);
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, ColumnFilter columnFilter, int maxResults, Expression... expressions)
    {
        ReadCommand command = getIndexReadCommand(store, columnFilter, null, maxResults, expressions);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator rows = command.executeLocally(controller))
        {
            return getKeys(rows);
        }
    }

    private static Set<DecoratedKey> getPaged(ColumnFamilyStore store, int pageSize, Expression... expressions)
    {
        Set<DecoratedKey> uniqueKeys = new TreeSet<>();

        DecoratedKey lastKey = null;

        int count;
        do
        {
            count = 0;
            ReadCommand command = getIndexReadCommand(store, ColumnFilter.all(store.metadata()), lastKey, pageSize, expressions);

            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator currentPage = command.executeLocally(controller))
            {
                if (currentPage == null)
                    break;

                while (currentPage.hasNext())
                {
                    try (UnfilteredRowIterator row = currentPage.next())
                    {
                        uniqueKeys.add(row.partitionKey());
                        lastKey = row.partitionKey();
                        count++;
                    }
                }
            }
        }
        while (count == pageSize);

        return uniqueKeys;
    }

    private static ReadCommand getIndexReadCommand(ColumnFamilyStore store, ColumnFilter columnFilter, DecoratedKey startKey, int maxResults, Expression[] expressions)
    {
        DataRange range = (startKey == null)
                            ? DataRange.allData(PARTITIONER)
                            : DataRange.forKeyRange(new Range<>(startKey, PARTITIONER.getMinimumToken().maxKeyBound()));

        RowFilter filter = RowFilter.create();
        for (Expression e : expressions)
            filter.add(store.metadata().getColumn(e.name), e.op, e.value);

        ReadCommand command =
            PartitionRangeReadCommand.create(store.metadata(),
                                             FBUtilities.nowInSeconds(),
                                             columnFilter,
                                             filter,
                                             DataLimits.cqlLimits(maxResults),
                                             range);
        return command;
    }

    private static Mutation newMutation(String key, String firstName, String lastName, int age, long timestamp)
    {
        Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KS_NAME, decoratedKey(AsciiType.instance.decompose(key)));
        List<Cell<?>> cells = new ArrayList<>(3);

        if (age >= 0)
            cells.add(buildCell(ByteBufferUtil.bytes("age"), Int32Type.instance.decompose(age), timestamp));
        if (firstName != null)
            cells.add(buildCell(ByteBufferUtil.bytes("first_name"), UTF8Type.instance.decompose(firstName), timestamp));
        if (lastName != null)
            cells.add(buildCell(ByteBufferUtil.bytes("last_name"), UTF8Type.instance.decompose(lastName), timestamp));

        update(rm, cells);
        return rm.build();
    }

    private static Set<String> getKeys(final UnfilteredPartitionIterator rows)
    {
        try
        {
            return new TreeSet<String>()
            {{
                while (rows.hasNext())
                {
                    try (UnfilteredRowIterator row = rows.next())
                    {
                        if (!row.isEmpty())
                            add(AsciiType.instance.compose(row.partitionKey().getKey()));
                    }
                }
            }};
        }
        finally
        {
            rows.close();
        }
    }

    private static List<String> convert(final Set<DecoratedKey> keys)
    {
        return new ArrayList<String>()
        {{
            for (DecoratedKey key : keys)
                add(AsciiType.instance.getString(key.getKey()));
        }};
    }

    private UntypedResultSet executeCQL(String cfName, String query, Object... values)
    {
        return QueryProcessor.executeOnceInternal(String.format(query, KS_NAME, cfName), values);
    }

    private Set<String> executeCQLWithKeys(String rawStatement)
    {
        Set<String> results = new TreeSet<>();
        for (UntypedResultSet.Row row : QueryProcessor.executeOnceInternal(rawStatement))
        {
            if (row.has("id"))
                results.add(row.getString("id"));
        }

        return results;
    }

    private static DecoratedKey decoratedKey(ByteBuffer key)
    {
        return PARTITIONER.decorateKey(key);
    }

    private static DecoratedKey decoratedKey(String key)
    {
        return decoratedKey(AsciiType.instance.fromString(key));
    }

    private static Row buildRow(Collection<Cell<?>> cells)
    {
        return buildRow(Iterables.toArray(cells, Cell.class));
    }

    private static Row buildRow(Cell<?>... cells)
    {
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(Clustering.EMPTY);
        for (Cell<?> c : cells)
            rowBuilder.addCell(c);
        return rowBuilder.build();
    }

    private static Cell<?> buildCell(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        TableMetadata cfm = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata();
        return BufferCell.live(cfm.getColumn(name), timestamp, value);
    }

    private static Cell<?> buildCell(TableMetadata cfm, ByteBuffer name, ByteBuffer value, long timestamp)
    {
        ColumnMetadata column = cfm.getColumn(name);
        assert column != null;
        return BufferCell.live(column, timestamp, value);
    }

    private static Expression buildExpression(ByteBuffer name, Operator op, ByteBuffer value)
    {
        return new Expression(name, op, value);
    }

    private static void update(Mutation.PartitionUpdateCollector rm, ByteBuffer name, ByteBuffer value, long timestamp)
    {
        TableMetadata metadata = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata();
        rm.add(PartitionUpdate.singleRowUpdate(metadata, rm.key(), buildRow(buildCell(metadata, name, value, timestamp))));
    }


    private static void update(Mutation.PartitionUpdateCollector rm, List<Cell<?>> cells)
    {
        TableMetadata metadata = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata();
        rm.add(PartitionUpdate.singleRowUpdate(metadata, rm.key(), buildRow(cells)));
    }

    private static class Expression
    {
        public final ByteBuffer name;
        public final Operator op;
        public final ByteBuffer value;

        public Expression(ByteBuffer name, Operator op, ByteBuffer value)
        {
            this.name = name;
            this.op = op;
            this.value = value;
        }
    }

    private static void assertRows(Iterable<String> actual, String... expected)
    {
        String message = String.format("Expected rows to contain %s but found %s", Arrays.toString(expected), actual);
        Assert.assertArrayEquals(message, expected, Iterables.toArray(actual, String.class));
    }

    private static void assertRowsSize(Set<String> actual, int expectedSize)
    {
        String message = String.format("Expected %s to have size %d but found size %d", actual, expectedSize, actual.size());
        Assert.assertEquals(message, expectedSize, actual.size());
    }
}
