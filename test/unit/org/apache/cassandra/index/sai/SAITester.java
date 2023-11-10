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
package org.apache.cassandra.index.sai;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.AttributeNotFoundException;
import javax.management.ObjectName;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.ResourceLeakDetector;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.codecs.CodecUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_RANDOM_SEED;
import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.quote;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class SAITester extends CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(SAITester.class);

    protected static final String CREATE_KEYSPACE_TEMPLATE = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                                             "{'class': 'SimpleStrategy', 'replication_factor': '1'}";

    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                          "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    protected static final String CREATE_INDEX_TEMPLATE = "CREATE INDEX IF NOT EXISTS ON %%s(%s) USING 'sai'";

    protected static final ColumnIdentifier V1_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v1", true);
    protected static final ColumnIdentifier V2_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v2", true);

    protected static final Injections.Counter indexBuildCounter = Injections.newCounter("IndexBuildCounter")
                                                                            .add(newInvokePoint().onClass(CompactionManager.class)
                                                                                                 .onMethod("submitIndexBuild",
                                                                                                           "SecondaryIndexBuilder",
                                                                                                           "ActiveCompactionsTracker"))
                                                                            .build();

    protected static final Injections.Counter perSSTableValidationCounter = Injections.newCounter("PerSSTableValidationCounter")
                                                                                      .add(newInvokePoint().onClass(OnDiskFormat.class)
                                                                                                           .onMethod("validatePerSSTableIndexComponents"))
                                                                                      .build();

    protected static final Injections.Counter perColumnValidationCounter = Injections.newCounter("PerColumnValidationCounter")
                                                                                     .add(newInvokePoint().onClass(OnDiskFormat.class)
                                                                                                          .onMethod("validatePerColumnIndexComponents"))
                                                                                     .build();

    private static Randomization random;

    public static final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    public static final PrimaryKey.Factory TEST_FACTORY = new PrimaryKey.Factory(Murmur3Partitioner.instance, EMPTY_COMPARATOR);

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();

        // Ensure that the on-disk format statics are loaded before the test run
        Version.LATEST.onDiskFormat();
    }

    @Rule
    public TestRule testRules = new ResourceLeakDetector();

    @Rule
    public FailureWatcher failureRule = new FailureWatcher();

    @After
    public void removeAllInjections()
    {
        Injections.deleteAll();
        CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES.reset();
        CassandraRelevantProperties.SAI_POSTINGS_SKIP.reset();
        V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER.setLimitBytes(V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMIT);
    }

    public static Randomization getRandom()
    {
        if (random == null)
            random = new Randomization();
        return random;
    }

    public enum CorruptionType
    {
        REMOVED
        {
            @Override
            public void corrupt(File file) throws IOException
            {
                if (!file.tryDelete())
                    throw new IOException("Unable to delete file: " + file);
            }
        },
        EMPTY_FILE
        {
            @Override
            public void corrupt(File file) throws IOException
            {
                try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
                {
                    channel.truncate(0);
                }
            }
        },
        TRUNCATED_HEADER
        {
            @Override
            public void corrupt(File file) throws IOException
            {
                try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
                {
                    channel.truncate(2);
                }
            }
        },
        TRUNCATED_DATA
        {
            @Override
            public void corrupt(File file) throws IOException
            {
                // header length is not fixed, use footer length to navigate a given data position
                try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
                {
                    channel.truncate(file.length() - CodecUtil.footerLength() - 2);
                }
            }
        },
        TRUNCATED_FOOTER
        {
            @Override
            public void corrupt(File file) throws IOException
            {
                try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
                {
                    channel.truncate(file.length() - CodecUtil.footerLength() + 2);
                }
            }
        },
        APPENDED_DATA
        {
            @Override
            public void corrupt(File file) throws IOException
            {
                try (RandomAccessFile raf = new RandomAccessFile(file.toJavaIOFile(), "rw"))
                {
                    raf.seek(file.length());

                    byte[] corruptedData = new byte[100];
                    new Random().nextBytes(corruptedData);
                    raf.write(corruptedData);
                }
            }
        };

        public abstract void corrupt(File file) throws IOException;
    }

    public static StorageAttachedIndex createMockIndex(ColumnMetadata column)
    {
        TableMetadata table = TableMetadata.builder(column.ksName, column.cfName)
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addRegularColumn(column.name, column.type)
                                           .partitioner(Murmur3Partitioner.instance)
                                           .caching(CachingParams.CACHE_NOTHING)
                                           .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", column.name.toString());

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(column.name.toString(), IndexMetadata.Kind.CUSTOM, options);

        ColumnFamilyStore cfs = MockSchema.newCFS(table);

        return new StorageAttachedIndex(cfs, indexMetadata);
    }

    public static StorageAttachedIndex createMockIndex(String columnName, AbstractType<?> cellType)
    {
        TableMetadata table = TableMetadata.builder("test", "test")
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addRegularColumn(columnName, cellType)
                                           .partitioner(Murmur3Partitioner.instance)
                                           .caching(CachingParams.CACHE_NOTHING)
                                           .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", columnName);

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(columnName, IndexMetadata.Kind.CUSTOM, options);

        ColumnFamilyStore cfs = MockSchema.newCFS(table);

        return new StorageAttachedIndex(cfs, indexMetadata);
    }

    public static IndexTermType createIndexTermType(AbstractType<?> cellType)
    {
        return IndexTermType.create(ColumnMetadata.regularColumn("sai", "internal", "val", cellType), Collections.emptyList(), IndexTarget.Type.SIMPLE);
    }

    public IndexIdentifier createIndexIdentifier(String indexName)
    {
        return createIndexIdentifier(keyspace(), currentTable(), indexName);
    }

    public static IndexIdentifier createIndexIdentifier(String keyspaceName, String tableName, String indexName)
    {
        return new IndexIdentifier(keyspaceName, tableName, indexName);
    }

    protected StorageAttachedIndexGroup getCurrentIndexGroup()
    {
        return StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
    }

    protected void dropIndex(IndexIdentifier indexIdentifier) throws Throwable
    {
        dropIndex("DROP INDEX %s." + indexIdentifier.indexName);
    }

    protected void simulateNodeRestart()
    {
        simulateNodeRestart(true);
    }

    protected void simulateNodeRestart(boolean wait)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.indexManager.listIndexes().forEach(index -> ((StorageAttachedIndexGroup)cfs.indexManager.getIndexGroup(index)).reset());
        cfs.indexManager.listIndexes().forEach(cfs.indexManager::buildIndex);
        cfs.indexManager.executePreJoinTasksBlocking(true);
        if (wait)
        {
            waitForTableIndexesQueryable();
        }
    }

    protected void corruptIndexComponent(IndexComponent indexComponent, CorruptionType corruptionType) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File file = IndexDescriptor.create(sstable).fileFor(indexComponent);
            corruptionType.corrupt(file);
        }
    }

    protected void corruptIndexComponent(IndexComponent indexComponent, IndexIdentifier indexIdentifier, CorruptionType corruptionType) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File file = IndexDescriptor.create(sstable).fileFor(indexComponent, indexIdentifier);
            corruptionType.corrupt(file);
        }
    }

    protected boolean indexNeedsFullRebuild(String index)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        return cfs.indexManager.needsFullRebuild(index);
    }

    protected void verifyInitialIndexFailed(String indexName)
    {
        // Verify that the initial index build fails...
        waitForAssert(() -> assertTrue(indexNeedsFullRebuild(indexName)));
    }

    protected boolean verifyChecksum(IndexTermType indexContext, IndexIdentifier indexIdentifier)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            if (!indexDescriptor.validatePerSSTableComponents(IndexValidation.CHECKSUM)
                || !indexDescriptor.validatePerIndexComponents(indexContext, indexIdentifier, IndexValidation.CHECKSUM))
                return false;
        }
        return true;
    }

    protected boolean validateComponents(IndexTermType indexTermType, String indexName)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        IndexIdentifier indexIdentifier = createIndexIdentifier(cfs.getKeyspaceName(), cfs.getTableName(), indexName);

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            if (!indexDescriptor.validatePerSSTableComponents(IndexValidation.HEADER_FOOTER)
                || !indexDescriptor.validatePerIndexComponents(indexTermType, indexIdentifier, IndexValidation.HEADER_FOOTER))
                return false;
        }
        return true;
    }

    protected Object getMBeanAttribute(ObjectName name, String attribute) throws Exception
    {
        return jmxConnection.getAttribute(name, attribute);
    }

    protected Object getMetricValue(ObjectName metricObjectName)
    {
        // lets workaround the fact that gauges have Value, but counters have Count
        Object metricValue;
        try
        {
            try
            {
                metricValue = getMBeanAttribute(metricObjectName, "Value");
            }
            catch (AttributeNotFoundException ignored)
            {
                metricValue = getMBeanAttribute(metricObjectName, "Count");
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return metricValue;
    }

    public void waitForCompactions()
    {
        waitForAssert(() -> assertFalse(CompactionManager.instance.isCompacting(ColumnFamilyStore.all(), ssTableReader -> true)), 10, TimeUnit.SECONDS);
    }

    protected void waitForCompactionsFinished()
    {
        waitForAssert(() -> assertEquals(0, getCompactionTasks()), 10, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, long value)
    {
        waitForAssert(() -> assertEquals(value, ((Number) getMetricValue(name)).longValue()), 10, TimeUnit.SECONDS);
    }

    protected ObjectName objectName(String name, String keyspace, String table, String index, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex," +
                                                "keyspace=%s,table=%s,index=%s,scope=%s,name=%s",
                                                keyspace, table, index, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    protected ObjectName objectNameNoIndex(String name, String keyspace, String table, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex," +
                                                "keyspace=%s,table=%s,scope=%s,name=%s",
                                                keyspace, table, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    protected long getSegmentBufferSpaceLimit()
    {
        return V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER.limitBytes();
    }

    protected long getSegmentBufferUsedBytes()
    {
        return V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER.currentBytesUsed();
    }

    protected int getColumnIndexBuildsInProgress()
    {
        return SegmentBuilder.getActiveBuilderCount();
    }

    protected void upgradeSSTables()
    {
        try
        {
            StorageService.instance.upgradeSSTables(KEYSPACE, false, currentTable());
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    protected long totalDiskSpaceUsed()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        return cfs.metric.totalDiskSpaceUsed.getCount();
    }

    protected long indexDiskSpaceUse()
    {
        return getCurrentIndexGroup().totalDiskUsage();
    }

    protected int getOpenIndexFiles()
    {
        return getCurrentIndexGroup().openIndexFiles();
    }

    protected long getDiskUsage()
    {
        return getCurrentIndexGroup().diskUsage();
    }

    protected void verifyNoIndexFiles()
    {
        assertTrue(indexFiles().isEmpty());
    }

    protected void verifyIndexFiles(IndexTermType indexTermType,
                                    IndexIdentifier indexIdentifier,
                                    int indexFiles)
    {
        verifyIndexFiles(indexTermType, indexIdentifier, indexFiles, indexFiles, indexFiles);
    }

    protected void verifyIndexFiles(IndexTermType indexTermType,
                                    IndexIdentifier indexIdentifier,
                                    int perSSTableFiles,
                                    int perColumnFiles,
                                    int completionMarkers)
    {
        Set<File> indexFiles = indexFiles();

        for (IndexComponent indexComponent : Version.LATEST.onDiskFormat().perSSTableIndexComponents(false))
        {
            Component component = SSTableFormat.Components.Types.CUSTOM.createComponent(Version.LATEST.fileNameFormatter().format(indexComponent, null));
            Set<File> tableFiles = componentFiles(indexFiles, component);
            assertEquals(tableFiles.toString(), perSSTableFiles, tableFiles.size());
        }

        for (IndexComponent indexComponent : Version.LATEST.onDiskFormat().perColumnIndexComponents(indexTermType))
        {
            String componentName = Version.LATEST.fileNameFormatter().format(indexComponent, indexIdentifier);
            Component component = SSTableFormat.Components.Types.CUSTOM.createComponent(componentName);
            Set<File> stringIndexFiles = componentFiles(indexFiles, component);
            if (isBuildCompletionMarker(indexComponent))
                assertEquals(completionMarkers, stringIndexFiles.size());
            else
                assertEquals(stringIndexFiles.toString(), perColumnFiles, stringIndexFiles.size());
        }
    }

    protected void verifySSTableIndexes(IndexIdentifier indexIdentifier, int count)
    {
        try
        {
            verifySSTableIndexes(indexIdentifier, count, count);
        }
        catch (Exception e)
        {
            throw Throwables.unchecked(e);
        }
    }

    protected void verifySSTableIndexes(IndexIdentifier indexIdentifier, int sstableContextCount, int sstableIndexCount)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup indexGroup = getCurrentIndexGroup();
        int contextCount = indexGroup.sstableContextManager().size();
        assertEquals("Expected " + sstableContextCount +" SSTableContexts, but got " + contextCount, sstableContextCount, contextCount);

        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexIdentifier.indexName);
        Collection<SSTableIndex> sstableIndexes = sai == null ? Collections.emptyList() : sai.view().getIndexes();
        assertEquals("Expected " + sstableIndexCount +" SSTableIndexes, but got " + sstableIndexes.toString(), sstableIndexCount, sstableIndexes.size());
    }

    protected boolean isBuildCompletionMarker(IndexComponent indexComponent)
    {
        return (indexComponent == IndexComponent.GROUP_COMPLETION_MARKER) ||
               (indexComponent == IndexComponent.COLUMN_COMPLETION_MARKER);

    }

    protected Set<File> indexFiles()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<Component> components = cfs.indexManager.listIndexGroups()
                                                    .stream()
                                                    .filter(g -> g instanceof StorageAttachedIndexGroup)
                                                    .map(Index.Group::getComponents)
                                                    .flatMap(Set::stream)
                                                    .collect(Collectors.toSet());

        Set<File> indexFiles = new HashSet<>();
        for (Component component : components)
        {
            List<File> files = cfs.getDirectories().getCFDirectories()
                                  .stream()
                                  .flatMap(dir -> Arrays.stream(dir.tryList()))
                                  .filter(File::isFile)
                                  .filter(f -> f.name().endsWith(component.name))
                                  .collect(Collectors.toList());
            indexFiles.addAll(files);
        }
        return indexFiles;
    }

    protected Set<File> componentFiles(Collection<File> indexFiles, Component component)
    {
        return indexFiles.stream().filter(c -> c.name().endsWith(component.name)).collect(Collectors.toSet());
    }

    public String createTable(String query)
    {
        return createTable(KEYSPACE, query);
    }

    @Override
    public UntypedResultSet execute(String query, Object... values)
    {
        return executeFormattedQuery(formatQuery(query), values);
    }

    @Override
    public Session sessionNet()
    {
        return sessionNet(getDefaultVersion());
    }

    public void flush(String keyspace, String table)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(table);
        if (store != null)
            store.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    public void compact(String keyspace, String table)
    {

        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(table);
        if (store != null)
            store.forceMajorCompaction();
    }

    protected void truncate(boolean snapshot)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        if (snapshot)
            cfs.truncateBlocking();
        else
            cfs.truncateBlockingWithoutSnapshot();
    }

    protected void rebuildIndexes(String... indexes)
    {
        ColumnFamilyStore.rebuildSecondaryIndex(KEYSPACE, currentTable(), indexes);
    }

    protected void reloadSSTableIndex()
    {
        getCurrentIndexGroup().unsafeReload();
    }

    protected void runInitializationTask() throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (Index i : cfs.indexManager.listIndexes())
        {
            assert i instanceof StorageAttachedIndex;
            cfs.indexManager.makeIndexNonQueryable(i, Index.Status.BUILD_FAILED);
            cfs.indexManager.buildIndex(i).get();
        }
    }

    protected int getCompactionTasks()
    {
        return CompactionManager.instance.getActiveCompactions() + CompactionManager.instance.getPendingTasks();
    }

    protected int snapshot(String snapshotName)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableSnapshot snapshot = cfs.snapshot(snapshotName);
        return snapshot.getDirectories().size();
    }

    protected void restoreSnapshot(String snapshot)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(snapshot);
        restore(cfs, lister);
    }

    protected void restore(ColumnFamilyStore cfs, Directories.SSTableLister lister)
    {
        File dataDirectory = cfs.getDirectories().getDirectoryForNewSSTables();

        for (File file : lister.listFiles())
        {
            file.tryMove(new File(dataDirectory.absolutePath() + File.pathSeparator() + file.name()));
        }
        cfs.loadNewSSTables();
    }

    public static void copyTo(DataInput in, OutputStream out, int length) throws IOException
    {
        byte[] buffer = new byte[64 * 1024];
        int copiedBytes = 0;

        while (copiedBytes + buffer.length < length)
        {
            in.readFully(buffer);
            out.write(buffer);
            copiedBytes += buffer.length;
        }

        if (copiedBytes < length)
        {
            int left = length - copiedBytes;
            in.readFully(buffer, 0, left);
            out.write(buffer, 0, left);
        }
    }

    protected void assertNumRows(int expected, String query, Object... args)
    {
        ResultSet rs = executeNet(String.format(query, args));
        assertEquals(expected, rs.all().size());
    }

    protected static Injection newFailureOnEntry(String name, Class<?> invokeClass, String method, Class<? extends Throwable> exception)
    {
        return Injections.newCustom(name)
                         .add(newInvokePoint().onClass(invokeClass).onMethod(method))
                         .add(newActionBuilder().actions().doThrow(exception, quote("Injected failure!")))
                         .build();
    }

    protected void assertValidationCount(int perSSTable, int perColumn)
    {
        Assert.assertEquals(perSSTable, perSSTableValidationCounter.get());
        Assert.assertEquals(perColumn, perColumnValidationCounter.get());
    }

    protected void resetValidationCount()
    {
        perSSTableValidationCounter.reset();
        perColumnValidationCounter.reset();
    }

    protected long indexFilesLastModified()
    {
        return indexFiles().stream().map(File::lastModified).max(Long::compare).orElse(0L);
    }

    protected void verifyIndexComponentsIncludedInSSTable() throws Exception
    {
        verifySSTableComponents(currentTable(), true);
    }

    protected void verifyIndexComponentsNotIncludedInSSTable() throws Exception
    {
        verifySSTableComponents(currentTable(), false);
    }

    private void verifySSTableComponents(String table, boolean indexComponentsExist) throws Exception
    {
        ColumnFamilyStore cfs = Objects.requireNonNull(Schema.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(table);
        for (SSTable sstable : cfs.getLiveSSTables())
        {
            Set<Component> components = sstable.getComponents();
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            Set<Component> ndiComponents = group == null ? Collections.emptySet() : group.getComponents();

            Set<Component> diff = Sets.difference(ndiComponents, components);
            if (indexComponentsExist)
                assertTrue("Expect all index components are tracked by SSTable, but " + diff + " are not included.",
                           !ndiComponents.isEmpty() && diff.isEmpty());
            else
                assertFalse("Expect no index components, but got " + components, components.toString().contains("SAI"));

            Set<Component> tocContents = TOCComponent.loadTOC(sstable.descriptor);
            assertEquals(components, tocContents);
        }
    }

    protected static void setBDKPostingsWriterSizing(int minimumPostingsLeaves, int postingsSkip)
    {
        CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES.setString(Integer.toString(minimumPostingsLeaves));
        CassandraRelevantProperties.SAI_POSTINGS_SKIP.setString(Integer.toString(postingsSkip));
    }

    protected static void setSegmentWriteBufferSpace(final int segmentSize)
    {
        V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER.setLimitBytes(segmentSize);
    }

    protected String getSingleTraceStatement(Session session, String query, String contains)
    {
        query = String.format(query, KEYSPACE + '.' + currentTable());
        QueryTrace trace = session.execute(session.prepare(query).bind().enableTracing()).getExecutionInfo().getQueryTrace();
        waitForTracingEvents();

        for (QueryTrace.Event event : trace.getEvents())
        {
            if (event.getDescription().contains(contains))
                return event.getDescription();
        }
        return null;
    }

    /**
     *  Because the tracing executor is single threaded, submitting an empty event should ensure
     *  that all tracing events mutations have been applied.
     */
    protected void waitForTracingEvents()
    {
        try
        {
            Stage.TRACING.executor().submit(() -> {}).get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to wait for tracing events", t);
        }
    }

    public static class Randomization
    {
        private final long seed;
        private final Random random;

        Randomization()
        {
            seed = TEST_RANDOM_SEED.getLong(System.nanoTime());
            random = new Random(seed);
        }

        public void printSeedOnFailure()
        {
            logger.error("Randomized test failed. To rerun test use -D{}={}", TEST_RANDOM_SEED.getKey(), seed);
        }

        public int nextInt()
        {
            return random.nextInt();
        }

        public int nextInt(int max)
        {
            return RandomInts.randomInt(random, max);
        }

        public int nextIntBetween(int minValue, int maxValue)
        {
            return RandomInts.randomIntBetween(random, minValue, maxValue);
        }

        public long nextLong()
        {
            return random.nextLong();
        }

        public short nextShort()
        {
            return (short)random.nextInt(Short.MAX_VALUE + 1);
        }

        public byte nextByte()
        {
            return (byte)random.nextInt(Byte.MAX_VALUE + 1);
        }

        public BigInteger nextBigInteger(int maxNumBits)
        {
            return new BigInteger(RandomInts.randomInt(random, maxNumBits), random);
        }

        public BigInteger nextBigInteger(int minNumBits, int maxNumBits)
        {
            return new BigInteger(RandomInts.randomIntBetween(random, minNumBits, maxNumBits), random);
        }

        public BigDecimal nextBigDecimal(int minUnscaledValue, int maxUnscaledValue, int minScale, int maxScale)
        {
            return BigDecimal.valueOf(RandomInts.randomIntBetween(random, minUnscaledValue, maxUnscaledValue),
                                      RandomInts.randomIntBetween(random, minScale, maxScale));
        }

        public float nextFloat()
        {
            return random.nextFloat();
        }

        public double nextDouble()
        {
            return random.nextDouble();
        }

        public String nextAsciiString(int minLength, int maxLength)
        {
            return RandomStrings.randomAsciiOfLengthBetween(random, minLength, maxLength);
        }

        public String nextTextString(int minLength, int maxLength)
        {
            return RandomStrings.randomRealisticUnicodeOfLengthBetween(random, minLength, maxLength);
        }

        public boolean nextBoolean()
        {
            return random.nextBoolean();
        }

        public void nextBytes(byte[] bytes)
        {
            random.nextBytes(bytes);
        }
    }

    public static class FailureWatcher extends TestWatcher
    {
        @Override
        protected void failed(Throwable e, Description description)
        {
            if (random != null)
                random.printSeedOnFailure();
        }
    }
    /**
     * Run repeated verification task concurrently with target test
     */
    protected static class TestWithConcurrentVerification
    {
        private static final int verificationMaxInMs = 300_000; // 300s

        private final Runnable verificationTask;
        private final CountDownLatch verificationStarted = new CountDownLatch(1);

        private final Runnable targetTask;
        private final CountDownLatch taskCompleted = new CountDownLatch(1);

        private final int verificationIntervalInMs;

        public TestWithConcurrentVerification(Runnable verificationTask, Runnable targetTask)
        {
            this(verificationTask, targetTask, 10);
        }

        /**
         * @param verificationTask to be run concurrently with target task
         * @param targetTask task to be performed once
         * @param verificationIntervalInMs interval between each verification task, -1 to run verification task once
         */
        public TestWithConcurrentVerification(Runnable verificationTask, Runnable targetTask, int verificationIntervalInMs)
        {
            this.verificationTask = verificationTask;
            this.targetTask = targetTask;
            this.verificationIntervalInMs = verificationIntervalInMs;
        }

        public void start()
        {
            Thread verificationThread = new Thread(() -> {
                verificationStarted.countDown();

                while (true)
                {
                    try
                    {
                        verificationTask.run();

                        if (verificationIntervalInMs < 0 || taskCompleted.await(verificationIntervalInMs, TimeUnit.MILLISECONDS))
                            break;
                    }
                    catch (Throwable e)
                    {
                        throw Throwables.unchecked(e);
                    }
                }
            });

            try
            {
                verificationThread.start();
                verificationStarted.await();

                targetTask.run();
                taskCompleted.countDown();

                verificationThread.join(verificationMaxInMs);
            }
            catch (InterruptedException e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
