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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Predicates;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ReadFailureException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.plan.Plan;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.ResourceLeakDetector;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.codecs.CodecUtil;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.expr;
import static org.apache.cassandra.inject.Expression.quote;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SAITester extends CQLTester
{
    protected static final String CREATE_KEYSPACE_TEMPLATE = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}";

    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
            "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    protected static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS ON %%s(%s) USING 'StorageAttachedIndex'";

    protected static int ASSERTION_TIMEOUT_SECONDS = 15;

    protected static Injections.Counter.CounterBuilder addConditions(Injections.Counter.CounterBuilder builder, Consumer<ActionBuilder.ConditionsBuilder> adder)
    {
        adder.accept(builder.lastActionBuilder().conditions());
        return builder;
    }


    protected static final Injections.Counter INDEX_BUILD_COUNTER = Injections.newCounter("IndexBuildCounter")
                                                                              .add(newInvokePoint().onClass(CompactionManager.class)
                                                                                                   .onMethod("submitIndexBuild", "SecondaryIndexBuilder", "TableOperationObserver"))
                                                                              .build();

    protected static final Injections.Counter perSSTableValidationCounter = addConditions(Injections.newCounter("PerSSTableValidationCounter")
                                                                                      .add(newInvokePoint().onClass("IndexDescriptor$IndexComponentsImpl")
                                                                                                           .onMethod("isValid")),
                                                                                          b -> b.not().when(expr(Expression.THIS).method("isPerIndexGroup").args()).and().not().when(expr("$validateChecksum"))
    ).build();

    protected static final Injections.Counter perColumnValidationCounter = addConditions(Injections.newCounter("PerColumnValidationCounter")
                                                                                     .add(newInvokePoint().onClass("IndexDescriptor$IndexComponentsImpl")
                                                                                                          .onMethod("isValid")),
                                                                                         b -> b.when(expr(Expression.THIS).method("isPerIndexGroup").args()).and().not().when(expr("$validateChecksum"))
    ).build();

    protected static ColumnIdentifier V1_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v1", true);
    protected static ColumnIdentifier V2_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v2", true);

    public static final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    public static final PrimaryKey.Factory TEST_FACTORY = Version.current(KEYSPACE).onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

    // Row-aware TEST_FACTORY that runs up to FA format version and does not use clustered row aware
    public static final PrimaryKey.Factory ROW_AWARE_TEST_FACTORY = (Version.current(KEYSPACE).onOrAfter(Version.GA)
                                                                     ? Version.FA
                                                                     : Version.current(KEYSPACE))
                                                                    .onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

    static
    {
        Version.ALL.size();
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
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(0).close();
                    }
                },
        TRUNCATED_HEADER
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(2).close();
                    }
                },
        TRUNCATED_DATA
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        // header length is not fixed, use footer length to navigate a given data position
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(file.length() - CodecUtil.footerLength() - 2).close();
                    }
                },
        TRUNCATED_FOOTER
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(file.length() - CodecUtil.footerLength() + 2).close();
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

    @Rule
    public TestRule testRules = new ResourceLeakDetector();

    @Before
    public void resetQueryOptimizationLevel()
    {
        // Enable the optimizer by default. If there are any tests that need to disable it, they can do so explicitly.
        QueryController.QUERY_OPT_LEVEL = 1;
        QueryController.QUERY_OPT_USE_TERM_STATS = true;
    }

    @Before
    public void resetLastValidSegmentRowId()
    {
        // Don't want this setting to impact peer tests
        SegmentBuilder.updateLastValidSegmentRowId(-1);
    }

    @After
    public void removeAllInjections()
    {
        Injections.deleteAll();
    }

    /**
     * Enable external execution of all queries because we want to use reconciliation in SELECT queries so that we can
     * simulate the application of the entire row filter in the coordinator node, even if unit tests are not multinode.
     */
    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        CQLTester.enableCoordinatorExecution();
    }

    /**
     * Creates a SAI index on the current table, waiting for it to become queryable.
     *
     * @param column the name of the indexed column, maybe with {@code FULL()}, {@code KEYS()} or {@code VALUES()} spec
     * @param options the index options, of the form {@code "{'option1': value1, 'option2': value2, ...}"}.
     * @return the name of the created index
     */
    public String createSAIIndex(String column, @Nullable String options)
    {
        String query = String.format(CREATE_INDEX_TEMPLATE, column);

        if (options != null)
            query += " WITH OPTIONS = " + options;

        return createIndex(query);
    }

    public static IndexContext createIndexContext(String name, AbstractType<?> validator, ColumnFamilyStore cfs)
    {
        return new IndexContext(cfs.getKeyspaceName(),
                                cfs.getTableName(),
                                cfs.metadata().id,
                                UTF8Type.instance,
                                new ClusteringComparator(),
                                ColumnMetadata.regularColumn("sai", "internal", name, validator),
                                IndexTarget.Type.SIMPLE,
                                IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, null),
                                cfs);
    }

    public static IndexContext createIndexContext(String name, AbstractType<?> validator)
    {
        return new IndexContext("test_ks",
                                "test_cf",
                                TableId.generate(),
                                UTF8Type.instance,
                                new ClusteringComparator(),
                                ColumnMetadata.regularColumn("sai", "internal", name, validator),
                                IndexTarget.Type.SIMPLE,
                                IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, null),
                                MockSchema.newCFS("test_ks"));
    }

    public static IndexContext createIndexContext(String columnName, String indexName, AbstractType<?> validator)
    {
        return new IndexContext("test_ks",
                                "test_cf",
                                TableId.generate(),
                                UTF8Type.instance,
                                new ClusteringComparator(),
                                ColumnMetadata.regularColumn("sai", "internal", columnName, validator),
                                IndexTarget.Type.SIMPLE,
                                IndexMetadata.fromSchemaMetadata(indexName, IndexMetadata.Kind.CUSTOM, null),
                                MockSchema.newCFS("test_ks"));
    }

    public Set<IndexContext> getIndexContexts(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Assertions.assertThat(group).isNotNull();
        return group.getIndexes()
                    .stream()
                    .map(StorageAttachedIndex::getIndexContext)
                    .collect(Collectors.toSet());
    }

    public IndexContext getIndexContext(String indexName)
    {
        return getIndexContext(KEYSPACE, currentTable(), indexName);
    }

    public IndexContext getIndexContext(String keysapce, String table, String indexName)
    {
        ColumnFamilyStore cfs = Keyspace.open(keysapce).getColumnFamilyStore(table);
        return StorageAttachedIndexGroup.getIndexGroup(cfs)
                                 .getIndexes()
                                 .stream()
                                 .map(StorageAttachedIndex::getIndexContext)
                                 .filter(ctx -> ctx.getIndexName().equals(indexName))
                                 .findFirst()
                                 .orElseThrow();
    }

    public static Vector<Float> vector(float... v)
    {
        var v2 = new Float[v.length];
        for (int i = 0; i < v.length; i++)
            v2[i] = v[i];
        return new Vector<>(v2);
    }

    protected void simulateNodeRestart()
    {
        simulateNodeRestart(true);
    }

    protected void simulateNodeRestart(boolean wait)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        cfs.indexManager.listIndexes().forEach(index -> ((StorageAttachedIndexGroup)cfs.indexManager.getIndexGroup(index)).reset());
        cfs.indexManager.listIndexes().forEach(cfs.indexManager::buildIndex);
        cfs.indexManager.executePreJoinTasksBlocking(true);
        if (wait)
        {
            waitForTableIndexesQueryable();
        }
    }

    protected static IndexDescriptor loadDescriptor(SSTableReader sstable, ColumnFamilyStore cfs)
    {
        return IndexDescriptor.load(sstable,
                                    StorageAttachedIndexGroup.getIndexGroup(cfs).getIndexes().stream().map(StorageAttachedIndex::getIndexContext).collect(Collectors.toSet()));
    }

    protected void corruptIndexComponent(IndexComponentType indexComponentType, CorruptionType corruptionType) throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File file = loadDescriptor(sstable, cfs).perSSTableComponents().get(indexComponentType).file();
            corruptionType.corrupt(file);
        }
    }

    protected void corruptIndexComponent(IndexComponentType indexComponentType, IndexContext indexContext, CorruptionType corruptionType) throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File file = loadDescriptor(sstable, cfs).perIndexComponents(indexContext).get(indexComponentType).file();
            corruptionType.corrupt(file);
        }
    }

    protected boolean verifyChecksum(IndexContext context)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());

        try (ColumnFamilyStore.RefViewFragment rvf = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            for (SSTableReader sstable : rvf.sstables)
            {
                IndexDescriptor indexDescriptor = loadDescriptor(sstable, cfs);
                if (indexDescriptor.isIndexEmpty(context))
                    continue;
                if (!indexDescriptor.perSSTableComponents().validateComponents(sstable, cfs.getTracker(), true)
                    || !indexDescriptor.perIndexComponents(context).validateComponents(sstable, cfs.getTracker(), true))
                    return false;
            }
        }

        return true;
    }

    protected void verifySAIVersionInUse(Version... expectedVersions)
    {
        verifySAIVersionInUse(KEYSPACE, currentTable(), expectedVersions);
    }

    protected void verifySAIVersionInUse(String keyspace,
                                         String table,
                                         Version... expectedVersions)
    {
        verifySAIVersionInUse(keyspace, table, true, expectedVersions);
    }

    protected void verifySAIVersionInUse(String keyspace,
                                         String table,
                                         boolean uniqueVersionPerSSTable,
                                         Version... expectedVersions)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Assert.assertNotNull(group);
        Set<Version> versions = Set.copyOf(Arrays.asList(expectedVersions)); // there might be duplicate expected versions

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            verifySAIVersionInUse(cfs, group, sstable, versions, uniqueVersionPerSSTable);
        }
    }

    private void verifySAIVersionInUse(ColumnFamilyStore cfs,
                                       StorageAttachedIndexGroup group,
                                       SSTableReader sstable,
                                       Set<Version> expectedVersions,
                                       boolean uniqueVersionPerSSTable)
    {
        Set<Version> foundVersions = new HashSet<>();

        IndexDescriptor indexDescriptor = loadDescriptor(sstable, cfs);
        Version perSSTableVersion = indexDescriptor.perSSTableComponents().version();
        foundVersions.add(perSSTableVersion);
        Assertions.assertThat(perSSTableVersion).isIn(expectedVersions);

        // This is to make sure the context uses the actual files we think
        SSTableContext sstableContext = group.sstableContextManager().getContext(sstable);
        perSSTableVersion = sstableContext.usedPerSSTableComponents().version();
        Assertions.assertThat(perSSTableVersion).isIn(expectedVersions);

        for (IndexContext indexContext : getIndexContexts(cfs.keyspace.getName(), cfs.name))
        {
            Version perIndexVersion = indexDescriptor.perIndexComponents(indexContext).version();
            Assertions.assertThat(perIndexVersion).isIn(expectedVersions);

            // Make sure the per-index version is the same as the per-sstable version.
            if (uniqueVersionPerSSTable && !perIndexVersion.equals(perSSTableVersion))
                Assert.fail("Per-index version " + perIndexVersion +
                            " is not equals to the per-sstable version " + perSSTableVersion);

            for (SSTableIndex sstableIndex : indexContext.getView())
            {
                if (sstableIndex.isEmpty())
                    continue;

                // Make sure the index does use components of the proper version.
                perIndexVersion = sstableIndex.usedPerIndexComponents().version();
                foundVersions.add(perIndexVersion);
                Assertions.assertThat(perIndexVersion).isIn(expectedVersions);
            }
        }

        Assertions.assertThat(foundVersions).isEqualTo(expectedVersions);
    }

    /**
     * Returns the set containing all SAI index versions found among all sstable indexes of the given table.
     * Memtable indexes are not included.
     */
    public static Set<Version> getSSTableIndexVersions(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        var group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        if (group == null)  // no indexes present on the table
            return Set.of();

        Set<IndexContext> indexContexts = group.getIndexes()
                                               .stream()
                                               .map(StorageAttachedIndex::getIndexContext)
                                               .collect(Collectors.toSet());

        var versions = new HashSet<Version>();
        for (var sstable : cfs.getLiveSSTables())
        {
            var indexDescriptor = group.descriptorFor(sstable);
            if (indexDescriptor != null)
            {
                versions.add(indexDescriptor.perSSTableComponents().version());
                for (IndexContext indexContext : indexContexts)
                    versions.add(indexDescriptor.perIndexComponents(indexContext).version());
            }
        }
        return versions;
    }

    protected static void assertFailureReason(ReadFailureException e, RequestFailureReason reason)
    {
        int expected = reason.codeForNativeProtocol();
        int actual = e.getFailuresMap().get(FBUtilities.getBroadcastAddressAndPort().address);
        assertEquals(expected, actual);
    }

    protected boolean isMetricPresent(ObjectName objectName) throws IOException
    {
        return jmxConnection.isRegistered(objectName);
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

    protected double getHistogramMean(ObjectName metricObjectName)
    {
        try
        {
            return ((Number) getMBeanAttribute(metricObjectName, "Mean")).doubleValue();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void assertMetricExists(ObjectName name)
    {
        Assertions.assertThatNoException().isThrownBy(() -> getMetricValue(name));
    }

    protected void assertMetricDoesNotExist(ObjectName name)
    {
        Assertions.assertThatThrownBy(() -> getMetricValue(name)).hasRootCauseInstanceOf(InstanceNotFoundException.class);
    }

    protected void startCompaction() throws Throwable
    {
        Iterable<ColumnFamilyStore> tables = StorageService.instance.getValidColumnFamilies(true, false, KEYSPACE, currentTable());
        tables.forEach(table ->
        {
            int gcBefore = CompactionManager.getDefaultGcBefore(table, FBUtilities.nowInSeconds());
            CompactionManager.instance.submitMaximal(table, gcBefore, false);
        });
    }

    public void waitForCompactions()
    {
        waitForAssert(() -> assertFalse(CompactionManager.instance.isCompacting(ColumnFamilyStore.all(), Predicates.alwaysTrue())), 10, TimeUnit.SECONDS);
    }

    protected void waitForCompactionsFinished()
    {
        waitForAssert(() -> assertEquals(0, getCompactionTasks()), 10, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, ObjectName name2)
    {
        waitForAssert(() -> {
            long jmxValue = ((Number) getMetricValue(name)).longValue();
            long jmxValue2 = ((Number) getMetricValue(name2)).longValue();

            jmxValue2 += 2; // add 2 for the first 2 queries in setupCluster

            assertEquals(jmxValue, jmxValue2);
        }, 10, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, long value)
    {
        waitForAssert(() -> assertEquals(value, ((Number) getMetricValue(name)).longValue()), 10, TimeUnit.SECONDS);
    }

    protected ObjectName objectName(String name, String keyspace, String table, String index, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,keyspace=%s,table=%s,index=%s,scope=%s,name=%s",
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
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,keyspace=%s,table=%s,scope=%s,name=%s",
                    keyspace, table, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    protected void upgradeSSTables()
    {
        upgradeSSTables(KEYSPACE, currentTable());
    }

    protected void upgradeSSTables(String keyspace, String table)
    {
        try
        {
            StorageService.instance.upgradeSSTables(keyspace, false, table);
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    protected long totalDiskSpaceUsed()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        return cfs.metric.totalDiskSpaceUsed.getCount();
    }

    protected long indexDiskSpaceUse()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        return Objects.requireNonNull(StorageAttachedIndexGroup.getIndexGroup(cfs)).totalDiskUsage();
    }

    protected int getOpenIndexFiles()
    {
        ColumnFamilyStore cfs = Schema.instance.getKeyspaceInstance(KEYSPACE).getColumnFamilyStore(currentTable());
        return StorageAttachedIndexGroup.getIndexGroup(cfs).openIndexFiles();
    }

    protected long getDiskUsage()
    {
        ColumnFamilyStore cfs = Schema.instance.getKeyspaceInstance(KEYSPACE).getColumnFamilyStore(currentTable());
        return StorageAttachedIndexGroup.getIndexGroup(cfs).diskUsage();
    }

    protected void verifyNoIndexFiles()
    {
        verifyNoIndexFiles(KEYSPACE, currentTable());
    }

    protected void verifyNoIndexFiles(String keyspace, String table)
    {
        assertTrue(indexFiles(keyspace, table).isEmpty());
    }

    // Verify every sstables is indexed correctly and the components are valid.
    protected void verifyIndexComponentFiles(@Nullable IndexContext numericIndexContext, @Nullable IndexContext stringIndexContext)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            // We create a descriptor from scratch, to ensure this discover from disk directly.
            IndexDescriptor descriptor = loadDescriptor(sstable, cfs);

            // Note that validation makes sure that all expected components exists, on top of validating those.
            descriptor.perSSTableComponents().validateComponents(sstable, cfs.getTracker(), true);
            if (numericIndexContext != null)
                descriptor.perIndexComponents(numericIndexContext).validateComponents(sstable, cfs.getTracker(), true);
            if (stringIndexContext != null)
                descriptor.perIndexComponents(stringIndexContext).validateComponents(sstable, cfs.getTracker(), true);
        }
    }


    // Note: this assumes the checked component files are at generation 0, which is not always the case with rebuild.
    // The `verifyIndexComponentFiles` method is probably a safer replacement overall, but many test still use this so
    // we keep it for now.
    protected void verifyIndexFiles(IndexContext numericIndexContext, IndexContext literalIndexContext, int numericFiles, int literalFiles)
    {
        verifyIndexFiles(numericIndexContext,
                         literalIndexContext,
                         Math.max(numericFiles, literalFiles),
                         numericFiles,
                         literalFiles,
                         numericFiles,
                         literalFiles);
    }

    // Same as namesake
    protected void verifyIndexFiles(IndexContext numericIndexContext,
                                    IndexContext literalIndexContext,
                                    int perSSTableFiles,
                                    int numericFiles,
                                    int literalFiles,
                                    int numericCompletionMarkers,
                                    int literalCompletionMarkers)
    {
        Set<File> indexFiles = indexFiles();

        for (IndexComponentType indexComponentType : Version.current(KEYSPACE).onDiskFormat()
                                                            .perSSTableComponentTypes(currentTableMetadata().hasClustering()))
        {
            String name = Version.current(KEYSPACE).fileNameFormatter().format(indexComponentType, (String)null, 0);
            Component component = new Component(Component.Type.CUSTOM, name);
            Set<File> tableFiles = componentFiles(indexFiles, component);
            assertEquals(tableFiles.toString(), perSSTableFiles, tableFiles.size());
        }

        if (literalIndexContext != null)
        {
            for (IndexComponentType indexComponentType : Version.current(KEYSPACE).onDiskFormat().perIndexComponentTypes(literalIndexContext))
            {
                Set<File> stringIndexFiles = componentFiles(indexFiles,
                                                            new Component(Component.Type.CUSTOM,
                                                                          Version.current(KEYSPACE)
                                                                                 .fileNameFormatter()
                                                                                 .format(indexComponentType, literalIndexContext, 0)));
                if (isBuildCompletionMarker(indexComponentType))
                    assertEquals(literalCompletionMarkers, stringIndexFiles.size());
                else
                    assertEquals(stringIndexFiles.toString(), literalFiles, stringIndexFiles.size());
            }
        }

        if (numericIndexContext != null)
        {
            for (IndexComponentType indexComponentType : Version.current(KEYSPACE).onDiskFormat().perIndexComponentTypes(numericIndexContext))
            {
                Set<File> numericIndexFiles = componentFiles(indexFiles,
                                                             new Component(Component.Type.CUSTOM,
                                                                           Version.current(KEYSPACE)
                                                                                  .fileNameFormatter()
                                                                                  .format(indexComponentType, numericIndexContext, 0)));
                if (isBuildCompletionMarker(indexComponentType))
                    assertEquals(numericCompletionMarkers, numericIndexFiles.size());
                else
                    assertEquals(numericIndexFiles.toString(), numericFiles, numericIndexFiles.size());
            }
        }
    }

    protected boolean isBuildCompletionMarker(IndexComponentType indexComponentType)
    {
        return (indexComponentType == IndexComponentType.GROUP_COMPLETION_MARKER) ||
               (indexComponentType == IndexComponentType.COLUMN_COMPLETION_MARKER);

    }

    protected Set<File> indexFiles()
    {
        return indexFiles(KEYSPACE, currentTable());
    }

    protected Set<File> indexFiles(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        return cfs.getDirectories().getCFDirectories()
                  .stream()
                  .flatMap(dir -> Arrays.stream(dir.tryList()))
                  .filter(File::isFile)
                  .filter(file -> Version.tryParseFileName(file.name()).isPresent())
                  .collect(Collectors.toSet());
    }

    /**
     * Checks that the set of all SAI index files in the TOC for all sstables (of the {@link #currentTable()}) are
     * exactly the provided files.
     *
     * @param files expected SAI index files (typically the result of {@link #indexFiles()} above). Should not contain
     *              non-SAI sstable files (or the test will fail).
     */
    protected void assertIndexFilesInToc(Set<File> files) throws IOException
    {
        Set<String> found = files.stream().map(File::name).collect(Collectors.toSet());
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            for (Component component : SSTable.readTOC(sstable.descriptor, false))
            {
                if (component.type != Component.Type.CUSTOM || !component.name.startsWith(Version.SAI_DESCRIPTOR))
                    continue;

                String tocFile = sstable.descriptor.fileFor(component).name();
                if (!found.remove(tocFile))
                    fail(String.format("TOC of %s contains unexpected SAI index file %s (all expected: %s)", sstable, tocFile, files));
            }
        }
        assertTrue("The following files could not be found in the sstable TOC files: " + found, found.isEmpty());
    }

    protected ObjectName bufferSpaceObjectName(String name) throws MalformedObjectNameException
    {
        return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,name=%s", name));
    }

    protected long getSegmentBufferSpaceLimit() throws Exception
    {
        ObjectName limitBytesName = bufferSpaceObjectName("SegmentBufferSpaceLimitBytes");
        return (long) (Long) getMetricValue(limitBytesName);
    }

    protected Object getSegmentBufferUsedBytes() throws Exception
    {
        ObjectName usedBytesName = bufferSpaceObjectName("SegmentBufferSpaceUsedBytes");
        return getMetricValue(usedBytesName);
    }

    protected Object getColumnIndexBuildsInProgress() throws Exception
    {
        ObjectName buildersInProgressName = bufferSpaceObjectName("ColumnIndexBuildsInProgress");
        return getMetricValue(buildersInProgressName);
    }

    protected void verifySSTableIndexes(String indexName, int count)
    {
        try
        {
            verifySSTableIndexes(indexName, count, count);
        }
        catch (Exception e)
        {
            throw Throwables.unchecked(e);
        }
    }

    protected void verifySSTableIndexes(String indexName, int sstableContextCount, int sstableIndexCount)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
        int contextCount = indexGroup == null ? 0 : indexGroup.sstableContextManager().size();
        assertEquals("Expected " + sstableContextCount +" SSTableContexts, but got " + contextCount, sstableContextCount, contextCount);

        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
        Collection<SSTableIndex> sstableIndexes = sai == null ? Collections.emptyList() : sai.getIndexContext().getView().getIndexes();
        assertEquals("Expected " + sstableIndexCount +" SSTableIndexes, but got " + sstableIndexes.toString(), sstableIndexCount, sstableIndexes.size());
    }

    protected void truncate(boolean snapshot)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        if (snapshot)
            cfs.truncateBlocking();
        else
            cfs.truncateBlockingWithoutSnapshot();
    }

    protected Set<String> listIndexNames(ColumnFamilyStore cfs)
    {
        return cfs.indexManager.listIndexes().stream().map(i -> i.getIndexMetadata().name).collect(Collectors.toSet());
    }

    protected void rebuildTableIndexes()
    {
        rebuildTableIndexes(KEYSPACE, currentTable());
    }

    protected void rebuildTableIndexes(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.indexManager.rebuildIndexesBlocking(listIndexNames(cfs));
    }

    protected void rebuildIndexes(String... indexes)
    {
        ColumnFamilyStore.rebuildSecondaryIndex(KEYSPACE, currentTable(), indexes);
    }

    protected void reloadSSTableIndex()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        StorageAttachedIndexGroup.getIndexGroup(cfs).unsafeReload();
    }

    protected void reloadSSTableIndexInPlace()
    {
        reloadSSTableIndexInPlace(KEYSPACE, currentTable());
    }

    // `reloadSSTalbleIndex` calls `unsafeReload`, which clear all contexts, and then recreate from scratch. This method
    // simply signal updates to every sstable without previously clearing anything.
    protected void reloadSSTableIndexInPlace(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        group.onSSTableChanged(Collections.emptySet(), cfs.getLiveSSTables(), group.getIndexes(), true);
    }

    protected void runInitializationTask() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
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

    protected int snapshot(String snapshotName)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Set<SSTableReader> snapshottedSSTables = cfs.snapshot(snapshotName);
        return snapshottedSSTables.size();
    }

    protected List<String> restoreSnapshot(String snapshot)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(snapshot);
        return restore(cfs, lister);
    }

    protected List<String> restore(ColumnFamilyStore cfs, Directories.SSTableLister lister)
    {
        File dataDirectory = cfs.getDirectories().getDirectoryForNewSSTables();

        List<String> fileNames = new ArrayList<>();
        for (File file : lister.listFiles())
        {
            if (file.tryMove(new File(dataDirectory.absolutePath() + File.pathSeparator() + file.name())))
            {
                fileNames.add(file.name());
            }
        }
        cfs.loadNewSSTables();
        return fileNames;
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
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            Set<Component> components = sstable.components();
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            Set<Component> ndiComponents = group == null ? Collections.emptySet() : group.activeComponents(sstable);

            Set<Component> diff = Sets.difference(ndiComponents, components);
            if (indexComponentsExist)
                assertTrue("Expect all index components are tracked by SSTable, but " + diff + " are not included.",
                           !ndiComponents.isEmpty() && diff.isEmpty());
            else
                assertFalse("Expect no index components, but got " + components, components.toString().contains("SAI"));

            Set<Component> tocContents = SSTable.readTOC(sstable.descriptor);
            assertEquals(components, tocContents);
        }
    }

    protected Set<File> componentFiles(Collection<File> indexFiles, Component component)
    {
        return indexFiles.stream().filter(c -> c.name().endsWith(component.name)).collect(Collectors.toSet());
    }

    protected Set<File> componentFiles(Collection<File> indexFiles, IndexComponentType indexComponentType, IndexContext indexContext)
    {
        String componentName = Version.current(KEYSPACE).fileNameFormatter().format(indexComponentType, indexContext, 0);
        return indexFiles.stream().filter(c -> c.name().endsWith(componentName)).collect(Collectors.toSet());
    }

    public void disableQueryOptimization()
    {
        QueryController.QUERY_OPT_LEVEL = 0;
    }

    /**
     * Run repeated verification task concurrently with target test
     */
    protected static class TestWithConcurrentVerification
    {
        private final Runnable verificationTask;
        private final CountDownLatch verificationStarted = new CountDownLatch(1);

        private final Runnable targetTask;
        private final CountDownLatch taskCompleted = new CountDownLatch(1);

        private final int verificationIntervalInMs;
        private static final int verificationMaxInMs = 300_000; // 300s

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
            AtomicReference<RuntimeException> verificationExeption = new AtomicReference<>();
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
                        verificationExeption.set(Throwables.unchecked(e));
                        return;
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
                RuntimeException rte = verificationExeption.get();
                if (rte != null)
                    throw rte;
            }
            catch (InterruptedException e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }

    protected PlanSelectionAssertion assertThatPlanFor(String query, Object[]... expectedRows)
    {
        return assertThatPlanFor(query, rs -> assertRowsIgnoringOrder(rs, expectedRows));
    }

    protected PlanSelectionAssertion assertThatPlanFor(String query, int numExpectedRows)
    {
        return assertThatPlanFor(query, rs -> Assertions.assertThat(rs.size()).isEqualTo(numExpectedRows));
    }

    private PlanSelectionAssertion assertThatPlanFor(String query, Consumer<UntypedResultSet> resultSetConsumer)
    {
        // First execute the query capturing warnings and check the query results
        disablePreparedReuseForTest();
        ClientWarn.instance.captureWarnings();
        resultSetConsumer.accept(execute(query));
        List<String> warnings = ClientWarn.instance.getWarnings();
        ClientWarn.instance.resetWarnings();

        ReadCommand command = parseReadCommand(query);
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        if (queryPlan == null)
            return new PlanSelectionAssertion(null, warnings);

        StorageAttachedIndexQueryPlan saiQueryPlan = (StorageAttachedIndexQueryPlan) queryPlan;
        Assertions.assertThat(saiQueryPlan).isNotNull();

        StorageAttachedIndexSearcher searcher = saiQueryPlan.searcherFor(command);
        try
        {
            return new PlanSelectionAssertion(searcher.buildPlan(), warnings);
        }
        finally
        {
            searcher.abort();
        }
    }

    protected static class PlanSelectionAssertion
    {
        private final double expectedRows;
        private final Set<String> selectedIndexes;
        private final List<String> warnings;

        public PlanSelectionAssertion(@Nullable Plan.RowsIteration plan, @Nullable List<String> warnings)
        {
            this.expectedRows = plan != null ? plan.expectedRows() : -1.0;
            this.selectedIndexes = plan != null ? plannedIndexes(plan) : Set.of();
            this.warnings = warnings;
        }

        private Set<String> plannedIndexes(Plan plan)
        {
            Set<String> indexes = new HashSet<>();
            plan.visitIndexesRecursive(i -> indexes.add(i.getIndexName()));
            return indexes;
        }

        public PlanSelectionAssertion hasEstimatedRowsCountBetween(double minRows, double maxRows)
        {
            Assertions.assertThat(expectedRows)
                      .as("Expected rows to be between %s and %s, but got: %s", minRows, maxRows, expectedRows)
                      .isBetween(minRows, maxRows);
            return this;
        }

        public PlanSelectionAssertion uses(String... indexes)
        {
            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select only %s, but got: %s", indexes, selectedIndexes)
                      .isEqualTo(Set.of(indexes));
            return this;
        }

        public PlanSelectionAssertion usesNone()
        {
            Assertions.assertThat(selectedIndexes).isEmpty();
            return this;
        }

        public PlanSelectionAssertion usesAnyOf(String index1, String index2, String... otherIndexes)
        {
            Set<String> expectedIndexes = new HashSet<>(otherIndexes.length + 1);
            expectedIndexes.add(index1);
            expectedIndexes.add(index2);
            expectedIndexes.addAll(Arrays.asList(otherIndexes));

            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select any of %s, but got: %s", expectedIndexes, selectedIndexes)
                      .containsAnyElementsOf(expectedIndexes);
            return this;
        }

        public PlanSelectionAssertion usesAtLeast(String... indexes)
        {
            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select at least %s, but got: %s", indexes, selectedIndexes)
                      .containsAll(Set.of(indexes));
            return this;
        }

        public PlanSelectionAssertion uses(int numIndexes)
        {
            Assertions.assertThat(selectedIndexes).hasSize(numIndexes);
            return this;
        }

        public void doesntWarn()
        {
            Assert.assertNull(warnings);
        }

        public void warns(String expectedWarning)
        {
            Assert.assertNotNull(warnings);
            Assert.assertEquals(1, warnings.size());
            Assert.assertEquals(expectedWarning, warnings.get(0));
        }
    }

    /**
     * {@link CQLTester} parameterized for {@link Version#ALL}.
     */
    @Ignore
    @RunWith(Parameterized.class)
    public abstract static class Versioned extends SAITester
    {
        @Parameterized.Parameter
        public Version version;

        @Parameterized.Parameters(name = "{0}")
        public static Collection<Object[]> data()
        {
            // See Version file for explanation of changes associated with each version
            return Version.ALL.stream()
                              .map(v -> new Object[]{ v })
                              .collect(Collectors.toList());
        }

        @Before
        public void setCurrentVersion() throws Throwable
        {
            SAIUtil.setCurrentVersion(version);
        }

        /**
         * {@link CQLTester} parameterized for {@link Version#ALL}, which are row-aware.
         */
        public abstract static class RowAware extends Versioned
        {
            @Parameterized.Parameters(name = "{0}")
            public static Collection<Object[]> data()
            {
                // See Version file for explanation of changes associated with each version
                return Version.ALL.stream()
                                  .filter(v -> v.onDiskFormat().indexFeatureSet().isRowAware())
                                  .map(v -> new Object[]{ v })
                                  .collect(Collectors.toList());
            }
        }
    }
}
