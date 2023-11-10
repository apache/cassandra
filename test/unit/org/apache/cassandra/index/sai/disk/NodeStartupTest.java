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
package org.apache.cassandra.index.sai.disk;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.collect.ObjectArrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.Schema;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class NodeStartupTest extends SAITester
{
    private static final int DOCS = 100;

    private static final Injections.Barrier preJoinWaitsForBuild = Injections.newBarrierAwait("pre_join_build", 1, false)
                                                                             .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startPreJoinTask").atEntry())
                                                                             .build();

    private static final Injections.Barrier buildReleasesPreJoin = Injections.newBarrierCountDown("pre_join_build", 1, false)
                                                                             .add(InvokePointBuilder.newInvokePoint().onClass(SecondaryIndexManager.class).onMethod("markIndexBuilt").atExit())
                                                                             .build();

    private static final Injections.Barrier buildWaitsForPreJoin = Injections.newBarrierAwait("build_pre_join", 1, false)
                                                                             .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry())
                                                                             .build();

    private static final Injections.Barrier preJoinReleasesBuild = Injections.newBarrierCountDown("build_pre_join", 1, false)
                                                                             .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startPreJoinTask").atExit())
                                                                             .build();

    private static final Injections.Barrier preJoinStartWaitsMidBuild = Injections.newBarrierAwait("pre_join_mid_build", 1, false)
                                                                                  .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startPreJoinTask").atEntry())
                                                                                  .build();

    private static final Injections.Barrier midBuildReleasesPreJoinStart = Injections.newBarrierCountDown("pre_join_mid_build", 1, false)
                                                                                     .add(InvokePointBuilder.newInvokePoint().onClass(SSTableIndexWriter.class).onMethod("addRow").atEntry())
                                                                                     .build();

    private static final Injections.Barrier midBuildWaitsPreJoinFinish = Injections.newBarrierAwait("mid_build_pre_join", 1, false)
                                                                                   .add(InvokePointBuilder.newInvokePoint().onClass(SSTableIndexWriter.class).onMethod("addRow").atExit())
                                                                                   .build();

    private static final Injections.Barrier preJoinFinishReleasesMidBuild = Injections.newBarrierCountDown("mid_build_pre_join", 1, false)
                                                                                      .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startPreJoinTask").atExit())
                                                                                      .build();

    private static final Injections.Barrier[] barriers = new Injections.Barrier[] { preJoinWaitsForBuild, buildReleasesPreJoin, buildWaitsForPreJoin,
                                                                                    preJoinReleasesBuild, preJoinStartWaitsMidBuild, midBuildReleasesPreJoinStart, midBuildWaitsPreJoinFinish, preJoinFinishReleasesMidBuild
    };

    private static final Injections.Counter buildCounter = Injections.newCounter("buildCounter")
                                                                     .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build").atEntry())
                                                                     .build();

    private static final Injections.Counter deletedPerSStableCounter = Injections.newCounter("deletedPrimaryKeyMapCounter")
                                                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                                                        .onClass(IndexDescriptor.class)
                                                                                                        .onMethod("deletePerSSTableIndexComponents")
                                                                                                        .atEntry())
                                                                                 .build();

    private static final Injections.Counter deletedPerIndexCounter = Injections.newCounter("deletedColumnIndexCounter")
                                                                               .add(InvokePointBuilder.newInvokePoint()
                                                                                                      .onClass(IndexDescriptor.class)
                                                                                                      .onMethod("deleteColumnIndex")
                                                                                                      .atEntry())
                                                                               .build();

    private static final Injections.Counter[] counters = new Injections.Counter[] { buildCounter, deletedPerSStableCounter, deletedPerIndexCounter };

    private static Throwable error = null;

    private IndexIdentifier indexIdentifier = null;
    private IndexTermType indexTermType = null;

    enum Populator
    {
        INDEXABLE_ROWS("populateIndexableRows"),
        NON_INDEXABLE_ROWS("populateNonIndexableRows"),
        TOMBSTONES("populateTombstones");

        private final String populator;

        Populator(String populator)
        {
            this.populator = populator;
        }

        public void populate(NodeStartupTest test)
        {
            try
            {
                test.getClass().getMethod(populator).invoke(test);
            }
            catch (Exception e)
            {
                e.printStackTrace();
                fail("Populator " + name() + " failed because " + e.getLocalizedMessage());
            }
            if (error != null)
            {
                fail("Populator " + name() + " failed because " + error.getLocalizedMessage());
            }
        }
    }

    enum IndexStateOnRestart
    {
        VALID,
        ALL_EMPTY,
        PER_SSTABLE_INCOMPLETE,
        PER_COLUMN_INCOMPLETE,
        PER_SSTABLE_CORRUPT,
        PER_COLUMN_CORRUPT
    }

    enum StartupTaskRunOrder
    {
        PRE_JOIN_RUNS_AFTER_BUILD(preJoinWaitsForBuild, buildReleasesPreJoin),
        PRE_JOIN_RUNS_BEFORE_BUILD(buildWaitsForPreJoin, preJoinReleasesBuild),
        PRE_JOIN_RUNS_MID_BUILD(preJoinStartWaitsMidBuild, midBuildReleasesPreJoinStart, midBuildWaitsPreJoinFinish, preJoinFinishReleasesMidBuild);

        private final Injection[] injections;

        StartupTaskRunOrder(Injections.Barrier... injections)
        {
            this.injections = injections;
        }

        public void enable()
        {
            Stream.of(injections).forEach(Injection::enable);
        }
    }

    @Before
    public void setup() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, v1 text)");
        indexIdentifier = createIndexIdentifier(createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName())));
        indexTermType = createIndexTermType(Int32Type.instance);
        Injections.inject(ObjectArrays.concat(barriers, counters, Injection.class));
        Stream.of(barriers).forEach(Injections.Barrier::reset);
        Stream.of(barriers).forEach(Injections.Barrier::disable);
        Stream.of(counters).forEach(Injections.Counter::reset);
        Stream.of(counters).forEach(Injection::enable);
        error = null;
    }

    @Parameterized.Parameter
    public Populator populator;
    @Parameterized.Parameter(1)
    public IndexStateOnRestart state;
    @Parameterized.Parameter(2)
    public StartupTaskRunOrder order;
    @Parameterized.Parameter(3)
    public int builds;
    @Parameterized.Parameter(4)
    public int deletedPerSSTable;
    @Parameterized.Parameter(5)
    public int deletedPerIndex;
    @Parameterized.Parameter(6)
    public int expectedDocuments;

    @SuppressWarnings("unused")
    @Parameterized.Parameters(name = "{0} {1} {2}")
    public static List<Object[]> startupScenarios()
    {
        List<Object[]> scenarios = new LinkedList<>();

        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, 0, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, 0, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.ALL_EMPTY, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 1, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.ALL_EMPTY, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 1, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.ALL_EMPTY, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 1, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 1, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 1, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 1, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 0, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 0, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 0, 1, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 2, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 2, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 2, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 0, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 0, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 0, 2, DOCS });
        scenarios.add( new Object[] { Populator.NON_INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, 0, 0 });
        scenarios.add( new Object[] { Populator.NON_INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, 0, 0 });
        scenarios.add( new Object[] { Populator.TOMBSTONES, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, 0, 0 });
        scenarios.add( new Object[] { Populator.TOMBSTONES, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, 0, 0 });

        return scenarios;
    }

    @Test
    public void startupOrderingTest()
    {
        populator.populate(this);

        Assertions.assertThat(getNotQueryableIndexes()).isEmpty();
        assertTrue(isGroupIndexComplete());
        assertTrue(isColumnIndexComplete());
        Assert.assertEquals(expectedDocuments, execute("SELECT * FROM %s WHERE v1 = '0'").size());

        setState(state);

        order.enable();

        simulateNodeRestart();

        Assertions.assertThat(getNotQueryableIndexes()).isEmpty();
        assertTrue(isGroupIndexComplete());
        assertTrue(isColumnIndexComplete());
        Assert.assertEquals(expectedDocuments, execute("SELECT * FROM %s WHERE v1 = '0'").size());

        Assert.assertEquals(builds, buildCounter.get());
        Assert.assertEquals(deletedPerSSTable, deletedPerSStableCounter.get());
        Assert.assertEquals(deletedPerIndex, deletedPerIndexCounter.get());
    }

    @SuppressWarnings("unused")
    public void populateIndexableRows()
    {
        try
        {
            for (int i = 0; i < DOCS; i++)
            {
                execute("INSERT INTO %s (id, v1) VALUES (?, '0')", i);
            }
            flush();
        }
        catch (Throwable e)
        {
            error = e;
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unused")
    public void populateNonIndexableRows()
    {
        try
        {
            for (int i = 0; i < DOCS; i++)
            {
                execute("INSERT INTO %s (id) VALUES (?)", i);
            }
            flush();
        }
        catch (Throwable e)
        {
            error = e;
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unused")
    public void populateTombstones()
    {
        try
        {
            for (int i = 0; i < DOCS; i++)
            {
                execute("DELETE FROM %s WHERE id=?", i);
            }
            flush();
        }
        catch (Throwable e)
        {
            error = e;
            e.printStackTrace();
        }
    }

    private boolean isGroupIndexComplete()
    {
        ColumnFamilyStore cfs = Objects.requireNonNull(Schema.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(currentTable());
        return cfs.getLiveSSTables().stream().allMatch(sstable -> IndexDescriptor.create(sstable).isPerSSTableIndexBuildComplete());
    }

    private boolean isColumnIndexComplete()
    {
        ColumnFamilyStore cfs = Objects.requireNonNull(Schema.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(currentTable());
        return cfs.getLiveSSTables().stream().allMatch(sstable -> IndexDescriptor.create(sstable).isPerColumnIndexBuildComplete(indexIdentifier));
    }

    private void setState(IndexStateOnRestart state)
    {
        switch (state)
        {
            case VALID:
                break;
            case ALL_EMPTY:
                Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).forEach(this::remove);
                Version.LATEST.onDiskFormat().perColumnIndexComponents(indexTermType).forEach(c -> remove(c, indexIdentifier));
                break;
            case PER_SSTABLE_INCOMPLETE:
                remove(IndexComponent.GROUP_COMPLETION_MARKER);
                break;
            case PER_COLUMN_INCOMPLETE:
                remove(IndexComponent.COLUMN_COMPLETION_MARKER, indexIdentifier);
                break;
            case PER_SSTABLE_CORRUPT:
                corrupt();
                break;
            case PER_COLUMN_CORRUPT:
                corrupt(indexIdentifier);
                break;
        }
    }

    private void remove(IndexComponent component)
    {
        try
        {
            corruptIndexComponent(component, CorruptionType.REMOVED);
        }
        catch (Exception e)
        {
            error = e;
            e.printStackTrace();
        }
    }

    private void remove(IndexComponent component, IndexIdentifier indexIdentifier)
    {
        try
        {
            corruptIndexComponent(component, indexIdentifier, CorruptionType.REMOVED);
        }
        catch (Exception e)
        {
            error = e;
            e.printStackTrace();
        }
    }

    private void corrupt()
    {
        try
        {
            corruptIndexComponent(IndexComponent.GROUP_META, CorruptionType.TRUNCATED_HEADER);
        }
        catch (Exception e)
        {
            error = e;
            e.printStackTrace();
        }
    }

    private void corrupt(IndexIdentifier indexIdentifier)
    {
        try
        {
            corruptIndexComponent(IndexComponent.META, indexIdentifier, CorruptionType.TRUNCATED_HEADER);
        }
        catch (Exception e)
        {
            error = e;
            e.printStackTrace();
        }
    }
}
