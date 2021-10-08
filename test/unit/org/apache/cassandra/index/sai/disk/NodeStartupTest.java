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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ObjectArrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.schema.SchemaManager;

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

    private static final Injections.Counter deleteComponentCounter = Injections.newCounter("deletedComponentCounter")
                                                                               .add(InvokePointBuilder.newInvokePoint().onClass(IndexComponents.class).onMethod("deleteComponent").atEntry())
                                                                               .build();

    private static final Injections.Counter[] counters = new Injections.Counter[] { buildCounter, deleteComponentCounter };

    private static Throwable error = null;

    private String indexName = null;

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
        PER_COLUMN_CORRUPT;
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
        createTable("CREATE TABLE %s (id text PRIMARY KEY, v1 int)");
        indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        Injections.inject(ObjectArrays.concat(barriers, counters, Injection.class));
        Stream.of(barriers).forEach(Injections.Barrier::reset);
        Stream.of(barriers).forEach(Injections.Barrier::disable);
        Stream.of(counters).forEach(Injections.Counter::reset);
        Stream.of(counters).forEach(Injection::enable);
        error = null;
    }

    @Parameterized.Parameter(0)
    public Populator populator;
    @Parameterized.Parameter(1)
    public IndexStateOnRestart state;
    @Parameterized.Parameter(2)
    public StartupTaskRunOrder order;
    @Parameterized.Parameter(3)
    public int builds;
    @Parameterized.Parameter(4)
    public int deletedComponents;
    @Parameterized.Parameter(5)
    public int expectedDocuments;

    @SuppressWarnings("unused")
    @Parameterized.Parameters(name = "{0} {1} {2}")
    public static List<Object[]> startupScenarios()
    {
        List<Object[]> scenarios = new LinkedList<>();

        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.ALL_EMPTY, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.ALL_EMPTY, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.ALL_EMPTY, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 2, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 9, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 9, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 9, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 5, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 5, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_INCOMPLETE, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 5, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 10, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 10, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_SSTABLE_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 10, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 1, 6, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 1, 6, DOCS });
        scenarios.add( new Object[] { Populator.INDEXABLE_ROWS, IndexStateOnRestart.PER_COLUMN_CORRUPT, StartupTaskRunOrder.PRE_JOIN_RUNS_MID_BUILD, 1, 6, DOCS });
        scenarios.add( new Object[] { Populator.NON_INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, 0 });
        scenarios.add( new Object[] { Populator.NON_INDEXABLE_ROWS, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, 0 });
        scenarios.add( new Object[] { Populator.TOMBSTONES, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_BEFORE_BUILD, 0, 0, 0 });
        scenarios.add( new Object[] { Populator.TOMBSTONES, IndexStateOnRestart.VALID, StartupTaskRunOrder.PRE_JOIN_RUNS_AFTER_BUILD, 0, 0, 0 });

        return scenarios;
    }

    @Test
    public void startupOrderingTest() throws Throwable
    {
        populator.populate(this);

        assertTrue(isIndexQueryable());
        assertTrue(isGroupIndexComplete());
        assertTrue(isColumnIndexComplete());
        Assert.assertEquals(expectedDocuments, execute("SELECT * FROM %s WHERE v1 >= 0").size());

        setState(state);

        order.enable();

        simulateNodeRestart();

        assertTrue(isIndexQueryable());
        assertTrue(isGroupIndexComplete());
        assertTrue(isColumnIndexComplete());
        Assert.assertEquals(expectedDocuments, execute("SELECT * FROM %s WHERE v1 >= 0").size());

        Assert.assertEquals(builds, buildCounter.get());
        Assert.assertEquals(deletedComponents, deleteComponentCounter.get());
    }

    public void populateIndexableRows()
    {
        try
        {
            for (int i = 0; i < DOCS; i++)
            {
                execute("INSERT INTO %s (id, v1) VALUES (?, 0)", i);
            }
            flush();
        }
        catch (Throwable e)
        {
            error = e;
            e.printStackTrace();
        }
    }

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

    private boolean isGroupIndexComplete() throws Exception
    {
        ColumnFamilyStore cfs = Objects.requireNonNull(SchemaManager.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(currentTable());
        return cfs.getLiveSSTables().stream().allMatch(sstable -> IndexComponents.isGroupIndexComplete(sstable.descriptor));
    }

    private boolean isColumnIndexComplete() throws Exception
    {
        ColumnFamilyStore cfs = Objects.requireNonNull(SchemaManager.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(currentTable());
        return cfs.getLiveSSTables().stream().allMatch(sstable -> IndexComponents.isColumnIndexComplete(sstable.descriptor, indexName));
    }

    private void setState(IndexStateOnRestart state)
    {
        switch (state)
        {
            case VALID:
                break;
            case ALL_EMPTY:
                allIndexComponents().forEach(this::remove);
                break;
            case PER_SSTABLE_INCOMPLETE:
                remove(IndexComponents.GROUP_COMPLETION_MARKER);
                break;
            case PER_COLUMN_INCOMPLETE:
                remove(IndexComponents.NDIType.COLUMN_COMPLETION_MARKER.newComponent(indexName));
                break;
            case PER_SSTABLE_CORRUPT:
                corrupt(IndexComponents.GROUP_META);
                break;
            case PER_COLUMN_CORRUPT:
                corrupt(IndexComponents.NDIType.META.newComponent(indexName));
                break;
        }
    }

    private Set<Component> allIndexComponents()
    {
        Set<Component> components = new HashSet<>();
        components.addAll(IndexComponents.PER_SSTABLE_COMPONENTS);
        components.addAll(IndexComponents.perColumnComponents(indexName, false));
        return components;
    }

    private void remove(Component component)
    {
        try
        {
            corruptNDIComponent(component, CorruptionType.REMOVED);
        }
        catch (Exception e)
        {
            error = e;
            e.printStackTrace();
        }
    }

    private void corrupt(Component component)
    {
        try
        {
            corruptNDIComponent(component, CorruptionType.TRUNCATED_HEADER);
        }
        catch (Exception e)
        {
            error = e;
            e.printStackTrace();
        }
    }
}
