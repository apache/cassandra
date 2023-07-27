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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.tools.SystemExitException;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;

import static java.lang.String.format;
import static org.apache.cassandra.Util.bulkLoadSSTables;
import static org.apache.cassandra.Util.getBackups;
import static org.apache.cassandra.Util.getSSTables;
import static org.apache.cassandra.Util.getSnapshots;
import static org.apache.cassandra.Util.relativizePath;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.LEGACY_SSTABLE_ACTIVITY;
import static org.apache.cassandra.db.SystemKeyspace.SSTABLE_ACTIVITY_V2;
import static org.apache.cassandra.distributed.shared.FutureUtils.waitOn;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.assertj.core.api.Assertions.assertThat;

public class SSTableIdGenerationTest extends TestBaseImpl
{
    private final static String ENABLE_UUID_FIELD_NAME = "uuid_sstable_identifiers_enabled";
    private final static String SNAPSHOT_TAG = "test";

    private int v;

    private static SecurityManager originalSecurityManager;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();

        originalSecurityManager = System.getSecurityManager();
        // we prevent system exit and convert it to exception becuase this is one of the expected test outcomes,
        // and we want to make an assertion on that
        ClusterUtils.preventSystemExit();
    }

    @AfterClass
    public static void afterClass() throws Throwable
    {
        System.setSecurityManager(originalSecurityManager);
    }

    /**
     * This test verifies that a node with uuid disabled actually creates sstables with sequential ids and
     * both the current and legacy sstable activity tables are updated.
     * Then, when enable uuid, we actually create sstables with uuid but keep and can read the old sstables. Also, only
     * update the current sstable activity table.
     */
    @Test
    public void testRestartWithUUIDEnabled() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.set(ENABLE_UUID_FIELD_NAME, false))
                                           .start()))
        {
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl", null));
            createSSTables(cluster.get(1), KEYSPACE, "tbl", 1, 2);
            assertSSTablesCount(cluster.get(1), 2, 0, KEYSPACE, "tbl");
            verfiySSTableActivity(cluster, true);

            restartNode(cluster, 1, true);

            createSSTables(cluster.get(1), KEYSPACE, "tbl", 3, 4);
            assertSSTablesCount(cluster.get(1), 2, 2, KEYSPACE, "tbl");
            verfiySSTableActivity(cluster, false);

            checkRowsNumber(cluster.get(1), KEYSPACE, "tbl", 9);
        }
    }

    /**
     * This test verifies that we should not be able to start a node with uuid disabled when there are uuid sstables
     */
    @Test
    public void testRestartWithUUIDDisabled() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.set(ENABLE_UUID_FIELD_NAME, true))
                                           .start()))
        {
            cluster.disableAutoCompaction(KEYSPACE);
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl", null));
            createSSTables(cluster.get(1), KEYSPACE, "tbl", 1, 2);
            assertSSTablesCount(cluster.get(1), 0, 2, KEYSPACE, "tbl");
            verfiySSTableActivity(cluster, false);

            Assertions.assertThatExceptionOfType(RuntimeException.class)
                      .isThrownBy(() -> restartNode(cluster, 1, false))
                      .withCauseInstanceOf(SystemExitException.class);
        }
    }

    @Test
    public final void testCompactionStrategiesWithMixedSSTables() throws Exception
    {
        testCompactionStrategiesWithMixedSSTables(SizeTieredCompactionStrategy.class,
                                                  TimeWindowCompactionStrategy.class,
                                                  LeveledCompactionStrategy.class);
    }

    /**
     * The purpose of this test is to verify that we can compact using the given strategy the mix of sstables created
     * with sequential id and with uuid. Then we verify whether the number results matches the number of rows which we
     * would get by merging data from the initial sstables.
     */
    @SafeVarargs
    private final void testCompactionStrategiesWithMixedSSTables(final Class<? extends AbstractCompactionStrategy>... compactionStrategyClasses) throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.set(ENABLE_UUID_FIELD_NAME, false))
                                           .start()))
        {
            // create a table and two sstables with sequential id for each strategy, the sstables will contain overlapping partitions
            for (Class<? extends AbstractCompactionStrategy> compactionStrategyClass : compactionStrategyClasses)
            {
                String tableName = "tbl_" + compactionStrategyClass.getSimpleName().toLowerCase();
                cluster.schemaChange(createTableStmt(KEYSPACE, tableName, compactionStrategyClass));

                createSSTables(cluster.get(1), KEYSPACE, tableName, 1, 2);
                assertSSTablesCount(cluster.get(1), 2, 0, KEYSPACE, tableName);
            }

            // restart the node with uuid enabled
            restartNode(cluster, 1, true);

            // create another two sstables with uuid for each previously created table
            for (Class<? extends AbstractCompactionStrategy> compactionStrategyClass : compactionStrategyClasses)
            {
                String tableName = "tbl_" + compactionStrategyClass.getSimpleName().toLowerCase();

                createSSTables(cluster.get(1), KEYSPACE, tableName, 3, 4);

                // expect to have a mix of sstables with sequential id and uuid
                assertSSTablesCount(cluster.get(1), 2, 2, KEYSPACE, tableName);

                // after compaction, we expect to have a single sstable with uuid
                cluster.get(1).forceCompact(KEYSPACE, tableName);
                assertSSTablesCount(cluster.get(1), 0, 1, KEYSPACE, tableName);

                // verify the number of rows
                checkRowsNumber(cluster.get(1), KEYSPACE, tableName, 9);
            }
        }
    }

    @Test
    public void testStreamingToNodeWithUUIDEnabled() throws Exception
    {
        testStreaming(true);
    }

    @Test
    public void testStreamingToNodeWithUUIDDisabled() throws Exception
    {
        testStreaming(false);
    }

    /**
     * The purpose of this test case is to verify the scenario when we need to stream mixed UUID and seq sstables to
     * a node which have: 1) UUID disabled, and 2) UUID enabled; then verify that we can read all the data properly
     * from that node alone.
     */
    private void testStreaming(boolean uuidEnabledOnTargetNode) throws Exception
    {
        // start both nodes with uuid disabled
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.set(ENABLE_UUID_FIELD_NAME, false).with(Feature.NETWORK))
                                           .start()))
        {
            // create an empty table and shutdown nodes 2, 3
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl", null));
            waitOn(cluster.get(2).shutdown());

            // create 2 sstables with overlapping partitions on node 1 (with seq ids)
            createSSTables(cluster.get(1), KEYSPACE, "tbl", 1, 2);

            // restart node 1 with uuid enabled
            restartNode(cluster, 1, true);

            // create 2 sstables with overlapping partitions on node 1 (with UUID ids)
            createSSTables(cluster.get(1), KEYSPACE, "tbl", 3, 4);

            assertSSTablesCount(cluster.get(1), 2, 2, KEYSPACE, "tbl");

            // now start node with UUID disabled and perform repair
            cluster.get(2).config().set(ENABLE_UUID_FIELD_NAME, uuidEnabledOnTargetNode);
            cluster.get(2).startup();

            assertSSTablesCount(cluster.get(2), 0, 0, KEYSPACE, "tbl");

            // at this point we have sstables with seq and uuid on nodes and no sstables on node
            // when we run repair, we expect streaming all 4 sstables from node 1 to node 2

            cluster.get(2).nodetool("repair", KEYSPACE);

            if (uuidEnabledOnTargetNode)
                assertSSTablesCount(cluster.get(2), 0, 4, KEYSPACE, "tbl");
            else
                assertSSTablesCount(cluster.get(2), 4, 0, KEYSPACE, "tbl");

            waitOn(cluster.get(1).shutdown());

            checkRowsNumber(cluster.get(2), KEYSPACE, "tbl", 9);
        }
    }

    @Test
    public void testSnapshot() throws Exception
    {
        File tmpDir = new File(Files.createTempDirectory("test"));
        Set<String> seqOnlyBackupDirs;
        Set<String> seqAndUUIDBackupDirs;
        Set<String> uuidOnlyBackupDirs;
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.with(Feature.NETWORK)
                                                                       .set("incremental_backups", true)
                                                                       .set("snapshot_before_compaction", false)
                                                                       .set("auto_snapshot", false)
                                                                       .set(ENABLE_UUID_FIELD_NAME, false))
                                           .start()))
        {
            // create the tables

            cluster.schemaChange("CREATE KEYSPACE new_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl_seq_only", null));
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl_seq_and_uuid", null));
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl_uuid_only", null));
            cluster.schemaChange(createTableStmt("new_ks", "tbl_seq_only", null));
            cluster.schemaChange(createTableStmt("new_ks", "tbl_seq_and_uuid", null));
            cluster.schemaChange(createTableStmt("new_ks", "tbl_uuid_only", null));

            // creating sstables
            createSSTables(cluster.get(1), KEYSPACE, "tbl_seq_only", 1, 2, 3, 4);
            createSSTables(cluster.get(1), KEYSPACE, "tbl_seq_and_uuid", 1, 2);
            createSSTables(cluster.get(1), "new_ks", "tbl_seq_only", 5, 6, 7, 8);
            createSSTables(cluster.get(1), "new_ks", "tbl_seq_and_uuid", 5, 6);

            restartNode(cluster, 1, true);

            createSSTables(cluster.get(1), KEYSPACE, "tbl_seq_and_uuid", 3, 4);
            createSSTables(cluster.get(1), KEYSPACE, "tbl_uuid_only", 1, 2, 3, 4);
            createSSTables(cluster.get(1), "new_ks", "tbl_seq_and_uuid", 7, 8);
            createSSTables(cluster.get(1), "new_ks", "tbl_uuid_only", 5, 6, 7, 8);

            Set<String> seqOnlySnapshotDirs = snapshot(cluster.get(1), KEYSPACE, "tbl_seq_only");
            Set<String> seqAndUUIDSnapshotDirs = snapshot(cluster.get(1), KEYSPACE, "tbl_seq_and_uuid");
            Set<String> uuidOnlySnapshotDirs = snapshot(cluster.get(1), KEYSPACE, "tbl_uuid_only");

            seqOnlyBackupDirs = getBackupDirs(cluster.get(1), KEYSPACE, "tbl_seq_only");
            seqAndUUIDBackupDirs = getBackupDirs(cluster.get(1), KEYSPACE, "tbl_seq_and_uuid");
            uuidOnlyBackupDirs = getBackupDirs(cluster.get(1), KEYSPACE, "tbl_uuid_only");

            // at this point, we should have sstables with backups and snapshots for all tables
            assertSSTablesCount(cluster.get(1), 4, 0, KEYSPACE, "tbl_seq_only");
            assertSSTablesCount(cluster.get(1), 2, 2, KEYSPACE, "tbl_seq_and_uuid");
            assertSSTablesCount(cluster.get(1), 0, 4, KEYSPACE, "tbl_uuid_only");

            assertBackupSSTablesCount(cluster.get(1), 4, 0, KEYSPACE, "tbl_seq_only");
            assertBackupSSTablesCount(cluster.get(1), 2, 2, KEYSPACE, "tbl_seq_and_uuid");
            assertBackupSSTablesCount(cluster.get(1), 0, 4, KEYSPACE, "tbl_uuid_only");

            assertSnapshotSSTablesCount(cluster.get(1), 4, 0, KEYSPACE, "tbl_seq_only");
            assertSnapshotSSTablesCount(cluster.get(1), 2, 2, KEYSPACE, "tbl_seq_and_uuid");
            assertSnapshotSSTablesCount(cluster.get(1), 0, 4, KEYSPACE, "tbl_uuid_only");

            checkRowsNumber(cluster.get(1), KEYSPACE, "tbl_seq_only", 9);
            checkRowsNumber(cluster.get(1), KEYSPACE, "tbl_seq_and_uuid", 9);
            checkRowsNumber(cluster.get(1), KEYSPACE, "tbl_uuid_only", 9);

            // truncate the first set of tables
            truncateAndAssertEmpty(cluster.get(1), KEYSPACE, "tbl_seq_only", "tbl_seq_and_uuid", "tbl_uuid_only");

            restore(cluster.get(1), seqOnlySnapshotDirs, "tbl_seq_only", 9);
            restore(cluster.get(1), seqAndUUIDSnapshotDirs, "tbl_seq_and_uuid", 9);
            restore(cluster.get(1), uuidOnlySnapshotDirs, "tbl_uuid_only", 9);

            truncateAndAssertEmpty(cluster.get(1), KEYSPACE, "tbl_seq_only", "tbl_seq_and_uuid", "tbl_uuid_only");

            restore(cluster.get(1), seqOnlyBackupDirs, "tbl_seq_only", 9);
            restore(cluster.get(1), seqAndUUIDBackupDirs, "tbl_seq_and_uuid", 9);
            restore(cluster.get(1), uuidOnlyBackupDirs, "tbl_uuid_only", 9);

            ImmutableSet<String> allBackupDirs = ImmutableSet.<String>builder().addAll(seqOnlyBackupDirs).addAll(seqAndUUIDBackupDirs).addAll(uuidOnlyBackupDirs).build();
            cluster.get(1).runOnInstance(rethrow(() -> allBackupDirs.forEach(dir -> bulkLoadSSTables(new File(dir), "new_ks"))));

            checkRowsNumber(cluster.get(1), "new_ks", "tbl_seq_only", 17);
            checkRowsNumber(cluster.get(1), "new_ks", "tbl_seq_and_uuid", 17);
            checkRowsNumber(cluster.get(1), "new_ks", "tbl_uuid_only", 17);


            for (String dir : allBackupDirs)
            {
                File src = new File(dir);
                File dest = relativizePath(tmpDir, src, 3);
                Files.createDirectories(dest.parent().toPath());
                FileUtils.moveDirectory(src.toJavaIOFile(), dest.toJavaIOFile());
            }
        }

        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.with(Feature.NETWORK, Feature.NATIVE_PROTOCOL)
                                                                       .set("incremental_backups", true)
                                                                       .set("snapshot_before_compaction", false)
                                                                       .set("auto_snapshot", false)
                                                                       .set(ENABLE_UUID_FIELD_NAME, false))
                                           .start()))
        {
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl_seq_only", null));
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl_seq_and_uuid", null));
            cluster.schemaChange(createTableStmt(KEYSPACE, "tbl_uuid_only", null));

            Function<String, String> relativeToTmpDir = d -> relativizePath(tmpDir, new File(d), 3).toString();
            restore(cluster.get(1), seqOnlyBackupDirs.stream().map(relativeToTmpDir).collect(Collectors.toSet()), "tbl_seq_only", 9);
            restore(cluster.get(1), seqAndUUIDBackupDirs.stream().map(relativeToTmpDir).collect(Collectors.toSet()), "tbl_seq_and_uuid", 9);
            restore(cluster.get(1), uuidOnlyBackupDirs.stream().map(relativeToTmpDir).collect(Collectors.toSet()), "tbl_uuid_only", 9);
        }
    }

    private static void restore(IInvokableInstance instance, Set<String> dirs, String targetTableName, int expectedRowsNum)
    {
        List<String> failedImports = instance.callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, targetTableName)
                                                                                    .importNewSSTables(dirs, false, false, true, true, true, true, true));
        assertThat(failedImports).isEmpty();
        checkRowsNumber(instance, KEYSPACE, targetTableName, expectedRowsNum);
    }

    private static void truncateAndAssertEmpty(IInvokableInstance instance, String ks, String... tableNames)
    {
        for (String tableName : tableNames)
        {
            instance.executeInternal(format("TRUNCATE %s.%s", ks, tableName));
            assertSSTablesCount(instance, 0, 0, ks, tableName);
            checkRowsNumber(instance, ks, tableName, 0);
        }
    }

    private static Set<String> snapshot(IInvokableInstance instance, String ks, String tableName)
    {
        Set<String> snapshotDirs = instance.callOnInstance(() -> ColumnFamilyStore.getIfExists(ks, tableName)
                                                                                  .snapshot(SNAPSHOT_TAG)
                                                                                  .getDirectories()
                                                                                  .stream()
                                                                                  .map(File::toString)
                                                                                  .collect(Collectors.toSet()));
        assertThat(snapshotDirs).isNotEmpty();
        return snapshotDirs;
    }

    private static String createTableStmt(String ks, String name, Class<? extends AbstractCompactionStrategy> compactionStrategy)
    {
        if (compactionStrategy == null)
            compactionStrategy = SizeTieredCompactionStrategy.class;
        return format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) " +
                      "WITH compaction = {'class':'%s', 'enabled':'false'}",
                      ks, name, compactionStrategy.getCanonicalName());
    }

    private void createSSTables(IInstance instance, String ks, String tableName, int... records)
    {
        String insert = format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)", ks, tableName);
        for (int record : records)
        {
            instance.executeInternal(insert, record, record, ++v);
            instance.executeInternal(insert, record, record + 1, ++v);
            instance.executeInternal(insert, record + 1, record + 1, ++v);
            instance.flush(ks);
        }
    }

    private static void assertSSTablesCount(Set<Descriptor> descs, String tableName, int expectedSeqGenIds, int expectedUUIDGenIds)
    {
        List<String> seqSSTables = descs.stream()
                                        .filter(desc -> desc.id instanceof SequenceBasedSSTableId)
                                        .map(descriptor -> descriptor.baseFile().toString())
                                        .sorted()
                                        .collect(Collectors.toList());
        List<String> uuidSSTables = descs.stream()
                                         .filter(desc -> desc.id instanceof UUIDBasedSSTableId)
                                         .map(descriptor -> descriptor.baseFile().toString())
                                         .sorted()
                                         .collect(Collectors.toList());
        assertThat(seqSSTables).describedAs("SSTables of %s with sequence based id", tableName).hasSize(expectedSeqGenIds);
        assertThat(uuidSSTables).describedAs("SSTables of %s with UUID based id", tableName).hasSize(expectedUUIDGenIds);
    }

    private static void assertSSTablesCount(IInvokableInstance instance, int expectedSeqGenIds, int expectedUUIDGenIds, String ks, String... tableNames)
    {
        instance.runOnInstance(rethrow(() -> Arrays.stream(tableNames).forEach(tableName -> assertSSTablesCount(getSSTables(ks, tableName), tableName, expectedSeqGenIds, expectedUUIDGenIds))));
    }

    private static void assertSnapshotSSTablesCount(IInvokableInstance instance, int expectedSeqGenIds, int expectedUUIDGenIds, String ks, String... tableNames)
    {
        instance.runOnInstance(rethrow(() -> Arrays.stream(tableNames).forEach(tableName -> assertSSTablesCount(getSnapshots(ks, tableName, SNAPSHOT_TAG), tableName, expectedSeqGenIds, expectedUUIDGenIds))));
    }

    private static void assertBackupSSTablesCount(IInvokableInstance instance, int expectedSeqGenIds, int expectedUUIDGenIds, String ks, String... tableNames)
    {
        instance.runOnInstance(rethrow(() -> Arrays.stream(tableNames).forEach(tableName -> assertSSTablesCount(getBackups(ks, tableName), tableName, expectedSeqGenIds, expectedUUIDGenIds))));
    }

    private static Set<String> getBackupDirs(IInvokableInstance instance, String ks, String tableName)
    {
        return instance.callOnInstance(() -> getBackups(ks, tableName).stream()
                                                                      .map(d -> d.directory)
                                                                      .map(File::toString)
                                                                      .collect(Collectors.toSet()));
    }

    private static void verfiySSTableActivity(Cluster cluster, boolean expectLegacyTableIsPopulated)
    {
        cluster.get(1).runOnInstance(() -> {
            RestorableMeter meter = new RestorableMeter(15, 120);
            SequenceBasedSSTableId seqGenId = new SequenceBasedSSTableId(1);
            SystemKeyspace.persistSSTableReadMeter("ks", "tab", seqGenId, meter);
            assertThat(SystemKeyspace.getSSTableReadMeter("ks", "tab", seqGenId)).matches(m -> m.fifteenMinuteRate() == meter.fifteenMinuteRate()
                                                                                               && m.twoHourRate() == meter.twoHourRate());

            checkSSTableActivityRow(SSTABLE_ACTIVITY_V2, seqGenId.toString(), true);
            if (expectLegacyTableIsPopulated)
                checkSSTableActivityRow(LEGACY_SSTABLE_ACTIVITY, seqGenId.generation, true);

            SystemKeyspace.clearSSTableReadMeter("ks", "tab", seqGenId);

            checkSSTableActivityRow(SSTABLE_ACTIVITY_V2, seqGenId.toString(), false);
            if (expectLegacyTableIsPopulated)
                checkSSTableActivityRow(LEGACY_SSTABLE_ACTIVITY, seqGenId.generation, false);

            UUIDBasedSSTableId uuidGenId = new UUIDBasedSSTableId(TimeUUID.Generator.nextTimeUUID());
            SystemKeyspace.persistSSTableReadMeter("ks", "tab", uuidGenId, meter);
            assertThat(SystemKeyspace.getSSTableReadMeter("ks", "tab", uuidGenId)).matches(m -> m.fifteenMinuteRate() == meter.fifteenMinuteRate()
                                                                                                && m.twoHourRate() == meter.twoHourRate());

            checkSSTableActivityRow(SSTABLE_ACTIVITY_V2, uuidGenId.toString(), true);

            SystemKeyspace.clearSSTableReadMeter("ks", "tab", uuidGenId);

            checkSSTableActivityRow(SSTABLE_ACTIVITY_V2, uuidGenId.toString(), false);
        });
    }

    private static void checkSSTableActivityRow(String table, Object genId, boolean expectExists)
    {
        String tableColName = SSTABLE_ACTIVITY_V2.equals(table) ? "table_name" : "columnfamily_name";
        String idColName = SSTABLE_ACTIVITY_V2.equals(table) ? "id" : "generation";
        String cql = "SELECT rate_15m, rate_120m FROM system.%s WHERE keyspace_name=? and %s=? and %s=?";
        UntypedResultSet results = executeInternal(format(cql, table, tableColName, idColName), "ks", "tab", genId);
        assertThat(results).isNotNull();

        if (expectExists)
        {
            assertThat(results.isEmpty()).isFalse();
            UntypedResultSet.Row row = results.one();
            assertThat(row.getDouble("rate_15m")).isEqualTo(15d, Offset.offset(0.001d));
            assertThat(row.getDouble("rate_120m")).isEqualTo(120d, Offset.offset(0.001d));
        }
        else
        {
            assertThat(results.isEmpty()).isTrue();
        }
    }

    private static void restartNode(Cluster cluster, int node, boolean uuidEnabled)
    {
        waitOn(cluster.get(node).shutdown());
        cluster.get(node).config().set(ENABLE_UUID_FIELD_NAME, uuidEnabled);
        cluster.get(node).startup();
    }

    private static void checkRowsNumber(IInstance instance, String ks, String tableName, int expectedNumber)
    {
        SimpleQueryResult result = instance.executeInternalWithResult(format("SELECT * FROM %s.%s", ks, tableName));
        Object[][] rows = result.toObjectArrays();
        assertThat(rows).withFailMessage("Invalid results for %s.%s - should have %d rows but has %d: \n%s", ks, tableName, expectedNumber,
                                         rows.length, result.toString()).hasNumberOfRows(expectedNumber);
    }
}
