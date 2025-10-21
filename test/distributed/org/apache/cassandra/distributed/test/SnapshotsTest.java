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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.service.snapshot.SnapshotOptions;
import org.apache.cassandra.service.snapshot.SnapshotType;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.config.CassandraRelevantProperties.SNAPSHOT_MANIFEST_ENRICH_OR_CREATE_ENABLED;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.apache.cassandra.schema.SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES;
import static org.apache.cassandra.schema.SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static oshi.PlatformEnum.MACOS;

public class SnapshotsTest extends TestBaseImpl
{
    public static final Integer SNAPSHOT_CLEANUP_PERIOD_SECONDS = 2;
    public static final Integer FIVE_SECONDS = 5;
    public static final Integer TEN_SECONDS = 10;
    private static final WithProperties properties = new WithProperties();
    private static Cluster cluster;

    private final String[] exoticSnapshotNamesOnMac = new String[]{ "snapshot", "snapshots", "backup", "backups",
                                                                    "snapshot.with.dots-and-dashes" };

    private final String[] exoticSnapshotNames = new String[]{ "snapshot", "snapshots", "backup", "backups",
                                                               "Snapshot", "Snapshots", "Backups", "Backup",
                                                               "snapshot.with.dots-and-dashes" };

    @BeforeClass
    public static void before() throws IOException
    {
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS, 0);
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS, SNAPSHOT_CLEANUP_PERIOD_SECONDS);
        properties.set(CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS, FIVE_SECONDS);
        cluster = init(Cluster.build(1)
                              .withDataDirCount(3)
                              .start());
    }

    @After
    public void clearAllSnapshots()
    {
        cluster.schemaChange(withKeyspace("DROP TABLE IF EXISTS %s.tbl;"));
        cluster.schemaChange(withKeyspace("DROP TABLE IF EXISTS %s.tbl2;"));
        cluster.get(1).nodetoolResult("clearsnapshot", "--all").asserts().success();
        for (String tag : new String[]{ "basic", "first", "second", "tag1" })
            waitForSnapshotCleared(tag);
        for (String tag : exoticSnapshotNames)
            waitForSnapshot(tag, false, true);
    }

    @AfterClass
    public static void after()
    {
        properties.close();
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testEverySnapshotDirHasManifestAndSchema()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));
        String[] dataDirs = (String[]) cluster.get(1).config().get("data_file_directories");
        String tableId = cluster.get(1).callOnInstance((SerializableCallable<String>) () -> {
            return ColumnFamilyStore.getIfExists("distributed_test_keyspace", "tbl").metadata().id.toHexString();
        });

        cluster.get(1)
               .nodetoolResult("snapshot", "-t", "mysnapshot", "-kt", format("%s.tbl", KEYSPACE))
               .asserts()
               .success();

        for (String dataDir : dataDirs)
        {
            Path snapshotDir = Paths.get(dataDir)
                                    .resolve(KEYSPACE)
                                    .resolve("tbl-" + tableId)
                                    .resolve("snapshots")
                                    .resolve("mysnapshot");

            assertTrue(snapshotDir.toFile().exists());
            assertTrue(snapshotDir.resolve("manifest.json").toFile().exists());
            assertTrue(snapshotDir.resolve("schema.cql").toFile().exists());
        }
    }

    @Test
    public void testLocalOrReplicatedSystemTablesSnapshotsDoNotHaveSchema()
    {
        cluster.get(1)
               .nodetoolResult("snapshot", "-t", "snapshot_with_local_or_replicated")
               .asserts()
               .success();

        String[] dataDirs = (String[]) cluster.get(1).config().get("data_file_directories");
        String[] paths = getSnapshotPaths(true);

        for (String dataDir : dataDirs)
        {
            for (String path : paths)
            {
                Path snapshotDir = Paths.get(dataDir)
                                        .resolve(path)
                                        .resolve("snapshots")
                                        .resolve("snapshot_with_local_or_replicated");

                if (snapshotDir.toFile().exists())
                    assertFalse(new File(snapshotDir, "schema.cql").exists());
            }
        }
    }

    @Test
    public void testMissingManifestIsCreatedOnStartupWithEnrichmentEnabled()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));
        populate(cluster);

        cluster.get(1)
               .nodetoolResult("snapshot", "-t", "snapshot_with_local_or_replicated")
               .asserts()
               .success();

        String[] dataDirs = (String[]) cluster.get(1).config().get("data_file_directories");
        String[] paths = getSnapshotPaths(false);

        assertManifestsPresence(dataDirs, paths, true);

        // remove all manifest files
        removeAllManifests(dataDirs, paths);
        assertManifestsPresence(dataDirs, paths, false);

        // restart which should add them back
        cluster.get(1).shutdown(true);
        cluster.get(1).startup();

        assertManifestsPresence(dataDirs, paths, true);

        // remove them again and reload by mbean, that should create them too
        removeAllManifests(dataDirs, paths);
        assertManifestsPresence(dataDirs, paths, false);
        cluster.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> SnapshotManager.instance.restart(true));
        assertManifestsPresence(dataDirs, paths, true);

        cluster.get(1).shutdown(true);

        // remove manifest only in the first data dir
        removeAllManifests(new String[]{ dataDirs[0]}, paths);

        // they will be still created for that first dir
        cluster.get(1).startup();
        assertManifestsPresence(dataDirs, paths, true);
    }

    @Test
    public void testMissingManifestIsNotCreatedOnStartupWithEnrichmentDisabled()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));
        populate(cluster);

        cluster.get(1)
               .nodetoolResult("snapshot", "-t", "snapshot_with_local_or_replicated")
               .asserts()
               .success();

        String[] dataDirs = (String[]) cluster.get(1).config().get("data_file_directories");
        String[] paths = getSnapshotPaths(false);

        assertManifestsPresence(dataDirs, paths, true);

        cluster.get(1).shutdown(true);

        // remove all manifest files
        removeAllManifests(dataDirs, paths);
        assertManifestsPresence(dataDirs, paths, false);

        try (WithProperties ignored = new WithProperties().set(SNAPSHOT_MANIFEST_ENRICH_OR_CREATE_ENABLED, false))
        {
            // restart which should NOT add them back because we disabled it by system property
            cluster.get(1).startup();

            // no manifests created
            assertManifestsPresence(dataDirs, paths, false);

            cluster.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> SnapshotManager.instance.restart(true));

            // no manifests created, even after restart of SnapshotManager
            assertManifestsPresence(dataDirs, paths, false);
        }
    }

    @Test
    public void testSnapshotsCleanupByTTL()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));
        populate(cluster, withKeyspace("%s"), "tbl");
        cluster.get(1).nodetoolResult("snapshot", "-kt", withKeyspace("%s.tbl"), "--ttl", format("%ds", FIVE_SECONDS),
                                      "-t", "basic").asserts().success();
        waitForSnapshotPresent("basic");
        waitForSnapshotCleared("basic");
    }

    @Test
    public void testSnapshotCleanupAfterRestart() throws Exception
    {
        int TWENTY_SECONDS = 20; // longer TTL to allow snapshot to survive node restart
        IInvokableInstance instance = cluster.get(1);

        // Create snapshot and check exists
        instance.nodetoolResult("snapshot", "--ttl", format("%ds", TWENTY_SECONDS),
                                "-t", "basic").asserts().success();
        waitForSnapshotPresent("basic");

        // Restart node
        long beforeStop = Clock.Global.currentTimeMillis();
        stopUnchecked(instance);
        instance.startup();
        long afterStart = Clock.Global.currentTimeMillis();

        // if stop & start of the node took more than 20 seconds
        // we assume that the snapshot should be expired by now, so we wait until we do not see it
        if (afterStart - beforeStop > 20_000)
        {
            waitForSnapshotCleared("basic");
            return;
        }
        else
        {
            // Check snapshot still exists after restart
            cluster.get(1).nodetoolResult("listsnapshots").asserts().stdoutContains("basic");
        }

        // Sleep for 2*TTL and then check snapshot is gone
        Thread.sleep(TWENTY_SECONDS * 1000L);
        waitForSnapshotCleared("basic");
    }

    @Test
    public void testSnapshotInvalidArgument() throws Exception
    {
        IInvokableInstance instance = cluster.get(1);

        instance.nodetoolResult("snapshot", "--ttl", format("%ds", 1), "-t", "basic")
                .asserts()
                .failure()
                .stderrContains(format("ttl for snapshot must be at least %d seconds", FIVE_SECONDS));

        instance.nodetoolResult("snapshot", "--ttl", "invalid-ttl").asserts().failure();
    }

    @Test
    public void testListingSnapshotsWithoutTTL()
    {
        // take snapshot without ttl
        cluster.get(1).nodetoolResult("snapshot", "-t", "snapshot_without_ttl").asserts().success();

        // take snapshot with ttl
        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      format("%ds", 1000),
                                      "-t", "snapshot_with_ttl").asserts().success();

        // list snaphots without TTL
        waitForSnapshot("snapshot_without_ttl", true, true);
        waitForSnapshot("snapshot_with_ttl", false, true);

        // list all snapshots
        waitForSnapshotPresent("snapshot_without_ttl");
        waitForSnapshotPresent("snapshot_with_ttl");
    }

    @Test
    public void testManualSnapshotCleanup()
    {
        // take snapshots with ttl
        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      format("%ds", TEN_SECONDS),
                                      "-t", "first").asserts().success();

        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      format("%ds", TEN_SECONDS),
                                      "-t", "second").asserts().success();

        waitForSnapshotPresent("first");
        waitForSnapshotPresent("second");

        cluster.get(1).nodetoolResult("clearsnapshot", "-t", "first").asserts().success();

        waitForSnapshotCleared("first");
        waitForSnapshotPresent("second");

        // wait for the second snapshot to be removed as well
        waitForSnapshotCleared("second");
    }

    @Test
    public void testSecondaryIndexCleanup()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));
        cluster.schemaChange(withKeyspace("CREATE INDEX value_idx ON %s.tbl (value)"));

        populate(cluster);

        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      format("%ds", FIVE_SECONDS),
                                      "-t", "first",
                                      "-kt", withKeyspace("%s.tbl")).asserts().success();

        waitForSnapshotPresent("first");
        waitForSnapshotCleared("first");
    }

    @Test
    public void testListSnapshotOfDroppedTable()
    {
        IInvokableInstance instance = cluster.get(1);

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));

        populate(cluster);

        instance.nodetoolResult("snapshot",
                                "-t", "tag1",
                                "-kt", withKeyspace("%s.tbl")).asserts().success();

        // Check snapshot is listed when table is not dropped
        waitForSnapshotPresent("tag1");

        // Drop Table
        cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl;"));

        // Check snapshot is listed after table is dropped
        waitForSnapshotPresent("tag1");

        // Restart node
        stopUnchecked(instance);
        instance.startup();

        // Check snapshot of dropped table still exists after restart
        waitForSnapshotPresent("tag1");
    }

    @Test
    public void testTTLSnapshotOfDroppedTable()
    {
        IInvokableInstance instance = cluster.get(1);

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));

        populate(cluster);

        instance.nodetoolResult("snapshot",
                                "-t", "tag1",
                                "-kt", withKeyspace("%s.tbl"),
                                "--ttl", format("%ds", FIVE_SECONDS)).asserts().success();

        // Check snapshot is listed when table is not dropped
        instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains("tag1");

        // Drop Table
        cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl;"));

        // Check snapshot is listed after table is dropped
        instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains("tag1");

        // Check snapshot is removed after at most 10s
        await().timeout(2L * FIVE_SECONDS, SECONDS)
               .pollInterval(1, SECONDS)
               .until(() -> !instance.nodetoolResult("listsnapshots").getStdout().contains("tag1"));
    }

    @Test
    public void testTTLSnapshotOfDroppedTableAfterRestart()
    {
        IInvokableInstance instance = cluster.get(1);

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));

        populate(cluster);

        instance.nodetoolResult("snapshot",
                                "-t", "tag1",
                                "-kt", withKeyspace("%s.tbl"),
                                "--ttl", "1h").asserts().success();


        // Check snapshot is listed when table is not dropped
        waitForSnapshotPresent("tag1");

        // Drop Table
        cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl;"));

        // Restart node
        stopUnchecked(instance);
        instance.startup();

        // Check snapshot still exists after restart
        waitForSnapshotPresent("tag1");
    }

    @Test
    public void testExoticSnapshotNames()
    {
        assumeThat(FBUtilities.getSystemInfo().platform(), not(MACOS));
        exoticSnapshotNamesInternal(exoticSnapshotNames);
    }

    @Test
    public void testExoticSnapshotNamesOnMacOS()
    {
        assumeThat(FBUtilities.getSystemInfo().platform(), is(MACOS));
        exoticSnapshotNamesInternal(exoticSnapshotNamesOnMac);
    }

    @Test
    public void testDuplicateSnapshotOnMacOS()
    {
        assumeThat(FBUtilities.getSystemInfo().platform(), is(MACOS));
        exoticSnapshotNamesInternal(new String[]{ "snapshot" });
        assertThatThrownBy(() -> exoticSnapshotNamesInternal(new String[]{ "Snapshot" }))
        .hasMessageContaining(withKeyspace("Snapshot Snapshot for %s.tbl already exists."));
    }

    @Test
    public void testSameTimestampOnEachTableOfSnaphot()
    {
        cluster.get(1).nodetoolResult("snapshot", "-t", "sametimestamp").asserts().success();
        waitForSnapshotPresent("sametimestamp");
        NodeToolResult result = cluster.get(1).nodetoolResult("listsnapshots");

        Pattern COMPILE = Pattern.compile(" +");
        long distinctTimestamps = Arrays.stream(result.getStdout().split("\n"))
                                        .filter(line -> line.startsWith("sametimestamp"))
                                        .map(line -> COMPILE.matcher(line).replaceAll(" ").split(" ")[7])
                                        .distinct()
                                        .count();

        // assert all dates are same so there is just one value accross all individual tables
        assertEquals(1, distinctTimestamps);
    }

    @Test
    public void testFailureToSnapshotTwiceOnSameEntityWithSameSnapshotName()
    {
        cluster.get(1).nodetoolResult("snapshot", "-t", "somename").asserts().success();

        NodeToolResult failedSnapshotResult = cluster.get(1).nodetoolResult("snapshot", "-t", "somename");
        failedSnapshotResult.asserts().failure();
        Throwable error = failedSnapshotResult.getError();
        assertThat(error.getMessage()).contains("already exists");
    }

    @Test
    public void testTakingSnapshoWithSameNameOnDifferentTablesDoesNotFail()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl2 (key int, value text, PRIMARY KEY (key))"));
        cluster.get(1).nodetoolResult("snapshot", "-t", "somename", "-kt", String.format("%s.tbl", KEYSPACE)).asserts().success();
        cluster.get(1).nodetoolResult("snapshot", "-t", "somename", "-kt", String.format("%s.tbl2", KEYSPACE)).asserts().success();
    }

    @Test
    public void testListingOfSnapshotsByKeyspaceAndTable()
    {
        IInvokableInstance instance = cluster.get(1);
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks1.tbl (key int, value text, PRIMARY KEY (key))");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks1.tbl2 (key int, value text, PRIMARY KEY (key))");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks2.tbl (key int, value text, PRIMARY KEY (key))");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks2.tbl2 (key int, value text, PRIMARY KEY (key))");

        populate(cluster, "ks1", "tbl");
        populate(cluster, "ks1", "tbl2");
        populate(cluster, "ks2", "tbl");
        populate(cluster, "ks2", "tbl2");

        instance.nodetoolResult("snapshot", "-t", "tagks1tbl", "-kt", "ks1.tbl").asserts().success();
        instance.nodetoolResult("snapshot", "-t", "tagks1tbl2", "-kt", "ks1.tbl2").asserts().success();
        instance.nodetoolResult("snapshot", "-t", "tagks2tbl", "-kt", "ks2.tbl").asserts().success();
        instance.nodetoolResult("snapshot", "-t", "tagks2tbl2", "-kt", "ks2.tbl2").asserts().success();

        waitForSnapshot("ks1", null, "tagks1tbl", true, false);
        waitForSnapshot("ks1", null, "tagks1tbl2", true, false);
        waitForSnapshot("ks1", null, "tagks2tbl", false, false);
        waitForSnapshot("ks1", null, "tagks2tbl2", false, false);

        waitForSnapshot("ks1", "tbl", "tagks1tbl", true, false);
        waitForSnapshot("ks1", "tbl", "tagks1tbl2", false, false);
        waitForSnapshot("ks1", "tbl", "tagks2tbl", false, false);
        waitForSnapshot("ks1", "tbl", "tagks2tbl2", false, false);

        waitForSnapshot(null, "tbl", "tagks1tbl", true, false);
        waitForSnapshot(null, "tbl", "tagks1tbl2", false, false);
        waitForSnapshot(null, "tbl", "tagks2tbl", true, false);
        waitForSnapshot(null, "tbl", "tagks2tbl2", false, false);

        NodeToolResult nodeToolResult = instance.nodetoolResult("listsnapshots", "-n", "tagks1tbl");
        nodeToolResult.asserts().success();
        List<String> snapshots = extractSnapshots(nodeToolResult.getStdout());
        assertEquals(1, snapshots.size());
        assertTrue(snapshots.get(0).contains("tagks1tbl"));
    }

    @Test
    public void testForcedSnapshot() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(3) // 3 dirs to disperse SSTables among different dirs
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk uuid primary key)");

            cluster.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> {
                Keyspace.open("distributed_test_keyspace").getColumnFamilyStore("tbl").disableAutoCompaction();
            });

            for (int i = 0; i < 10; i++)
            {
                cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk) values (?)", UUID.randomUUID());
                cluster.get(1).flush(KEYSPACE);
            }

            takeEphemeralSnapshotWithSameName(cluster);
            List<File> manifests1 = getManifests(cluster);
            List<String> ssTablesFromManifest1 = getSSTablesFromManifest(manifests1.get(0));

            for (int i = 0; i < 10; i++)
            {
                cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk) values (?)", UUID.randomUUID());
                cluster.get(1).flush(KEYSPACE);
            }
            takeEphemeralSnapshotWithSameName(cluster);
            List<File> manifests2 = getManifests(cluster);
            List<String> ssTablesFromManifest2 = getSSTablesFromManifest(manifests2.get(0));

            assertEquals(manifests1, manifests2);
            assertTrue(ssTablesFromManifest1.size() < ssTablesFromManifest2.size());
            assertTrue(ssTablesFromManifest2.containsAll(ssTablesFromManifest1));
        }
    }

    private List<String> getSSTablesFromManifest(File manifest) throws Throwable
    {
        SnapshotManifest snapshotManifest = SnapshotManifest.deserializeFromJsonFile(manifest);
        return snapshotManifest.getFiles();
    }

    private List<File> getManifests(Cluster cluster)
    {
        List<String> manifestsPaths = cluster.get(1).callOnInstance((SerializableCallable<List<String>>) () -> {
            ColumnFamilyStore cfs = Keyspace.open("distributed_test_keyspace").getColumnFamilyStore("tbl");

            List<String> allManifests = new ArrayList<>();
            for (File file : cfs.getDirectories().getSnapshotDirsWithoutCreation("a_snapshot"))
            {
                File maybeManifest = new File(file, "manifest.json");
                if (maybeManifest.exists())
                    allManifests.add(maybeManifest.absolutePath());
            }

            assertEquals(3, allManifests.size()); // 3 because 3 data dirs
            return allManifests;
        });

        List<File> manifests = new ArrayList<>();
        for (String manifest : manifestsPaths)
            manifests.add(new File(manifest));

        return manifests;
    }

    private void takeEphemeralSnapshotWithSameName(Cluster cluster)
    {
        cluster.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> {
            try
            {
                SnapshotManager.instance.takeSnapshot(SnapshotOptions.systemSnapshot("a_snapshot", SnapshotType.REPAIR, (r) -> true, "distributed_test_keyspace.tbl")
                                                                     .ephemeral()
                                                                     .build());
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        });
    }

    private void populate(Cluster cluster)
    {
        for (int i = 0; i < 100; i++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (key, value) VALUES (?, 'txt')"), ConsistencyLevel.ONE, i);
    }

    private void populate(Cluster cluster, String keyspace, String table)
    {
        for (int i = 0; i < 100; i++)
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (key, value) VALUES (?, 'txt')", keyspace, table), ConsistencyLevel.ONE, i);
    }

    private void waitForSnapshotPresent(String snapshotName)
    {
        waitForSnapshot(snapshotName, true, false);
    }

    private void waitForSnapshotCleared(String snapshotName)
    {
        waitForSnapshot(snapshotName, false, false);
    }

    private void waitForSnapshot(String keyspaceName, String tableName, String snapshotName, boolean expectPresent, boolean noTTL)
    {
        await().timeout(20, SECONDS)
               .pollDelay(0, SECONDS)
               .pollInterval(1, SECONDS)
               .until(() -> waitForSnapshotInternal(keyspaceName, tableName, snapshotName, expectPresent, noTTL));
    }

    private void waitForSnapshot(String snapshotName, boolean expectPresent, boolean noTTL)
    {
        waitForSnapshot(null, null, snapshotName, expectPresent, noTTL);
    }

    private boolean waitForSnapshotInternal(String keyspaceName, String tableName, String snapshotName, boolean expectPresent, boolean noTTL)
    {
        List<String> args = new ArrayList<>();
        args.add("listsnapshots");
        NodeToolResult listsnapshots;
        if (noTTL)
            args.add("-nt");

        if (keyspaceName != null)
        {
            args.add("-k");
            args.add(keyspaceName);
        }

        if (tableName != null)
        {
            args.add("-t");
            args.add(tableName);
        }

        if (snapshotName != null)
        {
            args.add("-n");
            args.add(snapshotName);
        }

        listsnapshots = cluster.get(1).nodetoolResult(args.toArray(new String[0]));

        List<String> lines = extractSnapshots(listsnapshots.getStdout());

        return expectPresent == lines.stream().anyMatch(line -> line.startsWith(snapshotName));
    }

    private void exoticSnapshotNamesInternal(String[] exoticSnapshotNames)
    {
        IInvokableInstance instance = cluster.get(1);
        cluster.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s.tbl (key int, value text, PRIMARY KEY (key))"));
        populate(cluster);

        for (String tag : exoticSnapshotNames)
        {
            NodeToolResult result = instance.nodetoolResult("snapshot",
                                                            "-t", tag,
                                                            "-kt", withKeyspace("%s.tbl"));

            if (result.getRc() != 0)
                throw new RuntimeException(result.getError());

            waitForSnapshot(tag, true, true);
        }
    }

    private void assertManifestsPresence(String[] dataDirs, String[] paths, boolean shouldExist)
    {
        for (String dataDir : dataDirs)
        {
            for (String path : paths)
            {
                Path snapshotDir = Paths.get(dataDir).resolve(path).resolve("snapshots").resolve("snapshot_with_local_or_replicated");
                assertEquals(shouldExist, new File(snapshotDir, "manifest.json").exists());
            }
        }
    }

    private void removeAllManifests(String[] dataDirs, String[] paths)
    {
        for (String dataDir : dataDirs)
        {
            for (String path : paths)
            {
                Path snapshotDir = Paths.get(dataDir).resolve(path).resolve("snapshots").resolve("snapshot_with_local_or_replicated");

                File manifest = new File(snapshotDir, "manifest.json");
                assertTrue(manifest.exists());
                manifest.delete();
                assertFalse(manifest.exists());
            }
        }
    }

    private String[] getSnapshotPaths(boolean forSystemKeyspaces)
    {
        return cluster.get(1).applyOnInstance((IIsolatedExecutor.SerializableFunction<Boolean, String[]>) forSystem -> {
            List<String> result = new ArrayList<>();

            for (Keyspace keyspace : Keyspace.all())
            {
                if (forSystem && !LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspace.getName()) && !REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspace.getName()))
                    continue;
                else if (LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspace.getName()) || REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspace.getName()))
                    continue;

                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                    result.add(format("%s/%s-%s", keyspace.getName(), cfs.name, cfs.metadata().id.toHexString()));
            }

            return result.toArray(new String[0]);
        }, forSystemKeyspaces);
    }

    private List<String> extractSnapshots(String listSnapshotsStdOut)
    {
        return Arrays.stream(listSnapshotsStdOut.split("\n"))
                     .filter(line -> !line.isEmpty())
                     .filter(line -> !line.startsWith("Snapshot Details:"))
                     .filter(line -> !line.startsWith("There are no snapshots"))
                     .filter(line -> !line.startsWith("Snapshot name"))
                     .filter(line -> !line.startsWith("Total TrueDiskSpaceUsed"))
                     .collect(toList());
    }
}
