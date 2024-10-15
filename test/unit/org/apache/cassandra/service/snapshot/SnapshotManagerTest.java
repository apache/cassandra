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

package org.apache.cassandra.service.snapshot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.service.DefaultFSErrorHandler;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SnapshotManagerTest
{
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    public static TemporaryFolder temporaryFolder2 = new TemporaryFolder();

    private static File rootDir1;
    private static File rootDir2;

    private static String[] dataDirs;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
        rootDir1 = new File(temporaryFolder.getRoot());
        rootDir2 = new File(temporaryFolder2.getRoot());

        dataDirs = new String[] {
        rootDir1.toPath().toAbsolutePath().toString(),
        rootDir2.toPath().toAbsolutePath().toString()
        };

        SchemaLoader.prepareServer();
    }

    @After
    public void afterTest()
    {
        PathUtils.clearDirectory(rootDir1.toPath());
        PathUtils.clearDirectory(rootDir2.toPath());
    }

    /**
     * Tests that if we remove all manifests files, that equals to stopping manager to track
     * such snapshot, however no data will be removed.
     */
    @Test
    public void testRemovingManifestsLogicallyRemovesSnapshot() throws Exception
    {
        try (SnapshotManager snapshotManager = new SnapshotManager(5, 10, dataDirs))
        {
            snapshotManager.start(true);
            List<TableSnapshot> tableSnapshots = generateTableSnapshots(10, 100);
            snapshotManager.addSnapshots(tableSnapshots);

            // we still have 1000 snapshots because we removed just one manifest
            removeManifestOfSnapshot(tableSnapshots.get(0));
            assertEquals(1000, snapshotManager.getSnapshots((t) -> true).size());

            // remove the second manifest, that will render snapshot to be logically removed
            removeManifestOfSnapshot(tableSnapshots.get(0));
            assertEquals(999, snapshotManager.getSnapshots((t) -> true).size());

            // check that data are still there
            assertFalse(tableSnapshots.get(0).hasManifest());
            assertTrue(tableSnapshots.get(0).exists());
            for (File snapshotDir : tableSnapshots.get(0).getDirectories())
                assertTrue(snapshotDir.exists());
        }
    }

    @Test
    public void testRestart() throws Exception
    {
        try (SnapshotManager snapshotManager = new SnapshotManager(5, 10, dataDirs))
        {
            snapshotManager.start(true);
            List<TableSnapshot> tableSnapshots = generateTableSnapshots(10, 100);

            snapshotManager.addSnapshots(tableSnapshots);

            // we have two dirs
            removeDirectoryOfSnapshot(tableSnapshots.get(0));
            removeDirectoryOfSnapshot(tableSnapshots.get(0));

            assertEquals(999, snapshotManager.getSnapshots((t) -> true).size());

            snapshotManager.restart();

            assertEquals(999, snapshotManager.getSnapshots((t) -> true).size());

            for (int i = 1; i < 100; i++)
            {
                // we have two dirs
                removeDirectoryOfSnapshot(tableSnapshots.get(i));
                removeDirectoryOfSnapshot(tableSnapshots.get(i));
            }

            // still 999
            assertEquals(900, snapshotManager.getSnapshots((t) -> true).size());

            snapshotManager.restart();

            assertEquals(900, snapshotManager.getSnapshots((t) -> true).size());
        }
    }

    private List<TableSnapshot> generateTableSnapshots(int keyspaces, int tables) throws IOException
    {
        List<TableSnapshot> tableSnapshots = new ArrayList<>();
        for (int i = 0; i < keyspaces; i++)
        {
            for (int j = 0; j < tables; j++)
            {
                String snapshotName = format("mysnapshot_%s_%s", i, j);
                File dir1 = new File(Paths.get(rootDir1.absolutePath(), "ks", "tb-1b255f4def2540a60000000000000005", "snapshots", snapshotName));
                File dir2 = new File(Paths.get(rootDir2.absolutePath(), "ks", "tb-1b255f4def2540a60000000000000005", "snapshots", snapshotName));
                dir1.tryCreateDirectories();
                dir2.tryCreateDirectories();
                TableSnapshot snapshot = generateSnapshotDetails(Set.of(dir1, dir2), snapshotName, "ks", "tb-1b255f4def2540a60000000000000005", null, false);
                SnapshotManifest manifest = new SnapshotManifest(List.of(), null, snapshot.getCreatedAt(), snapshot.isEphemeral());
                manifest.serializeToJsonFile(new File(dir1.toPath().resolve("manifest.json")));
                manifest.serializeToJsonFile(new File(dir2.toPath().resolve("manifest.json")));
                generateFileInSnapshot(snapshot);
                tableSnapshots.add(snapshot);
            }
        }

        return tableSnapshots;
    }

    private void generateFileInSnapshot(TableSnapshot tableSnapshot) throws IOException
    {
        for (File snapshotDir : tableSnapshot.getDirectories())
            Files.createFile(snapshotDir.toPath().resolve("schema.cql"));
    }

    private void removeDirectoryOfSnapshot(TableSnapshot tableSnapshot)
    {
        for (File snapshotDir : tableSnapshot.getDirectories())
        {
            if (snapshotDir.exists())
            {
                snapshotDir.deleteRecursive();
                break;
            }
        }
    }

    private void removeManifestOfSnapshot(TableSnapshot tableSnapshot)
    {
        for (File snapshotDir : tableSnapshot.getDirectories())
        {
            if (snapshotDir.exists())
            {
                File manifest = new File(snapshotDir, "manifest.json");
                if (!manifest.exists())
                    continue;

                manifest.delete();
                return;
            }
        }
    }

    private TableSnapshot generateSnapshotDetails(Set<File> roots,
                                                  String tag,
                                                  String keyspace,
                                                  String table,
                                                  Instant expiration,
                                                  boolean ephemeral)
    {
        try
        {
            Set<File> snapshotDirs = new HashSet<>();
            for (File root : roots)
            {
                root.tryCreateDirectories();
                snapshotDirs.add(root);
            }

            return new TableSnapshot(keyspace,
                                     table,
                                     UUID.randomUUID(),
                                     tag,
                                     Instant.EPOCH,
                                     expiration,
                                     snapshotDirs,
                                     ephemeral);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
