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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.setInt(10);
        SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.setInt(5);

        rootDir1 = new File(temporaryFolder.getRoot());
        rootDir2 = new File(temporaryFolder2.getRoot());

        dataDirs = new String[]{
        rootDir1.toPath().toAbsolutePath().toString(),
        rootDir2.toPath().toAbsolutePath().toString()
        };
    }

    private static class ThrowingTask extends AbstractSnapshotTask<Void>
    {
        public ThrowingTask()
        {
            super(null);
        }

        @Override
        public Void call()
        {
            throw new RuntimeException("an exception");
        }

        @Override
        public SnapshotTaskType getTaskType()
        {
            return SnapshotTaskType.SNAPSHOT;
        }
    }

    @Test
    public void testTaskThrowingException()
    {
        doWithManager(manager -> assertThatThrownBy(() -> manager.executeTask(new ThrowingTask())).isInstanceOf(RuntimeException.class)
           .hasRootCauseMessage("an exception")
           .hasMessageContaining("Exception occured while executing"));

        doWithManager(manager -> {

            manager.close();

            assertThatThrownBy(() -> manager.executeTask(new ThrowingTask())).isInstanceOf(RuntimeException.class)
               .hasRootCauseMessage("an exception")
               .hasMessageContaining("Exception occured while executing");

            manager.start(false);

            assertThatThrownBy(() -> manager.executeTask(new ThrowingTask())).isInstanceOf(RuntimeException.class)
               .hasRootCauseMessage("an exception")
               .hasMessageContaining("Exception occured while executing");
        });
    }

    /**
     * Tests that if we remove all manifests files, that equals to stopping manager to track
     * such snapshot, however no data will be removed.
     */
    @Test
    public void testRemovingManifestsLogicallyRemovesSnapshot()
    {
        doWithManager(manager -> {
            List<TableSnapshot> tableSnapshots = generateTableSnapshots(10, 100);

            for (TableSnapshot snapshot : tableSnapshots)
                manager.addSnapshot(snapshot);

            // we still have 1000 snapshots because we removed just one manifest
            removeManifestOfSnapshot(tableSnapshots.get(0));
            assertEquals(1000, manager.getSnapshots((t) -> true).size());

            // remove the second manifest, that will render snapshot to be logically removed
            removeManifestOfSnapshot(tableSnapshots.get(0));
            assertEquals(999, manager.getSnapshots((t) -> true).size());

            // check that data are still there
            assertFalse(tableSnapshots.get(0).hasManifest());
            assertTrue(tableSnapshots.get(0).exists());
            for (File snapshotDir : tableSnapshots.get(0).getDirectories())
                assertTrue(snapshotDir.exists());
        });
    }

    private void doWithManager(Consumer<SnapshotManager> action)
    {
        try (MockedStatic<DatabaseDescriptor> mockedDD = Mockito.mockStatic(DatabaseDescriptor.class))
        {
            mockedDD.when(() -> DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations()).thenReturn(dataDirs);
            mockedDD.when(() -> DatabaseDescriptor.getLocalSystemKeyspacesDataFileLocations()).thenReturn(new String[]{});
            mockedDD.when(() -> DatabaseDescriptor.getAllDataFileLocations()).thenReturn(dataDirs);
            mockedDD.when(() -> DatabaseDescriptor.getDumpHeapOnUncaughtException()).thenReturn(false);

            try (SnapshotManager snapshotManager = new SnapshotManager(SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(10),
                                                                       SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(5),
                                                                       dataDirs).start(true))
            {
                action.accept(snapshotManager);
            }
            finally
            {
                clearDirectory(rootDir1.toPath());
                clearDirectory(rootDir2.toPath());
            }
        }
    }

    private List<TableSnapshot> generateTableSnapshots(int keyspaces, int tables) throws RuntimeException
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
                try
                {
                    manifest.serializeToJsonFile(new File(dir1.toPath().resolve("manifest.json")));
                    manifest.serializeToJsonFile(new File(dir2.toPath().resolve("manifest.json")));
                    generateFileInSnapshot(snapshot);
                }
                catch (Exception ex)
                {
                    throw new RuntimeException(ex);
                }

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


    /**
     * Empties everything in directory of "path" but keeps the directory itself.
     *
     * @param path directory to be emptied
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public static void clearDirectory(Path path)
    {
        if (PathUtils.isDirectory(path))
            PathUtils.forEach(path, PathUtils::deleteRecursive);
    }

}
