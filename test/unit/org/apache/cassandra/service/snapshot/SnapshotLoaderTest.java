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
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;
import org.assertj.core.util.Lists;

import static org.apache.cassandra.service.snapshot.SnapshotLoader.SNAPSHOT_DIR_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotLoaderTest
{
    static String DATA_DIR_1 = "data1";
    static String DATA_DIR_2 = "data2";
    static String DATA_DIR_3 = "data3";
    static String[] DATA_DIRS = new String[]{DATA_DIR_1, DATA_DIR_2, DATA_DIR_3};

    static String KEYSPACE_1 = "ks1";
    static String TABLE1_NAME = "table_1";
    static UUID TABLE1_ID = UUID.randomUUID();
    static String TABLE2_NAME = "table2";
    static UUID TABLE2_ID = UUID.randomUUID();

    static String KEYSPACE_2 = "ks2";
    static String TABLE3_NAME = "table_3";
    static UUID TABLE3_ID = UUID.randomUUID();

    static String TAG1 = "tag1";
    static String TAG2 = "tag2";
    static String TAG3 = "tag3";

    static String INVALID_NAME = "#test#";
    static String INVALID_ID = "XPTO";

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testMatcher()
    {
        String INDEX_SNAPSHOT = "/user/.ccm/test/node1/data0/ks/indexed_table-24b241e0c58f11eca526336fc2c671ab/snapshots/test";
        assertThat(SNAPSHOT_DIR_PATTERN.matcher(INDEX_SNAPSHOT).find()).isTrue();

        String TABLE_SNAPSHOT = "/Users/pmottagomes/.ccm/test/node1/data0/ks/my_table-1a025b40c58f11eca526336fc2c671ab/snapshots/test";
        assertThat(SNAPSHOT_DIR_PATTERN.matcher(TABLE_SNAPSHOT).find()).isTrue();

        String DROPPED_SNAPSHOT = "/Users/pmottagomes/.ccm/test/node1/data0/ks/my_table-e5c58330c58d11eca526336fc2c671ab/snapshots/dropped-1650997415751-my_table";
        assertThat(SNAPSHOT_DIR_PATTERN.matcher(DROPPED_SNAPSHOT).find()).isTrue();
    }

    @Test
    public void testNoSnapshots() throws IOException
    {
        // Create table directories on all data directories without snapshots
        File baseDir  = new File(tmpDir.newFolder());
        for (String dataDir : DATA_DIRS)
        {
            createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(TABLE1_NAME, TABLE1_ID));
            createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(TABLE2_NAME, TABLE2_ID));
            createDir(baseDir, dataDir, KEYSPACE_2, tableDirName(TABLE3_NAME, TABLE3_ID));
        }

        // Check no snapshots are found
        SnapshotLoader loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        assertThat(loader.loadSnapshots()).isEmpty();
    }

    @Test
    public void testSnapshotsWithoutManifests() throws IOException
    {
        Set<File> tag1Files = new HashSet<>();
        Set<File> tag2Files = new HashSet<>();
        Set<File> tag3Files = new HashSet<>();

        // Create one snapshot per table - without manifests:
        // - ks1.t1 : tag1
        // - ks1.t2 : tag2
        // - ks2.t3 : tag3
        File baseDir  = new File(tmpDir.newFolder());
        for (String dataDir : DATA_DIRS)
        {
            tag1Files.add(createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(TABLE1_NAME, TABLE1_ID), Directories.SNAPSHOT_SUBDIR, TAG1));
            tag2Files.add(createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(TABLE2_NAME, TABLE2_ID), Directories.SNAPSHOT_SUBDIR, TAG2));
            tag3Files.add(createDir(baseDir, dataDir, KEYSPACE_2, tableDirName(TABLE3_NAME, TABLE3_ID), Directories.SNAPSHOT_SUBDIR, TAG3));
        }

        // Verify all 3 snapshots are found correctly from data directories
        SnapshotLoader loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        Set<TableSnapshot> snapshots = loader.loadSnapshots();
        assertThat(snapshots).hasSize(3);
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE1_NAME, TABLE1_ID, TAG1, null, null, tag1Files, false));
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE2_NAME, TABLE2_ID,  TAG2, null, null, tag2Files, false));
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_2, TABLE3_NAME, TABLE3_ID,  TAG3, null, null, tag3Files, false));

        // Verify snapshot loading for a specific keyspace
        loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                  Paths.get(baseDir.toString(), DATA_DIR_2),
                                                  Paths.get(baseDir.toString(), DATA_DIR_3)));

        snapshots = loader.loadSnapshots(KEYSPACE_1);
        assertThat(snapshots).hasSize(2);
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE1_NAME, TABLE1_ID, TAG1, null, null, tag1Files, false));
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE2_NAME, TABLE2_ID,  TAG2, null, null, tag2Files, false));

        loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                  Paths.get(baseDir.toString(), DATA_DIR_2),
                                                  Paths.get(baseDir.toString(), DATA_DIR_3)));
        snapshots = loader.loadSnapshots(KEYSPACE_2);
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_2, TABLE3_NAME, TABLE3_ID,  TAG3, null, null, tag3Files, false));
    }

    @Test
    public void testEphemeralSnapshotWithoutManifest() throws IOException
    {
        Set<File> tag1Files = new HashSet<>();

        // Create one snapshot per table - without manifests:
        // - ks1.t1 : tag1
        File baseDir  = new File(tmpDir.newFolder());
        boolean ephemeralFileCreated = false;
        for (String dataDir : DATA_DIRS)
        {
            File dir = createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(TABLE1_NAME, TABLE1_ID), Directories.SNAPSHOT_SUBDIR, TAG1);
            tag1Files.add(dir);
            if (!ephemeralFileCreated)
            {
                createEphemeralMarkerFile(dir);
                ephemeralFileCreated = true;
            }
        }

        // Verify snapshot is found correctly from data directories
        SnapshotLoader loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));

        Set<TableSnapshot> snapshots = loader.loadSnapshots();
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE1_NAME, TABLE1_ID, TAG1, null, null, tag1Files, true));
        Assert.assertTrue(snapshots.stream().findFirst().get().isEphemeral());
    }

    @Test
    public void testSnapshotsWithManifests() throws IOException
    {
        Set<File> tag1Files = new HashSet<>();
        Set<File> tag2Files = new HashSet<>();
        Set<File> tag3Files = new HashSet<>();

        // Create one snapshot per table:
        // - ks1.t1 : tag1
        // - ks1.t2 : tag2
        // - ks2.t3 : tag3
        File baseDir  = new File(tmpDir.newFolder());
        for (String dataDir : DATA_DIRS)
        {
            tag1Files.add(createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(TABLE1_NAME, TABLE1_ID), Directories.SNAPSHOT_SUBDIR, TAG1));
            tag2Files.add(createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(TABLE2_NAME, TABLE2_ID), Directories.SNAPSHOT_SUBDIR, TAG2));
            tag3Files.add(createDir(baseDir, dataDir, KEYSPACE_2, tableDirName(TABLE3_NAME, TABLE3_ID), Directories.SNAPSHOT_SUBDIR, TAG3));
        }

        // Write manifest for snapshot tag1 on random location
        Instant tag1Ts = Instant.now();
        File tag1ManifestLocation = tag1Files.toArray(new File[0])[ThreadLocalRandom.current().nextInt(tag1Files.size())];
        writeManifest(tag1ManifestLocation, tag1Ts, null);

        // Write manifest for snapshot tag2 on random location
        Instant tag2Ts = Instant.now().plusSeconds(10);
        DurationSpec.IntSecondsBound tag2Ttl = new DurationSpec.IntSecondsBound("10h");
        File tag2ManifestLocation = tag2Files.toArray(new File[0])[ThreadLocalRandom.current().nextInt(tag2Files.size())];
        writeManifest(tag2ManifestLocation, tag2Ts, tag2Ttl);

        // Write manifest for snapshot tag3 on random location
        Instant tag3Ts = Instant.now().plusSeconds(20);
        File tag3ManifestLocation = tag3Files.toArray(new File[0])[ThreadLocalRandom.current().nextInt(tag3Files.size())];
        writeManifest(tag3ManifestLocation, tag3Ts, null);

        // Verify all 3 snapshots are found correctly from data directories
        SnapshotLoader loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        Set<TableSnapshot> snapshots = loader.loadSnapshots();
        assertThat(snapshots).hasSize(3);
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE1_NAME, TABLE1_ID, TAG1, tag1Ts, null, tag1Files, false));
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE2_NAME, TABLE2_ID,  TAG2, tag2Ts, tag2Ts.plusSeconds(tag2Ttl.toSeconds()), tag2Files, false));
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_2, TABLE3_NAME, TABLE3_ID,  TAG3, tag3Ts, null, tag3Files, false));

        // Verify snapshot loading for a specific keyspace
        loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                  Paths.get(baseDir.toString(), DATA_DIR_2),
                                                  Paths.get(baseDir.toString(), DATA_DIR_3)));

        snapshots = loader.loadSnapshots(KEYSPACE_1);
        assertThat(snapshots).hasSize(2);
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE1_NAME, TABLE1_ID, TAG1, tag1Ts, null, tag1Files, false));
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_1, TABLE2_NAME, TABLE2_ID,  TAG2, tag2Ts, tag2Ts.plusSeconds(tag2Ttl.toSeconds()), tag2Files, false));

        loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                  Paths.get(baseDir.toString(), DATA_DIR_2),
                                                  Paths.get(baseDir.toString(), DATA_DIR_3)));
        snapshots = loader.loadSnapshots(KEYSPACE_2);
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots).contains(new TableSnapshot(KEYSPACE_2, TABLE3_NAME, TABLE3_ID,  TAG3, tag3Ts, null, tag3Files, false));
    }

    @Test
    public void testInvalidSnapshotsAreNotLoaded() throws IOException
    {
        Set<File> tag1Files = new HashSet<>();
        Set<File> tag2Files = new HashSet<>();
        Set<File> tag3Files = new HashSet<>();

        // Create invalid snapshot directory structure
        // - /data_dir/#test#/table1-validuuid/snapshot/tag1
        // - /data_dir/ks1/#test#-validuuid/snapshot/tag2
        // - /data_dir/ks2/table3-invaliduuid/snapshot/tag3
        File baseDir  = new File(tmpDir.newFolder());
        for (String dataDir : DATA_DIRS)
        {
            tag1Files.add(createDir(baseDir, dataDir, INVALID_NAME, tableDirName(TABLE1_NAME, TABLE1_ID), Directories.SNAPSHOT_SUBDIR, TAG1));
            tag2Files.add(createDir(baseDir, dataDir, KEYSPACE_1, tableDirName(INVALID_NAME, TABLE2_ID), Directories.SNAPSHOT_SUBDIR, TAG2));
            tag3Files.add(createDir(baseDir, dataDir, KEYSPACE_2, String.format("%s-%s", TABLE3_NAME, INVALID_ID), Directories.SNAPSHOT_SUBDIR, TAG3));
        }

        // Check no snapshots are loaded
        SnapshotLoader loader = new SnapshotLoader(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        assertThat(loader.loadSnapshots()).isEmpty();
    }

    @Test
    public void testParseUUID()
    {
        assertThat(SnapshotLoader.Visitor.parseUUID("c7e513243f0711ec9bbc0242ac130002")).isEqualTo(UUID.fromString("c7e51324-3f07-11ec-9bbc-0242ac130002"));
    }

    private void writeManifest(File snapshotDir, Instant creationTime, DurationSpec.IntSecondsBound ttl) throws IOException
    {
        SnapshotManifest manifest = new SnapshotManifest(Lists.newArrayList("f1", "f2", "f3"), ttl, creationTime, false);
        manifest.serializeToJsonFile(getManifestFile(snapshotDir));
    }

    private static File createDir(File baseDir, String... subdirs)
    {
        File file = new File(Paths.get(baseDir.toString(), subdirs).toString());
        file.toJavaIOFile().mkdirs();
        return file;
    }

    private static void createEphemeralMarkerFile(File dir)
    {
        Assert.assertTrue(new File(dir, "ephemeral.snapshot").createFileIfNotExists());
    }

    static String tableDirName(String tableName, UUID tableId)
    {
        return String.format("%s-%s", tableName, removeDashes(tableId));
    }

    static String removeDashes(UUID id)
    {
        return id.toString().replace("-", "");
    }

    public static File getManifestFile(File snapshotDir)
    {
        return new File(snapshotDir, "manifest.json");
    }
}
