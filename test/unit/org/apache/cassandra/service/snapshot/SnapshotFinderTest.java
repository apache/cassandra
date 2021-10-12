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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.util.Lists;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotFinderTest
{
    static String DATA_DIR_1 = "data1";
    static String DATA_DIR_2 = "data2";
    static String DATA_DIR_3 = "data3";
    static String[] DATA_DIRS = new String[]{DATA_DIR_1, DATA_DIR_2, DATA_DIR_3};

    static String KEYSPACE_1 = "ks1";
    static String TABLE1_NAME = "table_1";
    static String TABLE1_ID = toString(TimeUUID.Generator.nextTimeAsUUID());
    static String TABLE2_NAME = "table2";
    static String TABLE2_ID = toString(TimeUUID.Generator.nextTimeAsUUID());

    static String KEYSPACE_2 = "ks2";
    static String TABLE3_NAME = "table_3";
    static String TABLE3_ID = toString(TimeUUID.Generator.nextTimeAsUUID());

    static String TAG1 = "tag1";
    static String TAG2 = "tag2";
    static String TAG3 = "tag3";

    static String INVALID_NAME = "#test#";
    static String INVALID_ID = "XPTO";

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testNoSnapshots() throws IOException
    {
        // Create table directories on all data directories without snapshots
        File baseDir  = new File(tmpDir.newFolder());
        for (String dataDir : DATA_DIRS) {
            createDir(baseDir, dataDir, KEYSPACE_1, TABLE1_NAME, TABLE1_ID);
            createDir(baseDir, dataDir, KEYSPACE_1, TABLE2_NAME, TABLE2_ID);
            createDir(baseDir, dataDir, KEYSPACE_2, TABLE3_NAME, TABLE3_ID);
        }

        // Check no snapshots are found
        SnapshotFinder finder = new SnapshotFinder(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        assertThat(finder.findAll()).isEmpty();
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
            tag1Files.add(createDir(baseDir, dataDir, KEYSPACE_1, String.format("%s-%s", TABLE1_NAME, TABLE1_ID), Directories.SNAPSHOT_SUBDIR, TAG1));
            tag2Files.add(createDir(baseDir, dataDir, KEYSPACE_1, String.format("%s-%s", TABLE2_NAME, TABLE2_ID), Directories.SNAPSHOT_SUBDIR, TAG2));
            tag3Files.add(createDir(baseDir, dataDir, KEYSPACE_2, String.format("%s-%s", TABLE3_NAME, TABLE3_ID), Directories.SNAPSHOT_SUBDIR, TAG3));
        }

        // Verify all 3 snapshots are found correctly from data directories
        SnapshotFinder finder = new SnapshotFinder(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        Set<TableSnapshot> found = finder.findAll();
        assertThat(found).hasSize(3);
        assertThat(found).contains(new TableSnapshot(KEYSPACE_1, TABLE1_NAME, TAG1, null, null, tag1Files, null));
        assertThat(found).contains(new TableSnapshot(KEYSPACE_1, TABLE2_NAME, TAG2, null, null, tag2Files, null));
        assertThat(found).contains(new TableSnapshot(KEYSPACE_2, TABLE3_NAME, TAG3, null, null, tag3Files, null));
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
            tag1Files.add(createDir(baseDir, dataDir, KEYSPACE_1, String.format("%s-%s", TABLE1_NAME, TABLE1_ID), Directories.SNAPSHOT_SUBDIR, TAG1));
            tag2Files.add(createDir(baseDir, dataDir, KEYSPACE_1, String.format("%s-%s", TABLE2_NAME, TABLE2_ID), Directories.SNAPSHOT_SUBDIR, TAG2));
            tag3Files.add(createDir(baseDir, dataDir, KEYSPACE_2, String.format("%s-%s", TABLE3_NAME, TABLE3_ID), Directories.SNAPSHOT_SUBDIR, TAG3));
        }

        // Write manifest for snapshot tag1 on random location
        Instant tag1Ts = Instant.now();
        File tag1ManifestLocation = tag1Files.toArray(new File[0])[ThreadLocalRandom.current().nextInt(tag1Files.size())];
        writeManifest(tag1ManifestLocation, tag1Ts, null);

        // Write manifest for snapshot tag2 on random location
        Instant tag2Ts = Instant.now().plusSeconds(10);
        DurationSpec tag2Ttl = new DurationSpec("10h");
        File tag2ManifestLocation = tag2Files.toArray(new File[0])[ThreadLocalRandom.current().nextInt(tag2Files.size())];
        writeManifest(tag2ManifestLocation, tag2Ts, tag2Ttl);

        // Write manifest for snapshot tag3 on random location
        Instant tag3Ts = Instant.now().plusSeconds(20);
        File tag3ManifestLocation = tag3Files.toArray(new File[0])[ThreadLocalRandom.current().nextInt(tag3Files.size())];
        writeManifest(tag3ManifestLocation, tag3Ts, null);

        // Verify all 3 snapshots are found correctly from data directories
        SnapshotFinder loader = new SnapshotFinder(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        Set<TableSnapshot> finder = loader.findAll();
        assertThat(finder).hasSize(3);
        assertThat(finder).contains(new TableSnapshot(KEYSPACE_1, TABLE1_NAME, TAG1, tag1Ts, null, tag1Files, null));
        assertThat(finder).contains(new TableSnapshot(KEYSPACE_1, TABLE2_NAME, TAG2, tag2Ts, tag2Ts.plusSeconds(tag2Ttl.toSeconds()), tag2Files, null));
        assertThat(finder).contains(new TableSnapshot(KEYSPACE_2, TABLE3_NAME, TAG3, tag3Ts, null, tag3Files, null));
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
            tag1Files.add(createDir(baseDir, dataDir, INVALID_NAME, String.format("%s-%s", TABLE1_NAME, TABLE1_ID), Directories.SNAPSHOT_SUBDIR, TAG1));
            tag2Files.add(createDir(baseDir, dataDir, KEYSPACE_1, String.format("%s-%s", INVALID_NAME, TABLE2_ID), Directories.SNAPSHOT_SUBDIR, TAG2));
            tag3Files.add(createDir(baseDir, dataDir, KEYSPACE_2, String.format("%s-%s", TABLE3_NAME, INVALID_ID), Directories.SNAPSHOT_SUBDIR, TAG3));
        }

        // Check no snapshots are loaded
        SnapshotFinder finder = new SnapshotFinder(Arrays.asList(Paths.get(baseDir.toString(), DATA_DIR_1),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_2),
                                                                 Paths.get(baseDir.toString(), DATA_DIR_3)));
        assertThat(finder.findAll()).isEmpty();
    }

    private void writeManifest(File snapshotDir, Instant creationTime, DurationSpec ttl) throws IOException
    {
        SnapshotManifest manifest = new SnapshotManifest(Lists.newArrayList("f1", "f2", "f3"), ttl, creationTime);
        manifest.serializeToJsonFile(getManifestFile(snapshotDir));
    }

    private static File createDir(File baseDir, String... subdirs)
    {
        File file = new File(Paths.get(baseDir.toString(), subdirs).toString());
        file.toJavaIOFile().mkdirs();
        return file;
    }

    static String toString(UUID id)
    {
        return ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(id));
    }

    public static File getManifestFile(File snapshotDir)
    {
        return new File(snapshotDir, "manifest.json");
    }
}
