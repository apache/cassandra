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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.utils.FBUtilities.now;
import static org.assertj.core.api.Assertions.assertThat;

public class TableSnapshotTest
{
    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    public static Set<File> createFolders(TemporaryFolder temp) throws IOException {
        File folder = new File(temp.newFolder());
        Set<File> folders = new HashSet<>();
        for (String folderName : Arrays.asList("foo", "bar", "buzz")) {
            File subfolder = new File(folder, folderName);
            subfolder.tryCreateDirectories();
            assertThat(subfolder.exists());
            folders.add(subfolder);
        };
        return folders;
    }

    @Test
    public void testSnapshotExists() throws IOException
    {
        Set<File> folders = createFolders(tempFolder);

        TableSnapshot snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        null,
        null,
        folders);

        assertThat(snapshot.exists()).isTrue();

        folders.forEach(FileUtils::deleteRecursive);

        assertThat(snapshot.exists()).isFalse();
    }

    @Test
    public void testSnapshotExpiring() throws IOException
    {
        Set<File> folders = createFolders(tempFolder);

        TableSnapshot snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        null,
        null,
        folders);

        assertThat(snapshot.isExpiring()).isFalse();
        assertThat(snapshot.isExpired(now())).isFalse();

        snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        now(),
        null,
        folders);

        assertThat(snapshot.isExpiring()).isFalse();
        assertThat(snapshot.isExpired(now())).isFalse();

        snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        now(),
        now().plusSeconds(1000),
        folders);

        assertThat(snapshot.isExpiring()).isTrue();
        assertThat(snapshot.isExpired(now())).isFalse();

        snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        now(),
        now().minusSeconds(1000),
        folders);

        assertThat(snapshot.isExpiring()).isTrue();
        assertThat(snapshot.isExpired(now())).isTrue();
    }

    private Long writeBatchToFile(File file) throws IOException
    {
        FileOutputStreamPlus out = new FileOutputStreamPlus(file);
        out.write(1);
        out.write(2);
        out.write(3);
        out.close();
        return 3L;
    }

    @Test
    public void testComputeSizeOnDisk() throws IOException
    {
        Set<File> folders = createFolders(tempFolder);

        TableSnapshot tableDetails = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        null,
        null,
        folders);

        Long res = 0L;

        for (File dir : folders)
        {
            writeBatchToFile(new File(dir, "tmp"));
            res += FileUtils.folderSize(dir);
        }

        assertThat(tableDetails.computeSizeOnDiskBytes()).isGreaterThan(0L);
        assertThat(tableDetails.computeSizeOnDiskBytes()).isEqualTo(res);
    }

    @Test
    public void testComputeTrueSize() throws IOException
    {
        Set<File> folders = createFolders(tempFolder);

        TableSnapshot tableDetails = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        null,
        null,
        folders);

        Long res = 0L;

        for (File dir : folders)
        {
            File file = new File(dir, "tmp");
            writeBatchToFile(file);
            res += file.length();
        }

        assertThat(tableDetails.computeTrueSizeBytes()).isGreaterThan(0L);
        assertThat(tableDetails.computeTrueSizeBytes()).isEqualTo(res);
    }

    @Test
    public void testGetCreatedAt() throws IOException
    {
        Set<File> folders = createFolders(tempFolder);

        // When createdAt is not null, getCreatedAt() should return it
        Instant createdAt = Instant.EPOCH;
        TableSnapshot withCreatedAt = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some1",
        createdAt,
        null,
        folders);
        assertThat(withCreatedAt.getCreatedAt()).isEqualTo(createdAt);

        // When createdAt is  null, it should return the snapshot folder minimum update time
        TableSnapshot withoutCreatedAt = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some1",
        null,
        null,
        folders);
        assertThat(withoutCreatedAt.getCreatedAt()).isEqualTo(Instant.ofEpochMilli(folders.stream().mapToLong(f -> f.lastModified()).min().getAsLong()));
    }

    @Test
    public void testGetLiveFileFromSnapshotFile()
    {
        testGetLiveFileFromSnapshotFile("~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/snapshots/1643481737850/me-1-big-Data.db",
                                        "~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/me-1-big-Data.db");
    }

    @Test
    public void testGetLiveFileFromSnapshotIndexFile()
    {
        testGetLiveFileFromSnapshotFile("~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/snapshots/1643481737850/.tbl_val_idx/me-1-big-Summary.db",
                                        "~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/.tbl_val_idx/me-1-big-Summary.db");
    }

    public void testGetLiveFileFromSnapshotFile(String snapshotFile, String expectedLiveFile)
    {
        assertThat(TableSnapshot.getLiveFileFromSnapshotFile(Paths.get(snapshotFile)).toString()).isEqualTo(expectedLiveFile);
    }
}
