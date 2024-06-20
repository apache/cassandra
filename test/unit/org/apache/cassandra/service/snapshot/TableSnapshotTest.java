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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import org.apache.cassandra.utils.Pair;
import org.mockito.MockedStatic;

import static org.apache.cassandra.utils.FBUtilities.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

public class TableSnapshotTest
{
    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    public static Set<File> createFolders(TemporaryFolder temp) throws IOException
    {
        File folder = new File(temp.newFolder());
        Set<File> folders = new HashSet<>();
        for (String folderName : Arrays.asList("foo", "bar", "buzz"))
        {
            File subfolder = new File(folder, folderName);
            subfolder.tryCreateDirectories();
            File manifest = new File(subfolder.toPath().resolve("manifest.json"));
            File schema = new File(subfolder.toPath().resolve("schema.cql"));
            manifest.createFileIfNotExists();
            schema.createFileIfNotExists();
            Files.write(manifest.toPath(), "{}".getBytes());
            Files.write(schema.toPath(), "cql schema".getBytes());
            folders.add(subfolder);
        }

        return folders;
    }

    @Test
    public void testTableSnapshotEquality() throws IOException
    {
        TableSnapshot snapshot1 = new TableSnapshot("ks", "tbl", UUID.randomUUID(), "some", null, null, createFolders(tempFolder), false);
        TableSnapshot snapshot2 = new TableSnapshot("ks", "tbl", UUID.randomUUID(), "some", null, null, createFolders(tempFolder), false);

        // they are not equal, because table id is random for each
        assertNotEquals(snapshot1, snapshot2);

        UUID tableId = UUID.randomUUID();

        snapshot1 = new TableSnapshot("ks", "tbl", tableId, "some", null, null, createFolders(tempFolder), false);
        snapshot2 = new TableSnapshot("ks", "tbl", tableId, "some", null, null, createFolders(tempFolder), false);

        // they are equal, even their directories differ
        assertEquals(snapshot1, snapshot2);

        Set<File> folders = createFolders(tempFolder);

        snapshot1 = new TableSnapshot("ks", "tbl", tableId, "some", Instant.now(), null, folders, false);
        snapshot2 = new TableSnapshot("ks", "tbl", tableId, "some", Instant.now().plusSeconds(1), null, folders, false);

        // they are equal, even their creation times differ
        assertEquals(snapshot1, snapshot2);

        snapshot1 = new TableSnapshot("ks", "tbl", tableId, "some", null, Instant.now(), folders, false);
        snapshot2 = new TableSnapshot("ks", "tbl", tableId, "some", null, Instant.now().plusSeconds(1), folders, false);

        // they are equal, even their expiration times differ
        assertEquals(snapshot1, snapshot2);

        snapshot1 = new TableSnapshot("ks", "tbl", tableId, "some", null, null, folders, false);
        snapshot2 = new TableSnapshot("ks", "tbl", UUID.randomUUID(), "some", null, null, folders, false);

        // they are not equal, because their tableId differs
        assertNotEquals(snapshot1, snapshot2);

        snapshot1 = new TableSnapshot("ks", "tbl", tableId, "some1", null, null, folders, false);
        snapshot2 = new TableSnapshot("ks", "tbl", tableId, "some2", null, null, folders, false);

        // they are not equal, because their tag differs
        assertNotEquals(snapshot1, snapshot2);

        snapshot1 = new TableSnapshot("ks", "tbl", tableId, "some1", null, null, folders, false);
        snapshot2 = new TableSnapshot("ks", "tbl2", tableId, "some1", null, null, folders, false);

        // they are not equal, because their table differs
        assertNotEquals(snapshot1, snapshot2);
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
        folders,
        false
        );

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
        folders,
        false
        );

        assertThat(snapshot.isExpiring()).isFalse();
        assertThat(snapshot.isExpired(now())).isFalse();

        snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        now(),
        null,
        folders,
        false
        );

        assertThat(snapshot.isExpiring()).isFalse();
        assertThat(snapshot.isExpired(now())).isFalse();

        snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        now(),
        now().plusSeconds(1000),
        folders,
        false
        );

        assertThat(snapshot.isExpiring()).isTrue();
        assertThat(snapshot.isExpired(now())).isFalse();

        snapshot = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some",
        now(),
        now().minusSeconds(1000),
        folders,
        false);

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
        folders,
        false);

        Long res = 0L;

        for (File dir : folders)
        {
            writeBatchToFile(new File(dir, "tmp"));
            res += FileUtils.folderSize(dir);
        }

        try (MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class))
        {
            fileUtilsMock.when(() -> FileUtils.folderSize(any())).thenCallRealMethod();

            assertThat(tableDetails.computeSizeOnDiskBytes()).isGreaterThan(0L);
            assertThat(tableDetails.computeSizeOnDiskBytes()).isEqualTo(res);

            // when we invoke computeSizeOnDiskBytes for the second time, it will use cached value
            // 3 invocations for folderSize are from the first invocation of computeSizeOnDiskBytes because
            // we have 3 data dirs, if we have not cached it, the number of invocations would be 6.
            fileUtilsMock.verify(() -> FileUtils.folderSize(any()), times(3));
        }
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
        folders,
        false
        );

        Long res = 0L;

        Set<String> files = new HashSet<>();
        for (File dir : folders)
        {
            File file = new File(dir, "tmp");
            files.add(file.toAbsolute().toString());
            writeBatchToFile(file);
            res += file.length();
            res += new File(dir, "manifest.json").length();
            res += new File(dir, "schema.cql").length();
        }

        try (MockedStatic<TableSnapshot> tableSnapshotMock = mockStatic(TableSnapshot.class))
        {
            tableSnapshotMock.when(() -> TableSnapshot.getLiveFileFromSnapshotFile(any())).thenCallRealMethod();

            assertThat(tableDetails.computeTrueSizeBytes(files)).isGreaterThan(0L);
            assertThat(tableDetails.computeTrueSizeBytes(files)).isEqualTo(res);

            // 6 because we avoided to call it for manifest and schema because they are cached
            tableSnapshotMock.verify(() -> TableSnapshot.getLiveFileFromSnapshotFile(any()), times(6));
        }
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
        folders,
        false
        );

        assertThat(withCreatedAt.getCreatedAt()).isEqualTo(createdAt);

        // When createdAt is  null, it should return the snapshot folder minimum update time
        TableSnapshot withoutCreatedAt = new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        "some1",
        null,
        null,
        folders,
        false
        );

        assertThat(withoutCreatedAt.getCreatedAt()).isEqualTo(Instant.ofEpochMilli(folders.stream().mapToLong(f -> f.lastModified()).min().getAsLong()));
    }

    @Test
    public void testShouldClearSnapshot() throws Exception
    {
        // TableSnapshot variables -> ephemeral / true / false, createdAt -> null / notnull

        Instant now = Instant.now();

        String keyspace = "ks";
        String table = "tbl";
        UUID id = UUID.randomUUID();
        String tag = "someTag";
        Instant snapshotCreation = now.minusSeconds(60);
        Set<File> folders = createFolders(tempFolder);

        List<TableSnapshot> snapshots = new ArrayList<>();

        for (boolean ephemeral : new boolean[]{ true, false })
            for (Instant createdAt : new Instant[]{ snapshotCreation, null })
                snapshots.add(new TableSnapshot(keyspace,
                                                table,
                                                id,
                                                tag,
                                                createdAt, // variable
                                                null,
                                                folders,
                                                ephemeral)); // variable

        List<Pair<String, Long>> testingMethodInputs = new ArrayList<>();

        for (String testingTag : new String[] {null, "", tag, "someothertag"})
            // 0 to deactive byTimestamp logic, now.toEpochMilli as true, snapshot minus 60s as false
            for (long olderThanTimestamp : new long[] {0, now.toEpochMilli(), snapshotCreation.minusSeconds(60).toEpochMilli()})
                testingMethodInputs.add(Pair.create(testingTag, olderThanTimestamp));

        for (Pair<String, Long> methodInput : testingMethodInputs)
        {
            String testingTag = methodInput.left();
            Long olderThanTimestamp = methodInput.right;
            for (TableSnapshot snapshot : snapshots)
            {
                // if shouldClear method returns true, it is only in case
                // 1. snapshot to clear is not ephemeral
                // 2. tag to clear is null, empty, or it is equal to snapshot tag
                // 3. byTimestamp is true
                if (ClearSnapshotTask.getClearSnapshotPredicate(testingTag, Set.of(keyspace), olderThanTimestamp, false).test(snapshot))
                {
                    // shouldClearTag = true
                    boolean shouldClearTag = (testingTag == null || testingTag.isEmpty()) || snapshot.getTag().equals(testingTag);
                    // notEphemeral
                    boolean notEphemeral = !snapshot.isEphemeral();
                    // byTimestamp
                    boolean byTimestamp = true;

                    if (olderThanTimestamp > 0L)
                    {
                        Instant createdAt = snapshot.getCreatedAt();
                        if (createdAt != null)
                            byTimestamp = createdAt.isBefore(Instant.ofEpochMilli(olderThanTimestamp));
                    }

                    assertTrue(notEphemeral);
                    assertTrue(shouldClearTag);
                    assertTrue(byTimestamp);
                }
            }
        }
    }
}
