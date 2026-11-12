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

package org.apache.cassandra.service.disk.usage;

import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.util.List;
import java.util.Random;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link DiskUsageMonitor#getDiskUsage()} counts the space taken by snapshots and backups only once,
 * see CASSANDRA-21883.
 * <p>
 * Snapshots and backups consist of hard links to SSTables. As long as such SSTable is live, its links take
 * no additional space on disk. They start to take the space only after their live SSTable is removed.
 * <p>
 * Every test puts {@link #DATA_SIZE} of data on disk and pretends that the same amount of space is free, so the usage
 * is 0.5 as long as these data are counted once, no matter how many links to them there are.
 */
public class DiskUsageMonitorTest extends CQLTester
{
    private static final int ROWS = 100;
    private static final int VALUE_SIZE = 10 * 1024;
    private static final long DATA_SIZE = (long) ROWS * VALUE_SIZE;

    // a manifest and a schema of a snapshot are way smaller than this
    private static final double TOLERANCE = 0.01;

    private boolean autoSnapshot;
    private boolean incrementalBackups;

    private ColumnFamilyStore cfs;
    private List<File> tableDirectories;
    private DiskUsageMonitor monitor;
    private FileStore store;

    @Before
    public void before() throws Throwable
    {
        autoSnapshot = DatabaseDescriptor.isAutoSnapshot();
        incrementalBackups = DatabaseDescriptor.isIncrementalBackupsEnabled();

        // the tests take all snapshots and backups on their own
        DatabaseDescriptor.setAutoSnapshot(false);
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);

        // each test works with its own keyspace, so nothing but the test changes what is in its directories
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int PRIMARY KEY, v blob) WITH compression = {'enabled': false}");
        cfs = ColumnFamilyStore.getIfExists(keyspace, table);
        cfs.disableAutoCompaction();
        tableDirectories = cfs.getDirectories().getCFDirectories();

        writeData(0);

        // The directories of the keyspace act as data directories, all of them are on one disk.
        // The space taken by other keyspaces, which may be flushed at any time, is left out this way.
        store = mock(FileStore.class);
        Multimap<FileStore, Directories.DataDirectory> directories = HashMultimap.create();
        for (File tableDirectory : tableDirectories)
            directories.put(store, new Directories.DataDirectory(tableDirectory.parent()));

        monitor = spy(new DiskUsageMonitor(() -> directories));
        doReturn(0L).when(monitor).getAllMemtableSize();

        long used = 0;
        for (Directories.DataDirectory directory : directories.values())
            used += directory.getSizeOnDisk();

        assertThat(used).isGreaterThanOrEqualTo(DATA_SIZE);

        // as much free space as there are data, the usage is 0.5
        when(store.getUsableSpace()).thenReturn(used);
        assertUsage(0.5);
    }

    @After
    public void after()
    {
        clearSnapshots();
        DatabaseDescriptor.setAutoSnapshot(autoSnapshot);
        DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackups);
    }

    @Test
    public void testSnapshotOfLiveSSTablesIsNotCounted()
    {
        takeSnapshot("first");
        assertUsage(0.5);

        // without the fix, every other snapshot would add the size of all data again
        takeSnapshot("second");
        takeSnapshot("third");
        assertUsage(0.5);

        clearSnapshots();
        assertUsage(0.5);
    }

    @Test
    public void testSnapshotIsCountedWhenLiveSSTablesAreRemoved()
    {
        takeSnapshot("first");
        assertUsage(0.5);

        // live SSTables are gone, but the snapshot still holds their data on disk
        removeLiveSSTables();
        assertUsage(0.5);

        // the data are gone only after the snapshot is cleared
        clearSnapshots();
        assertUsage(0.0);
    }

    @Test
    public void testSnapshotOfDroppedTableIsCounted() throws Throwable
    {
        takeSnapshot("first");
        assertUsage(0.5);

        execute(String.format("DROP TABLE %s.%s", cfs.keyspace.getName(), cfs.getTableName()));
        LifecycleTransaction.waitForDeletions();
        assertThat(ColumnFamilyStore.getIfExists(cfs.keyspace.getName(), cfs.getTableName())).isNull();

        // the table does not exist anymore, but its snapshot still holds the data on disk
        assertUsage(0.5);

        clearSnapshots();
        assertUsage(0.0);
    }

    @Test
    public void testSnapshotsOfRemovedSSTablesAreCountedOnce()
    {
        takeSnapshot("first");
        takeSnapshot("second");
        assertUsage(0.5);

        // both snapshots link the very same files, they take the space on disk once, not twice
        removeLiveSSTables();
        assertUsage(0.5);

        clearSnapshot("first");
        assertUsage(0.5);

        clearSnapshot("second");
        assertUsage(0.0);
    }

    @Test
    public void testBackupOfLiveSSTablesIsNotCounted() throws Throwable
    {
        DatabaseDescriptor.setIncrementalBackupsEnabled(true);

        // the very same amount of data again, which are linked to backups directory when flushed
        writeData(ROWS);

        File backups = Directories.getBackupsDirectory(cfs.getLiveSSTables().iterator().next().descriptor);
        assertThat(FileUtils.folderSize(backups)).isGreaterThanOrEqualTo(DATA_SIZE);

        // twice as much data as there is free space, it would be 0.75 if the backup was counted
        assertUsage(2.0 / 3);

        // the backup still holds a half of the data after live SSTables are removed
        removeLiveSSTables();
        assertUsage(0.5);
    }

    /**
     * Writes {@link #DATA_SIZE} of data to a new SSTable.
     */
    private void writeData(int firstKey) throws Throwable
    {
        Random random = new Random(firstKey);
        for (int i = firstKey; i < firstKey + ROWS; i++)
        {
            byte[] value = new byte[VALUE_SIZE];
            random.nextBytes(value);
            execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", cfs.keyspace.getName(), cfs.getTableName()), i, ByteBuffer.wrap(value));
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private void takeSnapshot(String tag)
    {
        cfs.snapshot(tag);
    }

    private void clearSnapshots()
    {
        // an empty name stands for all snapshots
        clearSnapshot("");
    }

    private void clearSnapshot(String tag)
    {
        // not via the table, it might be dropped already
        Directories.clearSnapshot(tag, tableDirectories, DatabaseDescriptor.getSnapshotRateLimiter());
    }

    private void removeLiveSSTables()
    {
        cfs.truncateBlockingWithoutSnapshot();
        LifecycleTransaction.waitForDeletions();
        assertThat(cfs.getLiveSSTables()).isEmpty();
    }

    private void assertUsage(double expected)
    {
        assertThat(monitor.getDiskUsage()).isCloseTo(expected, within(TOLERANCE));
    }
}
