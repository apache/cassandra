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
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.DefaultFSErrorHandler;

import static org.apache.cassandra.service.snapshot.TableSnapshotTest.createFolders;
import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotManagerTest
{
    static long ONE_DAY_SECS = 86400;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
    }

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private TableSnapshot generateSnapshotDetails(String tag, Instant expiration) throws Exception {
        return new TableSnapshot(
            "ks",
            "tbl",
            tag,
            Instant.EPOCH,
            expiration,
            createFolders(temporaryFolder),
            (file) -> 0L
        );
    }

    @Test
    public void testLoadSnapshots() throws Exception {
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", Instant.now().plusSeconds(ONE_DAY_SECS));
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null);
        List<TableSnapshot> snapshots = Arrays.asList(expired, nonExpired, nonExpiring);

        // Create SnapshotManager with 3 snapshots: expired, non-expired and non-expiring
        SnapshotManager manager = new SnapshotManager(3, 3, snapshots::stream);
        manager.loadSnapshots();

        // Only expiring snapshots should be loaded
        assertThat(manager.getExpiringSnapshots()).hasSize(2);
        assertThat(manager.getExpiringSnapshots()).contains(expired);
        assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
    }

    @Test
    public void testClearExpiredSnapshots() throws Exception {
        SnapshotManager manager = new SnapshotManager(3, 3, Stream::empty);

        // Add 3 snapshots: expired, non-expired and non-expiring
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", Instant.now().plusMillis(ONE_DAY_SECS));
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null);
        manager.addSnapshot(expired);
        manager.addSnapshot(nonExpired);
        manager.addSnapshot(nonExpiring);

        // Only expiring snapshot should be indexed and all should exist
        assertThat(manager.getExpiringSnapshots()).hasSize(2);
        assertThat(manager.getExpiringSnapshots()).contains(expired);
        assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
        assertThat(expired.exists()).isTrue();
        assertThat(nonExpired.exists()).isTrue();
        assertThat(nonExpiring.exists()).isTrue();

        // After clearing expired snapshots, expired snapshot should be removed while the others should remain
        manager.clearExpiredSnapshots();
        assertThat(manager.getExpiringSnapshots()).hasSize(1);
        assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
        assertThat(expired.exists()).isFalse();
        assertThat(nonExpired.exists()).isTrue();
        assertThat(nonExpiring.exists()).isTrue();
    }

    @Test
    public void testScheduledCleanup() throws Exception {
        SnapshotManager manager = new SnapshotManager(0, 1, Stream::empty);
        try
        {
            // Start snapshot manager which should start expired snapshot cleanup thread
            manager.start();

            // Add 2 expiring snapshots: one to expire in 2 seconds, another in 1 day
            int TTL_SECS = 2;
            TableSnapshot toExpire = generateSnapshotDetails("to-expire", Instant.now().plusSeconds(TTL_SECS));
            TableSnapshot nonExpired = generateSnapshotDetails("non-expired", Instant.now().plusMillis(ONE_DAY_SECS));
            manager.addSnapshot(toExpire);
            manager.addSnapshot(nonExpired);

            // Check both snapshots still exist
            assertThat(toExpire.exists()).isTrue();
            assertThat(nonExpired.exists()).isTrue();
            assertThat(manager.getExpiringSnapshots()).hasSize(2);
            assertThat(manager.getExpiringSnapshots()).contains(toExpire);
            assertThat(manager.getExpiringSnapshots()).contains(nonExpired);

            // Sleep 4 seconds
            Thread.sleep((TTL_SECS + 2) * 1000L);

            // Snapshot with ttl=2s should be gone, while other should remain
            assertThat(manager.getExpiringSnapshots()).hasSize(1);
            assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
            assertThat(toExpire.exists()).isFalse();
            assertThat(nonExpired.exists()).isTrue();
        }
        finally
        {
            manager.stop();
        }
    }

    @Test
    public void testClearSnapshot() throws Exception
    {
        // Given
        SnapshotManager manager = new SnapshotManager(1, 3, Stream::empty);
        TableSnapshot expiringSnapshot = generateSnapshotDetails("snapshot", Instant.now().plusMillis(50000));
        manager.addSnapshot(expiringSnapshot);
        assertThat(manager.getExpiringSnapshots()).contains(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isTrue();

        // When
        manager.clearSnapshot(expiringSnapshot);

        // Then
        assertThat(manager.getExpiringSnapshots()).doesNotContain(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isFalse();
    }
}
