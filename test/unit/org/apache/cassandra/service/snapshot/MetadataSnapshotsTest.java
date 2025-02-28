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

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.DiskErrorsHandlerService;

import static org.apache.cassandra.service.snapshot.TableSnapshotTest.createFolders;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.assertj.core.api.Assertions.assertThat;

public class MetadataSnapshotsTest
{
    static long ONE_DAY_SECS = 86400;

    @BeforeClass
    public static void beforeClass()
    {
        CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.setInt(3);
        CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.setInt(3);

        DatabaseDescriptor.daemonInitialization();
        DiskErrorsHandlerService.configure();
    }

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SnapshotManager manager;

    @Before
    public void beforeTest()
    {
        manager = SnapshotManager.instance;
    }

    @After
    public void afterTest() throws Exception
    {
        SnapshotManager.instance.clearAllSnapshots();
        SnapshotManager.instance.close();
    }

    private TableSnapshot generateSnapshotDetails(String tag, Instant expiration, boolean ephemeral)
    {
        try
        {
            return new TableSnapshot("ks",
                                     "tbl",
                                     UUID.randomUUID(),
                                     tag,
                                     Instant.EPOCH,
                                     expiration,
                                     createFolders(temporaryFolder),
                                     ephemeral);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testExpiringSnapshots()
    {
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH, false);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusSeconds(ONE_DAY_SECS), false);
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null, false);
        List<TableSnapshot> snapshots = Arrays.asList(expired, nonExpired, nonExpiring);

        // Create SnapshotManager with 3 snapshots: expired, non-expired and non-expiring
        manager.start(false);
        for (TableSnapshot snapshot : snapshots)
            manager.addSnapshot(snapshot);

        // Only expiring snapshots should be loaded
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).hasSize(2);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(expired);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(nonExpired);
    }

    @Test
    public void testClearExpiredSnapshots()
    {
        manager.start(false);

        // Add 3 snapshots: expired, non-expired and non-expiring
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH, false);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusMillis(ONE_DAY_SECS), false);
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null, false);
        manager.addSnapshot(expired);
        manager.addSnapshot(nonExpired);
        manager.addSnapshot(nonExpiring);

        // Only expiring snapshot should be indexed and all should exist
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).hasSize(2);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(expired);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(nonExpired);
        assertThat(expired.exists()).isTrue();
        assertThat(nonExpired.exists()).isTrue();
        assertThat(nonExpiring.exists()).isTrue();

        // After clearing expired snapshots, expired snapshot should be removed while the others should remain
        manager.clearExpiredSnapshots();
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).hasSize(1);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(nonExpired);
        assertThat(expired.exists()).isFalse();
        assertThat(nonExpired.exists()).isTrue();
        assertThat(nonExpiring.exists()).isTrue();
    }

    @Test
    public void testScheduledCleanup() throws Exception
    {
        manager.start(true);

        // Add 2 expiring snapshots: one to expire in 6 seconds, another in 1 day
        int TTL_SECS = 6;
        TableSnapshot toExpire = generateSnapshotDetails("to-expire", now().plusSeconds(TTL_SECS), false);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusMillis(ONE_DAY_SECS), false);
        manager.addSnapshot(toExpire);
        manager.addSnapshot(nonExpired);

        // Check both snapshots still exist
        assertThat(toExpire.exists()).isTrue();
        assertThat(nonExpired.exists()).isTrue();
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).hasSize(2);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(toExpire);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(nonExpired);

        // Sleep 10 seconds
        Thread.sleep((TTL_SECS + 4) * 1000L);

        // Snapshot with ttl=6s should be gone, while other should remain
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).hasSize(1);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(nonExpired);
        assertThat(toExpire.exists()).isFalse();
        assertThat(nonExpired.exists()).isTrue();
    }

    @Test
    public void testClearSnapshot()
    {
        // Given
        manager.start(false);
        TableSnapshot expiringSnapshot = generateSnapshotDetails("snapshot", now().plusMillis(50000), false);
        manager.addSnapshot(expiringSnapshot);
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).contains(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isTrue();

        // When
        manager.clearSnapshot(expiringSnapshot);

        // Then
        assertThat(manager.getSnapshots(TableSnapshot::isExpiring)).doesNotContain(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isFalse();
    }
}