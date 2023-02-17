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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.DefaultFSErrorHandler;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.service.snapshot.TableSnapshotTest.createFolders;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

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
    public void testLoadSnapshots() throws Exception {
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH, false);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusSeconds(ONE_DAY_SECS), false);
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null, false);
        List<TableSnapshot> snapshots = Arrays.asList(expired, nonExpired, nonExpiring);

        // Create SnapshotManager with 3 snapshots: expired, non-expired and non-expiring
        SnapshotManager manager = new SnapshotManager(3, 3);
        manager.addSnapshots(snapshots);

        // Only expiring snapshots should be loaded
        assertThat(manager.getExpiringSnapshots()).hasSize(2);
        assertThat(manager.getExpiringSnapshots()).contains(expired);
        assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
    }

    @Test
    public void testClearExpiredSnapshots() throws Exception {
        SnapshotManager manager = new SnapshotManager(3, 3);

        // Add 3 snapshots: expired, non-expired and non-expiring
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH, false);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusMillis(ONE_DAY_SECS), false);
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null, false);
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
        SnapshotManager manager = new SnapshotManager(0, 1);
        try
        {
            // Start snapshot manager which should start expired snapshot cleanup thread
            manager.start();

            // Add 2 expiring snapshots: one to expire in 2 seconds, another in 1 day
            int TTL_SECS = 2;
            TableSnapshot toExpire = generateSnapshotDetails("to-expire", now().plusSeconds(TTL_SECS), false);
            TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusMillis(ONE_DAY_SECS), false);
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
        SnapshotManager manager = new SnapshotManager(1, 3);
        TableSnapshot expiringSnapshot = generateSnapshotDetails("snapshot", now().plusMillis(50000), false);
        manager.addSnapshot(expiringSnapshot);
        assertThat(manager.getExpiringSnapshots()).contains(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isTrue();

        // When
        manager.clearSnapshot(expiringSnapshot);

        // Then
        assertThat(manager.getExpiringSnapshots()).doesNotContain(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isFalse();
    }

    @Test // see CASSANDRA-18211
    public void testConcurrentClearingOfSnapshots() throws Exception
    {

        AtomicReference<Long> firstInvocationTime = new AtomicReference<>(0L);
        AtomicReference<Long> secondInvocationTime = new AtomicReference<>(0L);

        SnapshotManager manager = new SnapshotManager(0, 5)
        {
            @Override
            public synchronized void clearSnapshot(TableSnapshot snapshot)
            {
                if (snapshot.getTag().equals("mysnapshot"))
                {
                    firstInvocationTime.set(currentTimeMillis());
                    Uninterruptibles.sleepUninterruptibly(10, SECONDS);
                }
                else if (snapshot.getTag().equals("mysnapshot2"))
                {
                    secondInvocationTime.set(currentTimeMillis());
                }
                super.clearSnapshot(snapshot);
            }
        };

        TableSnapshot expiringSnapshot = generateSnapshotDetails("mysnapshot", Instant.now().plusSeconds(15), false);
        manager.addSnapshot(expiringSnapshot);

        manager.resumeSnapshotCleanup();

        Thread nonExpiringSnapshotCleanupThred = new Thread(() -> manager.clearSnapshot(generateSnapshotDetails("mysnapshot2", null, false)));

        // wait until the first snapshot expires
        await().pollInterval(1, SECONDS)
               .pollDelay(0, SECONDS)
               .timeout(1, MINUTES)
               .until(() -> firstInvocationTime.get() > 0);

        // this will block until the first snapshot is cleaned up
        nonExpiringSnapshotCleanupThred.start();
        nonExpiringSnapshotCleanupThred.join();

        assertTrue(secondInvocationTime.get() - firstInvocationTime.get() > 10_000);
    }
}