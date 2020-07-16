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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.ReplicaFilteringProtectionOptions.DEFAULT_FAIL_THRESHOLD;
import static org.apache.cassandra.config.ReplicaFilteringProtectionOptions.DEFAULT_WARN_THRESHOLD;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

/**
 * Exercises the functionality of {@link org.apache.cassandra.service.ReplicaFilteringProtection}, the
 * mechanism that ensures distributed index and filtering queries at read consistency levels > ONE/LOCAL_ONE
 * avoid stale replica results.
 */
public class ReplicaFilteringProtectionTest extends TestBaseImpl
{
    private static final int REPLICAS = 2;
    private static final int ROWS = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(REPLICAS)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .set("commitlog_sync", "batch")
                                                          .set("num_tokens", 1)).start());

        // Make sure we start w/ the correct defaults:
        cluster.get(1).runOnInstance(() -> assertEquals(DEFAULT_WARN_THRESHOLD, StorageService.instance.getCachedReplicaRowsWarnThreshold()));
        cluster.get(1).runOnInstance(() -> assertEquals(DEFAULT_FAIL_THRESHOLD, StorageService.instance.getCachedReplicaRowsFailThreshold()));
    }

    @AfterClass
    public static void teardown()
    {
        cluster.close();
    }

    @Test
    public void testMissedUpdatesBelowCachingWarnThreshold()
    {
        String tableName = "missed_updates_no_warning";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text)"));

        // The warning threshold provided is one more than the total number of rows returned
        // to the coordinator from all replicas and therefore should not be triggered.
        testMissedUpdates(tableName, REPLICAS * ROWS, Integer.MAX_VALUE, false);
    }

    @Test
    public void testMissedUpdatesAboveCachingWarnThreshold()
    {
        String tableName = "missed_updates_cache_warn";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text)"));

        // The warning threshold provided is one less than the total number of rows returned
        // to the coordinator from all replicas and therefore should be triggered but not fail the query.
        testMissedUpdates(tableName, REPLICAS * ROWS - 1, Integer.MAX_VALUE, true);
    }

    @Test
    public void testMissedUpdatesAroundCachingFailThreshold()
    {
        String tableName = "missed_updates_cache_fail";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text)"));

        // The failure threshold provided is exactly the total number of rows returned
        // to the coordinator from all replicas and therefore should just warn.
        testMissedUpdates(tableName, 1, REPLICAS * ROWS, true);

        try
        {
            // The failure threshold provided is one less than the total number of rows returned
            // to the coordinator from all replicas and therefore should fail the query.
            testMissedUpdates(tableName, 1, REPLICAS * ROWS - 1, true);
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass().getName(), OverloadedException.class.getName());
        }
    }

    private void testMissedUpdates(String tableName, int warnThreshold, int failThreshold, boolean shouldWarn)
    {
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsWarnThreshold(warnThreshold));
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsFailThreshold(failThreshold));

        String fullTableName = KEYSPACE + '.' + tableName;

        for (int i = 0; i < ROWS; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + fullTableName + "(k, v) VALUES (?, 'old')", ALL, i);
        }

        String query = "SELECT * FROM " + fullTableName + " WHERE v = ? LIMIT ? ALLOW FILTERING";

        Object[][] initialRows = cluster.coordinator(1).execute(query, ALL, "old", ROWS);
        assertRows(initialRows, row(1, "old"), row(0, "old"), row(2, "old"));

        // Update all rows on only one replica, leaving the entire dataset of the remaining replica out-of-date:
        for (int i = 0; i < ROWS; i++)
        {
            cluster.get(1).executeInternal("UPDATE " + fullTableName + " SET v = 'new' WHERE k = ?", i);
        }

        // The replica that missed the results creates a mismatch at every row, and we threfore cache a version
        // of that row for all replicas.
        SimpleQueryResult oldResult = cluster.coordinator(1).executeWithResult(query, ALL, "old", ROWS);
        assertRows(oldResult.toObjectArrays());
        List<String> oldWarnings = oldResult.warnings();
        assertEquals(shouldWarn, oldWarnings.stream().anyMatch(w -> w.contains("cached_replica_rows_warn_threshold")));
        assertEquals(shouldWarn ? 1 : 0, oldWarnings.size());

        // The previous query peforms a blocking read-repair, which removes replica divergence. This
        // will only warn, therefore, if the warning threshold is actually below the number of replicas.
        // (i.e. The row cache counter is decremented/reset as each partition is consumed.)
        SimpleQueryResult newResult = cluster.coordinator(1).executeWithResult(query, ALL, "new", ROWS);
        Object[][] newRows = newResult.toObjectArrays();
        assertRows(newRows, row(1, "new"), row(0, "new"), row(2, "new"));
        List<String> newWarnings = newResult.warnings();
        assertEquals(warnThreshold < REPLICAS, newWarnings.stream().anyMatch(w -> w.contains("cached_replica_rows_warn_threshold")));

        // Update all rows on only one replica, leaving the entire dataset of the remaining replica out-of-date:
        for (int i = 0; i < ROWS; i++)
        {
            cluster.get(1).executeInternal("UPDATE " + fullTableName + " SET v = 'future' WHERE k = ?", i);
        }

        // Another mismatch is introduced, and we once again cache a version of each row during resolution.
        SimpleQueryResult futureResult = cluster.coordinator(1).executeWithResult(query, ALL, "future", ROWS);
        Object[][] futureRows = futureResult.toObjectArrays();
        assertRows(futureRows, row(1, "future"), row(0, "future"), row(2, "future"));
        List<String> futureWarnings = futureResult.warnings();
        assertEquals(shouldWarn, futureWarnings.stream().anyMatch(w -> w.contains("cached_replica_rows_warn_threshold")));
        assertEquals(shouldWarn ? 1 : 0, futureWarnings.size());
    }
}
