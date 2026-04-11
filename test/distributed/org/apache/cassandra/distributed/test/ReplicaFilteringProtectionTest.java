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

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
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
 * Exercises the functionality of {@link org.apache.cassandra.service.reads.ReplicaFilteringProtection}, the
 * mechanism that ensures distributed index and filtering queries at read consistency levels > ONE/LOCAL_ONE
 * avoid stale replica results.
 */
public class ReplicaFilteringProtectionTest extends TestBaseImpl
{
    private static final int REPLICAS = 2;
    private static final int PARTITIONS = 3;
    private static final int ROWS_PER_PARTITION = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(REPLICAS)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)).start());

        // Make sure we start w/ the correct defaults:
        cluster.get(1).runOnInstance(() -> assertEquals(DEFAULT_WARN_THRESHOLD, StorageService.instance.getCachedReplicaRowsWarnThreshold()));
        cluster.get(1).runOnInstance(() -> assertEquals(DEFAULT_FAIL_THRESHOLD, StorageService.instance.getCachedReplicaRowsFailThreshold()));
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testMissedUpdatesBelowCachingWarnThreshold()
    {
        String tableName = "missed_updates_no_warning";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int, c int, v text, PRIMARY KEY (k, c))"));

        // The warning threshold provided is one more than the total number of rows returned
        // to the coordinator from all replicas and therefore should not be triggered.
        testMissedUpdates(tableName, REPLICAS * ROWS_PER_PARTITION, Integer.MAX_VALUE, false);
    }

    @Test
    public void testMissedUpdatesAboveCachingWarnThreshold()
    {
        String tableName = "missed_updates_cache_warn";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int, c int, v text, PRIMARY KEY (k, c))"));

        // The warning threshold provided is one less than the total number of rows returned
        // to the coordinator from all replicas and therefore should be triggered but not fail the query.
        testMissedUpdates(tableName, REPLICAS * ROWS_PER_PARTITION - 1, Integer.MAX_VALUE, true);
    }

    @Test
    public void testMissedUpdatesAroundCachingFailThreshold()
    {
        String tableName = "missed_updates_cache_fail";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int, c int, v text, PRIMARY KEY (k, c))"));

        // The failure threshold provided is exactly the total number of rows returned
        // to the coordinator from all replicas and therefore should just warn.
        testMissedUpdates(tableName, 1, REPLICAS * ROWS_PER_PARTITION, true);

        try
        {
            // The failure threshold provided is one less than the total number of rows returned
            // to the coordinator from all replicas and therefore should fail the query.
            testMissedUpdates(tableName, 1, REPLICAS * ROWS_PER_PARTITION - 1, true);
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getClass().getName(), OverloadedException.class.getName());
        }
    }

    private void testMissedUpdates(String tableName, int warnThreshold, int failThreshold, boolean shouldWarn)
    {
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsWarnThreshold(warnThreshold));
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsFailThreshold(failThreshold));

        String fullTableName = KEYSPACE + '.' + tableName;

        // Case 1: Insert and query rows at ALL to verify baseline.
        for (int i = 0; i < PARTITIONS; i++)
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
                cluster.coordinator(1).execute("INSERT INTO " + fullTableName + "(k, c, v) VALUES (?, ?, 'old')", ALL, i, j);

        long histogramSampleCount = rowsCachedPerQueryCount(cluster.get(1), tableName);

        String query = "SELECT * FROM " + fullTableName + " WHERE v = ? LIMIT ? ALLOW FILTERING";

        Object[][] initialRows = cluster.coordinator(1).execute(query, ALL, "old", PARTITIONS * ROWS_PER_PARTITION);
        assertRows(initialRows,
                   row(1, 0, "old"), row(1, 1, "old"), row(1, 2, "old"),
                   row(0, 0, "old"), row(0, 1, "old"), row(0, 2, "old"),
                   row(2, 0, "old"), row(2, 1, "old"), row(2, 2, "old"));

        // Make sure only one sample was recorded for the query.
        assertEquals(histogramSampleCount + 1, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 2: Update all rows on only one replica, leaving the entire dataset of the remaining replica out-of-date.
        updateAllRowsOn(1, fullTableName, "new");

        // The replica that missed the results creates a mismatch at every row, and we therefore cache a version
        // of that row for all replicas.
        SimpleQueryResult oldResult = cluster.coordinator(1).executeWithResult(query, ALL, "old", PARTITIONS * ROWS_PER_PARTITION);
        assertRows(oldResult.toObjectArrays());
        verifyWarningState(shouldWarn, oldResult);

        // We should have made 3 row "completion" requests.
        assertEquals(PARTITIONS, protectionQueryCount(cluster.get(1), tableName));

        // Queries should be caching 3 rows per partition per replica at any given time.
        assertEquals(PARTITIONS * REPLICAS, maxRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 2, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 3: Observe the effects of blocking read-repair.

        // The previous query peforms a blocking read-repair, which removes replica divergence. This
        // will only warn, therefore, if the warning threshold is actually below the number of replicas.
        // (i.e. The row cache counter is decremented/reset as each partition is consumed.)
        SimpleQueryResult newResult = cluster.coordinator(1).executeWithResult(query, ALL, "new", PARTITIONS * ROWS_PER_PARTITION);
        Object[][] newRows = newResult.toObjectArrays();
        assertRows(newRows,
                   row(1, 0, "new"), row(1, 1, "new"), row(1, 2, "new"),
                   row(0, 0, "new"), row(0, 1, "new"), row(0, 2, "new"),
                   row(2, 0, "new"), row(2, 1, "new"), row(2, 2, "new"));

        verifyWarningState(warnThreshold < REPLICAS * ROWS_PER_PARTITION, newResult);

        // We still sould only have made 3 row "completion" requests, with no replica divergence in the last query.
        assertEquals(PARTITIONS, protectionQueryCount(cluster.get(1), tableName));

        // Queries should be caching 3 rows per partition per replica at any given time.
        assertEquals(REPLICAS * ROWS_PER_PARTITION, minRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 3, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 4: Introduce another mismatch by updating all rows on only one replica.

        updateAllRowsOn(1, fullTableName, "future");

        // Another mismatch is introduced, and we once again cache a version of each row during resolution.
        SimpleQueryResult futureResult = cluster.coordinator(1).executeWithResult(query, ALL, "future", PARTITIONS * ROWS_PER_PARTITION);
        Object[][] futureRows = futureResult.toObjectArrays();
        assertRows(futureRows,
                   row(1, 0, "future"), row(1, 1, "future"), row(1, 2, "future"),
                   row(0, 0, "future"), row(0, 1, "future"), row(0, 2, "future"),
                   row(2, 0, "future"), row(2, 1, "future"), row(2, 2, "future"));

        verifyWarningState(shouldWarn, futureResult);

        // We sould have made 3 more row "completion" requests.
        assertEquals(PARTITIONS * 2, protectionQueryCount(cluster.get(1), tableName));

        // Queries should be caching 3 rows per partition per replica at any given time.
        assertEquals(PARTITIONS * REPLICAS, maxRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 4, rowsCachedPerQueryCount(cluster.get(1), tableName));
    }

    private void updateAllRowsOn(int node, String table, String value)
    {
        for (int i = 0; i < PARTITIONS; i++)
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
                cluster.get(node).executeInternal("UPDATE " + table + " SET v = ? WHERE k = ? and c = ?", value, i, j);
    }

    private void verifyWarningState(boolean shouldWarn, SimpleQueryResult futureResult)
    {
        List<String> futureWarnings = futureResult.warnings();
        assertEquals(shouldWarn, futureWarnings.stream().anyMatch(w -> w.contains("cached_replica_rows_warn_threshold")));
        assertEquals(shouldWarn ? 1 : 0, futureWarnings.size());
    }

    private long protectionQueryCount(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.replicaFilteringProtectionRequests.getCount());
    }

    private long maxRowsCachedPerQuery(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.rfpRowsCachedPerQuery.getSnapshot().getMax());
    }

    private long minRowsCachedPerQuery(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.rfpRowsCachedPerQuery.getSnapshot().getMin());
    }

    private long rowsCachedPerQueryCount(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.rfpRowsCachedPerQuery.getCount());
    }
}
