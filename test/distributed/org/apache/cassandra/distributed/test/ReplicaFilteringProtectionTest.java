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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.exceptions.TooManyCachedRowsException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

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
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text) WITH read_repair = 'NONE'"));

        // The warning threshold provided is exactly the total number of rows returned
        // to the coordinator from all replicas and therefore should not be triggered.
        testMissedUpdates(tableName, REPLICAS * ROWS, Integer.MAX_VALUE, false);
    }

    @Test
    public void testMissedUpdatesAboveCachingWarnThreshold()
    {
        String tableName = "missed_updates_cache_warn";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text) WITH read_repair = 'NONE'"));

        // The warning threshold provided is one less than the total number of rows returned
        // to the coordinator from all replicas and therefore should be triggered but not fail the query.
        testMissedUpdates(tableName, REPLICAS * ROWS - 1, Integer.MAX_VALUE, true);
    }

    @Test
    public void testMissedUpdatesAroundCachingFailThreshold()
    {
        String tableName = "missed_updates_cache_fail";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text) WITH read_repair = 'NONE'"));

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
            assertEquals(e.getClass().getName(), TooManyCachedRowsException.class.getName());
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

        // TODO: These should be able to use ICoordinator#executeWithResult() once CASSANDRA-15920 is resolved.
        Object[] oldResponse = cluster.get(1).callOnInstance(() -> executeInternal(query, "old", ROWS));
        Object[][] oldRows = (Object[][]) oldResponse[0];
        assertRows(oldRows);
        @SuppressWarnings("unchecked") List<String> oldWarnings = (List<String>) oldResponse[1];
        assertEquals(shouldWarn, oldWarnings.stream().anyMatch(w -> w.contains("cached_replica_rows_warn_threshold")));
        assertEquals(shouldWarn ? 1 : 0, oldWarnings.size());

        Object[] newResponse = cluster.get(1).callOnInstance(() -> executeInternal(query, "new", ROWS));
        Object[][] newRows = (Object[][]) newResponse[0];
        assertRows(newRows, row(1, "new"), row(0, "new"), row(2, "new"));
        @SuppressWarnings("unchecked") List<String> newWarnings = (List<String>) newResponse[1];
        assertEquals(shouldWarn, newWarnings.stream().anyMatch(w -> w.contains("cached_replica_rows_warn_threshold")));
        assertEquals(shouldWarn ? 1 : 0, newWarnings.size());
    }

    // TODO: This should no longer be necessary once CASSANDRA-15920 is resolved.
    private static Object[] executeInternal(String query, Object... boundValues)
    {
        ClientState clientState = ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042));
        ClientWarn.instance.captureWarnings();

        CQLStatement prepared = QueryProcessor.getStatement(query, clientState);
        List<ByteBuffer> boundBBValues = new ArrayList<>();

        for (Object boundValue : boundValues)
            boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

        prepared.validate(QueryState.forInternalCalls().getClientState());
        ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                             QueryOptions.create(ConsistencyLevel.ALL,
                                                                 boundBBValues,
                                                                 false,
                                                                 Integer.MAX_VALUE,
                                                                 null,
                                                                 null,
                                                                 ProtocolVersion.V4,
                                                                 null),
                                             System.nanoTime());

        List<String> warnings = ClientWarn.instance.getWarnings();
        return new Object[] { RowUtil.toQueryResult(res).toObjectArrays(), warnings == null ? Collections.emptyList() : warnings };
    }
}
