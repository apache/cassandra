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

package org.apache.cassandra.distributed.upgrade;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.utils.StorageCompatibilityMode.NONE;
import static org.apache.cassandra.utils.StorageCompatibilityMode.UPGRADING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests TTL the overflow policy triggers at the correct limit: year 2038 <=nb or 2186 >=oa
 * <p>
 * <=oa overflow policy triggers at year 2038. That could be <=4.1 or 5.0 with 4.x storage compatibility
 * >oa overflow policy triggers at year 2106. That is >=5.0 using >=5.x storage compatibility
 *
 * @see StorageCompatibilityMode
 */
public class MixedModeTTLOverflowUpgradeTest extends UpgradeTestBase
{
    private static final int SMALL_TTL = 3600;

    private static final String T_REGULAR = "table_regular";
    private static final String T_CLUST = "table_clust";
    private static final String T_STATIC = "table_static";
    private static final String T_COMPLEX = "table_complex";
    private static final String T_FROZEN = "table_frozen";
    private static final String T_INDEX = "table_indexed";
    private static final String INDEX = "idx";
    private static final String TYPE = "complex_type";

    private static final int NODE_1_MAX_TTL_KEY_OFFSET = 1000;
    private static final int NODE_2_MAX_TTL_KEY_OFFSET = 2000;
    private static final int NODE_1_MIXED_TTL_KEY_OFFSET = 3000;
    private static final int NODE_2_MIXED_TTL_KEY_OFFSET = 4000;

    enum Step
    {
        BEFORE_UPGRADE,
        NODE_1_UPGRADED,
        NODE_2_UPGRADED,
        NODE_1_UPGRADING_MODE,
        NODE_2_UPGRADING_MODE,
        NODE_1_FINAL,
        NODE_2_FINAL,
    }

    private static volatile long clusterStatupTime = 0;

    @Test
    public void testTTLOverflowDuringUpgrade() throws Throwable
    {
        testTTLOverflow((cluster, node) -> {
            cluster.disableAutoCompaction(KEYSPACE);
            if (node == 1) // only node1 is upgraded, and the cluster is in mixed versions mode
            {
                verify(Step.NODE_1_UPGRADED, cluster, true);

                // We restart the upgraded node out of 4.0 storage compatibility,
                // and we set it to be compatible with 5.0.
                // 2038 should still be the limit because there is still a not upgraded node.
                restartNodeWithCompatibilityMode(cluster, 1, UPGRADING);
                verify(Step.NODE_1_UPGRADING_MODE, cluster, true);
            }
            else // both nodes have been upgraded, and the cluster isn't in mixed version mode anymore
            {
                // Once we have completed the upgrade, 2038 should still be the limit because there is still one node
                // in 5.0 storage compatibility mode.
                verify(Step.NODE_2_UPGRADED, cluster, true);

                // We restart the last upgraded node in 5.0 compatibility mode, so both nodes are now in 5.0
                // compatibility mode, and the limit should be 2106.
                restartNodeWithCompatibilityMode(cluster, 2, UPGRADING);
                verify(Step.NODE_2_UPGRADING_MODE, cluster, false);

                // We restart get both nodes out of compatibility mode, so the limit should be 2106.
                restartNodeWithCompatibilityMode(cluster, 1, NONE);
                verify(Step.NODE_1_FINAL, cluster, false);
                restartNodeWithCompatibilityMode(cluster, 2, NONE);
                verify(Step.NODE_2_FINAL, cluster, false);
            }
        });
    }

    @Test
    public void testTTLOverflowAfterUpgrade() throws Throwable
    {
        testTTLOverflow((cluster, node) -> {
            cluster.disableAutoCompaction(KEYSPACE);
            if (node == 1) // only node1 is upgraded, and the cluster is in mixed versions mode
            {
                verify(Step.NODE_1_UPGRADED, cluster, true);
            }
            else // both nodes have been upgraded, and the cluster isn't in mixed version mode anymore
            {
                verify(Step.NODE_2_UPGRADED, cluster, true);

                // We restart one node on 5.0 >oa hence 2038 should still be the limit as the other node is 5.0 <=oa
                // We're on compatibility mode where oa and oa nodes are a possibility
                restartNodeWithCompatibilityMode(cluster, 1, UPGRADING);
                verify(Step.NODE_1_UPGRADING_MODE, cluster, true);

                // We restart the other node so they're all on 5.0 >oa hence 2106 should be the limit
                restartNodeWithCompatibilityMode(cluster, 2, UPGRADING);
                verify(Step.NODE_2_UPGRADING_MODE, cluster, false);

                // We restart the cluster out of compatibility mode once everything is 5.0oa TTL 2106
                restartNodeWithCompatibilityMode(cluster, 1, NONE);
                verify(Step.NODE_1_FINAL, cluster, false);

                restartNodeWithCompatibilityMode(cluster, 2, NONE);
                verify(Step.NODE_2_FINAL, cluster, false);
            }
        });
    }

    private static void testTTLOverflow(RunOnClusterAndNode runAfterNodeUpgrade) throws Throwable
    {
        new TestCase()
                .nodes(2)
                .nodesToUpgradeOrdered(1, 2)
                .upgradesToCurrentFrom(v40)
                .setup(cluster -> {
                    cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v1 int, v2 int)", KEYSPACE, T_REGULAR));
                    cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int, c int, v1 int, v2 int, PRIMARY KEY (k, c))", KEYSPACE, T_CLUST));
                    cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int, c int, v1 int static, v2 int, PRIMARY KEY (k, c))", KEYSPACE, T_STATIC));
                    cluster.schemaChange(String.format("CREATE TYPE %s.%s (a int, b int)", KEYSPACE, TYPE));
                    cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v1 %s, v2 int)", KEYSPACE, T_COMPLEX, TYPE));
                    cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v1 frozen<%s>, v2 int)", KEYSPACE, T_FROZEN, TYPE));
                    cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v1 int, v2 int)", KEYSPACE, T_INDEX));
                    cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s (v1)", INDEX, KEYSPACE, T_INDEX));

                    cluster.disableAutoCompaction(KEYSPACE);
                    clusterStatupTime = Clock.Global.currentTimeMillis();
                    verify(Step.BEFORE_UPGRADE, cluster, true);
                })
                .runAfterNodeUpgrade(runAfterNodeUpgrade)
                .run();
    }

    private static void verify(Step step, UpgradeableCluster cluster, boolean expectPolicyTriggerAt2038)
    {
        insert(cluster, step.ordinal(), expectPolicyTriggerAt2038);
        query(cluster, step.ordinal(), expectPolicyTriggerAt2038);
        cluster.coordinator(1).instance().flush(KEYSPACE);
        query(cluster, step.ordinal(), expectPolicyTriggerAt2038);
        cluster.coordinator(2).instance().flush(KEYSPACE);
        query(cluster, step.ordinal(), expectPolicyTriggerAt2038);
    }

    private static void insert(UpgradeableCluster cluster, int step, boolean expectPolicyTriggerAt2038)
    {
        BiConsumer<ICoordinator, String> execute = (c, q) -> {
            if (expectPolicyTriggerAt2038)
                assertPolicyTriggersAt2038(c, q);
            else
                c.execute(q, ALL);
        };

        inserts(step + NODE_1_MAX_TTL_KEY_OFFSET, Attributes.MAX_TTL).forEach(q -> execute.accept(cluster.coordinator(1), q));
        inserts(step + NODE_2_MAX_TTL_KEY_OFFSET, Attributes.MAX_TTL).forEach(q -> execute.accept(cluster.coordinator(1), q));
        inserts(step + NODE_1_MIXED_TTL_KEY_OFFSET, SMALL_TTL).forEach(q -> cluster.coordinator(1).execute(q, ALL));
        inserts(step + NODE_2_MIXED_TTL_KEY_OFFSET, SMALL_TTL).forEach(q -> cluster.coordinator(2).execute(q, ALL));
        v1Updates(step + NODE_1_MIXED_TTL_KEY_OFFSET, Attributes.MAX_TTL).forEach(q -> execute.accept(cluster.coordinator(1), q));
        v1Updates(step + NODE_2_MIXED_TTL_KEY_OFFSET, Attributes.MAX_TTL).forEach(q -> execute.accept(cluster.coordinator(2), q));
    }

    private static int getTTL(Object[][] result)
    {
        Object r = result[0][0];
        if (r instanceof Number)
            return ((Number) r).intValue();
        else
            return ((List<? extends Number>) r).get(0).intValue();
    }

    private static void query(UpgradeableCluster cluster, int step, boolean expectPolicyTriggerAt2038)
    {
        BiConsumer<String, Integer> verifyQuery = (q, expectedTTL) -> {
            int ttlLocal1 = getTTL(cluster.coordinator(1).execute(q, LOCAL_ONE));
            int ttlLocal2 = getTTL(cluster.coordinator(2).execute(q, LOCAL_ONE));
            int ttlAll1 = getTTL(cluster.coordinator(1).execute(q, ALL));
            int ttlAll2 = getTTL(cluster.coordinator(2).execute(q, ALL));
            long t1 = Clock.Global.currentTimeMillis();
            int delta = (int) Math.max(1 + (t1 - clusterStatupTime) / 1000, 1);
            assertThat(ttlLocal1).describedAs("TTL from query %s", q).isCloseTo(expectedTTL, Offset.offset(delta));
            assertThat(ttlLocal2).describedAs("TTL from query %s", q).isCloseTo(expectedTTL, Offset.offset(delta));
            assertThat(ttlAll1).describedAs("TTL from query %s", q).isCloseTo(expectedTTL, Offset.offset(delta));
            assertThat(ttlAll2).describedAs("TTL from query %s", q).isCloseTo(expectedTTL, Offset.offset(delta));
        };

        if (!expectPolicyTriggerAt2038)
        {
            queries(step + NODE_1_MAX_TTL_KEY_OFFSET, "v1").forEach(q -> verifyQuery.accept(q, Attributes.MAX_TTL));
            queries(step + NODE_2_MAX_TTL_KEY_OFFSET, "v1").forEach(q -> verifyQuery.accept(q, Attributes.MAX_TTL));
            queries(step + NODE_1_MAX_TTL_KEY_OFFSET, "v2").forEach(q -> verifyQuery.accept(q, Attributes.MAX_TTL));
            queries(step + NODE_2_MAX_TTL_KEY_OFFSET, "v2").forEach(q -> verifyQuery.accept(q, Attributes.MAX_TTL));
            queries(step + NODE_1_MIXED_TTL_KEY_OFFSET, "v1").forEach(q -> verifyQuery.accept(q, Attributes.MAX_TTL));
            queries(step + NODE_2_MIXED_TTL_KEY_OFFSET, "v1").forEach(q -> verifyQuery.accept(q, Attributes.MAX_TTL));
        }
        queries(step + NODE_1_MIXED_TTL_KEY_OFFSET, "v2").forEach(q -> verifyQuery.accept(q, SMALL_TTL));
        queries(step + NODE_2_MIXED_TTL_KEY_OFFSET, "v2").forEach(q -> verifyQuery.accept(q, SMALL_TTL));
    }

    private static Stream<String> inserts(int key, int ttl)
    {
        return Stream.of(String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, %d, %d) USING TTL %d", KEYSPACE, T_REGULAR, key, key, key, ttl),
                         String.format("INSERT INTO %s.%s (k, c, v1, v2) VALUES (%d, %d, %d, %d) USING TTL %d", KEYSPACE, T_CLUST, key, key, key, key, ttl),
                         String.format("INSERT INTO %s.%s (k, c, v1, v2) VALUES (%d, %d, %d, %d) USING TTL %d", KEYSPACE, T_STATIC, key, key, key, key, ttl),
                         String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, {a: %d, b: %d}, %d) USING TTL %d", KEYSPACE, T_COMPLEX, key, key, key, key, ttl),
                         String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, {a: %d, b: %d}, %d) USING TTL %d", KEYSPACE, T_FROZEN, key, key, key, key, ttl),
                         String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, %d, %d) USING TTL %d", KEYSPACE, T_INDEX, key, key, key, ttl));
    }

    private static Stream<String> v1Updates(int key, int ttl)
    {
        return Stream.of(String.format("UPDATE %s.%s USING TTL %d SET v1 = %d WHERE k = %d", KEYSPACE, T_REGULAR, ttl, key * 100, key),
                         String.format("UPDATE %s.%s USING TTL %d SET v1 = %d WHERE k = %d AND c = %d", KEYSPACE, T_CLUST, ttl, key * 100, key, key),
                         String.format("UPDATE %s.%s USING TTL %d SET v1 = %d WHERE k = %d", KEYSPACE, T_STATIC, ttl, key * 100, key),
                         String.format("UPDATE %s.%s USING TTL %d SET v1 = {a: %d, b: %d} WHERE k = %d", KEYSPACE, T_COMPLEX, ttl, key * 100, key * 100, key),
                         String.format("UPDATE %s.%s USING TTL %d SET v1 = {a: %d, b: %d} WHERE k = %d", KEYSPACE, T_FROZEN, ttl, key * 100, key * 100, key),
                         String.format("UPDATE %s.%s USING TTL %d SET v1 = %d WHERE k = %d", KEYSPACE, T_INDEX, ttl, key * 100, key));
    }

    private static Stream<String> queries(int key, String col)
    {
        return Stream.of(String.format("SELECT ttl(%s) FROM %s.%s WHERE k = %d", col, KEYSPACE, T_REGULAR, key),
                         String.format("SELECT ttl(%s) FROM %s.%s WHERE k = %d AND c = %d", col, KEYSPACE, T_CLUST, key, key),
                         String.format("SELECT ttl(%s) FROM %s.%s WHERE k = %d", col, KEYSPACE, T_STATIC, key),
                         String.format("SELECT ttl(%s) FROM %s.%s WHERE k = %d", col, KEYSPACE, T_COMPLEX, key),
                         String.format("SELECT ttl(%s) FROM %s.%s WHERE k = %d", col, KEYSPACE, T_FROZEN, key),
                         String.format("SELECT ttl(%s) FROM %s.%s WHERE k = %d", col, KEYSPACE, T_INDEX, key));
    }

    private static void restartNodeWithCompatibilityMode(UpgradeableCluster cluster, int node, StorageCompatibilityMode mode) throws Throwable
    {
        cluster.get(node).shutdown().get();
        cluster.get(node).config().set("storage_compatibility_mode", mode.toString());
        cluster.get(node).startup();
    }

    private static void assertPolicyTriggersAt2038(ICoordinator coordinator, String query)
    {
        Assertions.assertThatThrownBy(() -> coordinator.execute(query, ALL))
                  .hasMessageContaining("exceeds maximum supported expiration date")
                  .hasMessageContaining("2038");
    }
}
