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

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_ONE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests TTL the overflow policy triggers at the correct limit
 * <p>
 * sstable version < BIG:oa, overflow policy triggers at year 2038. That could be <=4.1 or 5.0 with 4.x storage compatibility
 * sstable version >= BIG:oa or BTI - overflow policy triggers at year 2106. That is >=5.0 using no storage compatibility
 *
 * @see StorageCompatibilityMode
 *
 * This test has been split by subclassing in order to avoid OOMs.
 */
public abstract class MixedModeTTLOverflowUpgradeTestBase extends UpgradeTestBase
{
    static final int SMALL_TTL = 3600;

    static final String T_REGULAR = "table_regular";
    static final String T_CLUST = "table_clust";
    static final String T_STATIC = "table_static";
    static final String T_COMPLEX = "table_complex";
    static final String T_FROZEN = "table_frozen";
    static final String T_INDEX = "table_indexed";
    static final String INDEX = "idx";
    static final String TYPE = "complex_type";

    static final int NODE_1_MAX_TTL_KEY_OFFSET = 1000;
    static final int NODE_2_MAX_TTL_KEY_OFFSET = 2000;
    static final int NODE_1_MIXED_TTL_KEY_OFFSET = 3000;
    static final int NODE_2_MIXED_TTL_KEY_OFFSET = 4000;

    enum Step
    {
        NODE1_PREV_NODE2_PREV,
        NODE1_40_NODE2_PREV,
        NODE1_UPGRADING_NODE2_PREV,
        NODE1_40_NODE2_40,
        NODE1_UPGRADING_NODE2_40,
        NODE1_UPGRADING_NODE2_UPGRADING,
        NODE1_NONE_NODE2_UPGRADING,
        NODE1_NONE_NODE2_NONE,
    }

    static volatile long clusterStatupTime = 0;

    static void testTTLOverflow(RunOnClusterAndNode runAfterNodeUpgrade) throws Throwable
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
                    verify(Step.NODE1_PREV_NODE2_PREV, cluster, true);
                })
                .runAfterNodeUpgrade(runAfterNodeUpgrade)
                .run();
    }

    /**
     * Verifies that the TTL overflow policy triggers at the correct limit for a variety types
     * @param step the step in the upgrade process (manily use a unique primary key for each verification)
     * @param cluster the cluster
     * @param expectPolicyTriggerAt2038 when true, we expect the overflow policy to trigger at 2038 and attempts to set
     *                                  a TTL which would result in expiration date after 2038 to fail. Otherwise, the
     *                                  allowed expiration date is 2106, and we cannot test that for now because of
     *                                  {@link Attributes#MAX_TTL} limit of 20 years.
     */
    static void verify(Step step, UpgradeableCluster cluster, boolean expectPolicyTriggerAt2038)
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

    static void restartNodeWithCompatibilityMode(UpgradeableCluster cluster, int node, StorageCompatibilityMode mode) throws Throwable
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
