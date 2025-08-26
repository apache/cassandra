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

package org.apache.cassandra.distributed.test.sai;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Version;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Distributed tests for {@link org.apache.cassandra.db.filter.IndexHints}.
 */
public class IndexHintsDistributedTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;

    @Test
    public void testIndexHintsWithCurrentVersion() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).set("storage_compatibility_mode", "NONE"))
                                           .start(), RF))
        {
            // null indicates that the query should succeed
            testSelectWithIndexHints(cluster, null);
        }
    }

    @Test
    public void testIndexHintsWithAllOldVersion() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).set("storage_compatibility_mode", "CASSANDRA_4"))
                                           .start(), RF))
        {
            testSelectWithIndexHints(cluster, "Index hints are not supported in clusters below 14.");
        }
    }

    @Test
    public void testIndexHintsWithMixedVersions() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(BB::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL).set("storage_compatibility_mode", "NONE"))
                                           .start(), RF))
        {
            testSelectWithIndexHints(cluster, "Index hints are not supported in clusters below 14.");
        }
    }

    private static void testSelectWithIndexHints(Cluster cluster, String expectedErrorMessage) throws Throwable
    {
        // create a schema with various indexes in the same column, so we can provide hints to select between them
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text)"));
        cluster.schemaChange(withKeyspace("CREATE INDEX legacy_idx ON %s.t(v) USING 'legacy_local_table'"));
        cluster.schemaChange(withKeyspace("CREATE INDEX non_analyzed_sai_idx ON %s.t(v) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        // insert some data
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (1, 'apple')"), ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (2, 'orange')"), ConsistencyLevel.ALL);

        // prepare a template query that will behave differently depending on index hints
        String select = withKeyspace("SELECT * FROM %s.t WHERE v = 'apple'");
        Object[][] eqRows = new Object[][]{ row(1, "apple") };

        beforeAndAfterFlush(cluster, KEYSPACE, () -> {
            // test included indexes
            assertSelect(cluster, expectedErrorMessage, select + " WITH included_indexes = {legacy_idx}", eqRows);
            assertSelect(cluster, expectedErrorMessage, select + " WITH included_indexes = {non_analyzed_sai_idx}", eqRows);

            // test excluded indexes
            assertSelect(cluster, expectedErrorMessage, select + " WITH excluded_indexes = {legacy_idx}", eqRows);
            assertSelect(cluster, expectedErrorMessage, select + " WITH excluded_indexes = {non_analyzed_sai_idx}", eqRows);
        });
    }

    private static void assertSelect(Cluster cluster, String expectedErrorMessage, String select, Object[]... expectedRows)
    {
        for (int i = 1; i <= cluster.size(); i++)
        {
            ICoordinator coordinator = cluster.coordinator(i);
            if (expectedErrorMessage == null)
                assertRows(coordinator.execute(select, ConsistencyLevel.ONE), expectedRows);
            else
                Assertions.assertThatThrownBy(() -> coordinator.execute(select, ConsistencyLevel.ONE))
                          .hasMessageContaining(expectedErrorMessage);
        }
    }

    /**
     * Injection to set the current version of the first cluster node to VERSION_40.
     */
    public static class BB
    {
        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, int node)
        {
            if (node == 1)
            {
                new ByteBuddy().rebase(MessagingService.class)
                               .method(named("currentVersion"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Version currentVersion()
        {
            return Version.VERSION_40;
        }
    }
}
