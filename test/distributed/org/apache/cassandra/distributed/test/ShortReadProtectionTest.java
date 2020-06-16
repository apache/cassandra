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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

/**
 * Tests short read protection, the mechanism that ensures distributed queries at read consistency levels > ONE/LOCAL_ONE
 * avoid short reads that might happen when a limit is used and reconciliation accepts less rows than such limit.
 */
public class ShortReadProtectionTest extends TestBaseImpl
{
    /**
     * Test GROUP BY with short read protection, particularly when there is a limit and regular row deletions.
     * <p>
     * See CASSANDRA-15459
     */
    @Test
    public void testGroupBySRPRegularRow() throws Throwable
    {
        testGroupBySRP("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))",
                       asList("INSERT INTO %s (pk, ck) VALUES (1, 1) USING TIMESTAMP 0",
                              "DELETE FROM %s WHERE pk=0 AND ck=0",
                              "INSERT INTO %s (pk, ck) VALUES (2, 2) USING TIMESTAMP 0"),
                       asList("DELETE FROM %s WHERE pk=1 AND ck=1",
                              "INSERT INTO %s (pk, ck) VALUES (0, 0) USING TIMESTAMP 0",
                              "DELETE FROM %s WHERE pk=2 AND ck=2"),
                       asList("SELECT * FROM %s LIMIT 1",
                              "SELECT * FROM %s LIMIT 10",
                              "SELECT * FROM %s GROUP BY pk LIMIT 1",
                              "SELECT * FROM %s GROUP BY pk LIMIT 10",
                              "SELECT * FROM %s GROUP BY pk, ck LIMIT 1",
                              "SELECT * FROM %s GROUP BY pk, ck LIMIT 10"));
    }

    /**
     * Test GROUP BY with short read protection, particularly when there is a limit and static row deletions.
     * <p>
     * See CASSANDRA-15459
     */
    @Test
    public void testGroupBySRPStaticRow() throws Throwable
    {
        testGroupBySRP("CREATE TABLE %s (pk int, ck int, s int static, PRIMARY KEY (pk, ck))",
                       asList("INSERT INTO %s (pk, s) VALUES (1, 1) USING TIMESTAMP 0",
                              "INSERT INTO %s (pk, s) VALUES (0, null)",
                              "INSERT INTO %s (pk, s) VALUES (2, 2) USING TIMESTAMP 0"),
                       asList("INSERT INTO %s (pk, s) VALUES (1, null)",
                              "INSERT INTO %s (pk, s) VALUES (0, 0) USING TIMESTAMP 0",
                              "INSERT INTO %s (pk, s) VALUES (2, null)"),
                       asList("SELECT * FROM %s LIMIT 1",
                              "SELECT * FROM %s LIMIT 10",
                              "SELECT * FROM %s GROUP BY pk LIMIT 1",
                              "SELECT * FROM %s GROUP BY pk LIMIT 10",
                              "SELECT * FROM %s GROUP BY pk, ck LIMIT 1",
                              "SELECT * FROM %s GROUP BY pk, ck LIMIT 10"));
    }

    private void testGroupBySRP(String createTable,
                                List<String> node1Queries,
                                List<String> node2Queries,
                                List<String> coordinatorQueries) throws Throwable
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(2)
                                           .withConfig(config -> config.set("hinted_handoff_enabled", false))
                                           .withInstanceInitializer(BBDropMutationsHelper::install)
                                           .start()))
        {
            String table = withKeyspace("%s.t");
            cluster.schemaChange(format(createTable, table));

            // populate data on node1
            IInvokableInstance node1 = cluster.get(1);
            for (String query : node1Queries)
                node1.executeInternal(format(query, table));

            // populate data on node2
            IInvokableInstance node2 = cluster.get(2);
            for (String query : node2Queries)
                node2.executeInternal(format(query, table));

            // ignore read repair writes
            node1.runOnInstance(BBDropMutationsHelper::enable);
            node2.runOnInstance(BBDropMutationsHelper::enable);

            // verify the behaviour of SRP with GROUP BY queries
            ICoordinator coordinator = cluster.coordinator(1);
            for (String query : coordinatorQueries)
                assertRows(coordinator.execute(format(query, table), ALL));
        }
    }

    /**
     * Byte Buddy helper to silently drop mutations.
     */
    public static class BBDropMutationsHelper
    {
        private static final AtomicBoolean enabled = new AtomicBoolean(false);

        static void enable()
        {
            enabled.set(true);
        }

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(Mutation.class)
                           .method(named("apply"))
                           .intercept(MethodDelegation.to(BBDropMutationsHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void execute(@SuperCall Callable<ResultMessage.Rows> r) throws Exception
        {
            if (enabled.get())
                return;
            r.call();
        }
    }
}
