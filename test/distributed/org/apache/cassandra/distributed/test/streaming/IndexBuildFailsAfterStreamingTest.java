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

package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.SecondaryIndexManager;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexBuildFailsAfterStreamingTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(BBHelper::install)
                                           .withConfig(c -> c.with(Feature.values())
                                                             .set("stream_entire_sstables", false)
                                                             .set("disk_failure_policy", "die"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (p int, c int, v int, PRIMARY KEY(p, c))"));
            cluster.schemaChange(withKeyspace("CREATE INDEX idx ON %s.tbl(v)"));

            for (int i = 0; i < 100; i++)
                cluster.get(1).executeInternal(withKeyspace("insert into %s.tbl (p, c, v) values (?, ?, ?)"), i, i, i);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).runOnInstance(() -> BBHelper.enabled.set(true));

            // pre-existing weird behaivour - nodetool repair fails, but the sstables are actually streamed & live on node2:
            cluster.get(2).runOnInstance(() -> assertTrue(Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().isEmpty()));
            cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().failure();
            cluster.get(2).runOnInstance(() -> assertFalse(Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().isEmpty()));
            for (int i = 0; i < 100; i++)
                assertRows(cluster.get(2).executeInternal(withKeyspace("select * from %s.tbl where p = ? and c = ?"), i, i), row(i, i, i));

            assertRows(cluster.get(1).executeInternal("select * from system.\"IndexInfo\" where table_name=? and index_name=?", KEYSPACE, "idx"), row(KEYSPACE, "idx", null));
            // index not built:
            assertEquals(0, cluster.get(2).executeInternal("select * from system.\"IndexInfo\" where table_name=? and index_name=?", KEYSPACE, "idx").length);
        }
    }

    public static class BBHelper
    {
        public static void install(ClassLoader classLoader, int num)
        {
            if (num == 2)
            {
                new ByteBuddy().rebase(SecondaryIndexManager.class)
                               .method(named("calculateIndexingPageSize").and(takesNoArguments()))
                               .intercept(to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }
        public static AtomicBoolean enabled = new AtomicBoolean();
        public static int calculateIndexingPageSize(@SuperCall Callable<Integer> zuper) throws Exception
        {
            if (enabled.get())
                throw new RuntimeException("On purpose fail 2i build");
            return zuper.call();
        }
    }
}
