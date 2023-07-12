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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class IndexBuildingFailureTest extends TestBaseImpl
{
    @Test
    public void testIndexAvailabilityOnFailedStreamingWithoutPriorData() throws Exception
    {
        testIndexAvailabilityOnFailedStreaming(false);
    }

    @Test
    public void testIndexAvailabilityOnFailedStreamingWithPriorData() throws Exception
    {
        // TODO: This fails for the moment, but only because the streaming-actuated index build marks the index
        // as being non-queryable for (what is now) no good reason.
        testIndexAvailabilityOnFailedStreaming(true);
    }

    private static void testIndexAvailabilityOnFailedStreaming(boolean priorIndexedData) throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP))
                                           .withInstanceInitializer(ByteBuddyHelper::install)
                                           .start()))
        {
            cluster.disableAutoCompaction(KEYSPACE);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.test (pk int PRIMARY KEY, v text)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX test_v_index ON %s.test(v)"));

            IInvokableInstance first = cluster.get(1);
            IInvokableInstance second = cluster.get(2);

            if (priorIndexedData)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.test(pk, v) VALUES (?, ?)"), ALL, 0, "v0");
                first.flush(KEYSPACE);
                second.flush(KEYSPACE);
            }
            
            first.executeInternal(withKeyspace("INSERT INTO %s.test(pk, v) VALUES (?, ?)"), 1, "v1");
            first.flush(KEYSPACE);
            
            Object[][] rs = second.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(0);

            // The repair job should fail when the index build fails. This should also fail the streaming transaction.
            NodeToolResult result = second.nodetoolResult("repair", KEYSPACE);
            result.asserts().failure();

            // By the way, the test won't see the compaction interruption from this as an uncought exception, because
            // CompactionExecutor catches it in afterExecute().

            // The SSTable should not be added to the table view, as the streaming transaction failed...
            rs = second.executeInternal(withKeyspace("select pk from %s.test where pk = ?"), 1);
            assertThat(rs.length).isEqualTo(0);
            
            // ...and querying the index also returns nothing, as the index for the streamed SSTable was never built.
            rs = second.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(0);
            
            // On restart, ensure that the index remains querable and does not include the data we attempted to stream. 
            second.shutdown().get();
            second.startup();

            // On restart, the base table should be unchanged...
            rs = second.executeInternal(withKeyspace("select pk from %s.test where pk = ?"), 1);
            assertThat(rs.length).isEqualTo(0);

            // ...and the index should remain queryable, because from its perspective, the streaming never happened.
            Boolean isQueryable = second.callOnInstance(() -> {
                SecondaryIndexManager sim = Keyspace.open(KEYSPACE).getColumnFamilyStore("test").indexManager;
                Index index = sim.getIndexByName("test_v_index");
                return sim.isIndexQueryable(index);
            });

            assertTrue("The index should be queryable on restart!", isQueryable);
            rs = second.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(0);
        }
    }

    public static class ByteBuddyHelper
    {
        static void install(ClassLoader loader, int node)
        {
            if (node == 2)
            {
                new ByteBuddy().redefine(CollatedViewIndexBuilder.class)
                               .method(named("isStopRequested"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(loader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static boolean isStopRequested()
        {
            return true;
        }
    }
}
