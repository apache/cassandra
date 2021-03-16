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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

public class SAIUtil
{
    public static void waitForIndexQueryable(Cluster cluster, String keyspace)
    {
        String query = String.format("SELECT is_queryable FROM system_views.indexes WHERE keyspace_name = '%s' ALLOW FILTERING", keyspace);
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> cluster.stream()
                                   .map(node -> node.executeInternalWithResult(query))
                                   .filter(SimpleQueryResult::hasNext)
                                   .map(result -> result.next().get("is_queryable"))
                                   .allMatch(flag -> (Boolean) flag));
    }
    
    public static void waitForIndexQueryable(IInvokableInstance node, String index)
    {
        await().atMost(5, TimeUnit.SECONDS)
               .untilAsserted(() -> assertTrue(getIndexState(node, index, "is_queryable")));
    }

    // This will pull one of the boolean index states from a node
    private static boolean getIndexState(IInvokableInstance node, String index, String state)
    {
        String query = String.format("SELECT %s FROM system_views.indexes WHERE index_name = '%s' ALLOW FILTERING", state, index);
        SimpleQueryResult queryResult = node.executeInternalWithResult(query);
        assertTrue("No rows returned", queryResult.hasNext());
        return queryResult.next().get(state);
    }
}
