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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

public abstract class DropCompactStorageTester extends UpgradeTestBase
{
    protected void runQueries(ICoordinator coordinator, ResultsRecorder helper, String[] queries)
    {
        for (String query : queries)
            helper.addResult(query, coordinator.execute(query, ConsistencyLevel.ALL));
    }

    public static class ResultsRecorder
    {
        final private Map<String, Object[][]> preUpgradeResults = new HashMap<>();

        public void addResult(String query, Object[][] results)
        {
            preUpgradeResults.put(query, results);
        }

        public Map<String, Object[][]> queriesAndResults()
        {
            return preUpgradeResults;
        }

        public void validateResults(UpgradeableCluster cluster, int node)
        {
            validateResults(cluster, node, ConsistencyLevel.ALL);
        }

        public void validateResults(UpgradeableCluster cluster, int node, ConsistencyLevel cl)
        {
            for (Map.Entry<String, Object[][]> entry : queriesAndResults().entrySet())
            {
                Object[][] postUpgradeResult = cluster.coordinator(node).execute(entry.getKey(), cl);
                assertRows(postUpgradeResult, entry.getValue());
            }
        }
    }
}
