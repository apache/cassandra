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

package org.apache.cassandra.metrics;

import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;


public class StorageProxyMetricsManagerTest
{
    @Test
    public void testMultipleConsistencyLevelsSameKeyspace()
    {
        String keyspace = "test_keyspace";

        StorageProxyMetrics series1 = StorageProxyMetricsManager.getMetrics(keyspace, ConsistencyLevel.ONE);
        StorageProxyMetrics series2 = StorageProxyMetricsManager.getMetrics(keyspace, ConsistencyLevel.ANY);

        // each consistency level whihin a keyspace should map to a separate series
        assertNotEquals(series1, series2);
    }

    @Test
    public void testMultipleKeyspacesSameConsistencyLevel()
    {
        ConsistencyLevel cl = ConsistencyLevel.ONE;

        StorageProxyMetrics series1 = StorageProxyMetricsManager.getMetrics("test_keyspace1", cl);
        StorageProxyMetrics series2 = StorageProxyMetricsManager.getMetrics("test_keyspace2", cl);

        // each keyspace should have separate series even when the CL is the same
        assertNotEquals(series1, series2);
    }
}
