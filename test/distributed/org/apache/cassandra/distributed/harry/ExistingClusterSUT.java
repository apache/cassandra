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

package org.apache.cassandra.distributed.harry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import harry.core.Configuration;
import harry.model.sut.SystemUnderTest;
import org.apache.cassandra.distributed.api.ICluster;

public class ExistingClusterSUT implements Configuration.SutConfiguration
{
    private final ICluster cluster;
    private final ClusterState clusterState;

    public ExistingClusterSUT(ICluster cluster, ClusterState clusterState)
    {
        this.cluster = cluster;
        this.clusterState = clusterState;
    }

    @Override
    public SystemUnderTest make()
    {
        return new SystemUnderTest()
        {
            int toQuery = 0;
            @Override
            public boolean isShutdown()
            {
                return false;
            }

            @Override
            public void shutdown()
            {
            }

            @Override
            public void schemaChange(String schemaChange)
            {
                cluster.schemaChange(schemaChange);
            }

            @Override
            public Object[][] execute(String s, SystemUnderTest.ConsistencyLevel consistencyLevel, Object... objects)
            {
                RuntimeException lastEx = null;
                for (int i = 0; i < 20; i++)
                {
                    toQuery++;
                    int coordinator = (toQuery % cluster.size()) + 1;
                    if (clusterState.isDown(coordinator))
                        continue;
                    try
                    {
                        return cluster.coordinator(coordinator).execute(s, org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, objects);
                    }
                    catch (RuntimeException e)
                    {
                        lastEx = e;
                        Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
                    }
                }
                throw lastEx;
            }

            @Override
            public CompletableFuture<Object[][]> executeAsync(String s, SystemUnderTest.ConsistencyLevel consistencyLevel, Object... objects)
            {
                return CompletableFuture.supplyAsync(() -> this.execute(s, consistencyLevel, objects));
            }
        };
    }
}