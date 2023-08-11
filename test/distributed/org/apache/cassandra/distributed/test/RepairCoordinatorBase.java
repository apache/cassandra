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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairParallelism;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;

import static org.apache.cassandra.config.CassandraRelevantProperties.NODETOOL_JMX_NOTIFICATION_POLL_INTERVAL_SECONDS;

public class RepairCoordinatorBase extends TestBaseImpl
{
    protected static Cluster CLUSTER;

    protected final RepairType repairType;
    protected final RepairParallelism parallelism;
    protected final boolean withNotifications;

    public RepairCoordinatorBase(RepairType repairType, RepairParallelism parallelism, boolean withNotifications)
    {
        this.repairType = repairType;
        this.parallelism = parallelism;
        this.withNotifications = withNotifications;
    }

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> testsWithoutType()
    {
        List<Object[]> tests = new ArrayList<>();
        for (RepairParallelism p : RepairParallelism.values())
        {
            tests.add(new Object[] { p, true });
            tests.add(new Object[] { p, false });
        }
        return tests;
    }

    @BeforeClass
    public static void before()
    {
        // This only works because the way CI works
        // In CI a new JVM is spun up for each test file, so this doesn't have to worry about another test file
        // getting this set first
        NODETOOL_JMX_NOTIFICATION_POLL_INTERVAL_SECONDS.setLong(1);
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build(2)
                              .withConfig(c -> c.with(Feature.NETWORK)
                                                .with(Feature.GOSSIP))
                              .start());

        CLUSTER.setUncaughtExceptionsFilter(throwable -> throwable instanceof RejectedExecutionException && "RepairJobTask has shut down".equals(throwable.getMessage()));
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    protected String tableName(String prefix) {
        return prefix + "_" + postfix();
    }

    protected String postfix()
    {
        return repairType.name().toLowerCase() + "_" + parallelism.name().toLowerCase() + "_" + withNotifications;
    }

    protected NodeToolResult repair(int node, String... args) {
        return DistributedRepairUtils.repair(CLUSTER, node, repairType, withNotifications, parallelism.append(args));
    }
}
