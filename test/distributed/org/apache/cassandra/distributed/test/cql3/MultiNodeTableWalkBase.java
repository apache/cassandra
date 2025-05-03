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

package org.apache.cassandra.distributed.test.cql3;

import java.io.IOException;

import accord.utils.RandomSource;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.TimeUUID;

import static net.bytebuddy.matcher.ElementMatchers.named;

public abstract class MultiNodeTableWalkBase extends SingleNodeTableWalkTest
{
    /**
     * This field lets the test run as if it was multiple nodes, but actually runs against a single node.
     * This behavior is desirable when this test fails to see if the issue can be reproduced on single node as well.
     */
    private static final boolean mockMultiNode = false;

    private final ReadRepairStrategy readRepair;

    protected MultiNodeTableWalkBase(ReadRepairStrategy readRepair)
    {
        this.readRepair = readRepair;
    }

    @Override
    protected TableMetadata defineTable(RandomSource rs, String ks)
    {
        TableMetadata tbl = super.defineTable(rs, ks);
        return tbl.unbuild().params(tbl.params.unbuild().readRepair(readRepair).build()).build();
    }

    @Override
    protected Cluster createCluster() throws IOException
    {
        return createCluster(mockMultiNode ? 1 : 3);
    }

    @Override
    protected void clusterConfig(IInstanceConfig c)
    {
        super.clusterConfig(c);
        c.set("range_request_timeout", "180s")
         .set("read_request_timeout", "180s")
         .set("write_request_timeout", "180s")
         .set("native_transport_timeout", "180s")
         .set("slow_query_log_timeout", "180s");
    }

    @Override
    protected void clusterInitializer(ClassLoader cl, int node)
    {
        BBHelper.install(cl, node);
    }

    @Override
    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new MultiNodeState(rs, cluster);
    }

    protected class MultiNodeState extends State
    {
        public MultiNodeState(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster);
        }

        @Override
        protected boolean isMultiNode()
        {
            // When a seed fails its useful to rerun the test as a single node to see if the issue persists... but doing so corrupts the random history!
            // To avoid that, this method hard codes that the test is multi node...
            return true;
        }

        @Override
        protected boolean allowRepair()
        {
            return hasEnoughMemtableForRepair() || hasEnoughSSTablesForRepair();
        }

        @Override
        protected IInvokableInstance selectInstance(RandomSource rs)
        {
            if (mockMultiNode)
            {
                rs.nextInt(0, 3); // needed to avoid breaking random history
                return cluster.get(1);
            }
            return super.selectInstance(rs);
        }

        @Override
        protected ConsistencyLevel selectCl()
        {
            return ConsistencyLevel.ALL;
        }

        @Override
        protected ConsistencyLevel mutationCl()
        {
            return ConsistencyLevel.NODE_LOCAL;
        }
    }

    /**
     * This is not a deterministic clock for TimeUUID, but it's a monotonic clock, which means that any instance that gets
     * a TimeUUID from this clock has the propery that its happens-after all other ones cross all instances.
     *
     * This class came around because TimeUUID.Generator.nextUnixMicros works with milliseconds, and when time doesn't
     * move forward (goes back or test is "too fast") then it becomes an instance local bump-counter; this counter allows
     * a logically later timeuuid to happens-before a logically earlier one!
     */
    @Shared
    public static class GlobalClock
    {
        private static long lastMicros = 0;
        public synchronized static long nextUnixMicros()
        {
            return ++lastMicros;
        }

        public synchronized static void reset()
        {
            // this method isn't actually needed for the property of this class, but it does help isolate any non-deterministic issues
            lastMicros = 0;
        }
    }

    public static class BBHelper
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(TimeUUID.Generator.class)
                           .method(named("nextUnixMicros"))
                           .intercept(MethodDelegation.to(GlobalClock.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            GlobalClock.reset();
        }
    }
}
