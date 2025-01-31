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
import javax.annotation.Nullable;

import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class MultiNodeTableWalkTest extends SingleNodeTableWalkTest
{
    /**
     * This field lets the test run as if it was multiple nodes, but actually runs against a single node.
     * This behavior is desirable when this test fails to see if the issue can be reproduced on single node as well.
     */
    private boolean mockMultiNode = true;

    public MultiNodeTableWalkTest()
    {
    }

    protected MultiNodeTableWalkTest(@Nullable TransactionalMode transactionalMode)
    {
        super(transactionalMode);
    }

    @Override
    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
        // Sometimes It's useful to validate that the error is localized to mutliple nodes rather than single node,
        // so uncomment the below to allow running the test as a single node
        // mockMultiNode = true;
    }

    @Override
    protected Cluster createCluster() throws IOException
    {
        return createCluster(mockMultiNode ? 1 : 3, c -> {
            c.set("range_request_timeout", "180s")
             .set("read_request_timeout", "180s")
             .set("transaction_timeout", "180s")
             .set("write_request_timeout", "180s")
             .set("native_transport_timeout", "180s")
             .set("slow_query_log_timeout", "180s");
        });
    }

    @Override
    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new MultiNodeState(rs, cluster);
    }

    public class MultiNodeState extends State
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
}
