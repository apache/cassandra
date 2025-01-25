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

import org.junit.Ignore;

import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.consensus.TransactionalMode;

@Ignore
public class MultiNodeTableWalkTest extends SingleNodeTableWalkTest
{

    public MultiNodeTableWalkTest()
    {
    }

    protected MultiNodeTableWalkTest(@Nullable TransactionalMode transactionalMode)
    {
        super(transactionalMode);
    }

    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
//        builder.withSeed(159634037219554562L); // see CASSANDRA-20243
        builder.withSeed(7034963806046484005L);
    }

    @Override
    protected Cluster createCluster() throws IOException
    {
        return createCluster(3, c -> {
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

    private class MultiNodeState extends State
    {
        public MultiNodeState(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster);
        }

        @Override
        public boolean allowNonPartitionQuery()
        {
            if (IGNORED_ISSUES.contains(KnownIssue.AF_MULTI_NODE_AND_NODE_LOCAL_WRITES))
            {
                return !indexes.isEmpty() && super.allowNonPartitionQuery();
            }
            return super.allowNonPartitionQuery();
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
