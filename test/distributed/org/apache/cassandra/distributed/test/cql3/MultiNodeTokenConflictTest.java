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
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

public class MultiNodeTokenConflictTest extends SingleNodeTokenConflictTest
{
    protected MultiNodeTokenConflictTest(@Nullable TransactionalMode transactionalMode)
    {
        super(transactionalMode);
    }

    public MultiNodeTokenConflictTest()
    {
        super();
    }

    @Override
    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
    }

    @Override
    protected TableMetadata defineTable(RandomSource rs, String ks)
    {
        TableMetadata tbl = super.defineTable(rs, ks);
        // disable RR for now, should make RR testing its own class
        return tbl.unbuild().params(tbl.params.unbuild().readRepair(ReadRepairStrategy.NONE).build()).build();
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
    protected Cluster createCluster() throws IOException
    {
        return createCluster(3);
    }

    @Override
    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new MultiNodeState(rs, cluster);
    }

    private class MultiNodeState extends State
    {
        MultiNodeState(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster);
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
