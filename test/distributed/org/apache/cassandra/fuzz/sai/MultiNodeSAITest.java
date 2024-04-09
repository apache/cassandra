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

package org.apache.cassandra.fuzz.sai;

import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.sut.injvm.InJvmSutBase;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class MultiNodeSAITest extends SingleNodeSAITest
{
    /**
     * Chosing a fetch size has implications for how well this test will excercise paging, short-read protection, and
     * other important parts of the distributed query apparatus. This should be set low enough to ensure a significant
     * number of queries during validation page, but not too low that more expesive queries time out and fail the test.
     */
    private static final int FETCH_SIZE = 10;

    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = Cluster.build()
                         .withNodes(2)
                         // At lower fetch sizes, queries w/ hundreds or thousands of matches can take a very long time. 
                         .withConfig(InJvmSutBase.defaultConfig().andThen(c -> c.set("range_request_timeout", "180s").set("read_request_timeout", "180s")
                                                                                .with(GOSSIP).with(NETWORK)))
                         .createWithoutStarting();
        cluster.setUncaughtExceptionsFilter(t -> {
            logger.error("Caught exception, reporting during shutdown. Ignoring.", t);
            return true;
        });
        cluster.startup();
        cluster = init(cluster);
        sut = new InJvmSut(cluster) {
            @Override
            public Object[][] execute(String cql, ConsistencyLevel cl, Object[] bindings)
            {
                // The goal here is to make replicas as out of date as possible, modulo the efforts of repair
                // and read-repair in the test itself.
                if (cql.contains("SELECT"))
                    return super.execute(cql, ConsistencyLevel.ALL, FETCH_SIZE, bindings);
                return super.execute(cql, ConsistencyLevel.NODE_LOCAL, bindings);
            }
        };
    }

    @Before
    public void beforeEach()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS harry");
        cluster.schemaChange("CREATE KEYSPACE harry WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
    }

    @Override
    protected void flush(SchemaSpec schema)
    {
        cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
        cluster.get(2).nodetool("flush", schema.keyspace, schema.table);
    }

    @Override
    protected void repair(SchemaSpec schema)
    {
        cluster.get(1).nodetool("repair", schema.keyspace);
    }

    @Override
    protected void compact(SchemaSpec schema)
    {
        cluster.get(1).nodetool("compact", schema.keyspace);
        cluster.get(2).nodetool("compact", schema.keyspace);
    }

    @Override
    protected void waitForIndexesQueryable(SchemaSpec schema)
    {
        SAIUtil.waitForIndexQueryable(cluster, schema.keyspace);
    }
}