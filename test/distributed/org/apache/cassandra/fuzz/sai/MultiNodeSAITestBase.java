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
import org.apache.cassandra.harry.SchemaSpec;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public abstract class MultiNodeSAITestBase extends SingleNodeSAITestBase
{
    public MultiNodeSAITestBase(boolean withAccord)
    {
        super(withAccord);
    }

    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = Cluster.build()
                         .withNodes(2)
                         // At lower fetch sizes, queries w/ hundreds or thousands of matches can take a very long time.
                         .withConfig(defaultConfig().andThen(c -> c.set("range_request_timeout", "180s")
                                                                   .set("read_request_timeout", "180s")
                                                                   .set("transaction_timeout", "180s")
                                                                   .set("write_request_timeout", "180s")
                                                                   .set("native_transport_timeout", "180s")
                                                                   .set("slow_query_log_timeout", "180s")
                                                                   .with(GOSSIP).with(NETWORK)))
                         .createWithoutStarting();
        cluster.startup();
        cluster = init(cluster);
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
        cluster.forEach(i -> i.nodetool("flush", schema.keyspace));
    }

    @Override
    protected void repair(SchemaSpec schema)
    {
        cluster.get(1).nodetool("repair", schema.keyspace);
    }

    @Override
    protected void compact(SchemaSpec schema)
    {
        cluster.forEach(i -> i.nodetool("compact", schema.keyspace));
    }

    @Override
    protected void waitForIndexesQueryable(SchemaSpec schema)
    {
        SAIUtil.waitForIndexQueryable(cluster, schema.keyspace);
    }
}