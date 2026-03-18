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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;

public class AccordMigrationDefaultPrimaryRangeTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMigrationDefaultPrimaryRangeTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder
                                               .withoutVNodes()
                                               .withConfig(config ->
                                                           config
                                                           .set("paxos_variant", "v2")
                                                           .with(Feature.NETWORK, Feature.GOSSIP)), 6);
    }

    @Test
    public void accordSinglePartitionKeyBatchTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}",
                                          "CREATE TABLE " + qualifiedRegularTableName + " (k int PRIMARY KEY, v int)");
        test(ddls, cluster -> {
            NodeToolResult result = cluster.get(1).nodetoolResult("consensus_admin", "begin-migration", KEYSPACE, regularTableName);
            System.out.println(result.toString());
        });
    }
}