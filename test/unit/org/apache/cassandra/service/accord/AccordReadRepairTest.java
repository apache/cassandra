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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters.Filter;
import org.apache.cassandra.distributed.test.accord.AccordTestBase;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;

public class AccordReadRepairTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(org.apache.cassandra.distributed.test.accord.AccordCQLTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder, 2);
        SHARED_CLUSTER.schemaChange("CREATE TYPE " + KEYSPACE + ".person (height int, age int)");
    }

    /*
     * SERIAL read and CAS create Accord transactions which will then invoke Cassandra coordination to perform the read
     * and proxy any read repairs that are generated.
     */
    @Test
    public void testSerialReadRepair() throws Exception
    {
        testReadRepair(cluster -> cluster.coordinator(1).execute("SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1;", ConsistencyLevel.SERIAL),
                       new Object[][] {{1, 1, 1, 1}});
    }

    @Test
    public void testCASFailedConditionReadRepair() throws Exception
    {
        // Even if the condition fails to apply the data checked when applying the condition should be repaired
        testReadRepair(cluster -> cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v1) VALUES (1, 1, 99) IF NOT EXISTS;", ConsistencyLevel.SERIAL),
                       new Object[][] {{false, 1, 1, 1, 1}});
    }

    @Test
    public void testCASReadRepair() throws Exception
    {
        // If the condition applies the read repair should preserve the existing timestamp
        testReadRepair(cluster -> cluster.coordinator(1).execute("UPDATE  " + qualifiedTableName + " SET v2 = 99 WHERE k = 1 and c = 1 IF EXISTS;", ConsistencyLevel.SERIAL),
                       new Object[][] {{Boolean.TRUE}});
    }

    /*
     * non-SERIAL consistency levels are coordinated by C* and then if a partition needs to be repaired an Accord transaction
     * is created for each partition repair to proxy the repair mutations safely.
     */
    @Test
    public void testNonSerialReadRepair() throws Exception
    {
        for (ConsistencyLevel cl : ImmutableList.of(ConsistencyLevel.QUORUM))
            testReadRepair(cluster -> cluster.coordinator(1).execute("SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1;", cl),
                           new Object[][] {{1, 1, 1, 1}});
    }

    void testReadRepair(Function<Cluster, Object[][]> accordTxn, Object[][] expected) throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, v1 int, v2 int, PRIMARY KEY ((k), c)) WITH transactional_mode='unsafe_writes';",
             cluster -> {
                 Filter mutationFilter = cluster.filters().verbs(Verb.MUTATION_REQ.id).to(2).drop().on();
                 cluster.filters().verbs(Verb.HINT_REQ.id, Verb.HINT_RSP.id).drop().on();
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v1, v2) VALUES (1, 1, 1, 1) USING TIMESTAMP 42;", ConsistencyLevel.ONE);
                 mutationFilter.off();
                 Filter blockNodeOneReads = cluster.filters().verbs(Verb.READ_REQ.id).to(1).drop().on();
                 assertThat(cluster.coordinator(2).executeWithResult("SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1;", ConsistencyLevel.ONE))
                 .isEmpty();
                 blockNodeOneReads.off();
                 // Should perform read repair
                 Object[][] result = accordTxn.apply(cluster);
                 assertRows(result, expected);
                 blockNodeOneReads.on();
                 // Side effect of the read repair should be visible now
                 assertThat(cluster.coordinator(2).executeWithResult("SELECT k, c, v1, WRITETIME(v1) FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1;", ConsistencyLevel.ONE))
                 .isEqualTo(1, 1, 1, 42L);
             });
    }
}
