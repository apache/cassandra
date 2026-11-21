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
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class AccordListPrependTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordListPrependTest.class);

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
                                                           .with(Feature.NETWORK, Feature.GOSSIP)), 3);
    }

    @Test
    public void recoveredPrependMatchesOriginalRegressionTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, l list<int>) WITH transactional_mode='full'");
        test(ddls, cluster -> {

            // Ensure that node 2 & node 3 try to recover the transaction
            cluster.filters().outbound().from(1).to(2, 3)
                   .verbs(Verb.ACCORD_APPLY_REQ.id, Verb.ACCORD_APPLY_AND_WAIT_REQ.id, Verb.ACCORD_INTEROP_APPLY_REQ.id,
                          Verb.ACCORD_BEGIN_RECOVER_REQ.id, Verb.ACCORD_BEGIN_INVALIDATE_REQ.id)
                   .drop();

            int[] accordRequests = Arrays.stream(Verb.values())
                                         .filter(v -> v.name().startsWith("ACCORD_") && v.name().endsWith("_REQ"))
                                         .mapToInt(v -> v.id)
                                         .toArray();

            cluster.filters().outbound().from(2, 3).to(1).verbs(accordRequests).drop();


            cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                           "  UPDATE " + qualifiedAccordTableName + " SET l = [1] + l WHERE k = 0;\n" +
                                           "COMMIT TRANSACTION", ConsistencyLevel.ANY);


            Util.spinUntilTrue(() -> {
                Object[][] result = cluster.coordinator(2).execute("SELECT l FROM " + qualifiedAccordTableName + " WHERE k = 0", ConsistencyLevel.QUORUM);
                return Arrays.equals(row(Collections.singletonList(1)), result[0]);
            });

            cluster.filters().reset();
            cluster.get(1).nodetoolResult("repair", "--full", KEYSPACE).asserts().success();

            assertRows(cluster.coordinator(1).execute("SELECT l FROM " + qualifiedAccordTableName + " WHERE k = 0", ConsistencyLevel.QUORUM), row(Collections.singletonList(1)));
        });
    }
}
