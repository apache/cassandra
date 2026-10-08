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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.junit.Assert.assertEquals;

public class AccordEmptyBranchRecoveryTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void setUp() throws IOException
    {
        cluster = init(Cluster.build(3)
                              .withoutVNodes()
                              .withConfig(c ->
                                          c.with(Feature.GOSSIP, Feature.NETWORK)
                                           .set("accord.recover_txn", "100ms")
                                           .set("accord.command_store_shard_count", "1"))
                              .start());
        cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor': 1}");
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void emptyBranchWithRecovery()
    {
        // Node 1 owns key 1, node 2 owns key 4, we simulate a case where on recovery we are forced
        // to rebuild the txn by merging. Previously there was a bug where a condition could be dropped
        // because TxnUpdate.select did not include conditions that did not have a fragment (DELETE WHERE pk=0 AND c < 0 AND c > 0).
        // This resulted in recovery running the else branch rather than performing the no-op.

        cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c)) WITH " + TransactionalMode.full.asCqlParam());

        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl WHERE k = 1 AND c = 0);\n" +
                       "  IF row1 IS NULL THEN\n" +
                       "    DELETE FROM ks.tbl WHERE k = 1 AND c < 0 AND c > 0;\n" +
                       "  ELSE\n" +
                       "    UPDATE ks.tbl SET v = 1 WHERE k = 1 AND c = 0;\n" +
                       "    UPDATE ks.tbl SET v = 2 WHERE k = 4 AND c = 0;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        cluster.filters().outbound().from(3).verbs(Verb.ACCORD_APPLY_REQ.id).drop();
        cluster.coordinator(3).executeWithResult(query, ConsistencyLevel.SERIAL);

        String read = "BEGIN TRANSACTION\n" +
                      "  SELECT * FROM ks.tbl WHERE k = 1 AND c = 0;\n" +
                      "COMMIT TRANSACTION";

        SimpleQueryResult result = cluster.coordinator(3).executeWithResult(read, ConsistencyLevel.SERIAL);
        assertEquals(0, result.toObjectArrays().length);

        read = "BEGIN TRANSACTION\n" +
               "  SELECT * FROM ks.tbl WHERE k = 4 AND c = 0;\n" +
               "COMMIT TRANSACTION";

        result = cluster.coordinator(3).executeWithResult(read, ConsistencyLevel.SERIAL);
        assertEquals(0, result.toObjectArrays().length);
    }
}
