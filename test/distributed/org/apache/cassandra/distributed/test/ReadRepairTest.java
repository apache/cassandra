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

package org.apache.cassandra.distributed.test;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.net.Verb.READ_REPAIR_REQ;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;

public class ReadRepairTest extends TestBaseImpl
{

    @Test
    public void readRepairTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                      ConsistencyLevel.ALL),
                       row(1, 1, 1));

            // Verify that data got repaired to the third node
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }
    }

    @Test
    public void failingReadRepairTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            for (int i = 1 ; i <= 2 ; ++i)
                cluster.get(i).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            cluster.filters().verbs(READ_REPAIR_REQ.id).to(3).drop();
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                      ConsistencyLevel.QUORUM),
                       row(1, 1, 1));

            // Data was not repaired
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));
        }
    }

    @Test
    public void movingTokenReadRepairTest() throws Throwable
    {
        try (Cluster cluster = (Cluster) init(Cluster.create(4), 3))
        {
            List<Token> tokens = cluster.tokens();

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            int i = 0;
            while (true)
            {
                Token t = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(i));
                if (t.compareTo(tokens.get(2 - 1)) < 0 && t.compareTo(tokens.get(1 - 1)) > 0)
                    break;
                ++i;
            }

            // write only to #4
            cluster.get(4).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1)", i);
            // mark #2 as leaving in #4
            cluster.get(4).acceptsOnInstance((InetSocketAddress endpoint) -> {
                StorageService.instance.getTokenMetadata().addLeavingEndpoint(InetAddressAndPort.getByAddressOverrideDefaults(endpoint.getAddress(), endpoint.getPort()));
                PendingRangeCalculatorService.instance.update();
                PendingRangeCalculatorService.instance.blockUntilFinished();
            }).accept(cluster.get(2).broadcastAddress());

            // prevent #4 from reading or writing to #3, so our QUORUM must contain #2 and #4
            // since #1 is taking over the range, this means any read-repair must make it to #1 as well
            // (as a speculative repair in this case, as we prefer to send repair mutations to the initial
            // set of read replicas, which are 2 and 3 here).
            cluster.filters().verbs(READ_REQ.id).from(4).to(3).drop();
            cluster.filters().verbs(READ_REPAIR_REQ.id).from(4).to(3).drop();
            assertRows(cluster.coordinator(4).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                      ConsistencyLevel.QUORUM, i),
                       row(i, 1, 1));

            // verify that #1 receives the write
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", i),
                       row(i, 1, 1));
        }
    }

    @Test
    public void emptyRangeTombstones1() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");
            cluster.get(1).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
            cluster.coordinator(2).execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 > ? and column1 <= ?",
                                           ConsistencyLevel.ALL,
                                           "test", 10, 10);
            cluster.coordinator(2).execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 > ? and column1 <= ?",
                                           ConsistencyLevel.ALL,
                                           "test", 11, 11);
            cluster.get(2).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
    }

    @Test
    public void emptyRangeTombstonesFromPaging() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");

            cluster.get(1).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 10 WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);

            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO distributed_test_keyspace.tbl (key, column1) VALUES (?, ?) USING TIMESTAMP 30", ConsistencyLevel.ALL, "test", i);

            consume(cluster.coordinator(2).executeWithPaging("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 >= ? and column1 <= ?",
                                           ConsistencyLevel.ALL, 1,
                                           "test", 8, 12));

            consume(cluster.coordinator(2).executeWithPaging("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 >= ? and column1 <= ?",
                                                             ConsistencyLevel.ALL, 1,
                                                             "test", 16, 20));
            cluster.get(2).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
    }

    @Test
    public void readRepairRTRangeMovementTest() throws Throwable
    {
        ExecutorService es = Executors.newFixedThreadPool(1);
        String key = "test1";
        try (Cluster cluster = init(Cluster.build()
                                           .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK)
                                                                       .set("read_request_timeout_in_ms", Integer.MAX_VALUE))
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                           .withNodes(3)
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");

            cluster.forEach(i -> i.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").disableAutoCompaction()));

            for (int i = 1; i <= 2; i++)
            {
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 50 WHERE key=?;", key);
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 80 WHERE key=? and column1 >= ? and column1 < ?;", key, 10, 100);
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 70 WHERE key=? and column1 = ?;", key, 30);
                cluster.get(i).flush(KEYSPACE);
            }
            cluster.get(3).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 100 WHERE key=?;", key);
            cluster.get(3).flush(KEYSPACE);

            // pause the read until we have bootstrapped a new node below
            SimpleCondition continueRead = new SimpleCondition();
            SimpleCondition readStarted = new SimpleCondition();
            cluster.filters().outbound().from(3).to(1,2).verbs(Verb.READ_REQ.id).messagesMatching((i, i1, iMessage) -> {
                try
                {
                    readStarted.signalAll();
                    continueRead.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                return false;
            }).drop();
            Future<Object[][]> read = es.submit(() -> cluster.coordinator(3)
                                                          .execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key=? and column1 >= ? and column1 <= ?",
                                                                   ConsistencyLevel.ALL, key, 20, 40));
            readStarted.await();
            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            cluster.bootstrap(config).startup();
            continueRead.signalAll();
            read.get();
        }
        finally
        {
            es.shutdown();
        }
    }


    private void consume(Iterator<Object[]> it)
    {
        while (it.hasNext())
            it.next();
    }
}
