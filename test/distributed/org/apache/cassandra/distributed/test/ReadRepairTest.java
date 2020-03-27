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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;

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
            cluster.filters().verbs(READ_REQ.ordinal()).from(4).to(3).drop();
            cluster.filters().verbs(READ_REPAIR_REQ.ordinal()).from(4).to(3).drop();
            assertRows(cluster.coordinator(4).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                      ConsistencyLevel.ALL, i),
                       row(i, 1, 1));

            // verify that #1 receives the write
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", i),
                       row(i, 1, 1));
        }
    }

}
