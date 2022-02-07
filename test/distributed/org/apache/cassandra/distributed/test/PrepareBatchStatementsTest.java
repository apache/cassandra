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

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.util.Lists;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class PrepareBatchStatementsTest extends TestBaseImpl
{
    @Test
    public void testPreparedBatch() throws Exception
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(1)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .start()))
        {
            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                            .addContactPoint("127.0.0.1")
                                                                                            .build();
                 Session s = cluster.connect())
            {
                c.schemaChange(withKeyspace("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"));
                c.schemaChange(withKeyspace("CREATE TABLE ks1.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));
                c.schemaChange(withKeyspace("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"));
                c.schemaChange(withKeyspace("CREATE TABLE ks2.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));


                String batch1 = "BEGIN BATCH\n" +
                                "UPDATE ks1.tbl SET v = ? where pk = ? and ck = ?;\n" +
                                "UPDATE ks2.tbl SET v = ? where pk = ? and ck = ?;\n" +
                                "APPLY BATCH;";
                String batch2 = "BEGIN BATCH\n" +
                                "INSERT INTO ks1.tbl (pk, ck, v) VALUES (?, ?, ?);\n" +
                                "INSERT INTO tbl (pk, ck, v) VALUES (?, ?, ?);\n" +
                                "APPLY BATCH;";


                PreparedStatement prepared;

                prepared = s.prepare(batch1);
                s.execute(prepared.bind(1, 1, 1, 1, 1, 1));
                c.get(1).runOnInstance(() -> {
                    // no USE here, only a fully qualified batch - should get stored ONCE
                    List<String> stmts = StorageService.instance.getPreparedStatements().stream().map(p -> p.right).collect(Collectors.toList());
                    assertEquals(Lists.newArrayList(batch1), stmts);
                    QueryProcessor.clearPreparedStatements(false);
                });

                s.execute("use ks2");
                prepared = s.prepare(batch1);
                s.execute(prepared.bind(1, 1, 1, 1, 1, 1));
                c.get(1).runOnInstance(() -> {
                    // after USE, fully qualified - should get stored twice! Once with null keyspace (new behaviour) once with ks2 keyspace (old behaviour)
                    List<String> stmts = StorageService.instance.getPreparedStatements().stream().map(p -> p.right).collect(Collectors.toList());
                    assertEquals(Lists.newArrayList(batch1, batch1), stmts);
                    QueryProcessor.clearPreparedStatements(false);
                });

                prepared = s.prepare(batch2);
                s.execute(prepared.bind(1, 1, 1, 1, 1, 1));
                c.get(1).runOnInstance(() -> {
                    // after USE, should get stored twice, once with keyspace, once without
                    List<String> stmts = StorageService.instance.getPreparedStatements().stream().map(p -> p.right).collect(Collectors.toList());
                    assertEquals(Lists.newArrayList(batch2, batch2), stmts);
                    QueryProcessor.clearPreparedStatements(false);
                });
            }
        }
    }
}