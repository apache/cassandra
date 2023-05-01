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

import org.junit.Test;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.accord.AccordService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class AccordInteropReadTest extends TestBaseImpl
{

    private static void localWrite(String s)
    {
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.parseStatement(s).prepare(ClientState.forInternalCalls());
        stmt.executeLocally(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
    }

    private static SimpleQueryResult localRead(String s)
    {
        SelectStatement stmt = (SelectStatement) QueryProcessor.parseStatement(s).prepare(ClientState.forInternalCalls());
        return RowUtil.toQueryResult(stmt.executeLocally(QueryState.forInternalCalls(), QueryOptions.DEFAULT));
    }

    private static Object[] obj(Object... values)
    {
        return values;
    }

    @Test
    public void serialReadTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                    .set("non_serial_write_strategy", "mixed")
                                                                    .set("lwt_strategy", "accord"))
                                        .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, PRIMARY KEY (k, c))");
            cluster.get(1).runOnInstance(() -> AccordService.instance().ensureKeyspaceIsAccordManaged("ks"));

            cluster.get(1).runOnInstance(() -> localWrite("INSERT INTO ks.tbl (k, c, v) VALUES (1, 1, 1)"));
            cluster.get(2).runOnInstance(() -> localWrite("INSERT INTO ks.tbl (k, c, v) VALUES (1, 1, 2)"));
            cluster.get(3).shutdown();
            cluster.get(1).runOnInstance(() -> QueryResultUtil.assertThat(localRead("SELECT * FROM ks.tbl WHERE k=1")).isEqualTo(obj(obj(1, 1, 1))));
            cluster.get(2).runOnInstance(() -> QueryResultUtil.assertThat(localRead("SELECT * FROM ks.tbl WHERE k=1")).isEqualTo(obj(obj(1, 1, 2))));


            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT v FROM ks.tbl WHERE k=0 AND c=0);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row1 IS NULL THEN\n" +
                           "    INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM ks.tbl WHERE k=1", ConsistencyLevel.SERIAL);
            QueryResultUtil.assertThat(result).isEqualTo(obj(obj(1, 1, 2)));
            cluster.get(1).runOnInstance(() -> QueryResultUtil.assertThat(localRead("SELECT * FROM ks.tbl WHERE k=1")).isEqualTo(obj(obj(1, 1, 2))));
            cluster.get(2).runOnInstance(() -> QueryResultUtil.assertThat(localRead("SELECT * FROM ks.tbl WHERE k=1")).isEqualTo(obj(obj(1, 1, 2))));
        }
    }
}
