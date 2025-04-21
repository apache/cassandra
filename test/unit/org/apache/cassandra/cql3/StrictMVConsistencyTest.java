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

package org.apache.cassandra.cql3;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.paxos.Paxos;

import java.io.IOException;

public class StrictMVConsistencyTest
{
    private static final String KEYSPACE = "cql_test_keyspace";
    private static Session session;
    private static EmbeddedCassandraService cassandra;
    private static Cluster cluster;
    private static final String BASE_TABLE = "basetablemetricstest";

    private static final String MV1 = "mv1metricstest";

    @BeforeClass
    public static void setUpClass() {
        try {
            cassandra = ServerTestUtils.startEmbeddedCassandraService();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        DatabaseDescriptor.setMaterializedViewStrictConsistencyEnabled(true);

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", KEYSPACE));
        session.execute(String.format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEYSPACE, MV1));
        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, BASE_TABLE));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1)) WITH strict_mv_consistency = true;", KEYSPACE, BASE_TABLE));
        session.execute(String.format("CREATE MATERIAlIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND val1 IS NOT NULL AND val2 IS NOT NULL PRIMARY KEY(val2, id, val1);", KEYSPACE, MV1, KEYSPACE, BASE_TABLE));
    }

    @Test
    public void testStrictMVConsistencyEnforced()
    {
        DatabaseDescriptor.setMaterializedViewStrictConsistencyEnforced(true);
        String tableNmae = "test_base_table_strict_mv_enforced";
        String mvName = "test_base_table_strict_mv_enforced_mv1";
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1)) WITH strict_mv_consistency = false;", KEYSPACE, tableNmae));
        invalidQueryExceptionTestHelper(String.format("CREATE MATERIAlIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND val1 IS NOT NULL AND val2 IS NOT NULL PRIMARY KEY(val2, id, val1);", KEYSPACE, mvName, KEYSPACE, tableNmae),
                                        "Materialized views can only be created on table with strict MV consistency enabled.");
        // test strict MV consistency enabled on base table, MV can be created
        tableNmae = "test_base_table_strict_mv_enforced1";
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1)) WITH strict_mv_consistency = true;", KEYSPACE, tableNmae));
        session.execute(String.format("CREATE MATERIAlIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND val1 IS NOT NULL AND val2 IS NOT NULL PRIMARY KEY(val2, id, val1);", KEYSPACE, mvName, KEYSPACE, tableNmae));
    }

    @Test
    public void testUnqualifiedQueriesShouldFail() throws Throwable
    {
        String partitionDelete = "DELETE FROM %s.%s WHERE id = %d";
        invalidQueryExceptionTestHelper(String.format(partitionDelete, KEYSPACE, BASE_TABLE, 1),
                                        "DELETE statements must restrict all PRIMARY KEY columns with equality relations for Strict MV consistency enabled table.");


        String batch = "BEGIN BATCH\n" +
                       "INSERT INTO %s.%s (id, val1, val2) VALUES (1, '1', '11');\n" +
                       "INSERT INTO %s.%s (id, val1, val2) VALUES (2, '1', '11');\n" +
                       "APPLY BATCH";
        invalidQueryExceptionTestHelper(String.format(batch, KEYSPACE, BASE_TABLE, KEYSPACE, BASE_TABLE),
                                        "Cannot use batch statement for strict MV enabled table: " + BASE_TABLE);

        String insert = "INSERT INTO %s.%s (id, val1, val2) VALUES (1, '1', '11') USING TIMESTAMP 12345;";
        invalidQueryExceptionTestHelper(String.format(insert, KEYSPACE, BASE_TABLE),
                                        "Cannot provide custom timestamp for strict MV consistency enabled table");

        // insert without timestamp should work
        insert = "INSERT INTO %s.%s (id, val1, val2) VALUES (1, '1', '11')";
        session.execute(String.format(insert, KEYSPACE, BASE_TABLE));

        // update with IN partition key
        String update = "UPDATE %s.%s SET val2 = '2' WHERE id IN (1, 2) AND val1='1';";
        invalidQueryExceptionTestHelper(String.format(update, KEYSPACE, BASE_TABLE),
                                        "Cannot use IN restritions in statement for strict MV consistency enabled table");

        // update with IN clustering key
        update = "UPDATE %s.%s SET val2 = '2' WHERE id=1 AND val1 IN ('1', '2');";
        invalidQueryExceptionTestHelper(String.format(update, KEYSPACE, BASE_TABLE),
                                        "Cannot use IN restritions in statement for strict MV consistency enabled table");

        // update with IN both partition key and clustering key
        update = "UPDATE %s.%s SET val2 = '2' WHERE id IN (1, 2) AND val1 IN ('1', '2');";
        invalidQueryExceptionTestHelper(String.format(update, KEYSPACE, BASE_TABLE),
                                        "Cannot use IN restritions in statement for strict MV consistency enabled table");

    }

    private void invalidQueryExceptionTestHelper(String query, String errorMessage)
    {
        try
        {
            session.execute(query);
        } catch (InvalidQueryException e)
        {
            Assert.assertEquals(e.getMessage(), errorMessage);
            return;
        }
        Assert.fail("Expecting InvalidRequestException but didn't get it.");
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }
}
