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

import accord.primitives.Unseekables;
import accord.topology.Topologies;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.types.utils.Bytes;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.cql3.CQLTester.row;
import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;
import static org.junit.Assert.*;

public abstract class AccordCQLTestBase extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCQLTestBase.class);

    private final TransactionalMode transactionalMode;

    protected AccordCQLTestBase(TransactionalMode transactionalMode) {
        this.transactionalMode = transactionalMode;
    }

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

    @Test
    public void testMultiPartitionReturn() throws Exception
    {
        test(cluster -> {
            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                    cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + "(k, c, v) VALUES (?, ?, ?);", ConsistencyLevel.ALL, i, j, i + j);
            }
            // multi row
            String cql = "BEGIN TRANSACTION\n" +
                         "  SELECT * FROM " + qualifiedTableName + " WHERE k=? AND c IN (?, ?);\n" +
                         "COMMIT TRANSACTION";
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult(cql, ConsistencyLevel.ANY, 0, 0, 1);
            assertThat(result).isEqualTo(QueryResults.builder()
                                                     .columns("k", "c", "v")
                                                     .row(0, 0, 0)
                                                     .row(0, 1, 1)
                                                     .build());
            // Results should be in Partiton/Clustering order, so make sure
            // multi partition
            cql = "BEGIN TRANSACTION\n" +
                  "  SELECT * FROM " + qualifiedTableName + " WHERE k IN (?, ?) AND c = ?;\n" +
                  "COMMIT TRANSACTION";
            for (boolean asc : Arrays.asList(true, false))
            {
                Object[] binds = asc ? row(0, 1, 0) : row(1, 0, 0);
                result = cluster.coordinator(1).executeWithResult(cql, ConsistencyLevel.ANY, binds);
                assertThat(result).isEqualTo(QueryResults.builder()
                                                         .columns("k", "c", "v")
                                                         .row(0, 0, 0)
                                                         .row(1, 0, 1)
                                                         .build());
            }

            // multi-partition, multi-clustering
            cql = "BEGIN TRANSACTION\n" +
                  "  SELECT * FROM " + qualifiedTableName + " WHERE k IN (?, ?) AND c IN (?, ?);\n" +
                  "COMMIT TRANSACTION";
            for (boolean asc : Arrays.asList(true, false))
            {
                Object[] binds = asc ? row(0, 1, 0, 1) : row(1, 0, 1, 0);
                result = cluster.coordinator(1).executeWithResult(cql, ConsistencyLevel.ANY, binds);
                assertThat(result).isEqualTo(QueryResults.builder()
                                                         .columns("k", "c", "v")
                                                         .row(0, 0, 0)
                                                         .row(0, 1, 1)
                                                         .row(1, 0, 1)
                                                         .row(1, 1, 2)
                                                         .build());
            }
        });
    }

    @Test
    public void testMultipleShards() throws Exception
    {
        String keyspace = "multipleShards";
        String currentTable = keyspace + ".tbl";
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + keyspace + ";",
                                          "CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}",
                                          "CREATE TABLE " + currentTable + " (k blob, c int, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'");
        List<String> tokens = tokens();
        List<ByteBuffer> keys = tokensToKeys(tokens);
        List<String> keyStrings = keys.stream().map(bb -> "0x" + ByteBufferUtil.bytesToHex(bb)).collect(Collectors.toList());
        StringBuilder query = new StringBuilder("BEGIN TRANSACTION\n");

        for (int i = 0; i < keys.size(); i++)
            query.append("  LET row" + i + " = (SELECT * FROM " + currentTable + " WHERE k=" + keyStrings.get(i) + " AND c=0);\n");

        query.append("  SELECT row0.v;\n")
             .append("  IF ");

        for (int i = 0; i < keyStrings.size(); i++)
            query.append((i > 0 ? " AND row" : "row") + i + " IS NULL");

        query.append(" THEN\n");

        for (int i = 0; i < keyStrings.size(); i++)
            query.append("    INSERT INTO " + currentTable + " (k, c, v) VALUES (" + keyStrings.get(i) + ", 0, " + i +");\n");

        query.append("  END IF\n");
        query.append("COMMIT TRANSACTION");

        test(ddls, cluster -> {
            // row0.v shouldn't have existed when the txn's SELECT was executed
            assertRowEqualsWithPreemptedRetry(cluster, new Object[]{ null }, query.toString());

            cluster.get(1).runOnInstance(() -> {
                StringBuilder sb = new StringBuilder("BEGIN TRANSACTION\n");
                for (int i = 0; i < keyStrings.size() - 1; i++)
                    sb.append(String.format("LET row%d = (SELECT * FROM %s WHERE k=%s AND c=0);\n", i, currentTable, keyStrings.get(i)));
                sb.append(String.format("SELECT * FROM %s WHERE k=%s AND c=0;\n", currentTable, keyStrings.get(keyStrings.size() - 1)));
                sb.append("COMMIT TRANSACTION");

                Unseekables<?> routables = AccordTestUtils.createTxn(sb.toString()).keys().toParticipants();
                long epoch = AccordService.instance().topology().epoch();
                Topologies topology = AccordService.instance().topology().withUnsyncedEpochs(routables, epoch, epoch);
                // we don't detect out-of-bounds read/write yet, so use this to validate we reach different shards
                Assertions.assertThat(topology.totalShards()).isEqualTo(2);
            });

            String check = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + currentTable + " WHERE k = ? AND c = ?;\n" +
                           "COMMIT TRANSACTION";

            for (int i = 0; i < keys.size(); i++)
                assertRowEqualsWithPreemptedRetry(cluster, new Object[] { keys.get(i), 0, i}, check, keys.get(i), 0);
        });
    }

    @Test
    public void testScalarBindVariables() throws Throwable
    {
        test(cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 0, 3);", ConsistencyLevel.ALL);
                 
                 String query = "BEGIN TRANSACTION\n" +
                                "  LET row1 = (SELECT v FROM " + qualifiedTableName + " WHERE k = ? AND c = ?);\n" +
                                "  LET row2 = (SELECT v FROM " + qualifiedTableName + " WHERE k = ? AND c = ?);\n" +
                                "  SELECT v FROM " + qualifiedTableName + " WHERE k = ? AND c = ?;\n" +
                                "  IF row1 IS NULL AND row2.v = ? THEN\n" +
                                "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, ?);\n" +
                                "  END IF\n" +
                                "COMMIT TRANSACTION";

                 Object[][] result = cluster.coordinator(1).execute(query,
                                                                    ConsistencyLevel.ANY,
                                                                    0, 0,
                                                                    1, 0,
                                                                    1, 0,
                                                                    3,
                                                                    0, 0, 1);
                 assertEquals(3, result[0][0]);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, 0, 1 }, check);
             });
    }

    @Test
    public void testRegularScalarIsNull() throws Throwable
    {
        testScalarIsNull("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testStaticScalarIsNull() throws Throwable
    {
        testScalarIsNull("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int static, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testScalarIsNull(String tableDDL) throws Exception {
        test(tableDDL,
             cluster ->
             {
                 String insertNull = "BEGIN TRANSACTION\n" +
                                     "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 LIMIT 1);\n" +
                                     "  SELECT row0.k, row0.v;\n" +
                                     "  IF row0.v IS NULL THEN\n" +
                                     "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, null);\n" +
                                     "  END IF\n" +
                                     "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null, null }, insertNull, 0, 0);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 LIMIT 1);\n" +
                                 "  SELECT row0.k, row0.v;\n" +
                                 "  IF row0.v IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, null }, insert, 0, 0, 1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT k, c, v  FROM " + qualifiedTableName + " WHERE k=0 AND c=0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, 0, 1 }, check);
             });
    }

    @Test
    public void testQueryStaticColumn() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, s int static, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 // select partition key, clustering key and static column, restrict on partition and clustering
                 testQueryStaticColumn(cluster,
                                       "LET row0 = (SELECT k, c, s, v FROM " + qualifiedTableName + " WHERE k = ? AND c = 0);\n" +
                                       "SELECT row0.k, row0.c, row0.s, row0.v;\n",

                                       "SELECT k, c, s, v FROM " + qualifiedTableName + " WHERE k = ? AND c = 0");

                 // select partition key, clustering key and static column, restrict on partition and limit to 1 row
                 testQueryStaticColumn(cluster,
                                       "LET row0 = (SELECT k, c, s, v FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1);\n" +
                                       "SELECT row0.k, row0.c, row0.s, row0.v;\n",

                                       "SELECT k, c, s, v FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1");

                 // select static column and regular column, restrict on partition and clustering
                 testQueryStaticColumn(cluster,
                                       "LET row0 = (SELECT s, v FROM " + qualifiedTableName + " WHERE k = ? AND c = 0);\n" +
                                       "SELECT row0.s, row0.v;\n",

                                       "SELECT s, v FROM " + qualifiedTableName + " WHERE k = ? AND c = 0");

                 // select just static column, restrict on partition and limit to 1 row
                 testQueryStaticColumn(cluster,
                                       "LET row0 = (SELECT s FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1);\n" +
                                       "SELECT row0.s;\n",

                                       "SELECT s FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1");
             });
    }

    private void testQueryStaticColumn(Cluster cluster, String accordReadQuery, String simpleReadQuery)
    {
        logger().info("Empty table");
        int key = 10;
        assertResultsFromAccordMatches(cluster, accordReadQuery, simpleReadQuery, key++);

        cluster.get(1).coordinator().execute("INSERT INTO " + qualifiedTableName + " (k, s) VALUES (?, null);", ConsistencyLevel.ALL, key);
        logger().info("null -> static column");
        assertResultsFromAccordMatches(cluster, accordReadQuery, simpleReadQuery, key++);

        cluster.get(1).coordinator().execute("INSERT INTO " + qualifiedTableName + " (k, s) VALUES (?, 1);", ConsistencyLevel.ALL, key);
        logger().info("Inserted 1 -> static column");
        assertResultsFromAccordMatches(cluster, accordReadQuery, simpleReadQuery, key++);

        cluster.get(1).coordinator().execute("INSERT INTO " + qualifiedTableName + " (k, c) VALUES (?, 0);", ConsistencyLevel.ALL, key);
        logger().info("Inserted 0 -> clustering");
        assertResultsFromAccordMatches(cluster, accordReadQuery, simpleReadQuery, key);
    }

    @Test
    public void testUpdateStaticColumn() throws Exception {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, s int static, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + '\'',
             cluster ->
             {
                 checkUpdateStatic(cluster, "SET s=1 WHERE k=?", 101, "[[101, null, 1, null]]", "[]");
                 checkUpdateStatic(cluster, "SET s=1, v=11 WHERE k=? AND c=0", 101, "[[101, 0, 1, 11]]", "[[101, 0, 1, 11]]");

                 // commented out until org.apache.cassandra.cql3.statements.ModificationStatement.createSelectForTxn is fixed
                 // checkUpdateStatic(cluster, "SET s+=1 WHERE k=?", 101, "[]", "[]");

                 checkUpdateStatic(cluster, "SET s+=1, v+=11 WHERE k=? AND c=0", 101, "[]", "[]");
             });
    }

    private void checkUpdateStatic(Cluster cluster, String update, int key, String expPart, String expClust)
    {
        Object[][] r1, r2, r3, r4, r;
        r = cluster.get(1).coordinator().execute("UPDATE " + qualifiedTableName + " " + update + " IF s = NULL;", ConsistencyLevel.QUORUM, key);
        Assertions.assertThat(Arrays.deepToString(r)).isEqualTo("[[true]]");
        r1 = cluster.get(1).coordinator().execute("SELECT * FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1;", ConsistencyLevel.SERIAL, key);
        r2 = cluster.get(1).coordinator().execute("SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = 0;", ConsistencyLevel.SERIAL, key);
        cluster.get(1).coordinator().execute("TRUNCATE " + qualifiedTableName, ConsistencyLevel.ALL);

        executeAsTxn(cluster, "UPDATE " + qualifiedTableName + " " + update + ";", key);
        r3 = executeAsTxn(cluster, "SELECT * FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1;", key).toObjectArrays();
        r4 = executeAsTxn(cluster, "SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = 0;", key).toObjectArrays();
        cluster.get(1).coordinator().execute("TRUNCATE " + qualifiedTableName, ConsistencyLevel.ALL);

        Assertions.assertThat(Arrays.deepToString(r1)).isEqualTo(expPart);
        Assertions.assertThat(Arrays.deepToString(r2)).isEqualTo(expClust);
        Assertions.assertThat(Arrays.deepToString(r3)).isEqualTo(expPart);
        Assertions.assertThat(Arrays.deepToString(r4)).isEqualTo(expClust);
    }

    private void assertResultsFromAccordMatches(Cluster cluster, String accordRead, String simpleRead, int key)
    {
        Object[][] simpleReadResult;
        if (transactionalMode.ignoresSuppliedConsistencyLevel)
            // With accord non-SERIAL write strategy the commit CL is effectively ANY so we need to read at SERIAL
            simpleReadResult = cluster.coordinator(1).execute(simpleRead, ConsistencyLevel.SERIAL, key);
        else
            simpleReadResult = cluster.get(1).executeInternal(simpleRead, key);
        Object[][] accordReadResult = executeWithRetry(cluster, accordRead, key).toObjectArrays();

        Assertions.assertThat(withRemovedNullOnlyRows(accordReadResult)).isEqualTo(withRemovedNullOnlyRows(simpleReadResult));
    }

    private static Object[][] withRemovedNullOnlyRows(Object[][] results)
    {
        return Arrays.stream(results)
                     .filter(row -> !Arrays.stream(row).allMatch(Objects::isNull))
                     .toArray(Object[][]::new);
    }

    @Test
    public void testScalarEQ() throws Throwable
    {
        testScalarCondition(3, "=", 3, "=");
    }
    
    @Test
    public void testScalarNEQ() throws Throwable
    {
        testScalarCondition(3, "!=", 4, "!=");
    }

    @Test
    public void testScalarLt() throws Throwable
    {
        testScalarCondition(3, "<", 4, ">");
    }

    @Test
    public void testScalarLte() throws Throwable
    {
        testScalarCondition(3, "<=", 3, ">=");
        setup();
        testScalarCondition(3, "<=", 4, ">=");
    }

    @Test
    public void testScalarGt() throws Throwable
    {
        testScalarCondition(4, ">", 3, "<");
    }

    @Test
    public void testScalarGte() throws Throwable
    {
        testScalarCondition(4, ">=", 3, "<=");
        setup();
        testScalarCondition(4, ">=", 4, "<=");
    }

    @Test
    public void testStaticScalarEQ() throws Throwable
    {
        testScalarCondition("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int static, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'", 3, "=", 3, "=");
    }

    private void testScalarCondition(int lhs, String operator, int rhs, String reversedOperator) throws Exception
    {
        testScalarCondition("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'", lhs, operator, rhs, reversedOperator);
    }

    private void testScalarCondition(String tableDDL, int lhs, String operator, int rhs, String reversedOperator) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 0, " + lhs + ");", ConsistencyLevel.ALL);

                 String query = "BEGIN TRANSACTION\n" +
                                "  LET row1 = (SELECT v FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1);\n" +
                                "  SELECT row1.v;\n" +
                                "  IF row1.v " + operator + " ? THEN\n" +
                                "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, ?);\n" +
                                "  END IF\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { lhs }, query, 0, rhs, 1, 0, 1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1, 0, 1 }, check, 1, 0);

                 String queryWithReversed = "BEGIN TRANSACTION\n" +
                                            "  LET row1 = (SELECT v FROM " + qualifiedTableName + " WHERE k = ? LIMIT 1);\n" +
                                            "  SELECT row1.v;\n" +
                                            "  IF ? " + reversedOperator + " row1.v THEN\n" +
                                            "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, ?);\n" +
                                            "  END IF\n" +
                                            "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { lhs }, queryWithReversed, 0, rhs, 2, 0, 1);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 2, 0, 1 }, check, 2, 0);
             });
    }

    @Test
    public void testReadOnlyTx() throws Exception
    {
        test(cluster ->
             {
                 String query = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0;\n" +
                                "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY);
                 assertFalse(result.hasNext());
             });
    }

    @Test
    public void testWriteOnlyTx() throws Exception
    {
        test(cluster ->
             {
                 String query = "BEGIN TRANSACTION\n" +
                                "  INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, ?);\n" +
                                "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY, 0, 0, 1);
                 assertFalse(result.hasNext());

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k=? AND c=?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 1}, check, 0, 0);
             });
    }

    @Test
    public void testReturningLetReferences() throws Throwable
    {
        test(cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 0, 3);", ConsistencyLevel.ALL);
             
                 String query = "BEGIN TRANSACTION\n" +
                                "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?);\n" +
                                "  LET row2 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?);\n" +
                                "  SELECT row1.v, row2.k, row2.c, row2.v;\n" +
                                "  IF row1 IS NULL AND row2.v = ? THEN\n" +
                                "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, ?);\n" +
                                "  END IF\n" +
                                "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY, 0, 0, 1, 0, 3, 0, 0, 1);
                 assertEquals(ImmutableList.of("row1.v", "row2.k", "row2.c", "row2.v"), result.names());
                 assertThat(result).hasSize(1).contains(null, 1, 0, 3);
             
                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 1}, check);
             });
    }

    @Test
    public void testFailedConditionWithCompleteInsert() throws Throwable
    {
        test(cluster ->
        {
            cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 0, 3);", ConsistencyLevel.ALL);

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?);\n" +
                           "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row0 IS NULL AND row1.v = ? THEN\n" +
                           "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (?, ?, ?);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY, 0, 0, 1, 0, 2, 0, 0, 1);
            assertEquals(ImmutableList.of("row1.v"), result.names());
            assertThat(result).hasSize(1).contains(3);

            String check = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0;\n" +
                           "COMMIT TRANSACTION";
            assertEmptyWithPreemptedRetry(cluster, check);
        });
    }

    @Test
    public void testReversedClusteringReference() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC) AND transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 1, 1)", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1);\n" +
                                 "  SELECT row1.k, row1.c, row1.v;\n" +
                                 "  IF row1.c = 1 THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET v += row1.c WHERE k=1 AND c=1;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[]{1, 1, 1}, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[]{1, 1, 2}, check);
             });
    }

    @Test
    public void testScalarShorthandAddition() throws Exception
    {
        testScalarShorthandOperation(1, "+=", 2);
    }

    @Test
    public void testScalarShorthandSubtraction() throws Exception
    {
        testScalarShorthandOperation(3, "-=", 2);
    }

    private void testScalarShorthandOperation(int startingValue, String operation, int endingvalue) throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v) VALUES (1, ?)", ConsistencyLevel.ALL, startingValue);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.v;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET v " + operation + " 1 WHERE k = 1;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEquals(cluster, new Object[] { startingValue }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT v FROM " + qualifiedTableName + " WHERE k = 1;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 2 }, check);
             });
    }

    @Test
    public void testConstantNonStaticRowReadBeforeUpdate() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, 3);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 2);\n" +
                                 "  SELECT row1.v;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET v += 1 WHERE k = 1 AND c = 2;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEquals(cluster, new Object[] { 3 }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT v FROM " + qualifiedTableName + " WHERE k = 1 AND c = 2;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 4 }, check);
             });
    }

    @Test
    public void testRangeDeletion() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, 3);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 3, ?)", ConsistencyLevel.ALL, 4);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 4, ?)", ConsistencyLevel.ALL, 5);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 2);\n" +
                                 "  SELECT row1.v;\n" +
                                 "  DELETE FROM " + qualifiedTableName + " WHERE k = 1 AND c >=3 AND c <= 4;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEquals(cluster, new Object[] { 3 }, update);

                 Object[][] check = cluster.coordinator(1).execute("SELECT * FROM " + qualifiedTableName + " WHERE k = 1;", ConsistencyLevel.SERIAL);
                 assertArrayEquals(new Object[] { 1, 2, 3 }, check[0]);
                 assertEquals(1, check.length);
             });
    }


    @Test
    public void testPartitionKeyReferenceCondition() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k INT, c INT, v INT, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC) AND transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 1, 1)", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1);\n" +
                                 "  SELECT row1.k, row1.c, row1.v;\n" +
                                 "  IF row1.k = 1 THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET v += row1.k WHERE k=1 AND c=1;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[]{1, 1, 1}, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 1 AND c = 1;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[]{1, 1, 2}, check);
             });
    }

    @Test
    public void testMultiCellListEqCondition() throws Exception
    {
        testListEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenListEqCondition() throws Exception
    {
        testListEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list frozen<list<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testListEqCondition(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 ListType<Integer> listType = ListType.getInstance(Int32Type.instance, true);
                 List<Integer> initialList = Arrays.asList(1, 2);
                 ByteBuffer initialListBytes = listType.getSerializer().serialize(initialList);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, initialListBytes);
                 assertFalse(result.hasNext());

                 List<Integer> updatedList = Arrays.asList(1, 2, 3);
                 ByteBuffer updatedListBytes = listType.getSerializer().serialize(updatedList);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list = ? THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_list = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {initialList}, update, 0, initialListBytes, updatedListBytes, 0);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, updatedList}, check, 0);
             }
        );
    }

    @Test
    public void testMultiCellSetEqCondition() throws Exception
    {
        testSetEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenSetEqCondition() throws Exception
    {
        testSetEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set frozen<set<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testSetEqCondition(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 SetType<Integer> setType = SetType.getInstance(Int32Type.instance, true);
                 Set<Integer> initialSet = ImmutableSet.of(1, 2);
                 ByteBuffer initialSetBytes = setType.getSerializer().serialize(initialSet);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, initialSetBytes);
                 assertFalse(result.hasNext());

                 Set<Integer> updatedSet = ImmutableSet.of(1, 2, 3);
                 ByteBuffer updatedSetBytes = setType.getSerializer().serialize(updatedSet);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  IF row1.int_set = ? THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_set = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {initialSet}, update, 0, initialSetBytes, updatedSetBytes, 0);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, updatedSet}, check, 0);
             }
        );
    }

    @Test
    public void testMultiCellMapEqCondition() throws Exception
    {
        testMapEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'", true);
    }

    @Test
    public void testFrozenMapEqCondition() throws Exception
    {
        testMapEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map frozen<map<text, int>>) WITH transactional_mode='" + transactionalMode + "'", false);
    }

    private void testMapEqCondition(String ddl, boolean isMultiCell) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 MapType<String, Integer> mapType = MapType.getInstance(UTF8Type.instance, Int32Type.instance, isMultiCell);
                 Map<String, Integer> initialMap = ImmutableMap.of("one", 1, "two", 2);
                 ByteBuffer initialMapBytes = mapType.getSerializer().serialize(initialMap);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, initialMapBytes);
                 assertFalse(result.hasNext());

                 Map<String, Integer> updatedMap = ImmutableMap.of("one", 1, "two", 2, "three", 3);
                 ByteBuffer updatedMapBytes = mapType.getSerializer().serialize(updatedMap);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  IF row1.int_map = ? THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_map = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialMap }, update, 0, initialMapBytes, updatedMapBytes, 0);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedMap }, check, 0);
             }
        );
    }

    @Test
    public void testMultiCellUDTEqCondition() throws Exception
    {
        testUDTEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenUDTEqCondition() throws Exception
    {
        testUDTEqCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testUDTEqCondition(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object initialPersonValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer initialPersonBuffer = CQLTester.makeByteBuffer(initialPersonValue, null);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, initialPersonBuffer);
                 assertFalse(result.hasNext());

                 Object updatedPersonValue = CQLTester.userType("height", 73, "age", 40);
                 ByteBuffer updatedPersonBuffer = CQLTester.makeByteBuffer(updatedPersonValue, null);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  IF row1.customer = ? THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET customer = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialPersonBuffer }, update, 0, initialPersonBuffer, updatedPersonBuffer, 0);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedPersonBuffer }, check, 0);
             }
        );
    }

    @Test
    public void testTupleEqCondition() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, pair tuple<text, int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 Object initialTupleValue = CQLTester.tuple("age", 37);
                 ByteBuffer initialTupleBuffer = CQLTester.makeByteBuffer(initialTupleValue, null);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, pair) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, initialTupleBuffer);
                 assertFalse(result.hasNext());

                 Object updatedTupleValue = CQLTester.userType("age", 40);
                 ByteBuffer updatedTupleBuffer = CQLTester.makeByteBuffer(updatedTupleValue, null);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.pair;\n" +
                                 "  IF row1.pair = ? THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET pair = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialTupleBuffer }, update, 0, initialTupleBuffer, updatedTupleBuffer, 0);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedTupleBuffer }, check, 0);
             }
        );
    }

    @Test
    public void testIsNullWithComplexDeletion() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, int_list list<int>, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 ListType<Integer> listType = ListType.getInstance(Int32Type.instance, true);
                 List<Integer> initialList = Arrays.asList(1, 2);
                 ByteBuffer initialListBytes = listType.getSerializer().serialize(initialList);

                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, int_list) VALUES (0, 0, ?);", ConsistencyLevel.ALL, initialListBytes);
                 cluster.forEach(i -> i.flush(KEYSPACE));
                 cluster.coordinator(1).execute("DELETE int_list FROM " + qualifiedTableName + " WHERE k = 0 AND c = 0;", ConsistencyLevel.ALL);

                 List<Integer> updatedList = Arrays.asList(1, 2, 3);
                 ByteBuffer updatedListBytes = listType.getSerializer().serialize(updatedList);
                 
                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, c, int_list) VALUES (?, ?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, insert, 0, 0, 0, 0, updatedListBytes);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, 0, updatedList }, check, 0, 0);
             }
        );
    }

    @Test
    public void testNullMultiCellListConditions() throws Exception
    {
        testNullListConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testNullFrozenListConditions() throws Exception
    {
        testNullListConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list frozen<list<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testNullListConditions(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (0, null);", ConsistencyLevel.ALL);

                 ListType<Integer> listType = ListType.getInstance(Int32Type.instance, true);
                 List<Integer> initialList = Arrays.asList(1, 2);
                 ByteBuffer initialListBytes = listType.getSerializer().serialize(initialList);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {null}, insert, 0, 0, initialListBytes);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, initialList}, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list IS NOT NULL THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_list = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";

                 List<Integer> updatedList = Arrays.asList(1, 2, 3);
                 ByteBuffer updatedListBytes = listType.getSerializer().serialize(updatedList);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {initialList}, update, 0, updatedListBytes, 0);
             }
        );
    }

    @Test
    public void testNullMultiCellSetConditions() throws Exception
    {
        testNullSetConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testNullFrozenSetConditions() throws Exception
    {
        testNullSetConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set frozen<set<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testNullSetConditions(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (0, null);", ConsistencyLevel.ALL);

                 SetType<Integer> setType = SetType.getInstance(Int32Type.instance, true);
                 Set<Integer> initialSet = ImmutableSet.of(1, 2);
                 ByteBuffer initialSetBytes = setType.getSerializer().serialize(initialSet);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  IF row1.int_set IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {null}, insert, 0, 0, initialSetBytes);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, initialSet}, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  IF row1.int_set IS NOT NULL THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_set = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";

                 Set<Integer> updatedSet = ImmutableSet.of(1, 2, 3);
                 ByteBuffer updatedSetBytes = setType.getSerializer().serialize(updatedSet);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {initialSet}, update, 0, updatedSetBytes, 0);
             }
        );
    }

    @Test
    public void testNullMultiCellMapConditions() throws Exception
    {
        testNullMapConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'", true);
    }

    @Test
    public void testNullFrozenMapConditions() throws Exception
    {
        testNullMapConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map frozen<map<text, int>>) WITH transactional_mode='" + transactionalMode + "'", false);
    }

    private void testNullMapConditions(String ddl, boolean isMultiCell) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (0, null);", ConsistencyLevel.ALL);

                 MapType<String, Integer> mapType = MapType.getInstance(UTF8Type.instance, Int32Type.instance, isMultiCell);
                 Map<String, Integer> initialMap = ImmutableMap.of("one", 1, "two", 2);
                 ByteBuffer initialMapBytes = mapType.getSerializer().serialize(initialMap);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  IF row1.int_map IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, insert, 0, 0, initialMapBytes);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, initialMap }, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  IF row1.int_map IS NOT NULL THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_map = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";

                 Map<String, Integer> updatedMap = ImmutableMap.of("one", 1, "two", 2, "three", 3);
                 ByteBuffer updatedMapBytes = mapType.getSerializer().serialize(updatedMap);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialMap }, update, 0, updatedMapBytes, 0);

                 String checkUpdate = "BEGIN TRANSACTION\n" +
                                      "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                      "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedMap }, checkUpdate, 0);
             }
        );
    }

    @Test
    public void testNullMultiCellUDTCondition() throws Exception
    {
        testNullUDTCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testNullFrozenUDTCondition() throws Exception
    {
        testNullUDTCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testNullUDTCondition(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object initialPersonValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer initialPersonBuffer = CQLTester.makeByteBuffer(initialPersonValue, null);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  IF row1.customer IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, insert, 0, 0, initialPersonBuffer);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, initialPersonBuffer }, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  IF row1.customer IS NOT NULL THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET customer = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";

                 Object updatedPersonValue = CQLTester.userType("height", 73, "age", 40);
                 ByteBuffer updatedPersonBuffer = CQLTester.makeByteBuffer(updatedPersonValue, null);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialPersonBuffer }, update, 0, updatedPersonBuffer, 0);

                 String checkUpdate = "BEGIN TRANSACTION\n" +
                                      "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                      "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedPersonBuffer }, checkUpdate, 0);
             }
        );
    }

    @Test
    public void testNullMultiCellSetElementConditions() throws Exception
    {
        testNullSetElementConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testNullFrozenSetElementConditions() throws Exception
    {
        testNullSetElementConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set frozen<set<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testNullSetElementConditions(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (0, {1});", ConsistencyLevel.ALL);

                 SetType<Integer> setType = SetType.getInstance(Int32Type.instance, true);
                 Set<Integer> initialSet = ImmutableSet.of(1, 2);
                 ByteBuffer initialSetBytes = setType.getSerializer().serialize(initialSet);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_set[2];\n" +
                                 "  IF row1.int_set[2] IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {null}, insert, 0, 0, initialSetBytes);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, initialSet}, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                         "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                         "  SELECT row1.int_set;\n" +
                         "  IF row1.int_set[2] IS NOT NULL THEN\n" +
                         "    UPDATE " + qualifiedTableName + " SET int_set = ? WHERE k = ?;\n" +
                         "  END IF\n" +
                         "COMMIT TRANSACTION";

                 Set<Integer> updatedSet = ImmutableSet.of(1, 2, 3);
                 ByteBuffer updatedSetBytes = setType.getSerializer().serialize(updatedSet);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {initialSet}, update, 0, updatedSetBytes, 0);
             }
        );
    }

    @Test
    public void testNullMultiCellMapElementConditions() throws Exception
    {
        testNullMapElementConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'", true);
    }

    @Test
    public void testNullFrozenMapElementConditions() throws Exception
    {
        testNullMapElementConditions("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map frozen<map<text, int>>) WITH transactional_mode='" + transactionalMode + "'", false);
    }

    private void testNullMapElementConditions(String ddl, boolean isMultiCell) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (0, null);", ConsistencyLevel.ALL);

                 MapType<String, Integer> mapType = MapType.getInstance(UTF8Type.instance, Int32Type.instance, isMultiCell);
                 Map<String, Integer> initialMap = ImmutableMap.of("one", 1, "two", 2);
                 ByteBuffer initialMapBytes = mapType.getSerializer().serialize(initialMap);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  IF row1.int_map[?] IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, insert, 0, "one", 0, initialMapBytes);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, initialMap }, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  IF row1.int_map[?] IS NOT NULL THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_map = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";

                 Map<String, Integer> updatedMap = ImmutableMap.of("one", 1, "two", 2, "three", 3);
                 ByteBuffer updatedMapBytes = mapType.getSerializer().serialize(updatedMap);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialMap }, update, 0, "two", updatedMapBytes, 0);

                 String checkUpdate = "BEGIN TRANSACTION\n" +
                                      "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                      "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedMap }, checkUpdate, 0);
             }
        );
    }

    @Test
    public void testNullMultiCellUDTFieldCondition() throws Exception
    {
        testNullUDTFieldCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testNullFrozenUDTFieldCondition() throws Exception
    {
        testNullUDTFieldCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testNullUDTFieldCondition(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object initialPersonValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer initialPersonBuffer = CQLTester.makeByteBuffer(initialPersonValue, null);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  IF row1.customer.age IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (?, ?);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, insert, 0, 0, initialPersonBuffer);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, initialPersonBuffer }, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  IF row1.customer.age IS NOT NULL THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET customer = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";

                 Object updatedPersonValue = CQLTester.userType("height", 73, "age", 40);
                 ByteBuffer updatedPersonBuffer = CQLTester.makeByteBuffer(updatedPersonValue, null);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialPersonBuffer }, update, 0, updatedPersonBuffer, 0);

                 String checkUpdate = "BEGIN TRANSACTION\n" +
                                      "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                      "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedPersonBuffer }, checkUpdate, 0);
             }
        );
    }

    @Test
    public void testMultiCellListSubstitution() throws Exception
    {
        testListSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'", true);
    }

    @Test
    public void testFrozenListSubstitution() throws Exception
    {
        testListSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list frozen<list<int>>) WITH transactional_mode='" + transactionalMode + "'", false);
    }

    private void testListSubstitution(String ddl, boolean isMultiCell) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 ListType<Integer> listType = ListType.getInstance(Int32Type.instance, isMultiCell);
                 List<Integer> initialList = Arrays.asList(1, 2);
                 ByteBuffer initialListBytes = listType.getSerializer().serialize(initialList);

                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (0, ?);", ConsistencyLevel.ALL, initialListBytes);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list IS NOT NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (?, row1.int_list);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialList }, insert, 0, 1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1, initialList }, check, 1);
             }
        );
    }

    @Test
    public void testMultiCellSetSubstitution() throws Exception
    {
        testSetSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'", true);
    }

    @Test
    public void testFrozenSetSubstitution() throws Exception
    {
        testSetSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set frozen<set<int>>) WITH transactional_mode='" + transactionalMode + "'", false);
    }

    private void testSetSubstitution(String ddl, boolean isMultiCell) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 SetType<Integer> setType = SetType.getInstance(Int32Type.instance, isMultiCell);
                 Set<Integer> initialSet = ImmutableSet.of(1, 2);
                 ByteBuffer initialSetBytes = setType.getSerializer().serialize(initialSet);

                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (0, ?);", ConsistencyLevel.ALL, initialSetBytes);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  IF row1.int_set IS NOT NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (?, row1.int_set);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialSet }, insert, 0, 1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1, initialSet }, check, 1);
             }
        );
    }

    @Test
    public void testMultiCellMapSubstitution() throws Exception
    {
        testMapSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'", true);
    }

    @Test
    public void testFrozenMapSubstitution() throws Exception
    {
        testMapSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map frozen<map<text, int>>) WITH transactional_mode='" + transactionalMode + "'", false);
    }

    private void testMapSubstitution(String ddl, boolean isMultiCell) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 MapType<String, Integer> mapType = MapType.getInstance(UTF8Type.instance, Int32Type.instance, isMultiCell);
                 Map<String, Integer> initialMap = ImmutableMap.of("one", 1, "two", 2);
                 ByteBuffer initialMapBytes = mapType.getSerializer().serialize(initialMap);

                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (0, ?);", ConsistencyLevel.ALL, initialMapBytes);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  IF row1.int_map IS NOT NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (?, row1.int_map);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[]{ initialMap }, insert, 0, 1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1, initialMap }, check, 1);
             }
        );
    }

    @Test
    public void testMultiCellUDTSubstitution() throws Exception
    {
        testUDTSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenUDTSubstitution() throws Exception
    {
        testUDTSubstitution("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testUDTSubstitution(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object initialPersonValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer initialPersonBuffer = CQLTester.makeByteBuffer(initialPersonValue, null);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (0, ?);", ConsistencyLevel.ALL, initialPersonBuffer);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  IF row1.customer IS NOT NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (?, row1.customer);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[]{ initialPersonBuffer }, insert, 0, 1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1, initialPersonBuffer }, check, 1);
             }
        );
    }

    @Test
    public void testTupleSubstitution() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, pair tuple<text, int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 Object initialTupleValue = CQLTester.tuple("age", 37);
                 ByteBuffer initialTupleBuffer = CQLTester.makeByteBuffer(initialTupleValue, null);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, pair) VALUES (0, ?);", ConsistencyLevel.ALL, initialTupleBuffer);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.pair;\n" +
                                 "  IF row1.pair IS NOT NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, pair) VALUES (?, row1.pair);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialTupleBuffer }, insert, 0, 1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1, initialTupleBuffer }, check, 1);
             }
        );
    }

    @Test
    public void testMultiCellListReplacement() throws Exception
    {
        testListReplacement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenListReplacement() throws Exception
    {
        testListReplacement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list frozen<list<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testListReplacement(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (0, [1, 2]);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (1, [3, 4]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list = [3, 4] THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_list = row1.int_list WHERE k=0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(3, 4)}, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, Arrays.asList(3, 4)}, check);
             }
        );
    }

    @Test
    public void testMultiCellSetReplacement() throws Exception
    {
        testSetReplacement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenSetReplacement() throws Exception
    {
        testSetReplacement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set frozen<set<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testSetReplacement(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (0, {1, 2});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (1, {3, 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  IF row1.int_set = {3, 4} THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_set = row1.int_set WHERE k=0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableSet.of(3, 4) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, ImmutableSet.of(3, 4) }, check);
             }
        );
    }

    @Test
    public void testListAppendFromReference() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (0, [1, 2]);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (1, [3, 4]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list = [3, 4] THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_list += row1.int_list WHERE k=0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(3, 4)}, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, Arrays.asList(1, 2, 3, 4)}, check);
             }
        );
    }

    @Test
    public void testSetByIndexFromMultiCellListElement() throws Exception
    {
        testListSetByIndexFromListElement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, src_int_list list<int>, dest_int_list list<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testSetByIndexFromFrozenListElement() throws Exception
    {
        testListSetByIndexFromListElement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, src_int_list frozen<list<int>>, dest_int_list list<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testListSetByIndexFromListElement(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, dest_int_list) VALUES (0, [1, 2]);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, src_int_list) VALUES (1, [3, 4]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.src_int_list;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET dest_int_list[0] = row1.src_int_list[0] WHERE k = 0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(3, 4)}, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT dest_int_list FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(3, 2)}, check);
             }
        );
    }

    @Test
    public void testListSetByIndexFromScalar() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (0, [1, 2]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0);\n" +
                                 "  SELECT row0.int_list;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET int_list[0] = 2 WHERE k = 0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(1, 2)}, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT int_list FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(2, 2)}, check);
             }
        );
    }

    @Test
    public void testAutoReadSelectionConstruction() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, counter int, other_counter int, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, counter, other_counter) VALUES (0, 0, 1, 1);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, counter, other_counter) VALUES (0, 1, 1, 1);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 AND c = 0);\n" +
                                 "  SELECT row0.counter, row0.other_counter;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET other_counter += 1, counter += row0.counter WHERE k = 0 AND c = 1;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEquals(cluster, new Object[] { 1, 1 }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT counter, other_counter FROM " + qualifiedTableName + " WHERE k = 0 AND c = 1;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 2, 2 }, check);
             }
        );
    }

    @Test
    public void testMultiMutationsSameKey() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, counter int, int_list list<int>, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, counter, int_list) VALUES (0, 0, 0, [1, 2]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 AND c = 0);\n" +
                                 "  SELECT row0.counter, row0.int_list;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET int_list[0] = 42 WHERE k = 0 AND c = 0;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET counter += 1 WHERE k = 0 AND c = 0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEquals(cluster, new Object[] { 0, Arrays.asList(1, 2) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT counter, int_list FROM " + qualifiedTableName + " WHERE k = 0 AND c = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {1, Arrays.asList(42, 2)}, check);
             }
        );
    }

    @Test
    public void testLetLargerThanOneWithPK() throws Exception
    {
        test(cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 0, 0);", ConsistencyLevel.ALL);

            String cql = "BEGIN TRANSACTION\n" +
                         "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0 LIMIT 2);\n" +
                         "  SELECT row1.v;\n" +
                         "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[]{ 0 }, cql, 1);
        });
    }

    @Test
    public void testLetLimitUsingBind() throws Exception
    {
        test(cluster -> {
             cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 0, 0);", ConsistencyLevel.ALL);

             String cql = "BEGIN TRANSACTION\n" +
                          "    LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 LIMIT ?);\n" +
                          "    SELECT row1.v;\n" +
                          "COMMIT TRANSACTION";
             assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0 }, cql, 1);
        });
    }

    @Test
    public void testListSetByIndexMultiRow() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, int_list list<int>, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, int_list) VALUES (0, 0, [1, 2]);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, int_list) VALUES (0, 1, [3, 4]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 AND c = 0);\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 AND c = 1);\n" +
                                 "  SELECT row0.int_list;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET int_list[0] = row1.int_list[0] WHERE k = 0 AND c = 0;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET int_list[0] = row0.int_list[0] WHERE k = 0 AND c = 1;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { Arrays.asList(1, 2) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                 "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 AND c = 0);\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 AND c = 1);\n" +
                                 "  SELECT row0.int_list, row1.int_list;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(3, 2), Arrays.asList(1, 4)}, check);
             }
        );
    }

    @Test
    public void testSetAppend() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (0, {1, 2});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (1, {3, 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET int_set += row1.int_set WHERE k=0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableSet.of(3, 4) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, ImmutableSet.of(1, 2, 3, 4) }, check);
             }
        );
    }

    @Test
    public void testAssignmentFromMultiCellSetElement() throws Exception
    {
        testAssignmentFromSetElement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testAssignmentFromFrozenSetElement() throws Exception
    {
        testAssignmentFromSetElement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int, int_set frozen<set<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testAssignmentFromSetElement(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v, int_set) VALUES (0, 0, {1, 2});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v, int_set) VALUES (1, 0, {3, 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET v = row1.int_set[4] WHERE k=0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableSet.of(3, 4) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT v FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 4 }, check);
             }
        );
    }

    @Test
    public void testMapAppend() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (0, {'one': 2});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (1, {'three': 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET int_map += row1.int_map WHERE k=0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableMap.of("three", 4) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, ImmutableMap.of("one", 2, "three", 4) }, check);
             }
        );
    }

    @Test
    public void testAssignmentFromMultiCellMapElement() throws Exception
    {
        testAssignmentFromMapElement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testAssignmentFromFrozenMapElement() throws Exception
    {
        testAssignmentFromMapElement("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int, int_map frozen<map<text, int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testAssignmentFromMapElement(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v, int_map) VALUES (0, 0, {'one': 2});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v, int_map) VALUES (1, 0, {'three': 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET v = row1.int_map[?] WHERE k=0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableMap.of("three", 4) }, update, "three");

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT v FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 4 }, check);
             }
        );
    }

    @Test
    public void testAssignmentFromMultiCellUDTField() throws Exception
    {
        testAssignmentFromUDTField("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testAssignmentFromFrozenUDTField() throws Exception
    {
        testAssignmentFromUDTField("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testAssignmentFromUDTField(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object initialPersonValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer initialPersonBuffer = CQLTester.makeByteBuffer(initialPersonValue, null);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v, customer) VALUES (0, 0, null);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v, customer) VALUES (1, 0, ?);", ConsistencyLevel.ALL, initialPersonBuffer);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET v = row1.customer.age WHERE k=0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialPersonBuffer }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT v FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 37 }, check);
             }
        );
    }

    @Test
    public void testSetMapElementFromMapElementReference() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (0, {'one': 2});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (1, {'three': 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET int_map[?] = row1.int_map[?] WHERE k=0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableMap.of("three", 4) }, update, "one", "three");

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT int_map[?] FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 4 }, check, "one");
             }
        );
    }

    @Test
    public void testSetUDTFieldFromUDTFieldReference() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 Object youngPerson = CQLTester.userType("height", 58, "age", 9);
                 ByteBuffer youngPersonBuffer = CQLTester.makeByteBuffer(youngPerson, null);
                 Object adultPerson = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer adultPersonBuffer = CQLTester.makeByteBuffer(adultPerson, null);

                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (0, ?);", ConsistencyLevel.ALL, youngPersonBuffer);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (1, ?);", ConsistencyLevel.ALL, adultPersonBuffer);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  UPDATE " + qualifiedTableName + " SET customer.age = row1.customer.age WHERE k = 0;\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { adultPersonBuffer }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT customer.height, customer.age FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 58, 37 }, check);
             }
        );
    }

    @Test
    public void testMultiCellListElementCondition() throws Exception
    {
        testListElementCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenListElementCondition() throws Exception
    {
        testListElementCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list frozen<list<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testListElementCondition(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (0, [1, 2]);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (1, [3, 4]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list[1] = 4 THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_list = [3, 4] WHERE k = 0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableList.of(3, 4) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, ImmutableList.of(3, 4) }, check);
             }
        );
    }

    @Test
    public void testMultiCellMapElementCondition() throws Exception
    {
        testMapElementCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenMapElementCondition() throws Exception
    {
        testMapElementCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map frozen<map<text, int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testMapElementCondition(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (0, {'one': 2});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (1, {'three': 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_map;\n" +
                                 "  IF row1.int_map[?] = 4 THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_map = {'three': 4} WHERE k = 0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableMap.of("three", 4) }, update, "three");

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, ImmutableMap.of("three", 4) }, check);
             }
        );
    }

    @Test
    public void testMultiCellUDTFieldCondition() throws Exception
    {
        testUDTFieldCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenUDTFieldCondition() throws Exception
    {
        testUDTFieldCondition("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testUDTFieldCondition(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object initialPersonValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer initialPersonBuffer = CQLTester.makeByteBuffer(initialPersonValue, null);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, initialPersonBuffer);
                 assertFalse(result.hasNext());

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, initialPersonBuffer }, check, 0);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                                 "  SELECT row1.customer;\n" +
                                 "  IF row1.customer.age = 37 THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET customer = ? WHERE k = ?;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";

                 Object updatedPersonValue = CQLTester.userType("height", 73, "age", 40);
                 ByteBuffer updatedPersonBuffer = CQLTester.makeByteBuffer(updatedPersonValue, null);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { initialPersonBuffer }, update, 0, updatedPersonBuffer, 0);

                 String checkUpdate = "BEGIN TRANSACTION\n" +
                                      "  SELECT * FROM " + qualifiedTableName + " WHERE k = ?;\n" +
                                      "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, updatedPersonBuffer }, checkUpdate, 0);
             }
        );
    }

    @Test
    public void testListSubtraction() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (0, [1, 2, 3, 4]);", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (1, [3, 4]);", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_list;\n" +
                                 "  IF row1.int_list = [3, 4] THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_list -= row1.int_list WHERE k=0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {Arrays.asList(3, 4)}, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, Arrays.asList(1, 2)}, check);
             }
        );
    }

    @Test
    public void testSetSubtraction() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (0, {1, 2, 3, 4});", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (1, {3, 4});", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  IF row1.int_set = {3, 4} THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_set -= row1.int_set WHERE k=0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableSet.of(3, 4) }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, ImmutableSet.of(1, 2) }, check);
             }
        );
    }

    @Test
    public void testMultiCellMapSubtraction() throws Exception
    {
        testMapSubtraction("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>, int_set set<text>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenMapSubtraction() throws Exception
    {
        testMapSubtraction("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>, int_set frozen<set<text>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testMapSubtraction(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (0, { 'one': 2, 'three': 4 });", ConsistencyLevel.ALL);
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (1, { 'three' });", ConsistencyLevel.ALL);

                 String update = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                 "  SELECT row1.int_set;\n" +
                                 "  IF row1.int_set = { 'three' } THEN\n" +
                                 "    UPDATE " + qualifiedTableName + " SET int_map -= row1.int_set WHERE k=0;\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableSet.of("three") }, update);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = 0;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, ImmutableMap.of("one", 2), null}, check);
             }
        );
    }

    @Test
    public void testMultiCellListSelection() throws Exception
    {
        testListSelection("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenListSelection() throws Exception
    {
        testListSelection("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_list frozen<list<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testListSelection(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_list) VALUES (1, [10, 20, 30, 40]);", ConsistencyLevel.ALL);

                 String selectEntireSet = "BEGIN TRANSACTION\n" +
                                          "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                          "  SELECT row1.int_list;\n" +
                                          "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableList.of(10, 20, 30, 40) }, selectEntireSet);

                 String selectSingleElement = "BEGIN TRANSACTION\n" +
                                              "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                              "  SELECT row1.int_list[0];\n" +
                                              "COMMIT TRANSACTION";

                 SimpleQueryResult result = executeWithRetry(cluster, selectSingleElement);
                 // TODO: Improve user frieldliness of the hex key name here...
                 Assertions.assertThat(result.names()).contains("row1.int_list[0x00000000]");
                 Assertions.assertThat(result.toObjectArrays()).isEqualTo(new Object[] { new Object[] { 10 } });
             }
        );
    }

    @Test
    public void testMultiCellSetSelection() throws Exception
    {
        testSetSelection("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set set<int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenSetSelection() throws Exception
    {
        testSetSelection("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_set frozen<set<int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testSetSelection(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_set) VALUES (1, {10, 20, 30, 40});", ConsistencyLevel.ALL);

                 String selectEntireSet = "BEGIN TRANSACTION\n" +
                                          "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                          "  SELECT row1.int_set;\n" +
                                          "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableSet.of(10, 20, 30, 40) }, selectEntireSet);

                 String selectSingleElement = "BEGIN TRANSACTION\n" +
                                              "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                              "  SELECT row1.int_set[10];\n" +
                                              "COMMIT TRANSACTION";

                 SimpleQueryResult result = executeWithRetry(cluster, selectSingleElement);
                 // TODO: Improve user frieldliness of the hex key name here...
                 Assertions.assertThat(result.names()).contains("row1.int_set[0x0000000a]");
                 Assertions.assertThat(result.toObjectArrays()).isEqualTo(new Object[] { new Object[] { 10 } });
             }
        );
    }

    @Test
    public void testMultiCellMapSelection() throws Exception
    {
        testMapSelection("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map map<text, int>) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testFrozenMapSelection() throws Exception
    {
        testMapSelection("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, int_map frozen<map<text, int>>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testMapSelection(String ddl) throws Exception
    {
        test(ddl,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, int_map) VALUES (1, { 'ten': 20, 'thirty': 40 });", ConsistencyLevel.ALL);

                 String selectEntireMap = "BEGIN TRANSACTION\n" +
                                          "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                          "  SELECT row1.int_map;\n" +
                                          "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { ImmutableMap.of("ten", 20, "thirty", 40) }, selectEntireMap);

                 String selectSingleElement = "BEGIN TRANSACTION\n" +
                                              "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 1);\n" +
                                              "  SELECT row1.int_map['ten'];\n" +
                                              "COMMIT TRANSACTION";

                 SimpleQueryResult result = executeWithRetry(cluster, selectSingleElement);
                 Assertions.assertThat(result.names()).contains("row1.int_map[" + Bytes.toHexString("ten".getBytes()) + ']');
                 Assertions.assertThat(result.toObjectArrays()).isEqualTo(new Object[] { new Object[] { 20 } });
             }
        );
    }

    @Test
    public void testScalarUpdateSubstitution()
    {
        String KEYSPACE = "ks" + System.currentTimeMillis();
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + qualifiedTableName + "1 (k int, c int, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'");
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + qualifiedTableName + "2 (k int, c int, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'");
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedTableName + "1 (k, c, v) VALUES (1, 2, 3);", ConsistencyLevel.ALL);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedTableName + "2 (k, c, v) VALUES (2, 2, 4);", ConsistencyLevel.ALL);

        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM " + qualifiedTableName + "1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM " + qualifiedTableName + "2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM " + qualifiedTableName + "1 WHERE k=1 AND c=2;\n" +
                       "  IF row1.v = 3 AND row2.v = 4 THEN\n" +
                       "    UPDATE " + qualifiedTableName + "1 SET v = row2.v WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";
        Object[][] result = SHARED_CLUSTER.coordinator(1).execute(query, ConsistencyLevel.ANY);
        assertEquals(3, result[0][0]);

        String check = "BEGIN TRANSACTION\n" +
                       "  SELECT * FROM " + qualifiedTableName + "1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";
        assertRowEqualsWithPreemptedRetry(SHARED_CLUSTER, new Object[]{1, 2, 4}, check);
    }

    @Test
    public void testRegularScalarInsertSubstitution() throws Exception
    {
        testScalarInsertSubstitution("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testStaticScalarInsertSubstitution() throws Exception
    {
        testScalarInsertSubstitution("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int static, PRIMARY KEY (k, c)) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testScalarInsertSubstitution(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 0, 1);", ConsistencyLevel.ALL);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = 0 LIMIT 1);\n" +
                                 "  SELECT row0.v;\n" +
                                 "  IF row0.v IS NOT NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 1, row0.v);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1 }, insert);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT k, c, v FROM " + qualifiedTableName + " WHERE k = 0 AND c = 1;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, 1, 1 }, check);
             }
        );
    }

    @Test
    public void testSelectMultiCellUDTReference() throws Exception
    {
        testSelectUDTReference("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testSelectFrozenUDTReference() throws Exception
    {
        testSelectUDTReference("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testSelectUDTReference(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object personValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer personBuffer = CQLTester.makeByteBuffer(personValue, null);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, personBuffer);
                 assertFalse(result.hasNext());

                 String read = "BEGIN TRANSACTION\n" +
                               "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                               "  SELECT row0.customer;\n" +
                               "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { personBuffer }, read, 0);
             }
        );
    }

    @Test
    public void testSelectMultiCellUDTFieldReference() throws Exception
    {
        testSelectUDTFieldReference("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer person) WITH transactional_mode='" + transactionalMode + "'");
    }

    @Test
    public void testSelectFrozenUDTFieldReference() throws Exception
    {
        testSelectUDTFieldReference("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, customer frozen<person>) WITH transactional_mode='" + transactionalMode + "'");
    }

    private void testSelectUDTFieldReference(String tableDDL) throws Exception
    {
        test(tableDDL,
             cluster ->
             {
                 Object personValue = CQLTester.userType("height", 74, "age", 37);
                 ByteBuffer personBuffer = CQLTester.makeByteBuffer(personValue, null);

                 String insert = "BEGIN TRANSACTION\n" +
                                 "  INSERT INTO " + qualifiedTableName + " (k, customer) VALUES (?, ?);\n" +
                                 "COMMIT TRANSACTION";
                 SimpleQueryResult result = cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ANY, 0, personBuffer);
                 assertFalse(result.hasNext());

                 String read = "BEGIN TRANSACTION\n" +
                               "  LET row0 = (SELECT * FROM " + qualifiedTableName + " WHERE k = ?);\n" +
                               "  SELECT row0.customer.age;\n" +
                               "COMMIT TRANSACTION";
                 result = assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 37 }, read, 0);
                 // TODO: Improve user frieldliness of the field name here...
                 assertEquals(ImmutableList.of("row0.customer.0x0001"), result.names());
             }
        );
    }

    @Test
    public void testMultiKeyQueryAndInsert() throws Throwable
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 String query1 = "BEGIN TRANSACTION\n" +
                                 "  LET select1 = (SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0);\n" +
                                 "  LET select2 = (SELECT * FROM " + qualifiedTableName + " WHERE k=1 AND c=0);\n" +
                                 "  SELECT v FROM " + qualifiedTableName + " WHERE k=0 AND c=0;\n" +
                                 "  IF select1 IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 0, 0);\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 0, 0);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertEmptyWithPreemptedRetry(cluster, query1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE k = ? AND c = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 0}, check, 0, 0);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {1, 0, 0}, check, 1, 0);

                 String query2 = "BEGIN TRANSACTION\n" +
                                 "  LET select1 = (SELECT * FROM " + qualifiedTableName + " WHERE k=1 AND c=0);\n" +
                                 "  LET select2 = (SELECT * FROM " + qualifiedTableName + " WHERE k=2 AND c=0);\n" +
                                 "  SELECT v FROM " + qualifiedTableName + " WHERE k=1 AND c=0;\n" +
                                 "  IF select1.v = ? THEN\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (1, 0, 1);\n" +
                                 "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (2, 0, 1);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0 }, query2, 0);

                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 0}, check, 0, 0);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {1, 0, 1}, check, 1, 0);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {2, 0, 1}, check, 2, 0);
             });
    }

    @Test
    public void demoTest() throws Throwable
    {
        SHARED_CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS demo_ks;");
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE demo_ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2};");
        SHARED_CLUSTER.schemaChange("CREATE TABLE demo_ks.org_docs ( org_name text, doc_id int, contents_version int static, title text, permissions int, PRIMARY KEY (org_name, doc_id) ) WITH transactional_mode='" + transactionalMode + "';");
        SHARED_CLUSTER.schemaChange("CREATE TABLE demo_ks.org_users ( org_name text, user text, members_version int static, permissions int, PRIMARY KEY (org_name, user) ) WITH transactional_mode='" + transactionalMode + "';");
        SHARED_CLUSTER.schemaChange("CREATE TABLE demo_ks.user_docs ( user text, doc_id int, title text, org_name text, permissions int, PRIMARY KEY (user, doc_id) ) WITH transactional_mode='" + transactionalMode + "';");

        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO demo_ks.org_users (org_name, user, members_version, permissions) VALUES ('demo', 'blake', 5, 777);\n", ConsistencyLevel.ALL);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO demo_ks.org_users (org_name, user, members_version, permissions) VALUES ('demo', 'scott', 5, 777);\n", ConsistencyLevel.ALL);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO demo_ks.org_docs (org_name, doc_id, contents_version, title, permissions) VALUES ('demo', 100, 5, 'README', 644);\n", ConsistencyLevel.ALL);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO demo_ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('blake', 1, 'recipes', NULL, 777);\n", ConsistencyLevel.ALL);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO demo_ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('blake', 100, 'README', 'demo', 644);\n", ConsistencyLevel.ALL);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO demo_ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('scott', 2, 'to do list', NULL, 777);\n", ConsistencyLevel.ALL);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO demo_ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('scott', 100, 'README', 'demo', 644);\n", ConsistencyLevel.ALL);

        String addDoc = "BEGIN TRANSACTION\n" +
                        "  LET demo_user = (SELECT * FROM demo_ks.org_users WHERE org_name='demo' LIMIT 1);\n" +
                        "  LET existing = (SELECT * FROM demo_ks.org_docs WHERE org_name='demo' AND doc_id=101);\n" +
                        "  SELECT members_version FROM demo_ks.org_users WHERE org_name='demo' LIMIT 1;\n" +
                        "  IF demo_user.members_version = 5 AND existing IS NULL THEN\n" +
                        "    UPDATE demo_ks.org_docs SET title='slides.key', permissions=777, contents_version = 6 WHERE org_name='demo' AND doc_id=101;\n" +
                        "    UPDATE demo_ks.user_docs SET title='slides.key', permissions=777 WHERE user='blake' AND doc_id=101;\n" +
                        "    UPDATE demo_ks.user_docs SET title='slides.key', permissions=777 WHERE user='scott' AND doc_id=101;\n" +
                        "  END IF\n" +
                        "COMMIT TRANSACTION";
        assertRowEquals(SHARED_CLUSTER, new Object[] { 5 }, addDoc);

        String addUser = "BEGIN TRANSACTION\n" +
                         "  LET demo_doc = (SELECT * FROM demo_ks.org_docs WHERE org_name='demo' LIMIT 1);\n" +
                         "  LET existing = (SELECT * FROM demo_ks.org_users WHERE org_name='demo' AND user='benedict');\n" +
                         "  SELECT contents_version FROM demo_ks.org_docs WHERE org_name='demo' LIMIT 1;\n" +
                         "  IF demo_doc.contents_version = 6 AND existing IS NULL THEN\n" +
                         "    UPDATE demo_ks.org_users SET permissions=777, members_version += 1 WHERE org_name='demo' AND user='benedict';\n" +
                         "    UPDATE demo_ks.user_docs SET title='README', permissions=644 WHERE user='benedict' AND doc_id=100;\n" +
                         "    UPDATE demo_ks.user_docs SET title='slides.key', permissions=777 WHERE user='benedict' AND doc_id=101;\n" +
                         "  END IF\n" +
                         "COMMIT TRANSACTION";
        assertRowEquals(SHARED_CLUSTER, new Object[] { 6 }, addUser);
    }

    // TODO: Implement support for basic arithmetic on references in INSERT
    @Ignore
    @Test
    public void testReferenceArithmeticInInsert() throws Exception
    {
        test(cluster -> {
             cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 0, 0)", ConsistencyLevel.ALL);

             String cql = "BEGIN TRANSACTION\n" +
                          "  LET a = (SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0);\n" +
                          "  IF a IS NOT NULL THEN\n" +
                          "    INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 1, a.v + 1);\n" +
                          "  END IF\n" +
                          "COMMIT TRANSACTION";
             assertEmptyWithPreemptedRetry(cluster, cql);
        });
    }

    // TODO: Implement support for basic arithmetic on references in UPDATE
    @Ignore
    @Test
    public void testReferenceArithmeticInUpdate() throws Exception
    {
        test(cluster -> {
             cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, 0, 0)", ConsistencyLevel.ALL);

             String cql = "BEGIN TRANSACTION\n" +
                          "  LET a = (SELECT * FROM " + qualifiedTableName + " WHERE k=0 AND c=0);\n" +
                          "  IF a IS NOT NULL THEN\n" +
                          "    UPDATE " + qualifiedTableName + " SET v = a.v + 1 WHERE k = 0 and c = 1;\n" +
                          "  END IF\n" +
                          "COMMIT TRANSACTION";
             assertEmptyWithPreemptedRetry(cluster, cql);
        });
    }

    // TODO: Re-enable when TrM integration is working
    @Ignore
    @Test
    public void testCASAndSerialRead() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (id int, c int, v int, s int static, PRIMARY KEY ((id), c)) WITH transactional_mode='" + transactionalMode + "';",
            cluster -> {
                ICoordinator coordinator = cluster.coordinator(1);
                int startingAccordCoordinateCount = getAccordCoordinateCount();
                assertRowEquals(cluster, new Object[]{false}, "UPDATE " + qualifiedTableName + " SET v = 4 WHERE id = 1 AND c = 2 IF EXISTS");
                assertRowEquals(cluster, new Object[]{false}, "UPDATE " + qualifiedTableName + " SET v = 4 WHERE id = 1 AND c = 2 IF v = 3");
                coordinator.execute("INSERT INTO " + qualifiedTableName + " (id, c, v, s) VALUES (1, 2, 3, 5);", ConsistencyLevel.ALL);
                assertRowSerial(cluster, "SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1 AND c = 2", 1, 2, 3, 5);
                assertRowEquals(cluster, new Object[]{true}, "UPDATE " + qualifiedTableName + " SET v = 4 WHERE id = 1 AND c = 2 IF v = 3");
                assertRowSerial(cluster, "SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1 AND c = 2", 1, 2, 4, 5);
                assertRowEquals(cluster, new Object[]{ false, 4 }, "UPDATE " + qualifiedTableName + " SET v = 4 WHERE id = 1 AND c = 2 IF v = 3");
                assertRowSerial(cluster, "SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1 AND c = 2", 1, 2, 4, 5);

                // Test working with a static column
                assertRowEquals(cluster, new Object[]{ false, 5 }, "UPDATE " + qualifiedTableName + " SET v = 5 WHERE id = 1 AND c = 2 IF s = 4");
                assertRowSerial(cluster, "SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1 AND c = 2", 1, 2, 4, 5);
                assertRowEquals(cluster, new Object[]{true}, "UPDATE " + qualifiedTableName + " SET v = 5 WHERE id = 1 AND c = 2 IF s = 5");
                assertRowSerial(cluster, "SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1 AND c = 2", 1, 2, 5, 5);
                assertRowEquals(cluster, new Object[]{true}, "UPDATE " + qualifiedTableName + " SET s = 6 WHERE id = 1 IF s = 5");
                assertRowSerial(cluster, "SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1 AND c = 2", 1, 2, 5, 6);

                // Test that read before write works with CAS
                assertRowEquals(cluster, new Object[]{true}, "UPDATE " + qualifiedTableName + " SET s +=1, v += 1 WHERE id = 1 AND c = 2 IF EXISTS");
                assertRowSerial(cluster, "SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1 AND c = 2", 1, 2, 6, 7);

                // Check range deletion works
                coordinator.execute("INSERT INTO " + qualifiedTableName + " (id, c, v, s) VALUES (1, 2, 6, 7);", ConsistencyLevel.ALL);
                coordinator.execute("INSERT INTO " + qualifiedTableName + " (id, c, v) VALUES (1, 3, 3);", ConsistencyLevel.ALL);
                assertRowEquals(cluster, new Object[]{true}, "BEGIN BATCH \n" +
                                                             "UPDATE " + qualifiedTableName + " SET s +=1, v += 1 WHERE id = 1 AND c = 2 IF EXISTS; \n" +
                                                             "DELETE FROM " + qualifiedTableName + " WHERE id = 1 AND c > 0 AND c < 10; \n" +
                                                             "APPLY BATCH;");
                Object[][] rangeDeletionCheck = coordinator.execute("SELECT id, c, v, s FROM " + qualifiedTableName + " WHERE id = 1", ConsistencyLevel.SERIAL);
                assertArrayEquals(new Object[] { 1, 2, 7, 8 }, rangeDeletionCheck[0]);
                assertEquals(1, rangeDeletionCheck.length);

                // Make sure all the consensus using queries actually were run on Accord
                if (transactionalMode.writesThroughAccord)
                    assertEquals( 20, getAccordCoordinateCount() - startingAccordCoordinateCount);
                else
                    // Non-serial writes don't go through Accord in these modes
                    assertEquals( 17, getAccordCoordinateCount() - startingAccordCoordinateCount);
            });
    }

    // Reproduces some bugs that simulator finds
    @Test
    public void testCASSimulatorLite() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (pk int, count int, seq1 text, seq2 list<int>, PRIMARY KEY (pk)) WITH transactional_mode='" + transactionalMode + "'",
             cluster -> {
                 ICoordinator coordinator = cluster.coordinator(1);
                 coordinator.execute("INSERT INTO " + qualifiedTableName + " (pk, count, seq1, seq2) VALUES (1, 0, '', []) USING TIMESTAMP 0", ConsistencyLevel.ALL);

                 ListType<Integer> LIST_TYPE = ListType.getInstance(Int32Type.instance, true);
                 ExecutorService es = Executors.newCachedThreadPool();
                 List<Future<Object[][]>> futures = new ArrayList<>();
                 for (int ii = 0; ii < 10; ii++)
                 {
                     int id = ii;
                     futures.add(es.submit(() -> coordinator.execute("UPDATE " + qualifiedTableName + " SET count = count + 1, seq1 = seq1 + ?, seq2 = seq2 + ? WHERE pk = ? IF EXISTS", ConsistencyLevel.ALL, id + ",", ByteBufferUtil.getArray(LIST_TYPE.decompose(singletonList(id))), 1)));
                 }
                 for (Future f : futures)
                     f.get();

                 Object[][] result = coordinator.execute("SELECT pk, count, seq1, seq2 FROM  " + qualifiedTableName + " WHERE pk = 1", ConsistencyLevel.SERIAL);

                 int[] seq1 = Arrays.stream(((String) result[0][2]).split(","))
                                    .filter(s -> !s.isEmpty())
                                    .mapToInt(Integer::parseInt)
                                    .toArray();
                int[] seq2 = ((ArrayList<Integer>) result[0][3]).stream().mapToInt(x -> x).toArray();
                logger.info("String append of ids executed {}", Arrays.toString(seq1));
                logger.info("List append of ids executed {}", Arrays.toString(seq2));
                assertArrayEquals("History doesn't match between the two columns", seq1, seq2);
             });
    }

    @Test
    public void testTransactionCasSimulatorLite() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (pk int, count int, seq1 text, seq2 list<int>, PRIMARY KEY (pk)) WITH transactional_mode='" + transactionalMode + "'",
             cluster ->
             {
                 ICoordinator coordinator = cluster.coordinator(1);
                 coordinator.execute("INSERT INTO " + qualifiedTableName + " (pk, count, seq1, seq2) VALUES (1, 0, '', []) USING TIMESTAMP 0", ConsistencyLevel.ALL);

                 ListType<Integer> LIST_TYPE = ListType.getInstance(Int32Type.instance, true);
                 ExecutorService es = Executors.newCachedThreadPool();
                 List<Future<SimpleQueryResult>> futures = new ArrayList<>();
                 for (int ii = 0; ii < 10; ii++)
                 {
                     int id = ii;
                     String update = "BEGIN TRANSACTION\n" +
                                     "  LET row1 = (SELECT * FROM " + qualifiedTableName + " WHERE pk = 1);\n" +
                                     "  UPDATE " + qualifiedTableName + " SET count += 1, seq1 = seq1 + ?, seq2 = seq2 + ? WHERE pk=1;\n" +
                                     "COMMIT TRANSACTION";
                     futures.add(es.submit(() -> coordinator.executeWithResult(update, ConsistencyLevel.ANY, id + ",", ByteBufferUtil.getArray(LIST_TYPE.decompose(singletonList(id))))));
                 }
                 for (Future f : futures)
                     f.get();

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + qualifiedTableName + " WHERE pk = 1;\n" +
                                "COMMIT TRANSACTION";
                 Object[][] result = coordinator.execute(check, ConsistencyLevel.ALL);

                 int[] seq1 = Arrays.stream(((String) result[0][2]).split(","))
                                    .filter(s -> !s.isEmpty())
                                    .mapToInt(Integer::parseInt)
                                    .toArray();
                 int[] seq2 = ((ArrayList<Integer>) result[0][3]).stream().mapToInt(x -> x).toArray();
                 logger.info("String append of ids executed {}", Arrays.toString(seq1));
                 logger.info("List append of ids executed {}", Arrays.toString(seq2));
                 assertArrayEquals("History doesn't match between the two columns", seq1, seq2);
             }
        );
    }

    @Test
    public void testSerialReadDescending() throws Throwable
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='" + transactionalMode + "'",
             cluster -> {
                 ICoordinator coordinator = cluster.coordinator(1);
                 for (int i = 1; i <= 10; i++)
                     coordinator.execute("INSERT INTO " + qualifiedTableName + " (k, c, v) VALUES (0, ?, ?) USING TIMESTAMP 0;", ConsistencyLevel.ALL, i, i * 10);
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedTableName + " WHERE k=0 ORDER BY c DESC LIMIT 1", AssertUtils.row(10, 100));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedTableName + " WHERE k=0 ORDER BY c DESC LIMIT 2", AssertUtils.row(10, 100), AssertUtils.row(9, 90));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedTableName + " WHERE k=0 ORDER BY c DESC LIMIT 3", AssertUtils.row(10, 100), AssertUtils.row(9, 90), AssertUtils.row(8, 80));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedTableName + " WHERE k=0 ORDER BY c DESC LIMIT 4", AssertUtils.row(10, 100), AssertUtils.row(9, 90), AssertUtils.row(8, 80), AssertUtils.row(7, 70));
             }
         );
    }
}
