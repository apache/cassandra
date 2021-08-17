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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class BatchTest extends CQLTester
{
    /**
     * Test batch statements
     * migrated from cql_tests.py:TestCQL.batch_test()
     */
    @Test
    public void testBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (userid text PRIMARY KEY, name text, password text)");

        execute("BEGIN BATCH\n" +
                "INSERT INTO %1$s (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');\n" + 
                "UPDATE %1$s SET password = 'ps22dhds' WHERE userid = 'user3';\n" + 
                "INSERT INTO %1$s (userid, password) VALUES ('user4', 'ch@ngem3c');\n" + 
                "DELETE name FROM %1$s WHERE userid = 'user1';\n" + 
                "APPLY BATCH;");

        assertRows(execute("SELECT userid FROM %s"),
                   row("user2"),
                   row("user4"),
                   row("user3"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.batch_and_list_test()
     */
    @Test
    public void testBatchAndList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>)");

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET l = l + [1] WHERE k = 0; " +
                "UPDATE %1$s SET l = l + [2] WHERE k = 0; " +
                "UPDATE %1$s SET l = l + [3] WHERE k = 0; " +
                "APPLY BATCH");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
                   row(list(1, 2, 3)));

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET l = [1] + l WHERE k = 1; " +
                "UPDATE %1$s SET l = [2] + l WHERE k = 1; " +
                "UPDATE %1$s SET l = [3] + l WHERE k = 1; " +
                "APPLY BATCH ");

        assertRows(execute("SELECT l FROM %s WHERE k = 1"),
                   row(list(3, 2, 1)));
    }

    @Test
    public void testBatchAndMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, int>)");

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET m[1] = 2 WHERE k = 0; " +
                "UPDATE %1$s SET m[3] = 4 WHERE k = 0; " +
                "APPLY BATCH");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map(1, 2, 3, 4)));
    }

    @Test
    public void testBatchAndSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>)");

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET s = s + {1} WHERE k = 0; " +
                "UPDATE %1$s SET s = s + {2} WHERE k = 0; " +
                "UPDATE %1$s SET s = s + {3} WHERE k = 0; " +
                "APPLY BATCH");

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row(set(1, 2, 3)));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_6115_test()
     */
    @Test
    public void testBatchDeleteInsert() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s (k, v) VALUES (0, 1)");
        execute("BEGIN BATCH DELETE FROM %1$s WHERE k=0 AND v=1; INSERT INTO %1$s (k, v) VALUES (0, 2); APPLY BATCH");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 2));
    }

    @Test
    public void testBatchWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");

        // test batch and update
        execute("BEGIN BATCH " +
                "INSERT INTO %1$s (k, s, i) VALUES (100, 'batchtext', 7); " +
                "INSERT INTO %1$s (k, s, i) VALUES (111, 'batchtext', 7); " +
                "UPDATE %1$s SET s=?, i=? WHERE k = 100; " +
                "UPDATE %1$s SET s=?, i=? WHERE k=111; " +
                "APPLY BATCH;", null, unset(), unset(), null);
        assertRows(execute("SELECT k, s, i FROM %s where k in (100,111)"),
                   row(100, null, 7),
                   row(111, "batchtext", null)
        );
    }

    @Test
    public void testBatchUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                "clustering_1 int," +
                "value int," +
                " PRIMARY KEY (partitionKey, clustering_1))");

        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 4, 4)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 5, 5)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 6, 6)");

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET value = 7 WHERE partitionKey = 0 AND clustering_1 = 1;" +
                "UPDATE %1$s SET value = 8 WHERE partitionKey = 0 AND (clustering_1) = (2);" +
                "UPDATE %1$s SET value = 10 WHERE partitionKey = 0 AND clustering_1 IN (3, 4);" +
                "UPDATE %1$s SET value = 20 WHERE partitionKey = 0 AND (clustering_1) IN ((5), (6));" +
                "APPLY BATCH;");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, 0),
                   row(0, 1, 7),
                   row(0, 2, 8),
                   row(0, 3, 10),
                   row(0, 4, 10),
                   row(0, 5, 20),
                   row(0, 6, 20));
    }

    @Test
    public void testBatchEmpty() throws Throwable
    {
        assertEmpty(execute("BEGIN BATCH APPLY BATCH;"));
    }

    @Test
    public void testBatchMultipleTable() throws Throwable
    {
        String tbl1 = KEYSPACE + "." + createTableName();
        String tbl2 = KEYSPACE + "." + createTableName();

        schemaChange(String.format("CREATE TABLE %s (k1 int PRIMARY KEY, v11 int, v12 int)", tbl1));
        schemaChange(String.format("CREATE TABLE %s (k2 int PRIMARY KEY, v21 int, v22 int)", tbl2));

        execute("BEGIN BATCH " +
                String.format("UPDATE %s SET v11 = 1 WHERE k1 = 0;", tbl1) +
                String.format("UPDATE %s SET v12 = 2 WHERE k1 = 0;", tbl1) +
                String.format("UPDATE %s SET v21 = 3 WHERE k2 = 0;", tbl2) +
                String.format("UPDATE %s SET v22 = 4 WHERE k2 = 0;", tbl2) +
                "APPLY BATCH;");

        assertRows(execute(String.format("SELECT * FROM %s", tbl1)), row(0, 1, 2));
        assertRows(execute(String.format("SELECT * FROM %s", tbl2)), row(0, 3, 4));

        flush();

        assertRows(execute(String.format("SELECT * FROM %s", tbl1)), row(0, 1, 2));
        assertRows(execute(String.format("SELECT * FROM %s", tbl2)), row(0, 3, 4));
    }

    @Test
    public void testBatchMultipleTablePrepare() throws Throwable
    {
        String tbl1 = KEYSPACE + "." + createTableName();
        String tbl2 = KEYSPACE + "." + createTableName();

        schemaChange(String.format("CREATE TABLE %s (k1 int PRIMARY KEY, v1 int)", tbl1));
        schemaChange(String.format("CREATE TABLE %s (k2 int PRIMARY KEY, v2 int)", tbl2));

        String query = "BEGIN BATCH " +
                   String.format("UPDATE %s SET v1 = 1 WHERE k1 = ?;", tbl1) +
                   String.format("UPDATE %s SET v2 = 2 WHERE k2 = ?;", tbl2) +
                   "APPLY BATCH;";
        prepare(query);
        execute(query, 0, 1);

        assertRows(execute(String.format("SELECT * FROM %s", tbl1)), row(0, 1));
        assertRows(execute(String.format("SELECT * FROM %s", tbl2)), row(1, 2));
    }

    @Test
    public void testBatchWithInRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a,b))");
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,1,1);
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,2,2);
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,3,3);

        for (String inClause : new String[] { "()", "(1, 2)"})
        {
            assertInvalidMessage("IN on the clustering key columns is not supported with conditional updates",
                                 "BEGIN BATCH " +
                                 "UPDATE %1$s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "UPDATE %1$s SET c = 200 WHERE a = 1 AND b IN " + inClause + " IF c = 1;" +
                                 "APPLY BATCH");

            assertInvalidMessage("IN on the clustering key columns is not supported with conditional deletions",
                                 "BEGIN BATCH " +
                                 "UPDATE %1$s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "DELETE FROM %1$s WHERE a = 1 AND b IN " + inClause + " IF c = 1;" +
                                 "APPLY BATCH");

            assertInvalidMessage("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)",
                                 "BEGIN BATCH " +
                                 "UPDATE %1$s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "UPDATE %1$s SET c = 200 WHERE a IN " + inClause + " AND b = 1 IF c = 1;" +
                                 "APPLY BATCH");

            assertInvalidMessage("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)",
                                 "BEGIN BATCH " +
                                 "UPDATE %1$s SET c = 100 WHERE a = 1 AND b = 1 IF c = 1;" +
                                 "DELETE FROM %1$s WHERE a IN " + inClause + " AND b = 1 IF c = 1;" +
                                 "APPLY BATCH");
        }
        assertRows(execute("SELECT * FROM %s"),
                   row(1,1,1),
                   row(1,2,2),
                   row(1,3,3));
    }

    @Test
    public void testBatchTTLConditionalInteraction() throws Throwable
    {

        createTable(String.format("CREATE TABLE %s.clustering (\n" +
                "  id int,\n" +
                "  clustering1 int,\n" +
                "  clustering2 int,\n" +
                "  clustering3 int,\n" +
                "  val int, \n" +
                " PRIMARY KEY(id, clustering1, clustering2, clustering3)" +
                ")", KEYSPACE));

        execute("DELETE FROM " + KEYSPACE +".clustering WHERE id=1");

        String clusteringInsert = "INSERT INTO " + KEYSPACE + ".clustering(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s); ";
        String clusteringTTLInsert = "INSERT INTO " + KEYSPACE + ".clustering(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) USING TTL %s; ";
        String clusteringConditionalInsert = "INSERT INTO " + KEYSPACE + ".clustering(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) IF NOT EXISTS; ";
        String clusteringConditionalTTLInsert = "INSERT INTO " + KEYSPACE + ".clustering(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s)  IF NOT EXISTS USING TTL %s; ";
        String clusteringUpdate = "UPDATE " + KEYSPACE + ".clustering SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringTTLUpdate = "UPDATE " + KEYSPACE + ".clustering USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringConditionalUpdate = "UPDATE " + KEYSPACE + ".clustering SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ;";
        String clusteringConditionalTTLUpdate = "UPDATE " + KEYSPACE + ".clustering USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ;";
        String clusteringDelete = "DELETE FROM " + KEYSPACE + ".clustering WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringRangeDelete = "DELETE FROM " + KEYSPACE + ".clustering WHERE id=%s AND clustering1=%s ;";
        String clusteringConditionalDelete = "DELETE FROM " + KEYSPACE + ".clustering WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ; ";


        execute("BEGIN BATCH " + String.format(clusteringInsert, 1, 1, 1, 1, 1) + " APPLY BATCH");

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"), row(1, 1, 1, 1, 1));

        StringBuilder cmd2 = new StringBuilder();
        cmd2.append("BEGIN BATCH ");
        cmd2.append(String.format(clusteringInsert, 1, 1, 1, 2, 2));
        cmd2.append(String.format(clusteringConditionalUpdate, 11, 1, 1, 1, 1, 1));
        cmd2.append("APPLY BATCH ");
        execute(cmd2.toString());


        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 1, 1, 1, 11),
                row(1, 1, 1, 2, 2)
        );


        StringBuilder cmd3 = new StringBuilder();
        cmd3.append("BEGIN BATCH ");
        cmd3.append(String.format(clusteringInsert, 1, 1, 2, 3, 23));
        cmd3.append(String.format(clusteringConditionalUpdate, 22, 1, 1, 1, 2, 2));
        cmd3.append(String.format(clusteringDelete, 1, 1, 1, 1));
        cmd3.append("APPLY BATCH ");
        execute(cmd3.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 1, 1, 2, 22),
                row(1, 1, 2, 3, 23)
        );

        StringBuilder cmd4 = new StringBuilder();
        cmd4.append("BEGIN BATCH ");
        cmd4.append(String.format(clusteringInsert, 1, 2, 3, 4, 1234));
        cmd4.append(String.format(clusteringConditionalUpdate, 234, 1, 1, 1, 2, 22));
        cmd4.append("APPLY BATCH ");
        execute(cmd4.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 1, 1, 2, 234),
                row(1, 1, 2, 3, 23),
                row(1, 2, 3, 4, 1234)
        );

        StringBuilder cmd5 = new StringBuilder();
        cmd5.append("BEGIN BATCH ");
        cmd5.append(String.format(clusteringRangeDelete, 1, 2));
        cmd5.append(String.format(clusteringConditionalUpdate, 1234, 1, 1, 1, 2, 234));
        cmd5.append("APPLY BATCH ");
        execute(cmd5.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 1, 1, 2, 1234),
                row(1, 1, 2, 3, 23)
        );

        StringBuilder cmd6 = new StringBuilder();
        cmd6.append("BEGIN BATCH ");
        cmd6.append(String.format(clusteringUpdate, 345, 1, 3, 4, 5));
        cmd6.append(String.format(clusteringConditionalUpdate, 1, 1, 1, 1, 2, 1234));
        cmd6.append("APPLY BATCH ");
        execute(cmd6.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 1, 1, 2, 1),
                row(1, 1, 2, 3, 23),
                row(1, 3, 4, 5, 345)
        );


        StringBuilder cmd7 = new StringBuilder();
        cmd7.append("BEGIN BATCH ");
        cmd7.append(String.format(clusteringDelete, 1, 3, 4, 5));
        cmd7.append(String.format(clusteringConditionalUpdate, 2300, 1, 1, 2, 3, 1));  // SHOULD NOT MATCH
        cmd7.append("APPLY BATCH ");
        execute(cmd7.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 1, 1, 2, 1),
                row(1, 1, 2, 3, 23),
                row(1, 3, 4, 5, 345)
        );

        StringBuilder cmd8 = new StringBuilder();
        cmd8.append("BEGIN BATCH ");
        cmd8.append(String.format(clusteringConditionalDelete, 1, 3, 4, 5, 345));
        cmd8.append(String.format(clusteringRangeDelete, 1, 1));
        cmd8.append(String.format(clusteringInsert, 1, 2, 3, 4, 5));
        cmd8.append("APPLY BATCH ");
        execute(cmd8.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 2, 3, 4, 5)
        );

        StringBuilder cmd9 = new StringBuilder();
        cmd9.append("BEGIN BATCH ");
        cmd9.append(String.format(clusteringConditionalInsert, 1, 3, 4, 5, 345));
        cmd9.append(String.format(clusteringDelete, 1, 2, 3, 4));
        cmd9.append("APPLY BATCH ");
        execute(cmd9.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 3, 4, 5, 345)
        );

        StringBuilder cmd10 = new StringBuilder();
        cmd10.append("BEGIN BATCH ");
        cmd10.append(String.format(clusteringTTLInsert, 1, 2, 3, 4, 5, 5));
        cmd10.append(String.format(clusteringConditionalTTLUpdate, 10, 5, 1, 3, 4, 5, 345));
        cmd10.append("APPLY BATCH ");
        execute(cmd10.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 2, 3, 4, 5), // 5 second TTL
                row(1, 3, 4, 5, 5)  // 10 second TTL
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 3, 4, 5, 5) // now 4 second TTL
        );

        StringBuilder cmd11 = new StringBuilder();
        cmd11.append("BEGIN BATCH ");
        cmd11.append(String.format(clusteringConditionalTTLInsert, 1, 2, 3, 4, 5, 5));
        cmd11.append(String.format(clusteringInsert,1, 4, 5, 6, 7));
        cmd11.append("APPLY BATCH ");
        execute(cmd11.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 2, 3, 4, 5), // This one has 5 seconds left
                row(1, 3, 4, 5, 5), // This one should have 4 seconds left
                row(1, 4, 5, 6, 7) // This one has no TTL
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 3, 4, 5, null), // We had a row here before from cmd9, but we've ttl'd out the value in cmd11
                row(1, 4, 5, 6, 7)
        );

        StringBuilder cmd12 = new StringBuilder();
        cmd12.append("BEGIN BATCH ");
        cmd12.append(String.format(clusteringConditionalTTLUpdate, 5, 5, 1, 3, 4, 5, null));
        cmd12.append(String.format(clusteringTTLUpdate, 5, 8, 1, 4, 5, 6));
        cmd12.append("APPLY BATCH ");
        execute(cmd12.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 3, 4, 5, 5),
                row(1, 4, 5, 6, 8)
        );

        Thread.sleep(6000);
        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering WHERE id=1"),
                row(1, 3, 4, 5, null),
                row(1, 4, 5, 6, null)
        );
    }


    @Test
    public void testBatchStaticTTLConditionalInteraction() throws Throwable
    {

        createTable(String.format("CREATE TABLE %s.clustering_static (\n" +
                "  id int,\n" +
                "  clustering1 int,\n" +
                "  clustering2 int,\n" +
                "  clustering3 int,\n" +
                "  sval int static, \n" +
                "  val int, \n" +
                " PRIMARY KEY(id, clustering1, clustering2, clustering3)" +
                ")", KEYSPACE));

        execute("DELETE FROM " + KEYSPACE +".clustering_static WHERE id=1");

        String clusteringInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s); ";
        String clusteringTTLInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) USING TTL %s; ";
        String clusteringStaticInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, sval, val) VALUES(%s, %s, %s, %s, %s, %s); ";
        String clusteringConditionalInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) IF NOT EXISTS; ";
        String clusteringConditionalTTLInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s)  IF NOT EXISTS USING TTL %s; ";
        String clusteringUpdate = "UPDATE " + KEYSPACE + ".clustering_static SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringStaticUpdate = "UPDATE " + KEYSPACE + ".clustering_static SET sval=%s WHERE id=%s ;";
        String clusteringTTLUpdate = "UPDATE " + KEYSPACE + ".clustering_static USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringStaticConditionalUpdate = "UPDATE " + KEYSPACE + ".clustering_static SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ;";
        String clusteringConditionalTTLUpdate = "UPDATE " + KEYSPACE + ".clustering_static USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ;";
        String clusteringStaticConditionalTTLUpdate = "UPDATE " + KEYSPACE + ".clustering_static USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ;";
        String clusteringStaticConditionalStaticUpdate = "UPDATE " + KEYSPACE +".clustering_static SET sval=%s WHERE id=%s IF sval=%s; ";
        String clusteringDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringRangeDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s ;";
        String clusteringConditionalDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ; ";
        String clusteringStaticConditionalDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ; ";


        execute("BEGIN BATCH " + String.format(clusteringStaticInsert, 1, 1, 1, 1, 1, 1) + " APPLY BATCH");

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"), row(1, 1, 1, 1, 1, 1));

        StringBuilder cmd2 = new StringBuilder();
        cmd2.append("BEGIN BATCH ");
        cmd2.append(String.format(clusteringInsert, 1, 1, 1, 2, 2));
        cmd2.append(String.format(clusteringStaticConditionalUpdate, 11, 1, 1, 1, 1, 1));
        cmd2.append("APPLY BATCH ");
        execute(cmd2.toString());


        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 1, 1, 11),
                row(1, 1, 1, 2, 1, 2)
        );


        StringBuilder cmd3 = new StringBuilder();
        cmd3.append("BEGIN BATCH ");
        cmd3.append(String.format(clusteringInsert, 1, 1, 2, 3, 23));
        cmd3.append(String.format(clusteringStaticUpdate, 22, 1));
        cmd3.append(String.format(clusteringDelete, 1, 1, 1, 1));
        cmd3.append("APPLY BATCH ");
        execute(cmd3.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 2),
                row(1, 1, 2, 3, 22, 23)
        );

        StringBuilder cmd4 = new StringBuilder();
        cmd4.append("BEGIN BATCH ");
        cmd4.append(String.format(clusteringInsert, 1, 2, 3, 4, 1234));
        cmd4.append(String.format(clusteringStaticConditionalTTLUpdate, 5, 234, 1, 1, 1, 2, 22));
        cmd4.append("APPLY BATCH ");
        execute(cmd4.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 234),
                row(1, 1, 2, 3, 22, 23),
                row(1, 2, 3, 4, 22, 1234)
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, null),
                row(1, 1, 2, 3, 22, 23),
                row(1, 2, 3, 4, 22, 1234)
        );

        StringBuilder cmd5 = new StringBuilder();
        cmd5.append("BEGIN BATCH ");
        cmd5.append(String.format(clusteringRangeDelete, 1, 2));
        cmd5.append(String.format(clusteringStaticConditionalUpdate, 1234, 1, 1, 1, 2, 22));
        cmd5.append("APPLY BATCH ");
        execute(cmd5.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 1234),
                row(1, 1, 2, 3, 22, 23)
        );

        StringBuilder cmd6 = new StringBuilder();
        cmd6.append("BEGIN BATCH ");
        cmd6.append(String.format(clusteringUpdate, 345, 1, 3, 4, 5));
        cmd6.append(String.format(clusteringStaticConditionalUpdate, 1, 1, 1, 1, 2, 22));
        cmd6.append("APPLY BATCH ");
        execute(cmd6.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 1),
                row(1, 1, 2, 3, 22, 23),
                row(1, 3, 4, 5, 22, 345)
        );


        StringBuilder cmd7 = new StringBuilder();
        cmd7.append("BEGIN BATCH ");
        cmd7.append(String.format(clusteringDelete, 1, 3, 4, 5));
        cmd7.append(String.format(clusteringStaticConditionalUpdate, 2300, 1, 1, 2, 3, 1));  // SHOULD NOT MATCH
        cmd7.append("APPLY BATCH ");
        execute(cmd7.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 1),
                row(1, 1, 2, 3, 22, 23),
                row(1, 3, 4, 5, 22, 345)
        );

        StringBuilder cmd8 = new StringBuilder();
        cmd8.append("BEGIN BATCH ");
        cmd8.append(String.format(clusteringConditionalDelete, 1, 3, 4, 5, 345));
        cmd8.append(String.format(clusteringRangeDelete, 1, 1));
        cmd8.append(String.format(clusteringInsert, 1, 2, 3, 4, 5));
        cmd8.append("APPLY BATCH ");
        execute(cmd8.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5)
        );

        StringBuilder cmd9 = new StringBuilder();
        cmd9.append("BEGIN BATCH ");
        cmd9.append(String.format(clusteringConditionalInsert, 1, 3, 4, 5, 345));
        cmd9.append(String.format(clusteringDelete, 1, 2, 3, 4));
        cmd9.append("APPLY BATCH ");
        execute(cmd9.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, 345)
        );

        StringBuilder cmd10 = new StringBuilder();
        cmd10.append("BEGIN BATCH ");
        cmd10.append(String.format(clusteringTTLInsert, 1, 2, 3, 4, 5, 5));
        cmd10.append(String.format(clusteringConditionalTTLUpdate, 10, 5, 1, 3, 4, 5, 345));
        cmd10.append("APPLY BATCH ");
        execute(cmd10.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5), // 5 second TTL
                row(1, 3, 4, 5, 22, 5)  // 10 second TTL
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, 5) // now 4 second TTL
        );

        StringBuilder cmd11 = new StringBuilder();
        cmd11.append("BEGIN BATCH ");
        cmd11.append(String.format(clusteringConditionalTTLInsert, 1, 2, 3, 4, 5, 5));
        cmd11.append(String.format(clusteringInsert,1, 4, 5, 6, 7));
        cmd11.append("APPLY BATCH ");
        execute(cmd11.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5), // This one has 5 seconds left
                row(1, 3, 4, 5, 22, 5), // This one should have 4 seconds left
                row(1, 4, 5, 6, 22, 7) // This one has no TTL
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, null), // We had a row here before from cmd9, but we've ttl'd out the value in cmd11
                row(1, 4, 5, 6, 22, 7)
        );

        StringBuilder cmd12 = new StringBuilder();
        cmd12.append("BEGIN BATCH ");
        cmd12.append(String.format(clusteringConditionalTTLUpdate, 5, 5, 1, 3, 4, 5, null));
        cmd12.append(String.format(clusteringTTLUpdate, 5, 8, 1, 4, 5, 6));
        cmd12.append("APPLY BATCH ");
        execute(cmd12.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, 5),
                row(1, 4, 5, 6, 22, 8)
        );

        Thread.sleep(6000);
        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, null),
                row(1, 4, 5, 6, 22, null)
        );

        StringBuilder cmd13 = new StringBuilder();
        cmd13.append("BEGIN BATCH ");
        cmd13.append(String.format(clusteringStaticConditionalDelete, 1, 3, 4, 5, 22));
        cmd13.append(String.format(clusteringInsert, 1, 2, 3, 4, 5));
        cmd13.append("APPLY BATCH ");
        execute(cmd13.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5),
                row(1, 4, 5, 6, 22, null)
        );

        StringBuilder cmd14 = new StringBuilder();
        cmd14.append("BEGIN BATCH ");
        cmd14.append(String.format(clusteringStaticConditionalStaticUpdate, 23, 1, 22));
        cmd14.append(String.format(clusteringDelete, 1, 4, 5, 6));
        cmd14.append("APPLY BATCH ");
        execute(cmd14.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 23, 5)
        );
    }

}
