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


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteTest extends SchemaLoader
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;
    private static PreparedStatement pstmtI;
    private static PreparedStatement pstmtU;
    private static PreparedStatement pstmtD;
    private static PreparedStatement pstmt1;
    private static PreparedStatement pstmt2;
    private static PreparedStatement pstmt3;
    private static PreparedStatement pstmt4;
    private static PreparedStatement pstmt5;

    @BeforeClass
    public static void setup() throws Exception
    {
        Schema.instance.clear();

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("drop keyspace if exists junit;");
        session.execute("create keyspace junit WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };");
        session.execute("CREATE TABLE junit.tpc_base (\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");");
        session.execute("CREATE TABLE junit.tpc_inherit_a (\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_a text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");");
        session.execute("CREATE TABLE junit.tpc_inherit_b (\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_b text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");");
        session.execute("CREATE TABLE junit.tpc_inherit_b2 (\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_b text ,\n" +
                "  inh_b2 text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");");
        session.execute("CREATE TABLE junit.tpc_inherit_c (\n" +
                "  id int ,\n" +
                "  cid int ,\n" +
                "  inh_c text ,\n" +
                "  val text ,\n" +
                "  PRIMARY KEY ( ( id ), cid )\n" +
                ");");

        pstmtI = session.prepare("insert into junit.tpc_inherit_b ( id, cid, inh_b, val) values (?, ?, ?, ?)");
        pstmtU = session.prepare("update junit.tpc_inherit_b set inh_b=?, val=? where id=? and cid=?");
        pstmtD = session.prepare("delete from junit.tpc_inherit_b where id=? and cid=?");
        pstmt1 = session.prepare("select id, cid, val from junit.tpc_base where id=? and cid=?");
        pstmt2 = session.prepare("select id, cid, inh_a, val from junit.tpc_inherit_a where id=? and cid=?");
        pstmt3 = session.prepare("select id, cid, inh_b, val from junit.tpc_inherit_b where id=? and cid=?");
        pstmt4 = session.prepare("select id, cid, inh_b, inh_b2, val from junit.tpc_inherit_b2 where id=? and cid=?");
        pstmt5 = session.prepare("select id, cid, inh_c, val from junit.tpc_inherit_c where id=? and cid=?");
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        cluster.close();
    }

    @Test
    public void lostDeletesTest()
    {

        for (int i = 0; i < 500; i++)
        {
            session.execute(pstmtI.bind(1, 1, "inhB", "valB"));

            ResultSetFuture[] futures = load();

            Assert.assertTrue(futures[0].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[1].getUninterruptibly().isExhausted());
            Assert.assertNotNull(futures[2].getUninterruptibly().one());
            Assert.assertTrue(futures[3].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[4].getUninterruptibly().isExhausted());

            session.execute(pstmtU.bind("inhBu", "valBu", 1, 1));

            futures = load();

            Assert.assertTrue(futures[0].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[1].getUninterruptibly().isExhausted());
            Assert.assertNotNull(futures[2].getUninterruptibly().one());
            Assert.assertTrue(futures[3].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[4].getUninterruptibly().isExhausted());

            session.execute(pstmtD.bind(1, 1));

            futures = load();

            Assert.assertTrue(futures[0].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[1].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[2].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[3].getUninterruptibly().isExhausted());
            Assert.assertTrue(futures[4].getUninterruptibly().isExhausted());
        }
    }

    private ResultSetFuture[] load() {
        return new ResultSetFuture[]{
                session.executeAsync(pstmt1.bind(1, 1)),
                session.executeAsync(pstmt2.bind(1, 1)),
                session.executeAsync(pstmt3.bind(1, 1)),
                session.executeAsync(pstmt4.bind(1, 1)),
                session.executeAsync(pstmt5.bind(1, 1))
        };
    }
}
