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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Splitter;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.messages.Commit;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

@SuppressWarnings("Convert2MethodRef")
public class AccordIntegrationTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordIntegrationTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @Test
    public void testRecovery() throws Exception
    {
        test(cluster -> {
            IMessageFilters.Filter lostApply = cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).drop();
            IMessageFilters.Filter lostCommit = cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).to(2).drop();

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT v FROM " + currentTable + " WHERE k=0 AND c=0);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row1 IS NULL THEN\n" +
                           "    INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            // row1.v shouldn't have existed when the txn's SELECT was executed
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, query);

            lostApply.off();
            lostCommit.off();

            // Querying again should trigger recovery...
            query = "BEGIN TRANSACTION\n" +
                    "  LET row1 = (SELECT v FROM " + currentTable + " WHERE k=0 AND c=0);\n" +
                    "  SELECT row1.v;\n" +
                    "  IF row1.v = 1 THEN\n" +
                    "    UPDATE " + currentTable + " SET v=2 WHERE k = 0 AND c = 0;\n" +
                    "  END IF\n" +
                    "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1 }, query);

            String check = "BEGIN TRANSACTION\n" +
                    "  SELECT * FROM " + currentTable + " WHERE k = ? AND c = ?;\n" +
                    "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 2}, check, 0, 0);

            query = "BEGIN TRANSACTION\n" +
                    "  LET row1 = (SELECT v FROM " + currentTable + " WHERE k=0 AND c=0);\n" +
                    "  SELECT row1.v;\n" +
                    "  IF row1 IS NULL THEN\n" +
                    "    INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 3);\n" +
                    "  END IF\n" +
                    "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 2 }, query);

            assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 2}, check, 0, 0);
        });
    }

    /*
    Sporadically fails with someone asking for a token() from SentinelKey, which apparently is unsupported.
    
    ERROR 19:07:47 Exception in thread Thread[AccordStage-1,5,SharedPool]
    java.lang.UnsupportedOperationException: null
	at org.apache.cassandra.service.accord.api.AccordRoutingKey$SentinelKey.token(AccordRoutingKey.java:152)
	at org.apache.cassandra.service.accord.api.AccordRoutingKey.routingHash(AccordRoutingKey.java:84)
	at accord.local.CommandStores$ShardedRanges.keyIndex(CommandStores.java:191)
	at accord.local.CommandStores$ShardedRanges.addKeyIndex(CommandStores.java:196)
	at accord.primitives.AbstractKeys.foldl(AbstractKeys.java:203)
	at accord.local.CommandStores$ShardedRanges.shards(CommandStores.java:179)
	at accord.local.CommandStores.mapReduce(CommandStores.java:426)
	at accord.local.CommandStores.mapReduceConsume(CommandStores.java:409)
	at accord.local.AsyncCommandStores.mapReduceConsume(AsyncCommandStores.java:66)
	at accord.local.Node.mapReduceConsumeLocal(Node.java:276)
	at accord.messages.PreAccept.process(PreAccept.java:90)
	at accord.messages.TxnRequest.process(TxnRequest.java:145)
	at org.apache.cassandra.service.accord.AccordVerbHandler.doVerb(AccordVerbHandler.java:46)
	at org.apache.cassandra.net.InboundSink.lambda$new$0(InboundSink.java:78)
     */
    @Ignore
    @Test
    public void multipleShards()
    {
        // can't reuse test() due to it using "int" for pk; this test needs "blob"
        String keyspace = "multipleShards";
        
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}");
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + keyspace + ".tbl (k blob, c int, v int, primary key (k, c))");
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().createEpochFromConfigUnsafe()));
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

        List<String> tokens = SHARED_CLUSTER.stream()
                                           .flatMap(i -> StreamSupport.stream(Splitter.on(",").split(i.config().getString("initial_token")).spliterator(), false))
                                           .collect(Collectors.toList());

        List<ByteBuffer> keys = tokens.stream()
                                      .map(t -> (Murmur3Partitioner.LongToken) Murmur3Partitioner.instance.getTokenFactory().fromString(t))
                                      .map(Murmur3Partitioner.LongToken::keyForToken)
                                      .collect(Collectors.toList());

        List<String> keyStrings = keys.stream().map(bb -> "0x" + ByteBufferUtil.bytesToHex(bb)).collect(Collectors.toList());
        StringBuilder query = new StringBuilder("BEGIN TRANSACTION\n");
        
        for (int i = 0; i < keys.size(); i++)
            query.append("  LET row" + i + " = (SELECT * FROM " + keyspace + ".tbl WHERE k=" + keyStrings.get(i) + " AND c=0);\n");

        query.append("  SELECT row0.v;\n")
             .append("  IF ");

        for (int i = 0; i < keys.size(); i++)
            query.append((i > 0 ? " AND row" : "row") + i + " IS NULL");

        query.append(" THEN\n");

        for (int i = 0; i < keys.size(); i++)
            query.append("    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (" + keyStrings.get(i) + ", 0, " + i +");\n");
        
        query.append("  END IF\n");
        query.append("COMMIT TRANSACTION");

        // row0.v shouldn't have existed when the txn's SELECT was executed
        assertRowEqualsWithPreemptedRetry(SHARED_CLUSTER, null, query.toString());

        SHARED_CLUSTER.get(1).runOnInstance(() -> {
            StringBuilder sb = new StringBuilder("BEGIN TRANSACTION\n");
            for (int i = 0; i < keyStrings.size(); i++)
                sb.append(String.format("LET row%d = (SELECT * FROM ks.tbl WHERE k=%s AND c=0);\n", i, keyStrings.get(i)));
            sb.append("COMMIT TRANSACTION");

            Unseekables<?, ?> routables = AccordTestUtils.createTxn(sb.toString()).keys().toUnseekables();
            Topologies topology = AccordService.instance().node.topology().withUnsyncedEpochs(routables, 1);
            // we don't detect out-of-bounds read/write yet, so use this to validate we reach different shards
            Assertions.assertThat(topology.totalShards()).isEqualTo(2);
        });

        String check = "BEGIN TRANSACTION\n" +
                       "  SELECT * FROM " + keyspace + ".tbl WHERE k = ? AND c = ?;\n" +
                       "COMMIT TRANSACTION";

        for (int i = 0; i < keys.size(); i++)
            assertRowEqualsWithPreemptedRetry(SHARED_CLUSTER, new Object[] { keys.get(i), 0, i}, check, keys.get(i), 0);
    }

    @Test
    public void testLostCommitReadTriggersFallbackRead() throws Exception
    {
        test(cluster -> {
            // It's expected that the required Read will happen regardless of whether this fails to return a read
            cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).messagesMatching((from, to, iMessage) -> cluster.get(from).callOnInstance(() -> {
                Message<?> msg = Instance.deserializeMessage(iMessage);
                if (msg.payload instanceof Commit)
                    return ((Commit) msg.payload).read != null;
                return false;
            })).drop();

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT * FROM " + currentTable + " WHERE k = 0 AND c = 0);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row1 IS NULL THEN\n" +
                           "    INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, query);

            String check = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + currentTable + " WHERE k = ? AND c = ?;\n" +
                           "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, 0, 1 }, check, 0, 0);
        });
    }

    @Test
    public void testMultiKeyQueryAndInsert() throws Throwable
    {
        test("CREATE TABLE " + currentTable + " (k int, c int, v int, primary key (k, c))",
             cluster -> 
             {
                 String query1 = "BEGIN TRANSACTION\n" +
                                 "  LET select1 = (SELECT * FROM " + currentTable + " WHERE k=0 AND c=0);\n" +
                                 "  LET select2 = (SELECT * FROM " + currentTable + " WHERE k=1 AND c=0);\n" +
                                 "  SELECT v FROM " + currentTable + " WHERE k=0 AND c=0;\n" +
                                 "  IF select1 IS NULL THEN\n" +
                                 "    INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 0);\n" +
                                 "    INSERT INTO " + currentTable + " (k, c, v) VALUES (1, 0, 0);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION";
                 assertEmptyWithPreemptedRetry(cluster, query1);

                 String check = "BEGIN TRANSACTION\n" +
                                "  SELECT * FROM " + currentTable + " WHERE k = ? AND c = ?;\n" +
                                "COMMIT TRANSACTION";
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 0}, check, 0, 0);
                 assertRowEqualsWithPreemptedRetry(cluster, new Object[] {1, 0, 0}, check, 1, 0);

                 String query2 = "BEGIN TRANSACTION\n" +
                                 "  LET select1 = (SELECT * FROM " + currentTable + " WHERE k=1 AND c=0);\n" +
                                 "  LET select2 = (SELECT * FROM " + currentTable + " WHERE k=2 AND c=0);\n" +
                                 "  SELECT v FROM " + currentTable + " WHERE k=1 AND c=0;\n" +
                                 "  IF select1.v = ? THEN\n" +
                                 "    INSERT INTO " + currentTable + " (k, c, v) VALUES (1, 0, 1);\n" +
                                 "    INSERT INTO " + currentTable + " (k, c, v) VALUES (2, 0, 1);\n" +
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
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE demo_ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2};");
        SHARED_CLUSTER.schemaChange("CREATE TABLE demo_ks.org_docs ( org_name text, doc_id int, contents_version int static, title text, permissions int, PRIMARY KEY (org_name, doc_id) );");
        SHARED_CLUSTER.schemaChange("CREATE TABLE demo_ks.org_users ( org_name text, user text, members_version int static, permissions int, PRIMARY KEY (org_name, user) );");
        SHARED_CLUSTER.schemaChange("CREATE TABLE demo_ks.user_docs ( user text, doc_id int, title text, org_name text, permissions int, PRIMARY KEY (user, doc_id) );");

        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().createEpochFromConfigUnsafe()));
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
                        "    UPDATE demo_ks.org_docs SET title='slides.key', permissions=777, contents_version += 1 WHERE org_name='demo' AND doc_id=101;\n" +
                        "    UPDATE demo_ks.user_docs SET title='slides.key', permissions=777 WHERE user='blake' AND doc_id=101;\n" +
                        "    UPDATE demo_ks.user_docs SET title='slides.key', permissions=777 WHERE user='scott' AND doc_id=101;\n" +
                        "  END IF\n" +
                        "COMMIT TRANSACTION";
        assertRowEqualsWithPreemptedRetry(SHARED_CLUSTER, new Object[] { 5 }, addDoc);

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
        assertRowEqualsWithPreemptedRetry(SHARED_CLUSTER, new Object[] { 6 }, addUser);
    }

//    @Test
//    public void acceptInvalidationTest()
//    {
//
//    }
//
//    @Test
//    public void applyAndCheckTest()
//    {
//
//    }
//
//    @Test
//    public void beginInvalidationTest()
//    {
//
//    }
//
//    @Test
//    public void checkStatusTest()
//    {
//
//    }
}
