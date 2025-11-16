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

import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.schema.SchemaConstants;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.db.SystemKeyspace.LOCAL;
import static org.apache.cassandra.distributed.impl.IsolatedExecutor.waitOn;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertNotNull;

public class AccordNodeForceRestartTest extends TestBaseImpl
{

    @Test
    public void testWithoutWriteAnyDataInStandalone() throws Throwable
    {
        testWithoutWriteAnyDataForNodeCount(1);
    }

    @Test
    public void testWithoutWriteAnyDataInCluster() throws Throwable
    {
        testWithoutWriteAnyDataForNodeCount(3);
    }

    public void testWithoutWriteAnyDataForNodeCount(int nodeCnt) throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(nodeCnt)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                             .set("accord.enabled", "true"))
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {

            String querySysTableCQL = String.format("select * from %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, LOCAL);

            assertSysTableNotNull(nodeCnt, cluster, querySysTableCQL);

            restartNode(cluster, getRandomIndex(nodeCnt));

            // restart the server and assert the data integerity
            assertSysTableNotNull(nodeCnt, cluster, querySysTableCQL);
        }
    }

    private static void assertSysTableNotNull(int nodeCnt, Cluster cluster, String querySysTableCQL)
    {
        for (int i = 1; i <= nodeCnt; i++)
        {
            assertNotNull("system.local table should not be null", cluster.coordinator(i).execute(querySysTableCQL, ConsistencyLevel.QUORUM));
        }
    }

    @Test
    public void testWithWriteSomeDataInStandalone() throws Throwable
    {
        testWithWriteSomeDataForNodeCount(1);
    }

    @Test
    public void testWithWriteSomeDataInCluster() throws Throwable
    {
        testWithWriteSomeDataForNodeCount(3);
    }

    public void testWithWriteSomeDataForNodeCount(int nodeCnt) throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(nodeCnt)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                             .set("accord.enabled", "true"))
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor': " + nodeCnt + "}");
            cluster.schemaChange("CREATE TABLE ks.accounts (\n" +
                                 "    account_holder text,\n" +
                                 "    account_balance bigint,\n" +
                                 "    PRIMARY KEY (account_holder)\n" +
                                 ") WITH transactional_mode = 'full'");

            long oneInitMoney = 10000;
            long anotherInitMoney = 10000;
            long sum = oneInitMoney + anotherInitMoney;
            int coordinatorNode = getRandomIndex(nodeCnt);
            String insertCQLForOne = "INSERT INTO ks.accounts(account_holder, account_balance) VALUES ('alice', " + oneInitMoney + ")";
            cluster.coordinator(coordinatorNode).executeWithResult(insertCQLForOne, ConsistencyLevel.QUORUM);
            String insertCQLForAnother = "INSERT INTO ks.accounts(account_holder, account_balance) VALUES ('bob', " + anotherInitMoney + ")";
            cluster.coordinator(coordinatorNode).executeWithResult(insertCQLForAnother, ConsistencyLevel.QUORUM);

            transferMoney(cluster, coordinatorNode);
            assertBankAccount(cluster, nodeCnt, sum);

            restartNode(cluster, getRandomIndex(nodeCnt));

            // restart the server and assert the data integerity
            assertBankAccount(cluster, nodeCnt, sum);
        }
    }

    public static int getRandomIndex(int nodeCnt) {
        return ThreadLocalRandom.current().nextInt(1,  nodeCnt + 1);
    }

    private void transferMoney(Cluster cluster, int coordinatorNode)
    {
        int transferCnt = 30;
        for (int i = 1; i <= transferCnt; i++)
        {
            int transferMoney = 50;
            String txnCQL = "BEGIN TRANSACTION\n" +
                            "    LET fromBalance = (SELECT account_balance FROM ks.accounts WHERE account_holder='alice');\n" +
                            "\n" +
                            "    IF fromBalance.account_balance >= " + transferMoney + " THEN\n" +
                            "        UPDATE ks.accounts SET account_balance -= " + transferMoney + " WHERE account_holder='alice';\n" +
                            "        UPDATE ks.accounts SET account_balance += " + transferMoney + " WHERE account_holder='bob';\n" +
                            "    END IF\n" +
                            "COMMIT TRANSACTION";

            cluster.coordinator(coordinatorNode).execute(txnCQL, ConsistencyLevel.QUORUM);
        }
    }

    /***
     * The balances of both accounts must remain non-negative, and their total sum must remain constant.
     * **/
    private void assertBankAccount(Cluster cluster, int nodeCnt, long sum)
    {
        for (int i = 1; i <= nodeCnt; i++)
        {
            SimpleQueryResult resultForOne = cluster.coordinator(i).executeWithResult("SELECT account_balance FROM ks.accounts WHERE account_holder='alice'",
                                                                                      ConsistencyLevel.QUORUM);
            long oneEndMoney = (Long) resultForOne.toObjectArrays()[0][0];
            Assertions.assertThat(oneEndMoney).isGreaterThanOrEqualTo(0);

            SimpleQueryResult resultForAnother = cluster.coordinator(i).executeWithResult("SELECT account_balance FROM ks.accounts WHERE account_holder='bob'",
                                                                                          ConsistencyLevel.QUORUM);
            long anotherEndMoney = (Long) resultForAnother.toObjectArrays()[0][0];
            Assertions.assertThat(anotherEndMoney).isGreaterThanOrEqualTo(0);
            Assertions.assertThat(oneEndMoney + anotherEndMoney).isEqualTo(sum);
        }
    }

    private static void restartNode(Cluster cluster, int nodeIndex)
    {
        waitOn(cluster.get(nodeIndex).shutdown());
        cluster.get(nodeIndex).config().set("accord.enabled", "true");
        cluster.get(nodeIndex).startup();
    }

    // Don't closeAllJournalSegments and persist the journal meta file to simulate the forced shoutdown
    public static class BB
    {
        public static void install(ClassLoader cl, Integer node)
        {
            new ByteBuddy().rebase(Journal.class)
                           .method(named("closeAllJournalSegmentsEnabled"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Boolean closeAllJournalSegmentsEnabled()
        {
            return false;
        }
    }
}
