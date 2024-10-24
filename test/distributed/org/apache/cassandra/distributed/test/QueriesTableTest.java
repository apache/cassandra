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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

import accord.impl.progresslog.DefaultProgressLogs;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.utils.Throwables;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.junit.Assert.assertTrue;

public class QueriesTableTest extends TestBaseImpl
{
    private static Cluster SHARED_CLUSTER;
    private static com.datastax.driver.core.Cluster DRIVER_CLUSTER;
    private static Session SESSION;

    @BeforeClass
    public static void createCluster() throws IOException
    {
        SHARED_CLUSTER = init(Cluster.build(1).withInstanceInitializer(QueryDelayHelper::install)
                                              .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)
                                                                .set("write_request_timeout", "10s")
                                                                .set("transaction_timeout", "15s")).start());

        DRIVER_CLUSTER = JavaDriverUtils.create(SHARED_CLUSTER);
        SESSION = DRIVER_CLUSTER.connect();
    }

    @AfterClass
    public static void closeCluster()
    {
        if (SESSION != null)
            SESSION.close();

        if (DRIVER_CLUSTER != null)
            DRIVER_CLUSTER.close();

        if (SHARED_CLUSTER != null)
            SHARED_CLUSTER.close();
    }

    @Test
    public void shouldExposeReadsAndWrites() throws Throwable
    {
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int primary key, v int)");
        SESSION.executeAsync("INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (0, 0)");
        SESSION.executeAsync("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 0");

        // Wait until the coordinator/local read and write are visible:
        Awaitility.await()
                  .atMost(60, SECONDS)
                  .pollInterval(1, SECONDS)
                  .dontCatchUncaughtExceptions()
                  .untilAsserted(QueriesTableTest::assertReadsAndWritesVisible);

        // Issue another read and write to unblock the original queries in progress:
        SESSION.execute("INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (0, 0)");
        SESSION.execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 0");

        waitForQueriesToFinish();
    }

    private static void assertReadsAndWritesVisible()
    {
        SimpleQueryResult result = SHARED_CLUSTER.get(1).executeInternalWithResult("SELECT * FROM system_views.queries");

        boolean readVisible = false;
        boolean coordinatorReadVisible = false;
        boolean writeVisible = false;
        boolean coordinatorWriteVisible = false;

        while (result.hasNext())
        {
            Row row = result.next();
            String threadId = row.get("thread_id").toString();
            String task = row.get("task").toString();

            readVisible |= threadId.contains("Read") && task.contains("SELECT");
            coordinatorReadVisible |= threadId.contains("Native-Transport-Requests") && task.contains("SELECT");
            writeVisible |= threadId.contains("Mutation") && task.contains("Mutation");
            coordinatorWriteVisible |= threadId.contains("Native-Transport-Requests") && task.contains("INSERT");
        }

        assertTrue(readVisible);
        assertTrue(coordinatorReadVisible);
        assertTrue(writeVisible);
        assertTrue(coordinatorWriteVisible);
    }

    @Test
    public void shouldExposeCAS() throws Throwable
    {
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".cas_tbl (k int primary key, v int)");
        SESSION.executeAsync("UPDATE " + KEYSPACE + ".cas_tbl SET v = 10 WHERE k = 0 IF v = 0");

        // Wait until the coordinator update and local read required by the CAS operation are visible:
        Awaitility.await()
                  .atMost(60, SECONDS)
                  .pollInterval(1, SECONDS)
                  .dontCatchUncaughtExceptions()
                  .untilAsserted(QueriesTableTest::assertCasVisible);

        // Issue a read to unblock the read generated by the original CAS operation:
        SESSION.executeAsync("SELECT * FROM " + KEYSPACE + ".cas_tbl WHERE k = 0");

        waitForQueriesToFinish();
    }

    private static void assertCasVisible()
    {
        SimpleQueryResult result = SHARED_CLUSTER.get(1).executeInternalWithResult("SELECT * FROM system_views.queries");

        boolean readVisible = false;
        boolean coordinatorUpdateVisible = false;

        while (result.hasNext())
        {
            Row row = result.next();
            String threadId = row.get("thread_id").toString();
            String task = row.get("task").toString();

            readVisible |= threadId.contains("Read") && task.contains("SELECT");
            coordinatorUpdateVisible |= threadId.contains("Native-Transport-Requests") && task.contains("UPDATE");
        }

        assertTrue(readVisible);
        assertTrue(coordinatorUpdateVisible);
    }

    @Test
    public void shouldExposeTransaction() throws Throwable
    {
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".accord_tbl (k int primary key, v int)  WITH transactional_mode='mixed_reads'");

        // Disable recovery to make sure only one local read occurs:
        for (IInvokableInstance instance : SHARED_CLUSTER)
            instance.runOnInstance(() -> DefaultProgressLogs.unsafePauseForTesting(true));

        String update = "BEGIN TRANSACTION\n" +
                        "  LET row1 = (SELECT * FROM " + KEYSPACE + ".accord_tbl WHERE k = 0);\n" +
                        "  SELECT row1.k, row1.v;\n" +
                        "  IF row1.v = 0 THEN\n" +
                        "    UPDATE " + KEYSPACE + ".accord_tbl SET v = 10 WHERE k = 0;\n" +
                        "  END IF\n" +
                        "COMMIT TRANSACTION";
        
        SESSION.executeAsync(update);

        // Wait until the coordinator update and local read required by the CAS operation are visible:
        Awaitility.await()
                  .atMost(60, SECONDS)
                  .pollInterval(1, SECONDS)
                  .dontCatchUncaughtExceptions()
                  .untilAsserted(QueriesTableTest::assertTransactionVisible);

        // Issue a read to unblock the read generated by the original CAS operation:
        SESSION.executeAsync("SELECT * FROM " + KEYSPACE + ".accord_tbl WHERE k = 0");

        waitForQueriesToFinish();
    }

    private static void assertTransactionVisible()
    {
        SimpleQueryResult queries = SHARED_CLUSTER.get(1).executeInternalWithResult("SELECT * FROM system_views.queries");

        boolean readVisible = false;
        boolean coordinatorTxnVisible = false;

        while (queries.hasNext())
        {
            Row row = queries.next();
            String threadId = row.get("thread_id").toString();
            String task = row.get("task").toString();

            readVisible |= threadId.contains("Read") && task.contains("SELECT");
            coordinatorTxnVisible |= threadId.contains("Native-Transport-Requests") && task.contains("BEGIN TRANSACTION");
        }

        assertTrue(readVisible);
        assertTrue(coordinatorTxnVisible);
    }

    private static void waitForQueriesToFinish() throws InterruptedException
    {
        // Continue to query the "queries" table until nothing is in progress...
        SimpleQueryResult result = SHARED_CLUSTER.get(1).executeInternalWithResult("SELECT * FROM system_views.queries");
        while (result.hasNext())
        {
            SECONDS.sleep(1);
            result = SHARED_CLUSTER.get(1).executeInternalWithResult("SELECT * FROM system_views.queries");
        }
    }

    @SuppressWarnings("resource")
    public static class QueryDelayHelper
    {
        private static final CyclicBarrier readBarrier = new CyclicBarrier(2);
        private static final CyclicBarrier writeBarrier = new CyclicBarrier(2);

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(Mutation.class)
                           .method(named("apply").and(takesArguments(3)))
                           .intercept(MethodDelegation.to(QueryDelayHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("executeLocally").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(QueryDelayHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void apply(Keyspace keyspace, boolean durableWrites, boolean isDroppable, @SuperCall Callable<Void> zuper)
        {
            try
            {
                if (keyspace.getName().contains(KEYSPACE))
                    writeBarrier.await();

                zuper.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController,
                                                                 @SuperCall Callable<UnfilteredPartitionIterator> zuper)
        {
            try
            {
                if (executionController.metadata().keyspace.contains(KEYSPACE))
                    readBarrier.await();

                return zuper.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
