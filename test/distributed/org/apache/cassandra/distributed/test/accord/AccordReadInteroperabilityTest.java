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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class AccordReadInteroperabilityTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteroperabilityTest.class);

    @Nonnull
    private final TransactionalMode mode;

    private final boolean migrated;

    public AccordReadInteroperabilityTest(@Nonnull TransactionalMode mode, boolean migrated)
    {
        this.mode = mode;
        this.migrated = migrated;
    }

    @Parameterized.Parameters(name = "transactionalMode={0}, migrated={1}")
    public static Collection<Object[]> data() {
        List<Object[]> tests = new ArrayList<>(TransactionalMode.values().length * 2);
        for (TransactionalMode mode : TransactionalMode.values())
        {
            tests.add(new Object[]{ mode, true });
            tests.add(new Object[]{ mode, false });
        }
        return tests;
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder.withConfig(config -> config.set("accord.range_migration", "auto")
                                                                                  .set("paxos_variant", "v2")),
                                    3);
    }

    @After
    public void tearDown()
    {
        SHARED_CLUSTER.setMessageSink(null);
    }


    private String testTransactionSelect()
    {
        return "BEGIN TRANSACTION\n" +
               "  SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 0;\n" +
               "COMMIT TRANSACTION";
    }

    private String testSelect()
    {
        return "SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 0";
    }

    private String testRangeSelect()
    {
        return "SELECT * FROM " + qualifiedAccordTableName + " WHERE token(k) > token(0)";
    }

    @Test
    public void testTransactionStatementReadIsAtQuorum() throws Throwable
    {
        testReadIsAtQuorum(testTransactionSelect());
    }

    @Test
    public void testNonSerialReadIsAtQuorum() throws Throwable
    {
        testReadIsAtQuorum(testSelect());
    }

    @Test
    public void testSerialReadIsAtQuorum() throws Throwable
    {
        testReadIsAtQuorum(testSelect(), ConsistencyLevel.SERIAL);
    }

    @Test
    public void testRangeReadIsAtQuorum() throws Throwable
    {
        testReadIsAtQuorum(testRangeSelect());
    }

    private void testReadIsAtQuorum(String query) throws Throwable
    {
        testReadIsAtQuorum(query, ConsistencyLevel.QUORUM);
    }

    private void testReadIsAtQuorum(String query, org.apache.cassandra.distributed.api.ConsistencyLevel cl) throws Throwable
    {
        // Transaction statement doesn't work during migration
        if (query.equals(testTransactionSelect()) && !migrated)
            return;
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c))" + (migrated ? " WITH " + transactionalMode.asCqlParam() : ""),
             cluster -> {
                 SHARED_CLUSTER.setMessageSink(new MessageCountingSink(SHARED_CLUSTER, MessageCountingSink.EXCLUDE_SYNC_POINT_MESSAGES));
                 if (!migrated)
                 {
                     String alterCQL = "ALTER TABLE " + qualifiedAccordTableName + " WITH " + transactionalMode.asCqlParam();
                     if (transactionalMode == TransactionalMode.off)
                         alterCQL = alterCQL + " AND " + TransactionalMigrationFromMode.full.asCqlParam();
                     cluster.coordinator(1).execute(alterCQL, ConsistencyLevel.ALL);
                     if (transactionalMode == TransactionalMode.off)
                     {
                         nodetool(cluster.coordinator(1), "repair", "-skip-paxos", KEYSPACE, accordTableName);
                     }
                     else
                     {
                         nodetool(cluster.coordinator(1), "repair", "-skip-paxos", "-skip-accord", KEYSPACE, accordTableName);
                         nodetool(cluster.coordinator(1), "repair", "-full", "-skip-accord", KEYSPACE, accordTableName);
                     }
                 }
                 cluster.coordinator(1).execute(query, cl);
                 // Transactional modes that write through Accord never have a point where they need to run interop reads
                 // they go straight from not being able to read to being able to read from a single replica
                 if (!transactionalMode.ignoresSuppliedReadCL() && !transactionalMode.nonSerialWritesThroughAccord)
                 {
                     assertEquals(2, messageCount(Verb.ACCORD_INTEROP_STABLE_THEN_READ_REQ));
                     assertEquals(2, messageCount(Verb.ACCORD_INTEROP_READ_RSP));
                 }
                 else
                 {
                     // Tricky to check for regular commit because a lot of background Accord things create commits
                     assertEquals(0, messageCount(Verb.ACCORD_INTEROP_STABLE_THEN_READ_REQ));
                     assertEquals(0, messageCount(Verb.ACCORD_INTEROP_READ_REQ));
                     assertEquals(0, messageCount(Verb.ACCORD_INTEROP_READ_RSP));
                     assertTrue(messageCount(Verb.ACCORD_GET_EPHMRL_READ_DEPS_REQ) > 0);
                 }
             });
    }
}
