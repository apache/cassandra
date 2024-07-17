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

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SimpleBuilders.MutationBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.lang.String.format;
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.config.CassandraRelevantProperties.HINT_DISPATCH_INTERVAL_MS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/*
 * Test that non-transactional updates have their timestamps preserved when written through Accord so that
 * `USING TIMESTAMP` continues to work and so that hints and batch log retry attempts are inserted with their
 * original timestamp and not a later Accord timestamp which could cause data resurrection.
 */
public class AccordTimestampPreservationTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTimestampPreservationTest.class);

    private static final int CLUSTERING_VALUE = 1;

    private static final String NORMAL_TABLE_FMT = "CREATE TABLE %s (id int, c int, v int, PRIMARY KEY ((id), c))";

    private static final String ACCORD_TABLE_FMT = NORMAL_TABLE_FMT + " WITH transactional_mode='full'";

    private static ICoordinator coordinator;

    private static final String expectedResult = "[[42]]";

    private static final int PKEY1 = 77;
    private static final int PKEY2 = 78;
    private static final int VALUE = 66;

    private static final long TIMESTAMP = 42;

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        HINT_DISPATCH_INTERVAL_MS.setLong(100);
        ServerTestUtils.daemonInitialization();
        // Otherwise repair complains if you don't specify a keyspace
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);
        AccordTestBase.setupCluster(builder -> builder.appendConfig(config -> config.set("write_request_timeout", "2s")), 3);
        ServerTestUtils.prepareServerNoRegister();
        coordinator = SHARED_CLUSTER.coordinator(1);
    }

    @After
    public void tearDown() throws Exception
    {
        unpauseBatchlog();
        deleteAllHints();
        unpauseHints();
        super.tearDown();
        // Reset migration state
        forEach(() -> {
            ConsensusRequestRouter.resetInstance();
            ConsensusKeyMigrationState.reset();
        });
        truncateSystemTables();
    }

    @Test
    public void testMutationPreservesTimestamp() throws Exception
    {
        test(createTables(ACCORD_TABLE_FMT, qualifiedAccordTableName), cluster -> {
            long startCount = getAccordCoordinateCount();
            coordinator.executeWithResult(insertCQL(qualifiedAccordTableName, PKEY1, VALUE), ALL);
            assertEquals(startCount + 1, getAccordWriteCount());
            int id = 1;
            for (IInvokableInstance instance : cluster)
            {
                logger.info("Checking instance " + id);
                id++;
                spinAssertEquals(expectedResult, () -> instance.executeInternalWithResult(checkCQL()).toString(), 20);
            }
        });
    }

    @Test
    public void testBatchlogPreservesTimestamp() throws Exception
    {
        test(ImmutableList.of(format(NORMAL_TABLE_FMT, qualifiedRegularTableName), format(ACCORD_TABLE_FMT, qualifiedAccordTableName)), cluster -> {
            pauseHints();
            blockMutationAndPreAccept(cluster);
            try
            {
                // Insert must span both Accord and non-Accord ranges or tables otherwise it bypasses the batchlog entirely
                coordinator.executeWithResult(batchInsert(true, PKEY1, PKEY2, VALUE), ALL);
                fail("Should have thrown WTE");
            }
            catch (Throwable t)
            {
                assertEquals(t.getClass().getName(), WriteTimeoutException.class.getName());
            }
            cluster.filters().reset();

            int id = 1;
            for (IInvokableInstance instance : cluster)
            {
                logger.info("Checking instance " + id);
                id++;
                spinAssertEquals(expectedResult, () -> instance.executeInternalWithResult(checkCQL()).toString(), 20);
            }
        });
    }

    @Test
    public void testHintsPreservesTimestamp() throws Exception
    {
        test(createTables(ACCORD_TABLE_FMT, qualifiedAccordTableName), cluster -> {
            String keyspace = KEYSPACE;
            int pkey1 = PKEY1;
            long timestamp = TIMESTAMP;
            int clustering = CLUSTERING_VALUE;
            String tableName = accordTableName;
            cluster.get(1).runOnInstance(() -> {
                ByteBuffer keyBuf = Int32Type.instance.fromString(Integer.toString(pkey1));
                DecoratedKey dk = DatabaseDescriptor.getPartitioner().decorateKey(keyBuf);
                MutationBuilder mutationBuilder = new MutationBuilder(KEYSPACE, dk);
                mutationBuilder.timestamp(timestamp);
                mutationBuilder.update(tableName).row(clustering).add("v", VALUE);
                Mutation m = mutationBuilder.build();
                ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(Keyspace.open(keyspace), ConsistencyLevel.ALL, dk.getToken(), ReplicaPlans.writeAll);
                for (Replica replica : plan.live().withoutSelf())
                    StorageProxy.submitHint(m, replica, null);
            });
            for (int i = 2; i <= 3; i++)
            {
                int instance = i;
                spinAssertEquals(expectedResult, () -> cluster.get(instance).executeInternalWithResult(checkCQL()).toString(), 20);
            }
        });
    }

    private String batchInsert(boolean logged, int pkey1, int pkey2, int value)
    {
        return batch(logged,
                     insertCQL(qualifiedAccordTableName, pkey1, value),
                     insertCQL(qualifiedRegularTableName, pkey2, value));
    }

    private String insertCQL(String table, int pkey, int value)
    {
        return insertCQL(table, pkey, value, false);
    }

    private String insertCQL(String table, int pkey, int value, boolean cas)
    {
        return format("INSERT INTO %s ( id, c, v ) VALUES ( %d, %d, %d )%s USING TIMESTAMP %d", table, pkey, CLUSTERING_VALUE, value, cas ? " IF NOT EXISTS" : "", TIMESTAMP);
    }

    private String checkCQL()
    {
        return format("SELECT WRITETIME(v) from %s WHERE id = %d", qualifiedAccordTableName, PKEY1);
    }
}
