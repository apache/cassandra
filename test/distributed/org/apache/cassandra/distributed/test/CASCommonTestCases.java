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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_PROPOSE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PROPOSE_REQ;

public abstract class CASCommonTestCases extends CASTestBase
{
    protected abstract Cluster getCluster();

    @Test
    public void simpleUpdate() throws Throwable
    {
        String tableName = tableName();
        String fullTableName = KEYSPACE + "." + tableName;
        getCluster().schemaChange("CREATE TABLE " + fullTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        getCluster().coordinator(1).execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
        assertRows(getCluster().coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL),
                   row(1, 1, 1));
        getCluster().coordinator(1).execute("UPDATE " + fullTableName + " SET v = 3 WHERE pk = 1 and ck = 1 IF v = 2", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
        assertRows(getCluster().coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL),
                   row(1, 1, 1));
        getCluster().coordinator(1).execute("UPDATE " + fullTableName + " SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
        assertRows(getCluster().coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL),
                   row(1, 1, 2));
    }

    @Test
    public void incompletePrepare() throws Throwable
    {
        String tableName = tableName();
        String fullTableName = KEYSPACE + "." + tableName;
        getCluster().schemaChange("CREATE TABLE " + fullTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        IMessageFilters.Filter drop = getCluster().filters().verbs(PAXOS2_PREPARE_REQ.id, PAXOS_PREPARE_REQ.id).from(1).to(2, 3).drop();
        try
        {
            getCluster().coordinator(1).execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
            Assert.assertTrue(false);
        }
        catch (RuntimeException t)
        {
            if (!t.getClass().getName().equals(CasWriteTimeoutException.class.getName()))
                throw new AssertionError(t);
        }
        drop.off();
        getCluster().coordinator(1).execute("UPDATE " + fullTableName + " SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
        assertRows(getCluster().coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL));
    }

    @Test
    public void incompletePropose() throws Throwable
    {
        String tableName = tableName();
        String fullTableName = KEYSPACE + "." + tableName;
        getCluster().schemaChange("CREATE TABLE " + fullTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        IMessageFilters.Filter drop1 = getCluster().filters().verbs(PAXOS2_PROPOSE_REQ.id, PAXOS_PROPOSE_REQ.id).from(1).to(2, 3).drop();
        try
        {
            getCluster().coordinator(1).execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
            Assert.assertTrue(false);
        }
        catch (RuntimeException t)
        {
            if (!t.getClass().getName().equals(CasWriteTimeoutException.class.getName()))
                throw new AssertionError(t);
        }
        drop1.off();
        // make sure we encounter one of the in-progress proposals so we complete it
        drop(getCluster(), 1, to(2), to(), to());
        getCluster().coordinator(1).execute("UPDATE " + fullTableName + " SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
        assertRows(getCluster().coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL),
                   row(1, 1, 2));
    }

    @Test
    public void incompleteCommit() throws Throwable
    {
        String tableName = tableName();
        String fullTableName = KEYSPACE + "." + tableName;
        getCluster().schemaChange("CREATE TABLE " + fullTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        try (AutoCloseable drop = drop(getCluster(), 1, to(), to(), to(2, 3)))
        {
            getCluster().coordinator(1).execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
            Assert.assertTrue(false);
        }
        catch (RuntimeException t)
        {
            if (!t.getClass().getName().equals(CasWriteTimeoutException.class.getName()))
                throw new AssertionError(t);
        }

        // make sure we see one of the successful commits
        try (AutoCloseable drop = drop(getCluster(), 1, to(2), to(2), to()))
        {
            getCluster().coordinator(1).execute("UPDATE " + fullTableName + " SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
            assertRows(getCluster().coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL),
                       row(1, 1, 2));
        }
    }

    /**
     *  - Prepare A to {1, 2, 3}
     *  - Propose A to {1}
     */
    @Test
    public void testRepairIncompletePropose() throws Throwable
    {
        String tableName = tableName();
        String fullTableName = KEYSPACE + "." + tableName;
        getCluster().schemaChange("CREATE TABLE " + fullTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        for (int repairWithout = 1 ; repairWithout <= 3 ; ++repairWithout)
        {
            try (AutoCloseable drop = drop(getCluster(), 1, to(), to(2, 3), to()))
            {
                getCluster().coordinator(1).execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, repairWithout);
                Assert.assertTrue(false);
            }
            catch (RuntimeException t)
            {
                if (!t.getClass().getName().equals(CasWriteTimeoutException.class.getName()))
                    throw new AssertionError(t);
            }
            int repairWith = repairWithout == 3 ? 2 : 3;
            repair(getCluster(), tableName, repairWithout, repairWith, repairWithout);

            try (AutoCloseable drop = drop(getCluster(), repairWith, to(repairWithout), to(), to()))
            {
                Object[][] rows = getCluster().coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = ?", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, repairWithout);
                if (repairWithout == 1) assertRows(rows); // invalidated
                else assertRows(rows, row(repairWithout, 1, 1)); // finished
            }
        }
    }

    /**
     *  - Prepare A to {1, 2, 3}
     *  - Propose A to {1, 2}
     *  -  Commit A to {1}
     *  - Repair using {2, 3}
     */
    @Test
    public void testRepairIncompleteCommit() throws Throwable
    {
        String tableName = tableName();
        String fullTableName = KEYSPACE + "." + tableName;
        getCluster().schemaChange("CREATE TABLE " + fullTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        for (int repairWithout = 1 ; repairWithout <= 3 ; ++repairWithout)
        {
            try (AutoCloseable drop = drop(getCluster(), 1, to(), to(3), to(2, 3)))
            {
                getCluster().coordinator(1).execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, repairWithout);
                Assert.assertTrue(false);
            }
            catch (RuntimeException t)
            {
                if (!t.getClass().getName().equals(CasWriteTimeoutException.class.getName()))
                    throw new AssertionError(t);
            }

            int repairWith = repairWithout == 3 ? 2 : 3;
            repair(getCluster(), tableName, repairWithout, repairWith, repairWithout);
            try (AutoCloseable drop = drop(getCluster(), repairWith, to(repairWithout), to(), to()))
            {
                //TODO dtest api is missing one with message?  booo... removed "" + repairWithout,
                assertRows(getCluster().coordinator(repairWith).execute("SELECT * FROM " + fullTableName + " WHERE pk = ?", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, repairWithout),
                           row(repairWithout, 1, 1));
            }
        }
    }
}
