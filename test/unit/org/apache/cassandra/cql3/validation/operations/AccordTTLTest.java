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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test was authored entirely by LLM
 *
 * TTL (and, closely related, local deletion time and list cell path) coverage for tables using Accord
 * ({@code transactional_mode = 'full'}).
 * <p>
 * Each write path into Accord is exercised: plain (non-transactional) statements, explicit transactions, transactions
 * whose writes depend on reads (LET references), conditional transactions and CAS (LWT) statements.
 * <p>
 * Two kinds of check are performed: user-visible behaviour (the data is visible, {@code TTL()} reports the expected
 * value, the data disappears once the TTL has elapsed), and the raw local expiration / deletion times stored by the
 * replica, which must be relative to the time the write was actually applied rather than an arbitrary epoch.
 * <p>
 * Note that explicit transactions may not specify {@code USING TTL}, so they are tested via {@code default_time_to_live}.
 */
public class AccordTTLTest extends CQLTester
{
    private static final int TTL = 10000;
    // generous allowance for the test to run slowly; anything near 1970 will be off by decades
    private static final int SLACK_SECONDS = 120;

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    private void createAccordTable()
    {
        createAccordTable("");
    }

    private void createAccordTable(String extraOptions)
    {
        createTable("CREATE TABLE %s (k int, c int, v int, s int static, l list<int>, m map<int, int>, PRIMARY KEY (k, c)) " +
                    "WITH transactional_mode = 'full'" + extraOptions);
    }

    private static String txn(String... statements)
    {
        StringBuilder sb = new StringBuilder("BEGIN TRANSACTION\n");
        for (String statement : statements)
            sb.append("  ").append(statement).append('\n');
        return sb.append("COMMIT TRANSACTION").toString();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Plain (non-transactional) statements routed through Accord
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testPlainInsertUsingTTL()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TTL " + TTL);
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testPlainUpdateUsingTTL()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("UPDATE %s USING TTL " + TTL + " SET v = 0 WHERE k = 0 AND c = 0");
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testPlainInsertDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testPlainDelete()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        exec("DELETE FROM %s WHERE k = 0 AND c = 0");
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0 AND c = 0"));
        assertLocalDeletionTimes(0, start);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Explicit transactions. These may not specify USING TTL, so TTLs can only be applied via default_time_to_live.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testTxnRejectsUsingTTL()
    {
        createAccordTable();
        assertRejected(txn("INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TTL " + TTL + ";"),
                       "Updates within transactions may not specify custom ttls");
        assertRejected(txn("UPDATE %s USING TTL " + TTL + " SET v = 0 WHERE k = 0 AND c = 0;"),
                       "Updates within transactions may not specify custom ttls");
    }

    @Test
    public void testTxnInsertDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        long start = FBUtilities.nowInSeconds();
        exec(txn("INSERT INTO %s (k, c, v) VALUES (0, 0, 0);"));
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testTxnUpdateDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        long start = FBUtilities.nowInSeconds();
        exec(txn("UPDATE %s SET v = 0 WHERE k = 0 AND c = 0;"));
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testTxnUpdateStaticDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        long start = FBUtilities.nowInSeconds();
        exec(txn("UPDATE %s SET s = 0 WHERE k = 0;"));
        assertRowsNet(exec("SELECT s FROM %s WHERE k = 0"), row(0));
        assertTTLWithin(exec("SELECT ttl(s) FROM %s WHERE k = 0").one().getInt(0), TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testTxnUpdateCollectionsDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        long start = FBUtilities.nowInSeconds();
        exec(txn("UPDATE %s SET l = [1, 2], m = {1: 1} WHERE k = 0 AND c = 0;"));
        assertRowsNet(exec("SELECT l, m FROM %s WHERE k = 0 AND c = 0"), row(list(1, 2), map(1, 1)));
        assertLocalExpiration(0, TTL, start);
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testTxnSelectReturnsTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        exec(txn("INSERT INTO %s (k, c, v) VALUES (0, 0, 0);"));
        com.datastax.driver.core.Row row = exec(txn("SELECT v, ttl(v) FROM %s WHERE k = 0 AND c = 0;")).one();
        assertNotNull("Row written with a TTL was not visible to a transaction", row);
        assertEquals(0, row.getInt(0));
        assertTTLWithin(row.getInt(1), TTL);
    }

    @Test
    public void testTxnSelectReturnsTTLOfPlainWrite()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TTL " + TTL);
        com.datastax.driver.core.Row row = exec(txn("SELECT v, ttl(v) FROM %s WHERE k = 0 AND c = 0;")).one();
        assertNotNull("Row written with a TTL was not visible to a transaction", row);
        assertEquals(0, row.getInt(0));
        assertTTLWithin(row.getInt(1), TTL);
    }

    @Test
    public void testTxnReferenceUpdateDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 42) USING TTL 0");
        long start = FBUtilities.nowInSeconds();
        exec(txn("LET row0 = (SELECT v FROM %1$s WHERE k = 0 AND c = 0);",
                 "UPDATE %1$s SET v = row0.v WHERE k = 1 AND c = 0;"));
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 1 AND c = 0"), row(42));
        assertTTLWithin(exec("SELECT ttl(v) FROM %s WHERE k = 1 AND c = 0").one().getInt(0), TTL);
        assertLocalExpiration(1, TTL, start);
    }

    @Test
    public void testTxnReferenceUpdateMixedWithPlainAssignmentDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 42) USING TTL 0");
        long start = FBUtilities.nowInSeconds();
        exec(txn("LET row0 = (SELECT v FROM %1$s WHERE k = 0 AND c = 0);",
                 "UPDATE %1$s SET v = row0.v, s = 7 WHERE k = 1 AND c = 0;"));
        assertRowsNet(exec("SELECT v, s FROM %s WHERE k = 1 AND c = 0"), row(42, 7));
        assertLocalExpiration(1, TTL, start);
    }

    @Test
    public void testTxnReferenceUpdateNoTTL()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 42)");
        exec(txn("LET row0 = (SELECT v FROM %1$s WHERE k = 0 AND c = 0);",
                 "UPDATE %1$s SET v = row0.v WHERE k = 1 AND c = 0;"));
        assertRowsNet(exec("SELECT v, ttl(v) FROM %s WHERE k = 1 AND c = 0"), row(42, null));
    }

    @Test
    public void testConditionalTxnInsertDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        long start = FBUtilities.nowInSeconds();
        exec(txn("LET row0 = (SELECT v FROM %1$s WHERE k = 0 AND c = 0);",
                 "IF row0 IS NULL THEN",
                 "  INSERT INTO %1$s (k, c, v) VALUES (0, 0, 0);",
                 "END IF"));
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testTxnDefaultTTLExpires()
    {
        int ttl = 2;
        createAccordTable(" AND default_time_to_live = " + ttl);
        exec(txn("INSERT INTO %s (k, c, v) VALUES (0, 0, 0);"));
        // must be visible immediately after the write...
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0 AND c = 0"), row(0));
        // ...and must disappear, both to transactional and non-transactional reads, once the TTL elapses
        awaitExpiry(ttl);
    }

    @Test
    public void testPlainTTLExpires()
    {
        int ttl = 2;
        createAccordTable();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TTL " + ttl);
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0 AND c = 0"), row(0));
        awaitExpiry(ttl);
    }

    private void awaitExpiry(int ttl)
    {
        Awaitility.await().atMost(ttl + 30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
                  .until(() -> exec("SELECT v FROM %s WHERE k = 0 AND c = 0").all().isEmpty());
        assertTrue(exec(txn("SELECT v FROM %s WHERE k = 0 AND c = 0;")).all().isEmpty());
    }

    @Test
    public void testTxnDeleteRow()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        exec(txn("DELETE FROM %s WHERE k = 0 AND c = 0;"));
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0 AND c = 0"));
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testTxnDeleteCell()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        exec(txn("DELETE v FROM %s WHERE k = 0 AND c = 0;"));
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0 AND c = 0"), row((Integer) null));
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testTxnDeletePartition()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        exec(txn("DELETE FROM %s WHERE k = 0;"));
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0"));
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testTxnSetCollection()
    {
        // overwriting a collection writes a complex deletion along with the new cells
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec(txn("UPDATE %s SET l = [1], m = {1: 1} WHERE k = 0 AND c = 0;"));
        assertRowsNet(exec("SELECT l, m FROM %s WHERE k = 0 AND c = 0"), row(list(1), map(1, 1)));
        assertLocalDeletionTimes(0, start);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // CAS (LWT) statements, which are executed by Accord for transactional_mode = 'full'
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testCasInsertUsingTTL()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0) IF NOT EXISTS USING TTL " + TTL), row(true));
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    @Test
    public void testCasUpdateUsingTTL()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET v = 1 WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0 AND c = 0"), row(1));
        assertTTLWithin(exec("SELECT ttl(v) FROM %s WHERE k = 0 AND c = 0").one().getInt(0), TTL);
    }

    @Test
    public void testCasInsertDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0) IF NOT EXISTS"), row(true));
        assertVisibleWithTTL(0, 0, TTL);
        assertLocalExpiration(0, TTL, start);
    }

    // List operations that require a read are evaluated when the transaction executes (as a TxnReferenceOperation),
    // as are those that require a timestamp if the statement has any operation requiring a read (see
    // ModificationStatement.forTxn), so their TTL must be carried by TxnWrite.Fragment rather than the base update

    @Test
    public void testCasListSetByIndexUsingTTL()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2])");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET l[0] = 5 WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(5, 2)));
        assertListElementTTL(0, 5, TTL, start);
        assertListElementTTL(0, 2, LivenessInfo.NO_TTL, start);
    }

    @Test
    public void testCasListSetByIndexUsingTTLZeroOverridesDefault()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2])");
        assertRowsNet(exec("UPDATE %s USING TTL 0 SET l[0] = 5 WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(5, 2)));
        assertListElementTTL(0, 5, LivenessInfo.NO_TTL, 0);
    }

    @Test
    public void testCasListSetByIndexDefaultTTL()
    {
        createAccordTable(" AND default_time_to_live = " + TTL);
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2]) USING TTL 0");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("UPDATE %s SET l[0] = 5 WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertListElementTTL(0, 5, TTL, start);
        assertListElementTTL(0, 2, LivenessInfo.NO_TTL, start);
    }

    @Test
    public void testCasListSetByIndexUsingBoundTTL()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2])");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("UPDATE %s USING TTL ? SET l[0] = 5 WHERE k = 0 AND c = 0 IF v = 0", TTL), row(true));
        assertListElementTTL(0, 5, TTL, start);
    }

    @Test
    public void testCasListAppendUsingTTL()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1])");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET l = l + [2] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(1, 2)));
        assertListElementTTL(0, 2, TTL, start);
        assertListElementTTL(0, 1, LivenessInfo.NO_TTL, start);
    }

    @Test
    public void testCasListPrependUsingTTL()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1])");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET l = [0] + l WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(0, 1)));
        assertListElementTTL(0, 0, TTL, start);
        assertListElementTTL(0, 1, LivenessInfo.NO_TTL, start);
    }

    @Test
    public void testCasListSetUsingTTL()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1])");
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET l = [7, 8] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(7, 8)));
        assertListElementTTL(0, 7, TTL, start);
        assertListElementTTL(0, 8, TTL, start);
        assertLocalDeletionTimes(0, start); // the overwrite's complex deletion
    }

    @Test
    public void testCasListDiscardUsingTTL()
    {
        // discarding writes only tombstones, so the TTL is irrelevant, but it must be accepted and not corrupt the update
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2])");
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET l = l - [1] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT v, l FROM %s WHERE k = 0 AND c = 0"), row(0, list(2)));
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testCasListAppendWithSetByIndexUsingTTL()
    {
        // CAS only evaluates list appends on execution if the statement has some other operation requiring a read,
        // in which case all operations requiring a timestamp are also evaluated on execution
        createTable("CREATE TABLE %s (k int, c int, v int, l list<int>, l2 list<int>, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        exec("INSERT INTO %s (k, c, v, l, l2) VALUES (0, 0, 0, [1, 2], [3])");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET l[0] = 5, l2 = l2 + [4] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l, l2 FROM %s WHERE k = 0 AND c = 0"), row(list(5, 2), list(3, 4)));
        assertListElementTTL(0, "l", 5, TTL, start);
        assertListElementTTL(0, "l2", 4, TTL, start);
        assertListElementTTL(0, "l2", 3, LivenessInfo.NO_TTL, start);
    }

    @Test
    public void testCasListAndRegularUpdateUsingTTL()
    {
        // the regular column is applied from the coordinator's base update, the list element from the TxnWrite.Fragment;
        // both must have the same TTL and expiration time
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2])");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("UPDATE %s USING TTL " + TTL + " SET v = 1, l[1] = 5 WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT v, l FROM %s WHERE k = 0 AND c = 0"), row(1, list(1, 5)));
        assertTTLWithin(exec("SELECT ttl(v) FROM %s WHERE k = 0 AND c = 0").one().getInt(0), TTL);
        long expiresAt = assertListElementTTL(0, 5, TTL, start);
        for (Row row : rows(readLocal(0)))
        {
            for (Cell<?> cell : cells(row))
            {
                if (cell.column().name.toString().equals("v"))
                    assertEquals("Base update and reference operation expiration times differ", expiresAt, cell.localDeletionTime());
            }
        }
    }

    @Test
    public void testCasBatchListAppendUsingTTL()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1])");
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("BEGIN BATCH\n" +
                           "  UPDATE %1$s USING TTL " + TTL + " SET l = l + [2] WHERE k = 0 AND c = 0 IF v = 0;\n" +
                           "  UPDATE %1$s SET l = l + [3] WHERE k = 0 AND c = 1;\n" +
                           "APPLY BATCH"), row(true));
        assertRowsNet(exec("SELECT c, l FROM %s WHERE k = 0"), row(0, list(1, 2)), row(1, list(3)));
        assertListElementTTL(0, 2, TTL, start);
        assertListElementTTL(0, 3, LivenessInfo.NO_TTL, start);
    }

    @Test
    public void testCasListAppendUsingTTLExpires()
    {
        int ttl = 2;
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1])");
        assertRowsNet(exec("UPDATE %s USING TTL " + ttl + " SET l = l + [2] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(1, 2)));
        Awaitility.await().atMost(ttl + 30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
                  .until(() -> list(1).equals(exec("SELECT l FROM %s WHERE k = 0 AND c = 0").one().getList(0, Integer.class)));
    }

    @Test
    public void testCasInsertListIfNotExistsUsingTTL()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        assertRowsNet(exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2]) IF NOT EXISTS USING TTL " + TTL), row(true));
        assertRowsNet(exec("SELECT v, l FROM %s WHERE k = 0 AND c = 0"), row(0, list(1, 2)));
        assertVisibleWithTTL(0, 0, TTL);
        assertListElementTTL(0, 1, TTL, start);
        assertListElementTTL(0, 2, TTL, start);
        assertListPathsUseWriteTimestamp(0, "l", 2);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // List cell paths. CAS is linearizable, so list elements must be ordered by the transaction's execution timestamp
    // rather than by the coordinator's clock, as they are for explicit transactions (and as Paxos orders them by ballot)
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testCasListAppendPathUsesExecutionTimestamp()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        assertRowsNet(exec("UPDATE %s SET l = l + [1, 2] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("UPDATE %s SET l = l + [3] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(1, 2, 3)));
        assertListPathsUseWriteTimestamp(0, "l", 3);
    }

    @Test
    public void testCasListSetPathUsesExecutionTimestamp()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1])");
        assertRowsNet(exec("UPDATE %s SET l = [7, 8] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(7, 8)));
        assertListPathsUseWriteTimestamp(0, "l", 2);
    }

    @Test
    public void testCasListAppendWithSetByIndexPathUsesExecutionTimestamp()
    {
        // whether or not the statement requires a read must make no difference
        createTable("CREATE TABLE %s (k int, c int, v int, l list<int>, l2 list<int>, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1])");
        assertRowsNet(exec("UPDATE %s SET l[0] = 5, l2 = l2 + [4] WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT l, l2 FROM %s WHERE k = 0 AND c = 0"), row(list(5), list(4)));
        assertListPathsUseWriteTimestamp(0, "l2", 1);
    }

    @Test
    public void testCasBatchListAppendPathUsesExecutionTimestamp()
    {
        createAccordTable();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        assertRowsNet(exec("BEGIN BATCH\n" +
                           "  UPDATE %1$s SET l = l + [1] WHERE k = 0 AND c = 0 IF v = 0;\n" +
                           "  UPDATE %1$s SET l = l + [2] WHERE k = 0 AND c = 1;\n" +
                           "APPLY BATCH"), row(true));
        assertListPathsUseWriteTimestamp(0, "l", 2);
    }

    @Test
    public void testTxnListAppendPathUsesExecutionTimestamp()
    {
        createAccordTable();
        exec(txn("UPDATE %s SET l = l + [1, 2] WHERE k = 0 AND c = 0;"));
        assertRowsNet(exec("SELECT l FROM %s WHERE k = 0 AND c = 0"), row(list(1, 2)));
        assertListPathsUseWriteTimestamp(0, "l", 2);
    }

    @Test
    public void testCasListDeleteByIndex()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2])");
        assertRowsNet(exec("DELETE l[0] FROM %s WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        // the element deletion requires a read so is performed using the transaction's reads; it must not delete the row
        assertRowsNet(exec("SELECT v, l FROM %s WHERE k = 0 AND c = 0"), row(0, list(2)));
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testTxnListDeleteByIndex()
    {
        // deleting a list element by index is performed using the transaction's reads; it must not delete the row
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v, l) VALUES (0, 0, 0, [1, 2])");
        exec(txn("DELETE l[0] FROM %s WHERE k = 0 AND c = 0;"));
        assertRowsNet(exec("SELECT v, l FROM %s WHERE k = 0 AND c = 0"), row(0, list(2)));
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testCasListDeleteByIndexAndColumn()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v, l, s) VALUES (0, 0, 0, [1, 2], 3)");
        assertRowsNet(exec("DELETE l[0], v FROM %s WHERE k = 0 AND c = 0 IF v = 0"), row(true));
        assertRowsNet(exec("SELECT v, l, s FROM %s WHERE k = 0 AND c = 0"), row(null, list(2), 3));
        assertLocalDeletionTimes(0, start);
    }

    @Test
    public void testCasDelete()
    {
        createAccordTable();
        long start = FBUtilities.nowInSeconds();
        exec("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        assertRowsNet(exec("DELETE FROM %s WHERE k = 0 AND c = 0 IF EXISTS"), row(true));
        assertRowsNet(exec("SELECT v FROM %s WHERE k = 0 AND c = 0"));
        assertLocalDeletionTimes(0, start);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------------

    private com.datastax.driver.core.ResultSet exec(String query, Object... values)
    {
        SimpleStatement statement = new SimpleStatement(formatQuery(query), values);
        statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        statement.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        return sessionNet().execute(statement);
    }

    private void assertRejected(String query, String message)
    {
        try
        {
            exec(query);
            fail("Expected query to be rejected: " + query);
        }
        catch (InvalidQueryException e)
        {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(message));
        }
    }

    private void assertVisibleWithTTL(int k, int c, int expectedTTL)
    {
        com.datastax.driver.core.Row row = exec("SELECT v, ttl(v) FROM %s WHERE k = ? AND c = ?", k, c).one();
        assertNotNull("Row written with a TTL of " + expectedTTL + "s was not visible immediately after the write", row);
        assertEquals(0, row.getInt(0));
        assertTTLWithin(row.getInt(1), expectedTTL);
    }

    private static void assertTTLWithin(int actual, int expected)
    {
        assertTrue("Expected remaining TTL in (" + (expected - SLACK_SECONDS) + ", " + expected + "] but was " + actual,
                   actual <= expected && actual > expected - SLACK_SECONDS);
    }

    private ImmutableBTreePartition readLocal(int k)
    {
        // Accord may respond to the client before the write is applied locally, so first read the partition
        // through Accord, which ensures any earlier transactions have been applied
        exec("SELECT * FROM %s WHERE k = ?", k);
        // read the raw local state directly, bypassing Accord; tombstones and expired cells are retained
        SinglePartitionReadCommand command = (SinglePartitionReadCommand) Util.cmd(getCurrentColumnFamilyStore(), k).build();
        return Util.getOnlyPartitionUnfiltered(command.withTransactionalSettings(false, command.nowInSec()));
    }

    private static List<Row> rows(ImmutableBTreePartition partition)
    {
        List<Row> rows = new ArrayList<>();
        if (!partition.staticRow().isEmpty())
            rows.add(partition.staticRow());
        for (Row row : partition)
            rows.add(row);
        return rows;
    }

    private static List<Cell<?>> cells(Row row)
    {
        List<Cell<?>> cells = new ArrayList<>();
        for (Cell<?> cell : row.cells())
            cells.add(cell);
        return cells;
    }

    /**
     * Asserts that every liveness info and cell stored locally for partition {@code k} is expiring with the given TTL,
     * and that its local expiration time is {@code ttl} seconds after the write was applied.
     */
    private void assertLocalExpiration(int k, int ttl, long startSeconds)
    {
        long end = FBUtilities.nowInSeconds();
        ImmutableBTreePartition partition = readLocal(k);
        int checked = 0;
        for (Row row : rows(partition))
        {
            LivenessInfo info = row.primaryKeyLivenessInfo();
            if (!info.isEmpty())
            {
                assertTrue("Expected expiring liveness info for " + row.clustering() + " but was " + info, info.isExpiring());
                assertEquals(ttl, info.ttl());
                assertExpirationTime("liveness info of " + row.clustering(), info.localExpirationTime(), ttl, startSeconds, end);
                ++checked;
            }
            for (Cell<?> cell : cells(row))
            {
                if (cell.isTombstone())
                    continue; // e.g. collection overwrite; covered by assertLocalDeletionTimes
                assertTrue("Expected expiring cell " + cell, cell.isExpiring());
                assertEquals(ttl, cell.ttl());
                assertExpirationTime("cell " + cell, cell.localDeletionTime(), ttl, startSeconds, end);
                ++checked;
            }
        }
        assertTrue("No expiring data found locally for partition " + k, checked > 0);
    }

    /**
     * Asserts that the (single) element of the given list column with the given value is stored locally with the given TTL,
     * expiring {@code ttl} seconds after the write was applied.
     *
     * @return the element's local expiration time
     */
    private long assertListElementTTL(int k, int value, int ttl, long startSeconds)
    {
        return assertListElementTTL(k, "l", value, ttl, startSeconds);
    }

    private long assertListElementTTL(int k, String column, int value, int ttl, long startSeconds)
    {
        long end = FBUtilities.nowInSeconds();
        for (Row row : rows(readLocal(k)))
        {
            for (Cell<?> cell : cells(row))
            {
                if (!cell.column().name.toString().equals(column) || cell.isTombstone() || Int32Type.instance.compose(cell.buffer()) != value)
                    continue;

                assertEquals("Unexpected TTL for list element " + cell, ttl, cell.ttl());
                if (ttl != LivenessInfo.NO_TTL)
                    assertExpirationTime("cell " + cell, cell.localDeletionTime(), ttl, startSeconds, end);
                return cell.localDeletionTime();
            }
        }
        throw new AssertionError("List " + column + " element " + value + " not found locally for partition " + k);
    }

    /**
     * Asserts that the time UUID path of every live element of the given list column is derived from the element's
     * write timestamp, i.e. that both were assigned by the transaction on execution (see AccordUpdateParameters)
     */
    private void assertListPathsUseWriteTimestamp(int k, String column, int expectedElements)
    {
        int checked = 0;
        for (Row row : rows(readLocal(k)))
        {
            for (Cell<?> cell : cells(row))
            {
                if (!cell.column().name.toString().equals(column) || cell.isTombstone())
                    continue;

                TimeUUID path = TimeUUID.deserialize(cell.path().get(0));
                assertEquals("List element " + cell + " path was not derived from its write timestamp",
                             TimeUUID.unixMicrosToMsb(cell.timestamp()), path.msb());
                ++checked;
            }
        }
        assertEquals("Unexpected number of elements of " + column + " for partition " + k, expectedElements, checked);
    }

    private static void assertExpirationTime(String what, long localExpirationTime, int ttl, long startSeconds, long endSeconds)
    {
        assertTrue(String.format("Local expiration time %d of %s should be within [%d, %d] (now + ttl)",
                                 localExpirationTime, what, startSeconds + ttl, endSeconds + ttl),
                   localExpirationTime >= startSeconds + ttl && localExpirationTime <= endSeconds + ttl);
    }

    /**
     * Asserts that every tombstone (partition, range, row, complex or cell deletion) stored locally for partition
     * {@code k} has a local deletion time corresponding to when the deletion was applied.
     * <p>
     * This includes any tombstones written while setting up the test (e.g. the complex deletion written by assigning a
     * collection literal), so {@code startSeconds} must be taken before the first write to the partition; otherwise
     * the test fails whenever the clock ticks over to the next second between that write and taking {@code startSeconds}.
     */
    private void assertLocalDeletionTimes(int k, long startSeconds)
    {
        long end = FBUtilities.nowInSeconds();
        ImmutableBTreePartition partition = readLocal(k);
        int checked = 0;

        DeletionTime partitionDeletion = partition.partitionLevelDeletion();
        if (!partitionDeletion.isLive())
        {
            assertDeletionTime("partition deletion", partitionDeletion.localDeletionTime(), startSeconds, end);
            ++checked;
        }

        for (Row row : rows(partition))
        {
            if (!row.deletion().isLive())
            {
                assertDeletionTime("row deletion of " + row.clustering(), row.deletion().time().localDeletionTime(), startSeconds, end);
                ++checked;
            }
            for (ColumnData cd : row)
            {
                if (cd.column().isComplex())
                {
                    DeletionTime complexDeletion = ((ComplexColumnData) cd).complexDeletion();
                    if (!complexDeletion.isLive())
                    {
                        assertDeletionTime("complex deletion of " + cd.column(), complexDeletion.localDeletionTime(), startSeconds, end);
                        ++checked;
                    }
                }
            }
            for (Cell<?> cell : cells(row))
            {
                if (cell.isTombstone())
                {
                    assertFalse(cell.isExpiring());
                    assertDeletionTime("cell " + cell, cell.localDeletionTime(), startSeconds, end);
                    ++checked;
                }
            }
        }
        assertTrue("No tombstones found locally for partition " + k, checked > 0);
    }

    private static void assertDeletionTime(String what, long localDeletionTime, long startSeconds, long endSeconds)
    {
        assertTrue(String.format("Local deletion time %d of %s should be within [%d, %d] (now)",
                                 localDeletionTime, what, startSeconds, endSeconds),
                   localDeletionTime >= startSeconds && localDeletionTime <= endSeconds);
    }
}
