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

package org.apache.cassandra.db.virtual;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProtocolModifiers;
import accord.messages.TxnRequest;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OptionaldPositiveInt;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Condition;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static accord.api.ProtocolModifiers.Toggles.SendStableMessages.TO_ALL;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static org.apache.cassandra.Util.spinUntilSuccess;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class AccordDebugKeyspaceTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDebugKeyspaceTest.class);

    private static final String QUERY_TXN_BLOCKED_BY =
        String.format("SELECT * FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_BLOCKED_BY);

    private static final String QUERY_TXN =
        String.format("SELECT txn_id, save_status FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN);

    private static final String QUERY_JOURNAL =
        String.format("SELECT txn_id, save_status FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.JOURNAL);

    private static final String SET_TRACE =
        String.format("UPDATE %s.%s SET permits = ? WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_TRACE =
        String.format("SELECT * FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE1 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE2 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_TRACES =
        String.format("SELECT * FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES1 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ? AND event_type = ? AND id_micros < ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES2 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES3 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String QUERY_REDUNDANT_BEFORE =
        String.format("SELECT * FROM %s.%s", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_MAJORITY_APPLIED_GEQ =
        String.format("SELECT * FROM %s.%s WHERE majority_applied >= ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ =
        String.format("SELECT * FROM %s.%s WHERE shard_applied >= ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    @BeforeClass
    public static void setUpClass()
    {
        daemonInitialization();
        DatabaseDescriptor.getAccord().queue_shard_count = new OptionaldPositiveInt(1);
        DatabaseDescriptor.getAccord().command_store_shard_count = new OptionaldPositiveInt(1);
        ProtocolModifiers.Toggles.setSendStableMessages(TO_ALL);

        CQLTester.setUpClass();

        AccordService.startup(ClusterMetadata.current().myNodeId());
        VirtualKeyspaceRegistry.instance.register(AccordDebugKeyspace.instance);
        requireNetwork();
    }

    @Test
    public void unknownIsEmpty()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        assertRows(execute(QUERY_TXN_BLOCKED_BY, TxnId.NONE.toString()));
        assertRows(execute(QUERY_TXN, TxnId.NONE.toString()));
        assertRows(execute(QUERY_JOURNAL, TxnId.NONE.toString()));
    }

    @Test
    public void tracing()
    {
        // simple test to confirm basic tracing functionality works, doesn't validate specific behaviours only requesting/querying/erasing
        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            var accord = accord();
            DatabaseDescriptor.getAccord().fetch_txn = "1s";
            TxnId id = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);

            execute(SET_TRACE, 1, id.toString(), "PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "PROGRESS"), row(id.toString(), "PROGRESS", 1));
            execute(SET_TRACE, 0, id.toString(), "PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "PROGRESS"));
            execute(SET_TRACE, 1, id.toString(), "PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "PROGRESS"), row(id.toString(), "PROGRESS", 1));
            execute(UNSET_TRACE1, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString(), "PROGRESS"));
            execute(SET_TRACE, 1, id.toString(), "PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "PROGRESS"), row(id.toString(), "PROGRESS", 1));
            execute(UNSET_TRACE2, id.toString(), "PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "PROGRESS"));
            execute(SET_TRACE, 1, id.toString(), "PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "PROGRESS"), row(id.toString(), "PROGRESS", 1));
            accord.node().coordinate(id, txn);
            filter.preAccept.awaitThrowUncheckedOnInterrupt();

            filter.apply.awaitThrowUncheckedOnInterrupt();
            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "PROGRESS").size()).isGreaterThan(0));
            execute(ERASE_TRACES1, id.toString(), "FETCH", Long.MAX_VALUE);
            execute(ERASE_TRACES2, id.toString(), "FETCH");
            execute(ERASE_TRACES1, id.toString(), "PROGRESS", Long.MAX_VALUE);
            Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "PROGRESS").size()).isEqualTo(0);
            // just check other variants don't fail
            execute(ERASE_TRACES2, id.toString(), "PROGRESS");
            execute(ERASE_TRACES3, id.toString());
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(filter);
        }
    }

    @Test
    public void redundantBefore() throws ExecutionException, InterruptedException
    {
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        var accord = accord();
        TableId tableId = Schema.instance.getTableMetadata(KEYSPACE, tableName).id;
        TxnId syncId1 = new TxnId(100, 200, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, accord.nodeId());
        TxnId syncId2 = new TxnId(101, 300, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, accord.nodeId());
        Ranges ranges1 = Ranges.of(TokenRange.create(new TokenKey(tableId, new LongToken(1)), new TokenKey(tableId, new LongToken(100))));
        Ranges ranges2 = Ranges.of(TokenRange.create(new TokenKey(tableId, new LongToken(100)), new TokenKey(tableId, new LongToken(200))));
        AsyncChains.getBlocking(accord.node().commandStores().forEach(safeStore -> {
            safeStore.commandStore().markShardDurable(safeStore, syncId1, ranges1, Status.Durability.Universal);
            safeStore.commandStore().markShardDurable(safeStore, syncId2, ranges2, Status.Durability.Majority);
        }));

        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE).size()).isGreaterThan(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_MAJORITY_APPLIED_GEQ, syncId1.toString()).size()).isEqualTo(2);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_MAJORITY_APPLIED_GEQ, syncId2.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ, syncId1.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ, syncId2.toString()).size()).isEqualTo(0);
    }

    @Test
    public void completedTxn() throws ExecutionException, InterruptedException
    {
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        var accord = accord();
        TxnId id = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
        Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
        AsyncChains.getBlocking(accord.node().coordinate(id, txn));

        spinUntilSuccess(() -> assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                                          row(id.toString(), KEYSPACE, tableName, anyInt(), 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "Self", any(), null, anyOf(SaveStatus.ReadyToExecute.name(), SaveStatus.Applying.name(), SaveStatus.Applied.name()))));
        assertRows(execute(QUERY_TXN, id.toString()), row(id.toString(), "Applied"));
        assertRows(execute(QUERY_JOURNAL, id.toString()), row(id.toString(), "PreAccepted"), row(id.toString(), "Applying"), row(id.toString(), "Applied"), row(id.toString(), "null"));
    }

    @Test
    public void inflight() throws ExecutionException, InterruptedException
    {
        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            var accord = accord();
            TxnId id = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                             "    LET r = (SELECT * FROM %s.%s WHERE k = ? AND c = ?);\n" +
                                             "    IF r IS NULL THEN\n " +
                                             "        INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                             "    END IF\n" +
                                             "COMMIT TRANSACTION", KEYSPACE, tableName, KEYSPACE, tableName);
            Txn txn = createTxn(insertTxn, 0, 0, 0, 0, 0);
            accord.node().coordinate(id, txn);

            filter.preAccept.awaitThrowUncheckedOnInterrupt();

            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), KEYSPACE, tableName, anyInt(), 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "Self", any(), null, anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));

            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), KEYSPACE, tableName, anyInt(), 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "Self", any(), null, SaveStatus.ReadyToExecute.name()));
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(filter);
        }
    }

    @Test
    public void blocked() throws ExecutionException, InterruptedException
    {
        ProtocolModifiers.Toggles.setPermitLocalExecution(false);
        ProtocolModifiers.Toggles.setPermittedFastPaths(new TxnId.FastPaths(Unoptimised));
        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            var accord = accord();
            TxnId first = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                             "    LET r = (SELECT * FROM %s.%s WHERE k = ? AND c = ?);\n" +
                                             "    IF r IS NULL THEN\n " +
                                             "        INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                             "    END IF\n" +
                                             "COMMIT TRANSACTION", KEYSPACE, tableName, KEYSPACE, tableName);
            accord.node().coordinate(first, createTxn(insertTxn, 0, 0, 0, 0, 0));

            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), KEYSPACE, tableName, anyInt(), 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "Self", any(), null, anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));

            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), KEYSPACE, tableName, anyInt(), 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "Self", anyNonNull(), null, SaveStatus.ReadyToExecute.name()));

            filter.reset();

            TxnId second = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            accord.node().coordinate(second, createTxn(insertTxn, 0, 0, 0, 0, 0));

            filter.commit.awaitThrowUncheckedOnInterrupt();

            Awaitility.await("waiting on key").atMost(1, TimeUnit.MINUTES)
                                          .until(() -> {
                                              UntypedResultSet rs = execute(QUERY_TXN_BLOCKED_BY, second.toString());
                                              return rs.size() == 2;
                                          });
            assertRows(execute(QUERY_TXN_BLOCKED_BY, second.toString()),
                       row(second.toString(), KEYSPACE, tableName, anyInt(), 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "Self", anyNonNull(), null, SaveStatus.Stable.name()),
                       row(second.toString(), KEYSPACE, tableName, anyInt(), 1, first.toString(), "Key", anyNonNull(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(filter);
        }
    }

    private static AccordService accord()
    {
        return (AccordService) AccordService.instance();
    }

    private static class AccordMsgFilter implements BiPredicate<Message<?>, InetAddressAndPort>
    {
        volatile Condition preAccept = Condition.newOneTimeCondition();
        volatile Condition commit = Condition.newOneTimeCondition();
        volatile Condition apply = Condition.newOneTimeCondition();

        void reset()
        {
            preAccept = Condition.newOneTimeCondition();
            commit = Condition.newOneTimeCondition();
            apply = Condition.newOneTimeCondition();
        }

        ConcurrentMap<TxnId, ConcurrentSkipListSet<Verb>> txnToVerbs = new ConcurrentHashMap<>();

        @Override
        public boolean test(Message<?> msg, InetAddressAndPort to)
        {
            if (!msg.verb().name().startsWith("ACCORD_"))
                return true;
            TxnId txnId = null;
            if (msg.payload instanceof TxnRequest)
            {
                txnId = ((TxnRequest<?>) msg.payload).txnId;
            }
            Set<Verb> seen;
            if (txnId != null)
            {
                seen = txnToVerbs.computeIfAbsent(txnId, ignore -> new ConcurrentSkipListSet<>());
                seen.add(msg.verb());
            }
            switch (msg.verb())
            {
                case ACCORD_APPLY_REQ:
                case ACCORD_APPLY_AND_WAIT_REQ:
                    apply.signalAll();
                case ACCORD_BEGIN_RECOVER_REQ:
                    return false;
                case ACCORD_PRE_ACCEPT_RSP:
                    preAccept.signalAll();
                    return true;
                case ACCORD_COMMIT_REQ:
                case ACCORD_STABLE_THEN_READ_REQ:
                    commit.signalAll();
                    return true;
                case ACCORD_PRE_ACCEPT_REQ:
                case ACCORD_ACCEPT_REQ:
                case ACCORD_ACCEPT_RSP:
                case ACCORD_CHECK_STATUS_REQ:
                case ACCORD_CHECK_STATUS_RSP:
                case ACCORD_READ_REQ:
                case ACCORD_READ_RSP:
                case ACCORD_AWAIT_REQ:
                case ACCORD_AWAIT_RSP:
                case ACCORD_AWAIT_ASYNC_RSP_REQ:
                    return true;
                default:
                    // many code paths don't log the error...
                    UnsupportedOperationException e = new UnsupportedOperationException(msg.verb().name());
                    logger.error("Unexpected verb {}", msg.verb(), e);
                    throw e;
            }
        }
    }
}