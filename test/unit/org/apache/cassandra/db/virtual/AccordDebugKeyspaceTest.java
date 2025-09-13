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

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProtocolModifiers;
import accord.messages.NoWaitRequest;
import accord.api.RoutingKey;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OptionaldPositiveInt;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.exceptions.ExceptionSerializer;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Condition;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static accord.api.ProtocolModifiers.Toggles.SendStableMessages.TO_ALL;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static org.apache.cassandra.Util.spinUntilSuccess;
import static org.apache.cassandra.net.Verb.ACCORD_APPLY_AND_WAIT_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_APPLY_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_BEGIN_RECOVER_REQ;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class AccordDebugKeyspaceTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDebugKeyspaceTest.class);

    private static final String QUERY_TXN_BLOCKED_BY =
        String.format("SELECT * FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_BLOCKED_BY);

    private static final String QUERY_TXN_BLOCKED_BY_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_BLOCKED_BY);

    private static final String QUERY_COMMANDS_FOR_KEY =
        String.format("SELECT txn_id, status FROM %s.%s WHERE key=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.COMMANDS_FOR_KEY);

    private static final String QUERY_COMMANDS_FOR_KEY_REMOTE =
        String.format("SELECT txn_id, status FROM %s.%s WHERE node_id = ? AND key=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.COMMANDS_FOR_KEY);

    private static final String QUERY_TXN =
        String.format("SELECT txn_id, save_status FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN);

    private static final String QUERY_TXN_REMOTE =
        String.format("SELECT txn_id, save_status FROM %s.%s WHERE node_id = ? AND txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN);

    private static final String QUERY_TXNS =
        String.format("SELECT save_status FROM %s.%s WHERE command_store_id = ? LIMIT 5", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN);

    private static final String QUERY_TXNS_REMOTE =
        String.format("SELECT save_status FROM %s.%s WHERE node_id = ? AND command_store_id = ? LIMIT 5", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN);

    private static final String QUERY_TXNS_SEARCH =
        String.format("SELECT save_status FROM %s.%s WHERE command_store_id = ? AND txn_id > ? LIMIT 5", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN);

    private static final String QUERY_TXNS_SEARCH_REMOTE =
        String.format("SELECT save_status FROM %s.%s WHERE node_id = ? AND command_store_id = ? AND txn_id > ? LIMIT 5", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN);

    private static final String QUERY_JOURNAL =
        String.format("SELECT txn_id, save_status FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.JOURNAL);

    private static final String ERASE_JOURNAL_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND command_store_id = ? AND txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.JOURNAL);

    private static final String QUERY_JOURNAL_REMOTE =
        String.format("SELECT txn_id, save_status FROM %s.%s WHERE node_id = ? AND txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.JOURNAL);

    private static final String SET_TRACE =
        String.format("UPDATE %s.%s SET permits = ? WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String SET_TRACE_REMOTE =
        String.format("UPDATE %s.%s SET permits = ? WHERE node_id = ? AND txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_TRACE =
        String.format("SELECT * FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_TRACE_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE1 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE1_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE2 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE2_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_TRACES =
        String.format("SELECT * FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String QUERY_TRACES_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES1 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ? AND event_type = ? AND id_micros < ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES1_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ? AND event_type = ? AND id_micros < ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES2 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES2_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ? AND event_type = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES3 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES3_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACES);

    private static final String QUERY_REDUNDANT_BEFORE =
        String.format("SELECT * FROM %s.%s", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_GEQ =
        String.format("SELECT * FROM %s.%s WHERE quorum_applied >= ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_GEQ_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND quorum_applied >= ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ =
        String.format("SELECT * FROM %s.%s WHERE shard_applied >= ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND shard_applied >= ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.REDUNDANT_BEFORE);

    @BeforeClass
    public static void setUpClass()
    {
        Config.setOverrideLoadConfig(() -> {
            Config config = new YamlConfigurationLoader().loadConfig();
            config.accord.queue_shard_count = new OptionaldPositiveInt(1);
            config.concurrent_accord_operations = 1;
            config.accord.command_store_shard_count = new OptionaldPositiveInt(1);
            config.accord.enable_virtual_debug_only_keyspace = true;
            return config;
        });
        daemonInitialization();
        ProtocolModifiers.Toggles.setSendStableMessages(TO_ALL);

        CQLTester.setUpClass();
        CassandraDaemon.getInstanceForTesting().setupVirtualKeyspaces();

        AccordService.startup(ClusterMetadata.current().myNodeId());
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
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        AccordService accord = accord();
        DatabaseDescriptor.getAccord().fetch_txn = "1s";
        int nodeId = accord.nodeId().id;

        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(Txn.Kind.Write, Routable.Domain.Key);
            Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
            filter.appliesTo(id);

            execute(SET_TRACE, 1, id.toString(), "WAIT_PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "WAIT_PROGRESS"), row(id.toString(), "WAIT_PROGRESS", 1));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS"), row(nodeId, id.toString(), "WAIT_PROGRESS", 1));
            execute(SET_TRACE, 0, id.toString(), "WAIT_PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "WAIT_PROGRESS"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS"));
            execute(SET_TRACE, 1, id.toString(), "WAIT_PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "WAIT_PROGRESS"), row(id.toString(), "WAIT_PROGRESS", 1));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS"), row(nodeId, id.toString(), "WAIT_PROGRESS", 1));
            execute(UNSET_TRACE1, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString(), "WAIT_PROGRESS"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS"));
            execute(SET_TRACE, 1, id.toString(), "WAIT_PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "WAIT_PROGRESS"), row(id.toString(), "WAIT_PROGRESS", 1));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS"), row(nodeId, id.toString(), "WAIT_PROGRESS", 1));
            execute(UNSET_TRACE2, id.toString(), "WAIT_PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "WAIT_PROGRESS"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS"));
            execute(SET_TRACE, 1, id.toString(), "WAIT_PROGRESS");
            assertRows(execute(QUERY_TRACE, id.toString(), "WAIT_PROGRESS"), row(id.toString(), "WAIT_PROGRESS", 1));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS"), row(nodeId, id.toString(), "WAIT_PROGRESS", 1));
            filter.appliesTo(id);
            accord.node().coordinate(id, txn).beginAsResult();
            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            filter.apply.awaitThrowUncheckedOnInterrupt();
            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "WAIT_PROGRESS").size()).isGreaterThan(0));
            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_TRACES_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS").size()).isGreaterThan(0));
            execute(ERASE_TRACES1, id.toString(), "FETCH", Long.MAX_VALUE);
            execute(ERASE_TRACES2, id.toString(), "FETCH");
            execute(ERASE_TRACES1, id.toString(), "WAIT_PROGRESS", Long.MAX_VALUE);
            Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "WAIT_PROGRESS").size()).isEqualTo(0);
            Assertions.assertThat(execute(QUERY_TRACES_REMOTE, nodeId, id.toString(), "WAIT_PROGRESS").size()).isEqualTo(0);
            // just check other variants don't fail
            execute(ERASE_TRACES2, id.toString(), "WAIT_PROGRESS");
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
        int nodeId = accord.nodeId().id;
        TableId tableId = Schema.instance.getTableMetadata(KEYSPACE, tableName).id;
        TxnId syncId1 = new TxnId(100, 200, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, accord.nodeId());
        TxnId syncId2 = new TxnId(101, 300, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, accord.nodeId());
        Ranges ranges1 = Ranges.of(TokenRange.create(new TokenKey(tableId, new LongToken(1)), new TokenKey(tableId, new LongToken(100))));
        Ranges ranges2 = Ranges.of(TokenRange.create(new TokenKey(tableId, new LongToken(100)), new TokenKey(tableId, new LongToken(200))));
        getBlocking(accord.node().commandStores().forAll("Test", safeStore -> {
            safeStore.commandStore().markShardDurable(safeStore, syncId1, ranges1, HasOutcome.Universal);
            safeStore.commandStore().markShardDurable(safeStore, syncId2, ranges2, HasOutcome.Quorum);
        }));

        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE).size()).isGreaterThan(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_GEQ, syncId1.toString()).size()).isEqualTo(2);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_GEQ, syncId2.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ, syncId1.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ, syncId2.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_REMOTE, nodeId).size()).isGreaterThan(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_GEQ_REMOTE, nodeId, syncId1.toString()).size()).isEqualTo(2);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_GEQ_REMOTE, nodeId, syncId2.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ_REMOTE, nodeId, syncId1.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_GEQ_REMOTE, nodeId, syncId2.toString()).size()).isEqualTo(0);
    }

    @Test
    public void reportInvalidRequestForUnsupportedRemoteToLocal()
    {
        AccordService accord = accord();
        int nodeId = accord.nodeId().id;
        TxnId id = accord.node().nextTxnIdWithDefaultFlags(Txn.Kind.Write, Routable.Domain.Key);
        try
        {
            execute(ERASE_JOURNAL_REMOTE, nodeId, 1, id.toString());
        }
        catch (ExceptionSerializer.RemoteException t)
        {
            Assertions.assertThat(t.originalClass).isEqualTo(InvalidRequestException.class.getName());
        }
    }

    @Test
    public void completedTxn() throws ExecutionException, InterruptedException
    {
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        AccordService accord = accord();
        int nodeId = accord.nodeId().id;
        TxnId id = accord.node().nextTxnIdWithDefaultFlags(Txn.Kind.Write, Routable.Domain.Key);
        Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
        String keyStr = txn.keys().get(0).toUnseekable().toString();
        getBlocking(accord.node().coordinate(id, txn));

        spinUntilSuccess(() -> assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                                          row(id.toString(), anyInt(), 0, "", "", any(), anyOf(SaveStatus.ReadyToExecute.name(), SaveStatus.Applying.name(), SaveStatus.Applied.name()))));
        assertRows(execute(QUERY_TXN, id.toString()), row(id.toString(), "Applied"));
        assertRows(execute(QUERY_TXN_REMOTE, nodeId, id.toString()), row(id.toString(), "Applied"));
        assertRows(execute(QUERY_JOURNAL, id.toString()), row(id.toString(), "PreAccepted"), row(id.toString(), "Applying"), row(id.toString(), "Applied"), row(id.toString(), null));
        assertRows(execute(QUERY_JOURNAL_REMOTE, nodeId, id.toString()), row(id.toString(), "PreAccepted"), row(id.toString(), "Applying"), row(id.toString(), "Applied"), row(id.toString(), null));
        assertRows(execute(QUERY_COMMANDS_FOR_KEY, keyStr), row(id.toString(), "APPLIED_DURABLE"));
        assertRows(execute(QUERY_COMMANDS_FOR_KEY_REMOTE, nodeId, keyStr), row(id.toString(), "APPLIED_DURABLE"));
    }

    @Test
    public void manyTxns()
    {
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        AccordService accord = accord();
        int nodeId = accord.nodeId().id;
        List<IAccordService.IAccordResult> await = new ArrayList<>();
        Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
        for (int i = 0 ; i < 100; ++i)
            await.add(accord.coordinateAsync(0, 0, txn, ConsistencyLevel.QUORUM, new Dispatcher.RequestTime(Clock.Global.nanoTime())));

        AccordCommandStore commandStore = (AccordCommandStore) accord.node().commandStores().unsafeForKey((RoutingKey) txn.keys().get(0).toUnseekable());
        await.forEach(IAccordService.IAccordResult::awaitAndGet);

        assertRows(execute(QUERY_TXNS, commandStore.id()),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied")
        );

        assertRows(execute(QUERY_TXNS_SEARCH, commandStore.id(), TxnId.NONE.toString()),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied")
        );

        assertRows(execute(QUERY_TXNS_REMOTE, nodeId, commandStore.id()),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied")
        );

        assertRows(execute(QUERY_TXNS_SEARCH_REMOTE, nodeId, commandStore.id(), TxnId.NONE.toString()),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied"),
                   row("Applied")
        );
    }

    @Test
    public void inflight() throws ExecutionException, InterruptedException
    {
        ProtocolModifiers.Toggles.setPermitLocalExecution(false);
        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            AccordService accord = accord();
            int nodeId = accord.nodeId().id;
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(Txn.Kind.Write, Routable.Domain.Key);
            String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                             "    LET r = (SELECT * FROM %s.%s WHERE k = ? AND c = ?);\n" +
                                             "    IF r IS NULL THEN\n " +
                                             "        INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                             "    END IF\n" +
                                             "COMMIT TRANSACTION", KEYSPACE, tableName, KEYSPACE, tableName);
            Txn txn = createTxn(insertTxn, 0, 0, 0, 0, 0);
            filter.appliesTo(id);
            accord.node().coordinate(id, txn).beginAsResult();

            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), anyInt(), 0, "", "", any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, id.toString()),
                       row(nodeId, id.toString(), anyInt(), 0, "", "", any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), anyInt(), 0, "", "", any(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, id.toString()),
                       row(nodeId, id.toString(), anyInt(), 0, "", "", any(), SaveStatus.ReadyToExecute.name()));
        }
        finally
        {
            filter.reset();
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
            AccordService accord = accord();
            int nodeId = accord.nodeId().id;
            TxnId first = accord.node().nextTxnIdWithDefaultFlags(Txn.Kind.Write, Routable.Domain.Key);
            String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                             "    LET r = (SELECT * FROM %s.%s WHERE k = ? AND c = ?);\n" +
                                             "    IF r IS NULL THEN\n " +
                                             "        INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                             "    END IF\n" +
                                             "COMMIT TRANSACTION", KEYSPACE, tableName, KEYSPACE, tableName);

            filter.appliesTo(first);
            accord.node().coordinate(first, createTxn(insertTxn, 0, 0, 0, 0, 0)).beginAsResult();

            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), anyInt(), 0, "", any(), any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, first.toString()),
                       row(nodeId, first.toString(), anyInt(), 0, "", any(), any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), anyInt(), 0, "", any(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, first.toString()),
                       row(nodeId, first.toString(), anyInt(), 0, "", any(), anyNonNull(), SaveStatus.ReadyToExecute.name()));

            filter.reset();

            TxnId second = accord.node().nextTxnIdWithDefaultFlags(Txn.Kind.Write, Routable.Domain.Key);
            filter.reset();
            filter.appliesTo(second);
            accord.node().coordinate(second, createTxn(insertTxn, 0, 0, 0, 0, 0)).beginAsResult();

            filter.commit.awaitThrowUncheckedOnInterrupt();

            Awaitility.await("waiting on key").atMost(1, TimeUnit.MINUTES)
                                          .until(() -> {
                                              UntypedResultSet rs = execute(QUERY_TXN_BLOCKED_BY, second.toString());
                                              return rs.size() == 2;
                                          });
            assertRows(execute(QUERY_TXN_BLOCKED_BY, second.toString()),
                       row(second.toString(), anyInt(), 0, "", "", anyNonNull(), SaveStatus.Stable.name()),
                       row(second.toString(), anyInt(), 1, any(), first.toString(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY + " AND depth < 1", second.toString()),
                       row(second.toString(), anyInt(), 0, any(), "", anyNonNull(), SaveStatus.Stable.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, second.toString()),
                       row(nodeId, second.toString(), anyInt(), 0, "", "", anyNonNull(), SaveStatus.Stable.name()),
                       row(nodeId, second.toString(), anyInt(), 1, any(), first.toString(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE + " AND depth < 1", nodeId, second.toString()),
                       row(nodeId, second.toString(), anyInt(), 0, any(), "", anyNonNull(), SaveStatus.Stable.name()));
        }
        finally
        {
            filter.reset();
            MessagingService.instance().outboundSink.remove(filter);
        }
    }

    @Test
    public void patchJournalVestigialTest()
    {
        testPatchJournal("ERASE_VESTIGIAL", "Vestigial");
    }

    @Test
    public void patchJournalInvalidateTest()
    {
        testPatchJournal("INVALIDATE", "Invalidated");
    }

    @Test
    public void patchJournalTruncateTest()
    {
        try
        {
            testPatchJournal("ERASE", "Erased");
            Assert.fail("Should have thrown");
        }
        catch (Throwable t)
        {
            Assert.assertTrue(t.getMessage().contains("No enum constant"));
        }
    }

    private void testPatchJournal(String cleanupAction, String expectedStatus)
    {
        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                             "  INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                             "COMMIT TRANSACTION",
                                             KEYSPACE,
                                             tableName);
            AccordService accord = accord();
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(Txn.Kind.Write, Routable.Domain.Key);
            filter.appliesTo(id);
            accord.node().coordinate(id, createTxn(insertTxn, 0, 0, 0)).beginAsResult();

            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            String QUERY_JOURNAL = String.format("SELECT txn_id, save_status, command_store_id FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.JOURNAL);
            String QUERY_TXN = String.format("SELECT txn_id, save_status FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN);

            UntypedResultSet rs = execute(QUERY_JOURNAL, id.toString());
            assertRows(rs, row(id.toString(), "PreAccepted", anyNonNull()));

            int commandStoreId = rs.one().getInt("command_store_id");
            String PATCH_JOURNAL = String.format("UPDATE %s.%s SET op = ? WHERE txn_id=? AND command_store_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_OPS);
            execute(PATCH_JOURNAL, cleanupAction, id.toString(), commandStoreId);

            assertRows(execute(QUERY_TXN, id.toString()),
                       row(id.toString(), expectedStatus));
            assertRows(execute(QUERY_JOURNAL, id.toString()),
                       row(id.toString(), "PreAccepted", commandStoreId),
                       row(id.toString(), expectedStatus, commandStoreId));
        }
        finally
        {
            filter.reset();
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
        Set<Verb> dropVerbs = Set.of(ACCORD_APPLY_REQ, ACCORD_APPLY_AND_WAIT_REQ, ACCORD_BEGIN_RECOVER_REQ);
        Set<TxnId> applyTo;

        void appliesTo(TxnId txnId)
        {
            if (applyTo == null)
                applyTo = Collections.newSetFromMap(new ConcurrentHashMap<>());
            applyTo.add(txnId);
        }

        @Override
        public boolean test(Message<?> msg, InetAddressAndPort to)
        {
            if (!msg.verb().name().startsWith("ACCORD_"))
                return true;
            TxnId txnId = null;
            if (msg.payload instanceof NoWaitRequest<?,?>)
            {
                txnId = ((NoWaitRequest<?,?>) msg.payload).txnId;
                if (applyTo != null && !applyTo.contains(txnId))
                    return true;
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
                    break;
                case ACCORD_PRE_ACCEPT_RSP:
                    preAccept.signalAll();
                    break;
                case ACCORD_COMMIT_REQ:
                case ACCORD_STABLE_THEN_READ_REQ:
                    commit.signalAll();
            }
            return !dropVerbs.contains(msg.verb());
        }
    }


}