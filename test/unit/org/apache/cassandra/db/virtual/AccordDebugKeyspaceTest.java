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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import accord.api.Journal;
import accord.api.ProtocolModifiers;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.messages.NoWaitRequest;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyException;

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
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundSink;
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
import org.apache.cassandra.service.accord.debug.TxnKindsAndDomains;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.api.ProtocolModifiers.SendStableMessages.TO_ALL;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static org.apache.cassandra.Util.spinUntilSuccess;
import static org.apache.cassandra.net.Verb.ACCORD_APPLY_AND_WAIT_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_APPLY_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_BEGIN_RECOVER_REQ;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.debug.TxnKindsAndDomains.ALL;

public class AccordDebugKeyspaceTest extends CQLTester
{
    private static final String QUERY_TXN_BLOCKED_BY =
        String.format("SELECT * FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_BLOCKED_BY);

    private static final String QUERY_TXN_BLOCKED_BY_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_BLOCKED_BY);

    private static final String QUERY_TXN_GRAPH =
        String.format("SELECT * FROM %s.%s WHERE txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_GRAPH);

    private static final String QUERY_TXN_GRAPH_DESC =
        String.format("SELECT * FROM %s.%s WHERE txn_id=? ORDER BY depth DESC", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_GRAPH);

    private static final String QUERY_TXN_GRAPH_INTERSECTS =
        String.format("SELECT * FROM %s.%s WHERE txn_id=? AND expr(intersects, ?)", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_GRAPH);

    private static final String QUERY_TXN_GRAPH_KIND =
        String.format("SELECT * FROM %s.%s WHERE txn_id=? AND expr(kind, ?)", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_GRAPH);

    private static final String QUERY_TXN_GRAPH_INTERSECTS_AND_KIND =
        String.format("SELECT * FROM %s.%s WHERE txn_id=? AND expr(intersects, ?) AND expr(kind, ?)", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_GRAPH);

    private static final String QUERY_TXN_GRAPH_MIN =
        String.format("SELECT * FROM %s.%s WHERE txn_id=? AND child_txn_id >= ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_GRAPH);

    private static final String QUERY_TXN_GRAPH_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND txn_id=?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_GRAPH);

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
        String.format("UPDATE %s.%s SET bucket_size = ?, trace_events = ? WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String SET_TRACE_REMOTE =
        String.format("UPDATE %s.%s SET bucket_size = ?, trace_events = ? WHERE node_id = ? AND txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_TRACE =
        String.format("SELECT txn_id, bucket_size, trace_events FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_TRACE_REMOTE =
        String.format("SELECT node_id, txn_id, bucket_size, trace_events FROM %s.%s WHERE node_id = ? AND txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE1 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACE);

    private static final String UNSET_TRACE1_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACE);

    private static final String QUERY_ALL_TRACES =
        String.format("SELECT * FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String QUERY_TRACES =
        String.format("SELECT * FROM %s.%s WHERE txn_id = ? AND event = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String QUERY_TRACES_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND txn_id = ? AND event = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES1 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ? AND id_micros < ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES1_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ? AND id_micros < ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES3 =
        String.format("DELETE FROM %s.%s WHERE txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String TRUNCATE_TRACES =
        String.format("TRUNCATE %s.%s", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_TRACES);

    private static final String ERASE_TRACES3_REMOTE =
        String.format("DELETE FROM %s.%s WHERE node_id = ? AND txn_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.TXN_TRACES);

    private static final String QUERY_REDUNDANT_BEFORE =
        String.format("SELECT * FROM %s.%s where table_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND table_id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ =
        String.format("SELECT * FROM %s.%s WHERE table_id = ? AND quorum_applied", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND table_id = ? AND quorum_applied", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ =
        String.format("SELECT * FROM %s.%s WHERE table_id = ? AND shard_applied", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE =
        String.format("SELECT * FROM %s.%s WHERE node_id = ? AND table_id = ? AND shard_applied", SchemaConstants.VIRTUAL_ACCORD_DEBUG_REMOTE, AccordDebugKeyspace.REDUNDANT_BEFORE);

    private static final String SET_PATTERN_TRACE =
        String.format("UPDATE %s.%s SET bucket_mode = ?, bucket_seen = ?, bucket_size = ?, chance = ?, if_intersects = ?, if_kind = ?, on_failure = ?, on_new = ?, trace_bucket_mode = ?, trace_bucket_size = ?, trace_bucket_sub_size = ?, trace_events = ? WHERE id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_PATTERN_TRACE);

    private static final String UNSET_PATTERN_TRACE =
        String.format("DELETE FROM %s.%s WHERE id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_PATTERN_TRACE);

    private static final String QUERY_PATTERN_TRACE =
        String.format("SELECT * FROM %s.%s WHERE id = ?", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.TXN_PATTERN_TRACE);

    private static final String QUERY_SHARD_EPOCHS =
        String.format("SELECT * FROM %s.%s", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.SHARD_EPOCHS);

    private static final String QUERY_LISTENERS_DEPS =
        String.format("SELECT * FROM %s.%s", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.LISTENERS_DEPS);

    @BeforeClass
    public static void setUpClass()
    {
        ProtocolModifiers.Configure.setPermittedFastPaths(new TxnId.FastPaths(Unoptimised));
        ProtocolModifiers.Configure.setSendStableMessages(TO_ALL);
        ProtocolModifiers.Configure.setPermitCoordinatorLocalExecution(false);
        ProtocolModifiers.Configure.setPermitLocalDelivery(false);
        Config.setOverrideLoadConfig(() -> {
            Config config = new YamlConfigurationLoader().loadConfig();
            config.accord.queue_shard_count = new OptionaldPositiveInt(1);
            config.accord.queue_thread_count = new OptionaldPositiveInt(1);
            config.accord.command_store_shard_count = new OptionaldPositiveInt(1);
            config.accord.enable_virtual_debug_only_keyspace = true;
            config.accord.permit_fast_quorum_medium_path = true;
            return config;
        });
        daemonInitialization();

        CQLTester.setUpClass();
        CassandraDaemon.getInstanceForTesting().setupVirtualKeyspaces();

        AccordService.localStartup(ClusterMetadata.current().myNodeId());
        AccordService.distributedStartup();
        requireNetwork();
    }

    @After
    public void afterTest() throws Throwable
    {
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
            Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
            filter.appliesTo(id);

            execute(SET_TRACE, 1, "{WaitProgress}", id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "{WaitProgress}"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()), row(nodeId, id.toString(), 1, "{WaitProgress}"));
            execute(SET_TRACE, 0, "{}", id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()));
            execute(SET_TRACE, 1, "{WaitProgress}", id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "{WaitProgress}"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()), row(nodeId, id.toString(), 1, "{WaitProgress}"));
            execute(UNSET_TRACE1, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()));
            execute(SET_TRACE, 1, "{WaitProgress}", id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "{WaitProgress}"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()), row(nodeId, id.toString(), 1, "{WaitProgress}"));
            accord.node().coordinate(id, txn).beginAsResult();
            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            filter.apply.awaitThrowUncheckedOnInterrupt();
            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "WaitProgress").size()).isGreaterThan(0));
            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_TRACES_REMOTE, nodeId, id.toString(), "WaitProgress").size()).isGreaterThan(0));
            execute(ERASE_TRACES1, id.toString(), Long.MAX_VALUE);
            execute(ERASE_TRACES1, id.toString(), Long.MAX_VALUE);
            Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "WaitProgress").size()).isEqualTo(0);
            Assertions.assertThat(execute(QUERY_TRACES_REMOTE, nodeId, id.toString(), "WaitProgress").size()).isEqualTo(0);
            // just check other variants don't fail
            execute(ERASE_TRACES3, id.toString());

        }
        finally
        {
            MessagingService.instance().outboundSink.remove(filter);
        }

        filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 1, 1, 1);
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
            filter.appliesTo(id);

            execute(SET_TRACE_REMOTE, 1, "{WaitProgress}", nodeId, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "{WaitProgress}"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()), row(nodeId, id.toString(), 1, "{WaitProgress}"));
            execute(SET_TRACE_REMOTE, 0, "{}", nodeId, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()));
            execute(SET_TRACE_REMOTE, 1, "{WaitProgress}", nodeId, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "{WaitProgress}"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()), row(nodeId, id.toString(), 1, "{WaitProgress}"));
            execute(UNSET_TRACE1_REMOTE, nodeId, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()));
            execute(SET_TRACE_REMOTE, 1, "{WaitProgress}", nodeId, id.toString());
            assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "{WaitProgress}"));
            assertRows(execute(QUERY_TRACE_REMOTE, nodeId, id.toString()), row(nodeId, id.toString(), 1, "{WaitProgress}"));
            accord.node().coordinate(id, txn).beginAsResult();
            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            filter.apply.awaitThrowUncheckedOnInterrupt();
            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "WaitProgress").size()).isGreaterThan(0));
            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_TRACES_REMOTE, nodeId, id.toString(), "WaitProgress").size()).isGreaterThan(0));
            execute(ERASE_TRACES1_REMOTE, nodeId, id.toString(), Long.MAX_VALUE);
            execute(ERASE_TRACES1_REMOTE, nodeId, id.toString(), Long.MAX_VALUE);
            Assertions.assertThat(execute(QUERY_TRACES, id.toString(), "WaitProgress").size()).isEqualTo(0);
            Assertions.assertThat(execute(QUERY_TRACES_REMOTE, nodeId, id.toString(), "WaitProgress").size()).isEqualTo(0);
            // just check other variants don't fail
            execute(ERASE_TRACES3_REMOTE, nodeId, id.toString());

        }
        finally
        {
            MessagingService.instance().outboundSink.remove(filter);
        }
    }

    @Test
    public void patternTracing()
    {
        // simple test to confirm basic tracing functionality works, doesn't validate specific behaviours only requesting/querying/erasing
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        AccordService accord = accord();
        DatabaseDescriptor.getAccord().fetch_txn = "1s";

        execute(SET_PATTERN_TRACE, "leaky", 0, 5, 1.0f, "tid:1:1|tid:1:2", "-{*X}", "-{WaitProgress}", "{}", "ring", 5, 1, "*", 1);
        assertRows(execute(QUERY_PATTERN_TRACE, 1), row(1, "LEAKY", 0, 5, 1.0f, 0, "tid:1:1|tid:1:2", "-{KX,RX}", "-{WaitProgress}", "{}", "RING", 5, 1, "*"));
        execute(UNSET_PATTERN_TRACE, 1);
        assertRows(execute(QUERY_PATTERN_TRACE, 1));

        RoutingKey matchKey;
        {
            Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
            matchKey = (RoutingKey) txn.keys().toParticipants().get(0);
        }

        int count = 5;
        {
            List<TxnId> txnIds = new ArrayList<>();
            execute(SET_PATTERN_TRACE, "leaky", 0, count, 1.0f, matchKey.toString(), "*", "{}", "*", "leaky", 1, 1, "*", 1);
            for (int i = 0 ; i < count + 1 ; ++i)
            {
                Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, i, 0);
                TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
                getBlocking(accord.node().coordinate(id, txn));
                if (i < count) assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "*"));
                else assertRows(execute(QUERY_TRACE, id.toString()));
                txnIds.add(id);
            }

            execute(UNSET_PATTERN_TRACE, 1);
            for (int i = 0 ; i < count ; ++i)
                assertRows(execute(QUERY_TRACE, txnIds.get(i).toString()));
        }

        {
            execute(SET_PATTERN_TRACE, "leaky", 0, count, 1.0f, matchKey.asRange().toString(), "{KE}", "{}", "{PreAccept}", "leaky", 1, 1, "*", 1);
            for (int i = 0 ; i < count ; ++i)
            {
                Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, i, 0);
                TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
                getBlocking(accord.node().coordinate(id, txn));
                assertRows(execute(QUERY_TRACE, id.toString()));
            }

            List<TxnId> txnIds = new ArrayList<>();
            for (int i = 0 ; i < count + 1 ; ++i)
            {
                Txn txn = createTxn(wrapInTxn(String.format("SELECT * FROM %s.%s WHERE k = ? AND c = ?", KEYSPACE, tableName)), 0, i);
                TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.EphemeralRead, Routable.Domain.Key);
                getBlocking(accord.node().coordinate(id, txn));
                if (i < count) assertRows(execute(QUERY_TRACE, id.toString()), row(id.toString(), 1, "*"));
                else assertRows(execute(QUERY_TRACE, id.toString()));
                txnIds.add(id);
            }

            execute(UNSET_PATTERN_TRACE, 1);
            for (int i = 0 ; i < count ; ++i)
                assertRows(execute(QUERY_TRACE, txnIds.get(i).toString()));
        }

        {
            Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 1, 1, 1);
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
            execute(SET_PATTERN_TRACE, "leaky", 0, count, 1.0f, "" + txn.keys().get(0).toUnseekable(), "{KW}", "*", "{}", "leaky", 1, 1, "{}", 1);

            AccordMsgFilter filter = new AccordMsgFilter();
            filter.dropVerbs = EnumSet.allOf(Verb.class);
            filter.appliesTo(id);
            MessagingService.instance().outboundSink.add(filter);
            try
            {
                boolean failed = false;
                try { getBlocking(accord.node().coordinate(id, txn)); }
                catch (Throwable ignore) { failed = true; }
                Assertions.assertThat(failed).isTrue();
            }
            finally
            {
                MessagingService.instance().outboundSink.remove(filter);
            }

            spinUntilSuccess(() -> Assertions.assertThat(execute(QUERY_ALL_TRACES, id.toString()).size()).isGreaterThan(0), 60);
            execute(UNSET_PATTERN_TRACE, 1);
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

        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE, tableId.toString()).size()).isGreaterThan(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " >= ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(2);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " >= ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " >= ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " >= ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " > ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " > ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " > ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " > ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " <= ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(3);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " <= ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(4);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " <= ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(4);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " <= ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(4);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " < ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(2);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ + " < ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(3);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " < ?", tableId.toString(), syncId1.toString()).size()).isEqualTo(3);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ + " < ?", tableId.toString(), syncId2.toString()).size()).isEqualTo(4);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_REMOTE, nodeId, tableId.toString()).size()).isGreaterThan(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE +  " >= ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(2);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE +  " >= ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE +  " >= ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE + " >= ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE + " > ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(1);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE + " > ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE + " > ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE + " > ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(0);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE + " <= ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(3);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE + " <= ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(4);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE + " <= ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(4);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE + " <= ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(4);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE + " < ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(2);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_QUORUM_APPLIED_INEQ_REMOTE + " < ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(3);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE + " < ?", nodeId, tableId.toString(), syncId1.toString()).size()).isEqualTo(3);
        Assertions.assertThat(execute(QUERY_REDUNDANT_BEFORE_FILTER_SHARD_APPLIED_INEQ_REMOTE + " < ?", nodeId, tableId.toString(), syncId2.toString()).size()).isEqualTo(4);
    }

    @Test
    public void reportInvalidRequestForUnsupportedRemoteToLocal()
    {
        AccordService accord = accord();
        int nodeId = accord.nodeId().id;
        TxnId id = accord.node().nextTxnIdWithDefaultFlags(Keys.of(), Txn.Kind.Write, Routable.Domain.Key);
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
    public void completedTxn()
    {
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        AccordService accord = accord();
        int nodeId = accord.nodeId().id;
        AccordMsgFilter filter = new AccordMsgFilter();
        Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
        TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
        filter.appliesTo(id);
        filter.dropVerbs = Set.of();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String keyStr = txn.keys().get(0).toUnseekable().toString();
            getBlocking(accord.node().coordinate(id, txn));
            filter.apply.awaitThrowUncheckedOnInterrupt();
            spinUntilSuccess(() -> assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                                              row(id.toString(), 0, anyInt(), "", "", any(), "Applied")));
            assertRows(execute(QUERY_TXN, id.toString()), row(id.toString(), "Applied"));
            assertRows(execute(QUERY_TXN_REMOTE, nodeId, id.toString()), row(id.toString(), "Applied"));
            assertRows(execute(QUERY_JOURNAL, id.toString()), row(id.toString(), "PreAccepted"), row(id.toString(), "Applying"), row(id.toString(), "Applied"), row(id.toString(), null));
            assertRows(execute(QUERY_JOURNAL_REMOTE, nodeId, id.toString()), row(id.toString(), "PreAccepted"), row(id.toString(), "Applying"), row(id.toString(), "Applied"), row(id.toString(), null));
            assertRows(execute(QUERY_COMMANDS_FOR_KEY, keyStr), row(id.toString(), "APPLIED_DURABLE"));
            assertRows(execute(QUERY_COMMANDS_FOR_KEY_REMOTE, nodeId, keyStr), row(id.toString(), "APPLIED_DURABLE"));
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(filter);
        }
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
        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            AccordService accord = accord();
            int nodeId = accord.nodeId().id;
            String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                             "    LET r = (SELECT * FROM %s.%s WHERE k = ? AND c = ?);\n" +
                                             "    IF r IS NULL THEN\n " +
                                             "        INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                             "    END IF\n" +
                                             "COMMIT TRANSACTION", KEYSPACE, tableName, KEYSPACE, tableName);
            Txn txn = createTxn(insertTxn, 0, 0, 0, 0, 0);
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
            filter.appliesTo(id);
            accord.node().coordinate(id, txn).beginAsResult();

            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), 0, anyInt(), "", "", any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, id.toString()),
                       row(nodeId, id.toString(), 0, anyInt(), "", "", any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), 0, anyInt(), "", "", any(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, id.toString()),
                       row(nodeId, id.toString(), 0, anyInt(), "", "", any(), SaveStatus.ReadyToExecute.name()));
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
        AccordMsgFilter filter = new AccordMsgFilter();
        MessagingService.instance().outboundSink.add(filter);
        try
        {
            String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            AccordService accord = accord();
            int nodeId = accord.nodeId().id;
            String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                             "    LET r = (SELECT * FROM %s.%s WHERE k = ? AND c = ?);\n" +
                                             "    IF r IS NULL THEN\n " +
                                             "        INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                             "    END IF\n" +
                                             "COMMIT TRANSACTION", KEYSPACE, tableName, KEYSPACE, tableName);
            Txn txn = createTxn(insertTxn, 0, 0, 0, 0, 0);
            TxnId first = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);

            filter.appliesTo(first);
            accord.node().coordinate(first, txn).beginAsResult();

            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), 0, anyInt(), "", any(), any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, first.toString()),
                       row(nodeId, first.toString(), 0, anyInt(), "", any(), any(), anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));
            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), 0, anyInt(), "", any(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, first.toString()),
                       row(nodeId, first.toString(), 0, anyInt(), "", any(), anyNonNull(), SaveStatus.ReadyToExecute.name()));

            filter.reset();

            TxnId second = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
            filter.reset();
            filter.appliesTo(second);
            logger.info("{}", second);
            accord.node().coordinate(second, txn).beginAsResult();

            filter.commit.awaitThrowUncheckedOnInterrupt();

            Awaitility.await("waiting on key").atMost(1, TimeUnit.MINUTES)
                                          .until(() -> {
                                              UntypedResultSet rs = execute(QUERY_TXN_BLOCKED_BY, second.toString());
                                              return rs.size() == 2;
                                          });
            assertRows(execute(QUERY_TXN_BLOCKED_BY, second.toString()),
                       row(second.toString(), 0, anyInt(), "", "", anyNonNull(), SaveStatus.Stable.name()),
                       row(second.toString(), 1, anyInt(), first.toString(), any(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY + " AND depth < 1", second.toString()),
                       row(second.toString(), 0, anyInt(), "", any(), anyNonNull(), SaveStatus.Stable.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE, nodeId, second.toString()),
                       row(nodeId, second.toString(), 0, anyInt(), "", "", anyNonNull(), SaveStatus.Stable.name()),
                       row(nodeId, second.toString(), 1, anyInt(), first.toString(), any(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
            assertRows(execute(QUERY_TXN_BLOCKED_BY_REMOTE + " AND depth < 1", nodeId, second.toString()),
                       row(nodeId, second.toString(), 0, anyInt(), "", any(), anyNonNull(), SaveStatus.Stable.name()));
        }
        finally
        {
            filter.reset();
            MessagingService.instance().outboundSink.remove(filter);
        }
    }

    // TODO (expected): test graph_all (though mostly shared logic)
    // TODO (required): we have some bug with visiting same txn twice via multiple intersecting dependency relations; test and fix
    @Test
    public void graph() throws TopologyException
    {
        AccordService accord = accord();
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        String insertTxn = String.format("BEGIN TRANSACTION\n" +
                                         "    LET r = (SELECT * FROM %s.%s WHERE k = ? AND c = ?);\n" +
                                         "    IF r IS NULL THEN\n " +
                                         "        INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?);\n" +
                                         "    END IF\n" +
                                         "COMMIT TRANSACTION", KEYSPACE, tableName, KEYSPACE, tableName);
        Txn txa = createTxn(insertTxn, 0, 0, 0, 0, 0);
        AccordCommandStore commandStore = (AccordCommandStore) accord.node().commandStores().unsafeForKey((RoutingKey) txa.keys().get(0).toUnseekable());
        // TODO (expected): test multi-key transactions (though functionally the same as range txns)
        Txn txb, txc;
        {
            int i = 0;
            Txn tmp;
            do { ++i; tmp = createTxn(insertTxn, i, 0, i, 0, 0); }
            while (!commandStore.unsafeGetRangesForEpoch().all().contains(tmp.keys().get(0).asKey()));
            txb = tmp;
            txc = createTxn(insertTxn, i, 0, 0, 0, 0);
        }

        long epoch = accord.currentEpoch();

        TxnId[] ida = ids(epoch, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1), 1, 2, 3, 4, 6);
        SaveStatus[] ssa = new SaveStatus[] { SaveStatus.PreAccepted, SaveStatus.Committed, SaveStatus.Committed, SaveStatus.Committed, SaveStatus.Committed };
        Timestamp[] tsa = toTimestamps(ida);
        tsa[2] = Timestamp.fromValues(epoch, 5, new Node.Id(1));
        FullRoute<?> rta = accord.node().computeRoute(ida[0], txa.keys().toParticipants());

        TxnId[] idb = ids(epoch, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(2), 1, 2, 3, 4, 6);
        SaveStatus[] ssb = new SaveStatus[] { SaveStatus.PreAccepted, SaveStatus.PreAccepted, SaveStatus.Committed, SaveStatus.Committed, SaveStatus.Committed };
        Timestamp[] tsb = toTimestamps(idb);
        tsb[2] = Timestamp.fromValues(epoch, 5, new Node.Id(2));
        FullRoute<?> rtb = accord.node().computeRoute(idb[0], txb.keys().toParticipants());

        TxnId[] ide = ids(epoch, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, new Node.Id(3), 1, 2, 3, 4, 6);
        SaveStatus[] sse = new SaveStatus[] { SaveStatus.Committed, SaveStatus.Committed, SaveStatus.Committed, SaveStatus.Committed, SaveStatus.Committed };
        Timestamp[] tse = toTimestamps(ide);
        Txn txe = accord.node().agent().emptySystemTxn(Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range);
        FullRoute<?> rte = accord.node().computeRoute(ide[0], txa.keys().toParticipants().toRanges().with(txb.keys().toParticipants().toRanges()));

        write(commandStore, preaccepted(rta, ida[0], tsa[0], txa));
        write(commandStore, committed(rta, ida[1], tsa[1], txa, ida, 0));
        write(commandStore, committed(rta, ida[2], tsa[2], txa, ida, 0, 1, 3));
        write(commandStore, committed(rta, ida[3], tsa[3], txa, ida, 0, 1, 2));
        write(commandStore, committed(rta, ida[4], tsa[4], txa, ida, 0, 1, 2, 3));

        write(commandStore, preaccepted(rtb, idb[0], tsb[0], txb));
        write(commandStore, preaccepted(rtb, idb[1], tsb[1], txb));
        write(commandStore, committed(rtb, idb[2], tsb[2], txb, idb, 0, 1, 3));
        write(commandStore, committed(rtb, idb[3], tsb[3], txb, idb, 0, 1, 2));
        write(commandStore, committed(rtb, idb[4], tsb[4], txb, idb, 0, 1, 2, 3));

        write(commandStore, committed1(rte, ide[0], ide[0], txe, rta, ida, new int[] { 0 }, rtb, idb, new int[] { 0 }, rte, ide, new int[] { }));
        write(commandStore, committed1(rte, ide[1], ide[1], txe, rta, ida, new int[] { 0, 1 }, rtb, idb, new int[] { 0, 1 }, rte, ide, new int[] { 0 }));
        write(commandStore, committed1(rte, ide[2], ide[2], txe, rta, ida, new int[] { 0, 1, 2 }, rtb, idb, new int[] { 0, 1, 2 }, rte, ide, new int[] { 0, 1 }));

        // txn_id, depth, command_store_id, parent_txn_id, execute_at, child_txn_id
        assertRows(execute(QUERY_TXN_GRAPH, ida[0].toString()));
        assertRows(execute(QUERY_TXN_GRAPH, ida[1].toString()), graphRows(commandStore, txa, ida, ssa, tsa, 1, 0));
        assertRows(execute(QUERY_TXN_GRAPH, ida[2].toString()), graphRows(commandStore, txa, ida, ssa, tsa, 2, 3, 0, 1));
        assertRows(execute(QUERY_TXN_GRAPH, ida[3].toString()), graphRows(commandStore, txa, ida, ssa, tsa, 3, 1, 0));
        assertRows(execute(QUERY_TXN_GRAPH, ida[4].toString()), graphRows(commandStore, txa, ida, ssa, tsa, 4, 2, 0, 3, 1));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, ida[4].toString(), rta.homeKey().toString()), graphRows(commandStore, txa, ida, ssa, tsa, 4, 2, 0, 3, 1));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, ida[4].toString(), rtb.homeKey().toString()));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, ida[4].toString(), ""));
        assertRows(execute(QUERY_TXN_GRAPH_KIND, ida[4].toString(), "{R*}"));
        assertRows(execute(QUERY_TXN_GRAPH_KIND, ida[4].toString(), "{*R}"));
        assertRows(execute(QUERY_TXN_GRAPH_KIND, ida[4].toString(), "{KW}"), graphRows(commandStore, txa, ida, ssa, tsa, 4, 2, 0, 3, 1));

        assertRows(execute(QUERY_TXN_GRAPH, idb[0].toString()));
        assertRows(execute(QUERY_TXN_GRAPH, idb[1].toString()));
        assertRows(execute(QUERY_TXN_GRAPH, idb[2].toString()), graphRows(commandStore, txb, idb, ssb, tsb, 2, 3, 1, 0));
        assertRows(execute(QUERY_TXN_GRAPH, idb[3].toString()), graphRows(commandStore, txb, idb, ssb, tsb, 3, 1, 0));
        assertRows(execute(QUERY_TXN_GRAPH, idb[4].toString()), graphRows(commandStore, txb, idb, ssb, tsb, 4, 2, 1, 0, 3));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, idb[4].toString(), rtb.homeKey().toString()), graphRows(commandStore, txb, idb, ssb, tsb, 4, 2, 1, 0, 3));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, idb[4].toString(), rta.homeKey().toString()));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, idb[4].toString(), ""));

        FullRoute[] rts = new FullRoute[] { rta, rtb, rte };
        TxnId[][] ids = new TxnId[][] { ida, idb, ide };
        Timestamp[][] tss = new Timestamp[][] { tsa, tsb, tse };
        SaveStatus[][] sss = new SaveStatus[][] { ssa, ssb, sse };

        int[] ide0_rows = new int[]
                          {
                          0, 2, 0, 1, 0,
                          0, 2, 0, 0, 0
                          };

        assertRows(execute(QUERY_TXN_GRAPH, ide[0].toString()), graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[0], ide0_rows));
        assertRows(execute(QUERY_TXN_GRAPH_REMOTE, 1, ide[0].toString()), prepend(new Object[] { 1 }, graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[0], ide0_rows)));

        int[] ide1_rows = new int[]
                          {
                          0, 2, 1, 0, 1,
                          0, 2, 1, 2, 0,
                          0, 2, 1, 1, 1,
                          0, 2, 1, 1, 0,
                          0, 2, 1, 0, 0
                          };

        assertRows(execute(QUERY_TXN_GRAPH, ide[1].toString()), graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[1], ide1_rows));
        assertRows(execute(QUERY_TXN_GRAPH_REMOTE, 1, ide[1].toString()), prepend(new Object[] { 1 }, graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[1], ide1_rows)));

        int[] ide2_rows = new int[]
        {
            0, 2, 2, 1, 2,
            0, 2, 2, 0, 2,
            0, 2, 2, 2, 1,
            0, 2, 2, 1, 1,
            0, 2, 2, 1, 0,
            0, 2, 2, 0, 0,
            1, 1, 2, 1, 3,
            1, 0, 2, 0, 3,
            1, 2, 1, 0, 1,
            1, 2, 1, 2, 0,
            2, 0, 3, 0, 1
        };

        assertRows(execute(QUERY_TXN_GRAPH, ide[2].toString()), graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_DESC, ide[2].toString()), graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[2], reverse(ide2_rows, 5)));
        assertRows(execute(QUERY_TXN_GRAPH_REMOTE, 1, ide[2].toString()), prepend(new Object[] { 1 }, graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[2], ide2_rows)));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, ide[2].toString(), AccordDebugKeyspace.toString(rte)), graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, ide[2].toString(), ""));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, ide[2].toString(), AccordDebugKeyspace.toString(rta)), graphRows(commandStore, rts, ids, sss, tss, ALL, rta, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS, ide[2].toString(), AccordDebugKeyspace.toString(rtb)), graphRows(commandStore, rts, ids, sss, tss, ALL, rtb, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_KIND, ide[2].toString(), "{KR}"));
        assertRows(execute(QUERY_TXN_GRAPH_KIND, ide[2].toString(), "{KW,RX}"), graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_KIND, ide[2].toString(), "{RX}"), graphRows(commandStore, rts, ids, sss, tss, TxnKindsAndDomains.parse("{RX}"), null, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_KIND, ide[2].toString(), "{KW}"), graphRows(commandStore, rts, ids, sss, tss, TxnKindsAndDomains.parse("{KW}"), null, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS_AND_KIND, ide[2].toString(), AccordDebugKeyspace.toString(rta), "{KW}"), graphRows(commandStore, rts, ids, sss, tss, TxnKindsAndDomains.parse("{KW}"), rta, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_INTERSECTS_AND_KIND, ide[2].toString(), AccordDebugKeyspace.toString(rtb), "{KW}"), graphRows(commandStore, rts, ids, sss, tss, TxnKindsAndDomains.parse("{KW}"), rtb, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_MIN, ide[2].toString(), ida[0].toString()), graphRows(commandStore, rts, ids, sss, tss, ALL, null, null, ide[2], ide2_rows));
        assertRows(execute(QUERY_TXN_GRAPH_MIN, ide[2].toString(), ida[1].toString()), graphRows(commandStore, rts, ids, sss, tss, ALL, null, ida[1], ide[2], ide2_rows));
    }

    private static int[] reverse(int[] copy, int stride)
    {
        int[] tmp = new int[stride];
        copy = copy.clone();
        for (int i = 0 ; i < copy.length/2 ; i += 5)
        {
            int j = copy.length - (stride + i);
            System.arraycopy(copy, i, tmp, 0, stride);
            System.arraycopy(copy, j, copy, i, stride);
            System.arraycopy(tmp, 0, copy, j, stride);
        }
        return copy;
    }

    private static Object[][] graphRows(CommandStore commandStore, Txn txn, TxnId[] ids, SaveStatus[] saveStatuses, Timestamp[] timestamps, int pk, int ... children)
    {
        int depth = 0;
        Object[][] result = new Object[children.length][];
        int nextParent = Integer.MAX_VALUE;
        int parent = pk;
        for (int i = 0 ; i < children.length ; i++)
        {
            int child = children[i];
            if (saveStatuses[child].hasBeen(Status.Committed))
            {
                if (i > 0)
                {
                    ++depth;
                    parent = nextParent;
                }
                nextParent = child;
            }

            result[i] = row(ids[pk].toString(), depth, commandStore.id(), ids[parent].toString(),
                            saveStatuses[child].hasBeen(Status.Committed) ? timestamps[child].toString() : "",
                            ids[child].toString(), saveStatuses[child].toString(), txn.keys().toParticipants().toString());
        }
        return result;
    }

    private static Object[][] prepend(Object[] prefix, Object[][] rows)
    {
        Object[][] result = new Object[rows.length][];
        for (int i = 0 ; i < result.length ; ++i)
        {
            result[i] = new Object[rows[i].length + prefix.length];
            System.arraycopy(prefix, 0, result[i], 0, prefix.length);
            System.arraycopy(rows[i], 0, result[i], prefix.length, rows[i].length);
        }
        return result;
    }

    private static Object[][] graphRows(CommandStore commandStore, FullRoute[] rts,
                                        TxnId[][] ids, SaveStatus[][] saveStatuses, Timestamp[][] timestamps,
                                        TxnKindsAndDomains kinds, Participants<?> intersecting, @Nullable TxnId min,
                                        TxnId pk, int ... children)
    {
        int count = 0;
        Object[][] result = new Object[children.length/5][];
        for (int i = 0 ; i < children.length ; i+=5)
        {
            int depth = children[i];
            int parentGroup = children[i + 1];
            int parent = children[i + 2];
            int childGroup = children[i + 3];
            int child = children[i + 4];

            TxnId childId = ids[childGroup][child];
            TxnId parentId = ids[parentGroup][parent];
            if (!kinds.matches(childId) || (!kinds.matches(parentId) && !parentId.equals(pk)))
                continue;

            if (intersecting != null && !intersecting.intersects(rts[childGroup]))
                continue;

            if (min != null && min.compareTo(childId) > 0)
                continue;

            Participants<?> via = rts[childGroup].participantsOnly().intersecting(rts[parentGroup], Minimal);
            if (intersecting != null)
                via = via.intersecting(intersecting, Minimal);

            result[count++] = row(pk.toString(), depth, commandStore.id(), parentId.toString(),
                                  saveStatuses[childGroup][child].hasBeen(Status.Committed) ? timestamps[childGroup][child].toString() : "",
                                  childId.toString(), saveStatuses[childGroup][child].toString(), via.toString());
        }
        if (count != result.length)
            result = Arrays.copyOf(result, count);
        return result;
    }

    private static Timestamp[] toTimestamps(TxnId[] ids)
    {
        Timestamp[] ts = new Timestamp[ids.length];
        System.arraycopy(ids, 0, ts, 0, ids.length);
        return ts;
    }

    private static TxnId[] ids(long epoch, Txn.Kind kind, Routable.Domain domain, Node.Id id, long ... hlcs)
    {
        TxnId[] ids = new TxnId[hlcs.length];
        for (int i = 0 ; i < hlcs.length ; ++i)
            ids[i] = new TxnId(epoch, hlcs[i], kind, domain, id);
        return ids;
    }

    private static void write(AccordCommandStore commandStore, Command command)
    {
        commandStore.journal.saveCommand(commandStore.id(), new Journal.CommandUpdate(null, command), ()->{});
    }

    private static Command committed(FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, TxnId[] ids, int ... deps)
    {
        Arrays.sort(deps);
        try (KeyDeps.Builder keys = KeyDeps.builder(); RangeDeps.BuilderByTxnId ranges = RangeDeps.byTxnIdBuilder())
        {
            for (int depIndex : deps)
            {
                TxnId dep = ids[depIndex];
                if (dep.is(Routable.Domain.Key)) keys.add(route.homeKey(), dep);
                else ranges.add(route.homeKey().asRange(), dep);
            }
            PartialDeps partialDeps = new PartialDeps(route, keys.build(), ranges.build());
            return Command.Committed.committed(txnId, SaveStatus.Committed, NotDurable, StoreParticipants.all(route), Ballot.ZERO, executeAt, txn.intersecting(route, true), partialDeps, Ballot.ZERO, null);
        }
    }

    private static Command committed1(FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Object ... triples)
    {
        try (KeyDeps.Builder keys = KeyDeps.builder(); RangeDeps.BuilderByTxnId ranges = RangeDeps.byTxnIdBuilder())
        {
            for (int i = 0 ; i < triples.length ; i+= 3)
            {
                FullRoute<?> rt = (FullRoute<?>) triples[i];
                TxnId[] ids = (TxnId[]) triples[i + 1];
                int[] deps = (int[]) triples[i + 2];
                for (int depIndex : deps)
                {
                    TxnId dep = ids[depIndex];
                    if (dep.is(Routable.Domain.Key)) keys.add(rt.homeKey(), dep);
                    else
                    {
                        for (Range range : rt.toRanges())
                            ranges.add(range, dep);
                    }
                }
            }
            PartialDeps partialDeps = new PartialDeps(route, keys.build(), ranges.build());
            return Command.Committed.committed(txnId, SaveStatus.Committed, NotDurable, StoreParticipants.all(route), Ballot.ZERO, executeAt, txn.intersecting(route, true), partialDeps, Ballot.ZERO, null);
        }
    }

    private static Command preaccepted(FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn) throws TopologyException
    {
        return Command.PreAccepted.preaccepted(txnId, SaveStatus.PreAccepted, NotDurable, StoreParticipants.all(route), Ballot.ZERO, executeAt, txn.intersecting(route, true), null);
    }

    @Ignore
    @Test
    public void patchJournalVestigialTest()
    {
        testPatchJournal("LOCALLY_ERASE_VESTIGIAL", "Vestigial");
    }

    @Test
    public void patchJournalInvalidateTest()
    {
        testPatchJournal("LOCALLY_INVALIDATE", "Invalidated");
    }

    @Test
    public void patchJournalTruncateTest()
    {
        try
        {
            testPatchJournal("ERASE", "Erased");
            Assert.fail("Should have thrown");
        }
        catch (InvalidRequestException t)
        {
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
            Txn txn = createTxn(insertTxn, 0, 0, 0);
            TxnId id = accord.node().nextTxnIdWithDefaultFlags(txn.keys(), Txn.Kind.Write, Routable.Domain.Key);
            filter.appliesTo(id);
            accord.node().coordinate(id, txn).beginAsResult();

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

    @Test
    public void testShardEpochsTable()
    {
        String table1 = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        String table2 = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        UntypedResultSet rs = execute(QUERY_SHARD_EPOCHS);
        Assert.assertTrue(rs.size() > 1);
    }

    private static AccordService accord()
    {
        return (AccordService) AccordService.instance();
    }

    private static class AccordMsgFilter implements OutboundSink.Filter
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
        public boolean test(Message<?> msg, InetAddressAndPort to, ConnectionType type)
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