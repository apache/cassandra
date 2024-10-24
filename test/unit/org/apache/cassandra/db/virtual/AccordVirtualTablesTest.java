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

import accord.primitives.SaveStatus;
import accord.messages.TxnRequest;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OptionaldPositiveInt;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.Condition;
import org.awaitility.Awaitility;

import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class AccordVirtualTablesTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AccordVirtualTablesTest.class);

    private static final String QUERY_TXN_BLOCKED_BY = "SELECT * FROM system_views.txn_blocked_by WHERE txn_id=?";
    private static final String QUERY_TXN_STATUS = "SELECT save_status FROM system_views.txn_blocked_by WHERE txn_id=? LIMIT 1";

    @BeforeClass
    public static void setUpClass()
    {
        daemonInitialization();
        DatabaseDescriptor.getAccord().queue_shard_count = new OptionaldPositiveInt(1);
        DatabaseDescriptor.getAccord().command_store_shard_count = new OptionaldPositiveInt(1);

        CQLTester.setUpClass();

        AccordService.startup(ClusterMetadata.current().myNodeId());
        addVirtualKeyspace();
        requireNetwork();
    }

    @Test
    public void unknownIsEmpty()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        assertRows(execute(QUERY_TXN_BLOCKED_BY, TxnId.NONE.toString()));
    }

    @Test
    public void completedTxn() throws ExecutionException, InterruptedException
    {
        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        var accord = accord();
        TxnId id = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
        Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
        AsyncChains.getBlocking(accord.node().coordinate(id, txn));

        assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                   row(id.toString(), anyInt(), 0, "", "Self", any(), null, anyOf(SaveStatus.Applying.name(), SaveStatus.Applied.name())));
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
            Txn txn = createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0);
            accord.node().coordinate(id, txn);

            filter.preAccept.awaitThrowUncheckedOnInterrupt();

            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), anyInt(), 0, "", "Self", any(), null, anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));

            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, id.toString()),
                       row(id.toString(), anyInt(), 0, "", "Self", any(), null, SaveStatus.ReadyToExecute.name()));
        }
        finally
        {
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
            var accord = accord();
            TxnId first = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            accord.node().coordinate(first, createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0));

            filter.preAccept.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), anyInt(), 0, "", "Self", any(), null, anyOf(SaveStatus.PreAccepted.name(), SaveStatus.ReadyToExecute.name())));

            filter.apply.awaitThrowUncheckedOnInterrupt();
            assertRows(execute(QUERY_TXN_BLOCKED_BY, first.toString()),
                       row(first.toString(), anyInt(), 0, "", "Self", anyNonNull(), null, SaveStatus.ReadyToExecute.name()));

            filter.reset();

            TxnId second = accord.node().nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            accord.node().coordinate(second, createTxn(wrapInTxn(String.format("INSERT INTO %s.%s(k, c, v) VALUES (?, ?, ?)", KEYSPACE, tableName)), 0, 0, 0));

            filter.commit.awaitThrowUncheckedOnInterrupt();

            Awaitility.await("waiting on key").atMost(1, TimeUnit.MINUTES)
                                          .until(() -> {
                                              UntypedResultSet rs = execute(QUERY_TXN_BLOCKED_BY, second.toString());
                                              return rs.size() == 2;
                                          });
            assertRows(execute(QUERY_TXN_BLOCKED_BY, second.toString()),
                       row(second.toString(), anyInt(), 0, "", "Self", anyNonNull(), null, SaveStatus.Stable.name()),
                       row(second.toString(), anyInt(), 1, first.toString(), "Key", anyNonNull(), anyNonNull(), SaveStatus.ReadyToExecute.name()));
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
                txnId = ((TxnRequest) msg.payload).txnId;
            }
            Set<Verb> seen = null;
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
                    commit.signalAll();
                    return true;
                case ACCORD_PRE_ACCEPT_REQ:
                case ACCORD_CHECK_STATUS_REQ:
                case ACCORD_CHECK_STATUS_RSP:
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