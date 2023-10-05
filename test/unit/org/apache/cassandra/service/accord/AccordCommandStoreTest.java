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

package org.apache.cassandra.service.accord;
import accord.local.Node;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.config.CassandraRelevantProperties;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.api.Result;
import accord.impl.CommandsForKey;
import accord.impl.SafeState;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import com.fasterxml.jackson.databind.deser.std.UUIDDeserializer;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDGen;

import static accord.local.Status.Durability.Durable;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.db.ConsistencyLevel.NODE_LOCAL;
import static org.apache.cassandra.service.accord.AccordTestUtils.Commands.preaccepted;
import static org.apache.cassandra.service.accord.AccordTestUtils.ballot;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.loaded;
import static org.apache.cassandra.service.accord.AccordTestUtils.timestamp;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.*;
import static org.junit.Assert.assertEquals;

public class AccordCommandStoreTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStoreTest.class);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, names list<text>, primary key (k, c))", "ks"));
//        SchemaLoader.createKeyspace("ksList", KeyspaceParams.simple(1),
//                                    parse("CREATE TABLE tblList (i int, names list<text>, primary key (i))", "ksList"));
        StorageService.instance.initServer();
    }

    @Before
    public void setUp() throws Exception
    {
        Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);
    }

    @Test
    public void commandLoadSave() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        PartialTxn depTxn = createPartialTxn(0);
        Key key = (Key)depTxn.keys().get(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        PartialDeps dependencies;
        try (PartialDeps.Builder builder = PartialDeps.builder(depTxn.covering()))
        {
            builder.add(key, txnId(1, clock.incrementAndGet(), 1));
            dependencies = builder.build();
        }

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1)");
        TxnId oldTxnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTxnId2 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTimestamp = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        PartialTxn txn = createPartialTxn(0);
        attrs.homeKey(key.toUnseekable());
        attrs.progressKey(key.toUnseekable());
        attrs.durability(Durable);
        Ballot promised = ballot(1, clock.incrementAndGet(), 1);
        Ballot accepted = ballot(1, clock.incrementAndGet(), 1);
        Timestamp executeAt = timestamp(1, clock.incrementAndGet(), 1);
        attrs.partialDeps(dependencies);
        ImmutableSortedSet<TxnId> waitingOnCommit = ImmutableSortedSet.of(oldTxnId1);
        ImmutableSortedMap<Timestamp, TxnId > waitingOnApply = ImmutableSortedMap.of(oldTimestamp, oldTxnId2);
        attrs.addListener(new Command.Listener(oldTxnId1));
        Pair<Writes, Result> result = AccordTestUtils.processTxnResult(commandStore, txnId, txn, executeAt);
        Command command = Command.SerializerSupport.executed(attrs, SaveStatus.Applied, executeAt, promised, accepted,
                                                             waitingOnCommit, waitingOnApply, result.left, result.right);

        AccordSafeCommand safeCommand = new AccordSafeCommand(loaded(txnId, null));
        safeCommand.set(command);
        AccordKeyspace.getCommandMutation(commandStore, safeCommand, commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", command);
        Command actual = AccordKeyspace.loadCommand(commandStore, txnId);
        logger.info("A: {}", actual);

        Assert.assertEquals(command, actual);
    }

    @Test
    public void commandsForKeyLoadSave()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Timestamp maxTimestamp = timestamp(1, clock.incrementAndGet(), 1);

        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);

        Command command1 = preaccepted(txnId1, txn, timestamp(1, clock.incrementAndGet(), 1));
        Command command2 = preaccepted(txnId2, txn, timestamp(1, clock.incrementAndGet(), 1));

        AccordSafeCommandsForKey cfk = new AccordSafeCommandsForKey(loaded(key, null));
        cfk.initialize(CommandsForKeySerializer.loader);
        cfk.updateMax(maxTimestamp);

        cfk.updateLastExecutionTimestamps(txnId1, true);
        Assert.assertEquals(txnId1.hlc(), cfk.current().timestampMicrosFor(txnId1, true));

        cfk.updateLastExecutionTimestamps(txnId2, true);
        Assert.assertEquals(txnId2.hlc(), cfk.current().timestampMicrosFor(txnId2, true));

        Assert.assertEquals(txnId2, cfk.current().lastExecutedTimestamp());
        Assert.assertEquals(txnId2.hlc(), cfk.current().lastExecutedMicros());


        cfk.register(command1);
        cfk.register(command2);

        AccordKeyspace.getCommandsForKeyMutation(commandStore, cfk, commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", cfk);
        CommandsForKey actual = AccordKeyspace.loadCommandsForKey(commandStore, key);
        logger.info("A: {}", actual);

        Assert.assertEquals(cfk.current(), actual);
    }



    @Test
    public void insertListType() throws Throwable


    {
        AtomicLong clock = new AtomicLong(0);
        CassandraRelevantProperties.ENABLE_NODELOCAL_QUERIES.setBoolean(true);
        // When debugging by hand, timeout better be more than a few milliseconds...
        DatabaseDescriptor.setTransactionTimeout(20*60*1000*1000);
        AccordService.instance().createEpochFromConfigUnsafe();
        Node thisNode = ((AccordService)AccordService.instance()).node();
        AccordCommandStore commandStore = createAccordCommandStore(System::currentTimeMillis, "ks", "tbl");

        String accordQuery = "BEGIN TRANSACTION\n" +
                       "INSERT INTO ks.tbl (k, c, names) VALUES (10, 100, ['Henrik','Jarek','Branimir']);\n" +
                       "COMMIT TRANSACTION";

        String legacyQuery = "INSERT INTO ks.tbl (k, c, names) VALUES (10, 100, ['Henrik','Jarek','Branimir']);\n";
        String query = accordQuery;

        QueryState queryState = QueryState.forInternalCalls();
        // Must get a timestamp once, otherwise it will return Long.MIN_VALUE
        queryState.getTimestamp();
        // Btw, queryState.generatedTimestamp() will return whatever is the last used timestamp, without incrementing it


        ClientState clientState = queryState.getClientState();
        QueryOptions options =  QueryOptions.create(ConsistencyLevel.ONE, List.of(), false, 16, null, ConsistencyLevel.SERIAL, ProtocolVersion.CURRENT, "ks");
        logger.debug("options.getTimestamp(): {}",options.getTimestamp(queryState));
        logger.debug("{} {}",queryState.getClientState().getClientOptions(), queryState.getNowInSeconds());

        CQLStatement.Raw parsed = QueryProcessor.parseStatement(query);
        CQLStatement statement = parsed.prepare(clientState);

        logger.debug("{} {}",queryState.getClientState().getClientOptions(), queryState.getNowInSeconds());
        long queryStartNanoTime =System.nanoTime();
        long timestampOriginal = FBUtilities.timestampMicros();
        int nowInSecOriginal = (int) (timestampOriginal / 1000 / 1000);
        //long accordTimestampMicros = commandStore.nextSystemTimestampMicros();
        long accordTimestampMicros = queryState.generatedTimestamp();
        int accordNodeId = commandStore.id();
        Timestamp executeAt = timestamp(1, accordTimestampMicros, thisNode.id().id);
        TxnId txnId = txnId(1, accordTimestampMicros, thisNode.id().id);
        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        attrs.durability(Durable);

        logger.debug("Accord epoch: {}", AccordService.instance().topology().epoch());
        /////UntypedResultSet result = QueryProcessor.executeInternal(query);
        // AccordService.coordinate() will generate a new TxnId, i
        ResultMessage result = QueryProcessor.instance.process(statement, queryState, options, queryStartNanoTime);
        //ResultMessage result = statement.execute(queryState, options, queryStartNanoTime);
        //UntypedResultSet result = QueryProcessor.executeOnceInternalWithNowAndTimestamp(nowInSecOriginal, timestampOriginal, query);

        logger.debug(String.valueOf(result));
        logger.debug("{} {} {} {} {} {}", queryState.getClientState(), queryState.getNowInSeconds(), queryState.getTimestamp(), queryState.generatedNowInSeconds(), queryState.generatedTimestamp(), executeAt);
        logger.debug(String.valueOf(options));
        if(statement instanceof UpdateStatement)
        {
            logger.debug("Mutations: {}", ((UpdateStatement)statement).mutations);

            List<PartitionUpdate> modifications = ((Mutation) ((UpdateStatement)statement).mutations.get(0)).modifications.values().asList();
            Object[] cells = ((ComplexColumnData) ((BTreeRow) modifications.get(0).holder.tree[0]).btree[0]).cells;
            logger.info("cells: {}({})  Size per: {} B", cells[0].getClass(), cells, ((BufferCell) cells[0]).path().size());
        }
        if(statement instanceof TransactionStatement)
        {
            logger.debug("Mutations: {}", ((TransactionStatement)statement).getUpdates().get(0).mutations);

            List<PartitionUpdate> modifications = ((Mutation) ((TransactionStatement)statement).getUpdates().get(0).mutations.get(0)).modifications.values().asList();
            Object[] cells = ((ComplexColumnData) ((BTreeRow) modifications.get(0).holder.tree[0]).btree[0]).cells;
            logger.info("cells: {}({})  Size per: {} B", cells[0].getClass(), cells, ((BufferCell) cells[0]).path().size());
        }
/*
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        long queryStartNanoTime =System.nanoTime();
//        long timestampOriginal = queryStartNanoTime/1000;
        long timestampOriginal = FBUtilities.timestampMicros();
        int nowInSecOriginal = (int) (timestampOriginal / 1000 / 1000);
        long accordTimestampMicros = commandStore.nextSystemTimestampMicros();
        ResultMessage result = prepared.statement.execute(queryState, options, queryStartNanoTime);
        QueryProcessor.executeOnceInternalWithNowAndTimestamp(nowInSecOriginal,timestampOriginal,query);

        logger.debug(String.valueOf(result));
        logger.debug("{} {} {} {} {}", queryState.getClientState(), queryState.getNowInSeconds(), queryState.getTimestamp(), queryState.generatedNowInSeconds(), queryState.generatedTimestamp());
        logger.debug(String.valueOf(options));
        logger.debug( "Mutations: {}", ((UpdateStatement) prepared.statement).mutations);
        //QueryProcessor.executeInternal(prepared);
        logger.info("queryStartNanoTime: {}  timestamp: {}  nowInSec: {}  Accord lastSystemTimestampMicros: {} {}", queryStartNanoTime, timestampOriginal, nowInSecOriginal, accordTimestampMicros, commandStore.lastSystemTimestampMicros);


*/
        /*        List<PartitionUpdate> modifications = ((Mutation) ((UpdateStatement) prepared.statement).mutations.get(0)).modifications.values().asList();
        Object[] cells = ((ComplexColumnData) ((BTreeRow) modifications.get(0).holder.tree[0]).btree[0]).cells;
        logger.info("cells: {}({})  Size per: {} B", cells[0].getClass(), cells, ((BufferCell) cells[0]).path().size());


        byte[] exampleUUID = nextTimeUUIDAsBytes();
        logger.debug("exampleUUID length and value {} {}", exampleUUID.length, exampleUUID);
        TimeUUID anotherUUID = new TimeUUID((long)exampleUUID[0], 0);
        logger.debug("anotherUUID msb lsb: {} {}", anotherUUID.asUUID().getMostSignificantBits(), anotherUUID.asUUID().getLeastSignificantBits());
        logger.info("org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUIDAsBytes: {}", nextTimeAsUUID());
        logger.info ("getUUID from the newsly inserted bytebuffer: {}", UUIDGen.getUUID(((BufferCell) cells[0]).path().get(0)));

        assert cells[0] instanceof BufferCell;
        Assert.assertEquals(0L, ((BufferCell) cells[0]).timestamp());
        //Assert.assertEquals(1696364507626000L, ((BufferCell) cells[1]).timestamp());
        //Assert.assertEquals(1696364507626000L, ((BufferCell) cells[2]).timestamp());
        Assert.assertEquals(((BufferCell) cells[0]).path().get(0), -115L);
*/
    }
}
