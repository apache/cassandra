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

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSortedMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.api.Result;
import accord.impl.CommandTimeseries;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeys;
import accord.impl.TimestampsForKey;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.KeyHistory;
import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutableKey;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.ImmutableBitSet;
import accord.utils.SimpleBitSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCachingState.Modified;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.utils.Pair;

import static accord.local.Status.Durability.Majority;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.Commands.preaccepted;
import static org.apache.cassandra.service.accord.AccordTestUtils.ballot;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.loaded;
import static org.apache.cassandra.service.accord.AccordTestUtils.timestamp;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AccordCommandStoreTest
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStoreTest.class);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
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

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1)");
        TxnId oldTxnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTxnId2 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTimestamp = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        PartialDeps dependencies;
        try (PartialDeps.Builder builder = PartialDeps.builder(depTxn.covering()))
        {
            builder.add(key, oldTxnId1);
            builder.add(key, oldTxnId2);
            dependencies = builder.build();
        }

        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        PartialTxn txn = createPartialTxn(0);
        Route<?> route = RoutingKeys.of(key.toUnseekable()).toRoute(key.toUnseekable());
        attrs.partialTxn(txn);
        attrs.route(route);
        attrs.durability(Majority);
        attrs.partialTxn(txn);
        Ballot promised = ballot(1, clock.incrementAndGet(), 1);
        Ballot accepted = ballot(1, clock.incrementAndGet(), 1);
        Timestamp executeAt = timestamp(1, clock.incrementAndGet(), 1);
        attrs.partialDeps(dependencies);
        SimpleBitSet waitingOnCommit = new SimpleBitSet(2);
        waitingOnCommit.set(0);
        SimpleBitSet waitingOnApply = new SimpleBitSet(2);
        waitingOnApply.set(1);
        Command.WaitingOn waitingOn = new Command.WaitingOn(dependencies, new ImmutableBitSet(waitingOnCommit), new ImmutableBitSet(waitingOnApply), new ImmutableBitSet(2));
        attrs.addListener(new Command.ProxyListener(oldTxnId1));
        Pair<Writes, Result> result = AccordTestUtils.processTxnResult(commandStore, txnId, txn, executeAt);

        Command command = Command.SerializerSupport.executed(attrs, SaveStatus.Applied, executeAt, promised, accepted,
                                                             waitingOn, result.left, CommandSerializers.APPLIED);
        AccordSafeCommand safeCommand = new AccordSafeCommand(loaded(txnId, null));
        safeCommand.set(command);

        Apply apply =
            Apply.SerializationSupport.create(txnId,
                                              route.slice(Ranges.of(TokenRange.fullRange("ks"))),
                                              1L,
                                              Apply.Kind.Minimal,
                                              depTxn.keys(),
                                              executeAt,
                                              dependencies,
                                              txn,
                                              result.left,
                                              CommandSerializers.APPLIED);
        commandStore.appendToJournal(apply);
        AccordKeyspace.getCommandMutation(commandStore, safeCommand, commandStore.nextSystemTimestampMicros()).apply();

        logger.info("E: {}", command);
        Command actual = AccordKeyspace.loadCommand(commandStore, txnId);
        logger.info("A: {}", actual);

        Assert.assertEquals(command, actual);
    }

    @Test
    public void timestampsForKeyLoadSave()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Timestamp maxTimestamp = timestamp(1, clock.incrementAndGet(), 1);

        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) getOnlyElement(txn.keys());
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);

        Command command1 = preaccepted(txnId1, txn, timestamp(1, clock.incrementAndGet(), 1));
        Command command2 = preaccepted(txnId2, txn, timestamp(1, clock.incrementAndGet(), 1));

        AccordSafeTimestampsForKey tfk = new AccordSafeTimestampsForKey(loaded(key, null));
        tfk.initialize();
        tfk.updateMax(maxTimestamp);

        CommandsForKeys.updateLastExecutionTimestamps(commandStore, tfk, txnId1, true);
        Assert.assertEquals(txnId1.hlc(), AccordSafeTimestampsForKey.timestampMicrosFor(tfk.current(), txnId1, true));

        CommandsForKeys.updateLastExecutionTimestamps(commandStore, tfk, txnId2, true);
        Assert.assertEquals(txnId2.hlc(), AccordSafeTimestampsForKey.timestampMicrosFor(tfk.current(), txnId2, true));

        Assert.assertEquals(txnId2, tfk.current().lastExecutedTimestamp());
        Assert.assertEquals(txnId2.hlc(), tfk.lastExecutedMicros());

        AccordSafeCommandsForKey cfk = new AccordSafeCommandsForKey(loaded(key, null));
        cfk.initialize(CommandsForKeySerializer.loader);

        AccordSafeCommandsForKeyUpdate ufk = new AccordSafeCommandsForKeyUpdate(loaded(key, null));
        ufk.initialize();

        CommandsForKeys.registerCommand(tfk, ufk, command1);
        CommandsForKeys.registerCommand(tfk, ufk, command2);

        AccordKeyspace.getTimestampsForKeyMutation(commandStore, tfk, commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", tfk);
        TimestampsForKey actual = AccordKeyspace.loadTimestampsForKey(commandStore, key);
        logger.info("A: {}", actual);

        Assert.assertEquals(tfk.current(), actual);
    }

    @Test
    public void commandsForKeyLoadSave()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) getOnlyElement(txn.keys());
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);

        Command command1 = preaccepted(txnId1, txn, timestamp(1, clock.incrementAndGet(), 1));
        Command command2 = preaccepted(txnId2, txn, timestamp(1, clock.incrementAndGet(), 1));

        AccordSafeTimestampsForKey tfk = new AccordSafeTimestampsForKey(loaded(key, null));
        tfk.initialize();

        AccordSafeCommandsForKey cfk = new AccordSafeCommandsForKey(loaded(key, null));
        cfk.initialize(CommandsForKeySerializer.loader);

        AccordSafeCommandsForKeyUpdate ufk = new AccordSafeCommandsForKeyUpdate(loaded(key, null));
        ufk.initialize();

        CommandsForKeys.registerCommand(tfk, ufk, command1);
        CommandsForKeys.registerCommand(tfk, ufk, command2);

        AccordKeyspace.getCommandsForKeyMutation(commandStore.id(), ufk.setUpdates(), commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", cfk);
        CommandsForKey actual = AccordKeyspace.loadDepsCommandsForKey(commandStore, key);
        logger.info("A: {}", actual);

        Assert.assertEquals(ufk.applyToDeps(cfk.current()), actual);
    }

    private static <K, V extends AccordSafeState<K, ?>> NavigableMap<K, V> toNavigableMap(V safeState)
    {
        TreeMap<K, V> map = new TreeMap<>();
        map.put(safeState.key(), safeState);
        return map;
    }

    @Test
    public void commandsForKeyUpdateTest()
    {
        // check that updates are reflected in CFKs without marking them modified
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) getOnlyElement(txn.keys());

        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        AccordSafeCommand safeCommand = commandStore.commandCache().acquireOrInitialize(txnId, t -> preaccepted(txnId, txn, timestamp(1, clock.incrementAndGet(), 1)));
        AccordSafeTimestampsForKey timestamps = commandStore.timestampsForKeyCache().acquireOrInitialize(key, k -> new TimestampsForKey((Key) k));
        AccordSafeCommandsForKey commands = commandStore.depsCommandsForKeyCache().acquireOrInitialize(key, k -> new CommandsForKey((Key) k, CommandsForKeySerializer.loader));
        AccordSafeCommandsForKeyUpdate update = commandStore.updatesForKeyCache().acquireOrInitialize(key, CommandsForKeyUpdate::empty);

        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.commandCache().getUnsafe(txnId).status());
        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.timestampsForKeyCache().getUnsafe(key).status());
        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.depsCommandsForKeyCache().getUnsafe(key).status());
        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.updatesForKeyCache().getUnsafe(key).status());

        AccordSafeCommandStore safeStore = commandStore.beginOperation(PreLoadContext.contextFor(txnId, Keys.of(key), KeyHistory.DEPS),
                                                                       toNavigableMap(safeCommand),
                                                                       toNavigableMap(timestamps),
                                                                       toNavigableMap(commands),
                                                                       new TreeMap<>(),
                                                                       toNavigableMap(update));

        AccordSafeCommandsForKeyUpdate updates = safeStore.getOrCreateCommandsForKeyUpdate(key);
        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.updatesForKeyCache().getUnsafe(key).status());

        Command initialCommand = safeCommand.current();
        CommandsForKey initialCFK = commands.current();
        CommandsForKeyUpdate initialUpdate = updates.current();

        updates.common().commands().add(txnId, initialCommand);

        CommandsForKeyUpdate expected = new CommandsForKeyUpdate(key, updates.deps().toImmutable(), updates.all().toImmutable(), updates.common().toImmutable());
        Assert.assertEquals(1, expected.common().commands().numChanges());
        Assert.assertTrue(expected.deps().isEmpty());
        Assert.assertTrue(expected.all().isEmpty());

        Assert.assertSame(initialCFK, commands.current());
        Assert.assertSame(initialUpdate, updates.current());

        safeStore.postExecute(toNavigableMap(safeCommand),
                              toNavigableMap(timestamps),
                              toNavigableMap(commands),
                              new TreeMap<>(),
                              toNavigableMap(updates));
        safeStore.complete();


        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.commandCache().getUnsafe(txnId).status());
        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.timestampsForKeyCache().getUnsafe(key).status());
        Assert.assertEquals(AccordCachingState.Status.LOADED, commandStore.depsCommandsForKeyCache().getUnsafe(key).status());
        Assert.assertEquals(AccordCachingState.Status.MODIFIED, commandStore.updatesForKeyCache().getUnsafe(key).status());

        CommandsForKey finalCFK = commandStore.depsCommandsForKeyCache().getUnsafe(key).get();
        Assert.assertEquals(txnId, getOnlyElement(finalCFK.commands().commands.keySet()));

        Modified<RoutableKey, CommandsForKeyUpdate> loadedUpdate = (Modified<RoutableKey, CommandsForKeyUpdate>)  commandStore.updatesForKeyCache().getUnsafe(key).state();
        Assert.assertNull(loadedUpdate.original);
        Assert.assertEquals(expected, loadedUpdate.get());
    }

    /**
     * Test that in memory cfk updates are applied to
     */
    @Test
    public void commandsForKeyUpdateOnLoadTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) getOnlyElement(txn.keys());

        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        Command command = preaccepted(txnId, txn, timestamp(1, clock.incrementAndGet(), 1));

        // make a cached update
        AccordSafeCommandsForKeyUpdate updates = commandStore.updatesForKeyCache().acquireOrInitialize(key, k -> null);
        updates.preExecute();
        updates.common().commands().remove(command.txnId());
        updates.setUpdates(); // apply the updates applied to the safe state to the cached value
        updates.postExecute();  // apply the cached value to the global state
        commandStore.updatesForKeyCache().release(updates);

        // make an out of date CFK
        CommandTimeseries.CommandLoader<ByteBuffer> loader = CommandsForKeySerializer.loader;
        CommandsForKey staleCFK = CommandsForKey.SerializerSupport.create(key, loader,
                                                                          ImmutableSortedMap.of(command.txnId(), loader.saveForCFK(command)));

        Assert.assertEquals(txnId, getOnlyElement(staleCFK.commands().commands.keySet()));

        // on loading the cfk into the cache, the in memory update should be applied
        AccordSafeCommandsForKey commands = commandStore.depsCommandsForKeyCache().acquireOrInitialize(key, k -> new CommandsForKey((Key) k, CommandsForKeySerializer.loader));
        commands.preExecute();

        Assert.assertEquals(txnId, getOnlyElement(staleCFK.commands().commands.keySet()));
    }
}
