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

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.burn.BurnTestBase;
import accord.burn.SimulationException;
import accord.impl.TopologyFactory;
import accord.impl.basic.Cluster;
import accord.impl.basic.RandomDelayQueue;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.EpochSupplier;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.RandomSource;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.tools.FieldUtil;
import org.apache.cassandra.utils.CloseableIterator;

import static accord.impl.PrefixedIntHashKey.ranges;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class AccordJournalBurnTest extends BurnTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournalBurnTest.class);

    public static void setUp() throws Throwable
    {
        StorageService.instance.registerMBeans();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();

        Keyspace.setInitialized();
        FieldUtil.transferFields(new KeySerializers.Impl(BurnTestKeySerializers.key,
                                                         BurnTestKeySerializers.routingKey,
                                                         BurnTestKeySerializers.range),
                                 KeySerializers.class);

        FieldUtil.transferFields(new CommandSerializers.QuerySerializers(BurnTestKeySerializers.read,
                                                                         BurnTestKeySerializers.query,
                                                                         BurnTestKeySerializers.update,
                                                                         BurnTestKeySerializers.write,
                                                                         BurnTestKeySerializers.tablesAndKeys),
                                 CommandSerializers.class);

        FieldUtil.transferFields(new DepsSerializers.Impl(BurnTestKeySerializers.range),
                                 DepsSerializers.class);

        FieldUtil.setInstanceUnsafe(ResultSerializers.class,
                                    BurnTestKeySerializers.result,
                                    "result");

        FieldUtil.setInstanceUnsafe(TopologySerializers.class,
                                    new TopologySerializers.ShardSerializer(BurnTestKeySerializers.range),
                                    "shard");
        // compact topology inlines all serialziation and uses TokenRange directly, so it has to be fully stubbed out
        // for this class.
        FieldUtil.setInstanceUnsafe(TopologySerializers.class,
                                    TopologySerializers.topology,
                                    "compactTopology");
    }

    private static final AtomicInteger counter = new AtomicInteger();

    @Before
    public void beforeTest() throws Throwable
    {

    }

    @Test
    public void testOne()
    {
        long seed = System.nanoTime();
        int operations = 1000;

        logger.info("Seed: {}", seed);
        Cluster.trace.trace("Seed: {}", seed);
        RandomSource random = new DefaultRandom(seed);
        try
        {
            List<Node.Id> clients = generateIds(true, 1 + random.nextInt(4));
            int rf;
            float chance = random.nextFloat();
            if (chance < 0.2f) rf = random.nextInt(2, 9);
            else if (chance < 0.4f) rf = 3;
            else if (chance < 0.7f) rf = 5;
            else if (chance < 0.8f) rf = 7;
            else rf = 9;

            List<Node.Id> nodes = generateIds(false, random.nextInt(rf, rf * 3));

            {
                ServerTestUtils.daemonInitialization();

                TableMetadata[] metadatas = new TableMetadata[1 + nodes.size()];
                metadatas[0] = AccordKeyspace.CommandsForKeys;
                for (int i = 0; i < nodes.size(); i++)
                    metadatas[1 + i] = AccordKeyspace.journalMetadata("journal_" + nodes.get(i), false);

                AccordKeyspace.TABLES = Tables.of(metadatas);
                setUp();
            }

            Keyspace ks = Schema.instance.getKeyspaceInstance("system_accord");

            burn(random, new TopologyFactory(rf, ranges(0, HASH_RANGE_START, HASH_RANGE_END, random.nextInt(Math.max(nodes.size() + 1, rf), nodes.size() * 3))),
                 clients,
                 nodes,
                 5 + random.nextInt(15),
                 5 + random.nextInt(15),
                 operations,
                 10 + random.nextInt(30),
                 new RandomDelayQueue.Factory(random).get(),
                 (nodeId, randomSource) -> {
                     try
                     {
                         File directory = new File(Files.createTempDirectory(Integer.toString(counter.incrementAndGet())));
                         directory.deleteRecursiveOnExit();
                         ColumnFamilyStore cfs = ks.getColumnFamilyStore("journal_" + nodeId);
                         cfs.disableAutoCompaction();
                         AccordJournal journal = new AccordJournal(new TestParams()
                         {
                             @Override
                             public int segmentSize()
                             {
                                 return 1 * 1024 * 1024;
                             }
                         }, directory, cfs)
                         {
                             @Override
                             public void start(Node node)
                             {
                                 super.start(node);
                                 unsafeSetStarted();
                             }

                             @Override
                             public void saveCommand(int store, CommandUpdate update, @Nullable Runnable onFlush)
                             {
                                 // For the purpose of this test, we do not have to wait for flush, since we do not test durability and are using mmap
                                 super.saveCommand(store, update, () -> {});
                                 if (onFlush != null)
                                     onFlush.run();
                             }

                             @Override
                             public void saveStoreState(int store, FieldUpdates fieldUpdates, @Nullable Runnable onFlush)
                             {
                                 super.saveStoreState(store, fieldUpdates, () -> {});
                                 if (onFlush != null)
                                     onFlush.run();
                             }

                             @Override
                             public void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush)
                             {
                                 super.saveTopology(topologyUpdate, () -> {});
                                 if (onFlush != null)
                                     onFlush.run();
                             }

                             @Override
                             protected SegmentCompactor<JournalKey, Object> compactor(ColumnFamilyStore cfs, Version userVersion)
                             {
                                 return new NemesisAccordSegmentCompactor<>(userVersion, cfs, randomSource.fork())
                                 {
                                     @Nullable
                                     @Override
                                     public Collection<StaticSegment<JournalKey, Object>> compact(Collection<StaticSegment<JournalKey, Object>> staticSegments)
                                     {
                                         if (journalTable == null)
                                             throw new IllegalStateException("Unsafe access to AccordJournal during <init>; journalTable was touched before it was published");
                                         Collection<StaticSegment<JournalKey, Object>> result = super.compact(staticSegments);
                                         journalTable.safeNotify(index -> index.remove(staticSegments));
                                         return result;
                                     }
                                 };
                             }

                             private CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                                    Directories directories,
                                                                                    LifecycleTransaction transaction,
                                                                                    Set<SSTableReader> nonExpiredSSTables)
                             {
                                 return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, false, 0);
                             }

                             int counter;
                             @Override
                             public void purge(CommandStores commandStores, EpochSupplier minEpoch)
                             {
                                 ++counter;
                                 this.journal.closeCurrentSegmentForTestingIfNonEmpty();
                                 this.journal.runCompactorForTesting();

                                 Set<SSTableReader> orig = cfs.getLiveSSTables();
                                 List<SSTableReader> all = new ArrayList<>(orig);
                                 if (all.size() <= 1)
                                     return;

                                 Set<SSTableReader> selected = new HashSet<>();
                                 int count = all.size();
                                 int removeCount = random.nextInt(1, count);
                                 while (removeCount-- > 0)
                                 {
                                     int removeIndex = random.nextInt(count);
                                     SSTableReader reader = all.get(removeIndex);
                                     if (reader == null)
                                         continue;
                                     all.set(removeIndex, null);
                                     selected.add(reader);
                                     --count;
                                 }

                                 if (selected.isEmpty())
                                     return;
                                 List<ISSTableScanner> scanners = selected.stream().map(SSTableReader::getScanner).collect(Collectors.toList());

                                 TreeMap<JournalKey, Command> before = read(commandStores);
                                 Collection<SSTableReader> newSStables;
                                 try (LifecycleTransaction txn = cfs.getTracker().tryModify(selected, OperationType.COMPACTION);
                                      CompactionController controller = new CompactionController(cfs, selected, 0);
                                      CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION,
                                                                                     scanners,
                                                                                     controller,
                                                                                     0,
                                                                                     nextTimeUUID(),
                                                                                     ActiveCompactionsTracker.NOOP, null,
                                                                                     () -> getCompactionInfo(node, cfs.getTableId()),
                                                                                     () -> Version.V1))
                                 {
                                     try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, cfs.getDirectories(), txn, selected))
                                     {
                                         while (ci.hasNext())
                                             writer.append(ci.next());

                                         ci.setTargetDirectory(writer.getSStableDirectory().path());
                                         // point of no return
                                         newSStables = writer.finish();
                                     }
                                     catch (IOException e)
                                     {
                                         throw new RuntimeException(e);
                                     }
                                 }
                                 TreeMap<JournalKey, Command> after = read(commandStores);
                                 for (Map.Entry<JournalKey, Command> e : before.entrySet())
                                 {
                                     Command b = e.getValue();
                                     Command a = after.get(e.getKey());
                                     Invariants.require(Objects.equals(a, b));
                                 }
                                 if (before.size() != after.size())
                                 {
                                     for (Map.Entry<JournalKey, Command> e : after.entrySet())
                                         Invariants.require(null != before.get(e.getKey()));
                                     Invariants.require(false);
                                 }
                                 Invariants.require(!orig.equals(cfs.getLiveSSTables()));
                             }

                             private TreeMap<JournalKey, Command> read(CommandStores commandStores)
                             {
                                 TreeMap<JournalKey, Command> result = new TreeMap<>(JournalKey.SUPPORT::compare);
                                 try (CloseableIterator<Journal.KeyRefs<JournalKey>> iter = journalTable.keyIterator(null, null))
                                 {
                                     JournalKey prev = null;
                                     while (iter.hasNext())
                                     {
                                         Journal.KeyRefs<JournalKey> ref = iter.next();
                                         if (ref.key().type != JournalKey.Type.COMMAND_DIFF)
                                             continue;

                                         JournalKey key = ref.key();
                                         if (key.equals(prev)) continue;
                                         CommandStore commandStore = commandStores.forId(ref.key().commandStoreId);
                                         Command command = loadCommand(key.commandStoreId, key.id, commandStore.unsafeGetRedundantBefore(), commandStore.durableBefore());
                                         if (command != null)
                                            result.put(key, command);
                                         prev = key;
                                     }
                                 }
                                 return result;
                             }

                             @Override
                             public void replay(CommandStores commandStores)
                             {
                                 // Make sure to replay _only_ static segments
                                 this.closeCurrentSegmentForTestingIfNonEmpty();
                                 super.replay(commandStores);
                             }

                             @Override
                             public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
                             {
                                 // TODO (required): we should be persisting in the journal, but this currently causes the burn test to take far too long
                                 return DurableBefore.NOOP_PERSISTER;
                             }
                         };

                         return journal;
                     }
                     catch (Throwable t)
                     {
                         throw new RuntimeException(t);
                     }
                 }
            );
        }
        catch (Throwable t)
        {
            logger.error("Exception running burn test for seed {}:", seed, t);
            throw SimulationException.wrap(seed, t);
        }
    }

    public static IAccordService.AccordCompactionInfos getCompactionInfo(Node node, TableId tableId)
    {
        IAccordService.AccordCompactionInfos compactionInfos = new IAccordService.AccordCompactionInfos(node.durableBefore(), node.topology().minEpoch());
        node.commandStores().forEachCommandStore(commandStore -> {
            RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
            if (redundantBefore == null)
                redundantBefore = RedundantBefore.EMPTY;
            CommandStores.RangesForEpoch rangesForEpoch = commandStore.unsafeGetRangesForEpoch();
            if (rangesForEpoch == null)
                rangesForEpoch = CommandStores.RangesForEpoch.EMPTY;
            compactionInfos.put(commandStore.id(), new IAccordService.AccordCompactionInfo(commandStore.id(),
                                                                                           redundantBefore,
                                                                                           rangesForEpoch,
                                                                                           tableId));
        });
        return compactionInfos;
    }


}
