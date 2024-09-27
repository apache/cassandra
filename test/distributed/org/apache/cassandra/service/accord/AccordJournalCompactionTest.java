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

import java.nio.file.Files;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSortedMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.HistoricalTransactionsAccumulator;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.local.CommandStores.RangesForEpoch;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.BootstrapBeganAtAccumulator;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.DurableBeforeAccumulator;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.IdentityAccumulator;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.RedundantBeforeAccumulator;


public class AccordJournalCompactionTest
{
    @BeforeClass
    public static void setUp() throws Throwable
    {
        ServerTestUtils.daemonInitialization();
        StorageService.instance.registerMBeans();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();

        StorageService.instance.initServer();
        Keyspace.setInitialized();
    }

    private AtomicInteger counter = new AtomicInteger();
    @Before
    public void beforeTest() throws Throwable
    {
        File directory = new File(Files.createTempDirectory(Integer.toString(counter.incrementAndGet())));
        directory.deleteRecursiveOnExit();
        DatabaseDescriptor.setAccordJournalDirectory(directory.path());
    }

    @Test
    public void segmentMergeTest() throws InterruptedException
    {
        Gen<RedundantBefore> redundantBeforeGen = AccordGenerators.redundantBefore(DatabaseDescriptor.getPartitioner());
        Gen<DurableBefore> durableBeforeGen = AccordGenerators.durableBeforeGen(DatabaseDescriptor.getPartitioner());
        Gen<Ranges> rangeGen = AccordGenerators.ranges(DatabaseDescriptor.getPartitioner());
        Gen<TxnId> txnIdGen = AccordGens.txnIds(Gens.pick(Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range);
        Gen<NavigableMap<Timestamp, Ranges>> safeToReadGen = AccordGenerators.safeToReadGen(DatabaseDescriptor.getPartitioner());
        Gen<RangesForEpoch.Snapshot> rangesForEpochGen = AccordGenerators.rangesForEpoch(DatabaseDescriptor.getPartitioner());
        Gen<Deps> historicalTransactionsGen = depsGen();


        AccordJournal journal = new AccordJournal(new TestParams()
        {
            @Override
            public int segmentSize()
            {
                return 1024 * 1024;
            }

            @Override
            public boolean enableCompaction()
            {
                return false;
            }
        });
        try
        {
            journal.start(null);
            Timestamp timestamp = Timestamp.NONE;

            RandomSource rs = new DefaultRandom();
            Gen<TxnId> bootstrappedAtTxn = new Gen<TxnId>()
            {
                TxnId prev = txnIdGen.next(rs);
                public TxnId next(RandomSource random)
                {
                    prev = new TxnId(prev.epoch() + 1, prev.hlc() + random.nextInt(1, 100), prev.kind(), prev.domain(), prev.node);
                    return prev;
                }
            };

            RedundantBeforeAccumulator redundantBeforeAccumulator = new RedundantBeforeAccumulator();
            DurableBeforeAccumulator durableBeforeAccumulator = new DurableBeforeAccumulator();
            BootstrapBeganAtAccumulator bootstrapBeganAtAccumulator = new BootstrapBeganAtAccumulator();
            IdentityAccumulator<NavigableMap<Timestamp, Ranges>> safeToReadAccumulator = new IdentityAccumulator<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
            IdentityAccumulator<RangesForEpoch.Snapshot> rangesForEpochAccumulator = new IdentityAccumulator<>(null);
            HistoricalTransactionsAccumulator historicalTransactionsAccumulator = new HistoricalTransactionsAccumulator();

            int count = 1_000;
            Condition condition = Condition.newOneTimeCondition();
            for (int i = 0; i <= count; i++)
            {
                timestamp = timestamp.next();
                AccordSafeCommandStore.FieldUpdates updates = new AccordSafeCommandStore.FieldUpdates();
                updates.durableBefore = durableBeforeGen.next(rs);
                updates.redundantBefore = redundantBeforeGen.next(rs);
                if (i % 100 == 0)
                    updates.newBootstrapBeganAt = new AccordSafeCommandStore.Sync(bootstrappedAtTxn.next(rs), rangeGen.next(rs));
                updates.safeToRead = safeToReadGen.next(rs);
                updates.rangesForEpoch = rangesForEpochGen.next(rs);
                updates.historicalTransactions = historicalTransactionsGen.next(rs);

                if (i == count)
                    journal.persistStoreState(1, updates, condition::signal);
                else
                    journal.persistStoreState(1, updates, null);

                redundantBeforeAccumulator.update(updates.redundantBefore);
                durableBeforeAccumulator.update(updates.durableBefore);
                if (updates.newBootstrapBeganAt != null)
                    bootstrapBeganAtAccumulator.update(updates.newBootstrapBeganAt);
                safeToReadAccumulator.update(updates.safeToRead);
                rangesForEpochAccumulator.update(updates.rangesForEpoch);
                historicalTransactionsAccumulator.update(updates.historicalTransactions);
            }

            condition.await();

            journal.closeCurrentSegmentForTesting();
            journal.runCompactorForTesting();

            Assert.assertEquals(redundantBeforeAccumulator.get(), journal.loadRedundantBefore(1));
            Assert.assertEquals(durableBeforeAccumulator.get(), journal.loadDurableBefore(1));
            Assert.assertEquals(bootstrapBeganAtAccumulator.get(), journal.loadBootstrapBeganAt(1));
            Assert.assertEquals(safeToReadAccumulator.get(), journal.loadSafeToRead(1));
            Assert.assertEquals(rangesForEpochAccumulator.get(), journal.loadRangesForEpoch(1));
            Assert.assertEquals(historicalTransactionsAccumulator.get(), journal.loadHistoricalTransactions(1));
        }
        finally
        {
            journal.shutdown();
        }
    }

    public static Gen<Deps> depsGen()
    {
        Gen<KeyDeps> keyDepsGen = AccordGens.keyDeps(keysForDeps());
        return AccordGens.deps((rs) -> keyDepsGen.next(rs),
                               (rs) -> Deps.NONE.rangeDeps,
                               (rs) -> Deps.NONE.directKeyDeps);
    }

    public static Gen<PartitionKey> keysForDeps()
    {
        Gen<Timestamp> timestampGen = AccordGens.timestamps();
        return rs -> {
            ColumnFamilyStore cfs = Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL);
            DecoratedKey dk = AccordJournalTable.makePartitionKey(cfs,
                                                                  new JournalKey(timestampGen.next(rs), JournalKey.Type.COMMAND_DIFF, 1),
                                                                  JournalKey.SUPPORT,
                                                                  1);
            return new PartitionKey(cfs.getTableId(),
                                    dk);
        };
    }
}
