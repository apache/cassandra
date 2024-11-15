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
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.RandomSource;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.api.Journal.FieldUpdates;
import static accord.local.CommandStores.RangesForEpoch;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.DurableBeforeAccumulator;


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
        ColumnFamilyStore cfs = Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL);
        cfs.disableAutoCompaction();

        RedundantBefore redundantBeforeAccumulator = RedundantBefore.EMPTY;
        DurableBeforeAccumulator durableBeforeAccumulator = new DurableBeforeAccumulator();
        NavigableMap<Timestamp, Ranges> safeToReadAtAccumulator = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
        NavigableMap<TxnId, Ranges> bootstrapBeganAtAccumulator = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
        RangesForEpoch.Snapshot rangesForEpochAccumulator = null;

        Gen<RedundantBefore> redundantBeforeGen = AccordGenerators.redundantBefore(DatabaseDescriptor.getPartitioner());
        Gen<DurableBefore> durableBeforeGen = AccordGenerators.durableBeforeGen(DatabaseDescriptor.getPartitioner());
        Gen<NavigableMap<Timestamp, Ranges>> safeToReadGen = AccordGenerators.safeToReadGen(DatabaseDescriptor.getPartitioner());
        Gen<RangesForEpoch.Snapshot> rangesForEpochGen = AccordGenerators.rangesForEpoch(DatabaseDescriptor.getPartitioner());
        Gen<Range> rangeGen = AccordGenerators.range(DatabaseDescriptor.getPartitioner());
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
        }, new AccordAgent());
        try
        {
            journal.start(null);
            journal.unsafeSetStarted();
            Timestamp timestamp = Timestamp.NONE;

            RandomSource rs = new DefaultRandom(1);

            int count = 1_000;
//            RedundantBefore redundantBefore = RedundantBefore.EMPTY;
            for (int i = 0; i <= count; i++)
            {
                timestamp = timestamp.next();
                FieldUpdates updates = new FieldUpdates();
                DurableBefore addDurableBefore = durableBeforeGen.next(rs);
                // TODO: improve redundant before generator and re-enable
//                updates.addRedundantBefore = redundantBeforeGen.next(rs);
//                updates.newRedundantBefore = redundantBefore = RedundantBefore.merge(redundantBefore, updates.addRedundantBefore);
                updates.newSafeToRead = safeToReadGen.next(rs);
                updates.newRangesForEpoch = rangesForEpochGen.next(rs);

                journal.durableBeforePersister().persist(addDurableBefore, null);
                journal.saveStoreState(1, updates, null);

                redundantBeforeAccumulator = updates.newRedundantBefore;
                durableBeforeAccumulator.update(addDurableBefore);
                if (updates.newBootstrapBeganAt != null)
                    bootstrapBeganAtAccumulator = updates.newBootstrapBeganAt;
                if (updates.newSafeToRead != null)
                    safeToReadAtAccumulator = updates.newSafeToRead;
                if (updates.newRangesForEpoch != null)
                    rangesForEpochAccumulator = updates.newRangesForEpoch;

                if (i % 100 == 0)
                    journal.closeCurrentSegmentForTestingIfNonEmpty();
                if (i % 200 == 0)
                    journal.runCompactorForTesting();
            }

//            Assert.assertEquals(redundantBeforeAccumulator.get(), journal.loadRedundantBefore(1));
            Assert.assertEquals(durableBeforeAccumulator.get(), journal.durableBeforePersister().load());
            Assert.assertEquals(bootstrapBeganAtAccumulator, journal.loadBootstrapBeganAt(1));
            Assert.assertEquals(safeToReadAtAccumulator, journal.loadSafeToRead(1));
            Assert.assertEquals(rangesForEpochAccumulator, journal.loadRangesForEpoch(1));
        }
        finally
        {
            journal.shutdown();
        }
    }

    public static Gen<Deps> depsGen()
    {
        Gen<KeyDeps> keyDepsGen = AccordGenerators.keyDepsGen(DatabaseDescriptor.getPartitioner());
        return AccordGens.deps(keyDepsGen::next,
                               (rs) -> Deps.NONE.rangeDeps,
                               (rs) -> Deps.NONE.directKeyDeps);
    }
}