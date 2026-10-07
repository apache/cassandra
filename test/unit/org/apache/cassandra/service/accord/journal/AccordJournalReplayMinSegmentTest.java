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
package org.apache.cassandra.service.accord.journal;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Participants;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.RandomSource;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Segment;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.service.accord.JournalKey.Type.COMMAND_DIFF;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replay from a save point uses {@code AccordJournal#keyIterator(min, max, includeActive=false, minSegment)} to find the
 * commands written since the save point. For the journal table, {@code TableKeyIterator} selects sstables using
 * {@code coveredClustering.end()}, which (because the table clusters by {@code descriptor DESC}) is the sstable's
 * SMALLEST segment. An sstable that contains records from segments both below and at/above {@code minSegment} is
 * therefore skipped, and the commands whose records live only in it are not replayed.
 * <p>
 * Uses only the default (legacy) code paths: the merging segment compactor and the regular key iterator.
 *
 * @author LLM
 */
public class AccordJournalReplayMinSegmentTest
{
    private static final int STORE = 0;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        CassandraRelevantProperties.JUNIT_STORAGE_COMPATIBILITY_MODE.setEnum(StorageCompatibilityMode.NONE);
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        cfs = Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL);
    }

    @Before
    public void before() throws Throwable
    {
        File directory = new File(Files.createTempDirectory("journal"));
        directory.deleteRecursiveOnExit();
        DatabaseDescriptor.setAccordJournalDirectory(directory.path());
        cfs.disableAutoCompaction();
        cfs.truncateBlockingWithoutSnapshot();
    }

    @Test
    public void replayKeyIterationFindsCommandsWrittenAfterSavePoint()
    {
        AccordJournal journal = new AccordJournal(new TestParams()
        {
            @Override public boolean enableCompaction() { return false; }
        });
        journal.start(null);
        try
        {
            RandomSource rs = RandomSource.wrap(new java.util.Random(0));
            Gen<TxnId> txnIds = AccordGens.txnIds();

            // "before the save point": txnA is written to segment S1
            TxnId txnA = txnIds.next(rs);
            save(journal, txnA);
            long s1 = journal.unsafeGetJournal().currentActiveSegment().id();
            journal.closeCurrentSegmentForTestingIfNonEmpty();

            // a save point is taken at S1; on restart AccordCommandStore.restoreState() returns minSegment = S1 + 1
            long minSegment = s1 + 1;

            // "after the save point": txnB is written to a later segment S2
            TxnId txnB = txnIds.next(rs);
            save(journal, txnB);
            long s2 = journal.unsafeGetJournal().currentActiveSegment().id();
            journal.closeCurrentSegmentForTestingIfNonEmpty();
            assertThat(s2).isGreaterThanOrEqualTo(minSegment);

            // while the segments are still in the journal, replay would find txnB (and only txnB)
            assertThat(replayKeys(journal, minSegment)).containsExactly(txnB);

            // the journal's segment compactor merges both static segments into one sstable (as it routinely does:
            // it compacts all static segments together, regardless of save points)
            journal.runCompactorForTesting();
            assertThat(journal.unsafeGetJournal().segments().count(Segment::isStatic)).isZero();
            List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
            assertThat(sstables).hasSize(1);
            SSTableReader sstable = sstables.get(0);
            long coveredStart = LongType.instance.compose(sstable.getSSTableMetadata().coveredClustering.start().bufferAt(0));
            long coveredEnd = LongType.instance.compose(sstable.getSSTableMetadata().coveredClustering.end().bufferAt(0));
            assertThat(coveredStart).isEqualTo(s2); // the LARGEST segment
            assertThat(coveredEnd).isEqualTo(s1);   // the SMALLEST segment

            // the data is all still there...
            assertThat(journal.load(STORE, txnB).count()).isEqualTo(1);
            SoftAssertions softly = new SoftAssertions();
            // replay from the save point must still find txnB (sstables are selected as a whole, so it may also
            // redundantly replay txnA, which is harmless)
            softly.assertThat(replayKeys(journal, minSegment)).describedAs("keys replayed from minSegment=%d (S1=%d, S2=%d)", minSegment, s1, s2)
                                                              .contains(txnB);
            // and the highest segment in the table is S2 (used to choose the next segment id on startup)
            softly.assertThat(journal.maxTableDescriptor()).describedAs("maxTableDescriptor").isEqualTo(s2);
            softly.assertAll();
        }
        finally
        {
            journal.stop();
            journal.close();
        }
    }

    /**
     * The keys Replay.ReplayStream would replay for {@link #STORE}, with replayer.minReplay = TxnId.NONE
     */
    private static List<TxnId> replayKeys(AccordJournal journal, long minSegment)
    {
        List<TxnId> result = new ArrayList<>();
        try (CloseableIterator<Journal.KeyRefs<JournalKey>> iter = journal.keyIterator(new JournalKey(TxnId.NONE, COMMAND_DIFF, STORE),
                                                                                       new JournalKey(TxnId.MAX.withoutNonIdentityFlags(), COMMAND_DIFF, STORE),
                                                                                       false, minSegment))
        {
            while (iter.hasNext())
            {
                JournalKey key = iter.next().key();
                if (key.type == COMMAND_DIFF)
                    result.add(key.id);
            }
        }
        return result;
    }

    private static void save(AccordJournal journal, TxnId txnId)
    {
        Participants<?> participants = RoutingKeys.of(new TokenKey(TableId.generate(), new ByteOrderedPartitioner.BytesToken(new byte[1])));
        Command command = Command.NotDefined.notDefined(txnId, SaveStatus.NotDefined, Status.Durability.NotDurable,
                                                        StoreParticipants.create(null, participants, null, null, participants, participants),
                                                        Ballot.ZERO);
        journal.saveCommand(STORE, new accord.api.Journal.CommandUpdate(null, command), () -> {});
    }
}
