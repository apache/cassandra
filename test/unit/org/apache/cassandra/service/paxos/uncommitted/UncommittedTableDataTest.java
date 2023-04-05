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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.service.paxos.uncommitted.UncommittedTableData.Data;
import org.apache.cassandra.service.paxos.uncommitted.UncommittedTableData.FilterFactory;
import org.apache.cassandra.service.paxos.uncommitted.UncommittedTableData.FlushWriter;
import org.apache.cassandra.service.paxos.uncommitted.UncommittedTableData.Merge;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.*;

public class UncommittedTableDataTest
{
    private static final String KS = "ks";
    private static final String TBL = "tbl";
    private static final TableId CFID = TableId.fromUUID(UUID.randomUUID());

    private static final String TBL2 = "tbl2";
    private static final TableId CFID2 = TableId.fromUUID(UUID.randomUUID());

    private File directory = null;

    private static class MockDataFile
    {
        final File data;
        final File crc;

        public MockDataFile(File data, File crc)
        {
            this.data = data;
            this.crc = crc;
        }

        boolean exists()
        {
            return data.exists() && crc.exists();
        }

        boolean isDeleted()
        {
            return !data.exists() && !crc.exists();
        }
    }

    private static final FilterFactory NOOP_FACTORY = new FilterFactory()
    {
        List<Range<Token>> getReplicatedRanges()
        {
            return new ArrayList<>(ALL_RANGES);
        }

        PaxosRepairHistory getPaxosRepairHistory()
        {
            return PaxosRepairHistory.EMPTY;
        }
    };

    private static UncommittedTableData load(File directory, TableId cfid)
    {
        return UncommittedTableData.load(directory, cfid, NOOP_FACTORY);
    }

    MockDataFile mockFile(String table, TableId cfid, long generation, boolean temp)
    {
        String fname = UncommittedDataFile.fileName(KS, table, cfid, generation) + (temp ? UncommittedDataFile.TMP_SUFFIX : "");
        File data = new File(directory, fname);
        File crc = new File(directory, UncommittedDataFile.crcName(fname));
        try
        {
            Files.write("data", data.toJavaIOFile(), Charset.defaultCharset());
            Files.write("crc", crc.toJavaIOFile(), Charset.defaultCharset());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        MockDataFile file = new MockDataFile(data, crc);
        Assert.assertTrue(file.exists());
        return file;
    }

    MockDataFile mockFile(long generation, boolean temp)
    {
        return mockFile(TBL, CFID, generation, temp);
    }

    @Before
    public void setUp() throws Exception
    {
        if (directory != null)
            FileUtils.deleteRecursive(directory);

        directory = new File(Files.createTempDir());
    }

    static PaxosKeyState uncommitted(int key, Ballot ballot)
    {
        return new PaxosKeyState(CFID, dk(key), ballot, false);
    }

    static PaxosKeyState committed(int key, Ballot ballot)
    {
        return new PaxosKeyState(CFID, dk(key), ballot, true);
    }

    private static FlushWriter startFlush(UncommittedTableData tableData, List<PaxosKeyState> updates) throws IOException
    {
        FlushWriter writer = tableData.flushWriter();
        writer.appendAll(updates);
        return writer;
    }

    private static FlushWriter startFlush(UncommittedTableData tableData, PaxosKeyState... updates) throws IOException
    {
        return startFlush(tableData, Lists.newArrayList(updates));
    }

    private static void flush(UncommittedTableData tableData, List<PaxosKeyState> updates) throws IOException
    {
        startFlush(tableData, updates).finish();
    }

    private static void flush(UncommittedTableData tableData, PaxosKeyState... states) throws IOException
    {
        flush(tableData, Lists.newArrayList(states));
    }

    private void mergeWithUpdates(UncommittedTableData tableData, List<PaxosKeyState> toAdd) throws IOException
    {
        flush(tableData, toAdd);
        tableData.createMergeTask().run();
    }

    static void assertIteratorContents(CloseableIterator<PaxosKeyState> iterator, Iterable<PaxosKeyState> expected)
    {
        try (CloseableIterator<PaxosKeyState> iter = iterator)
        {
            Assert.assertEquals(Lists.newArrayList(expected), Lists.newArrayList(iter));
        }
    }

    static void assertFileContents(UncommittedDataFile file, int generation, List<PaxosKeyState> expected)
    {
        Assert.assertEquals(generation, file.generation());
        assertIteratorContents(file.iterator(ALL_RANGES), expected);
    }

    static void assertIteratorContents(UncommittedTableData tableData, int generation, List<PaxosKeyState> expected)
    {
        Assert.assertEquals(generation, tableData.nextGeneration() - 1);
        assertIteratorContents(tableData.iterator(ALL_RANGES), expected);
    }


    /**
     * Test various merge scenarios
     */
    @Test
    public void testMergeWriter() throws IOException
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);

        mergeWithUpdates(tableData, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                       new PaxosKeyState(CFID, dk(5), ballots[1], false),
                                       new PaxosKeyState(CFID, dk(7), ballots[1], false),
                                       new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        assertIteratorContents(tableData, 1, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                                new PaxosKeyState(CFID, dk(5), ballots[1], false),
                                                new PaxosKeyState(CFID, dk(7), ballots[1], false),
                                                new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        // add a commit from the past for key 3, update key 5, and commit key 7
        mergeWithUpdates(tableData, kl(new PaxosKeyState(CFID, dk(3), ballots[0], true),
                                       new PaxosKeyState(CFID, dk(5), ballots[2], false),
                                       new PaxosKeyState(CFID, dk(7), ballots[2], true)));

        // key 7 should be gone because committed keys aren't written out
        assertIteratorContents(tableData, 3, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                                new PaxosKeyState(CFID, dk(5), ballots[2], false),
                                                new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        // add a new key and update an adjacent one
        mergeWithUpdates(tableData, kl(new PaxosKeyState(CFID, dk(4), ballots[3], false),
                                       new PaxosKeyState(CFID, dk(5), ballots[3], false)));
        assertIteratorContents(tableData, 5, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                                new PaxosKeyState(CFID, dk(4), ballots[3], false),
                                                new PaxosKeyState(CFID, dk(5), ballots[3], false),
                                                new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        // add 2 new keys
        mergeWithUpdates(tableData, kl(new PaxosKeyState(CFID, dk(6), ballots[4], false),
                                       new PaxosKeyState(CFID, dk(7), ballots[4], false)));
        assertIteratorContents(tableData, 7, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                                new PaxosKeyState(CFID, dk(4), ballots[3], false),
                                                new PaxosKeyState(CFID, dk(5), ballots[3], false),
                                                new PaxosKeyState(CFID, dk(6), ballots[4], false),
                                                new PaxosKeyState(CFID, dk(7), ballots[4], false),
                                                new PaxosKeyState(CFID, dk(9), ballots[1], false)));
    }

    @Test
    public void committedOpsArentWritten() throws Exception
    {
        Ballot[] ballots = createBallots(2);

        UncommittedTableData tableData = load(directory, CFID);
        mergeWithUpdates(tableData, kl(new PaxosKeyState(CFID, dk(1), ballots[0], false)));
        assertIteratorContents(tableData, 1, kl(new PaxosKeyState(CFID, dk(1), ballots[0], false)));

        mergeWithUpdates(tableData, kl(new PaxosKeyState(CFID, dk(1), ballots[1], true)));
        assertIteratorContents(tableData, 3, kl());
    }

    @Test
    public void testIterator() throws Exception
    {
        Ballot[] ballots = createBallots(10);
        List<PaxosKeyState> expected = new ArrayList<>(ballots.length);
        UncommittedTableData tableData = load(directory, CFID);

        for (int i=0; i<ballots.length; i++)
        {
            Ballot ballot = ballots[i];
            DecoratedKey dk = dk(i);
            expected.add(new PaxosKeyState(CFID, dk, ballot, false));
        }

        mergeWithUpdates(tableData, expected);

        assertIteratorContents(tableData.iterator(Collections.singleton(r(null, tk(4)))), expected.subList(0, 5));
        assertIteratorContents(tableData.iterator(Collections.singleton(r(2, 6))), expected.subList(3, 7));
        assertIteratorContents(tableData.iterator(Collections.singleton(r(tk(7), null))), expected.subList(8, 10));
        assertIteratorContents(tableData.iterator(Lists.newArrayList(r(0, 4), r(5, 8))),
                               Iterables.concat(expected.subList(1, 5), expected.subList(6, 9)));


    }

    @Test
    public void flush() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);

        List<PaxosKeyState> updates = kl(uncommitted(3, ballots[1]),
                                         uncommitted(5, ballots[1]),
                                         committed(7, ballots[1]),
                                         uncommitted(9, ballots[1]));
        flush(tableData, updates);

        Data data = tableData.data();
        UncommittedDataFile updateFile = Iterables.getOnlyElement(data.files);
        assertFileContents(updateFile, 0, updates);
        assertIteratorContents(tableData.iterator(ALL_RANGES), updates);
    }

    @Test
    public void merge() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);
        flush(tableData,
              uncommitted(3, ballots[1]),
              committed(7, ballots[1]));
        flush(tableData,
              uncommitted(5, ballots[1]),
              uncommitted(9, ballots[1]));

        List<File> updateFiles = tableData.data().files.stream().map(UncommittedDataFile::file).collect(Collectors.toList());
        Assert.assertTrue(Iterables.all(updateFiles, File::exists));

        tableData.createMergeTask().run();

        Assert.assertFalse(Iterables.any(updateFiles, File::exists));

        Data data = tableData.data();
        Assert.assertEquals(1, data.files.size());

        List<PaxosKeyState> expected = kl(uncommitted(3, ballots[1]),
                                          uncommitted(5, ballots[1]),
                                          uncommitted(9, ballots[1]));
        assertFileContents(Iterables.getOnlyElement(data.files), 2, expected);
        assertIteratorContents(tableData.iterator(ALL_RANGES), expected);
    }

    /**
     * nothing should break when a merge results in an empty file
     */
    @Test
    public void emptyMerge() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);
        flush(tableData,
              committed(3, ballots[1]),
              committed(7, ballots[1]));

        tableData.createMergeTask().run();

        Data data = tableData.data();

        assertFileContents(Iterables.getOnlyElement(data.files), 1, Collections.emptyList());
        assertIteratorContents(tableData.iterator(ALL_RANGES), Collections.emptyList());
    }

    @Test
    public void load() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);
        flush(tableData,
              uncommitted(3, ballots[1]),
              committed(7, ballots[1]));
        tableData.createMergeTask().run();
        flush(tableData,
              uncommitted(5, ballots[1]),
              uncommitted(9, ballots[1]));
        List<PaxosKeyState> expected = kl(uncommitted(3, ballots[1]),
                                          uncommitted(5, ballots[1]),
                                          uncommitted(9, ballots[1]));
        assertIteratorContents(tableData.iterator(ALL_RANGES), expected);

        // cleanup shouldn't touch files for other tables
        MockDataFile mockStateFile = mockFile(TBL2, CFID2, 2, false);
        MockDataFile mockUpdateFile = mockFile(TBL2, CFID2, 3, false);
        MockDataFile mockTempUpdate = mockFile(TBL2, CFID2, 4, true);

        UncommittedTableData tableData2 = load(directory, CFID);
        assertIteratorContents(tableData2.iterator(ALL_RANGES), expected);
        Assert.assertTrue(mockStateFile.exists() && mockUpdateFile.exists() && mockTempUpdate.exists());
    }

    /**
     * Shouldn't break if there wasn't a merge before shutdown
     */
    @Test
    public void loadWithoutStateFile() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);

        List<PaxosKeyState> updates = kl(uncommitted(3, ballots[1]),
                                         uncommitted(5, ballots[1]),
                                         committed(7, ballots[1]),
                                         uncommitted(9, ballots[1]));
        flush(tableData, updates);
        assertIteratorContents(tableData.iterator(ALL_RANGES), updates);

        UncommittedTableData data2 = load(directory, CFID);
        assertIteratorContents(data2.iterator(ALL_RANGES), updates);
    }

    /**
     * Test that incomplete update flushes are cleaned up
     */
    @Test
    public void updateRecovery() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);

        List<PaxosKeyState> updates = kl(uncommitted(3, ballots[1]),
                                         uncommitted(5, ballots[1]),
                                         committed(7, ballots[1]),
                                         uncommitted(9, ballots[1]));
        flush(tableData, updates);
        assertIteratorContents(tableData.iterator(ALL_RANGES), updates);
        MockDataFile tmpUpdate = mockFile(tableData.nextGeneration(), true);

        UncommittedTableData tableData2 = load(directory, CFID);
        Assert.assertEquals(1, tableData2.nextGeneration());
        assertIteratorContents(tableData2.iterator(ALL_RANGES), updates);
        Assert.assertTrue(tmpUpdate.isDeleted());
    }

    /**
     * Test that incomplete state merges are cleaned up
     */
    @Test
    public void stateRecovery() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);

        List<PaxosKeyState> updates = kl(uncommitted(3, ballots[1]),
                                         uncommitted(5, ballots[1]),
                                         committed(7, ballots[1]),
                                         uncommitted(9, ballots[1]));
        flush(tableData, updates);
        assertIteratorContents(tableData.iterator(ALL_RANGES), updates);
        MockDataFile tmpUpdate = mockFile(tableData.nextGeneration(), true);

        UncommittedTableData tableData2 = load(directory, CFID);
        Assert.assertEquals(1, tableData2.nextGeneration());
        assertIteratorContents(tableData2.iterator(ALL_RANGES), updates);
        Assert.assertTrue(tmpUpdate.isDeleted());
    }

    @Test
    public void orphanCrc() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);

        List<PaxosKeyState> updates = kl(uncommitted(3, ballots[1]),
                                         uncommitted(5, ballots[1]),
                                         uncommitted(7, ballots[1]),
                                         uncommitted(9, ballots[1]));
        flush(tableData, updates);
        long updateGeneration = Iterables.getOnlyElement(tableData.data().files).generation();
        tableData.createMergeTask().run();
        assertIteratorContents(tableData.iterator(ALL_RANGES), updates);

        MockDataFile oldUpdate = mockFile(updateGeneration, false);
        FileUtils.deleteWithConfirm(oldUpdate.data);
        UncommittedTableData tableData2 = load(directory, CFID);
        assertIteratorContents(tableData2.iterator(ALL_RANGES), updates);
        Assert.assertTrue(oldUpdate.isDeleted());
    }

    @Test
    public void referenceCountingTest() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);
        flush(tableData,
              uncommitted(3, ballots[1]),
              committed(7, ballots[1]));

        // initial state
        UncommittedDataFile updateFile = Iterables.getOnlyElement(tableData.data().files);
        Assert.assertEquals(0, updateFile.getActiveReaders());
        Assert.assertFalse(updateFile.isMarkedDeleted());

        // referenced state
        CloseableIterator<PaxosKeyState> iterator = tableData.iterator(ALL_RANGES);
        Assert.assertEquals(1, updateFile.getActiveReaders());
        Assert.assertFalse(updateFile.isMarkedDeleted());

        // marked deleted state
        tableData.createMergeTask().run();
        Assert.assertEquals(1, updateFile.getActiveReaders());
        Assert.assertTrue(updateFile.isMarkedDeleted());
        Assert.assertTrue(updateFile.file().exists());

        // unreference and delete
        iterator.close();
        Assert.assertEquals(0, updateFile.getActiveReaders());
        Assert.assertTrue(updateFile.isMarkedDeleted());
        Assert.assertFalse(updateFile.file().exists());
    }

    /**
     * Test that we don't compact update sequences with gaps. ie: we shouldn't compact update generation 4
     * if we can't include generation 3
     */
    @Test
    public void outOfOrderFlush() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);
        FlushWriter pendingFlush = startFlush(tableData,
                                              uncommitted(3, ballots[1]),
                                              committed(7, ballots[1]));
        Assert.assertNull(tableData.currentMerge());

        flush(tableData,
              uncommitted(5, ballots[1]),
              uncommitted(9, ballots[1]));

        // schedule a merge
        Merge merge = tableData.createMergeTask();
        Assert.assertFalse(!merge.dependsOnActiveFlushes());
        Assert.assertFalse(merge.isScheduled);

        // completing the first flush should cause the merge to be scheduled
        pendingFlush.finish();
        Assert.assertTrue(!merge.dependsOnActiveFlushes());
        Assert.assertTrue(merge.isScheduled);

        while (tableData.currentMerge() != null)
            Thread.sleep(1);

        // confirm that the merge has completed
        Assert.assertEquals(3, tableData.nextGeneration());
        Data data = tableData.data();
        Assert.assertEquals(2, Iterables.getOnlyElement(data.files).generation());
        assertIteratorContents(tableData.iterator(ALL_RANGES), kl(uncommitted(3, ballots[1]),
                                                                  uncommitted(5, ballots[1]),
                                                                  uncommitted(9, ballots[1])));
    }

    @Test
    public void abortedFlush() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = load(directory, CFID);
        FlushWriter pendingFlush = startFlush(tableData,
                                              uncommitted(3, ballots[1]),
                                              committed(7, ballots[1]));
        Assert.assertNull(tableData.currentMerge());

        List<PaxosKeyState> secondFlushUpdates = kl(uncommitted(5, ballots[1]),
                                                    uncommitted(9, ballots[1]));
        flush(tableData, secondFlushUpdates);
        tableData.createMergeTask();

        // the second flush should have triggered a merge
        Merge merge = tableData.currentMerge();
        Assert.assertFalse(!merge.dependsOnActiveFlushes());
        Assert.assertFalse(merge.isScheduled);

        // completing the first merge should cause the merge to be scheduled
        pendingFlush.abort(null);
        Assert.assertTrue(!merge.dependsOnActiveFlushes());
        Assert.assertTrue(merge.isScheduled);

        while (tableData.currentMerge() != null)
            Thread.sleep(1);

        // confirm that the merge has completed
        Assert.assertEquals(3, tableData.nextGeneration());
        Data data = tableData.data();
        Assert.assertEquals(2, Iterables.getOnlyElement(data.files).generation());
        assertIteratorContents(tableData.iterator(ALL_RANGES), secondFlushUpdates);
    }

    /**
     * keys that aren't locally replicated shouldn't be written on merge
     */
    @Test
    public void rangePurge() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = UncommittedTableData.load(directory, CFID, new FilterFactory() {
            List<Range<Token>> getReplicatedRanges()
            {
                return Lists.newArrayList(new Range<>(tk(4), tk(7)));
            }

            PaxosRepairHistory getPaxosRepairHistory()
            {
                return PaxosRepairHistory.EMPTY;
            }
        });

        flush(tableData, uncommitted(3, ballots[1]),
                         uncommitted(5, ballots[1]),
                         uncommitted(7, ballots[1]),
                         uncommitted(9, ballots[1]));
        tableData.createMergeTask().run();
        assertIteratorContents(tableData.iterator(ALL_RANGES), kl(uncommitted(5, ballots[1]),
                                                                  uncommitted(7, ballots[1])));
    }

    @Test
    public void rangePurge2() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = UncommittedTableData.load(directory, CFID, new FilterFactory() {
            List<Range<Token>> getReplicatedRanges()
            {
                return Lists.newArrayList(new Range<>(tk(4), tk(6)), new Range(tk(8), tk(10)));
            }

            PaxosRepairHistory getPaxosRepairHistory()
            {
                return PaxosRepairHistory.EMPTY;
            }
        });

        flush(tableData, uncommitted(3, ballots[1]),
              uncommitted(5, ballots[1]),
              uncommitted(7, ballots[1]),
              uncommitted(9, ballots[1]));
        tableData.createMergeTask().run();
        assertIteratorContents(tableData.iterator(ALL_RANGES), kl(uncommitted(5, ballots[1]),
                                                                  uncommitted(9, ballots[1])));
    }

    /**
     * ballots below the low bound should be purged
     */
    @Test
    public void lowBoundPurge() throws Throwable
    {
        Ballot[] ballots = createBallots(5);
        UncommittedTableData tableData = UncommittedTableData.load(directory, CFID, new FilterFactory() {
            List<Range<Token>> getReplicatedRanges()
            {
                return Lists.newArrayList(ALL_RANGES);
            }

            PaxosRepairHistory getPaxosRepairHistory()
            {
                return PaxosRepairHistory.add(PaxosRepairHistory.EMPTY, ALL_RANGES, ballots[1]);
            }
        });

        flush(tableData, uncommitted(3, ballots[0]),
                         uncommitted(5, ballots[1]),
                         uncommitted(7, ballots[2]));
        tableData.createMergeTask().run();
        assertIteratorContents(tableData.iterator(ALL_RANGES),  kl(uncommitted(5, ballots[1]),
                                                                   uncommitted(7, ballots[2])));
    }
}
