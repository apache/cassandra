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
package org.apache.cassandra.db.commitlog;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;

public class CommitLogReaderTest extends CQLTester
{
    private static final long CORRUPTED_COMMIT_LOG_FILE_ID = 111L;
    private static final String CORRUPTED_COMMIT_LOG_FILE_NAME = "CommitLog-7-1234567.log";

    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();

        JVMStabilityInspector.replaceKiller(new KillerForTests(false));

        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.batch);

        // Once per-JVM is enough
        prepareServer();
    }

    @Before
    public void before() throws IOException
    {
        clearCorruptedCommitLogFile();
        CommitLog.instance.resetUnsafe(true);

        // always reset to what Cassandra's default is and let each test method
        // handle its expected failure policy itself for better test encapsulation.
        DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop);
    }

    @Test
    public void testReadAll() throws Throwable
    {
        int samples = 1000;
        populateData(samples);
        ArrayList<File> toCheck = getCommitLogs();

        CommitLogReader reader = new CommitLogReader();

        TestCLRHandler testHandler = new TestCLRHandler(currentTableMetadata());
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, CommitLogReader.ALL_MUTATIONS, false);

        Assert.assertEquals("Expected 1000 seen mutations, got: " + testHandler.seenMutationCount(),
                            1000, testHandler.seenMutationCount());

        confirmReadOrder(testHandler, 0);
    }

    @Test
    public void testReadCount() throws Throwable
    {
        int samples = 50;
        int readCount = 10;
        populateData(samples);
        ArrayList<File> toCheck = getCommitLogs();

        CommitLogReader reader = new CommitLogReader();
        TestCLRHandler testHandler = new TestCLRHandler();

        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, readCount - testHandler.seenMutationCount(), false);

        Assert.assertEquals("Expected " + readCount + " seen mutations, got: " + testHandler.seenMutations.size(),
                            readCount, testHandler.seenMutationCount());
    }

    @Test
    public void testReadFromMidpoint() throws Throwable
    {
        int samples = 1000;
        int readCount = 500;
        CommitLogPosition midpoint = populateData(samples);
        ArrayList<File> toCheck = getCommitLogs();

        CommitLogReader reader = new CommitLogReader();
        TestCLRHandler testHandler = new TestCLRHandler();

        // Will skip on incorrect segments due to id mismatch on midpoint
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, midpoint, readCount, false);

        // Confirm correct count on replay
        Assert.assertEquals("Expected " + readCount + " seen mutations, got: " + testHandler.seenMutations.size(),
                            readCount, testHandler.seenMutationCount());

        confirmReadOrder(testHandler, samples / 2);
    }

    @Test
    public void testReadFromMidpointTooMany() throws Throwable
    {
        int samples = 1000;
        int readCount = 5000;
        CommitLogPosition midpoint = populateData(samples);
        ArrayList<File> toCheck = getCommitLogs();

        CommitLogReader reader = new CommitLogReader();
        TestCLRHandler testHandler = new TestCLRHandler(currentTableMetadata());

        // Reading from mid to overflow by 4.5k
        // Will skip on incorrect segments due to id mismatch on midpoint
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, midpoint, readCount, false);

        Assert.assertEquals("Expected " + samples / 2 + " seen mutations, got: " + testHandler.seenMutations.size(),
                            samples / 2, testHandler.seenMutationCount());

        confirmReadOrder(testHandler, samples / 2);
    }

    @Test
    public void testReadCountFromMidpoint() throws Throwable
    {
        int samples = 1000;
        int readCount = 10;
        CommitLogPosition midpoint = populateData(samples);
        ArrayList<File> toCheck = getCommitLogs();

        CommitLogReader reader = new CommitLogReader();
        TestCLRHandler testHandler = new TestCLRHandler();

        for (File f: toCheck)
            reader.readCommitLogSegment(testHandler, f, midpoint, readCount, false);

        // Confirm correct count on replay
        Assert.assertEquals("Expected " + readCount + " seen mutations, got: " + testHandler.seenMutations.size(),
            readCount, testHandler.seenMutationCount());

        confirmReadOrder(testHandler, samples / 2);
    }

    @Test
    public void testSyncMarkerChecksumReadFailed_ignoreReplayErrorsDisabled() throws Throwable
    {
        File corruptedSegmentFile = createAndWriteCorruptedCommitLogFile();
        CommitLogReader reader = new CommitLogReader();
        // use real CLR handler to test actual behavior
        CommitLogReadHandler clrHandler =
                new CommitLogReplayer(new CommitLog(null), null, null, null);

        // ignore.replay.errors disabled, so we expect the exception here
        Assertions.assertThatThrownBy(() ->
                                      reader.readCommitLogSegment(clrHandler,
                                                                  corruptedSegmentFile,
                                                                  CommitLogPosition.NONE,
                                                                  CommitLogReader.ALL_MUTATIONS,
                                                                  false)
                  ).isInstanceOf(CommitLogReplayer.CommitLogReplayException.class);
    }

    @Test
    public void testSyncMarkerChecksumReadFailed_ignoreReplayErrorsEnabled() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(COMMITLOG_IGNORE_REPLAY_ERRORS, "true"))
        {
            File corruptedSegmentFile = createAndWriteCorruptedCommitLogFile();

            CommitLogReader reader = new CommitLogReader();
            // use real CLR handler to test actual behavior
            CommitLogReadHandler clrHandler =
            new CommitLogReplayer(new CommitLog(null), null, null, null);

            // ignore.replay.errors enabled, so we don't expect any errors
            reader.readCommitLogSegment(clrHandler, corruptedSegmentFile, CommitLogPosition.NONE, CommitLogReader.ALL_MUTATIONS, false);
        }
    }

    @Test
    public void testSyncMarkerChecksumReadFailed_ignoreReplayErrorsDisabled_commitFailurePolicyIgnore() throws Throwable
    {
        DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);

        File corruptedSegmentFile = createAndWriteCorruptedCommitLogFile();

        CommitLogReader reader = new CommitLogReader();
        // use real CLR handler to test actual behavior
        CommitLogReadHandler clrHandler =
                new CommitLogReplayer(new CommitLog(null), null, null, null);

        // commit.failure.policy=ignore, so we don't expect any errors
        reader.readCommitLogSegment(clrHandler, corruptedSegmentFile, CommitLogPosition.NONE, CommitLogReader.ALL_MUTATIONS, false);
    }

    /**
     * Since we have both table and non mixed into the CL, we ignore updates that aren't for the table the test handler
     * is configured to check.
     * @param handler
     * @param offset integer offset of count we expect to see in record
     */
    private void confirmReadOrder(TestCLRHandler handler, int offset)
    {
        ColumnMetadata cd = currentTableMetadata().getColumn(new ColumnIdentifier("data", false));
        int i = 0;
        int j = 0;
        while (i + j < handler.seenMutationCount())
        {
            PartitionUpdate pu = handler.seenMutations.get(i + j).getPartitionUpdate(currentTableMetadata());
            if (pu == null)
            {
                j++;
                continue;
            }

            for (Row r : pu)
            {
                String expected = Integer.toString(i + offset);
                String seen = new String(r.getCell(cd).buffer().array());
                if (!expected.equals(seen))
                    Assert.fail("Mismatch at index: " + i + ". Offset: " + offset + " Expected: " + expected + " Seen: " + seen);
            }
            i++;
        }
    }

    static ArrayList<File> getCommitLogs()
    {
        File dir = new File(DatabaseDescriptor.getCommitLogLocation());
        File[] files = dir.tryList();
        ArrayList<File> results = new ArrayList<>();
        for (File f : files)
        {
            if (f.isDirectory())
                continue;
            results.add(f);
        }
        Assert.assertFalse("Didn't find any commit log files.", results.isEmpty());
        return results;
    }

    static class TestCLRHandler implements CommitLogReadHandler
    {
        public List<Mutation> seenMutations = new ArrayList<Mutation>();
        public boolean sawStopOnErrorCheck = false;

        private final TableMetadata metadata;

        // Accept all
        public TestCLRHandler()
        {
            this.metadata = null;
        }

        public TestCLRHandler(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        public boolean shouldSkipSegmentOnError(CommitLogReadException exception)
        {
            sawStopOnErrorCheck = true;
            return false;
        }

        public void handleUnrecoverableError(CommitLogReadException exception)
        {
            sawStopOnErrorCheck = true;
        }

        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            if (metadata == null || m.getPartitionUpdate(metadata) != null)
                seenMutations.add(m);
        }

        public int seenMutationCount() { return seenMutations.size(); }
    }

    /**
     * Returns offset of active written data at halfway point of data
     */
    CommitLogPosition populateData(int entryCount) throws Throwable
    {
        Assert.assertEquals("entryCount must be an even number.", 0, entryCount % 2);

        createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");
        int midpoint = entryCount / 2;

        for (int i = 0; i < midpoint; i++) {
            execute("INSERT INTO %s (idx, data) VALUES (?, ?)", i, Integer.toString(i));
        }

        CommitLogPosition result = CommitLog.instance.getCurrentPosition();

        for (int i = midpoint; i < entryCount; i++)
            execute("INSERT INTO %s (idx, data) VALUES (?, ?)", i, Integer.toString(i));

        Keyspace.open(keyspace())
                .getColumnFamilyStore(currentTable())
                .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        return result;
    }

    private static File createAndWriteCorruptedCommitLogFile() throws IOException
    {
        final ByteBuffer corruptedSegmentByteBuffer =
                ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize());

        final CommitLogDescriptor commitLogDescriptor =
                new CommitLogDescriptor(CORRUPTED_COMMIT_LOG_FILE_ID, null, EncryptionContextGenerator.createDisabledContext());

        CommitLogDescriptor.writeHeader(corruptedSegmentByteBuffer, commitLogDescriptor);

        // write corrupted sync marker:
        // put wrong offset
        corruptedSegmentByteBuffer.putInt(42);
        // put wrong CRC
        corruptedSegmentByteBuffer.putInt(42);

        final File corruptedLogFile = new File(DatabaseDescriptor.getCommitLogLocation(), CORRUPTED_COMMIT_LOG_FILE_NAME);
        try (FileOutputStream fos = new FileOutputStream(corruptedLogFile.toJavaIOFile()))
        {
            fos.write(corruptedSegmentByteBuffer.array());
            fos.flush();
        }

        return corruptedLogFile;
    }

    private static void clearCorruptedCommitLogFile()
    {
        new File(DatabaseDescriptor.getCommitLogLocation(), CORRUPTED_COMMIT_LOG_FILE_NAME).deleteIfExists();
    }
}
