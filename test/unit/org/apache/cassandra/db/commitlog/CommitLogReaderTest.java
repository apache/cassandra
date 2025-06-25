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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.util.File;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class CommitLogReaderTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReaderTest.class);
    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();

        DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);
        JVMStabilityInspector.replaceKiller(new KillerForTests(false));

        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.batch);

        // Once per-JVM is enough
        prepareServer();
    }

    @Before
    public void before() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
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
        Assert.assertTrue("Didn't find any commit log files.", 0 != results.size());
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

        public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
        {
            sawStopOnErrorCheck = true;
            return false;
        }

        public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
        {
            sawStopOnErrorCheck = true;
        }

        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            if ((metadata == null) || (metadata != null && m.getPartitionUpdate(metadata) != null)) {
                seenMutations.add(m);
            }
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
     @Test
    public void testSkipCorruptCommitLogFile() throws Throwable {
    // Arrange
    CommitLogReader reader = new CommitLogReader();
    TestCLRHandler testHandler = new TestCLRHandler();

   // Create a temp file to simulate a corrupt commit log
    java.io.File tmp = java.io.File.createTempFile("corrupt", ".log");
    tmp.deleteOnExit();
    try (java.io.FileOutputStream out = new java.io.FileOutputStream(tmp)) {
        out.write(new byte[]{0x00, 0x01, 0x02}); // garbage content
    }

    // Wrap into Cassandra File object
    File fakeCorruptFile = new File(tmp.getAbsolutePath());

    // Act: Try reading it
    try {
        reader.readAllFiles(testHandler, new File[]{fakeCorruptFile}, new CommitLogPosition(0, 0));
    } catch (Throwable t) {
        Assert.fail("Should not throw exception even for corrupt log file: " + t.getMessage());
    }

    // Assert
    Assert.assertEquals(0, testHandler.seenMutationCount());
    logger.info("Test passed: corrupt commit log file was skipped successfully without loop or crash");
}
}
}
