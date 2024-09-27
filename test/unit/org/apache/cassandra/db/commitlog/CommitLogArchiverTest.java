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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.apache.cassandra.io.util.PathUtils.forEach;
import static org.junit.Assert.assertTrue;

public class CommitLogArchiverTest extends CQLTester
{
    private static Path dirName;
    // "2024:03:22 20:43:12.633222";
    private static final long rpiTime = 1711140192633222L;
    private File dir;

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static MockedStatic<CommitLogArchiver> archiverMock;

    @BeforeClass
    public static void setUpClass()
    {
        try
        {
            dirName = temporaryFolder.newFolder().toPath();
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }

        archiverMock = Mockito.mockStatic(CommitLogArchiver.class);
        archiverMock.when(CommitLogArchiver::construct)
                    .thenReturn(new CommitLogArchiver("/bin/cp %path " + dirName,
                                                      "/bin/cp -f %from %to",
                                                      dirName.toString(),
                                                      1711140192633222L,
                                                      CommitLogPosition.NONE,
                                                      TimeUnit.MICROSECONDS));

        CQLTester.setUpClass();
    }

    @AfterClass
    public static void tearDownClass()
    {
        CQLTester.tearDownClass();
        archiverMock.close();
    }

    @Before
    public void before()
    {
        dir = new File(dirName);
        // to prevent other test cases' archive files from affecting us
        if (dir.isDirectory() && dir.tryList().length > 0)
            forEach(dirName, PathUtils::deleteRecursive);
    }

    @Test
    public void testArchiver()
    {
        String table = createTable(KEYSPACE, "CREATE TABLE %s (a TEXT PRIMARY KEY, b blob);");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);

        ByteBuffer value = ByteBuffer.allocate(1024);
        // Make sure that new CommitLogSegment will be allocated as the CommitLogSegment size is 5M
        // and if new CommitLogSegment is allocated then the old CommitLogSegment will be archived.
        for (int i = 1; i <= 10; ++i)
        {
            new RowUpdateBuilder(cfs.metadata(), rpiTime - i, "name-" + i)
            .add("b", value)
            .build()
            .apply();
        }

        CommitLog.instance.forceRecycleAllSegments();
        CommitLog.instance.segmentManager.awaitManagementTasksCompletion();
        // If the number of files that under backup dir is bigger than 1, that means the
        // archiver for commitlog is effective.
        assertTrue(dir.isDirectory() && dir.tryList().length > 0);
    }

    @Test
    public void testRestoreInDifferentPrecision() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a INT , b INT, c INT, PRIMARY KEY(a, b));");
        // default level is microsecond
        long timeInMicroSecond1 = rpiTime;
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ? ", 3, 0, 0, timeInMicroSecond1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ? ", 3, 1, 1, timeInMicroSecond1);

        long timeInMicroSecond2 = timeInMicroSecond1 + 1;
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 4, 0, 0, timeInMicroSecond2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 4, 1, 1, timeInMicroSecond2);
        assertRows(execute("SELECT * FROM %s"), row(4, 0, 0), row(4, 1, 1), row(3, 0, 0), row(3, 1, 1));

        CommitLog.instance.forceRecycleAllSegments();
        CommitLog.instance.segmentManager.awaitManagementTasksCompletion();
        execute("TRUNCATE TABLE %s");
        assertRowCount(execute("SELECT * FROM %s"), 0);

        // replay log
        CommitLog.instance.archiver.maybeRestoreArchive();
        CommitLogSegment.resetReplayLimit();
        // restore archived files
        CommitLog.instance.recoverFiles(CommitLog.instance.getUnmanagedFiles());
        // restore poin time is rpiTime in microseconds , so row(4, 0, 0) and row(4, 1, 1) is skipped
        assertRows(execute("SELECT * FROM %s"), row(3, 0, 0), row(3, 1, 1));

        // set to millisecond level
        CommitLog.instance.archiver.setPrecision(TimeUnit.MILLISECONDS);
        long timeInMilliSecond3 = timeInMicroSecond1 / 1000 + 1;
        execute("TRUNCATE TABLE %s");
        assertRowCount(execute("SELECT * FROM %s"), 0);

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ? ", 2, 0, 0, timeInMilliSecond3);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ? ", 2, 1, 1, timeInMilliSecond3);

        long timeInMilliSecond4 = timeInMilliSecond3 - 1;
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ? ", 1, 0, 0, timeInMilliSecond4);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ? ", 1, 1, 1, timeInMilliSecond4);

        assertRows(execute("SELECT * FROM %s"), row(1, 0, 0), row(1, 1, 1), row(2, 0, 0), row(2, 1, 1));

        CommitLog.instance.forceRecycleAllSegments();
        CommitLog.instance.segmentManager.awaitManagementTasksCompletion();
        execute("TRUNCATE TABLE %s");
        assertRowCount(execute("SELECT * FROM %s"), 0);
        // replay log
        CommitLog.instance.archiver.maybeRestoreArchive();
        CommitLogSegment.resetReplayLimit();
        CommitLog.instance.recoverFiles(CommitLog.instance.getUnmanagedFiles());
        // restore poin time is rpiTime in millseconds, so row(2, 0, 0) and row(2, 1, 1) is skipped
        assertRows(execute("SELECT * FROM %s"), row(1, 0, 0), row(1, 1, 1));
    }
}
