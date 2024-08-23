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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class CommitLogArchiverTest extends CQLTester
{
    private static Path dirName = Path.of("/tmp/backup_commitlog_for_test");
    private static String rpiTime = "2024:03:22 20:43:12.633222";
    private static CommitLogArchiver archiver;

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        PathUtils.createDirectoryIfNotExists(dirName);
        CommitLog commitLog = CommitLog.instance;
        Properties properties = new Properties();
        archiver = commitLog.archiver;
        properties.putAll(Map.of("archive_command", "/bin/cp %path " + dirName,
                                 "restore_command", "/bin/cp -f %from %to",
                                 "restore_directories", dirName,
                                 "restore_point_in_time", rpiTime));
        CommitLogArchiver commitLogArchiver = CommitLogArchiver.getArchiverFromProperty(properties);
        // set the archiver at the very beginning
        commitLog.setCommitlogArchiver(commitLogArchiver);
        // set the CommitLogSegment size to 1M
        DatabaseDescriptor.setCommitLogSegmentSize(1);
    }

    @AfterClass
    public static void afterClass() throws IOException
    {
        File dir = new File(dirName);
        dir.deleteRecursive();
    }

    @Test
    public void testArchiver()
    {
        File dir = new File(dirName);
        assertTrue(dir.isDirectory() && dir.tryList().length == 0);

        String table = createTable(KEYSPACE, "CREATE TABLE %s (a TEXT PRIMARY KEY, b TEXT);");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        long ts = CommitLogArchiver.getMicroSeconds(rpiTime);

        String value = "";
        // Make sure that new CommitLogSegment will be allocated as the CommitLogSegment size is 1M
        // and if new CommitLogSegment is allocated then the old CommitLogSegment will be archived.
        for (int i = 0; i != 50000; ++i)
        {
            value += i;
        }
        for (int i = 1; i <= 2000; ++i)
        {
            new RowUpdateBuilder(cfs.metadata(), ts - i, "name-" + i)
                    .add("b", value)
                    .build()
                    .apply();
        }

        // If the number of files that under backup dir is bigger than 1, that means the
        // arhiver for commitlog is effective.
        assertTrue(dir.isDirectory() && dir.tryList().length > 0);
        // wait for all task to be executed
        Map<String, Future<?>>  archivePending = CommitLog.instance.archiver.archivePending;
        CommitLogArchiver oldArchive = CommitLog.instance.archiver;
        CommitLog.instance.setCommitlogArchiver(archiver);
        for (String name : archivePending.keySet())
        {
            oldArchive.maybeWaitForArchiving(name);
        }
    }
}
