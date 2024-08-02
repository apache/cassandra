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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.File;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class CommitLogArchiverTest extends CQLTester
{
    private static String dirName = "backup_commitlog";
    private static Path backupDir;
    private static String rpiTime = "2024:03:22 20:43:12.633222";
    private static CommitLogArchiver archiver;

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        backupDir = Files.createTempDirectory(dirName);
        CommitLog commitLog = CommitLog.instance;
        Properties properties = new Properties();
        archiver = commitLog.archiver;
        properties.putAll(new HashMap<String, String>() {{
                          put("archive_command", "/bin/cp %path " + backupDir.toString());
                          put("restore_command", "/bin/cp -f %from %to");
                          put("restore_directories", backupDir.toString());
                          put("restore_point_in_time", rpiTime);}});
        CommitLogArchiver commitLogArchiver = CommitLogArchiver.getArchiverFromProperty(properties);
        commitLog.setCommitlogArchiver(commitLogArchiver);
    }

    @AfterClass
    public static void afterClass() throws IOException
    {
        File dir = new File(backupDir);
        dir.deleteRecursive();
        CommitLog.instance.setCommitlogArchiver(archiver);
    }

    @Test
    public void testArchiveAndRestore() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        String table = createTable(KEYSPACE, "CREATE TABLE %s (a TEXT PRIMARY KEY, b INT);");

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        long ts = CommitLogArchiver.getMicroSeconds(rpiTime);
        for (int i = 1; i <= 10; ++i)
        {
            new RowUpdateBuilder(cfs.metadata(), ts - i, "name-" + i)
                    .add("b", i)
                    .build()
                    .apply();
        }
        // commitlog may be archived and deleted by resetUnsafe then the commitlog in the write path may throw no such file exception
        CommitLog.instance.resetUnsafe(false, false);
        assertTrue(Files.list(backupDir).count() > 0);
    }
}
