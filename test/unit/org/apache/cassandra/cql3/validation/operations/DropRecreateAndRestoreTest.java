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
package org.apache.cassandra.cql3.validation.operations;

import java.util.List;

import org.apache.cassandra.io.util.File;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableId;

public class DropRecreateAndRestoreTest extends CQLTester
{
    @Test
    public void testCreateWithIdRestore() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);


        long time = System.currentTimeMillis();
        TableId id = currentTableMetadata().id;
        assertRows(execute("SELECT * FROM %s"), row(0, 0, 0), row(0, 1, 1));
        Thread.sleep(5);

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);
        assertRows(execute("SELECT * FROM %s"), row(1, 0, 2), row(1, 1, 3), row(0, 0, 0), row(0, 1, 1));

        // Drop will flush and clean segments. Hard-link them so that they can be restored later.
        List<String> segments = CommitLog.instance.getActiveSegmentNames();
        File logPath = new File(DatabaseDescriptor.getCommitLogLocation());
        for (String segment: segments)
            FileUtils.createHardLink(new File(logPath, segment), new File(logPath, segment + ".save"));

        execute("DROP TABLE %s");

        assertInvalidThrow(InvalidRequestException.class, "SELECT * FROM %s");

        execute(String.format("CREATE TABLE %%s (a int, b int, c int, PRIMARY KEY(a, b)) WITH ID = %s", id));

        // Restore saved segments
        for (String segment: segments)
            FileUtils.renameWithConfirm(new File(logPath, segment + ".save"), new File(logPath, segment));
        try
        {
            // Restore to point in time.
            CommitLog.instance.archiver.restorePointInTime = time;
            CommitLog.instance.resetUnsafe(false);
        }
        finally
        {
            CommitLog.instance.archiver.restorePointInTime = Long.MAX_VALUE;
        }

        assertRows(execute("SELECT * FROM %s"), row(0, 0, 0), row(0, 1, 1));
    }

    @Test(expected = AlreadyExistsException.class)
    public void testCreateWithIdDuplicate() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        TableId id = currentTableMetadata().id;
        execute(String.format("CREATE TABLE %%s (a int, b int, c int, PRIMARY KEY(a, b)) WITH ID = %s", id));
    }

    @Test(expected = ConfigurationException.class)
    public void testCreateWithIdInvalid() throws Throwable
    {
        createTableMayThrow(String.format("CREATE TABLE %%s (a int, b int, c int, PRIMARY KEY(a, b)) WITH ID = %s", 55));
    }

    @Test(expected = ConfigurationException.class)
    public void testAlterWithId() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        TableId id = currentTableMetadata().id;
        execute(String.format("ALTER TABLE %%s WITH ID = %s", id));
    }
}
