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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VerifyTest extends CQLTester
{

    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testVerifyDefault()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk,ck))");
        createIndex("CREATE INDEX idx1 ON %s(a) USING 'sai'");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", i, i * 2, i * 3, i * 4);
        }

        flush(keyspace());

        invokeNodetool("verify", "--force", keyspace(), currentTable()).assertOnCleanExit();
    }

    @Test
    public void testVerifySaiOnly()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX idx1 ON %s(a) USING 'sai'");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", i, i * 2, i * 3, i * 4);
        }
        flush(keyspace());

        // SAI-only mode: verify only SAI components
        invokeNodetool("verify", "--force", "--sai-only", keyspace(), currentTable()).assertOnCleanExit();
    }

    @Test
    public void testVerifyIncludeSai()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX idx1 ON %s(a) USING 'sai'");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", i, i * 2, i * 3, i * 4);
        }
        flush(keyspace());

        // Include-sai mode: verify both data files and SAI components
        invokeNodetool("verify", "--force", "--include-sai", keyspace(), currentTable()).assertOnCleanExit();
    }

    @Test
    public void testVerifySaiOnlyWithoutSaiIndex()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        // No SAI index created

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", i, i * 2, i * 3, i * 4);
        }
        flush(keyspace());

        // SAI-only mode on table without SAI should succeed (skipped)
        invokeNodetool("verify", "--force", "--sai-only", keyspace(), currentTable()).assertOnCleanExit();
    }

    @Test
    public void testVerifyConflictingFlags()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");

        // Both --sai-only and --include-sai should fail
        ToolRunner.ToolResult result = invokeNodetool("verify", "--force", "--sai-only", "--include-sai", keyspace(), currentTable());
        result.asserts().failure();
        result.getStdout().contains("Cannot specify both --sai-only and --include-sai");
    }

    @Test
    public void testVerifySaiWithCorruption ()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX idx1 ON %s(a) USING 'sai'");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", i, i * 2, i * 3, i * 4);
        }
        flush(keyspace());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull("SAI group should exist", group);

        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        Set<Component> saiComponents = StorageAttachedIndexGroup.getLiveComponents(sstable,
                                                                                   group.getIndexes().stream().map(i -> (StorageAttachedIndex)i).collect(Collectors.toSet()));

        assertFalse("SAI components should exist", saiComponents.isEmpty());

        Component componentToCorrupt = saiComponents.iterator().next();
        File saiFile = sstable.descriptor.fileFor(componentToCorrupt);


        assertTrue("SAI file should exist", saiFile.exists());


        try (FileChannel channel = saiFile.newReadWriteChannel())
        {
            channel.truncate(10);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        invokeNodetool("verify", "--force", keyspace(), currentTable()).asserts().success();
        invokeNodetool("verify", "--force", "--sai-only", keyspace(), currentTable()).asserts().failure().errorContains("file truncated");
        invokeNodetool("verify", "--force", "--include-sai", keyspace(), currentTable()).asserts().failure().errorContains("file truncated");
    }
}
