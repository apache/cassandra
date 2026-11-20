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

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "Record the files dropped from the page cache while they exist",
            targetClass = "NativeLibrary",
            targetMethod = "trySkipCache(String, long, long)",
            action = "org.apache.cassandra.db.SSTableImporterTest.droppingFromPageCache($1)"),
    @BMRule(name = "Record the synced files",
            targetClass = "SyncUtil",
            targetMethod = "force(org.apache.cassandra.io.util.File)",
            action = "org.apache.cassandra.db.SSTableImporterTest.synced.add($1.absolutePath())")
})
public class SSTableImporterTest
{
    private static final String KEYSPACE = "SSTableImporterTest";
    private static final String SYNCED = "Synced";
    private static final String COPIED = "Copied";
    private static final String CACHED = "Cached";
    private static final String COMPACTED = "Compacted";
    private static final String CRASHED = "Crashed";

    private static final Set<String> droppedFromPageCache = ConcurrentHashMap.newKeySet();
    static final Set<String> synced = ConcurrentHashMap.newKeySet();

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, SYNCED),
                                    SchemaLoader.standardCFMD(KEYSPACE, COPIED),
                                    SchemaLoader.standardCFMD(KEYSPACE, CACHED),
                                    SchemaLoader.standardCFMD(KEYSPACE, COMPACTED),
                                    SchemaLoader.standardCFMD(KEYSPACE, CRASHED));
    }

    public static void droppingFromPageCache(String path)
    {
        if (new File(path).exists())
            droppedFromPageCache.add(path);
    }

    @Test
    public void testImportSyncsTheSSTables() throws IOException
    {
        for (boolean copyData : new boolean[]{ true, false })
        {
            ColumnFamilyStore cfs = table(SYNCED);
            Path staged = writeSSTableAndMoveToDirectory(cfs, "synced");
            List<String> stagedFiles = Stream.of(new File(staged).tryList()).map(File::absolutePath).collect(Collectors.toList());
            assertThat(new SSTableImporter(cfs).importNewSSTables(SSTableImporter.Options.options(staged.toString()).verifySSTables(true).copyData(copyData).build())).isEmpty();
            assertThat(synced).containsAll(stagedFiles);
            unload(cfs);
        }
    }

    @Test
    @BMRule(name = "Fail to hard link sstables",
            targetClass = "org.apache.cassandra.io.sstable.SSTable",
            targetMethod = "hardlink",
            action = "throw new org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"Invalid cross-device link\"))")
    public void testImportSyncsTheCopiesItMakesWhenItCannotLinkTheSSTables() throws IOException
    {
        ColumnFamilyStore cfs = table(COPIED);
        Path staged = writeSSTableAndMoveToDirectory(cfs, "copied");
        assertThat(new SSTableImporter(cfs).importNewSSTables(SSTableImporter.Options.options(staged.toString()).verifySSTables(true).copyData(true).build())).isEmpty();
        Descriptor copy = Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor;
        assertThat(synced).contains(copy.fileFor(Components.DATA).absolutePath());
    }

    @Test
    @BMRule(name = "Tidy the readers that verified the sstables before moving them",
            targetClass = "SSTableImporter",
            targetMethod = "moveSSTables",
            action = "org.apache.cassandra.db.lifecycle.LifecycleTransaction.waitForDeletions()")
    public void testImportKeepsTheSSTablesInThePageCache() throws IOException
    {
        for (boolean copyData : new boolean[]{ true, false })
        {
            ColumnFamilyStore cfs = table(CACHED);
            Path staged = writeSSTableAndMoveToDirectory(cfs, "cached");
            assertThat(new SSTableImporter(cfs).importNewSSTables(SSTableImporter.Options.options(staged.toString()).verifySSTables(true).copyData(copyData).build())).isEmpty();
            assertThat(droppedFromPageCache).noneMatch(path -> path.contains(CACHED));
            unload(cfs);
        }
    }

    private static ColumnFamilyStore table(String name)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(name);
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static void write(ColumnFamilyStore cfs, int key)
    {
        SchemaLoader.insertData(KEYSPACE, cfs.getTableName(), key, 1);
        Util.flush(cfs);
    }

    private static Descriptor unload(ColumnFamilyStore cfs)
    {
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        cfs.clearUnsafe();
        sstable.selfRef().release();
        return sstable.descriptor;
    }

    private static Path writeSSTableAndMoveToDirectory(ColumnFamilyStore cfs, String name) throws IOException
    {
        write(cfs, 0);
        Descriptor descriptor = unload(cfs);
        Path staged = Files.createDirectories(Files.createTempDirectory(name).toRealPath().resolve(KEYSPACE).resolve(descriptor.directory.name()));
        for (File file : descriptor.directory.tryList(file -> file.name().startsWith(descriptor.baseFile().name())))
            Files.move(file.toPath(), staged.resolve(file.name()));
        return staged;
    }
}
