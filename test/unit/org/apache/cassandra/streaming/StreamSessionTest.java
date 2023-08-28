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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.cassandra.io.util.File;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.Util;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamSessionTest extends CQLTester
{
    private static Directories dirs;
    private static Directories dirs2;

    private static List<File> files;
    private static List<Directories.DataDirectory> datadirs;

    private static ColumnFamilyStore cfs;
    private static ColumnFamilyStore cfs2;
    private static List<FakeFileStore> filestores = Lists.newArrayList(new FakeFileStore(), new FakeFileStore(), new FakeFileStore());

    @BeforeClass
    public static void before() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        ByteBuddyAgent.install();
        new ByteBuddy().redefine(ColumnFamilyStore.class)
                       .method(named("getIfExists").and(takesArguments(1)))
                       .intercept(MethodDelegation.to(BBKeyspaceHelper.class))
                       .make()
                       .load(ColumnFamilyStore.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
        Path tmpDir = Files.createTempDirectory("StreamSessionTest");
        files = Lists.newArrayList(new File(tmpDir, "1"),
                                   new File(tmpDir, "2"),
                                   new File(tmpDir, "3"));
        datadirs = files.stream().map(Directories.DataDirectory::new).collect(Collectors.toList());
        DatabaseDescriptor.setMinFreeSpacePerDriveInMebibytes(0);
        DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(1.0);
    }

    @Test
    public void basicDiskSpaceTest() throws InterruptedException
    {
        createTable("create table %s (k int primary key, i int)");
        dirs = new Directories(getCurrentColumnFamilyStore().metadata(), datadirs);
        cfs = new MockCFS(getCurrentColumnFamilyStore(), dirs);

        Map<TableId, Long> perTableIdIncomingBytes = new HashMap<>();
        perTableIdIncomingBytes.put(cfs.metadata.id, 999L);

        filestores.get(0).usableSpace = 334;
        filestores.get(1).usableSpace = 334;
        filestores.get(2).usableSpace = 334;

        Keyspace.all().forEach(ks -> ks.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));
        do
        {
            Thread.sleep(100);
        } while (!CompactionManager.instance.active.getCompactions().isEmpty());

        assertTrue(StreamSession.checkDiskSpace(perTableIdIncomingBytes, nextTimeUUID(), filestoreMapper));

        filestores.get(0).usableSpace = 332;
        assertFalse(StreamSession.checkDiskSpace(perTableIdIncomingBytes, nextTimeUUID(), filestoreMapper));

    }

    @Test
    public void multiTableDiskSpaceTest() throws InterruptedException
    {
        createTable("create table %s (k int primary key, i int)");
        dirs = new Directories(getCurrentColumnFamilyStore().metadata(), datadirs.subList(0,2));
        cfs = new MockCFS(getCurrentColumnFamilyStore(), dirs);
        createTable("create table %s (k int primary key, i int)");
        dirs2 = new Directories(getCurrentColumnFamilyStore().metadata(), datadirs.subList(1,3));
        cfs2 = new MockCFS(getCurrentColumnFamilyStore(), dirs2);

        Map<TableId, Long> perTableIdIncomingBytes = new HashMap<>();
        // cfs has datadirs 0, 1
        // cfs2 has datadirs 1, 2
        // this means that the datadir 1 will get 1000 bytes streamed, and the other ddirs 500bytes:
        perTableIdIncomingBytes.put(cfs.metadata.id, 1000L);
        perTableIdIncomingBytes.put(cfs2.metadata.id, 1000L);

        filestores.get(0).usableSpace = 501;
        filestores.get(1).usableSpace = 1001;
        filestores.get(2).usableSpace = 501;

        Keyspace.all().forEach(ks -> ks.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));
        do
        {
            Thread.sleep(100);
        } while (!CompactionManager.instance.active.getCompactions().isEmpty());

        assertTrue(StreamSession.checkDiskSpace(perTableIdIncomingBytes, nextTimeUUID(), filestoreMapper));

        filestores.get(1).usableSpace = 999;
        assertFalse(StreamSession.checkDiskSpace(perTableIdIncomingBytes, nextTimeUUID(), filestoreMapper));

    }

    static Function<File, FileStore> filestoreMapper = (f) -> {
        for (int i = 0; i < files.size(); i++)
        {
            if (f.toPath().startsWith(files.get(i).toPath()))
                return filestores.get(i);
        }
        throw new RuntimeException("Bad file: "+f);
    };

    private static class MockCFS extends ColumnFamilyStore
    {
        MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), Util.newSeqGen(), cfs.metadata, dirs, false, false, true);
        }
    }

    // return our mocked tables:
    public static class BBKeyspaceHelper
    {
        public static ColumnFamilyStore getIfExists(TableId id)
        {
            if (id == cfs.metadata.id)
                return cfs;
            return cfs2;
        }
    }

    public static class FakeFileStore extends FileStore
    {
        public long usableSpace;

        @Override
        public long getUsableSpace()
        {
            return usableSpace;
        }

        @Override
        public String name() {return null;}
        @Override
        public String type() {return null;}
        @Override
        public boolean isReadOnly() {return false;}
        @Override
        public long getTotalSpace() {return 0;}
        @Override
        public long getUnallocatedSpace() {return 0;}
        @Override
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {return false;}
        @Override
        public boolean supportsFileAttributeView(String name) {return false;}
        @Override
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {return null;}
        @Override
        public Object getAttribute(String attribute) throws IOException {return null;}
    }
}
