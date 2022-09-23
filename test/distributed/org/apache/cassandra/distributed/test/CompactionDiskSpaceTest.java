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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileStoreUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.ActiveCompactions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.fail;

public class CompactionDiskSpaceTest extends TestBaseImpl
{
    @Test
    public void testNoSpaceLeft() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(1)
                                          .withConfig(config -> config.set("min_free_space_per_drive_in_mb", "0"))
                                          .withDataDirCount(3)
                                          .withInstanceInitializer(BB::install)
                                          .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, x int) with compaction = {'class':'SizeTieredCompactionStrategy'}");
            cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, x) values (1,1)", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            cluster.setUncaughtExceptionsFilter((t) -> t.getMessage() != null && t.getMessage().contains("Not enough space for compaction") && t.getMessage().contains(KEYSPACE+".tbl"));
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                BB.estimatedRemaining.set(2000);
                BB.freeSpace.set(2000);
                BB.sstableDir = cfs.getLiveSSTables().iterator().next().descriptor.directory;
                try
                {
                    cfs.forceMajorCompaction();
                    fail("This should fail, we have 2000b free space, 2000b estimated remaining compactions and the new compaction > 0");
                }
                catch (Exception e)
                {
                    // ignored
                }
                // and available space again:
                BB.estimatedRemaining.set(1000);
                cfs.forceMajorCompaction();

                BB.estimatedRemaining.set(2000);
                BB.freeSpace.set(10000);
                cfs.forceMajorCompaction();

                // make sure we fail if other dir on the same file store runs out of disk
                BB.freeSpace.set(0);
                for (Directories.DataDirectory newDir : cfs.getDirectories().getWriteableLocations())
                {
                    File newSSTableDir = cfs.getDirectories().getLocationForDisk(newDir);
                    if (!BB.sstableDir.equals(newSSTableDir))
                    {
                        BB.sstableDir = cfs.getDirectories().getLocationForDisk(newDir);
                        break;
                    }
                }
                try
                {
                    cfs.forceMajorCompaction();
                    fail("this should fail, data dirs share filestore");
                }
                catch (Exception e)
                {
                    //ignored
                }
            });
        }
    }

    public static class BB
    {
        static final AtomicLong estimatedRemaining = new AtomicLong();
        static final AtomicLong freeSpace = new AtomicLong();
        static File sstableDir;
        public static void install(ClassLoader cl, Integer node)
        {
            new ByteBuddy().rebase(ActiveCompactions.class)
                           .method(named("estimatedRemainingWriteBytes"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(FileStoreUtils.class)
                           .method(named("tryGetSpace"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Map<File, Long> estimatedRemainingWriteBytes()
        {
            if (sstableDir != null)
                return ImmutableMap.of(sstableDir, estimatedRemaining.get());
            return Collections.emptyMap();
        }

        public static long tryGetSpace(FileStore fileStore, PathUtils.IOToLongFunction<FileStore> function)
        {
            try
            {
                if (sstableDir != null && Files.getFileStore(sstableDir.toPath()).equals(fileStore))
                    return freeSpace.get();
            }
            catch (IOException e)
            {
                // ignore
            }
            return Long.MAX_VALUE;
        }
    }
}