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

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.junit.Assert.fail;

public class LeveledCompactionTaskTest extends TestBaseImpl
{
    @Test
    public void testBuildCompactionCandidatesForAvailableDiskSpace() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(config -> config.set("autocompaction_on_startup_enabled", false))
                                             .withInstanceInitializer(BB::install).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (id int primary key) WITH compaction = {'class':'LeveledCompactionStrategy', 'enabled':'false'}"));
            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id) VALUES (?)"), ConsistencyLevel.ALL, i);
                if (i % 10 == 0)
                    cluster.get(1).flush(KEYSPACE);
            }
            cluster.get(1).flush(KEYSPACE);
            cluster.setUncaughtExceptionsFilter((exception) -> exception.getMessage() != null && exception.getMessage().contains("Not enough space for compaction"));

            cluster.get(1).runOnInstance(() -> {
                BB.hasDiskSpaceResult = false;
                try
                {
                    Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").enableAutoCompaction(true);
                    fail("This should fail, but only due to having no disk space");
                }
                catch (Exception e)
                {
                    if (!(e.getMessage() != null && e.getMessage().contains("Not enough space for compaction")))
                        throw e;
                }
            });
        }
    }

    public static class BB
    {
        static volatile boolean hasDiskSpaceResult = true;

        @SuppressWarnings("resource")
        public static void install(ClassLoader cl, int id)
        {
            new ByteBuddy().rebase(Directories.class)
                           .method(named("hasAvailableDiskSpace").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static boolean hasAvailableDiskSpace(long estimatedSSTables, long expectedTotalWriteSize)
        {
            return hasDiskSpaceResult;
        }
    }
}
