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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.junit.Assert.assertEquals;

public class CompactionOverlappingSSTableTest extends TestBaseImpl
{
    @Test
    public void partialCompactionOverlappingTest() throws IOException, TimeoutException
    {

        try (Cluster cluster = init(builder().withNodes(1)
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(BB::install)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key) with compaction = {'class':'SizeTieredCompactionStrategy', 'enabled': 'false'} AND gc_grace_seconds=0"));
            Set<Integer> expected = Sets.newHashSetWithExpectedSize(990);
            for (int i = 0; i < 1000; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ONE, i);
                if (i >= 10)
                    expected.add(i);
            }
            cluster.get(1).flush(KEYSPACE);
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("delete from %s.tbl where id = ?"), ConsistencyLevel.ONE, i);
                cluster.get(1).flush(KEYSPACE);
            }
            assertEquals(expected, Arrays.stream(cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl"), ConsistencyLevel.ONE))
                                         .map(x -> x[0])
                                         .collect(Collectors.toSet()));

            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS); // make sure tombstones are gc:able

            cluster.get(1).runOnInstance(() -> {
                BB.enabled.set(true);
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                cfs.forceMajorCompaction();
                assertEquals("We should have 2 sstables (not 1) after major compaction since we reduced the scope of the compaction",
                             2, Iterables.size(cfs.getSSTables(SSTableSet.CANONICAL)));
            });
            assertEquals(expected, Arrays.stream(cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl"), ConsistencyLevel.ONE))
                                         .map(x -> x[0])
                                         .collect(Collectors.toSet()));
        }
    }

    public static class BB
    {
        static AtomicBoolean enabled = new AtomicBoolean();
        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(Directories.class)
                           .method(named("hasDiskSpaceForCompactionsAndStreams").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean hasDiskSpaceForCompactionsAndStreams(Map<File,Long> ignore1, Map<File,Long> ignore2)
        {
            if (enabled.get())
            {
                enabled.set(false);
                return false;
            }
            return true;
        }
    }
}
