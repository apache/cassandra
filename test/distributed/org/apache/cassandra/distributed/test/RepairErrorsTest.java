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
import java.util.Collection;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

public class RepairErrorsTest extends TestBaseImpl
{
    @Test
    public void testRemoteValidationFailure() throws IOException
    {
        Cluster.Builder builder = Cluster.build(2)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .withInstanceInitializer(ByteBuddyHelper::install);
        try (Cluster cluster = builder.createWithoutStarting())
        {
            cluster.setUncaughtExceptionsFilter((i, throwable) -> {
                if (i == 2)
                    return throwable.getMessage() != null && throwable.getMessage().contains("IGNORE");
                return false;
            });

            cluster.startup();
            init(cluster);

            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, x int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));
            long mark = cluster.get(1).logs().mark();
            cluster.forEach(i -> i.nodetoolResult("repair", "--full").asserts().failure());
            Assertions.assertThat(cluster.get(1).logs().grep(mark, "^ERROR").getResult()).isEmpty();
        }
    }

    @Test
    public void testNoSuchRepairSessionAnticompaction() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .withInstanceInitializer(ByteBuddyHelper::installACNoSuchRepairSession)
                                           .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, x int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));
            long mark = cluster.get(1).logs().mark();
            cluster.forEach(i -> i.nodetoolResult("repair", KEYSPACE).asserts().failure());
            assertTrue(cluster.get(1).logs().grep(mark, "^ERROR").getResult().isEmpty());
        }
    }

    public static class ByteBuddyHelper
    {
        public static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(CompactionIterator.class)
                               .method(named("next"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void installACNoSuchRepairSession(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(CompactionManager.class)
                               .method(named("validateSSTableBoundsForAnticompaction"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static UnfilteredRowIterator next()
        {
            throw new RuntimeException("IGNORE");
        }

        @SuppressWarnings("unused")
        public static void validateSSTableBoundsForAnticompaction(UUID sessionID,
                                                                  Collection<SSTableReader> sstables,
                                                                  RangesAtEndpoint ranges)
        {
            throw new CompactionInterruptedException(String.valueOf(sessionID));
        }
    }
}
