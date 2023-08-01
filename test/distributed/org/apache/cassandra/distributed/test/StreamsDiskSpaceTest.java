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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.io.util.File;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.ActiveCompactions;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.streaming.StreamManager;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class StreamsDiskSpaceTest extends TestBaseImpl
{
    @Test
    public void testAbortStreams() throws Exception
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .withInstanceInitializer((cl, id) -> BB.doInstall(cl, id, StreamManager.class, "getTotalRemainingOngoingBytes"))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int) with compaction={'class': 'SizeTieredCompactionStrategy'}");
            for (int i = 0; i < 10000; i++)
                cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (?,?)", i, i);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).runOnInstance(() -> BB.ongoing.set(Long.MAX_VALUE / 2));
            cluster.get(1).nodetoolResult("repair", "-full").asserts().failure();
            cluster.get(2).nodetoolResult("repair", "-full").asserts().failure();
            cluster.get(2).runOnInstance(() -> BB.ongoing.set(0));
            cluster.get(1).nodetoolResult("repair", "-full").asserts().success();
        }
    }

    @Test
    public void testAbortStreamsWhenOngoingCompactionsLeaveInsufficientSpace() throws Exception
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .withInstanceInitializer((cl, id) -> BB.doInstall(cl, id, ActiveCompactions.class, "estimatedRemainingWriteBytes"))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int) with compaction={'class': 'SizeTieredCompactionStrategy'}");
            for (int i = 0; i < 10000; i++)
                cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (?,?)", i, i);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                BB.datadir = cfs.getDirectories().getLocationForDisk(cfs.getDirectories().getWriteableLocation(0));
            });
            cluster.get(2).runOnInstance(() -> BB.ongoing.set(Long.MAX_VALUE / 2));

            cluster.get(1).nodetoolResult("repair", "-full").asserts().failure();
            cluster.get(2).nodetoolResult("repair", "-full").asserts().failure();
            cluster.get(2).runOnInstance(() -> BB.ongoing.set(0));
            cluster.get(1).nodetoolResult("repair", "-full").asserts().success();
        }
    }

    @Test
    public void testAbortStreamsWhenTooManyPendingCompactions() throws Exception
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                      .set("reject_repair_compaction_threshold", 1024)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .withInstanceInitializer(BB::installCSMGetEstimatedRemainingTasks)
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int) with compaction={'class': 'SizeTieredCompactionStrategy'}");
            for (int i = 0; i < 10000; i++)
                cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (?,?)", i, i);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).runOnInstance(() -> {
                BB.ongoing.set(DatabaseDescriptor.getRepairPendingCompactionRejectThreshold() + 1);
            });

            cluster.get(1).nodetoolResult("repair", "-full").asserts().failure();
            cluster.get(2).nodetoolResult("repair", "-full").asserts().failure();
            cluster.get(2).runOnInstance(() -> BB.ongoing.set(0));
            cluster.get(1).nodetoolResult("repair", "-full").asserts().success();
        }
    }

    public static class BB
    {
        public static AtomicLong ongoing = new AtomicLong();
        public static File datadir;
        private static void doInstall(ClassLoader cl, int id, Class<?> clazz, String method)
        {
            if (id != 2)
                return;
            new ByteBuddy().rebase(clazz)
                           .method(named(method))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static long getTotalRemainingOngoingBytes()
        {
            return ongoing.get();
        }

        public static Map<File, Long> estimatedRemainingWriteBytes()
        {
            Map<File, Long> ret = new HashMap<>();
            if (datadir != null)
                ret.put(datadir, ongoing.get());
            return ret;
        }

        public static int getEstimatedRemainingTasks(int additionalSSTables, long additionalBytes, boolean isIncremental)
        {
            return (int) ongoing.get();
        }

        private static void installCSMGetEstimatedRemainingTasks(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(CompactionStrategyManager.class)
                               .method(named("getEstimatedRemainingTasks").and(takesArguments(3)))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }
}