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

import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;

public class IntegrationTestBase extends TestBaseImpl
{
    protected static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);
    protected static Cluster cluster;

    private static boolean initialized = false;

    @BeforeClass
    public static void before() throws Throwable
    {
        init(1, defaultConfig());
    }

    protected static void init(int nodes, Consumer<IInstanceConfig> cfg) throws Throwable
    {
        Invariants.checkState(!initialized);
        cluster = Cluster.build()
                         .withNodes(nodes)
                         .withConfig(cfg)
                         .createWithoutStarting();
        cluster.startup();
        cluster = init(cluster);
        initialized = true;
    }

    @AfterClass
    public static void afterClass()
    {
        if (cluster != null)
            cluster.close();
    }

    // TODO: meta-randomize this
    public static Consumer<IInstanceConfig> defaultConfig()
    {
        return (cfg) -> {
            cfg.set("row_cache_size", "50MiB")
               .set("index_summary_capacity", "50MiB")
               .set("counter_cache_size", "50MiB")
               .set("key_cache_size", "50MiB")
               .set("file_cache_size", "50MiB")
               .set("index_summary_capacity", "50MiB")
               .set("memtable_heap_space", "128MiB")
               .set("memtable_offheap_space", "128MiB")
               .set("memtable_flush_writers", 1)
               .set("concurrent_compactors", 1)
               .set("concurrent_reads", 5)
               .set("concurrent_writes", 5)
               .set("compaction_throughput_mb_per_sec", 10)
               .set("hinted_handoff_enabled", false);
        };
    }

}