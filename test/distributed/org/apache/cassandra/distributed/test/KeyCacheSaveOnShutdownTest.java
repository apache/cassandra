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

import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class KeyCacheSaveOnShutdownTest extends TestBaseImpl
{
    private static final int PARTITIONS = 100;

    @Test
    public void drainSavesKeyCacheWhenEnabled() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                               .set("key_cache_save_on_shutdown", true)
                                                               .set("key_cache_size", "50MiB")
                                                               .set("sstable", Map.of("selected_format", "big")))
                                             .start()))
        {
            IInvokableInstance node = cluster.get(1);
            populateKeyCache(cluster);
            assertThat(cachedKeysForTable(node)).isEqualTo(PARTITIONS);

            long mark = node.logs().mark();
            node.nodetoolResult("drain").asserts().success();
            assertThat(node.logs().grep(mark, "Saving key cache before shutdown").getResult()).hasSize(1);

            node.shutdown().get();
            node.startup();

            // The in-JVM startup does not load saved caches, so load the file written by drain explicitly.
            node.runOnInstance(() -> CacheService.instance.keyCache.loadSaved());
            assertThat(cachedKeysForTable(node)).isEqualTo(PARTITIONS);
        }
    }

    @Test
    public void drainDoesNotSaveKeyCacheByDefault() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                               .set("key_cache_size", "50MiB")
                                                               .set("sstable", Map.of("selected_format", "big")))
                                             .start()))
        {
            IInvokableInstance node = cluster.get(1);
            populateKeyCache(cluster);
            assertThat(cachedKeysForTable(node)).isEqualTo(PARTITIONS);

            long mark = node.logs().mark();
            node.nodetoolResult("drain").asserts().success();
            assertThat(node.logs().grep(mark, "Saving key cache before shutdown").getResult()).isEmpty();

            node.shutdown().get();
            node.startup();

            node.runOnInstance(() -> CacheService.instance.keyCache.loadSaved());
            assertThat(cachedKeysForTable(node)).isZero();
        }
    }

    @Test
    public void drainCompletesWhenKeyCacheSaveFails() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                               .set("key_cache_save_on_shutdown", true)
                                                               .set("key_cache_size", "50MiB")
                                                               .set("sstable", Map.of("selected_format", "big")))
                                             .start()))
        {
            IInvokableInstance node = cluster.get(1);
            populateKeyCache(cluster);
            assertThat(cachedKeysForTable(node)).isEqualTo(PARTITIONS);

            // AutoSavingCache.Writer requires the saved caches directory to exist.
            node.runOnInstance(() -> new File(DatabaseDescriptor.getSavedCachesLocation()).deleteRecursive());

            long mark = node.logs().mark();
            node.nodetoolResult("drain").asserts().success();
            assertThat(node.logs().grep(mark, "Unable to save key cache during drain").getResult()).hasSize(1);
            assertThat(node.callOnInstance(() -> StorageService.instance.isDrained())).isTrue();

            node.shutdown().get();
            node.startup();

            node.runOnInstance(() -> CacheService.instance.keyCache.loadSaved());
            assertThat(cachedKeysForTable(node)).isZero();
            assertThat(node.executeInternal(withKeyspace("SELECT pk FROM %s.tbl")).length).isEqualTo(PARTITIONS);
        }
    }

    private static void populateKeyCache(Cluster cluster)
    {
        IInvokableInstance node = cluster.get(1);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v text)"));
        for (int i = 0; i < PARTITIONS; i++)
        {
            node.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, 'v')"), i);
        }
        node.flush(KEYSPACE);

        // Key cache entries are only created by reads that go to an sstable.
        for (int i = 0; i < PARTITIONS; i++)
        {
            node.executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE pk = ?"), i);
        }
    }

    private static int cachedKeysForTable(IInvokableInstance node)
    {
        return node.callOnInstance(() ->
        {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            int count = 0;
            Iterator<KeyCacheKey> keys = CacheService.instance.keyCache.keyIterator();
            while (keys.hasNext())
            {
                if (keys.next().sameTable(cfs.metadata()))
                {
                    count++;
                }
            }
            return count;
        });
    }
}
