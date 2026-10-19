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

package org.apache.cassandra.db.compression;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.CompressionDictionaryHelper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Dropping a table closes its dictionary cache. A periodic refresh which read the latest dictionary just before the
 * drop must not cache it afterwards: nothing would release it until it is garbage collected.
 */
@RunWith(BMUnitRunner.class)
public class CompressionDictionaryDropTest extends CQLTester
{
    private static volatile String pausedTable;
    private static final CountDownLatch read = new CountDownLatch(1);
    private static final CountDownLatch dropped = new CountDownLatch(1);
    private static final CountDownLatch added = new CountDownLatch(1);

    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();
        Config config = DatabaseDescriptor.getRawConfig();
        config.compression_dictionary_refresh_initial_delay = new DurationSpec.IntSecondsBound("1s");
        config.compression_dictionary_refresh_interval = new DurationSpec.IntSecondsBound("1s");
        prepareServer();
    }

    @Test
    @BMRules(rules = {
        @BMRule(name = "refresh waits for the drop before it caches the dictionary it read",
                targetClass = "org.apache.cassandra.db.compression.CompressionDictionaryScheduler",
                targetMethod = "refreshDictionaryFromSystemTable",
                targetLocation = "AT INVOKE org.apache.cassandra.db.compression.ICompressionDictionaryCache.add",
                action = "org.apache.cassandra.db.compression.CompressionDictionaryDropTest.beforeAdd($0.tableName)"),
        @BMRule(name = "refresh cached the dictionary it read",
                targetClass = "org.apache.cassandra.db.compression.CompressionDictionaryScheduler",
                targetMethod = "refreshDictionaryFromSystemTable",
                targetLocation = "AFTER INVOKE org.apache.cassandra.db.compression.ICompressionDictionaryCache.add",
                action = "org.apache.cassandra.db.compression.CompressionDictionaryDropTest.afterAdd($0.tableName)")
    })
    public void refreshDuringDrop()
    {
        CompressionDictionary dictionary = CompressionDictionaryHelper.INSTANCE.trainDictionary(KEYSPACE, "trained");
        String table = createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        CompressionDictionaryManager manager = getCurrentColumnFamilyStore().compressionDictionaryManager();
        // stores the dictionary, so that the next refresh reads it
        manager.handleNewDictionary(dictionary);
        pausedTable = table;

        assertThat(Uninterruptibles.awaitUninterruptibly(read, 1, TimeUnit.MINUTES)).as("refresh read the dictionary").isTrue();
        dropTable("DROP TABLE %s");
        assertThat(manager.get(dictionary.dictId())).as("cached right after the drop").isNull();
        dropped.countDown();

        assertThat(Uninterruptibles.awaitUninterruptibly(added, 1, TimeUnit.MINUTES)).as("refresh added the dictionary").isTrue();
        assertThat(manager.get(dictionary.dictId())).as("cached after the refresh which read it before the drop").isNull();
    }

    public static void beforeAdd(String table)
    {
        if (!table.equals(pausedTable) || read.getCount() == 0)
            return;

        read.countDown();
        Uninterruptibles.awaitUninterruptibly(dropped);
    }

    public static void afterAdd(String table)
    {
        if (table.equals(pausedTable) && dropped.getCount() == 0)
            added.countDown();
    }
}
