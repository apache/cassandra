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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;

import static com.google.common.collect.Iterables.getOnlyElement;

public class AccordDeleteCommandStoreTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDeleteCommandStoreTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder
                                               .withoutVNodes()
                                               .withConfig(config ->
                                                           config
                                                           .set("accord.shard_durability_cycle", "20s")
                                                           .set("accord.topology_sync_propagator_enabled_pre_start", true)
                                                           .with(Feature.NETWORK, Feature.GOSSIP)), 4);
    }

    @Test
    public void deleteCommandStoresTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");
        test(ddls, cluster -> {
            String newToken = cluster.get(1).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));

            int numberOfCommandStores = cluster.get(2).callOnInstance(() -> {
                Util.spinUntilTrue(() -> AccordService.instance().node().commandStores().all().length > 0);
                return AccordService.instance().node().commandStores().all().length;
            });

            cluster.get(2).runOnInstance(() -> {
                StorageService.instance.move(Long.toString(Long.parseLong(newToken) + 1));
                Util.spinUntilTrue(() -> AccordService.instance().node().commandStores().all().length < numberOfCommandStores,
                                   60);
            });
        });
    }
}

