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

package org.apache.cassandra.service.accord;

import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.burn.BurnTestBase;
import accord.burn.SimulationException;
import accord.impl.TopologyFactory;
import accord.impl.basic.Cluster;
import accord.impl.basic.RandomDelayQueue;
import accord.local.Node;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.tools.FieldUtil;

import static accord.impl.PrefixedIntHashKey.ranges;


public class AccordJournalBurnTest extends BurnTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournalBurnTest.class);

    public static void setUp() throws Throwable
    {
        StorageService.instance.registerMBeans();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();

        Keyspace.setInitialized();
        FieldUtil.transferFields(new KeySerializers.Impl(BurnTestKeySerializers.key,
                                                         BurnTestKeySerializers.routingKey,
                                                         BurnTestKeySerializers.range),
                                 KeySerializers.class);

        FieldUtil.transferFields(new CommandSerializers.QuerySerializers(BurnTestKeySerializers.read,
                                                                         BurnTestKeySerializers.query,
                                                                         BurnTestKeySerializers.update,
                                                                         BurnTestKeySerializers.write),
                                 CommandSerializers.class);
        FieldUtil.transferFields(new DepsSerializers.Impl(BurnTestKeySerializers.range),
                                 DepsSerializers.class);
        FieldUtil.setInstanceUnsafe(ResultSerializers.class,
                                    BurnTestKeySerializers.result,
                                    "result");
    }

    private AtomicInteger counter = new AtomicInteger();
    @Before
    public void beforeTest() throws Throwable
    {

    }

    @Test
    public void testOne()
    {
        long seed = System.nanoTime();
        int operations = 1000;

        logger.info("Seed: {}", seed);
        Cluster.trace.trace("Seed: {}", seed);
        RandomSource random = new DefaultRandom(seed);
        try
        {
            List<Node.Id> clients = generateIds(true, 1 + random.nextInt(4));
            int rf;
            float chance = random.nextFloat();
            if (chance < 0.2f)      { rf = random.nextInt(2, 9); }
            else if (chance < 0.4f) { rf = 3; }
            else if (chance < 0.7f) { rf = 5; }
            else if (chance < 0.8f) { rf = 7; }
            else                    { rf = 9; }

            List<Node.Id> nodes = generateIds(false, random.nextInt(rf, rf * 3));

            {
                ServerTestUtils.daemonInitialization();

                TableMetadata[] metadatas = new TableMetadata[4 + nodes.size()];
                metadatas[0] = AccordKeyspace.Commands;
                metadatas[1] = AccordKeyspace.CommandsForKeys;
                metadatas[2] = AccordKeyspace.Topologies;
                metadatas[3] = AccordKeyspace.EpochMetadata;
                for (int i = 0; i < nodes.size(); i++)
                    metadatas[4 + i] = AccordKeyspace.journalMetadata("journal_" + nodes.get(i));

                AccordKeyspace.TABLES = Tables.of(metadatas);
                setUp();
            }
            Keyspace ks = Schema.instance.getKeyspaceInstance("system_accord");

            burn(random, new TopologyFactory(rf, ranges(0, HASH_RANGE_START, HASH_RANGE_END, random.nextInt(Math.max(nodes.size() + 1, rf), nodes.size() * 3))),
                 clients,
                 nodes,
                 5 + random.nextInt(15),
                 5 + random.nextInt(15),
                 operations,
                 10 + random.nextInt(30),
                 new RandomDelayQueue.Factory(random).get(),
                 (node) -> {
                     try
                     {
                         File directory = new File(Files.createTempDirectory(Integer.toString(counter.incrementAndGet())));
                         directory.deleteRecursiveOnExit();
                         ColumnFamilyStore cfs = ks.getColumnFamilyStore("journal_" + node);
                         cfs.disableAutoCompaction();
                         AccordJournal journal = new AccordJournal(new TestParams()
                         {
                             @Override
                             public int segmentSize()
                             {
                                 return 32 * 1024 * 1024;
                             }

                             @Override
                             public boolean enableCompaction()
                             {
                                 return false;
                             }
                         }, new AccordAgent(), directory, cfs);

                         journal.start(null);
                         journal.unsafeSetStarted();
                         return journal;
                     }
                     catch (Throwable t)
                     {
                         throw new RuntimeException(t);
                     }
                 }
            );
        }
        catch (Throwable t)
        {
            logger.error("Exception running burn test for seed {}:", seed, t);
            throw SimulationException.wrap(seed, t);
        }
    }


}
