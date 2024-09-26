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

import java.io.IOException;
import java.nio.file.Files;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.RedundantBefore;
import accord.primitives.Timestamp;
import accord.utils.AccordGens;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.RandomSource;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.concurrent.Condition;


public class AccordJournalCompactionTest
{
    @BeforeClass
    public static void setUp() throws Throwable
    {
        ServerTestUtils.daemonInitialization();
        StorageService.instance.registerMBeans();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();
        DatabaseDescriptor.setAccordJournalDirectory(directory.path());
        StorageService.instance.initServer();
        Keyspace.setInitialized();
    }

    @Test
    public void segmentMergeTest() throws IOException, InterruptedException
    {
        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();

        Gen<RedundantBefore> redundantBeforeGen = AccordGenerators.redundantBefore(DatabaseDescriptor.getPartitioner());

        AccordJournal journal = new AccordJournal(new TestParams()
        {
            @Override
            public int segmentSize()
            {
                return 1024 * 1024;
            }

            @Override
            public boolean enableCompaction()
            {
                return false;
            }
        });
        try
        {
            journal.start(null);
            Timestamp timestamp = Timestamp.NONE;
            RandomSource rng = new DefaultRandom();
            RedundantBefore prev = RedundantBefore.EMPTY;
            for (int i = 0; i < 2_000; i++)
            {
                timestamp = timestamp.next();
                RedundantBefore next = redundantBeforeGen.next(rng);
                journal.appendRedundantBefore(1, next, null);
                prev = RedundantBefore.merge(prev, next);
            }

            RedundantBefore next = redundantBeforeGen.next(rng);
            Condition condition = Condition.newOneTimeCondition();
            journal.appendRedundantBefore(1, next, condition::signal);
            prev = RedundantBefore.merge(prev, next);
            condition.await();

            journal.closeCurrentSegmentForTesting();
            journal.runCompactorForTesting();

            Assert.assertEquals(prev, journal.loadRedundantBefore(1));
        }
        finally
        {
            journal.shutdown();
        }
    }


    @Test
    public void mergeKeysTest()
    {
        AccordJournal accordJournal = new AccordJournal(TestParams.INSTANCE);
        try
        {
            accordJournal.start(null);
            Gen<Timestamp> timestampGen = AccordGens.timestamps();
            // TODO: we might benefit from some unification of generators
            Gen<RedundantBefore> redundantBeforeGen = AccordGenerators.redundantBefore(DatabaseDescriptor.getPartitioner());
            RandomSource rng = new DefaultRandom();
            // Probably all redundant befores will be written with the same timestamp?
            timestampGen.next(rng);
            RedundantBefore expected = RedundantBefore.EMPTY;
            for (int i = 0; i < 10; i++)
            {
                RedundantBefore redundantBefore = redundantBeforeGen.next(rng);
                expected = RedundantBefore.merge(expected, redundantBefore);
                accordJournal.appendRedundantBefore(1, redundantBefore, () -> {});
            }

            RedundantBefore actual = accordJournal.loadRedundantBefore(1);
            Assert.assertEquals(expected, actual);
        }
        finally
        {
            accordJournal.shutdown();
        }
    }
}
