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

package org.apache.cassandra.service.pager;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.fuzz.HarryTestBase;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.SchemaGenerators;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

/**
 * Validates paged reads against Harry's model on divergent and synchronized replicas.
 */
@RunWith(Parameterized.class)
public class PagingHarryTest extends HarryTestBase
{
    @Parameterized.Parameter
    public boolean replicatedWrites;

    public PagingHarryTest()
    {
        super(null);
    }

    @Parameterized.Parameters(name = "replicatedWrites={0}")
    public static Object[][] parameters()
    {
        return new Object[][]{ { false }, { true } };
    }

    @BeforeClass
    public static void before() throws Throwable
    {
        init(2);
    }

    @Test
    public void simplifiedPagingTest()
    {
        withRandom(TEST_SEED.isPresent() ? TEST_SEED.getLong() : System.nanoTime(),
                   rng -> runHistory(rng,
                                     SchemaGenerators.trivialSchema(KEYSPACE, "simplified", 1000).generate(rng),
                                     schema -> {},
                                     DEFAULT_REPAIR_SKIP));
    }

    @Test
    public void pagingTest()
    {
        for (int i = 0; i < (TEST_SEED.isPresent() ? 1 : 3); i++)
        {
            logger.info("Starting iteration {}...", i);
            long seed = TEST_SEED.isPresent() ? TEST_SEED.getLong() : System.nanoTime();
            withRandom(seed, rng -> runHistory(rng,
                                               schemaGenerator(rng.nextBoolean()).generate(rng),
                                               schema -> {},
                                               rng.nextBoolean() ? DEFAULT_REPAIR_SKIP : Integer.MAX_VALUE));
        }
    }

    @Override
    protected boolean supportsFilteringIntersections()
    {
        return replicatedWrites;
    }

    @Override
    protected InJvmDTestVisitExecutor.ConsistencyLevelSelector consistencyLevelSelector()
    {
        if (replicatedWrites)
            return visit -> ALL;
        return super.consistencyLevelSelector();
    }
}
