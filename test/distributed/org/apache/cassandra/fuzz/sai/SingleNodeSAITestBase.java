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

package org.apache.cassandra.fuzz.sai;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.collect.Streams;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.fuzz.HarryTestBase;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

// TODO: "WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};",
public abstract class SingleNodeSAITestBase extends HarryTestBase
{
    protected SingleNodeSAITestBase(TransactionalMode transactionalMode)
    {
        super(transactionalMode);
    }

    @Test
    public void simplifiedSaiTest()
    {
        withRandom(rng -> saiTest(rng,
                                  SchemaGenerators.trivialSchema(KEYSPACE, "simplified", 1000).generate(rng),
                                  () -> true,
                                  DEFAULT_REPAIR_SKIP));
    }

    @Test
    public void indexOnlySaiTest()
    {
        for (int i = 0; i < iterations(); i++)
        {
            logger.info("Starting iteration {}...", i);
            withRandom(rng -> saiTest(rng,
                                      schemaGenerator(rng.nextBoolean()).generate(rng),
                                      () -> true,
                                      rng.nextBoolean() ? DEFAULT_REPAIR_SKIP : Integer.MAX_VALUE));
        }
    }

    @Test
    public void mixedFilteringSaiTest()
    {
        for (int i = 0; i < iterations(); i++)
        {
            logger.info("Starting iteration {}...", i);
            withRandom(rng -> saiTest(rng,
                                      schemaGenerator(rng.nextBoolean()).generate(rng),
                                      () -> rng.nextFloat() < 0.7f,
                                      rng.nextBoolean() ? DEFAULT_REPAIR_SKIP : Integer.MAX_VALUE));
        }
    }

    private void saiTest(EntropySource rng, SchemaSpec schema, Supplier<Boolean> createIndex, int repairSkip)
    {
        runHistory(rng, schema, s -> createIndexes(s, createIndex), repairSkip);
    }

    private void createIndexes(SchemaSpec schema, Supplier<Boolean> createIndex)
    {
        AtomicInteger indexCount = new AtomicInteger();

        Streams.concat(schema.clusteringKeys.stream(), schema.regularColumns.stream(), schema.staticColumns.stream())
               .forEach(column -> {
                   if (createIndex.get())
                   {
                       logger.info("Adding index to column {}...", column.name);
                       cluster.schemaChange(String.format("CREATE INDEX %s_sai_idx ON %s.%s (%s) USING 'sai' ",
                                                          column.name, schema.keyspace, schema.table, column.name));
                       indexCount.incrementAndGet();
                   }
                   else
                   {
                       logger.info("Leaving column {} unindexed...", column.name);
                   }
               });

        CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(indexCount.get());
        waitForIndexesQueryable(schema);
    }

    @Override
    protected Generator<SchemaSpec> schemaGenerator(boolean disableReadRepair)
    {
        SchemaSpec.OptionsBuilder builder = SchemaSpec.optionsBuilder().disableReadRepair(disableReadRepair)
                                                                       .compactionStrategy("LeveledCompactionStrategy");
        return SchemaGenerators.schemaSpecGen(KEYSPACE, "basic_sai", MAX_PARTITION_SIZE, builder);
    }

    @Override
    protected boolean isEqualityOnly(ColumnSpec<?> column)
    {
        return IndexTermType.isEqOnlyType(column.type.asServerType());
    }

    @Override
    protected boolean supportsFilteringIntersections()
    {
        return true;
    }

    protected void waitForIndexesQueryable(SchemaSpec schema) {}
}
