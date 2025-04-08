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

package org.apache.cassandra.fuzz.harry.sstable;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.stress.HarryStress;
import org.apache.cassandra.harry.stress.LevelledSStableGenerator;
import org.apache.cassandra.harry.stress.RotationStrategy;
import org.apache.cassandra.harry.stress.config.StressSchemaConfig;
import org.apache.cassandra.harry.stress.TokenIndex;
import org.apache.cassandra.harry.stress.TokenIndexGenerator;
import org.apache.cassandra.harry.stress.VisitGenerator;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.harry.stress.distribution.Distributions;
import org.apache.cassandra.io.util.File;

public class RangeAwareSSTableLoadAndStressTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(RangeAwareSSTableLoadAndStressTest.class);

    private static final String SCHEMA_CONFIG = "test/resources/harry/stress/levelled-sstable-schema.yaml";

    private static final int NUM_NODES = 4;
    private static final int RF = 3;

    private static final long INITIAL_LTS = 1;
    private static final long VISITS = 100_000;
    private static final long END_LTS = INITIAL_LTS + VISITS;
    private static final long STRESS_VISITS = 16_000;
    private static final int CONCURRENCY = 2;
    private static final int RATE_PER_SECOND = 20_000;
    private static final int SSTABLE_SIZE_MIB = 1;
    private static final int[] LEVEL_WEIGHTS = { 1, 2, 4, 8, 16 };

    @Test
    public void rangeAwareLoadAndStress() throws Throwable
    {
        LevelledSSTableGeneratorTest.initForOfflineTool();

        StressSchemaConfig config = StressSchemaConfig.load(Paths.get(SCHEMA_CONFIG));
        SchemaSpec schema = config.schema();
        RotationStrategy rotation = config.rotationStrategy();

        Distribution visitSize = Distributions.fixed(1);
        VisitGenerator.OpKindGenFactory opKindGen = new VisitGenerator.RandomOpKindGenFactory();
        Generator<VisitGenerator.VisitType> visitTypeGen = Generators.constant(VisitGenerator.VisitType.MUTATE);

        try (Cluster cluster = init(Cluster.build(NUM_NODES)
                                           .withConfig(c -> c.set("num_tokens", 1))
                                           .start(), RF))
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + schema.keyspace +
                                 " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter0': " + RF + "}");
            cluster.schemaChange(schema.compile());
            cluster.forEach(n -> n.nodetool("disableautocompaction", schema.keyspace, schema.table));

            List<RangeReplicas> ranges = getTopology(cluster, schema);
            for (RangeReplicas r : ranges)
                System.out.println(String.format("  range (%d, %d] -> nodes %s", r.startToken, r.endToken, java.util.Arrays.toString(r.replicas)));
            logger.info("data_placements returned {} ranges", ranges.size());

            File tokenDir = new File(Files.createTempDirectory("harry-token-index"));
            TokenIndexGenerator.generate(tokenDir.toJavaIOFile(), schema, rotation,
                                         config.rowPopulation(), visitTypeGen,
                                         visitSize, INITIAL_LTS, VISITS);
            TokenIndex tokenIndex = new TokenIndex(new File(tokenDir, "merged_tokens"),
                                                   new File(tokenDir, "merged_tokens.idx"));

            // Ror each range, generate levelled SSTables and import into replicas
            for (RangeReplicas range : ranges)
            {
                File sstableDir = new File(Files.createDirectories(Files.createTempDirectory("range-aware-sstables")
                                                                        .resolve(String.format("%d-%d", range.startToken, range.endToken))
                                                                        .resolve(schema.keyspace)
                                                                        .resolve(schema.table + '-' + "0".repeat(32))));
                LevelledSStableGenerator generator = new LevelledSStableGenerator(schema, config.rowPopulation(), config.columnPopulation(), visitSize, opKindGen,
                                                                                  false, SSTABLE_SIZE_MIB,
                                                                                  new LevelledSStableGenerator.SSTableLevelPicker(LEVEL_WEIGHTS),
                                                                                  tokenIndex, sstableDir);
                // Handle wraparound ranges
                if (range.startToken <= range.endToken)
                {
                    generator.generate(range.startToken, range.endToken);
                }
                else
                {
                    generator.generate(Long.MIN_VALUE, range.endToken);
                    generator.generate(range.startToken, Long.MAX_VALUE);
                }

                for (int node : range.replicas)
                    cluster.get(node).nodetoolResult("import", "-cd", "-t", schema.keyspace, schema.table, sstableDir.absolutePath())
                                     .asserts().success();
            }
            logger.info("Generated and imported SSTables for all ranges");
            tokenIndex.close();

            // Continue HarryStress live from where SSTableGenerator has finished
            HarryStress stress = new HarryStress(schema,
                                                 config.rowPopulation(),
                                                 config.columnPopulation(),
                                                 visitTypeGen,
                                                 visitSize,
                                                 opKindGen,
                                                 config.rotationStrategy(),
                                                 /*metricsOut=*/ null,
                                                 /*reportIntervalSeconds=*/ 30,
                                                 () -> (statement, run) -> {
                                                     Object[][] rs = cluster.coordinator(1).execute(statement.cql(),
                                                                                                    ConsistencyLevel.QUORUM,
                                                                                                    statement.bindings());
                                                     System.out.println("rs = " + Arrays.toString(rs));
                                                     if (run != null)
                                                         run.run();
                                                     return rs;
                                                 },
                                                 CONCURRENCY,
                                                 RATE_PER_SECOND,
                                                 /*minPartitionIdx=*/ 0,
                                                 /*maxPartitionIdx=*/ Long.MAX_VALUE,
                                                 /*initialLts=*/ END_LTS);

            stress.replay(INITIAL_LTS, END_LTS);
            stress.start(END_LTS + STRESS_VISITS, Long.MAX_VALUE);
            logger.info("HarryStress completed an additional {} visits past the loaded SSTable history", STRESS_VISITS);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<RangeReplicas> getTopology(Cluster cluster, SchemaSpec schema)
    {
        Object[][] rows = cluster.coordinator(1).execute(
            "SELECT range_start, range_end, write_replicas FROM system_views.data_placements " +
            "WHERE keyspace_name = ? AND table_name = ?",
            ConsistencyLevel.ONE, schema.keyspace, schema.table);
        List<RangeReplicas> result = new ArrayList<>();

        for (Object[] row : rows)
        {
            long startToken = Long.parseLong((String) row[0]);
            long endToken = Long.parseLong((String) row[1]);
            int[] replicas = toIdx((Set<Integer>) row[2]);
            result.add(new RangeReplicas(startToken, endToken, replicas));
        }
        return result;
    }

    private static int[] toIdx(Set<Integer> nodeIds)
    {
        // In dtest clusters, TCM node IDs are assigned 1..N matching the instance indices
        int[] idxs = new int[nodeIds.size()];
        int i = 0;
        for (int nodeId : nodeIds)
            idxs[i++] = nodeId;
        return idxs;
    }

    // A token range and its write-replica set
    private static final class RangeReplicas
    {
        final long startToken;
        final long endToken;
        final int[] replicas;

        RangeReplicas(long startToken, long endToken, int[] replicas)
        {
            this.startToken = startToken;
            this.endToken = endToken;
            this.replicas = replicas;
        }
    }
}
