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

package org.apache.cassandra.harry.stress;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.stress.config.StressSchemaConfig;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.harry.stress.distribution.Distributions;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.ClusterMetadataService;

/**
 * Offline range-aware SSTable generator for a live (e.g. CCM) cluster.
 *
 * <p>Discovers token ranges and their replicas from {@code system_views.data_placements} +
 * {@code system_views.cluster_metadata_directory}, then generates one set of Harry-deterministic
 * SSTables per range into a separate directory. Each output directory follows the layout the
 * {@code nodetool import} parser expects:
 *
 * <pre>{@code
 *   <output-dir>/range_<start>_<end>/<keyspace>/<table>-<32-hex-id>/*.db
 * }</pre>
 *
 * <p>The tool does NOT push the SSTables onto the cluster — it only generates them locally and
 * prints a per-node import plan listing which directory should be loaded on which node. The user
 * is responsible for copying the directories to each node and running {@code nodetool import}.
 *
 * <p>This is the offline / CLI sibling of {@code RangeAwareSSTableLoadAndStressTest}; the
 * single-node analogue is {@code LevelledSSTableGeneratorTest}.
 */
public class RangeAwareSSTableGenerator
{
    public static void main(String... args) throws IOException
    {
        Args parsed;
        try
        {
            parsed = Args.parse(args);
        }
        catch (Args.HelpRequested e)
        {
            return;
        }

        System.out.println("Parsed args: " + parsed);

        // Init order matters: tool mode must be enabled before any Schema.* access (else
        // local-system keyspaces are not loaded and HarrySSTableWriter.build() will fail).
        // Murmur3 must match the cluster (Murmur3 is Cassandra's default).
        DatabaseDescriptor.toolInitialization(false);
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.initializeForClients();

        StressSchemaConfig config = StressSchemaConfig.load(Paths.get(parsed.schemaPath));
        SchemaSpec schema = config.schema();

        Cluster.Builder clusterBuilder = Cluster.builder()
                                                .addContactPoints(parsed.contactPoints)
                                                .withPort(parsed.port)
                                                .withQueryOptions(new QueryOptions().setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        if (parsed.username != null)
            clusterBuilder.withCredentials(parsed.username, parsed.password == null ? "" : parsed.password);

        try (Cluster cluster = clusterBuilder.build();
             Session session = cluster.connect())
        {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + schema.keyspace +
                            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute(schema.compile());

            Map<Integer, String> nodeIdToEndpoint = readNodeDirectory(session);
            List<RangeReplicas> ranges = readPlacements(session, schema.keyspace, schema.table, nodeIdToEndpoint);
            if (ranges.isEmpty())
                throw new IllegalStateException("No placement rows found for " + schema.keyspace + "." + schema.table +
                                                ". Make sure the keyspace+table already exist on the cluster.");

            System.out.println("Discovered " + ranges.size() + " ranges for " + schema.keyspace + "." + schema.table + ":");
            for (RangeReplicas r : ranges)
                System.out.println(String.format("  (%d, %d] -> %s", r.startToken, r.endToken, r.endpoints));
            System.out.println();

            // 2. Build the global token index (same parameters as RangeAwareSSTableLoadAndStressTest).
            //    A single index is used for all ranges; per-range generation just slices it by token.
            Distribution visitSize = Distributions.fixed(1);
            VisitGenerator.OpKindGenFactory opKindGen = new VisitGenerator.RandomOpKindGenFactory();
            Generator<VisitGenerator.VisitType> visitTypeGen = Generators.constant(VisitGenerator.VisitType.MUTATE);

            Path outputRoot = Paths.get(parsed.outputDir).toAbsolutePath();
            Files.createDirectories(outputRoot);
            File tokenDir = new File(Files.createDirectories(outputRoot.resolve("_tokens")));
            System.out.println("Generating token index in " + tokenDir + " (initialLts=" + parsed.initialLts +
                               ", visits=" + parsed.visits + ")");
            TokenIndexGenerator.generate(tokenDir.toJavaIOFile(), schema, config.rotationStrategy(),
                                         config.rowPopulation(), visitTypeGen,
                                         visitSize, parsed.initialLts, parsed.visits);

            // 3. For each range, generate a set of levelled SSTables under its own directory.
            Map<String, List<String>> importPlan = new LinkedHashMap<>();
            try (TokenIndex tokenIndex = new TokenIndex(new File(tokenDir, "merged_tokens"),
                                                       new File(tokenDir, "merged_tokens.idx")))
            {
                for (RangeReplicas range : ranges)
                {
                    // Directory layout the nodetool importer expects: <ks>/<table>-<32hex>/. The 32-hex id
                    // is a placeholder — the importer rehomes the SSTables to the live table on load.
                    String rangeDirName = "range_" + range.startToken + "_" + range.endToken;
                    File sstableDir = new File(Files.createDirectories(outputRoot.resolve(rangeDirName)
                                                                                  .resolve(schema.keyspace)
                                                                                  .resolve(schema.table + '-' + "0".repeat(32))));

                    System.out.println();
                    System.out.println("Range (" + range.startToken + ", " + range.endToken + "]");
                    System.out.println("  Endpoints: " + range.endpoints);
                    System.out.println("  Output:    " + sstableDir.absolutePath());

                    LevelledSStableGenerator generator = new LevelledSStableGenerator(schema, config.rowPopulation(), config.columnPopulation(),
                                                                                      visitSize, opKindGen, parsed.disableCompression, parsed.sstableSizeMiB,
                                                                                      new LevelledSStableGenerator.SSTableLevelPicker(parsed.levelWeights),
                                                                                      tokenIndex, sstableDir);
                    // Wraparound: a range whose start > end straddles MIN/MAX. Generate it as two slices.
                    if (range.startToken <= range.endToken)
                    {
                        generator.generate(range.startToken, range.endToken);
                    }
                    else
                    {
                        generator.generate(Long.MIN_VALUE, range.endToken);
                        generator.generate(range.startToken, Long.MAX_VALUE);
                    }

                    for (String endpoint : range.endpoints)
                        importPlan.computeIfAbsent(endpoint, k -> new ArrayList<>()).add(sstableDir.absolutePath());
                }
            }

            // 4. Print per-node import plan.
            System.out.println();
            System.out.println("=================== IMPORT PLAN ===================");
            System.out.println("For each node below, copy the listed directories to the node and run:");
            System.out.println("    nodetool import -l -e " + schema.keyspace + ' ' + schema.table + " <dir>");
            System.out.println("(-l keeps generated LCS levels; -e runs extended verification.)");
            System.out.println();
            for (Map.Entry<String, List<String>> e : importPlan.entrySet())
            {
                System.out.println("Node " + e.getKey() + ':');
                for (String dir : e.getValue())
                    System.out.println("  " + dir);
            }
        }
    }

    private static Map<Integer, String> readNodeDirectory(Session session)
    {
        // Map TCM node_id -> "<native_address>:<native_port>" (the address the user uses as a contact point).
        Map<Integer, String> result = new HashMap<>();
        for (Row r : session.execute("SELECT node_id, native_address, native_port FROM system_views.cluster_metadata_directory").all())
        {
            int nodeId = r.getInt("node_id");
            String addr = r.getInet("native_address").getHostAddress();
            int port = r.getInt("native_port");
            result.put(nodeId, addr + ':' + port);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<RangeReplicas> readPlacements(Session session, String keyspace, String table,
                                                      Map<Integer, String> nodeIdToEndpoint)
    {
        // 2-component partition-key prefix returns every range for the table. write_replicas is the
        // set of RF TCM NodeId.id() values for each range.
        List<Row> rows = session.execute(
            "SELECT range_start, range_end, write_replicas FROM system_views.data_placements " +
            "WHERE keyspace_name = ? AND table_name = ?",
            keyspace, table).all();
        List<RangeReplicas> result = new ArrayList<>();
        for (Row row : rows)
        {
            long startToken = Long.parseLong(row.getString("range_start"));
            long endToken = Long.parseLong(row.getString("range_end"));
            Set<Integer> nodeIds = row.getSet("write_replicas", Integer.class);
            Set<String> endpoints = new TreeSet<>();
            for (Integer id : nodeIds)
            {
                String endpoint = nodeIdToEndpoint.get(id);
                if (endpoint == null)
                    throw new IllegalStateException("No directory entry for node_id=" + id);
                endpoints.add(endpoint);
            }
            result.add(new RangeReplicas(startToken, endToken, endpoints));
        }
        return result;
    }

    private static final class RangeReplicas
    {
        final long startToken;
        final long endToken;
        final Set<String> endpoints;

        RangeReplicas(long startToken, long endToken, Set<String> endpoints)
        {
            this.startToken = startToken;
            this.endToken = endToken;
            this.endpoints = endpoints;
        }
    }

    private static final class Args
    {
        final String[] contactPoints;
        final int port;
        final String username;
        final String password;
        final String schemaPath;
        final String outputDir;
        final long initialLts;
        final long visits;
        final int sstableSizeMiB;
        final int[] levelWeights;
        final boolean disableCompression;

        Args(String[] contactPoints, int port, String username, String password,
             String schemaPath, String outputDir,
             long initialLts, long visits, int sstableSizeMiB, int[] levelWeights, boolean disableCompression)
        {
            this.contactPoints = contactPoints;
            this.port = port;
            this.username = username;
            this.password = password;
            this.schemaPath = schemaPath;
            this.outputDir = outputDir;
            this.initialLts = initialLts;
            this.visits = visits;
            this.sstableSizeMiB = sstableSizeMiB;
            this.levelWeights = levelWeights;
            this.disableCompression = disableCompression;
        }

        static Args parse(String[] args)
        {
            String[] contactPoints = null;
            int port = 9042;
            String username = null;
            String password = null;
            String schemaPath = null;
            String outputDir = null;
            long initialLts = 1;
            long visits = 100_000;
            int sstableSizeMiB = 64;
            int[] levelWeights = { 1, 2, 4, 8, 16 };
            boolean disableCompression = true;

            for (int i = 0; i < args.length; i++)
            {
                String a = args[i];
                switch (a)
                {
                    case "-h":
                    case "--help":
                        printHelp();
                        throw new HelpRequested();
                    case "--contact-points":
                        contactPoints = require(args, ++i, a).split(",");
                        break;
                    case "--port":
                        port = Integer.parseInt(require(args, ++i, a));
                        break;
                    case "--username":
                        username = require(args, ++i, a);
                        break;
                    case "--password":
                        password = require(args, ++i, a);
                        break;
                    case "--schema":
                        schemaPath = require(args, ++i, a);
                        break;
                    case "--output-dir":
                        outputDir = require(args, ++i, a);
                        break;
                    case "--initial-lts":
                        initialLts = Long.parseLong(require(args, ++i, a));
                        break;
                    case "--visits":
                        visits = Long.parseLong(require(args, ++i, a));
                        break;
                    case "--sstable-size-mib":
                        sstableSizeMiB = Integer.parseInt(require(args, ++i, a));
                        break;
                    case "--levels":
                    {
                        String[] parts = require(args, ++i, a).split(",");
                        levelWeights = new int[parts.length];
                        for (int j = 0; j < parts.length; j++)
                            levelWeights[j] = Integer.parseInt(parts[j].trim());
                        break;
                    }
                    case "--disable-compression":
                        disableCompression = true;
                        break;
                    default:
                        printHelp();
                        throw new IllegalArgumentException("Unknown argument: " + a);
                }
            }

            if (contactPoints == null || schemaPath == null || outputDir == null)
            {
                printHelp();
                throw new IllegalArgumentException("--contact-points, --schema, and --output-dir are required");
            }
            return new Args(contactPoints, port, username, password, schemaPath, outputDir,
                            initialLts, visits, sstableSizeMiB, levelWeights, disableCompression);
        }

        private static String require(String[] args, int idx, String flag)
        {
            if (idx >= args.length)
                throw new IllegalArgumentException("Missing value for " + flag);
            return args[idx];
        }

        private static void printHelp()
        {
            System.out.println("Usage: RangeAwareSSTableGenerator [options]");
            System.out.println();
            System.out.println("Required:");
            System.out.println("  --contact-points host1,host2,...   Comma-separated CCM contact points");
            System.out.println("  --schema <path>                    YAML schema config (keyspace, table, columnspec, rotation; same");
            System.out.println("                                       format StressSchemaConfig consumes). The keyspace+table MUST");
            System.out.println("                                       already exist on the cluster and match this definition.");
            System.out.println("  --output-dir <dir>                 Directory to write per-range SSTable directories into");
            System.out.println();
            System.out.println("Optional:");
            System.out.println("  --port <p>                         Native protocol port (default 9042)");
            System.out.println("  --username <u>                     Cassandra username");
            System.out.println("  --password <p>                     Cassandra password");
            System.out.println("  --initial-lts <n>                  First LTS to generate from (default 1)");
            System.out.println("  --visits <n>                       Total writes/visits to generate (default 100000)");
            System.out.println("  --sstable-size-mib <n>             Max SSTable size in MiB before rolling (default 1)");
            System.out.println("  --levels <w0,w1,...>               LCS level weights (default 1,2,4,8,16; index = level)");
            System.out.println("  --disable-compression              Disable SSTable compression");
            System.out.println();
            System.out.println("Output layout (one subdirectory per token range):");
            System.out.println("  <output-dir>/range_<start>_<end>/<keyspace>/<table>-<32 zeros>/*.db");
            System.out.println();
            System.out.println("After it finishes, the tool prints which range directories should be imported into which");
            System.out.println("node. Copy each directory to its target node and run:");
            System.out.println("  nodetool import -l -e <keyspace> <table> <range-dir-on-node>");
        }

        static final class HelpRequested extends RuntimeException
        {
            HelpRequested() { super(); }
        }

        @Override
        public String toString()
        {
            return "Args{" +
                   "contactPoints=" + Arrays.toString(contactPoints) +
                   ", port=" + port +
                   ", username='" + username + '\'' +
                   ", password='" + password + '\'' +
                   ", schemaPath='" + schemaPath + '\'' +
                   ", outputDir='" + outputDir + '\'' +
                   ", initialLts=" + initialLts +
                   ", visits=" + visits +
                   ", sstableSizeMiB=" + sstableSizeMiB +
                   ", levelWeights=" + Arrays.toString(levelWeights) +
                   ", disableCompression=" + disableCompression +
                   '}';
        }
    }
}
