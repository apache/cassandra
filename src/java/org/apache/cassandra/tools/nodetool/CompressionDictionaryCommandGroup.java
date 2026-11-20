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
package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.db.compression.CompressionDictionaryScope;
import org.apache.cassandra.db.compression.CompressionDictionaryTrainingConfig;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.db.compression.TrainingState;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static java.lang.System.lineSeparator;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.joining;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_SCOPE_PARAMETER_NAME;

@Command(name = "compressiondictionary",
         description = "Manage compression dictionaries",
         subcommands = { CompressionDictionaryCommandGroup.TrainDictionary.class,
                         CompressionDictionaryCommandGroup.ListDictionaries.class,
                         CompressionDictionaryCommandGroup.ExportDictionary.class,
                         CompressionDictionaryCommandGroup.ImportDictionary.class,
                         CompressionDictionaryCommandGroup.CleanupDictionaries.class,
                         CompressionDictionaryCommandGroup.PromoteDictionary.class,
                         CompressionDictionaryCommandGroup.DiscardDictionary.class})
public class CompressionDictionaryCommandGroup
{
    @Command(name = "train",
             description = "Manually trigger compression dictionary training for a table. If no SSTables are available, the memtable will be flushed first.")
    public static class TrainDictionary extends AbstractCommand
    {
        private static final String MAX_DICT_SIZE_PARAM_NAME = "--max-dict-size";
        private static final String MAX_TOTAL_SAMPLE_SIZE_PARAM_NAME = "--max-total-sample-size";

        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Option(names = { "-f", "--force" }, description = "Force the dictionary training even if there are not enough samples")
        private boolean force = false;

        @Option(names = "--scope", paramLabel = "scope",
                description = "Scope of the dictionary, the nodes which compress with it: " +
                              "node (this node only), rack (the rack of this node), dc (the datacenter " +
                              "of this node), until the dictionary is promoted, or cluster (all nodes). " +
                              "The default is cluster.")
        private String scope = CompressionDictionaryScope.Kind.CLUSTER.displayName();

        @Option(names = MAX_DICT_SIZE_PARAM_NAME, description = "Maximum size of a trained compression dictionary. " +
                                                                "Larger dictionaries may provide better compression but use more memory. When not set, " +
                                                                "the value from compression configuration from CQL for a given table is used. " +
                                                                "The default value is " + DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE + '.')
        private String trainingMaxDictionarySize;

        @Option(names = MAX_TOTAL_SAMPLE_SIZE_PARAM_NAME, description = "Maximum total size of sample data to collect for dictionary training. " +
                                                                        "More sample data generally produces better dictionaries but takes longer to train. " +
                                                                        "The recommended sample size is 100x the dictionary size. When not set, " +
                                                                        "the value from compression configuration from CQL for a give table is used. " +
                                                                        "The default value is " + DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE + '.')
        private String trainingMaxTotalSampleSize;

        @Override
        public void execute(NodeProbe probe)
        {
            PrintStream out = probe.output().out;
            PrintStream err = probe.output().err;

            validateParameters(err, trainingMaxDictionarySize, trainingMaxTotalSampleSize);
            validateScope(err, scope);

            try
            {
                out.printf("Starting compression dictionary training for %s.%s...%n", keyspace, table);
                out.printf("Training from existing SSTables (flushing first if needed)%n");

                Map<String, String> parameters = new HashMap<>();
                if (trainingMaxDictionarySize != null)
                    parameters.put(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, trainingMaxDictionarySize);

                if (trainingMaxTotalSampleSize != null)
                    parameters.put(TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, trainingMaxTotalSampleSize);

                parameters.put(TRAINING_SCOPE_PARAMETER_NAME, scope);

                probe.trainCompressionDictionary(keyspace, table, force, parameters);

                // Wait for training completion (10 minutes timeout for SSTable-based training)
                out.println("Sampling from existing SSTables and training.");
                long maxWaitMillis = TimeUnit.MINUTES.toMillis(10);
                long startTime = Clock.Global.currentTimeMillis();

                while (Clock.Global.currentTimeMillis() - startTime < maxWaitMillis)
                {
                    TrainingState trainingState = probe.getCompressionDictionaryTrainingState(keyspace, table);
                    TrainingStatus status = trainingState.getStatus();
                    displayProgress(trainingState, startTime, out, status);
                    if (TrainingStatus.COMPLETED == status)
                    {
                        out.printf("%nTraining completed successfully for %s.%s%n", keyspace, table);
                        return;
                    }
                    else if (TrainingStatus.FAILED == status)
                    {
                        err.printf("%nTraining failed for %s.%s%n", keyspace, table);
                        try
                        {
                            String failureMessage = trainingState.getFailureMessage();
                            if (failureMessage != null && !failureMessage.isEmpty())
                            {
                                err.printf("Reason: %s%n", failureMessage);
                            }
                        }
                        catch (Exception e)
                        {
                            // If we can't get the failure message, just continue without it
                        }
                        System.exit(1);
                    }

                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                }

                err.printf("%nTraining did not complete within expected timeframe (10 minutes).%n");
                System.exit(1);
            }
            catch (Exception e)
            {
                err.printf("Failed to trigger training: %s%n", e.getMessage());
                System.exit(1);
            }
        }

        private static void displayProgress(TrainingState trainingState, long startTime, PrintStream out, TrainingStatus status)
        {
            // Display meaningful statistics
            long sampleCount = trainingState.getSampleCount();
            long totalSampleSize = trainingState.getTotalSampleSize();
            long elapsedSeconds = (Clock.Global.currentTimeMillis() - startTime) / 1000;
            double sampleSizeMB = totalSampleSize / (1024.0 * 1024.0);

            out.printf("\rStatus: %s | Samples: %d | Size: %.2f MiB | Elapsed: %ds",
                       status, sampleCount, sampleSizeMB, elapsedSeconds);
        }

        private static void validateParameters(PrintStream err, String trainingMaxDictionarySize, String trainingMaxTotalSampleSize)
        {
            if (trainingMaxDictionarySize != null)
            {
                try
                {
                    CompressionDictionaryTrainingConfig.getMaxDictionarySize(Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, trainingMaxDictionarySize));
                }
                catch (Throwable t)
                {
                    err.println("Invalid value for " + MAX_DICT_SIZE_PARAM_NAME + ": " + t.getMessage());
                    System.exit(1);
                }
            }

            if (trainingMaxTotalSampleSize != null)
            {
                try
                {
                    CompressionDictionaryTrainingConfig.getMaxTotalSampleSize(Map.of(TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, trainingMaxTotalSampleSize));
                }
                catch (Throwable t)
                {
                    err.println("Invalid value for " + MAX_TOTAL_SAMPLE_SIZE_PARAM_NAME + ": " + t.getMessage());
                    System.exit(1);
                }
            }
        }
    }

    @Command(name = "list",
             description = "List available dictionaries of specific keyspace and table.")
    public static class ListDictionaries extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Override
        protected void execute(NodeProbe probe)
        {
            ListingHelper.list(probe,
                               () ->
                               {
                                   try
                                   {
                                       return probe.listCompressionDictionaries(keyspace, table);
                                   }
                                   catch (Throwable t)
                                   {
                                       throw new RuntimeException(t);
                                   }
                               },
                               List.of(CompressionDictionaryDetailsTabularData.TABULAR_DATA_TYPE_TABLE_ID_INDEX,
                                       CompressionDictionaryDetailsTabularData.TABULAR_DATA_TYPE_RAW_DICTIONARY_INDEX));
        }
    }

    @Command(name = "promote",
    description = "Promote the latest dictionary for given keyspace and table, trained or imported with --scope " +
                  "node, rack or dc, to a wider scope: add a datacenter or racks of it to its scope, " +
                  "or promote it to the whole cluster.")
    public static class PromoteDictionary extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Option(names = "--dc", paramLabel = "dc",
                description = "Add this datacenter, or the racks given by --rack of it, to the scope of the dictionary. " +
                              "Without it, the dictionary is promoted to the whole cluster.")
        private String datacenter;

        @Option(names = "--rack", paramLabel = "rack",
                description = "Add this rack of the datacenter given by --dc to the scope of the dictionary, " +
                              "can be repeated. Without it, the whole datacenter is added.")
        private List<String> racks = new ArrayList<>();

        @Override
        protected void execute(NodeProbe probe)
        {
            if (datacenter == null && !racks.isEmpty())
            {
                probe.output().err.printf("Failed to promote dictionary: --rack requires --dc%n");
                System.exit(1);
            }

            try
            {
                probe.promoteDictionary(keyspace, table, datacenter, racks.toArray(new String[0]));
            }
            catch (Throwable e)
            {
                probe.output().err.printf("Failed to promote dictionary: %s%n", e.getMessage());
                System.exit(1);
            }
        }
    }

    @Command(name = "discard",
    description = "Discard the latest dictionary for given keyspace and table, scoped narrower than the cluster, " +
                  "the nodes of its scope compress with the latest dictionary whose scope contains them again.")
    public static class DiscardDictionary extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Override
        protected void execute(NodeProbe probe)
        {
            try
            {
                probe.discardDictionary(keyspace, table);
            }
            catch (Throwable e)
            {
                probe.output().err.printf("Failed to discard dictionary: %s%n", e.getMessage());
                System.exit(1);
            }
        }
    }

    @Command(name = "cleanup", description = "Clean up orphaned dictionaries by deleting them from " + SystemDistributedKeyspace.NAME
                                             + '.' + SystemDistributedKeyspace.COMPRESSION_DICTIONARIES +
                                             " table, these are ones for which a table they were trained for was dropped, " +
                                             "or whose scope has no joined node, or which are scoped narrower than the " +
                                             "cluster while a newer dictionary is used by all nodes.")
    public static class CleanupDictionaries extends AbstractCommand
    {
        @Option(names = {"-d", "--dry"}, description = "Only display orphaned dictionaries, do not remove them.")
        private boolean dry;

        @Override
        protected void execute(NodeProbe probe)
        {
            if (dry)
            {
                ListingHelper.list(probe,
                                   probe::getOrphanedCompressionDictionaries,
                                   List.of(CompressionDictionaryDetailsTabularData.TABULAR_DATA_TYPE_RAW_DICTIONARY_INDEX));
            }
            else
            {
                probe.clearOrphanedCompressionDictionaries();
            }
        }
    }

    @Command(name = "export",
             description = "Export dictionary from Cassandra to local file.")
    public static class ExportDictionary extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Parameters(index = "2", description = "File name to save dictionary to", arity = "1")
        private String dictionaryPath;

        @Option(paramLabel = "dictId", names = { "-i", "--id" },
        description = "The dictionary id. When not specified, the current dictionary is returned.")
        private long dictId = -1;

        @Override
        protected void execute(NodeProbe probe)
        {
            if (dictId <= 0 && dictId != -1)
            {
                probe.output().err.printf("Dictionary id has to be strictly positive number.%n");
                System.exit(1);
            }

            try
            {
                CompositeData compressionDictionary;

                if (dictId == -1)
                {
                    compressionDictionary = probe.getCompressionDictionary(keyspace, table);
                }
                else
                {
                    compressionDictionary = probe.getCompressionDictionary(keyspace, table, dictId);
                }

                if (compressionDictionary == null)
                {
                    probe.output().err.printf("Dictionary%s does not exist for %s.%s.%n",
                                              dictId == -1 ? "" : " with id " + dictId,
                                              keyspace, table);
                    System.exit(1);
                }

                CompressionDictionaryDataObject dataObject = CompressionDictionaryDetailsTabularData.fromCompositeData(compressionDictionary);
                String dictionary = JsonUtils.writeAsPrettyJsonString(dataObject);
                FileUtils.write(new File(dictionaryPath), List.of(dictionary), CREATE, TRUNCATE_EXISTING, WRITE);
            }
            catch (Throwable e)
            {
                probe.output().err.printf("Failed to export dictionary: %s%n", e.getMessage());
                System.exit(1);
            }
        }
    }

    @Command(name = "import",
             description = "Import local dictionary to Cassandra.")
    public static class ImportDictionary extends AbstractCommand
    {
        @Option(names = "--scope", paramLabel = "scope",
                description = "Scope of the dictionary, the nodes which compress with it: " +
                              "node (this node only), rack (the rack of this node), dc (the datacenter " +
                              "of this node), until the dictionary is promoted, or cluster (all nodes). " +
                              "The default is cluster.")
        private String scope = CompressionDictionaryScope.Kind.CLUSTER.displayName();

        @Parameters(index = "0", description = "The path to a dictionary JSON", arity = "1")
        private String dictionaryPath;

        @Override
        protected void execute(NodeProbe probe)
        {
            File dictionaryFile = new File(dictionaryPath);
            validateFile(dictionaryFile, probe);
            validateScope(probe.output().err, scope);

            try
            {
                String jsonContent = FileUtils.readLines(dictionaryFile).stream().collect(joining(lineSeparator()));
                CompressionDictionaryDataObject dictionaryDataObject = JsonUtils.JSON_OBJECT_MAPPER.readValue(jsonContent, CompressionDictionaryDataObject.class);
                CompositeData compositeData = CompressionDictionaryDetailsTabularData.fromCompressionDictionaryDataObject(dictionaryDataObject);
                probe.importCompressionDictionary(compositeData, scope);
            }
            catch (ValueInstantiationException ex)
            {
                // we catch this when validation of data object fails - that will happen when
                // JSON is invalid, and we attempt to deserialize it to data object by Jackson
                // We can fail fast on the client, so we will never reach Cassandra node with a payload
                // which would fail there too, so we do not contact a node unnecessarily.
                probe.output().err.printf("Unable to import dictionary JSON: %s%n", ex.getCause().getMessage());
                System.exit(1);
            }
            catch (Throwable t)
            {
                probe.output().err.printf("Unable to import dictionary JSON: %s%n", t.getMessage());
                System.exit(1);
            }
        }

        private void validateFile(File dictionaryFile, NodeProbe probe)
        {
            if (!dictionaryFile.exists())
            {
                probe.output().err.printf("Path %s does not exist.%n", dictionaryPath);
                System.exit(1);
            }
            if (!dictionaryFile.isFile())
            {
                probe.output().err.printf("Path %s is not a file.%n", dictionaryPath);
                System.exit(1);
            }
            if (!dictionaryFile.isReadable())
            {
                probe.output().err.printf("Path %s is not readable.%n", dictionaryPath);
                System.exit(1);
            }
        }
    }

    /**
     * Fails fast on the client on a scope the node would reject.
     */
    private static void validateScope(PrintStream err, String scope)
    {
        try
        {
            CompressionDictionaryScope.Kind.parse(scope);
        }
        catch (IllegalArgumentException e)
        {
            err.println("Invalid value for --scope: " + e.getMessage());
            System.exit(1);
        }
    }

    private static class ListingHelper
    {
        /**
         * Lists dictionaries, the concrete mean of querying them is delegated to a specific subcommand.
         *
         * @param probe probe to query Cassandra with
         * @param tabularDataSupplier supplier of tabular data containing dictionaries to display
         * @param removedColumnsIndices supplier of an array with indexes of columns in tabular data to be ignored
         *                              when displaying them
         */
        public static void list(NodeProbe probe,
                                Supplier<TabularData> tabularDataSupplier,
                                List<Integer> removedColumnsIndices)
        {
            try
            {
                TableBuilder tableBuilder = new TableBuilder();
                TabularData tabularData = tabularDataSupplier.get();
                List<String> indexNames = tabularData.getTabularType().getIndexNames();

                // ignore columns not meant to be displayed to a user
                List<String> columns = new ArrayList<>();
                for (int i = 0; i < indexNames.size(); i++)
                {
                    if (!removedColumnsIndices.contains(i))
                    {
                        columns.add(indexNames.get(i));
                    }
                }

                tableBuilder.add(columns);

                boolean hasOuput = false;
                for (Object eachDict : tabularData.keySet())
                {
                    final List<?> dictRow = (List<?>) eachDict;

                    List<String> rowValues = new ArrayList<>();

                    for (int i = 0; i < dictRow.size(); i++)
                    {
                        // ignore columns not meant to be displayed to a user
                        if (removedColumnsIndices.contains(i))
                            continue;

                        Object value = dictRow.get(i);
                        if (CompressionDictionaryDetailsTabularData.SCOPE_NAME.equals(indexNames.get(i)))
                            rowValues.add(CompressionDictionaryScope.fromFlattened((String[][]) value).toString()); // null for the whole cluster
                        else
                            rowValues.add(value == null ? "" : value.toString());
                        hasOuput = true;
                    }
                    tableBuilder.add(rowValues);
                }

                if (hasOuput)
                    tableBuilder.printTo(probe.output().out);
            }
            catch (Exception e)
            {
                probe.output().err.printf("Failed to list dictionaries: %s%n", e.getMessage());
                System.exit(1);
            }
        }
    }
}
