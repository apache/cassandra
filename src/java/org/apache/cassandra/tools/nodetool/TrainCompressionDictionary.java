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
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Clock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "traincompressiondictionary",
description = "Manually trigger compression dictionary training for a table. If no SSTables are available, the memtable will be flushed first.")
public class TrainCompressionDictionary extends AbstractCommand
{
    @Parameters(index = "0", description = "The keyspace name", arity = "1")
    private String keyspace;

    @Parameters(index = "1", description = "The table name", arity = "1")
    private String table;

    @Option(names = {"-a", "--async"},
    description = "Run training asynchronously without waiting for completion")
    private boolean async = false;

    @Option(names = {"-s", "--status"},
    description = "Show current training status instead of starting new training")
    private boolean showStatus = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (showStatus)
        {
            showTrainingStatus(probe);
            return;
        }

        PrintStream out = probe.output().out;
        PrintStream err = probe.output().err;

        try
        {
            out.printf("Starting compression dictionary training for %s.%s...%n", keyspace, table);
            out.printf("Training from existing SSTables (flushing first if needed)%n");

            probe.trainCompressionDictionary(keyspace, table);

            if (async)
            {
                out.printf("Training started asynchronously for %s.%s%n", keyspace, table);
                out.printf("Use 'nodetool traincompressiondictionary --status %s %s' to check progress.%n",
                           keyspace, table);
                return;
            }

            // Wait for training completion (10 minutes timeout for SSTable-based training)
            out.println("Sampling from existing SSTables and training.");
            long maxWaitMillis = TimeUnit.MINUTES.toMillis(10);
            long startTime = Clock.Global.currentTimeMillis();

            while (Clock.Global.currentTimeMillis() - startTime < maxWaitMillis)
            {
                String statusStr = probe.getCompressionDictionaryTrainingStatus(keyspace, table);
                TrainingStatus status = TrainingStatus.valueOf(statusStr);
                if (TrainingStatus.COMPLETED == status)
                {
                    out.printf("%nTraining completed successfully for %s.%s%n", keyspace, table);
                    return;
                }
                else if (TrainingStatus.FAILED == status)
                {
                    err.printf("%nTraining failed for %s.%s%n", keyspace, table);
                    System.exit(1);
                }

                // Display meaningful statistics
                long sampleCount = probe.getCompressionDictionaryTrainingSampleCount(keyspace, table);
                long totalSampleSize = probe.getCompressionDictionaryTrainingTotalSampleSize(keyspace, table);
                long elapsedSeconds = (Clock.Global.currentTimeMillis() - startTime) / 1000;
                double sampleSizeMB = totalSampleSize / (1024.0 * 1024.0);

                out.printf("\rStatus: %s | Samples: %d | Size: %.2f MiB | Elapsed: %ds",
                           status, sampleCount, sampleSizeMB, elapsedSeconds);

                Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            }

            err.printf("%nTraining did not complete within expected timeframe (10 minutes). Use --status to check current state.%n");
            System.exit(1);
        }
        catch (Exception e)
        {
            err.printf("Failed to trigger training: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    private void showTrainingStatus(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        PrintStream err = probe.output().err;
        String statusStr = null;
        try
        {
            statusStr = probe.getCompressionDictionaryTrainingStatus(keyspace, table);
        }
        catch (Exception e)
        {
            err.printf("Failed to get training status: %s%n", e.getMessage());
            System.exit(1);
        }

        TrainingStatus status = TrainingStatus.valueOf(statusStr);
        if (status == TrainingStatus.FAILED)
        {
            showStatistics(probe, err, status);
        }
        else
        {
            showStatistics(probe, out, status);
        }
    }

    private void showStatistics(NodeProbe probe, PrintStream out, TrainingStatus status)
    {
        try
        {
            TableBuilder tableBuilder = new TableBuilder();
            tableBuilder.add("keyspace", keyspace);
            tableBuilder.add("table", table);
            tableBuilder.add("status", status.name());

            if (status == TrainingStatus.SAMPLING || status == TrainingStatus.TRAINING)
            {
                long sampleCount = probe.getCompressionDictionaryTrainingSampleCount(keyspace, table);
                long totalSampleSize = probe.getCompressionDictionaryTrainingTotalSampleSize(keyspace, table);

                tableBuilder.add("samples collected", String.format("%d", sampleCount));
                tableBuilder.add("total sample size", FileUtils.stringifyFileSize(totalSampleSize));
            }

            tableBuilder.printTo(out);
        }
        catch (Exception e)
        {
            out.printf("Unable to retrieve training statistics: %s%n", e.getMessage());
        }
    }
}
