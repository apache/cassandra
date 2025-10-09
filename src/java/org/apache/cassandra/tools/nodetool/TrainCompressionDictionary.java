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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.db.compression.ManualTrainingOptions;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.Clock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "traincompressiondictionary",
description = "Manually trigger compression dictionary training for a table")
public class TrainCompressionDictionary extends AbstractCommand
{
    @Parameters(index = "0", description = "The keyspace name", arity = "1")
    private String keyspace;

    @Parameters(index = "1", description = "The table name", arity = "1")
    private String table;

    @Option(names = {"-d", "--max-sampling-duration"},
    description = "Maximum time to collect samples before training dictionary (default: 600 seconds)")
    private int maxSamplingDurationSeconds = 600;

    @Option(names = {"-r", "--sampling-rate"},
    description = "Sampling rate as a double value in range (0, 1]. 1.0 means sample all data, 0.5 means sample 50%% of data")
    private Double samplingRate;

    @Option(names = {"-a", "--async"},
    description = "Run training asynchronously without waiting for completion")
    private boolean async = false;

    @Option(names = {"-s", "--status"},
    description = "Show current training status instead of starting new training")
    private boolean showStatus = false;

    @Option(names = {"-e", "--use-existing-sstables"},
    description = "Train from existing SSTable chunks instead of sampling from writes")
    private boolean useExistingSSTables = false;

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
        if (maxSamplingDurationSeconds <= 0)
        {
            err.printf("Invalid value for max-sampling-duration: %s%n", maxSamplingDurationSeconds);
            System.exit(1);
        }

        if (samplingRate != null && (samplingRate <= 0.0 || samplingRate > 1.0))
        {
            err.printf("Invalid value for sampling-rate: %s. Must be in range (0, 1]%n", samplingRate);
            System.exit(1);
        }
        try
        {
            out.printf("Starting compression dictionary training for %s.%s...%n", keyspace, table);
            if (useExistingSSTables)
            {
                out.printf("Training from existing SSTables%n");
            }
            else
            {
                out.printf("Will collect samples for up to %d seconds before training%n", maxSamplingDurationSeconds);
            }
            if (samplingRate != null)
            {
                out.printf("Using sampling rate: %.2f (%.1f%%)%n", samplingRate, samplingRate * 100);
            }

            // Build options map
            Map<String, String> options = new HashMap<>();
            options.put(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, String.valueOf(maxSamplingDurationSeconds));
            options.put(ManualTrainingOptions.USE_EXISTING_SSTABLES_KEY, String.valueOf(useExistingSSTables));

            probe.trainCompressionDictionary(keyspace, table, options);

            // Update sampling rate if provided (after training has started)
            if (samplingRate != null)
            {
                // Convert from double (0, 1] to integer format (1/rate)
                // Examples: 1.0 -> 1 (sample every time), 0.5 -> 2 (roughly sample every 2nd), 0.1 -> 10 (roughly sample every 10th)
                int integerSamplingRate = (int) Math.round(1.0 / samplingRate);
                probe.updateCompressionDictionaryTrainingSamplingRate(keyspace, table, integerSamplingRate);
            }

            if (async)
            {
                out.printf("Training started asynchronously for %s.%s%n", keyspace, table);
                out.printf("Use 'nodetool traincompressiondictionary --status %s %s' to check progress.%n",
                           keyspace, table);
                return;
            }

            // Wait for completion (training will start automatically after sampling period)
            if (useExistingSSTables)
            {
                out.println("Sampling from existing SSTables and training.");
            }
            else
            {
                out.println("Collecting samples and training. (Since the trainer samples chunk data on " +
                            "writing to new SSTable, you might consider running nodetool 'flush' along " +
                            "with this command to have chunk available for sampling)");
            }
            long maxWaitMillis = TimeUnit.SECONDS.toMillis(maxSamplingDurationSeconds + 300); // Add 5 minutes for training
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

            err.printf("%nTraining did not complete within expected timeframe (%d seconds sampling + 5 minutes training). Use --status to check current state.%n",
                       maxSamplingDurationSeconds);
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
        switch (status)
        {
            case NOT_STARTED:
                out.printf("Trainer is not running for %s.%s%n", keyspace, table);
                break;
            case SAMPLING:
                out.printf("Trainer is collecting sample data for %s.%s%n", keyspace, table);
                showStatistics(probe, out);
                break;
            case TRAINING:
                out.printf("Training is in progress for %s.%s%n", keyspace, table);
                showStatistics(probe, out);
                break;
            case COMPLETED:
                out.printf("Training is completed for %s.%s%n", keyspace, table);
                break;
            case FAILED:
                err.printf("Training failed for %s.%s%n", keyspace, table);
                break;
            default:
                err.printf("Encountered unexpected training status for %s.%s: %s%n", keyspace, table, status);
        }
    }

    private void showStatistics(NodeProbe probe, PrintStream out)
    {
        try
        {
            long sampleCount = probe.getCompressionDictionaryTrainingSampleCount(keyspace, table);
            long totalSampleSize = probe.getCompressionDictionaryTrainingTotalSampleSize(keyspace, table);
            double sampleSizeMB = totalSampleSize / (1024.0 * 1024.0);

            out.printf("  Samples collected: %d%n", sampleCount);
            out.printf("  Total sample size: %.2f MiB%n", sampleSizeMB);
        }
        catch (Exception e)
        {
            out.printf("  Unable to retrieve training statistics: %s%n", e.getMessage());
        }
    }
}
