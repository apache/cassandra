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
import org.apache.cassandra.db.compression.TrainingState;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.Clock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "traincompressiondictionary",
description = "Manually trigger compression dictionary training for a table. If no SSTables are available, the memtable will be flushed first.")
public class TrainCompressionDictionary extends AbstractCommand
{
    @Parameters(index = "0", description = "The keyspace name", arity = "1")
    private String keyspace;

    @Parameters(index = "1", description = "The table name", arity = "1")
    private String table;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        PrintStream err = probe.output().err;

        try
        {
            out.printf("Starting compression dictionary training for %s.%s...%n", keyspace, table);
            out.printf("Training from existing SSTables (flushing first if needed)%n");

            probe.trainCompressionDictionary(keyspace, table);

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
}
