/**
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

package org.apache.cassandra.stress;

import java.io.PrintStream;
import org.apache.commons.lang3.time.DurationFormatUtils;

import com.yammer.metrics.stats.Snapshot;


/**
 * Gathers and aggregates statistics for an operation
 */
public class StressStatistics
{
    
    private Session client;
    private PrintStream output;

    private long durationInSeconds;
    /** The sum of the interval_op_rate values collected by tallyAverages */
    private int tallyOpRateSum;
    /** The number of interval_op_rate values collected by tallyAverages */
    private int tallyOpRateCount;
    /** The sum of the interval_key_rate values collected by tallyAverages */
    private int tallyKeyRateSum;
    /** The number of interval_key_rate values collected by tallyAverages */
    private int tallyKeyRateCount;

    /** The sum of the latency values collected by tallyAverages */
    private double tallyLatencySum;
    /** The number of latency values collected by tallyAverages */
    private int tallyLatencyCount;
    /** The sum of the 95%tile latency values collected by tallyAverages */
    private double tally95thLatencySum;
    /** The number of 95%tile latency values collected by tallyAverages */
    private int tally95thLatencyCount;
    /** The sum of the 99.9%tile latency values collected by tallyAverages */
    private double tally999thLatencySum;
    /** The number of 99.9%tile latency values collected by tallyAverages */
    private int tally999thLatencyCount;
    

    public StressStatistics(Session client, PrintStream out)
    {
        this.client = client;
        this.output = out;

        tallyOpRateSum = 0;
        tallyOpRateCount = 0;
    }

    /**
     * Collect statistics per-interval
     */
    public void addIntervalStats(int totalOperations, int intervalOpRate, 
                                 int intervalKeyRate, Snapshot latency, 
                                 long currentTimeInSeconds)
    {
        this.tallyAverages(totalOperations, intervalKeyRate, intervalKeyRate, 
                                latency, currentTimeInSeconds);
    }

    /**
     * Collect interval_op_rate and interval_key_rate averages
     */
    private void tallyAverages(int totalOperations, int intervalOpRate, 
                                 int intervalKeyRate, Snapshot latency, 
                                 long currentTimeInSeconds)
    {
        //Skip the first and last 10% of values.
        //The middle values of the operation are the ones worthwhile
        //to collect and average:
        if (totalOperations > (0.10 * client.getNumKeys()) &&
            totalOperations < (0.90 * client.getNumKeys())) {
                tallyOpRateSum += intervalOpRate;
                tallyOpRateCount += 1;
                tallyKeyRateSum += intervalKeyRate;
                tallyKeyRateCount += 1;
                tallyLatencySum += latency.getMedian();
                tallyLatencyCount += 1;
                tally95thLatencySum += latency.get95thPercentile();
                tally95thLatencyCount += 1;
                tally999thLatencySum += latency.get999thPercentile();
                tally999thLatencyCount += 1;
            }
        durationInSeconds = currentTimeInSeconds;
    }

    public void printStats()
    {
        output.println("\n");
        if (tallyOpRateCount > 0) {
            output.println("Averages from the middle 80% of values:");
            output.println(String.format("interval_op_rate          : %d", 
                                         (tallyOpRateSum / tallyOpRateCount)));
            output.println(String.format("interval_key_rate         : %d", 
                                         (tallyKeyRateSum / tallyKeyRateCount)));
            output.println(String.format("latency median            : %.1f", 
                                         (tallyLatencySum / tallyLatencyCount)));
            output.println(String.format("latency 95th percentile   : %.1f",
                                         (tally95thLatencySum / tally95thLatencyCount)));
            output.println(String.format("latency 99.9th percentile : %.1f", 
                                         (tally999thLatencySum / tally999thLatencyCount)));
        }
        output.println("Total operation time      : " + DurationFormatUtils.formatDuration(
            durationInSeconds*1000, "HH:mm:ss", true));
    }

}
