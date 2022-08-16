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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import com.google.common.collect.Lists;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.metrics.SamplingManager;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.join;

@Command(name = "profileload", description = "Low footprint profiling of activity for a period of time")
public class ProfileLoad extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <cfname> <duration>", description = "The keyspace, column family name, and duration in milliseconds (Default: 10000)")
    private List<String> args = new ArrayList<>();

    @Option(name = "-s", description = "Capacity of the sampler, higher for more accuracy (Default: 256)")
    private int capacity = 256;

    @Option(name = "-k", description = "Number of the top samples to list (Default: 10)")
    private int topCount = 10;

    @Option(name = "-a", description = "Comma separated list of samplers to use (Default: all)")
    private String samplers = join(SamplerType.values(), ',');

    @Option(name = {"-i", "--interval"}, description = "Schedule a new job that samples every interval milliseconds (Default: disabled) in the background")
    private int intervalMillis = -1; // -1 for disabled.

    @Option(name = {"-t", "--stop"}, description = "Stop the scheduled sampling job identified by <keyspace> and <cfname>. Jobs are stopped until the last schedules complete.")
    private boolean shouldStop = false;

    @Option(name = {"-l", "--list"}, description = "List the scheduled sampling jobs")
    private boolean shouldList = false;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3 || args.size() == 2 || args.size() == 1 || args.size() == 0,
                      "Invalid arguments, either [keyspace table/* duration] or [keyspace table/*] or [duration] or no args.\n" +
                      "Optionally, use * to represent all tables under the keyspace.");
        checkArgument(topCount > 0, "TopK count (-k) option must have positive value");
        checkArgument(topCount < capacity,
                      "TopK count (-k) option must be smaller then the summary capacity (-s)");
        checkArgument(capacity <= 1024, "Capacity (-s) cannot exceed 1024.");
        String keyspace = null;
        String table = null;
        int durationMillis = 10000;
        /* There are 3 possible outcomes after processing the args.
         * - keyspace == null && table == null. We need to sample all tables
         * - keyspace == KEYSPACE && table == *. We need to sample all tables under the specified KEYSPACE
         * - keyspace = KEYSPACE && table == TABLE. Sample the specific KEYSPACE.table combination
         */
        if (args.size() == 3)
        {
            keyspace = args.get(0);
            table = args.get(1);
            durationMillis = Integer.parseInt(args.get(2));
        }
        else if (args.size() == 2)
        {
            keyspace = args.get(0);
            table = args.get(1);
        }
        else if (args.size() == 1)
        {
            durationMillis = Integer.parseInt(args.get(0));
        }
        keyspace = nullifyWildcard(keyspace);
        table = nullifyWildcard(table);

        checkArgument(durationMillis > 0, "Duration: %s must be positive", durationMillis);

        checkArgument(!hasInterval() || intervalMillis >= durationMillis,
                      "Invalid scheduled sampling interval. Expecting interval >= duration, but interval: %s ms; duration: %s ms",
                      intervalMillis, durationMillis);
        // generate the list of samplers
        List<String> targets = Lists.newArrayList();
        Set<String> available = Arrays.stream(SamplerType.values()).map(Enum::toString).collect(Collectors.toSet());
        for (String s : samplers.split(","))
        {
            String sampler = s.trim().toUpperCase();
            checkArgument(available.contains(sampler), String.format("'%s' sampler is not available from: %s", s, Arrays.toString(SamplerType.values())));
            targets.add(sampler);
        }

        PrintStream out = probe.output().out;

        Map<String, List<CompositeData>> results;
        try
        {
            // handle scheduled samplings, i.e. start or stop
            if (hasInterval() || shouldStop)
            {
                // keyspace and table are nullable
                boolean opSuccess = probe.handleScheduledSampling(keyspace, table, capacity, topCount, durationMillis, intervalMillis, targets, shouldStop);
                if (!opSuccess)
                {
                    if (shouldStop)
                        out.printf("Unable to stop the non-existent scheduled sampling for keyspace: %s, table: %s%n", keyspace, table);
                    else
                        out.printf("Unable to schedule sampling for keyspace: %s, table: %s due to existing samplings. " +
                                   "Stop the existing sampling jobs first.%n", keyspace, table);
                }
                return;
            }
            else if (shouldList)
            {
                List<Pair<String, String>> sampleTasks = new ArrayList<>();
                int maxKsLength = "KEYSPACE".length();
                int maxTblLength = "TABLE".length();
                for (String fullTableName : probe.getSampleTasks())
                {
                    String[] parts = fullTableName.split("\\.");
                    checkState(parts.length == 2, "Unable to parse the full table name: %s", fullTableName);
                    sampleTasks.add(Pair.create(parts[0], parts[1]));
                    maxKsLength = Math.max(maxKsLength, parts[0].length());
                }
                // print the header line and put enough space between KEYSPACE AND TABLE.
                String lineFormat = "%" + maxKsLength + "s %" + maxTblLength + "s%n";
                out.printf(lineFormat, "KEYSPACE", "TABLE");
                sampleTasks.forEach(pair -> out.printf(lineFormat, pair.left, pair.right));
                return;
            }
            else
            {
                // blocking sample all the tables or all the tables under a keyspace
                if (keyspace == null || table == null)
                    results = probe.getPartitionSample(keyspace, capacity, durationMillis, topCount, targets);
                else // blocking sample the specific table
                    results = probe.getPartitionSample(keyspace, table, capacity, durationMillis, topCount, targets);
            }
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }

        AtomicBoolean first = new AtomicBoolean(true);
        SamplingManager.ResultBuilder rb = new SamplingManager.ResultBuilder(first, results, targets);
        out.println(SamplingManager.formatResult(rb));
    }

    private boolean hasInterval()
    {
        return intervalMillis != -1;
    }

    private String nullifyWildcard(String input)
    {
        return input != null && input.equals("*") ? null : input;
    }
}
