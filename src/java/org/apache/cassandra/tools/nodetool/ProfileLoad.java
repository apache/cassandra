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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.join;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.Lists;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "profileload", description = "Low footprint profiling of activity for a period of time")
public class ProfileLoad extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <cfname> <duration>", description = "The keyspace, column family name, and duration in milliseconds")
    private List<String> args = new ArrayList<>();

    @Option(name = "-s", description = "Capacity of the sampler, higher for more accuracy (Default: 256)")
    private int capacity = 256;

    @Option(name = "-k", description = "Number of the top samples to list (Default: 10)")
    private int topCount = 10;

    @Option(name = "-a", description = "Comma separated list of samplers to use (Default: all)")
    private String samplers = join(SamplerType.values(), ',');

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3 || args.size() == 1 || args.size() == 0, "Invalid arguments, either [keyspace table duration] or [duration] or no args");
        checkArgument(topCount < capacity, "TopK count (-k) option must be smaller then the summary capacity (-s)");
        String keyspace = null;
        String table = null;
        Integer durationMillis = 10000;
        if(args.size() == 3)
        {
            keyspace = args.get(0);
            table = args.get(1);
            durationMillis = Integer.valueOf(args.get(2));
        }
        else if (args.size() == 1)
        {
            durationMillis = Integer.valueOf(args.get(0));
        }
        // generate the list of samplers
        List<String> targets = Lists.newArrayList();
        List<String> available = Arrays.stream(SamplerType.values()).map(Enum::toString).collect(Collectors.toList());
        for (String s : samplers.split(","))
        {
            String sampler = s.trim().toUpperCase();
            checkArgument(available.contains(sampler), String.format("'%s' sampler is not available from: %s", s, Arrays.toString(SamplerType.values())));
            targets.add(sampler);
        }

        Map<String, List<CompositeData>> results;
        try
        {
            if (keyspace == null)
                results = probe.getPartitionSample(capacity, durationMillis, topCount, targets);
            else
                results = probe.getPartitionSample(keyspace, table, capacity, durationMillis, topCount, targets);

        } catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }

        AtomicBoolean first = new AtomicBoolean(true);
        ResultBuilder rb = new ResultBuilder(first, results, targets);

        for(String sampler : Lists.newArrayList("READS", "WRITES", "CAS_CONTENTIONS"))
        {
            rb.forType(SamplerType.valueOf(sampler), "Frequency of " + sampler.toLowerCase().replaceAll("_", " ") + " by partition")
            .addColumn("Table", "table")
            .addColumn("Partition", "value")
            .addColumn("Count", "count")
            .addColumn("+/-", "error")
            .print(probe.output().out);
        }

        rb.forType(SamplerType.WRITE_SIZE, "Max mutation size by partition")
            .addColumn("Table", "table")
            .addColumn("Partition", "value")
            .addColumn("Bytes", "count")
            .print(probe.output().out);

        rb.forType(SamplerType.LOCAL_READ_TIME, "Longest read query times")
            .addColumn("Query", "value")
            .addColumn("Microseconds", "count")
            .print(probe.output().out);
    }

    private class ResultBuilder
    {
        private SamplerType type;
        private String description;
        private AtomicBoolean first;
        private Map<String, List<CompositeData>> results;
        private List<String> targets;
        private List<Pair<String, String>> dataKeys;

        public ResultBuilder(AtomicBoolean first, Map<String, List<CompositeData>> results, List<String> targets)
        {
            super();
            this.first = first;
            this.results = results;
            this.targets = targets;
            this.dataKeys = new ArrayList<>();
            this.dataKeys.add(Pair.create("  ", "  "));
        }

        public ResultBuilder forType(SamplerType type, String description)
        {
            ResultBuilder rb = new ResultBuilder(first, results, targets);
            rb.type = type;
            rb.description = description;
            return rb;
        }

        public ResultBuilder addColumn(String title, String key)
        {
            this.dataKeys.add(Pair.create(title, key));
            return this;
        }

        private String get(CompositeData cd, String key)
        {
            if (cd.containsKey(key))
                return cd.get(key).toString();
            return key;
        }

        public void print(PrintStream outStream)
        {
            if (targets.contains(type.toString()))
            {
                if (!first.get())
                    outStream.println();
                first.set(false);
                outStream.println(description + ':');
                TableBuilder out = new TableBuilder();
                out.add(dataKeys.stream().map(p -> p.left).collect(Collectors.toList()).toArray(new String[] {}));
                List<CompositeData> topk = results.get(type.toString());
                for (CompositeData cd : topk)
                {
                    out.add(dataKeys.stream().map(p -> get(cd, p.right)).collect(Collectors.toList()).toArray(new String[] {}));
                }
                if (topk.size() == 0)
                {
                    outStream.println("   Nothing recorded during sampling period...");
                }
                else
                {
                    out.printTo(outStream);
                }
            }
        }
    }
}
