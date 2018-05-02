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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;

import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TableMetrics.Sampler;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "toppartitions", description = "Sample and print the most active partitions")
public class TopPartitions extends NodeToolCmd
{
    @Arguments(usage = "[keyspace table] [duration]", description = "The keyspace, table name, and duration in milliseconds")
    private List<String> args = new ArrayList<>();
    @Option(name = "-s", description = "Capacity of stream summary, closer to the actual cardinality of partitions will yield more accurate results (Default: 256)")
    private int size = 256;
    @Option(name = "-k", description = "Number of the top partitions to list (Default: 10)")
    private int topCount = 10;
    @Option(name = "-a", description = "Comma separated list of samplers to use (Default: all)")
    private String samplers = join(TableMetrics.Sampler.values(), ',');
    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3 || args.size() == 1 || args.size() == 0, "Invalid arguments, either [keyspace table duration] or [duration] or no args");
        checkArgument(topCount < size, "TopK count (-k) option must be smaller then the summary capacity (-s)");
        String keyspace = null;
        String table = null;
        Integer duration = 10000;
        if(args.size() == 3)
        {
            keyspace = args.get(0);
            table = args.get(1);
            duration = Integer.valueOf(args.get(2));
        }
        else if (args.size() == 1)
        {
            duration = Integer.valueOf(args.get(0));
        }
        // generate the list of samplers
        List<String> targets = Lists.newArrayList();
        for (String s : samplers.split(","))
        {
            try
            {
                targets.add(Sampler.valueOf(s.toUpperCase()).toString());
            } catch (Exception e)
            {
                throw new IllegalArgumentException(s + " is not a valid sampler, choose one of: " + join(Sampler.values(), ", "));
            }
        }

        Map<String, Map<String, CompositeData>> results;
        try
        {
            if (keyspace == null)
            {
                results = probe.getPartitionSample(size, duration, topCount, targets);
            }
            else
            {
                results = probe.getPartitionSample(keyspace, table, size, duration, topCount, targets);
            }
        } catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
        boolean first = true;
        for(String sampler : targets)
        {
            if(!first)
                System.out.println();
            first = false;
            System.out.printf(sampler + " Sampler Top %d partitions:%n", topCount);
            TableBuilder out = new TableBuilder();
            out.add("\t", "Table", "Partition", "Count", "+/-");
            List<Pair<String, CompositeData>> topk = new ArrayList<>(topCount);
            for (Entry<String, Map<String, CompositeData>> tableResult : results.entrySet())
            {
                String tableName = tableResult.getKey();
                CompositeData sampling = tableResult.getValue().get(sampler);
                // weird casting for http://bugs.sun.com/view_bug.do?bug_id=6548436
                for(CompositeData cd : (List<CompositeData>) (Object) Lists.newArrayList(((TabularDataSupport) sampling.get("partitions")).values()))
                {
                    topk.add(Pair.create(tableName, cd));
                }
            }
            Collections.sort(topk, new Ordering<Pair<String, CompositeData>>()
            {
                public int compare(Pair<String, CompositeData> left, Pair<String, CompositeData> right)
                {
                    return Long.compare((long) right.right.get("count"), (long) left.right.get("count"));
                }
            });
            for (Pair<String, CompositeData> entry : topk.subList(0, Math.min(topk.size(), 10)))
            {
                CompositeData cd = entry.right;
                out.add("\t", entry.left, cd.get("string").toString(), cd.get("count").toString(), cd.get("error").toString());
            }
            out.printTo(System.out);
            if (topk.size() == 0)
            {
                System.out.println("\t Nothing recorded during sampling period...");
            }
        }
    }
}
