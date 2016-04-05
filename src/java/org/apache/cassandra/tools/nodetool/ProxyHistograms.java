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

import static java.lang.String.format;
import io.airlift.command.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "proxyhistograms", description = "Print statistic histograms for network operations")
public class ProxyHistograms extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        String[] percentiles = {"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
        double[] readLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Read"));
        double[] writeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Write"));
        double[] rangeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("RangeSlice"));
        double[] casReadLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("CASRead"));
        double[] casWriteLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("CASWrite"));
        double[] viewWriteLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("ViewWrite"));

        System.out.println("proxy histograms");
        System.out.println(format("%-10s%19s%19s%19s%19s%19s%19s",
                "Percentile", "Read Latency", "Write Latency", "Range Latency", "CAS Read Latency", "CAS Write Latency", "View Write Latency"));
        System.out.println(format("%-10s%19s%19s%19s%19s%19s%19s",
                "", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)"));
        for (int i = 0; i < percentiles.length; i++)
        {
            System.out.println(format("%-10s%19.2f%19.2f%19.2f%19.2f%19.2f%19.2f",
                    percentiles[i],
                    readLatency[i],
                    writeLatency[i],
                    rangeLatency[i],
                    casReadLatency[i],
                    casWriteLatency[i],
                    viewWriteLatency[i]));
        }
        System.out.println();
    }
}
