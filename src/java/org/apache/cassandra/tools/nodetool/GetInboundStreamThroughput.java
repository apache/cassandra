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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getinboundstreamthroughput", description = "Print the inbound throughput cap for streaming and entire SSTable streaming in the system in MiB/s. ")
public class GetInboundStreamThroughput extends NodeToolCmd
{
    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-e", "--entire-sstable-inbound-throughput" }, description = "Print entire SSTable inbound streaming throughput in MiB/s")
    private boolean entireSSTableThroughput;

    @Override
    public void execute(NodeProbe probe)
    {
        double throughputInDouble;

        if (entireSSTableThroughput)
        {
            throughputInDouble = probe.getEntireSSTableStreamThroughputInboundMibAsDouble();
            probe.output().out.printf("Current inbound entire SSTable stream throughput: %s%n",
                                      throughputInDouble > 0 ? throughputInDouble + " MiB/s" : "unlimited");
        }
        else
        {
            throughputInDouble = probe.getStreamThroughputInboundMibAsDouble();
            probe.output().out.printf("Current inbound stream throughput: %s%n",
                                      throughputInDouble > 0 ? throughputInDouble + " MiB/s" : "unlimited");
        }
    }
}
