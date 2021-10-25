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

@Command(name = "getstreamthroughput", description = "Print the Mb/s throughput cap for streaming and entire SSTable streaming in the system")
public class GetStreamThroughput extends NodeToolCmd
{
    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-esst", "--print-entire-sstable-throughput" }, description = "Print entire SSTable streaming throughput")
    private boolean printEntireSSTableThroughput;

    @Override
    public void execute(NodeProbe probe)
    {
        if (printEntireSSTableThroughput)
            probe.output().out.println("Current entire SSTable stream throughput: " + probe.getEntireSSTableStreamThroughput() + " Mb/s");
        else
            probe.output().out.println("Current stream throughput: " + probe.getStreamThroughput() + " Mb/s");
    }
}
