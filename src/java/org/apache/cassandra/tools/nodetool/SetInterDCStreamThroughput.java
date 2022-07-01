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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "setinterdcstreamthroughput", description = "Set the throughput cap for inter-datacenter streaming and entire SSTable inter-datacenter streaming in the system, or 0 to disable throttling")
public class SetInterDCStreamThroughput extends NodeToolCmd
{
    @SuppressWarnings("UnusedDeclaration")
    @Arguments(title = "inter_dc_stream_throughput", usage = "<value_in_mb>", description = "Value in megabits, 0 to disable throttling", required = true)
    private int interDCStreamThroughput;

    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-e", "--entire-sstable-throughput" }, description = "Set entire SSTable streaming throughput in MiB/s")
    private boolean setEntireSSTableThroughput;

    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-i", "--inter_dc_stream_throughput_mib" }, description = "Set streaming throughput in MiB/s, if you see 1 MiB/s, " +
                                                                               "please check for precise value in megabits/s as this might be rounded")
    private boolean interDCStreamThroughputInMebibytes;

    @Override
    public void execute(NodeProbe probe)
    {
        if (setEntireSSTableThroughput && interDCStreamThroughputInMebibytes)
            probe.output().out.println("You cannot use -e and -i at the same time");

        if (setEntireSSTableThroughput)
            probe.setEntireSSTableInterDCStreamThroughput(interDCStreamThroughput);
        else if (interDCStreamThroughputInMebibytes )
            probe.setInterDCStreamThroughputMiB(interDCStreamThroughput);
        else
            probe.setInterDCStreamThroughput(interDCStreamThroughput);
    }
}
