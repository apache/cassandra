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

@Command(name = "setstreamthroughput", description = "Set throughput cap for streaming and entire SSTable streaming in the system, or 0 to disable throttling")
public class SetStreamThroughput extends NodeToolCmd
{
    @SuppressWarnings("UnusedDeclaration")
    @Arguments(title = "stream_throughput", usage = "<value_in_mb>", description = "Value in megabits, 0 to disable throttling", required = true)
    private int streamThroughput;

    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-e", "--entire-sstable-throughput" }, description = "Set entire SSTable streaming throughput in MiB/s")
    private boolean setEntireSSTableThroughput;

    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-m", "--mib" }, description = "Set streaming throughput in MiB/s")
    private boolean streamThroughputInMebibytes;

    @Override
    public void execute(NodeProbe probe)
    {
        if (setEntireSSTableThroughput && streamThroughputInMebibytes)
            throw new IllegalArgumentException("You cannot use -e and -m at the same time");

        if (setEntireSSTableThroughput)
            probe.setEntireSSTableStreamThroughput(streamThroughput);
        else if (streamThroughputInMebibytes )
            probe.setStreamThroughputMiB(streamThroughput);
        else
            probe.setStreamThroughput(streamThroughput);
    }
}
