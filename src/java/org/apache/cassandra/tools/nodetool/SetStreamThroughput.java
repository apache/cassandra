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

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "setstreamthroughput", description = "Set throughput cap for streaming and entire SSTable streaming in the system, or 0 to disable throttling")
public class SetStreamThroughput extends AbstractCommand
{
    @Parameters(paramLabel = "stream_throughput", description = "Value in megabits, 0 to disable throttling", arity = "1")
    private int streamThroughput;

    @Option(names = { "-e", "--entire-sstable-throughput" }, description = "Set entire SSTable streaming throughput in MiB/s")
    private boolean setEntireSSTableThroughput;

    @Option(names = { "-m", "--mib" }, description = "Set streaming throughput in MiB/s")
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
