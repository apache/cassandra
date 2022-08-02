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

import com.google.common.math.DoubleMath;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getstreamthroughput", description = "Print the throughput cap for streaming and entire SSTable streaming in the system in rounded megabits. " +
                                                     "For precise number, please, use option -d")
public class GetStreamThroughput extends NodeToolCmd
{
    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-e", "--entire-sstable-throughput" }, description = "Print entire SSTable streaming throughput in MiB/s")
    private boolean entireSSTableThroughput;

    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-m", "--mib" }, description = "Print the throughput cap for streaming in MiB/s")
    private boolean streamThroughputMiB;

    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-d", "--precise-mbit" }, description = "Print the throughput cap for streaming in precise Mbits (double)")
    private boolean streamThroughputDoubleMbit;

    @Override
    public void execute(NodeProbe probe)
    {
        int throughput;
        double throughputInDouble;

        if (entireSSTableThroughput)
        {
            if (streamThroughputDoubleMbit || streamThroughputMiB)
                throw new IllegalArgumentException("You cannot use more than one flag with this command");

            throughputInDouble = probe.getEntireSSTableStreamThroughput();
            probe.output().out.printf("Current entire SSTable stream throughput: %s%n",
                                      throughputInDouble > 0 ? throughputInDouble + " MiB/s" : "unlimited");
        }
        else if (streamThroughputMiB)
        {
            if (streamThroughputDoubleMbit)
                throw new IllegalArgumentException("You cannot use more than one flag with this command");

            throughputInDouble = probe.getStreamThroughputMibAsDouble();
            probe.output().out.printf("Current stream throughput: %s%n",
                                      throughputInDouble > 0 ? throughputInDouble + " MiB/s" : "unlimited");
        }
        else if (streamThroughputDoubleMbit)
        {
            throughputInDouble = probe.getStreamThroughputAsDouble();
            probe.output().out.printf("Current stream throughput: %s%n",
                                      throughputInDouble > 0 ? throughputInDouble + " Mb/s" : "unlimited");
        }
        else
        {
            throughputInDouble = probe.getStreamThroughputAsDouble();
            throughput = probe.getStreamThroughput();

            if (throughput <= 0)
                probe.output().out.printf("Current stream throughput: unlimited%n");
            else if (DoubleMath.isMathematicalInteger(throughputInDouble))
                probe.output().out.printf("Current stream throughput: %s%n", throughput + " Mb/s");
            else
                throw new RuntimeException("Use the -d flag to quiet this error and get the exact throughput in megabits/s");
        }
    }
}
