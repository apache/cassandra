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

@Command(name = "getcompactionthroughput", description = "Print the MiB/s throughput cap for compaction in the system as a rounded number")
public class GetCompactionThroughput extends NodeToolCmd
{
    @SuppressWarnings("UnusedDeclaration")
    @Option(name = { "-d", "--precise-mib" }, description = "Print the MiB/s throughput cap for compaction in the system as a precise number (double)")
    private boolean  compactionThroughputAsDouble;

    @Override
    public void execute(NodeProbe probe)
    {
        double throughput = probe.getCompactionThroughputMebibytesAsDouble();

        if (compactionThroughputAsDouble)
            probe.output().out.println("Current compaction throughput: " + throughput + " MiB/s");
        else
        {
            if (!DoubleMath.isMathematicalInteger(throughput))
                throw new RuntimeException("Use the -d flag to quiet this error and get the exact throughput in MiB/s");

            probe.output().out.println("Current compaction throughput: " + probe.getCompactionThroughput() + " MB/s");
        }
    }
}
