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

package org.apache.cassandra.tools.nodetool.clustermetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tcm.serialization.Version;

@Command(name = "dump", description = "Dumps Cluster Metadata into a file")
public class DumpClusterMetadata extends NodeTool.NodeToolCmd
{

    @Option(title = "Epoch", name = { "-e", "--epoch" },
    description = "Epoch at which cluster metadata should be dumped")
    private Long epoch;

    @Option(title = "Transform Epoch", name = { "-te", "--transform-epoch" },
    description = "The epoch to which the cluster meta data should be transformed before dumping")
    private Long transformEpoch;

    @Option(title = "Version", name = { "-v", "--version" },
    description = "Searialization version")
    private Version version;

    protected void execute(NodeProbe probe)
    {
        if (epoch == null && transformEpoch == null && version == null)
        {
            try
            {
                String fileLocation = probe.getCMSOperationsProxy().dumpClusterMetadata();
                printCMSDumpLocation(probe, fileLocation);
            }
            catch (IOException e)
            {
                //TODO: fix it
                throw new RuntimeException(e);
            }
        }
        else if (epoch != null && transformEpoch != null && version != null)
        {
            try
            {
                String fileLocation = probe.getCMSOperationsProxy().dumpClusterMetadata(epoch,
                                                                                        transformEpoch,
                                                                                        version.name());
                printCMSDumpLocation(probe, fileLocation);
            }
            catch (IOException e)
            {
                e.printStackTrace(output.err);
                System.exit(1);
            }
        }
        else
        {
            List<String> invalidArgs = new ArrayList<>(2);
            if (null == epoch)
            {
                invalidArgs.add("epoch");
            }
            if (null == transformEpoch)
            {
                invalidArgs.add("transform-epoch");
            }
            if (null == version)
            {
                invalidArgs.add("version");
            }

            probe.output().err.println("The three aguments epoch, transform-epoch and version should be specified " +
                                       "together. Arguments " + String.join(",", invalidArgs) + " must be " +
                                       "passed.");
            System.exit(1);
        }
    }

    private void printCMSDumpLocation(NodeProbe probe, String fileLocation)
    {
        probe.output().out.println("Cluster Metadata dumped to file: " + fileLocation);
    }
}
