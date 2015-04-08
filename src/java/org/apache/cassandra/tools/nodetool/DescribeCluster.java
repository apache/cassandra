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

import java.util.List;
import java.util.Map;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "describecluster", description = "Print the name, snitch, partitioner and schema version of a cluster")
public class DescribeCluster extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        // display cluster name, snitch and partitioner
        System.out.println("Cluster Information:");
        System.out.println("\tName: " + probe.getClusterName());
        System.out.println("\tSnitch: " + probe.getEndpointSnitchInfoProxy().getSnitchName());
        System.out.println("\tPartitioner: " + probe.getPartitioner());

        // display schema version for each node
        System.out.println("\tSchema versions:");
        Map<String, List<String>> schemaVersions = probe.getSpProxy().getSchemaVersions();
        for (String version : schemaVersions.keySet())
        {
            System.out.println(format("\t\t%s: %s%n", version, schemaVersions.get(version)));
        }
    }
}