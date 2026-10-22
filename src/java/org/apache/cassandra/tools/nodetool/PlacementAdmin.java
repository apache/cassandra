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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The {@code nodetool placement} command group reports the cluster's data placement, that is, how the token ranges
 * of each keyspace are placed on nodes for reads and for writes.
 */
@Command(name = "placement", description = "Inspect data placement and replica relationships",
         subcommands = { PlacementAdmin.Peers.class })
public class PlacementAdmin
{
    @Command(name = "peers", description = "Display replica peers for a node. Replica peers are nodes that share ownership of the same token ranges. " +
                                           "This information helps avoid performing concurrent operations on a node and its peers.")
    public static class Peers extends AbstractCommand
    {
        @Option(paramLabel = "node", names = { "-n", "--node" },
                description = "The node to display peers for (accepts Node ID, Host ID, or IP address). " +
                              "Includes all nodes when this option is not specified. (Mutually exclusive with --in-dc)")
        private String node;

        @Option(paramLabel = "specific_dc", names = { "-dc", "--in-dc" },
                description = "Limits the report to nodes that belong to the specified datacenters. " +
                              "This option can be repeated to include multiple datacenters. " +
                              "Includes all datacenters when it is not specified. (Mutually exclusive with --node)")
        private List<String> datacenters = new ArrayList<>();

        @Option(paramLabel = "keyspace", names = { "-k", "--keyspaces" }, split = ",",
                description = "Include only these keyspaces (comma-separated). Includes all keyspaces when " +
                              "this option is not specified. Providing a single keyspace shows peers based " +
                              "on its specific replication factor.")
        private List<String> keyspaces = new ArrayList<>();

        @Option(paramLabel = "exclude_keyspace", names = { "-x", "--exclude-keyspaces" }, split = ",",
                description = "Exclude these keyspaces (comma-separated) from the report.")
        private List<String> excludedKeyspaces = new ArrayList<>();

        @Option(paramLabel = "catch_up", names = { "--catch-up" },
                description = "Fetch the latest metadata from the CMS before executing. Fails if " +
                              "synchronization is not possible.")
        private boolean catchUp = false;

        @Option(paramLabel = "format", names = { "-f", "--format" },
                description = "Output format: yaml, json, minified-yaml, or minified-json. Case-insensitive. " +
                              "(Default: yaml)")
        private String format = "yaml";

        @Override
        protected void execute(NodeProbe probe)
        {
            checkArgument(keyspaces.isEmpty() || excludedKeyspaces.isEmpty(),
                          "--keyspaces cannot be combined with --exclude-keyspaces");
            checkArgument(node == null || datacenters.isEmpty(), "--node cannot be combined with --in-dc");

            output.out.println(probe.getStorageService()
                                    .getReplicaPeers(node,
                                                     new LinkedHashSet<>(datacenters),
                                                     new LinkedHashSet<>(keyspaces),
                                                     new LinkedHashSet<>(excludedKeyspaces),
                                                     catchUp,
                                                     format));
        }
    }
}
