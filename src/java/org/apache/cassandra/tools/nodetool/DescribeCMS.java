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

import java.util.Map;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "describecms", description = "Describe the current Cluster Metadata Service")
public class DescribeCMS extends NodeTool.NodeToolCmd
{
    @Override
    protected void execute(NodeProbe probe)
    {
        Map<String, String> info = probe.getCMSOperationsProxy().describeCMS();
        System.out.printf("Cluster Metadata Service:%n");
        System.out.printf("Members: %s%n", info.get("MEMBERS"));
        System.out.printf("Is Member: %s%n", info.get("IS_MEMBER"));
        System.out.printf("Service State: %s%n", info.get("SERVICE_STATE"));
        System.out.printf("Is Migrating: %s%n", info.get("IS_MIGRATING"));
        System.out.printf("Epoch: %s%n", info.get("EPOCH"));
        System.out.printf("Local Pending Count: %s%n", info.get("LOCAL_PENDING"));
        System.out.printf("Commits Paused: %s%n", info.get("COMMITS_PAUSED"));
        System.out.printf("Replication factor: %s%n", info.get("REPLICATION_FACTOR"));
    }
}
