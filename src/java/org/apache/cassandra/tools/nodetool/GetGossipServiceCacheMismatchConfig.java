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
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getgossipmismatchfixerconfig", description = "gets gossip mismatch fixer configurations")
public class GetGossipServiceCacheMismatchConfig extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("comparison enabled: " + probe.getGossipServiceCacheMismatchComparisonEnabled());
        sb.append("\ncomparison interval in seconds: " + probe.getGossipServiceCacheMismatchComparisonIntervalInSec());
        sb.append("\nfix enabled: " + probe.getGossipServiceCacheMismatchFixEnabled());
        sb.append("\nmismatch conviction threshold: " + probe.getGossipServiceCacheMismatchFixConvictionThreshold());
        System.out.println(sb);
    }
}
