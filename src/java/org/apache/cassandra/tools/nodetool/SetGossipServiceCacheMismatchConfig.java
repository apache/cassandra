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
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setgossipmismatchfixerconfig", description = "sets gossip mismatch fixer configurations")
public class SetGossipServiceCacheMismatchConfig extends NodeToolCmd
{
    @Arguments(title = "<gossipservicecachemismatchparam> <value>", usage = "<gossipservicecachemismatchparam> <value>",
            description = "gossipservicecachemismatchparam param and value.\nPossible gossipservicecachemismatchparam parameters are as following: " +
                    "[compare_gossip_and_storage_service_cache|gossip_and_storage_service_cache_comparison_interval|sync_gossip_and_storage_service_cache_if_mismatch|gossip_and_storage_service_cache_mismatch_conviction_threshold]",
            required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "gossipservicecachemismatchparam requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        if (paramType.equals("compare_gossip_and_storage_service_cache"))
        {
            probe.setCompareGossipAndStorageServiceCache(Boolean.parseBoolean(paramVal));
        }
        else if (paramType.equals("gossip_and_storage_service_cache_comparison_interval"))
        {
            probe.setGossipAndStorageServiceCacheComparisonInterval(Integer.parseInt(paramVal), TimeUnit.SECONDS);
        }
        else if (paramType.equals("sync_gossip_and_storage_service_cache_if_mismatch"))
        {
            probe.setSyncGossipAndStorageServiceCacheIfMismatched(Boolean.parseBoolean(paramVal));
        }
        else if (paramType.equals("gossip_and_storage_service_cache_mismatch_conviction_threshold"))
        {
            probe.setGossipAndStorageServiceCacheMismatchConvictionThreshold(Integer.parseInt(paramVal));
        }
    }
}
