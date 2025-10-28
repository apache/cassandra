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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "checklocalmvbackfillstatus", description = "Check local materialized view backfill status")
public class CheckLocalMVBackfillStatus extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<keyspace.view>", description = "The keyspace and view name in dot notation")
    private List<String> args = new ArrayList<>();

    protected void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        
        checkArgument(args.size() == 1, "checklocalmvbackfillstatus requires keyspace.view argument");
        
        String[] input = args.get(0).split("\\.");
        checkArgument(input.length == 2, "checklocalmvbackfillstatus requires keyspace.view argument in format: keyspace_name.view_name");
        
        String keyspace = input[0];
        String view = input[1];

        try
        {
            Map<String, Object> status = probe.getLocalMVBackfillStatus(keyspace, view);
            
            Boolean primaryRangesFinished = (Boolean) status.get("primaryRangesFinished");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rangeDetails = (List<Map<String, Object>>) status.get("rangeDetails");
            
            out.println("Local MV Backfill Status for " + keyspace + "." + view);
            out.println("============================================================");
            out.println("Primary Ranges Finished: " + primaryRangesFinished);
            out.println();
            
            if (rangeDetails != null && !rangeDetails.isEmpty())
            {
                out.println("Base Table Ranges:");
                for (Map<String, Object> detail : rangeDetails)
                {
                    @SuppressWarnings("unchecked")
                    List<String> ranges = (List<String>) detail.get("ranges");
                    String rangeStatus = (String) detail.get("status");
                    
                    for (String range : ranges)
                    {
                        out.println("  " + range + " -> " + rangeStatus);
                    }
                }
            }
            else
            {
                out.println("No backfill operations found for this view.");
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error checking MV backfill status: " + e.getMessage(), e);
        }
    }
}

