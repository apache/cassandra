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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@Command(name = "mvbackfill", description = "Perform materialized view backfill for specified base table ranges")
public class MVBackfill extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<keyspace.view>", description = "The keyspace and view name in dot notation")
    private List<String> args = new ArrayList<>();

    @Option(title = "start_token", name = {"-st", "--start-token"}, 
            description = "Use -st to specify a token at which the backfill range starts (exclusive)")
    private String startToken = EMPTY;

    @Option(title = "end_token", name = {"-et", "--end-token"}, 
            description = "Use -et to specify a token at which backfill range ends (inclusive)")
    private String endToken = EMPTY;

    @Option(title = "force_restart", name = {"-fr", "--force-restart"}, 
            description = "Use -fr to force restart the backfill from the beginning, ignoring previous progress")
    private boolean forceRestart = false;

    @Override
    protected void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        
        // Parse keyspace and view name
        checkArgument(args.size() == 1, "mvbackfill requires keyspace.view argument");
        
        String[] input = args.get(0).split("\\.");
        checkArgument(input.length == 2, "mvbackfill requires keyspace.view argument in format: keyspace_name.view_name");
        
        String keyspace = input[0];
        String view = input[1];

        // Validate range options
        boolean hasRangeSpec = !startToken.isEmpty() || !endToken.isEmpty();

        if (hasRangeSpec && (startToken.isEmpty() || endToken.isEmpty()))
        {
            throw new IllegalArgumentException("Both start token (-st) and end token (-et) must be specified when using range-based backfill");
        }

        try
        {
            String rangeSpec = null;
            if (hasRangeSpec)
            {
                rangeSpec = startToken + ":" + endToken;
            }

            out.println(String.format("Starting MV backfill for %s.%s", keyspace, view));
            if (forceRestart)
            {
                out.println("Force restart enabled - backfill will start from the beginning");
            }
            
            if (rangeSpec != null)
            {
                out.println(String.format("Backfilling token range: %s", rangeSpec));
                probe.mvBackfillWithRanges(keyspace, view, rangeSpec, forceRestart);
            }
            else
            {
                out.println("Backfilling primary ranges (default behavior)");
                probe.mvBackfillPrimaryRange(keyspace, view, forceRestart);
            }
            
            out.println(String.format("MV backfill for %s.%s completed successfully", keyspace, view));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error occurred during MV backfill: " + e.getMessage(), e);
        }
    }
}

