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

import com.google.common.annotations.VisibleForTesting;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Allows to set Query Analytics configuration through nodetool.
 */
@Command(name = "setqueryanalyticsconfig", description = "sets the query analytics configuration")
public class SetQueryAnalyticsConfig extends NodeToolCmd
{
    @VisibleForTesting
    @Arguments(title = "<param> <value>", usage = "<param> <value>",
    description = "Query analytics param and value.\nPossible parameters are: " +
                  "[enabled]",
    required = true)
    protected List<String> args = new ArrayList<>();

    @VisibleForTesting
    protected PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setqueryanalyticsconfig requires param and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        switch (paramType)
        {
            case "enabled":
                boolean enabledValue = Boolean.parseBoolean(paramVal);
                probe.setQueryAnalyticsEnabled(enabledValue);
                out.println("Query Analytics enabled: " + paramVal);
                
                // Warn if enabling QAN but no producer is configured
                if (enabledValue) {
                    String config = probe.queryAnalyticsConfiguration();
                    if (config.contains("producer:") && config.contains("class_name:")) {
                        // Check if class_name is empty or null after the colon
                        String[] lines = config.split("\n");
                        for (String line : lines) {
                            if (line.trim().startsWith("class_name:")) {
                                String className = line.substring(line.indexOf(':') + 1).trim();
                                if (className.isEmpty() || "null".equals(className)) {
                                    out.println("WARNING: QueryAnalytics is enabled but no producer is configured. Metrics will not be sent.");
                                }
                                break;
                            }
                        }
                    } else {
                        out.println("WARNING: QueryAnalytics is enabled but no producer is configured. Metrics will not be sent.");
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown parameter: " + paramType + 
                    ". Valid parameters: enabled");
        }
    }
}
