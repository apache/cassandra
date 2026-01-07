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

import com.google.common.annotations.VisibleForTesting;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Allows to set ViewKeyRebuild configuration through nodetool.
 */
@Command(name = "setviewkeyrebuildconfig", description = "Sets the view key rebuild configuration")
public class SetViewKeyRebuildConfig extends NodeToolCmd
{
    @VisibleForTesting
    @Arguments(title = "<param> <value>", usage = "<param> <value>",
    description = "View key rebuild param and value.\nPossible parameters are: " +
                  "[rebuild_on_deletion_enabled, apply_mutations_enabled, verbose_logging_enabled, view_read_enabled]",
    required = true)
    protected List<String> args = new ArrayList<>();

    @VisibleForTesting
    protected PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setviewkeyrebuildconfig requires param and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        switch (paramType)
        {
            case "rebuild_on_deletion_enabled":
                probe.setViewKeyRebuildOnDeletionEnabled(Boolean.parseBoolean(paramVal));
                out.println("View key rebuild_on_deletion_enabled set to: " + paramVal);
                break;
            case "apply_mutations_enabled":
                probe.setViewKeyRebuildApplyMutationsEnabled(Boolean.parseBoolean(paramVal));
                out.println("View key apply_mutations_enabled set to: " + paramVal);
                break;
            case "verbose_logging_enabled":
                probe.setViewKeyRebuildVerboseLoggingEnabled(Boolean.parseBoolean(paramVal));
                out.println("View key verbose_logging_enabled set to: " + paramVal);
                break;
            case "view_read_enabled":
                probe.setViewKeyRebuildViewReadEnabled(Boolean.parseBoolean(paramVal));
                out.println("View key view_read_enabled set to: " + paramVal);
                break;
            default:
                throw new IllegalArgumentException("Unknown parameter: " + paramType +
                    ". Valid parameters: rebuild_on_deletion_enabled, apply_mutations_enabled, verbose_logging_enabled, view_read_enabled");
        }
    }
}
