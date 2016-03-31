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
import java.util.Map;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "viewbuildstatus", description = "Show progress of a materialized view build")
public class ViewBuildStatus extends NodeTool.NodeToolCmd
{
    private final static String SUCCESS = "SUCCESS";

    @Arguments(usage = "<keyspace> <view> | <keyspace.view>", description = "The keyspace and view name")
    private List<String> args = new ArrayList<>();

    protected void execute(NodeProbe probe)
    {
        String keyspace = null, view = null;
        if (args.size() == 2)
        {
            keyspace = args.get(0);
            view = args.get(1);
        }
        else if (args.size() == 1)
        {
            String[] input = args.get(0).split("\\.");
            checkArgument(input.length == 2, "viewbuildstatus requires keyspace and view name arguments");
            keyspace = input[0];
            view = input[1];
        }
        else
        {
            checkArgument(false, "viewbuildstatus requires keyspace and view name arguments");
        }

        Map<String, String> buildStatus = probe.getViewBuildStatuses(keyspace, view);
        boolean failed = false;
        TableBuilder builder = new TableBuilder();

        builder.add("Host", "Info");
        for (Map.Entry<String, String> status : buildStatus.entrySet())
        {
            if (!status.getValue().equals(SUCCESS)) {
                failed = true;
            }
            builder.add(status.getKey(), status.getValue());
        }

        if (failed) {
            System.out.println(String.format("%s.%s has not finished building; node status is below.", keyspace, view));
            System.out.println();
            builder.printTo(System.out);
            System.exit(1);
        } else {
            System.out.println(String.format("%s.%s has finished building", keyspace, view));
            System.exit(0);
        }
    }
}
