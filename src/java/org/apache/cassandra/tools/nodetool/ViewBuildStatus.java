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

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "viewbuildstatus", description = "Show progress of a materialized view build")
public class ViewBuildStatus extends AbstractCommand
{
    private final static String SUCCESS = "SUCCESS";

    @CassandraUsage(usage = "<keyspace> <view> | <keyspace.view>", description = "The keyspace and view name")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", description = "Keyspace or keyspace and view in format <keyspace> <view> | <keyspace.view>", arity = "0..1")
    private String keyspaceView;

    @Parameters(index = "1", description = "View name", arity = "0..1")
    private String view;

    protected void execute(NodeProbe probe)
    {
        args = concatArgs(keyspaceView, view);
        PrintStream out = probe.output().out;
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

        if (failed)
        {
            String message = String.format("%s.%s has not finished building; node status is below.", keyspace, view);
            out.println(message);
            out.println();
            builder.printTo(out);
            throw new RuntimeException(message);
        }
        else
            out.println(String.format("%s.%s has finished building", keyspace, view));
    }
}
