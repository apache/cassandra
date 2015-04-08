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

import static com.google.common.collect.Iterables.toArray;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.join;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "clearsnapshot", description = "Remove the snapshot with the given name from the given keyspaces. If no snapshotName is specified we will remove all snapshots")
public class ClearSnapshot extends NodeToolCmd
{
    @Arguments(usage = "[<keyspaces>...] ", description = "Remove snapshots from the given keyspaces")
    private List<String> keyspaces = new ArrayList<>();

    @Option(title = "snapshot_name", name = "-t", description = "Remove the snapshot with a given name")
    private String snapshotName = EMPTY;

    @Override
    public void execute(NodeProbe probe)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("Requested clearing snapshot(s) for ");

        if (keyspaces.isEmpty())
            sb.append("[all keyspaces]");
        else
            sb.append("[").append(join(keyspaces, ", ")).append("]");

        if (!snapshotName.isEmpty())
            sb.append(" with snapshot name [").append(snapshotName).append("]");

        System.out.println(sb.toString());

        try
        {
            probe.clearSnapshot(snapshotName, toArray(keyspaces, String.class));
        } catch (IOException e)
        {
            throw new RuntimeException("Error during clearing snapshots", e);
        }
    }
}