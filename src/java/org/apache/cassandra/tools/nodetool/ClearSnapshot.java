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
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "clearsnapshot", description = "Remove the snapshot with the given name from the given keyspaces")
public class ClearSnapshot extends NodeToolCmd
{
    @Arguments(usage = "[<keyspaces>...] ", description = "Remove snapshots from the given keyspaces")
    private List<String> keyspaces = new ArrayList<>();

    @Option(title = "snapshot_name", name = "-t", description = "Remove the snapshot with a given name")
    private String snapshotName = EMPTY;

    @Option(title = "clear_all_snapshots", name = "--all", description = "Removes all snapshots")
    private boolean clearAllSnapshots = false;

    @Option(title = "older_than", name = "--older-than", description = "Clear snapshots older than specified time period.")
    private String olderThan;

    @Option(title = "older_than_timestamp", name = "--older-than-timestamp",
    description = "Clear snapshots older than specified timestamp. It has to be a string in ISO format, for example '2022-12-03T10:15:30Z'")
    private String olderThanTimestamp;

    @Override
    public void execute(NodeProbe probe)
    {
        if (snapshotName.isEmpty() && !clearAllSnapshots)
            throw new IllegalArgumentException("Specify snapshot name or --all");

        if (!snapshotName.isEmpty() && clearAllSnapshots)
            throw new IllegalArgumentException("Specify only one of snapshot name or --all");

        if (olderThan != null && olderThanTimestamp != null)
            throw new IllegalArgumentException("Specify only one of --older-than or --older-than-timestamp");

        if (!snapshotName.isEmpty() && olderThan != null)
            throw new IllegalArgumentException("Specifying snapshot name together with --older-than flag is not allowed");

        if (!snapshotName.isEmpty() && olderThanTimestamp != null)
            throw new IllegalArgumentException("Specifying snapshot name together with --older-than-timestamp flag is not allowed");

        if (olderThanTimestamp != null)
            try
            {
                Instant.parse(olderThanTimestamp);
            }
            catch (DateTimeParseException ex)
            {
                throw new IllegalArgumentException("Parameter --older-than-timestamp has to be a valid instant in ISO format.");
            }

        Long olderThanInSeconds = null;
        if (olderThan != null)
            // fail fast when it is not valid
            olderThanInSeconds = new DurationSpec.LongSecondsBound(olderThan).toSeconds();

        StringBuilder sb = new StringBuilder();

        sb.append("Requested clearing snapshot(s) for ");

        if (keyspaces.isEmpty())
            sb.append("[all keyspaces]");
        else
            sb.append('[').append(join(keyspaces, ", ")).append(']');

        if (snapshotName.isEmpty())
            sb.append(" with [all snapshots]");
        else
            sb.append(" with snapshot name [").append(snapshotName).append(']');

        if (olderThanInSeconds != null)
            sb.append(" older than ")
              .append(olderThanInSeconds)
              .append(" seconds.");

        if (olderThanTimestamp != null)
            sb.append(" older than timestamp ").append(olderThanTimestamp);

        probe.output().out.println(sb);

        try
        {
            Map<String, Object> parameters = new HashMap<>();
            if (olderThan != null)
                parameters.put("older_than", olderThan);
            if (olderThanTimestamp != null)
                parameters.put("older_than_timestamp", olderThanTimestamp);

            probe.clearSnapshot(parameters, snapshotName, toArray(keyspaces, String.class));
        } catch (IOException e)
        {
            throw new RuntimeException("Error during clearing snapshots", e);
        }
    }
}
