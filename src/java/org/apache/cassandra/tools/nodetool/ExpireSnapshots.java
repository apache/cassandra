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
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.time.Instant;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.CompositeData;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "expiresnapshots", description = "Removes snapshots that are older than a TTL in days")
public class ExpireSnapshots extends NodeToolCmd
{
    @Option(title = "ttl", name = {"-t", "--ttl"}, description = "TTL (in days) to expire snapshots", required = true)
    private int ttl = -1;

    @Option(title = "dry-run", name = { "--dry-run" }, description = "Run tool without actually deleting snapshots")
    boolean dryRun = false;

    private final static String timestampRegex ="^\\S*(?<timestamp>\\d{13})\\S*$";
    private final static Pattern timestampPattern = Pattern.compile(timestampRegex);

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        try
        {
            out.println(String.format("Starting %s of snapshots older than %s days", dryRun ? "simulated cleanup" : "cleanup", ttl));

            if (ttl <= 0) {
                throw new RuntimeException("ttl must be greater than 0");
            }

            int snapshotsCleaned = 0;

            final Instant ttlInstant = getTtlInstant(ttl);

            final Map<String,TabularData> snapshotDetails = probe.getSnapshotDetails();
            if (snapshotDetails.isEmpty())
            {
                out.println("There are no snapshots");
                return;
            }

            List<Map<String, String>> snapshots = tabularDataToList(snapshotDetails);

            for (Map<String, String> snapshot: snapshots) {

                Instant snapDate;

                String snapshotName = snapshot.get("Snapshot name");
                snapDate = getInstantFromSnapshotName(snapshotName);

                if (snapDate == Instant.MIN) {
                    out.println(String.format("Unable to find timestamp in snapshot name. Snapshot Name: %s Pattern: %s", snapshotName, timestampRegex));
                    continue;
                }

                if (snapDate.isBefore(ttlInstant)) {
                    out.println(String.format("Snapshot %s older than %s days", snapshotName, ttl));

                if (!dryRun) {
                    probe.clearSnapshot(snapshotName);
                }

                snapshotsCleaned++;
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error during expiresnapshots", e);
        }
    }

    private static Instant getTtlInstant(int ttl) {
        return LocalDateTime.now().minusDays(ttl).toInstant(ZoneOffset.UTC);
    }

    public static Instant getInstantFromSnapshotName(String snapshotName) {
        Matcher matcher = timestampPattern.matcher(snapshotName);
        if (matcher.find()) {
            return Instant.ofEpochMilli(Long.parseLong(matcher.group("timestamp")));
        } else {
            return Instant.MIN;
        }
    }

    public static List<Map<String, String>> tabularDataToList(Map<String, TabularData> input) {

        List<Map<String, String>> output = new ArrayList<Map<String, String>>();

        if (input.isEmpty()) {
            return output;
        }

        List<String> indexNames = input.entrySet().iterator().next().getValue().getTabularType().getIndexNames();

        input.forEach((key, value) -> value.values().forEach((detail) -> {
            final CompositeData compositeDetail = (CompositeData) detail;
            final HashMap<String, String> newMap = new HashMap<>();
            indexNames.forEach((index) -> newMap.put(index, (String) compositeDetail.get(index)));
            output.add(newMap);
        }));

        return output;
    }
}
