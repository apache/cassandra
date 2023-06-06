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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.management.openmbean.TabularData;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.emptyMap;
import static org.apache.cassandra.config.DatabaseDescriptor.getAllDataFileLocations;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

public class ClearSnapshotTest extends CQLTester
{
    private static final Pattern DASH_PATTERN = Pattern.compile("-");
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        requireNetwork();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testClearSnapshot_RemoveByName()
    {
        ToolResult tool = invokeNodetool("snapshot", "-t", "some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails(emptyMap());
        assertThat(snapshots_before).containsKey("some-name");

        tool = invokeNodetool("clearsnapshot", "-t", "some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails(emptyMap());
        assertThat(snapshots_after).doesNotContainKey("some-name");
    }

    @Test
    public void testClearSnapshot_RemoveMultiple()
    {
        ToolResult tool = invokeNodetool("snapshot", "-t", "some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        tool = invokeNodetool("snapshot", "-t", "some-other-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails(emptyMap());
        assertThat(snapshots_before).hasSize(2);

        tool = invokeNodetool("clearsnapshot", "--all");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails(emptyMap());
        assertThat(snapshots_after).isEmpty();
    }

    @Test
    public void testClearSnapshotWithOlderThanFlag() throws Throwable
    {
        Instant start = Instant.ofEpochMilli(currentTimeMillis());
        prepareData(start);

        // wait 10 seconds for the sake of the test
        await().timeout(15, TimeUnit.SECONDS).until(() -> Instant.now().isAfter(start.plusSeconds(10)));

        // clear all snapshots for specific keyspace older than 3 hours for a specific keyspace
        invokeNodetool("clearsnapshot", "--older-than", "3h", "--all", "--", KEYSPACE).assertOnCleanExit();

        await().until(() -> {
            String output = invokeNodetool("listsnapshots").getStdout();
            return !output.contains("snapshot-to-clear-ks1-tb1") &&
                   output.contains("some-other-snapshot-ks1-tb1") &&
                   output.contains("last-snapshot-ks1-tb1") &&
                   output.contains("snapshot-to-clear-ks2-tb2") &&
                   output.contains("some-other-snapshot-ks2-tb2") &&
                   output.contains("last-snapshot-ks2-tb2");
        });

        // clear all snapshots older than 2 hours for all keyspaces
        invokeNodetool("clearsnapshot", "--older-than", "2h", "--all").assertOnCleanExit();

        await().until(() -> {
            String output = invokeNodetool("listsnapshots").getStdout();

            return !output.contains("some-other-snapshot-ks1-tb1") &&
                   output.contains("last-snapshot-ks1-tb1") &&
                   !output.contains("snapshot-to-clear-ks2-tb2") &&
                   !output.contains("some-other-snapshot-ks2-tb2") &&
                   output.contains("last-snapshot-ks2-tb2");
        });

        // clear all snapshosts older than 1 second
        invokeNodetool("clearsnapshot", "--older-than", "1s", "--all", "--", currentKeyspace()).assertOnCleanExit();

        await().until(() -> {
            String output = invokeNodetool("listsnapshots").getStdout();
            return output.contains("last-snapshot-ks1-tb1") &&
                   !output.contains("last-snapshot-ks2-tb2");
        });

        invokeNodetool("clearsnapshot", "--older-than", "1s", "--all").assertOnCleanExit();
        await().until(() -> !invokeNodetool("listsnapshots").getStdout().contains("last-snapshot-ks1-tb1"));
    }


    @Test
    public void testClearSnapshotWithOlderThanTimestampFlag() throws Throwable
    {
        Instant start = Instant.ofEpochMilli(currentTimeMillis());
        prepareData(start);

        // wait 10 seconds for the sake of the test
        await().timeout(15, TimeUnit.SECONDS).until(() -> Instant.now().isAfter(start.plusSeconds(10)));

        // clear all snapshots for specific keyspace older than 3 hours for a specific keyspace
        invokeNodetool("clearsnapshot", "--older-than-timestamp",
                       Instant.now().minus(3, HOURS).toString(),
                       "--all", "--", KEYSPACE).assertOnCleanExit();

        await().until(() -> {
            String output = invokeNodetool("listsnapshots").getStdout();
            return !output.contains("snapshot-to-clear-ks1-tb1") &&
                   output.contains("some-other-snapshot-ks1-tb1") &&
                   output.contains("last-snapshot-ks1-tb1") &&
                   output.contains("snapshot-to-clear-ks2-tb2") &&
                   output.contains("some-other-snapshot-ks2-tb2") &&
                   output.contains("last-snapshot-ks2-tb2");
        });

        // clear all snapshots older than 2 hours for all keyspaces
        invokeNodetool("clearsnapshot", "--older-than-timestamp",
                       Instant.now().minus(2, HOURS).toString(),
                       "--all").assertOnCleanExit();

        await().until(() -> {
            String output = invokeNodetool("listsnapshots").getStdout();

            return !output.contains("some-other-snapshot-ks1-tb1") &&
                   output.contains("last-snapshot-ks1-tb1") &&
                   !output.contains("snapshot-to-clear-ks2-tb2") &&
                   !output.contains("some-other-snapshot-ks2-tb2") &&
                   output.contains("last-snapshot-ks2-tb2");
        });

        // clear all snapshots older than now for all keyspaces
        invokeNodetool("clearsnapshot", "--older-than-timestamp",
                       Instant.now().toString(),
                       "--all").assertOnCleanExit();

        await().until(() -> {
            String output = invokeNodetool("listsnapshots").getStdout();
            return !output.contains("last-snapshot-ks1-tb1") &&
                   !output.contains("last-snapshot-ks2-tb2");
        });
    }

    @Test
    public void testIncompatibleFlags()
    {
        ToolResult invalidCommand1 = invokeNodetool("clearsnapshot",
                                                    "--older-than-timestamp", Instant.now().toString(),
                                                    "--older-than", "3h",
                                                    "--all");
        invalidCommand1.asserts().failure();
        assertTrue(invalidCommand1.getStdout().contains("Specify only one of --older-than or --older-than-timestamp"));

        ToolResult invalidCommand2 = invokeNodetool("clearsnapshot", "-t", "some-snapshot-tag", "--all");
        invalidCommand2.asserts().failure();
        assertTrue(invalidCommand2.getStdout().contains("Specify only one of snapshot name or --all"));

        ToolResult invalidCommand3 = invokeNodetool("clearsnapshot", "--", "keyspace");
        invalidCommand3.asserts().failure();
        assertTrue(invalidCommand3.getStdout().contains("Specify snapshot name or --all"));

        ToolResult invalidCommand4 = invokeNodetool("clearsnapshot",
                                                    "--older-than-timestamp", Instant.now().toString(),
                                                    "-t", "some-snapshot-tag");
        invalidCommand4.asserts().failure();
        assertTrue(invalidCommand4.getStdout().contains("Specifying snapshot name together with --older-than-timestamp flag is not allowed"));

        ToolResult invalidCommand5 = invokeNodetool("clearsnapshot",
                                                    "--older-than", "3h",
                                                    "-t", "some-snapshot-tag");
        invalidCommand5.asserts().failure();
        assertTrue(invalidCommand5.getStdout().contains("Specifying snapshot name together with --older-than flag is not allowed"));

        ToolResult invalidCommand6 = invokeNodetool("clearsnapshot",
                                                    "--older-than-timestamp", "123",
                                                    "--all", "--", "somekeyspace");
        invalidCommand6.asserts().failure();
        assertTrue(invalidCommand6.getStdout().contains("Parameter --older-than-timestamp has to be a valid instant in ISO format."));

        ToolResult invalidCommand7 = invokeNodetool("clearsnapshot",
                                                    "--older-than", "3k",
                                                    "--all", "--", "somekeyspace");
        invalidCommand7.asserts().failure();
        assertTrue(invalidCommand7.getStdout().contains("Invalid duration: 3k"));
    }

    private void rewriteManifest(String tableId,
                                 String[] dataDirs,
                                 String keyspace,
                                 String tableName,
                                 String snapshotName,
                                 Instant createdAt) throws Exception
    {
        Path manifestPath = findManifest(dataDirs, keyspace, tableId, tableName, snapshotName);
        SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(new File(manifestPath));
        SnapshotManifest manifestWithEphemeralFlag = new SnapshotManifest(manifest.files, null, createdAt, false);
        manifestWithEphemeralFlag.serializeToJsonFile(new File(manifestPath));
    }

    private Path findManifest(String[] dataDirs, String keyspace, String tableId, String tableName, String snapshotName)
    {
        for (String dataDir : dataDirs)
        {
            Path manifest = Paths.get(dataDir)
                                 .resolve(keyspace)
                                 .resolve(format("%s-%s", tableName, tableId))
                                 .resolve("snapshots")
                                 .resolve(snapshotName)
                                 .resolve("manifest.json");

            if (Files.exists(manifest))
            {
                return manifest;
            }
        }

        throw new IllegalStateException("Unable to find manifest!");
    }

    private void prepareData(Instant start) throws Throwable
    {
        String tableName = createTable(KEYSPACE, "CREATE TABLE %s (id int primary key)");
        execute("INSERT INTO %s (id) VALUES (?)", 1);
        flush(KEYSPACE);

        String keyspace2 = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        String tableName2 = createTable(keyspace2, "CREATE TABLE %s (id int primary key)");
        execute(formatQuery(keyspace2, "INSERT INTO %s (id) VALUES (?)"), 1);
        flush(keyspace2);

        invokeNodetool("snapshot", "-t", "snapshot-to-clear-ks1-tb1", "-cf", tableName, "--", KEYSPACE).assertOnCleanExit();
        invokeNodetool("snapshot", "-t", "some-other-snapshot-ks1-tb1", "-cf", tableName, "--", KEYSPACE).assertOnCleanExit();
        invokeNodetool("snapshot", "-t", "last-snapshot-ks1-tb1", "-cf", tableName, "--", KEYSPACE).assertOnCleanExit();

        invokeNodetool("snapshot", "-t", "snapshot-to-clear-ks2-tb2", "-cf", tableName2, "--", keyspace2).assertOnCleanExit();
        invokeNodetool("snapshot", "-t", "some-other-snapshot-ks2-tb2", "-cf", tableName2, "--", keyspace2).assertOnCleanExit();
        invokeNodetool("snapshot", "-t", "last-snapshot-ks2-tb2", "-cf", tableName2, "--", keyspace2).assertOnCleanExit();

        Optional<TableMetadata> tableMetadata = Keyspace.open(KEYSPACE).getMetadata().tables.get(tableName);
        Optional<TableMetadata> tableMetadata2 = Keyspace.open(keyspace2).getMetadata().tables.get(tableName2);

        String tableId = DASH_PATTERN.matcher(tableMetadata.orElseThrow(() -> new IllegalStateException(format("no metadata found for %s.%s", KEYSPACE, tableName)))
                                              .id.asUUID().toString()).replaceAll("");

        String tableId2 = DASH_PATTERN.matcher(tableMetadata2.orElseThrow(() -> new IllegalStateException(format("no metadata found for %s.%s", keyspace2, tableName2)))
                                               .id.asUUID().toString()).replaceAll("");

        rewriteManifest(tableId, getAllDataFileLocations(), KEYSPACE, tableName, "snapshot-to-clear-ks1-tb1", start.minus(5, HOURS));
        rewriteManifest(tableId, getAllDataFileLocations(), KEYSPACE, tableName, "some-other-snapshot-ks1-tb1", start.minus(2, HOURS));
        rewriteManifest(tableId, getAllDataFileLocations(), KEYSPACE, tableName, "last-snapshot-ks1-tb1", start.minus(1, SECONDS));
        rewriteManifest(tableId2, getAllDataFileLocations(), keyspace2, tableName2, "snapshot-to-clear-ks2-tb2", start.minus(5, HOURS));
        rewriteManifest(tableId2, getAllDataFileLocations(), keyspace2, tableName2, "some-other-snapshot-ks2-tb2", start.minus(2, HOURS));
        rewriteManifest(tableId2, getAllDataFileLocations(), keyspace2, tableName2, "last-snapshot-ks2-tb2", start.minus(1, SECONDS));
    }
}
