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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class TpStatsJsonTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    // ── 1. command exits cleanly and stdout is parseable JSON ────────────
    @Test
    public void testOutputIsValidJson()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tpstatsjson");
        tool.assertOnCleanExit();
        assertThat(parseRoot(tool.getStdout())).isNotNull();
    }

    // ── 2. top-level shape and per-pool metric fields ────────────────────
    @Test
    public void testStructure()
    {
        ObjectNode root = runAndParse();

        // A healthy node produces exactly these two keys; no errors key.
        assertThat(collectFieldNames(root))
                .containsExactlyInAnyOrder("thread_pools", "dropped_messages");

        ObjectNode pools = (ObjectNode) root.get("thread_pools");
        assertThat(pools.size()).isGreaterThan(0);

        // Every pool must expose exactly the five required metric fields.
        pools.fields().forEachRemaining(entry ->
        {
            List<String> poolKeys = collectFieldNames((ObjectNode) entry.getValue());
            assertThat(poolKeys)
                    .containsExactlyInAnyOrder("active", "pending", "completed",
                                               "blocked", "all_time_blocked");
        });

        // dropped_messages is present (may be empty on a fresh node, that is fine).
        assertThat(root.has("dropped_messages")).isTrue();
    }

    // ── 3. every object-node in the tree has alphabetically sorted keys ──
    @Test
    public void testKeyOrderingIsDeterministic()
    {
        assertKeysSorted(runAndParse());
    }

    // ── 4. a write + flush visibly advances at least one completed counter ─
    @Test
    public void testDataChangesAreVisible() throws Throwable
    {
        ObjectNode before = runAndParse();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, c int)");
        execute("INSERT INTO %s (pk, c) VALUES (?, ?)", 1, 1);
        flush();

        ObjectNode after = runAndParse();

        // At least one thread pool's counters must have advanced.
        assertThat(after.get("thread_pools")).isNotEqualTo(before.get("thread_pools"));
    }

    // ── 5. N/A normalisation — the main contract for partially missing data ─
    /**
     * Exercises the {@code "N/A"} -&gt; {@code null} normalisation path
     * end-to-end through the exact same {@link TpStatsJson#WRITER} the
     * command uses, with fully controlled input.
     * <p>
     * Setup: two pools.  {@code ReadStage} has {@code "N/A"} on {@code blocked}
     * and realistic numbers everywhere else.  {@code MutationStage} is
     * entirely populated.  A single {@code dropped_messages} entry is included.
     * <p>
     * Assertions
     * <ol>
     *   <li>The {@code blocked} field in {@code ReadStage} is JSON {@code null}.</li>
     *   <li>All four sibling fields in {@code ReadStage} carry their expected values.</li>
     *   <li>{@code MutationStage} is completely intact — nothing bled across pools.</li>
     *   <li>{@code dropped_messages} serialised correctly.</li>
     *   <li>No {@code errors} key is present — {@code "N/A"} is normalisation,
     *       not a failure, so it must not pollute the error map.</li>
     *   <li>Every object-node in the tree has its keys in alphabetical order.</li>
     * </ol>
     */
    @Test
    public void testPartiallyMissingMetricsProduceNullWithRestIntact() throws IOException
    {
        // ── build the map exactly as execute() would ─────────────────────
        Map<String, Object> readStage = new HashMap<>();
        readStage.put("active",           TpStatsJson.normalizeMetricValue(42));
        readStage.put("pending",          TpStatsJson.normalizeMetricValue(0));
        readStage.put("completed",        TpStatsJson.normalizeMetricValue(1000L));
        readStage.put("blocked",          TpStatsJson.normalizeMetricValue("N/A")); // ← missing MBean
        readStage.put("all_time_blocked", TpStatsJson.normalizeMetricValue(7L));

        Map<String, Object> mutationStage = new HashMap<>();
        mutationStage.put("active",           TpStatsJson.normalizeMetricValue(1));
        mutationStage.put("pending",          TpStatsJson.normalizeMetricValue(0));
        mutationStage.put("completed",        TpStatsJson.normalizeMetricValue(500L));
        mutationStage.put("blocked",          TpStatsJson.normalizeMetricValue(0));
        mutationStage.put("all_time_blocked", TpStatsJson.normalizeMetricValue(2L));

        Map<String, Object> pools = new HashMap<>();
        pools.put("ReadStage",     readStage);
        pools.put("MutationStage", mutationStage);

        Map<String, Object> root = new HashMap<>();
        root.put("thread_pools",     pools);
        root.put("dropped_messages", Collections.singletonMap("MUTATION_REQ", 3));
        // Deliberately no "errors" key — N/A is not an error.

        // ── serialise with the command's ObjectWriter ─────────────────────
        ObjectNode parsed = parseRoot(TpStatsJson.WRITER.writeValueAsString(root));

        // (6) key ordering holds at every level
        assertKeysSorted(parsed);

        // (5) no errors key
        assertThat(parsed.has("errors")).isFalse();

        // (1) the N/A field is null
        ObjectNode rs = (ObjectNode) parsed.get("thread_pools").get("ReadStage");
        assertThat(rs.get("blocked").isNull()).isTrue();

        // (2) siblings in ReadStage are present and correct
        assertThat(rs.get("active").asInt()).isEqualTo(42);
        assertThat(rs.get("pending").asInt()).isEqualTo(0);
        assertThat(rs.get("completed").asLong()).isEqualTo(1000L);
        assertThat(rs.get("all_time_blocked").asLong()).isEqualTo(7L);

        // (3) MutationStage is completely intact
        ObjectNode ms = (ObjectNode) parsed.get("thread_pools").get("MutationStage");
        assertThat(ms.get("active").asInt()).isEqualTo(1);
        assertThat(ms.get("pending").asInt()).isEqualTo(0);
        assertThat(ms.get("completed").asLong()).isEqualTo(500L);
        assertThat(ms.get("blocked").asInt()).isEqualTo(0);
        assertThat(ms.get("all_time_blocked").asLong()).isEqualTo(2L);

        // (4) dropped_messages came through
        assertThat(parsed.get("dropped_messages").get("MUTATION_REQ").asInt()).isEqualTo(3);
    }

    // ── 6. errors key serialises correctly and sorts with the rest ────────
    /**
     * When {@code execute()} catches a section-level failure it adds an entry
     * to the {@code errors} map.  This test constructs exactly that shape and
     * verifies: the key is present, its content is intact, and it sorts
     * alphabetically between {@code dropped_messages} and {@code thread_pools}
     * as required by the determinism contract.
     */
    @Test
    public void testErrorsFieldAppearsAndIsSortedWhenSectionFails() throws IOException
    {
        Map<String, Object> root = new HashMap<>();
        root.put("thread_pools",     Collections.emptyMap());
        root.put("dropped_messages", Collections.emptyMap());
        root.put("errors", Collections.singletonMap("thread_pools",
                Collections.singletonList("failed to enumerate pools: connection refused")));

        ObjectNode parsed = parseRoot(TpStatsJson.WRITER.writeValueAsString(root));

        // errors sorts between dropped_messages and thread_pools alphabetically.
        List<String> topKeys = collectFieldNames(parsed);
        assertThat(topKeys).isEqualTo(List.of("dropped_messages", "errors", "thread_pools"));

        // Content is reachable and correct.
        assertThat(parsed.get("errors").get("thread_pools").get(0).asText())
                .contains("connection refused");
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /** Invoke the command, assert clean exit, and return the parsed root node. */
    private static ObjectNode runAndParse()
    {
        ToolRunner.ToolResult result = ToolRunner.invokeNodetool("tpstatsjson");
        result.assertOnCleanExit();
        return parseRoot(result.getStdout());
    }

    private static ObjectNode parseRoot(String json)
    {
        try
        {
            return (ObjectNode) JsonUtils.JSON_OBJECT_MAPPER.readTree(json);
        }
        catch (IOException e)
        {
            throw new AssertionError("tpstatsjson output is not valid JSON", e);
        }
    }

    private static List<String> collectFieldNames(ObjectNode node)
    {
        List<String> keys = new ArrayList<>();
        node.fieldNames().forEachRemaining(keys::add);
        return keys;
    }

    /**
     * Recursively asserts that every {@link ObjectNode} in the tree has its
     * field names in natural (alphabetical) order.  This is the precise
     * definition of "deterministic output" for this command.
     */
    private static void assertKeysSorted(ObjectNode node)
    {
        List<String> keys   = collectFieldNames(node);
        List<String> sorted = new ArrayList<>(keys);
        Collections.sort(sorted);
        assertThat(keys).as("field names of " + node.getClass().getSimpleName())
                        .isEqualTo(sorted);

        // Recurse into every child that is itself an object.
        node.fields().forEachRemaining(entry ->
        {
            if (entry.getValue().isObject())
                assertKeysSorted((ObjectNode) entry.getValue());
        });
    }
}
