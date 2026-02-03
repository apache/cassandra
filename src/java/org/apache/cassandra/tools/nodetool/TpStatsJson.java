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

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.JsonUtils;

import picocli.CommandLine.Command;

/**
 * Prints thread-pool and dropped-message statistics as a single JSON object
 * to standard output.  Every key at every nesting level is sorted
 * alphabetically, making the output safe to {@code diff} in CI without
 * post-processing.
 *
 * <h4>Output format — pretty-printed, not compact</h4>
 * <p>
 * Pretty-printing is chosen deliberately.  The primary consumer of this
 * command is CI regression checks in which the output of two runs is diffed.
 * With pretty JSON each value lives on its own line; a single counter
 * changing from {@code 5} to {@code 6} produces a one-line diff.  Compact
 * (single-line) JSON collapses the entire document into one line, turning
 * every change into a full-document replacement and making diffs
 * uninformative.  Jackson's {@code DefaultPrettyPrinter} is deterministic
 * across platforms: consistent two-space indentation, no trailing
 * whitespace, and platform-independent line endings when written to a
 * {@code PrintStream}.  Users who need compact output for transport or
 * storage can pipe through {@code jq -c}.
 * </p>
 *
 * <h4>Key ordering</h4>
 * <p>
 * {@link SerializationFeature#ORDER_MAP_ENTRIES_BY_KEYS} is enabled on a
 * command-local {@link ObjectWriter}.  It recursively sorts every
 * {@code Map&lt;String, …&gt;} during serialization.  The shared
 * {@link JsonUtils#JSON_OBJECT_MAPPER} is never mutated.
 * </p>
 *
 * <h4>Error handling — two tiers, nothing silent</h4>
 * <p>
 * The two data sections ({@code thread_pools} and {@code dropped_messages})
 * are fetched independently so that a failure in one does not suppress the
 * other.  Within {@code thread_pools} each individual metric is fetched
 * inside its own try-block so that one bad gauge does not kill the whole
 * pool entry.  Any failure is recorded — never silently swallowed — in a
 * top-level {@code errors} object that maps section names to a list of
 * human-readable messages.  The {@code errors} key appears in the output
 * only when at least one failure actually occurred; a clean run produces no
 * trace of error-handling machinery.
 * </p>
 *
 * <h4>N/A normalisation</h4>
 * <p>
 * {@link NodeProbe#getThreadPoolMetric} returns the string {@code "N/A"}
 * when the backing MBean is not registered.  This sentinel is converted to
 * JSON {@code null} so that downstream parsers see a typed absence rather
 * than a magic string.  This is data normalisation, not an error; it does
 * not populate the {@code errors} map.
 * </p>
 */
@Command(name = "tpstatsjson",
         description = "Print thread pool and dropped message statistics as deterministic JSON")
public class TpStatsJson extends AbstractCommand
{
    // Pretty-print + sorted keys.  Rationale for pretty-printing is in the
    // class javadoc above.  ORDER_MAP_ENTRIES_BY_KEYS recurses into nested
    // maps automatically; no TreeMap is required anywhere in the code.
    // Package-private so that the test suite can serialise controlled maps
    // through the exact same pipeline the command uses.
    static final ObjectWriter WRITER = JsonUtils.JSON_OBJECT_MAPPER
            .writerWithDefaultPrettyPrinter()
            .with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    // Metric definitions: each row is { json_field_name, JMX_metric_name }.
    // The array order here is kept logical for source readers; the output
    // key order is entirely determined by the ObjectWriter and is always
    // alphabetical regardless of iteration order.
    private static final String[][] POOL_METRICS =
    {
        { "active",           "ActiveTasks" },
        { "pending",          "PendingTasks" },
        { "completed",        "CompletedTasks" },
        { "blocked",          "CurrentlyBlockedTasks" },
        { "all_time_blocked", "TotalBlockedTasks" }
    };

    @Override
    public void execute(NodeProbe probe)
    {
        Map<String, Object>       root   = new HashMap<>();
        Map<String, List<String>> errors = new HashMap<>();

        // ── thread_pools ──────────────────────────────────────────────────
        // Outer try: enumerating pools via JMX.
        // Inner try (per metric): reading a single gauge/counter.
        // Both levels record failures explicitly; neither swallows them.
        Map<String, Object> pools = new HashMap<>();
        try
        {
            for (Map.Entry<String, String> tp : probe.getThreadPools().entries())
            {
                String poolName = tp.getValue();
                Map<String, Object> pool = new HashMap<>();

                for (String[] m : POOL_METRICS)
                {
                    try
                    {
                        pool.put(m[0], normalizeMetricValue(
                                probe.getThreadPoolMetric(tp.getKey(), poolName, m[1])));
                    }
                    catch (Exception e)
                    {
                        // Value set to null so the pool entry stays structurally
                        // complete (five keys, always).  The failure is recorded
                        // in errors so nothing is silently lost.
                        pool.put(m[0], null);
                        errors.computeIfAbsent("thread_pools", k -> new ArrayList<>())
                              .add(poolName + "." + m[0] + ": " + e.getMessage());
                    }
                }
                pools.put(poolName, pool);
            }
        }
        catch (Exception e)
        {
            // getThreadPools() itself failed — no pool data is available.
            errors.computeIfAbsent("thread_pools", k -> new ArrayList<>())
                  .add("failed to enumerate pools: " + e.getMessage());
        }
        root.put("thread_pools", pools);

        // ── dropped_messages ──────────────────────────────────────────────
        try
        {
            root.put("dropped_messages", probe.getDroppedMessages());
        }
        catch (Exception e)
        {
            root.put("dropped_messages", Collections.emptyMap());
            errors.computeIfAbsent("dropped_messages", k -> new ArrayList<>())
                  .add(e.getMessage());
        }

        // ── errors (present only when at least one failure occurred) ──────
        if (!errors.isEmpty())
            root.put("errors", errors);

        // ── serialise ─────────────────────────────────────────────────────
        try
        {
            probe.output().out.println(WRITER.writeValueAsString(root));
        }
        catch (IOException e)
        {
            throw new RuntimeException("failed to serialise tpstatsjson output", e);
        }
    }

    /**
     * Normalises a raw value returned by {@link NodeProbe#getThreadPoolMetric}.
     * The sentinel string {@code "N/A"} (emitted when the backing MBean is not
     * registered) is converted to {@code null}; all other values — numeric or
     * otherwise — pass through unchanged.
     *
     * Package-private so the test suite can exercise the normalisation contract
     * directly without spinning up a JMX server.
     */
    static Object normalizeMetricValue(Object raw)
    {
        return "N/A".equals(raw) ? null : raw;
    }
}
