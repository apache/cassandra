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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A dictionary trained with the rack or datacenter scope ({@code nodetool compressiondictionary train --scope
 * rack|dc}) is used by the nodes of the rack or datacenter of the node which trained it.
 * {@code nodetool compressiondictionary promote --dc <dc> [--rack <rack> ...]} widens its scope by racks or
 * datacenters, never shrinks it, and {@code promote} alone widens it to the whole cluster.
 */
public class CompressionDictionaryRackScopeTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final int ROWS = 1000;
    private static final int NODES = 4;

    @Test
    public void rackScopeWidenedByRacksAndDatacenters() throws Throwable
    {
        // two datacenters of two racks of one node each
        try (Cluster cluster = init(Cluster.build(NODES)
                                           .withRacks(2, 2, 1)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);
            List<Location> locations = locations(cluster);
            Location first = locations.get(0);
            int sameDatacenterOtherRack = find(locations, l -> l.datacenter.equals(first.datacenter) && !l.rack.equals(first.rack));
            int otherDatacenter = find(locations, l -> !l.datacenter.equals(first.datacenter));
            String otherDc = locations.get(otherDatacenter - 1).datacenter;
            String otherRack = locations.get(sameDatacenterOtherRack - 1).rack;

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", "--scope", "rack", KEYSPACE, TABLE)).isZero();
            long dictId = currentDictId(cluster, 1);
            assertThat(dictId).isPositive();
            assertRows(persistedDictionaries(cluster), row(dictId, Set.of(rackValue(first.datacenter, first.rack))));
            assertUsedBy(cluster, dictId, node -> node == 1);
            listContains(cluster, "rack " + first.datacenter + '/' + first.rack);

            // nothing else is trained while the dictionary is scoped narrower than the cluster
            cluster.get(otherDatacenter).nodetoolResult("compressiondictionary", "train", "--force", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("is scoped to rack " + first.datacenter + '/' + first.rack + ", promote or discard it first");

            // illegal promotions, the scope stays as it is
            promoteFails(cluster, "--rack requires --dc", "--rack", otherRack);
            promoteFails(cluster, "There is no node in rack " + first.datacenter + "/no_such_rack", "--dc", first.datacenter, "--rack", "no_such_rack");
            promoteFails(cluster, "There is no node in dc no_such_dc", "--dc", "no_such_dc");
            promoteFails(cluster, "scoped to rack " + first.datacenter + '/' + first.rack + ", can not be promoted: rack " + first.datacenter + '/' + first.rack + " is already in its scope, nothing to promote",
                         "--dc", first.datacenter, "--rack", first.rack);
            assertRows(persistedDictionaries(cluster), row(dictId, Set.of(rackValue(first.datacenter, first.rack))));

            // add the other rack of the datacenter, promoted on a node outside of the scope
            assertThat(cluster.get(otherDatacenter).nodetool("compressiondictionary", "promote", "--dc", first.datacenter, "--rack", otherRack, KEYSPACE, TABLE)).isZero();
            assertRows(persistedDictionaries(cluster), row(dictId, Set.of(rackValue(first.datacenter, first.rack), rackValue(first.datacenter, otherRack))));
            assertUsedBy(cluster, dictId, node -> node == 1 || node == sameDatacenterOtherRack);

            // the whole datacenter absorbs its racks, adding a rack of it again is not a promotion
            assertThat(cluster.get(1).nodetool("compressiondictionary", "promote", "--dc", first.datacenter, KEYSPACE, TABLE)).isZero();
            assertRows(persistedDictionaries(cluster), row(dictId, Set.of(Map.of("dc", first.datacenter))));
            promoteFails(cluster, "scoped to dc " + first.datacenter + ", can not be promoted: rack " + first.datacenter + '/' + otherRack + " is already in its scope, nothing to promote",
                         "--dc", first.datacenter, "--rack", otherRack);
            assertUsedBy(cluster, dictId, node -> locations.get(node - 1).datacenter.equals(first.datacenter));

            // the other datacenter too, then the whole cluster
            assertThat(cluster.get(1).nodetool("compressiondictionary", "promote", "--dc", otherDc, KEYSPACE, TABLE)).isZero();
            assertRows(persistedDictionaries(cluster), row(dictId, Set.of(Map.of("dc", first.datacenter), Map.of("dc", otherDc))));
            assertUsedBy(cluster, dictId, node -> true);

            assertThat(cluster.get(1).nodetool("compressiondictionary", "promote", KEYSPACE, TABLE)).isZero();
            assertRows(persistedDictionaries(cluster), row(dictId, null));
            promoteFails(cluster, "is already used by all nodes", "--dc", first.datacenter);
            promoteFails(cluster, "is already used by all nodes");
            listContains(cluster, "cluster");
        }
    }

    /**
     * Promotes a dictionary step by step from a node to the whole cluster, each step from another node:
     * node, its rack, both racks of its datacenter, its datacenter, plus a rack of the other datacenter, plus both racks
     * of the other datacenter, both datacenters, the whole cluster.
     */
    @Test
    public void promotionFromNodeToCluster() throws Throwable
    {
        // two datacenters of two racks of one node each
        try (Cluster cluster = init(Cluster.build(NODES)
                                           .withRacks(2, 2, 1)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);
            List<Location> locations = locations(cluster);
            // a: the node which trains the dictionary, b: the other rack of its datacenter,
            // c and d: the two racks of the other datacenter
            int a = 1;
            Location la = locations.get(a - 1);
            int b = find(locations, l -> l.datacenter.equals(la.datacenter) && !l.rack.equals(la.rack));
            int c = find(locations, l -> !l.datacenter.equals(la.datacenter));
            Location lb = locations.get(b - 1);
            Location lc = locations.get(c - 1);
            int d = find(locations, l -> l.datacenter.equals(lc.datacenter) && !l.rack.equals(lc.rack));
            Location ld = locations.get(d - 1);
            String dc1 = la.datacenter;
            String dc2 = lc.datacenter;
            Map<String, String> rackA = rackValue(dc1, la.rack);
            Map<String, String> rackB = rackValue(dc1, lb.rack);
            Map<String, String> rackC = rackValue(dc2, lc.rack);
            Map<String, String> rackD = rackValue(dc2, ld.rack);

            // 1. one node
            assertThat(cluster.get(a).nodetool("compressiondictionary", "train", "--force", "--scope", "node", KEYSPACE, TABLE)).isZero();
            long dictId = currentDictId(cluster, a);
            assertThat(dictId).isPositive();
            int nodeIdA = cluster.get(a).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            assertScope(cluster, dictId, Set.of(Map.of("node_id", String.valueOf(nodeIdA))), "node " + nodeIdA, Set.of(a));

            // 2. its rack, the node is dropped as the rack contains it
            promote(cluster, c, "--dc", dc1, "--rack", la.rack);
            assertScope(cluster, dictId, Set.of(rackA), racks(rackA), Set.of(a));

            // asking for a rack, or two, which the scope covers already is not a promotion
            promoteFails(cluster, "can not be promoted: rack " + dc1 + '/' + la.rack + " is already in its scope, nothing to promote", "--dc", dc1, "--rack", la.rack);

            // 3. both racks of its datacenter
            promote(cluster, d, "--dc", dc1, "--rack", lb.rack);
            assertScope(cluster, dictId, Set.of(rackA, rackB), racks(rackA, rackB), Set.of(a, b));

            // 4. its datacenter, which absorbs its racks
            promote(cluster, b, "--dc", dc1);
            assertScope(cluster, dictId, Set.of(Map.of("dc", dc1)), "dc " + dc1, Set.of(a, b));

            promoteFails(cluster, "can not be promoted: " + racks(rackA, rackB) + " are already in its scope, nothing to promote",
                         "--dc", dc1, "--rack", la.rack, "--rack", lb.rack);

            // 5. plus a rack of the other datacenter
            promote(cluster, a, "--dc", dc2, "--rack", lc.rack);
            assertScope(cluster, dictId, Set.of(Map.of("dc", dc1), rackC), racks(rackC) + ", dc " + dc1, Set.of(a, b, c));

            // 6. plus both racks of the other datacenter, every node uses it, the scope is still not the whole cluster
            promote(cluster, c, "--dc", dc2, "--rack", ld.rack);
            assertScope(cluster, dictId, Set.of(Map.of("dc", dc1), rackC, rackD), racks(rackC, rackD) + ", dc " + dc1, Set.of(a, b, c, d));
            cluster.get(d).nodetoolResult("compressiondictionary", "train", "--force", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("promote or discard it first");

            // 7. both datacenters, which absorb the racks
            promote(cluster, d, "--dc", dc2);
            assertScope(cluster, dictId, Set.of(Map.of("dc", dc1), Map.of("dc", dc2)), dcs(dc1, dc2), Set.of(a, b, c, d));

            // 8. the whole cluster
            promote(cluster, b);
            assertScope(cluster, dictId, null, "cluster", Set.of(a, b, c, d));
            promoteFails(cluster, "is already used by all nodes");

            // every node writes new sstables with it
            Map<Integer, Map<String, Long>> before = new HashMap<>();
            for (int node = 1; node <= NODES; node++)
                before.put(node, sstableDictionaries(cluster, node));
            insert(cluster, ROWS, 2 * ROWS);
            cluster.forEach(i -> i.flush(KEYSPACE));
            for (int node = 1; node <= NODES; node++)
            {
                Map<String, Long> after = sstableDictionaries(cluster, node);
                after.keySet().removeAll(before.get(node).keySet());
                assertThat(after.values()).as("new sstables of node %s", node).isNotEmpty().containsOnly(dictId);
            }

            // the dictionary is used by all nodes, so the next one can be trained
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            assertThat(cluster.get(d).nodetool("compressiondictionary", "train", "--force", KEYSPACE, TABLE)).isZero();
            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, d) > dictId);
        }
    }

    @Test
    public void datacenterScopeDiscarded() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NODES)
                                           .withRacks(2, 2, 1)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);
            List<Location> locations = locations(cluster);
            int otherDatacenter = find(locations, l -> !l.datacenter.equals(locations.get(0).datacenter));
            String otherDc = locations.get(otherDatacenter - 1).datacenter;

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", KEYSPACE, TABLE)).isZero();
            long global = currentDictId(cluster, 1);
            assertUsedBy(cluster, global, node -> true);

            // dictionary ids are second-granular, the one of the datacenter has to be newer
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            assertThat(cluster.get(otherDatacenter).nodetool("compressiondictionary", "train", "--force", "--scope", "dc", KEYSPACE, TABLE)).isZero();
            long scoped = currentDictId(cluster, otherDatacenter);
            assertThat(scoped).isGreaterThan(global);
            assertRows(persistedDictionaries(cluster), row(scoped, Set.of(Map.of("dc", otherDc))), row(global, null));
            assertUsedBy(cluster, node -> locations.get(node - 1).datacenter.equals(otherDc) ? scoped : global);

            // discarded on a node outside of the scope, the nodes of the datacenter go back to the dictionary of the cluster
            assertThat(cluster.get(1).nodetool("compressiondictionary", "discard", KEYSPACE, TABLE)).isZero();
            assertRows(persistedDictionaries(cluster), row(global, null));
            assertUsedBy(cluster, global, node -> true);
        }
    }

    /** promotes on the given node, which succeeds */
    private static void promote(Cluster cluster, int node, String... options)
    {
        List<String> args = new ArrayList<>(List.of("compressiondictionary", "promote"));
        args.addAll(List.of(options));
        args.add(KEYSPACE);
        args.add(TABLE);
        cluster.get(node).nodetoolResult(args.toArray(new String[0])).asserts().success();
    }

    /**
     * The dictionary is stored with the scope value, listed with the scope display, and the given nodes compress with
     * it while the others compress without a dictionary.
     */
    private static void assertScope(Cluster cluster, long dictId, Set<Map<String, String>> value, String display, Set<Integer> users)
    {
        assertRows(persistedDictionaries(cluster), row(dictId, value));
        listContains(cluster, display);
        assertUsedBy(cluster, dictId, users::contains);
    }

    /** how racks are listed: sorted by datacenter, then rack */
    @SafeVarargs
    private static String racks(Map<String, String>... racks)
    {
        List<String> listed = new ArrayList<>();
        for (Map<String, String> rack : racks)
            listed.add("rack " + rack.get("dc") + '/' + rack.get("rack"));
        listed.sort(null);
        return String.join(", ", listed);
    }

    /** how datacenters are listed: sorted by name */
    private static String dcs(String... dcs)
    {
        List<String> listed = new ArrayList<>();
        for (String dc : dcs)
            listed.add("dc " + dc);
        listed.sort(null);
        return String.join(", ", listed);
    }

    private static void insert(Cluster cluster, int from, int to)
    {
        String insert = withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (?, ?)");
        for (int pk = from; pk < to; pk++)
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, pk, "2025-10-20 11:00:01 INFO com.example.UserService request " + pk + " processed in 12ms");
    }

    /** sstable id to the id of the dictionary it was written with, 0 when none */
    private static Map<String, Long> sstableDictionaries(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            Map<String, Long> result = new HashMap<>();
            for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getLiveSSTables())
            {
                CompressionDictionary dictionary = sstable.getCompressionMetadata().compressionDictionary();
                result.put(sstable.descriptor.id.toString(), dictionary == null ? 0L : dictionary.dictId().id);
            }
            return result;
        });
    }

    private static void promoteFails(Cluster cluster, String message, String... options)
    {
        List<String> args = new ArrayList<>(List.of("compressiondictionary", "promote"));
        args.addAll(List.of(options));
        args.add(KEYSPACE);
        args.add(TABLE);
        cluster.get(1).nodetoolResult(args.toArray(new String[0])).asserts().failure().stderrContains(message);
    }

    private static void listContains(Cluster cluster, String scope)
    {
        cluster.get(1).nodetoolResult("compressiondictionary", "list", KEYSPACE, TABLE).asserts().success().stdoutContains(scope);
    }

    /** the nodes the predicate accepts compress with the dictionary, the others without one */
    private static void assertUsedBy(Cluster cluster, long dictId, IntPredicate users)
    {
        assertUsedBy(cluster, node -> users.test(node) ? dictId : 0L);
    }

    private static void assertUsedBy(Cluster cluster, java.util.function.IntToLongFunction expected)
    {
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            for (int node = 1; node <= NODES; node++)
                assertThat(currentDictId(cluster, node)).as("dictionary of node %s", node).isEqualTo(expected.applyAsLong(node));
        });
    }

    private static int find(List<Location> locations, java.util.function.Predicate<Location> predicate)
    {
        for (int node = 1; node <= locations.size(); node++)
        {
            if (predicate.test(locations.get(node - 1)))
                return node;
        }
        throw new AssertionError("No node matches in " + locations);
    }

    private static List<Location> locations(Cluster cluster)
    {
        List<Location> locations = new ArrayList<>();
        for (int node = 1; node <= NODES; node++)
        {
            String[] location = cluster.get(node).callOnInstance(() -> {
                Location l = ClusterMetadata.current().directory.location(ClusterMetadata.current().myNodeId());
                return new String[]{ l.datacenter, l.rack };
            });
            locations.add(new Location(location[0], location[1]));
        }
        return locations;
    }

    private static Map<String, String> rackValue(String datacenter, String rack)
    {
        return Map.of("dc", datacenter, "rack", rack);
    }

    /** creates the table with autocompaction disabled and flushes rows written without any dictionary on every node */
    private static void populate(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text) " +
                                          "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}"));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, TABLE).asserts().success());

        insert(cluster, 0, ROWS);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    /** dict_id and scope of the persisted dictionaries, scope is null for the whole cluster */
    private static Object[][] persistedDictionaries(Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT dict_id, scope FROM system_distributed.compression_dictionaries", ConsistencyLevel.QUORUM);
    }

    private static long currentDictId(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            CompressionDictionary current = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE)
                                                    .compressionDictionaryManager()
                                                    .getCurrent();

            return current == null ? 0L : current.dictId().id;
        });
    }
}
