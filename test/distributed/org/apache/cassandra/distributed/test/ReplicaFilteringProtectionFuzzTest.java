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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Iterators;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.junit.Assert.assertTrue;

/**
 * Fuzzes filtered single-partition reads over two divergent replicas, which use replica filtering protection.
 * Each seed writes inserts, statics, range deletions and flushes to one replica at a time, then compares limited and
 * paged reads, in both orders, against the unlimited unpaged read. A failure reports its seed and a reduced history.
 */
public class ReplicaFilteringProtectionFuzzTest extends TestBaseImpl
{
    private static final int SEEDS = 20;
    private static final int OPS = 300;
    private static final int[] PAGE_SIZES = { 1, 2, 3, 5 };
    private static final int[] LIMITS = { 0, 2, 3, 7 };

    private static Cluster cluster;
    private static int nextPk = 0;

    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = init(Cluster.build(2).withConfig(c -> c.set("hinted_handoff_enabled", false)).start());
        cluster.forEach(i -> i.nodetool("disableautocompaction"));
    }

    @AfterClass
    public static void after()
    {
        if (null != cluster)
            cluster.close();
    }

    @Test
    public void fuzz()
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (pk int, c1 int, c2 int, s int static, v int, pad text, " +
                             "PRIMARY KEY (pk, c1, c2)) WITH read_repair = 'NONE'");
        List<String> failures = new ArrayList<>();
        for (int seed = 0; seed < SEEDS && failures.isEmpty(); seed++)
        {
            List<Op> ops = generate(new Random(seed));
            String failure = replay(ops, null);
            if (null == failure)
                continue;
            failures.add("seed=" + seed + ' ' + failure);
            String mode = failure.substring(0, failure.indexOf(" expected="));
            List<Op> minimal = shrink(ops, mode);
            failures.add("minimal (" + minimal.size() + " of " + ops.size() + " ops) " + replay(minimal, mode) + ":\n" + String.join("\n", render(minimal)));
        }
        assertTrue(String.join("\n", failures), failures.isEmpty());
    }

    private static final class Op
    {
        final int node;
        final String cql; // null for a flush; the first bind value is the partition key
        final Object[] args;

        Op(int node, String cql, Object... args)
        {
            this.node = node;
            this.cql = cql;
            this.args = args;
        }

        @Override
        public String toString()
        {
            return "node" + node + ' ' + (null == cql ? "flush" : cql + ' ' + Arrays.toString(args));
        }
    }

    private static List<Op> generate(Random rng)
    {
        List<Op> ops = new ArrayList<>();
        for (int ts = 1; ts <= OPS; ts++)
        {
            int node = rng.nextInt(2) + 1;
            int op = rng.nextInt(100);
            if (op < 85)
            {
                boolean withStatic = rng.nextBoolean();
                String cql = "INSERT INTO " + KEYSPACE + ".t (pk, c1, c2, v, pad" + (withStatic ? ", s" : "") + ") VALUES (?, ?, ?, ?, ?" + (withStatic ? ", ?" : "") + ") USING TIMESTAMP " + ts;
                Object[] row = { rng.nextInt(60), rng.nextInt(4), rng.nextInt(3), "p".repeat(rng.nextInt(60)) };
                Object[] args = withStatic ? Arrays.copyOf(row, 5) : row;
                if (withStatic)
                    args[4] = rng.nextInt(10);
                ops.add(new Op(node, cql, args));
            }
            else if (op < 97)
            {
                int a = rng.nextInt(60), b = rng.nextInt(60);
                ops.add(new Op(node, "DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP " + ts + " WHERE pk = ? AND c1 > ? AND c1 <= ?", Math.min(a, b), Math.max(a, b)));
            }
            else
            {
                ops.add(new Op(node, null));
            }
        }
        return ops;
    }

    /**
     * Applies the ops to a fresh partition and returns the first read differing from the unlimited unpaged read, or
     * null. When mode is set, only that read is checked.
     */
    private static String replay(List<Op> ops, String mode)
    {
        int pk = nextPk++;
        for (Op op : ops)
        {
            if (null == op.cql)
            {
                cluster.get(op.node).flush(KEYSPACE);
                continue;
            }
            Object[] args = new Object[op.args.length + 1];
            args[0] = pk;
            System.arraycopy(op.args, 0, args, 1, op.args.length);
            cluster.get(op.node).executeInternal(op.cql, args);
        }

        for (String order : new String[]{ "ASC", "DESC" })
        {
            String base = "SELECT c1, c2, v, s FROM " + KEYSPACE + ".t WHERE pk = ? AND c2 < 3 AND v > 0 ORDER BY c1 " + order;
            for (int coordinator = 1; coordinator <= 2; coordinator++)
            {
                List<String> all = render(cluster.coordinator(coordinator).execute(base + " ALLOW FILTERING", ALL, pk));
                for (int limit : LIMITS)
                {
                    String query = base + (0 == limit ? "" : " LIMIT " + limit) + " ALLOW FILTERING";
                    List<String> expected = 0 == limit ? all : all.subList(0, Math.min(limit, all.size()));
                    List<String> modes = new ArrayList<>();
                    String suffix = " order=" + order + " limit=" + limit + " coordinator=" + coordinator;
                    modes.add("unpaged=0" + suffix);
                    for (int pageSize : PAGE_SIZES)
                        modes.add("rows=" + pageSize + suffix);
                    for (String m : modes)
                    {
                        if (null != mode && !mode.equals(m))
                            continue;
                        int size = Integer.parseInt(m.substring(m.indexOf('=') + 1, m.indexOf(' ')));
                        Object[][] rows;
                        if (m.startsWith("unpaged"))
                            rows = cluster.coordinator(coordinator).execute(query, ALL, pk);
                        else
                            rows = Iterators.toArray(cluster.coordinator(coordinator).executeWithPaging(query, ALL, size, pk), Object[].class);
                        String diff = diff(expected, render(rows));
                        if (null != diff)
                            return m + ' ' + diff;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Delta debugging: removes chunks of ops while the given read still differs.
     */
    private static List<Op> shrink(List<Op> ops, String mode)
    {
        int n = 2;
        while (ops.size() >= 2)
        {
            int chunk = (int) Math.ceil(ops.size() / (double) n);
            boolean reduced = false;
            for (int start = 0; start < ops.size(); start += chunk)
            {
                List<Op> candidate = new ArrayList<>(ops.subList(0, start));
                candidate.addAll(ops.subList(Math.min(ops.size(), start + chunk), ops.size()));
                if (!candidate.isEmpty() && null != replay(candidate, mode))
                {
                    ops = candidate;
                    n = Math.max(n - 1, 2);
                    reduced = true;
                    break;
                }
            }
            if (!reduced)
            {
                if (n >= ops.size())
                    break;
                n = Math.min(ops.size(), n * 2);
            }
        }
        return ops;
    }

    private static List<String> render(List<Op> ops)
    {
        List<String> out = new ArrayList<>();
        for (Op op : ops)
            out.add(op.toString());
        return out;
    }

    private static List<String> render(Object[][] rows)
    {
        List<String> out = new ArrayList<>();
        for (Object[] row : rows)
            out.add(Arrays.toString(row));
        return out;
    }

    private static String diff(List<String> expected, List<String> actual)
    {
        if (actual.equals(expected))
            return null;
        List<String> extra = new ArrayList<>(actual);
        extra.removeAll(expected);
        List<String> missing = new ArrayList<>(expected);
        missing.removeAll(actual);
        return "expected=" + expected.size() + " actual=" + actual.size() + " extra=" + extra + " missing=" + missing;
    }
}
