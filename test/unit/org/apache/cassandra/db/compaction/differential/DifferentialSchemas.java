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

package org.apache.cassandra.db.compaction.differential;

import java.util.List;

/**
 * A minimal, meaningful corpus of table shapes, path-agnostic so read-path and flush-path differential
 * engines on higher branches can reuse it. Every shape produces two overlapping flushes that merge.
 *
 * <p>References no compaction, flush or read type; a shape drives writes only through a
 * {@link DifferentialWorkload}. Each shape multiplies its base partition/row counts by a {@code scale}, so
 * the same fixtures serve both the fast matrix ({@code scale == 1}) and a burn test (large {@code scale}).
 */
public final class DifferentialSchemas
{
    private DifferentialSchemas()
    {
    }

    /**
     * The minimal starter corpus, growing from the five shapes a review flagged as the coverage floor to
     * the full set: clustering-free, simple clustering, static + clustering, a multi-cell collection,
     * reversed clustering, two multi-block (wide-partition) shapes that force the block-navigation path,
     * a deletions shape covering every tombstone kind, compound clustering, a wide-column shape, a
     * range tombstone that spans index blocks, a four-input k-way merge, a mixed-type shape, and expiring
     * (TTL) cells.
     */
    public static List<DifferentialSchema> minimalCorpus()
    {
        return List.of(new ClusteringFree(),
                       new SimpleClustering(),
                       new StaticAndClustering(),
                       new MulticellCollection(),
                       new ReversedClustering(),
                       new WideMultiBlock("multi-block-partition",
                                          "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))"),
                       new WideMultiBlock("reversed-multi-block",
                                          "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                                          "WITH CLUSTERING ORDER BY (ck DESC)"),
                       new Deletions(),
                       new CompoundClustering(),
                       new WideColumns(),
                       new SpanningTombstone(),
                       new KWayMerge(),
                       new MixedTypes(),
                       new TtlCells());
    }

    /** Base that carries the name and definition, so a shape only has to supply its writes. */
    private abstract static class BaseSchema implements DifferentialSchema
    {
        private final String name;
        private final String tableDefinition;

        BaseSchema(String name, String tableDefinition)
        {
            this.name = name;
            this.tableDefinition = tableDefinition;
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public String tableDefinition()
        {
            return tableDefinition;
        }

        /** So the parameterized case label reads as the shape name rather than an object hash. */
        @Override
        public String toString()
        {
            return name;
        }
    }

    /**
     * pk only, no clustering. The two rounds rewrite the SAME partition keys with different values, so
     * the flushes genuinely overlap and the merge must reconcile per-partition (this is the shape the
     * review flagged as uncovered).
     */
    private static final class ClusteringFree extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 20;

        ClusteringFree()
        {
            super("clustering-free", "CREATE TABLE %s (pk bigint PRIMARY KEY, v1 bigint, v2 text)");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                workload.execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk, "v" + pk);
            workload.flush();

            // same keys, newer values: the two sstables overlap on every partition and must merge
            for (long pk = 0; pk < partitions; pk++)
                workload.execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk + 100, "updated" + pk);
            workload.flush();
        }
    }

    /** pk + ck, plain regular columns; the two rounds overlap on shared clusterings. */
    private static final class SimpleClustering extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 6;
        private static final int ROWS_PER_PARTITION = 40;

        SimpleClustering()
        {
            super("simple-clustering", "CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, " +
                                       "PRIMARY KEY (pk, ck))");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                                     pk, ck, ck, "r1-" + ck);
            workload.flush();

            // overwrite the lower half of each partition, so the flushes overlap
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION / 2; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                                     pk, ck, ck + 100, "r2-" + ck);
            workload.flush();
        }
    }

    /** A static column alongside clustering and regular columns; both rounds touch the static row. */
    private static final class StaticAndClustering extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 5;
        private static final int ROWS_PER_PARTITION = 8;

        StaticAndClustering()
        {
            super("static-and-clustering", "CREATE TABLE %s (pk bigint, s1 text static, ck bigint, v text, " +
                                           "PRIMARY KEY (pk, ck))");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
            {
                workload.execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", pk, "static-r1-" + pk);
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r1-" + ck);
            }
            workload.flush();

            // overwrite the static row and part of each partition's rows
            for (long pk = 0; pk < partitions; pk++)
            {
                workload.execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", pk, "static-r2-" + pk);
                for (long ck = 0; ck < ROWS_PER_PARTITION / 2; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r2-" + ck);
            }
            workload.flush();
        }
    }

    /**
     * A multi-cell map next to clustering. The second round updates single map elements, adding new
     * cell paths onto columns that already exist so the merge unions the collections.
     */
    private static final class MulticellCollection extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 4;
        private static final int ROWS_PER_PARTITION = 8;

        MulticellCollection()
        {
            super("multicell-collection", "CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, " +
                                          "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            // the map is written as a CQL literal (not a bind parameter) so the corpus stays free of
            // CQLTester's map() helper and of any binding type
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, " +
                                     mapOf("k" + ck, ck, "shared", pk) + ", ?)",
                                     pk, ck, "v" + ck);
            workload.flush();

            // add a new element and rewrite an existing path on the shared rows
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck += 2)
                    workload.execute("UPDATE %s SET m[?] = ?, m[?] = ? WHERE pk = ? AND ck = ?",
                                     "added" + ck, ck * 10, "shared", pk + 100, pk, ck);
            workload.flush();
        }

        /** A CQL map literal {@code {'k1': v1, 'k2': v2}} with text keys and bigint values. */
        private static String mapOf(String k1, long v1, String k2, long v2)
        {
            return "{'" + k1 + "': " + v1 + ", '" + k2 + "': " + v2 + '}';
        }
    }

    /** Reversed clustering order (DESC), a known BTI cursor risk; the two rounds overlap on shared rows. */
    private static final class ReversedClustering extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 5;
        private static final int ROWS_PER_PARTITION = 30;

        ReversedClustering()
        {
            super("reversed-clustering", "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                                         "WITH CLUSTERING ORDER BY (ck DESC)");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r1-v" + ck);
            workload.flush();

            // overwrite a mid-range window in each partition, so the flushes overlap under DESC order
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = ROWS_PER_PARTITION / 3; ck < 2 * ROWS_PER_PARTITION / 3; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r2-v" + ck);
            workload.flush();
        }
    }

    /** ~600-byte padding, wide enough that a partition of {@link #WIDE_ROWS_PER_PARTITION} rows spans many
     *  column-index blocks under any reasonable column_index_size. */
    private static final String WIDE_PADDING = "x".repeat(600);

    /** 160 rows * ~600 bytes ≈ 96 KiB per partition: over a 64 KiB block, and many blocks at 4 KiB. */
    private static final int WIDE_ROWS_PER_PARTITION = 160;

    /**
     * Fills each partition with {@link #WIDE_ROWS_PER_PARTITION} wide rows, so the partition spans many
     * column-index blocks. An explicit timestamp is applied when {@code timestamp} is non-null.
     */
    private static void writeWideFill(DifferentialWorkload workload, long partitions, Long timestamp)
    {
        for (long pk = 0; pk < partitions; pk++)
            for (long ck = 0; ck < WIDE_ROWS_PER_PARTITION; ck++)
                if (timestamp == null)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, WIDE_PADDING + "-r1-" + ck);
                else
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                     pk, ck, WIDE_PADDING + "-r1-" + ck, timestamp);
    }

    /**
     * A handful of partitions each far larger than a column-index block, so partitions span multiple
     * blocks and the cursor block-navigation path runs. Multi-block-ness comes purely from large padded
     * values (the corpus must not touch the global column_index_size). The DDL sets the clustering order,
     * so one instance covers forward order and another covers reversed (DESC), which targets the reverse
     * block-cursor path. The two rounds overlap on a mid-range window inside each wide partition.
     */
    private static final class WideMultiBlock extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 2;

        WideMultiBlock(String name, String tableDefinition)
        {
            super(name, tableDefinition);
        }

        @Override
        public boolean spansMultipleIndexBlocks()
        {
            return true;
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            writeWideFill(workload, partitions, null);
            workload.flush();

            // overwrite a mid-range window inside each wide partition, so the flushes overlap across blocks
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = WIDE_ROWS_PER_PARTITION / 4; ck < WIDE_ROWS_PER_PARTITION / 2; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, WIDE_PADDING + "-r2-" + ck);
            workload.flush();
        }
    }

    /**
     * Every tombstone kind, with round-2 deletions that shadow round-1 data so the merge must drop cells.
     * Explicit timestamps make purge deterministic and keep both paths in agreement.
     */
    private static final class Deletions extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 5;
        private static final int ROWS_PER_PARTITION = 20;
        private static final long ROUND1_TS = 1000;
        private static final long ROUND2_TS = 2000;

        Deletions()
        {
            // large gc_grace so the tombstones are retained through compaction, deterministically
            super("deletions", "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                               "WITH gc_grace_seconds = 864000");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                     pk, ck, "r1-" + ck, ROUND1_TS);
            workload.flush();

            // round 2: every tombstone kind at a newer timestamp, each shadowing round-1 data
            for (long pk = 0; pk < partitions; pk++)
            {
                // partition-level delete: shadows the whole partition written in round 1
                if (pk % 5 == 0)
                    workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", ROUND2_TS, pk);
                // row delete: shadows one round-1 row
                else if (pk % 5 == 1)
                    workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", ROUND2_TS, pk, 0L);
                // range tombstone: shadows a band of round-1 rows
                else if (pk % 5 == 2)
                    workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?",
                                     ROUND2_TS, pk, 4L, 12L);
                // otherwise: a plain newer write, so the sstable also carries live overlap
                else
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                     pk, 0L, "r2-" + pk, ROUND2_TS);
            }
            workload.flush();
        }
    }

    /** Compound clustering (pk, ck1, ck2), exercising the composite comparator; rounds overlap. */
    private static final class CompoundClustering extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 3;
        private static final int CK1_COUNT = 5;
        private static final int CK2_COUNT = 4;

        CompoundClustering()
        {
            super("compound-clustering", "CREATE TABLE %s (pk bigint, ck1 text, ck2 int, v text, " +
                                         "PRIMARY KEY (pk, ck1, ck2))");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (int ck1 = 0; ck1 < CK1_COUNT; ck1++)
                    for (int ck2 = 0; ck2 < CK2_COUNT; ck2++)
                        workload.execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)",
                                         pk, "c" + ck1, ck2, "r1-" + ck1 + '-' + ck2);
            workload.flush();

            // overwrite the ck2 == 0 slice of each ck1 prefix, so the flushes overlap
            for (long pk = 0; pk < partitions; pk++)
                for (int ck1 = 0; ck1 < CK1_COUNT; ck1++)
                    workload.execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)",
                                     pk, "c" + ck1, 0, "r2-" + ck1);
            workload.flush();
        }
    }

    /** A wide-column table (~50 regular columns), a scaled stand-in for the 1000-field production target. */
    private static final class WideColumns extends BaseSchema
    {
        private static final int COLUMN_COUNT = 50;
        private static final int BASE_ROWS = 15;

        WideColumns()
        {
            super("wide-columns", buildDefinition());
        }

        private static String buildDefinition()
        {
            StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint PRIMARY KEY");
            for (int i = 0; i < COLUMN_COUNT; i++)
                ddl.append(", c").append(i).append(" int");
            ddl.append(')');
            return ddl.toString();
        }

        /** Builds one INSERT that sets every {@code step}-th column to {@code i + valueOffset}. */
        private static String buildInsert(int step, int valueOffset)
        {
            StringBuilder sql = new StringBuilder("INSERT INTO %s (pk");
            for (int i = 0; i < COLUMN_COUNT; i += step)
                sql.append(", c").append(i);
            sql.append(") VALUES (?");
            for (int i = 0; i < COLUMN_COUNT; i += step)
                sql.append(", ").append(i + valueOffset);
            sql.append(')');
            return sql.toString();
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long rows = (long) BASE_ROWS * scale;

            // round 1: full rows, every column set
            String fullInsert = buildInsert(1, 0);
            for (long pk = 0; pk < rows; pk++)
                workload.execute(fullInsert, pk);
            workload.flush();

            // round 2: overwrite every other column on the same rows, so the merge unions cells
            String subsetInsert = buildInsert(2, 1000);
            for (long pk = 0; pk < rows; pk++)
                workload.execute(subsetInsert, pk);
            workload.flush();
        }
    }

    /**
     * A wide, multi-block partition whose round-2 range tombstone opens in an early block and closes in a
     * far-later block, so the merge must carry an OPEN range tombstone across block boundaries. A plain
     * newer write keeps the flushes overlapping live. Large gc_grace retains the tombstone through compaction.
     */
    private static final class SpanningTombstone extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 2;
        // open/close bounds sit far apart so they land in different index blocks
        private static final long RT_OPEN_CK = 20;
        private static final long RT_CLOSE_CK = 140;
        private static final long ROUND1_TS = 1000;
        private static final long ROUND2_TS = 2000;

        SpanningTombstone()
        {
            // large gc_grace so the spanning tombstone survives compaction, deterministically
            super("spanning-tombstone", "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                                        "WITH gc_grace_seconds = 864000");
        }

        @Override
        public boolean spansMultipleIndexBlocks()
        {
            return true;
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            writeWideFill(workload, partitions, ROUND1_TS);
            workload.flush();

            // round 2: a range tombstone that spans many blocks, plus a plain newer write for live overlap
            for (long pk = 0; pk < partitions; pk++)
            {
                workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?",
                                 ROUND2_TS, pk, RT_OPEN_CK, RT_CLOSE_CK);
                workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                 pk, 0L, WIDE_PADDING + "-r2", ROUND2_TS);
            }
            workload.flush();
        }
    }

    /**
     * Four overlapping flushes all rewriting the same keys and clusterings, so one compaction reconciles
     * four sstables in a k-way merge. Each round supplies a strictly newer value for every cell.
     */
    private static final class KWayMerge extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 4;
        private static final int ROWS_PER_PARTITION = 10;
        private static final int ROUNDS = 4;

        KWayMerge()
        {
            super("k-way-merge", "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (int round = 0; round < ROUNDS; round++)
            {
                for (long pk = 0; pk < partitions; pk++)
                    for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                        workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                                         pk, ck, "r" + round + '-' + ck);
                workload.flush();
            }
        }
    }

    /**
     * Type diversity: blob, a frozen collection, a multi-cell set, and scalars (decimal, timestamp, uuid).
     * All values are written as CQL literals so the corpus stays free of path-specific helpers. Round 2
     * overwrites a subset, rewriting single-cell values and unioning into the multi-cell set.
     */
    private static final class MixedTypes extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 3;
        private static final int ROWS_PER_PARTITION = 8;

        MixedTypes()
        {
            super("mixed-types", "CREATE TABLE %s (pk bigint, ck bigint, b blob, fl frozen<list<int>>, " +
                                 "s set<int>, d decimal, t timestamp, u uuid, v text, PRIMARY KEY (pk, ck))");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, b, fl, s, d, t, u, v) VALUES " +
                                     "(?, ?, 0xcafebabe, [1, 2, 3], {1, 2, 3}, 3.14, 1672531200000, " +
                                     "123e4567-e89b-12d3-a456-426614174000, ?)",
                                     pk, ck, "r1-" + ck);
            workload.flush();

            // round 2: overwrite the even rows, rewriting single-cell values and adding set elements
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck += 2)
                    workload.execute("UPDATE %s SET b = 0xdeadbeef, fl = [9, 9], s = s + {4, 5}, " +
                                     "d = 2.71, v = ? WHERE pk = ? AND ck = ?",
                                     "r2-" + ck, pk, ck);
            workload.flush();
        }
    }

    /**
     * Expiring cells: round-1 cells carry a large TTL so they stay live for the whole run (no clock pinning,
     * no mid-run expiry). Round 2 overwrites a subset, some cells again with a TTL and some without, so the
     * merge reconciles expiring against non-expiring liveness for the same cell.
     */
    private static final class TtlCells extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 5;
        private static final int ROWS_PER_PARTITION = 10;
        // large enough (10 days) that every cell is still live throughout the test
        private static final int LARGE_TTL = 864000;

        TtlCells()
        {
            super("ttl-expiring", "CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL " + LARGE_TTL,
                                     pk, ck, "a" + ck, "b" + ck);
            workload.flush();

            // round 2: overwrite v1 on a subset, alternating between expiring and non-expiring cells
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck += 2)
                {
                    if (ck % 4 == 0)
                        workload.execute("UPDATE %s USING TTL " + LARGE_TTL + " SET v1 = ? WHERE pk = ? AND ck = ?",
                                         "a2-" + ck, pk, ck);
                    else
                        workload.execute("UPDATE %s SET v1 = ? WHERE pk = ? AND ck = ?", "a2-" + ck, pk, ck);
                }
            workload.flush();
        }
    }
}
