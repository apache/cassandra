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
     * The minimal starter corpus. The returned list names each shape. The shapes cover:
     * <ul>
     *   <li>clustering-free, simple clustering, and compound clustering;</li>
     *   <li>reversed clustering;</li>
     *   <li>a static column with clustering;</li>
     *   <li>a multi-cell collection;</li>
     *   <li>a wide-column shape and a shape with more than 64 columns, to exercise the wide column encoding;</li>
     *   <li>two wide-partition shapes that span many index blocks;</li>
     *   <li>a deletions shape that covers every tombstone kind;</li>
     *   <li>a range tombstone that spans index blocks;</li>
     *   <li>a four-input merge;</li>
     *   <li>a mixed-type shape;</li>
     *   <li>expiring (TTL) cells;</li>
     *   <li>a deep partition whose row count scales with {@code scale};</li>
     *   <li>a purge shape whose expired cells and tombstones the merge must drop;</li>
     *   <li>a cross-sstable range-tombstone shape whose two overlapping tombstone bands the merge must reconcile
     *       across index blocks;</li>
     *   <li>a wide-partition deletion shape whose partition- and range-level tombstones the cursor navigates
     *       block to block;</li>
     *   <li>a wide-partition purge shape whose expired interior cells collapse whole index blocks;</li>
     *   <li>a static column on a wide partition, including a static row that survives a partition delete;</li>
     *   <li>a shape whose single rows each exceed one index block.</li>
     * </ul>
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
                       new TtlCells(),
                       new DeepPartition(),
                       new Purge(),
                       new CrossSSTableRangeTombstone(),
                       new WideMultiBlockDeletion(),
                       new WideMultiBlockPurge(),
                       new StaticMultiBlock(),
                       new SingleRowExceedsBlock(),
                       new SuperWideColumns());
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
     * the flushes genuinely overlap and the merge must reconcile per-partition.
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

    /**
     * A single partition whose depth scales with {@code scale}: at the matrix size it already spans many
     * column-index blocks, and at burn scale it becomes a very deep partition (the block-navigation path
     * under real depth). Unlike {@link WideMultiBlock}, which scales the partition count and keeps a fixed
     * depth, this shape scales the row count of one partition. The two rounds overwrite a mid-range window,
     * so the flushes overlap deep inside the partition.
     */
    private static final class DeepPartition extends BaseSchema
    {
        // one partition, so all the scale goes into depth rather than breadth
        private static final long PK = 0;

        DeepPartition()
        {
            super("deep-partition", "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        }

        @Override
        public boolean spansMultipleIndexBlocks()
        {
            return true;
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long rows = (long) WIDE_ROWS_PER_PARTITION * scale;
            for (long ck = 0; ck < rows; ck++)
                workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", PK, ck, WIDE_PADDING + "-r1-" + ck);
            workload.flush();

            // overwrite the second quarter of the partition, so the two flushes overlap across many blocks
            for (long ck = rows / 4; ck < rows / 2; ck++)
                workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", PK, ck, WIDE_PADDING + "-r2-" + ck);
            workload.flush();
        }
    }

    /**
     * Purge coverage: {@code gc_grace_seconds = 0}, so expired cells and tombstones are purgeable the moment
     * "now" passes them. Even rows are permanent (they survive purge and keep the merged output non-empty);
     * odd rows carry a tiny TTL (they must expire); round 2 deletes a band of the permanent rows (the
     * tombstone and the data it shadows must purge). A driver runs this with a pinned future "now" so the
     * purge is deterministic, and guards that the merge actually drops rows.
     */
    private static final class Purge extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 5;
        private static final int ROWS_PER_PARTITION = 20;
        // short enough that the pinned future "now" is always past it, so the cell is expired
        private static final int SHORT_TTL = 1;
        private static final long ROUND1_TS = 1000;
        private static final long ROUND2_TS = 2000;
        // round 2 deletes clusterings [0, DELETE_BAND) of every partition
        private static final long DELETE_BAND = 4;

        Purge()
        {
            super("purge", "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                           "WITH gc_grace_seconds = 0");
        }

        @Override
        public boolean expectsPurge()
        {
            return true;
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                {
                    if (ck % 2 == 0)
                        workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                         pk, ck, "live-" + ck, ROUND1_TS);
                    else
                        workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL " + SHORT_TTL,
                                         pk, ck, "exp-" + ck, ROUND1_TS);
                }
            workload.flush();

            // round 2: delete a band of the permanent rows, so a tombstone shadows live round-1 data
            for (long pk = 0; pk < partitions; pk++)
                workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck < ?",
                                 ROUND2_TS, pk, 0L, DELETE_BAND);
            workload.flush();
        }
    }

    /**
     * Cross-sstable range-tombstone reconciliation over a multi-block partition. Sstable A carries a wide
     * fill plus one range tombstone band; sstable B carries a second, partially-overlapping band at a newer
     * timestamp, plus a point delete inside the first band and a live newer write. The merge must reconcile
     * two range tombstones from different sstables: coalesce the overlap, keep the newer band where it
     * supersedes, and carry every marker across index-block boundaries. This is the marker-boundary path
     * that historically breaks the cursor (see the reverse block-cursor and NAMES exclusive-bound bugs).
     */
    private static final class CrossSSTableRangeTombstone extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 2;
        private static final long ROUND1_TS = 1000;
        private static final long RT_A_TS = 1500;
        private static final long ROUND2_TS = 2000;
        // two bands, each many rows wide so they cross index blocks; B overlaps A and extends past it
        private static final long RT_A_OPEN = 20;
        private static final long RT_A_CLOSE = 100;
        private static final long RT_B_OPEN = 60;
        private static final long RT_B_CLOSE = 140;
        // a point delete inside band A, before the overlap with band B
        private static final long POINT_DELETE_CK = 30;

        CrossSSTableRangeTombstone()
        {
            // large gc_grace so both tombstone bands survive compaction, deterministically
            super("cross-sstable-range-tombstone",
                  "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
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
            // sstable A also carries the first range tombstone band
            for (long pk = 0; pk < partitions; pk++)
                workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck < ?",
                                 RT_A_TS, pk, RT_A_OPEN, RT_A_CLOSE);
            workload.flush();

            // sstable B: a newer overlapping band, a point delete inside band A, and a live newer write
            for (long pk = 0; pk < partitions; pk++)
            {
                workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck < ?",
                                 ROUND2_TS, pk, RT_B_OPEN, RT_B_CLOSE);
                workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?",
                                 ROUND2_TS, pk, POINT_DELETE_CK);
                workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                 pk, 0L, WIDE_PADDING + "-r2", ROUND2_TS);
            }
            workload.flush();
        }
    }

    /**
     * Deletion over a multi-block partition, with the tombstones retained (large gc_grace). Round 2 shadows
     * round-1 data with a partition-level delete on some wide partitions and a block-spanning range delete on
     * others, plus a live newer write for overlap. The cursor must keep emitting and skipping correctly block
     * to block while a partition- or range-level deletion shadows the promoted multi-block index.
     */
    private static final class WideMultiBlockDeletion extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 3;
        private static final long ROUND1_TS = 1000;
        private static final long ROUND2_TS = 2000;
        // a range delete band wide enough to span several index blocks
        private static final long RANGE_OPEN = 20;
        private static final long RANGE_CLOSE = 140;

        WideMultiBlockDeletion()
        {
            // large gc_grace so the tombstones are retained through compaction, deterministically
            super("wide-multi-block-deletion",
                  "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
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

            // round 2: partition-level and range-level deletions over the wide partitions, plus a live write
            for (long pk = 0; pk < partitions; pk++)
            {
                if (pk % 3 == 0)
                    // partition-level delete: shadows every block of the wide partition
                    workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", ROUND2_TS, pk);
                else if (pk % 3 == 1)
                    // range delete spanning many blocks
                    workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck < ?",
                                     ROUND2_TS, pk, RANGE_OPEN, RANGE_CLOSE);
                else
                    // a plain newer write, so the sstable also carries live overlap on this partition
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                     pk, 0L, WIDE_PADDING + "-r2", ROUND2_TS);
            }
            workload.flush();
        }
    }

    /**
     * Purge that collapses whole interior index blocks of a multi-block partition. Each wide partition keeps
     * a live edge at each end (they survive purge and keep the merged output non-empty) and fills its
     * interior with short-TTL cells that expire, so purge empties entire interior blocks and shifts the
     * output block layout; the written promoted index must still match the iterator's. A mid-range window is
     * rewritten in round 2, also expiring, so the two flushes overlap across blocks. {@code gc_grace = 0} so
     * the expired cells purge the moment the pinned "now" passes them.
     */
    private static final class WideMultiBlockPurge extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 2;
        // short enough that the pinned future "now" is always past it, so the interior cells expire
        private static final int SHORT_TTL = 1;
        private static final long ROUND1_TS = 1000;
        private static final long ROUND2_TS = 2000;
        // the first and last EDGE rows of each partition are permanent; the interior expires
        private static final long EDGE = 8;

        WideMultiBlockPurge()
        {
            super("wide-multi-block-purge",
                  "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                  "WITH gc_grace_seconds = 0");
        }

        @Override
        public boolean spansMultipleIndexBlocks()
        {
            return true;
        }

        @Override
        public boolean expectsPurge()
        {
            return true;
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long partitions = (long) BASE_PARTITIONS * scale;
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < WIDE_ROWS_PER_PARTITION; ck++)
                {
                    if (ck < EDGE || ck >= WIDE_ROWS_PER_PARTITION - EDGE)
                        // permanent edge row: survives purge, keeps a live block at each end of the partition
                        workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                         pk, ck, WIDE_PADDING + "-edge-" + ck, ROUND1_TS);
                    else
                        // interior row on a tiny TTL: it expires, so purge empties this block
                        workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL " + SHORT_TTL,
                                         pk, ck, WIDE_PADDING + "-exp-" + ck, ROUND1_TS);
                }
            workload.flush();

            // round 2: rewrite a mid-range interior window, also expiring, so the two flushes overlap on blocks
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = WIDE_ROWS_PER_PARTITION / 4; ck < WIDE_ROWS_PER_PARTITION / 2; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL " + SHORT_TTL,
                                     pk, ck, WIDE_PADDING + "-exp2-" + ck, ROUND2_TS);
            workload.flush();
        }
    }

    /**
     * A static column on a multi-block (promoted-index) partition. The static row sits at the partition
     * head, before the promoted row index begins, so its liveness and deletion interplay with block
     * navigation is a classic cursor divergence point. Every partition is wide enough to span many index
     * blocks. Round 2 covers three cases across the partitions:
     * <ul>
     *   <li>{@code pk % 3 == 0}: rewrite the static row and a mid-range window of rows, so the static row
     *       changes across rounds while the blocks overlap.</li>
     *   <li>{@code pk % 3 == 1}: a partition delete older than a later static write, so a live static row
     *       survives a partition deletion that shadows every regular row across all blocks.</li>
     *   <li>{@code pk % 3 == 2}: rewrite the static row and apply a block-spanning range delete.</li>
     * </ul>
     * Large gc_grace retains the tombstones through compaction, deterministically.
     */
    private static final class StaticMultiBlock extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 3;
        private static final long ROUND1_TS = 1000;
        private static final long ROUND2_TS = 2000;
        // a static write newer than the round-2 partition delete, so the static row survives it
        private static final long STATIC_SURVIVES_TS = 3000;
        // a range delete band wide enough to span several index blocks
        private static final long RANGE_OPEN = 20;
        private static final long RANGE_CLOSE = 140;

        StaticMultiBlock()
        {
            super("static-multi-block",
                  "CREATE TABLE %s (pk bigint, s text static, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
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
            for (long pk = 0; pk < partitions; pk++)
            {
                workload.execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?", pk, "static-r1-" + pk, ROUND1_TS);
                for (long ck = 0; ck < WIDE_ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                     pk, ck, WIDE_PADDING + "-r1-" + ck, ROUND1_TS);
            }
            workload.flush();

            for (long pk = 0; pk < partitions; pk++)
            {
                if (pk % 3 == 0)
                {
                    // rewrite the static row and a mid-range window of rows, so the blocks overlap
                    workload.execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?", pk, "static-r2-" + pk, ROUND2_TS);
                    for (long ck = WIDE_ROWS_PER_PARTITION / 4; ck < WIDE_ROWS_PER_PARTITION / 2; ck++)
                        workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                         pk, ck, WIDE_PADDING + "-r2-" + ck, ROUND2_TS);
                }
                else if (pk % 3 == 1)
                {
                    // partition delete, then a NEWER static write: the static row survives while the delete
                    // shadows every regular row across all blocks
                    workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", ROUND2_TS, pk);
                    workload.execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?", pk, "static-survives-" + pk, STATIC_SURVIVES_TS);
                }
                else
                {
                    // rewrite the static row and delete a block-spanning range of rows
                    workload.execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?", pk, "static-r2-" + pk, ROUND2_TS);
                    workload.execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck < ?",
                                     ROUND2_TS, pk, RANGE_OPEN, RANGE_CLOSE);
                }
            }
            workload.flush();
        }
    }

    /** ~5000-byte value, over the 4 KiB test column_index_size, so a single row exceeds one index block. */
    private static final String OVERSIZED_PADDING = "x".repeat(5000);

    /**
     * A single row larger than one column-index block. Each row carries an ~5000-byte value, over the 4 KiB
     * test column_index_size, so one row alone cuts a block and exercises the value-copy-across-chunks path.
     * Round 2 overwrites the lower half so the flushes overlap across blocks, and both rounds write the last
     * row at the SAME timestamp with different oversized values, a cross-sstable tie the merge must break by
     * value comparison, identically on both paths.
     */
    private static final class SingleRowExceedsBlock extends BaseSchema
    {
        private static final int BASE_PARTITIONS = 2;
        private static final int ROWS_PER_PARTITION = 6;
        private static final long ROUND1_TS = 1000;
        private static final long ROUND2_TS = 2000;
        // both writes of the tie share this timestamp, so the merge must break the tie by value
        private static final long TIE_TS = 3000;

        SingleRowExceedsBlock()
        {
            super("single-row-exceeds-block", "CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
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
            long tieCk = ROWS_PER_PARTITION - 1;
            for (long pk = 0; pk < partitions; pk++)
            {
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                     pk, ck, OVERSIZED_PADDING + "-r1-" + ck, ROUND1_TS);
                // the last row's cross-sstable tie: this sstable's side of it
                workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                 pk, tieCk, OVERSIZED_PADDING + "-tieA", TIE_TS);
            }
            workload.flush();

            for (long pk = 0; pk < partitions; pk++)
            {
                // overwrite the lower half with newer oversized values, so the flushes overlap across blocks
                for (long ck = 0; ck < ROWS_PER_PARTITION / 2; ck++)
                    workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                     pk, ck, OVERSIZED_PADDING + "-r2-" + ck, ROUND2_TS);
                // the other side of the tie: same (pk, ck) and timestamp, different value
                workload.execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                 pk, tieCk, OVERSIZED_PADDING + "-tieB", TIE_TS);
            }
            workload.flush();
        }
    }

    /**
     * A table with more than 64 regular columns, past the point where the row encoding switches to the
     * large-superset column-subset representation that the 50-column {@link WideColumns} shape never reaches.
     * Round 1 sets every column. Round 2 rewrites subsets whose present-column counts straddle the
     * superset/2 subset-encoding boundary (34, 35, 36 of 70), so the merge unions cells under the large
     * encoding while the subset serialization flips between encoding present and absent columns.
     */
    private static final class SuperWideColumns extends BaseSchema
    {
        private static final int COLUMN_COUNT = 70;
        private static final int BASE_ROWS = 12;

        SuperWideColumns()
        {
            super("super-wide-columns", buildDefinition());
        }

        private static String buildDefinition()
        {
            StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint PRIMARY KEY");
            for (int i = 0; i < COLUMN_COUNT; i++)
                ddl.append(", c").append(i).append(" int");
            ddl.append(')');
            return ddl.toString();
        }

        /** An INSERT that sets columns {@code [0, present)} to their index. */
        private static String buildInsert(int present)
        {
            StringBuilder sql = new StringBuilder("INSERT INTO %s (pk");
            for (int i = 0; i < present; i++)
                sql.append(", c").append(i);
            sql.append(") VALUES (?");
            for (int i = 0; i < present; i++)
                sql.append(", ").append(i);
            sql.append(')');
            return sql.toString();
        }

        @Override
        public void write(DifferentialWorkload workload, int scale)
        {
            long rows = (long) BASE_ROWS * scale;

            // round 1: every one of the 70 columns set
            String fullInsert = buildInsert(COLUMN_COUNT);
            for (long pk = 0; pk < rows; pk++)
                workload.execute(fullInsert, pk);
            workload.flush();

            // round 2: rewrite subsets whose present-column counts straddle the 34/35/36 subset boundary.
            // Only three distinct statements occur, so build them once instead of per row.
            String[] subsetInserts = { buildInsert(34), buildInsert(35), buildInsert(36) };
            for (long pk = 0; pk < rows; pk++)
                workload.execute(subsetInserts[(int) (pk % 3)], pk);
            workload.flush();
        }
    }
}
