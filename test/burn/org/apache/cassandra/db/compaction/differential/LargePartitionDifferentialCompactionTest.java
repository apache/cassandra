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


import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertTrue;

/**
 * ONE giant partition, merged from many inputs — sized for the POSITIONAL boundaries that
 * row count alone never touches:
 *
 *  - intra-partition offsets crossing Integer.MAX_VALUE (2GiB) — int casts and vint widths
 *    on the partition-relative position arithmetic (previousUnfilteredSize chains, header
 *    lengths, index block offsets);
 *  - the promoted index at hundreds of thousands of index blocks in a single partition,
 *    orders of magnitude past the scale the index-promotion logic is normally exercised at;
 *  - small partitions on BOTH sides of the giant one in KEY order — but partitions are stored in
 *    TOKEN order, and under Murmur3 the tokens run pk 2 &lt; pk 3 &lt; pk 0 &lt; pk 1, so both
 *    small partitions precede the giant one and it is always LAST. What this exercises is
 *    per-partition state ENTERING the monster; demonstrating the reset AFTER it would need a key
 *    whose token exceeds pk 1's.
 *
 * Lives in test/burn, not test/unit — this genuinely crosses the 2GiB boundary by default now,
 * not just on an opt-in scaled run. Defaults use DISJOINT windows (ck_stride = rows_per_sstable)
 * to maximize merged partition size for a given row count: distinct rows = (sstables-1) *
 * ck_stride + rows_per_sstable — NOT sstables * rows_per_sstable (an earlier "2.6GiB" boundary
 * run conflated pre-merge input rows with merged output rows and only reached ~1.4GiB). At the
 * defaults below that's ~8.8M distinct rows at ~280B/row: a ~2.3GiB single MERGED partition
 * SERIALIZED, which is the size the intra-partition offset arithmetic sees and the only one this
 * class is about. On DISK it is far smaller — the padding is a repeated character and Data.db is
 * compressed, so a measured run wrote a 156MiB output sstable and peaked under 1GiB across the
 * whole test directory. Judge the workload from the "Compacted (…) N total partitions merged" line
 * and the parameter log below, never from megabytes on disk. The
 * memtable may auto-flush large rounds, so the sstables parameter is a minimum input count,
 * which the differential does not care about. All scale parameters remain property-configurable
 * via -Dtest.jvm.args, e.g. to shrink back down for a quick local check:
 *
 *   ant test-burn -Dtest.name=...LargePartitionDifferentialCompactionTest \
 *       -Dtest.timeout=14400000 \
 *       -Dtest.jvm.args="-Dcassandra.test.differential.largepartition.sstables=4
 *                        -Dcassandra.test.differential.largepartition.rows_per_sstable=250000
 *                        -Dcassandra.test.differential.largepartition.value_padding=120"
 */
public class LargePartitionDifferentialCompactionTest extends DifferentialCompactionTester
{

    private static final int SSTABLES =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_LARGEPARTITION_SSTABLES.getInt();
    private static final int ROWS_PER_SSTABLE =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_LARGEPARTITION_ROWS_PER_SSTABLE.getInt();
    private static final String VALUE_PADDING =
        "p".repeat(CassandraRelevantProperties.TEST_DIFFERENTIAL_LARGEPARTITION_VALUE_PADDING.getInt());
    /**
     * Window stride between rounds. Default: DISJOINT windows (equal to rows_per_sstable),
     * which maximizes the merged partition size — this class's whole point is crossing the
     * 2GiB boundary. Override down to rows_per_sstable / 2 for half-window overlap instead,
     * where every output row in the overlap merges from two inputs.
     */
    private static final long CK_STRIDE = Math.max(1,
        CassandraRelevantProperties.TEST_DIFFERENTIAL_LARGEPARTITION_CK_STRIDE.getInt(ROWS_PER_SSTABLE));

    /**
     * Reserved tie partition. The small side partitions are pk 0 and pk 2, the giant one is pk 1, and
     * every delete in the scenario targets pk 1 — so pk 3 is untouched and its one row is guaranteed to
     * reach the output. Written once per round at the explicit tie timestamp, it is an SSTABLES-way
     * same-timestamp tie across SSTABLES sstables, and unlike the bulk ties it exists at every
     * parameter setting, disjoint clustering windows included.
     */
    private static final long TIE_PK = 3;
    private static final long TIE_CK = 0;

    /**
     * v2 of the reserved tie row for {@code round}, zero-padded to a fixed width so that unsigned byte
     * order is DESCENDING in the round: the greatest value bytes belong to the FIRST round written. v1
     * ascends with the round instead, so a merge that resolved the tie by write order in either
     * direction — rather than per cell on the value bytes — cannot produce both.
     */
    private static String tieValue(int round)
    {
        return String.format("tie%010d", SSTABLES - 1 - round);
    }

    @Override
    protected boolean scaleCapture()
    {
        return true;
    }

    @Test
    public void giantPartition() throws Throwable
    {
        long distinctRows = (long) (SSTABLES - 1) * CK_STRIDE + ROWS_PER_SSTABLE;
        logger.info("large-partition parameters: sstables={} rowsPerSSTable={} ckStride={} valuePadding={}B " +
                    "-> ~{} distinct rows in one merged partition, ~{}MiB serialized",
                    SSTABLES, ROWS_PER_SSTABLE, CK_STRIDE, VALUE_PADDING.length(),
                    distinctRows,
                    distinctRows * (40 + VALUE_PADDING.length()) / (1 << 20));

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        assertTrue("the reserved tie needs at least two input sstables to be a cross-sstable tie: sstables=" +
                   SSTABLES, SSTABLES >= 2);

        String insert = "INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)";
        String insertTtl = insert + " USING TTL 86400";
        String insertTs = insert + " USING TIMESTAMP 5000";

        // A clustering of the giant partition written at the explicit tie timestamp in two ADJACENT
        // rounds is a genuine cross-sstable same-timestamp tie (rounds are sstables here, one flush
        // each). Counted from the same boolean the write branch uses, so a predicate that stops producing
        // ties fails the assertion below instead of passing silently. Two adjacent-round sets are enough
        // for existence — CK_STRIDE < ROWS_PER_SSTABLE makes consecutive windows overlap — and bound
        // the memory by one round's tie writes rather than by the whole run. Not tracked at all where
        // the windows are disjoint: no BULK row is overwritten then, so the reserved tie is the only
        // cross-sstable tie there. It has no tally of its own — an sstable-count precondition and an
        // absolute assertion on its resolved winner cover it instead.
        boolean overlappingWindows = CK_STRIDE < ROWS_PER_SSTABLE;
        Set<Long> tieCksThisRound = new HashSet<>();
        Set<Long> tieCksPrevRound = new HashSet<>();
        long adjacentRoundTieWrites = 0;

        for (int round = 0; round < SSTABLES; round++)
        {
            long ckBase = round * CK_STRIDE;

            // small partitions on both sides of the giant one in KEY order — both precede it by TOKEN,
            // which is the order they are stored in; see the class javadoc — every round
            execute(insert, 0L, (long) round, (long) round, "side" + round);
            execute(insert, 2L, (long) round, (long) round, "side" + round);
            // the reserved tie: same clustering, same timestamp, every round
            execute(insertTs, TIE_PK, TIE_CK, (long) round, tieValue(round));

            for (int j = 0; j < ROWS_PER_SSTABLE; j++)
            {
                long ck = ckBase + j;
                long v1 = ck * 31 + round;
                String v2 = j % 31 == 30 ? null : "v" + round + "_" + ck + VALUE_PADDING;
                boolean ttlWrite = j % 7 == 3;
                // ck, not j: the clustering window shifts by CK_STRIDE every round, so a predicate on the
                // in-round offset j can never select the same clustering in two rounds and produces ZERO
                // cross-sstable ties. A predicate on ck holds in every round whose window covers it.
                boolean tieWrite = !ttlWrite && ck % 13 == 7;
                if (ttlWrite)
                    execute(insertTtl, 1L, ck, v1, v2);
                else if (tieWrite)
                {
                    execute(insertTs, 1L, ck, v1, "tie" + round + "_" + ck + VALUE_PADDING);
                    if (overlappingWindows)
                    {
                        tieCksThisRound.add(ck);
                        if (tieCksPrevRound.contains(ck))
                            adjacentRoundTieWrites++;
                    }
                }
                else
                    execute(insert, 1L, ck, v1, v2);
            }

            // tombstones at various depths of the giant partition: bounded ranges inside
            // this round's window, an open-ended slice off its tail, scattered row deletes
            for (int i = 0; i < 10; i++)
            {
                long start = ckBase + (long) i * (ROWS_PER_SSTABLE / 12);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, start, start + 7);
            }
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, ckBase + ROWS_PER_SSTABLE - 11);
            for (int i = 0; i < 20; i++)
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, ckBase + (long) i * (ROWS_PER_SSTABLE / 21));

            flush();
            tieCksPrevRound = tieCksThisRound;
            tieCksThisRound = new HashSet<>();
            logger.info("large-partition round {}/{} flushed", round + 1, SSTABLES);
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);

        // Disjoint windows (ck_stride == rows_per_sstable — the 2GiB boundary run) overwrite no bulk
        // row, so the reserved tie is the only cross-sstable tie; assert the bulk count only where the
        // windows overlap.
        if (overlappingWindows)
        {
            logger.info("cross-sstable same-timestamp tie writes in the giant partition (clusterings " +
                        "written at the tie timestamp in two adjacent rounds): {}", adjacentRoundTieWrites);
            assertTrue("no cross-sstable same-timestamp tie was written despite overlapping clustering " +
                       "windows: the tie predicate must be a function of ck, not of the in-round offset j, " +
                       "or the shifting window never selects a clustering twice (adjacentRoundTieWrites=" +
                       adjacentRoundTieWrites + ')', adjacentRoundTieWrites > 0);
        }
        else
        {
            logger.info("disjoint clustering windows (ckStride={} >= rowsPerSSTable={}): the bulk rows " +
                        "are never overwritten, so the only cross-sstable tie is the reserved one",
                        CK_STRIDE, ROWS_PER_SSTABLE);
        }

        // ABSOLUTE: at equal timestamps the greater raw value bytes win, decided PER CELL — so the
        // reserved tie must keep v1 from the LAST round and v2 from the FIRST. Byte equality between the
        // two paths cannot see a tie-break they both get wrong, and the count above only proves ties were
        // written, not that one was decided. The read lands on a compaction output rather than on a
        // read-time merge of the inputs: the cross-generation rung commits a real cursor compaction and
        // the live set is its output (scale mode makes the logical dump a digest, so the JSON-based
        // absolute assertions the unit scenarios use are not available here).
        assertRows(execute("SELECT v1, v2 FROM %s WHERE pk = ? AND ck = ?", TIE_PK, TIE_CK),
                   row((long) (SSTABLES - 1), tieValue(0)));
    }
}
