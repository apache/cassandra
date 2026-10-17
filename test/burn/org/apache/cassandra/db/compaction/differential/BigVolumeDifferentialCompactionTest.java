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
 * Compacts 40 million written rows from 20 input sstables. The test writes 20,000 partitions of
 * 100 rows in each of 20 rounds, and flushes after each round.
 *
 * Each round writes a window of clusterings that covers half of the window of the round before
 * it, so most output rows really merge from two or three inputs. One partition is kept apart and
 * holds a single row, for the timestamp tie: see TIE_PK.
 *
 * With the default settings the writes are mixed as follows:
 * <ul>
 *   <li>about 5 percent multi-cell map cells;</li>
 *   <li>about 14 percent rows with a TTL;</li>
 *   <li>6.7 percent of writes at the tie timestamp;</li>
 *   <li>about 3 percent cell tombstones, made by an overwrite with null;</li>
 *   <li>row deletes in each round;</li>
 *   <li>range deletes, both bounded and open at one end;</li>
 *   <li>partition deletes that repeat, with data written again afterwards.</li>
 * </ul>
 *
 * The tie timestamp and the covering windows together give a same-timestamp tie across sstables
 * at 50 of the 1012 distinct clusterings of each bulk partition. The tie-break decides 49 of
 * them. The other one also takes a TTL write in a third round, which wins on timestamp. Across
 * the table that gives about 1 million rows with a tie, of which the tie-break decides 980,000.
 *
 * The test runs in scale-capture mode: see DifferentialCompactionTester.scaleCapture. The dump
 * of each output goes into a SHA-256 digest, and the byte comparison reads a stream, so the
 * memory the harness uses stays constant. It compacts twice, as the other tests do.
 *
 * This test is in test/burn, and not in test/unit, because it runs a compaction of several
 * gigabytes that takes minutes. It is not a unit test.
 *
 * You can change every scale value with a system property. The defaults give the run of 40
 * million rows and 20 sstables above. The properties must reach the forked test JVM through
 * -Dtest.jvm.args:
 *
 *   ant test-burn -Dtest.name=...BigVolumeDifferentialCompactionTest \
 *       -Dtest.timeout=14400000 \
 *       -Dtest.jvm.args="-Dcassandra.test.differential.bigvolume.rounds=40
 *                        -Dcassandra.test.differential.bigvolume.partitions=10000
 *                        -Dcassandra.test.differential.bigvolume.rows_per_round=100
 *                        -Dcassandra.test.differential.bigvolume.value_padding=200"
 *
 *  - rounds          = number of input sstables (one flush per round)
 *  - partitions      = partitions per round
 *  - rows_per_round  = rows per partition per round (total rows = rounds x partitions x this, plus
 *                      one reserved tie row per round in partition PARTITIONS; see TIE_PK)
 *  - value_padding   = extra bytes appended to every text value (row size knob)
 *
 * Delete counts scale with the partition count; the clustering stride is derived so
 * consecutive rounds always overlap by roughly half a window.
 */
public class BigVolumeDifferentialCompactionTest extends DifferentialCompactionTester
{

    private static final int ROUNDS = CassandraRelevantProperties.TEST_DIFFERENTIAL_BIGVOLUME_ROUNDS.getInt();
    private static final int PARTITIONS = CassandraRelevantProperties.TEST_DIFFERENTIAL_BIGVOLUME_PARTITIONS.getInt();
    private static final int ROWS_PER_ROUND = CassandraRelevantProperties.TEST_DIFFERENTIAL_BIGVOLUME_ROWS_PER_ROUND.getInt();
    private static final String VALUE_PADDING =
        "p".repeat(CassandraRelevantProperties.TEST_DIFFERENTIAL_BIGVOLUME_VALUE_PADDING.getInt());
    /** < ROWS_PER_ROUND so consecutive rounds overlap by roughly half a window (100 -> 48). */
    private static final int CK_STRIDE = Math.max(1, ROWS_PER_ROUND / 2 - 2);

    /**
     * Reserved tie partition, outside {@code [0, PARTITIONS)} — which is the whole space every delete
     * in the workload can reach: the row, bounded-range and open-ended-range deletes all take their
     * partition key modulo PARTITIONS, and the cycling partition delete stays inside
     * {@code [max(0, PARTITIONS - 5), max(0, PARTITIONS - 5) + min(5, PARTITIONS))}. Its one row is
     * written once per round at the explicit tie timestamp, so it is a ROUNDS-way same-timestamp tie
     * across ROUNDS sstables whose winner is guaranteed to reach the output.
     */
    private static final long TIE_PK = PARTITIONS;
    private static final long TIE_CK = 0;

    /**
     * v2 of the reserved tie row for {@code round}, zero-padded to a fixed width so that unsigned byte
     * order is DESCENDING in the round: the greatest value bytes belong to the FIRST round written. v1
     * ascends with the round instead, so a merge that resolved the tie by write order in either
     * direction — rather than per cell on the value bytes — cannot produce both.
     */
    private static String tieValue(int round)
    {
        return String.format("tie%010d", ROUNDS - 1 - round);
    }

    @Override
    protected boolean scaleCapture()
    {
        return true;
    }

    @Test
    public void fortyMillionRowsTwentySSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, m map<text, bigint>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        assertTrue("the reserved tie needs at least two input sstables to be a cross-sstable tie: rounds=" +
                   ROUNDS, ROUNDS >= 2);
        logger.info("big-volume parameters: rounds={} partitions={} rowsPerRound={} valuePadding={}B " +
                    "ckStride={} -> {} total rows across {} input sstables, plus one reserved tie row " +
                    "per round in partition {}",
                    ROUNDS, PARTITIONS, ROWS_PER_ROUND, VALUE_PADDING.length(), CK_STRIDE,
                    (long) ROUNDS * PARTITIONS * ROWS_PER_ROUND, ROUNDS, TIE_PK);

        String insert = "INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)";
        String insertTtl = insert + " USING TTL 86400";
        String insertTs = insert + " USING TIMESTAMP 5000";
        String insertMap = "INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?)";

        // A clustering written at the explicit tie timestamp in two ADJACENT rounds is a genuine
        // cross-sstable same-timestamp tie (rounds are sstables here, one flush each). Counted from the
        // same boolean the write branch uses, so a predicate that stops producing ties fails the
        // assertion below instead of passing silently. Two adjacent-round sets are enough for existence
        // — CK_STRIDE < ROWS_PER_ROUND makes consecutive windows overlap — and bound the memory by one
        // round's tie writes rather than by the whole run. One partition is enough: the predicate does
        // not read pk, and within a round a clustering is written at most once. Not tracked at all
        // where the windows are disjoint: no BULK row is overwritten then, so the reserved tie is the
        // only cross-sstable tie there. It has no tally of its own — a round-count precondition and an
        // absolute assertion on its resolved winner cover it instead.
        boolean overlappingWindows = CK_STRIDE < ROWS_PER_ROUND;
        Set<Long> tieCksThisRound = new HashSet<>();
        Set<Long> tieCksPrevRound = new HashSet<>();
        long adjacentRoundTieWrites = 0;

        for (int round = 0; round < ROUNDS; round++)
        {
            long ckBase = (long) round * CK_STRIDE;
            for (long pk = 0; pk < PARTITIONS; pk++)
            {
                for (int j = 0; j < ROWS_PER_ROUND; j++)
                {
                    long ck = ckBase + j;
                    long v1 = ck * 31 + round;
                    String v2 = j % 31 == 30 ? null : "v" + round + "_" + ck + VALUE_PADDING;
                    boolean mapWrite = j % 20 == 5;
                    boolean ttlWrite = !mapWrite && j % 7 == 3;
                    // ck, not j: the clustering window shifts by CK_STRIDE every round, so a predicate
                    // on the in-round offset j can never select the same clustering in two rounds and
                    // produces ZERO cross-sstable ties. A predicate on ck holds in every round whose
                    // window covers it.
                    boolean tieWrite = !mapWrite && !ttlWrite && ck % 13 == 7;
                    if (mapWrite)
                        execute(insertMap, pk, ck, v1, v2, map("k" + (ck % 3), ck, "r", (long) round));
                    else if (ttlWrite)
                        execute(insertTtl, pk, ck, v1, v2);
                    else if (tieWrite)
                    {
                        execute(insertTs, pk, ck, v1, "tie" + round + "_" + ck + VALUE_PADDING);
                        if (pk == 0 && overlappingWindows)
                        {
                            tieCksThisRound.add(ck);
                            if (tieCksPrevRound.contains(ck))
                                adjacentRoundTieWrites++;
                        }
                    }
                    else
                        execute(insert, pk, ck, v1, v2);
                }
            }

            // the reserved tie: same clustering, same timestamp, every round
            execute(insertTs, TIE_PK, TIE_CK, (long) round, tieValue(round));

            // delete counts scale with the partition count (defaults: 1000 / 303 / 200)
            for (int i = 0; i < Math.max(1, PARTITIONS / 20); i++)
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?",
                        (long) ((round * 97 + i * 13) % PARTITIONS), ckBase + (i % ROWS_PER_ROUND));
            for (int i = 0; i < Math.max(1, PARTITIONS / 66); i++)
            {
                long start = ckBase + (i % 40);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                        (long) ((round * 53 + i * 29) % PARTITIONS), start, start + 4);
            }
            for (int i = 0; i < Math.max(1, PARTITIONS / 100); i++)
            {
                long pk = (round * 41 + i * 17) % PARTITIONS;
                if (i % 2 == 0)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", pk, ckBase + ROWS_PER_ROUND / 2);
                else
                    execute("DELETE FROM %s WHERE pk = ? AND ck <= ?", pk, ckBase + 5);
            }
            // cycling partition deletes over the same 5 partitions: tombstone + resurrection
            execute("DELETE FROM %s WHERE pk = ?", (long) (Math.max(0, PARTITIONS - 5) + round % Math.min(5, PARTITIONS)));

            flush();
            tieCksPrevRound = tieCksThisRound;
            tieCksThisRound = new HashSet<>();
            logger.info("big-volume round {}/{} flushed", round + 1, ROUNDS);
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);

        if (overlappingWindows)
        {
            logger.info("cross-sstable same-timestamp tie writes (clusterings written at the tie timestamp " +
                        "in two adjacent rounds): {} per partition, in each of {} partitions",
                        adjacentRoundTieWrites, PARTITIONS);
            assertTrue("no cross-sstable same-timestamp tie was written despite overlapping clustering " +
                       "windows: the tie predicate's clustering selection must key on ck rather than on " +
                       "the in-round offset j, or the shifting window never selects a clustering twice " +
                       "(adjacentRoundTieWrites=" +
                       adjacentRoundTieWrites + ')', adjacentRoundTieWrites > 0);
        }
        else
        {
            logger.info("disjoint clustering windows (ckStride={} >= rowsPerRound={}): the bulk rows are " +
                        "never overwritten, so the only cross-sstable tie is the reserved one",
                        CK_STRIDE, ROWS_PER_ROUND);
        }

        // ABSOLUTE: at equal timestamps the greater raw value bytes win, decided PER CELL — so the
        // reserved tie must keep v1 from the LAST round and v2 from the FIRST. Byte equality between the
        // two paths cannot see a tie-break they both get wrong, and the tally above only proves ties were
        // written, not that one was decided. The read lands on a compaction output rather than on a
        // read-time merge of the inputs: the cross-generation rung commits a real cursor compaction and
        // the live set is its output (scale mode makes the logical dump a digest, so the JSON-based
        // absolute assertions the unit scenarios use are not available here).
        assertRows(execute("SELECT v1, v2 FROM %s WHERE pk = ? AND ck = ?", TIE_PK, TIE_CK),
                   row((long) (ROUNDS - 1), tieValue(0)));
    }
}
