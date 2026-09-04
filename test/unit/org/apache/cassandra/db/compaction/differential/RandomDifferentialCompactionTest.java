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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.AbstractTypeGenerators.ValueDomain;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Generators;

import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Randomized soak for the cursor-vs-iterator differential harness: random schemas restricted to
 * the currently supported cursor compaction surface (the same unsupportedMetadata filter
 * production uses), random multi-round workloads with overwrites and deletes flushed into
 * overlapping sstables, then byte+logical differential comparison of both compaction paths.
 *
 * Workload space: INSERT / UPDATE (no row liveness) / primary-key-only INSERT (liveness, no
 * cells) / INSERT USING TTL (long-lived, far from the expiry boundary) / explicit USING
 * TIMESTAMP collisions from a small pool (same-timestamp tie-breaks); null values on
 * non-key columns (cell tombstones on simple columns; mapped to empty buffers on clustering
 * columns); row deletes, partition deletes, single-sided and prefix range deletes,
 * multi-column cell deletes, static cell deletes. Composite partition keys (1-2 components).
 * Plus one designated same-timestamp tie per example — one primary key rewritten once per round
 * at a single timestamp — so the tie-break is reached without depending on the random draw.
 *
 * Example count is property-gated: -Dcassandra.test.differential.examples=N (default
 * {@value #DEFAULT_EXAMPLES}; a full validation run uses thousands).
 *
 * Reproducing a failure: every failure message is wrapped in a seed; rerun with
 * -Dcassandra.test.differential.seed=N (the failing seed becomes example 0), or plug it
 * into {@code withFixedSeed} below.
 *
 * Known coverage gaps (deliberate, covered by the deterministic corpus): EMPTY_BYTES value
 * domain (invalid CQL for some generated multi-cell shapes), collection-element deletes
 * (DELETE m['k'] needs element values of the right type), expired TTLs (timing-dependent).
 */
public class RandomDifferentialCompactionTest extends DifferentialCompactionTester
{
    static
    {
        // make sure generated blobs are deterministic per seed
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);
    }

    private static final int DEFAULT_EXAMPLES = 10;
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_DIFFERENTIAL_EXAMPLES.getInt(DEFAULT_EXAMPLES);

    /** Long enough that expiry can never fall between the two differential runs. */
    private static final int SOAK_TTL_SECONDS = 30 * 24 * 60 * 60;

    /**
     * Base of the small explicit-timestamp pool, deliberately ABOVE the wall clock. CQL timestamps are
     * MICROSECONDS since the epoch, so a pool near 1e6 is 1970. Such a pool loses on timestamp to every
     * wall-clock write this workload makes to the same primary key, which leaves the tie-break under test
     * unable to influence an output byte at all. The expression reads the clock instead of a literal, so
     * the base cannot silently fall behind it. It truncates to a whole day, so all runs on one day share
     * one pool and a pinned seed still reproduces the same timestamps. A year of headroom covers a long
     * soak and a skewed clock. No guardrail rejects a future timestamp: maximum_timestamp_warn_threshold
     * and maximum_timestamp_fail_threshold both default to null. The consequence to accept is that
     * wall-clock DELETEs cannot shadow these rows. A tie-break can only be observed if the tied writes
     * win.
     */
    private static final long TIE_POOL_BASE =
        TimeUnit.DAYS.toMicros(TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis()) + 365);

    /** Width of the pool the random workload draws from; the designated tie sits just above it. */
    private static final int TIE_POOL_WIDTH = 3;

    @Test
    public void randomizedDifferential() throws Throwable
    {
        // a zero or negative example count would make SeedRunner.run iterate zero times and the
        // test pass having compared nothing
        assertTrue("cassandra.test.differential.examples must be > 0, got " + EXAMPLES, EXAMPLES > 0);
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        assertTrue("the explicit-timestamp pool must sit above the wall clock: TIE_POOL_BASE=" +
                   TIE_POOL_BASE + " nowMicros=" + nowMicros, TIE_POOL_BASE > nowMicros);
        new SeedRunner(EXAMPLES).run(this::runOneExample);
    }

    private void runOneExample(long seed) throws Throwable
    {
        JavaRandom qtRandom = new JavaRandom(seed);
        Random workload = new Random(seed);

        Gen<String> udtName = Generators.unique(IDENTIFIER_GEN);
        TypeGenBuilder safePrimary = AbstractTypeGenerators.withoutUnsafeEquality().withUDTNames(udtName);
        TableMetadata metadata;
        do
        {
            metadata = new TableMetadataBuilder()
                       .withKeyspaceName(KEYSPACE)
                       .withTableKinds(TableMetadata.Kind.REGULAR)
                       .withKnownMemtables()
                       .withDefaultTypeGen(AbstractTypeGenerators.builder()
                                                                 .withoutEmpty()
                                                                 .withMaxDepth(2)
                                                                 .withDefaultSetKey(safePrimary)
                                                                 .withoutTypeKinds(AbstractTypeGenerators.TypeKind.COUNTER)
                                                                 .withUDTNames(udtName))
                       .withPartitionColumnsBetween(1, 2)
                       .withPrimaryColumnTypeGen(new TypeGenBuilder(safePrimary).withMaxDepth(1))
                       .withClusteringColumnsBetween(0, 3)
                       .withRegularColumnsBetween(1, 5)
                       .withStaticColumnsBetween(0, 2)
                       .build(qtRandom);
        }
        // unsupportedMetadata is the same filter production uses to route to the cursor pipeline.
        // The second condition rejects invalid CQL the generator can produce: static columns
        // require clustering columns.
        while (CursorCompactor.unsupportedMetadata(metadata)
               || (metadata.clusteringColumns().isEmpty() && !metadata.staticColumns().isEmpty()));

        maybeCreateUDTs(metadata);
        String createTableCql = metadata.toCqlString(true, false, false)
                                        .replaceAll("org.apache.cassandra.db.marshal.", "");
        logger.info("randomizedDifferential seed={} schema:\n{}", seed, createTableCql);
        createTable(KEYSPACE, createTableCql);
        // the CQL embeds the generator's table name; createTable's returned name is not it
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, metadata.name);
        cfs.disableAutoCompaction();

        // ~12% of non-key values are null: cell tombstones on simple columns. The data generator
        // maps null to an empty buffer on clustering columns, because null clustering is invalid and
        // empty is legal. That exercises the empty-vs-valued clustering comparison. The generator
        // never applies the domain to partition keys.
        Gen<ValueDomain> valueDomains = SourceDSL.integers().between(0, 99)
                                                 .map(i -> i < 12 ? ValueDomain.NULL : ValueDomain.NORMAL);
        Gen<ByteBuffer[]> dataGen = CassandraGenerators.data(metadata, valueDomains);

        int partitionColumnCount = metadata.partitionKeyColumns().size();
        int clusteringColumnCount = metadata.clusteringColumns().size();
        int primaryColumnCount = partitionColumnCount + clusteringColumnCount;
        String insertStmt = insertStmt(metadata);
        String deleteRowStmt = deleteStmt(metadata, primaryColumnCount);
        String deletePartitionStmt = deleteStmt(metadata, partitionColumnCount);

        // select-order index of every column, for UPDATE/cell-delete binding
        Map<String, Integer> selectOrderIndex = new HashMap<>();
        {
            Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
            for (int i = 0; it.hasNext(); i++)
                selectOrderIndex.put(it.next().name.toString(), i);
        }
        List<ColumnMetadata> regularColumns = ImmutableList.copyOf(metadata.regularColumns());
        List<ColumnMetadata> staticColumns = ImmutableList.copyOf(metadata.staticColumns());

        List<ByteBuffer[]> rows = new ArrayList<>();
        int rounds = 2 + workload.nextInt(3); // 2-4 sstables
        // which round wins the designated tie; see the arrangement loop below
        int tieWinnerOffset = workload.nextInt(rounds);

        // One DETERMINISTIC cross-sstable same-timestamp tie per example: the same primary key written
        // once per round with fresh values at one timestamp. The tie-break is then reached in EVERY
        // example, instead of only when the random draw happens to collide on a key. Its timestamp sits
        // just above the pool the random workload draws from, so a pooled write that lands on the same
        // key loses on timestamp rather than joining the tie; low cardinality key types make such a write
        // likely. The whole pool sits above the wall clock, so no wall-clock write or delete can shadow
        // the tie either. The tie row never enters `rows`, so none of the delete loops below can target it.
        long tieTimestamp = TIE_POOL_BASE + TIE_POOL_WIDTH;
        // no value domain: every tie candidate is a live cell, so the value comparison alone decides
        // the winner, not the tombstone-or-expiring-beats-live branch of
        // CellLivenessInfo.resolveSameTimestampTie on a NULL column
        Gen<ByteBuffer[]> tieDataGen = CassandraGenerators.data(metadata, null);
        ByteBuffer[] tieKey = tieDataGen.generate(qtRandom);
        List<ByteBuffer[]> tieWrites = new ArrayList<>();
        for (int round = 0; round < rounds; round++)
        {
            ByteBuffer[] tieRow = tieDataGen.generate(qtRandom);
            System.arraycopy(tieKey, 0, tieRow, 0, primaryColumnCount);
            tieWrites.add(tieRow);
        }
        // Spread the winners ACROSS rounds: column c's greatest bytes are arranged into round
        // (c + tieWinnerOffset) % rounds. Leaving the maxima wherever the generator put them makes the
        // assertion below bite only by luck, because a merge that kept the last writer agrees with the
        // rule about 1/rounds of the time per column. Putting them all in ONE round would instead make a
        // merge that kept that round pass every time. With the spread, no rule of the form "always keep
        // sstable k" can agree with the real rule on a schema with two or more regular columns. The
        // per-example offset means such a rule cannot agree on a single-regular-column schema either
        // without getting lucky in every example. The offset is drawn from `workload`, so a pinned seed
        // still reproduces the arrangement.
        for (int c = 0; c < regularColumns.size(); c++)
        {
            int valueIndex = selectOrderIndex.get(regularColumns.get(c).name.toString());
            int winner = (c + tieWinnerOffset) % rounds;
            for (int round = 0; round < rounds; round++)
            {
                if (round != winner
                    && ByteBufferUtil.compareUnsigned(tieWrites.get(round)[valueIndex],
                                                      tieWrites.get(winner)[valueIndex]) > 0)
                {
                    ByteBuffer greater = tieWrites.get(round)[valueIndex];
                    tieWrites.get(round)[valueIndex] = tieWrites.get(winner)[valueIndex];
                    tieWrites.get(winner)[valueIndex] = greater;
                }
            }
        }

        for (int round = 0; round < rounds; round++)
        {
            // Watermark taken at ROUND start: the explicit-timestamp branch below draws its overwrite
            // target only from `rows` BELOW this index, that is, from a key written in an EARLIER round.
            // Every iteration appends to `rows`, including earlier iterations of this round. Drawing from
            // all of `rows` therefore lets both writes land in one memtable. The memtable reconciles them
            // and the compactor's tie-break is never reached.
            int rowsBeforeThisRound = rows.size();
            int inserts = 15 + workload.nextInt(26); // 15-40 rows
            for (int i = 0; i < inserts; i++)
            {
                ByteBuffer[] row = dataGen.generate(qtRandom);
                boolean overwrite = !rows.isEmpty() && workload.nextInt(100) < 30;
                if (overwrite)
                {
                    // overwrite: keep a previously used primary key, fresh non-key values —
                    // this is what makes the merge actually reconcile rather than concatenate
                    ByteBuffer[] prev = rows.get(workload.nextInt(rows.size()));
                    System.arraycopy(prev, 0, row, 0, primaryColumnCount);
                }

                int mode = workload.nextInt(100);
                if (overwrite && rowsBeforeThisRound > 0 && workload.nextInt(100) < 40)
                {
                    // explicit-timestamp collision candidate: re-keyed onto a row from an EARLIER round,
                    // and stamped from a pool offset DERIVED from the primary key. Two pooled writes to
                    // one key therefore land on the SAME timestamp, rather than merely being ordered by
                    // it. The pair that ties is two pooled writes. The earlier-round target only
                    // guarantees this write lands in a later sstable than the row it re-keys onto, which
                    // is what lets a tie form across sstables instead of inside one memtable. Guaranteed
                    // coverage comes from the designated tie below, not from this draw.
                    ByteBuffer[] prev = rows.get(workload.nextInt(rowsBeforeThisRound));
                    System.arraycopy(prev, 0, row, 0, primaryColumnCount);
                    long ts = TIE_POOL_BASE + Math.floorMod(primaryKeyHash(row, primaryColumnCount), TIE_POOL_WIDTH);
                    execute(insertStmt + " USING TIMESTAMP " + ts, (Object[]) row);
                }
                else if (mode < 15 && !regularColumns.isEmpty())
                {
                    // UPDATE: writes cells without primary-key liveness (different row flags)
                    execute(updateStmt(metadata, regularColumns),
                            updateParams(row, regularColumns, selectOrderIndex, primaryColumnCount));
                }
                else if (mode < 22)
                {
                    // primary-key-only INSERT: row liveness with zero cells
                    execute(pkOnlyInsertStmt(metadata), (Object[]) Arrays.copyOf(row, primaryColumnCount));
                }
                else if (mode < 30)
                {
                    // long TTL: liveness info with ttl + expiration far from the runs
                    execute(insertStmt + " USING TTL " + SOAK_TTL_SECONDS, (Object[]) row);
                }
                else
                {
                    execute(insertStmt, (Object[]) row);
                }
                rows.add(row);
            }

            // row deletes against known keys
            for (int i = 0; i < 3 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                execute(deleteRowStmt, (Object[]) Arrays.copyOf(victim, primaryColumnCount));
            }

            // range deletes (clustering tables only): single-sided slices and clustering-prefix
            // deletes against known keys; single-sided bounds cannot produce inverted ranges
            for (int i = 0; i < 2 && clusteringColumnCount > 0 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                if (clusteringColumnCount >= 2 && workload.nextBoolean())
                {
                    // prefix delete: equality on a strict prefix of the clustering columns
                    int depth = 1 + workload.nextInt(clusteringColumnCount - 1);
                    execute(deleteStmt(metadata, partitionColumnCount + depth),
                            (Object[]) Arrays.copyOf(victim, partitionColumnCount + depth));
                }
                else
                {
                    int eqDepth = workload.nextInt(clusteringColumnCount);
                    String op = new String[]{ ">=", ">", "<=", "<" }[workload.nextInt(4)];
                    execute(rangeDeleteStmt(metadata, eqDepth, op),
                            (Object[]) Arrays.copyOf(victim, partitionColumnCount + eqDepth + 1));
                }
            }

            // cell deletes: random subset of regular columns at a known row; occasionally a
            // static cell delete instead
            for (int i = 0; i < 2 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                if (!staticColumns.isEmpty() && workload.nextInt(100) < 30)
                {
                    ColumnMetadata col = staticColumns.get(workload.nextInt(staticColumns.size()));
                    execute(cellDeleteStmt(metadata, List.of(col), partitionColumnCount),
                            (Object[]) Arrays.copyOf(victim, partitionColumnCount));
                }
                else
                {
                    List<ColumnMetadata> subset = randomSubset(regularColumns, workload);
                    execute(cellDeleteStmt(metadata, subset, primaryColumnCount),
                            (Object[]) Arrays.copyOf(victim, primaryColumnCount));
                }
            }

            // occasional partition delete
            if (workload.nextInt(100) < 40 && !rows.isEmpty())
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                execute(deletePartitionStmt, (Object[]) Arrays.copyOf(victim, partitionColumnCount));
            }

            // the designated tie: the same primary key, the same timestamp, fresh values, once per round
            execute(insertStmt + " USING TIMESTAMP " + tieTimestamp, (Object[]) tieWrites.get(round));

            flush(KEYSPACE, metadata.name);
        }

        assertCursorMatchesIterator(cfs);

        // The tie candidates must be in DIFFERENT sstables. A memtable reconciles two writes to one key
        // itself, so the compactor's tie-break is never reached inside one memtable. One tie write per
        // round and one flush per round makes that structural. This assertion observes the flush half of
        // it: autocompaction is off and an auto-flush can only ADD sstables, so moving the per-round flush
        // to after the round loop fails here. Dropping the flush outright fails earlier, in compactPath's
        // "scenario produced no input sstables". Hoisting the tie write out of the round loop is caught by
        // the per-column assertion below, not by this one.
        int inputSSTables = cfs.getLiveSSTables().size();
        assertTrue("one flush per round must leave one input sstable per round, got " + inputSSTables +
                   " for " + rounds + " rounds", inputSSTables >= rounds);
        // The differential restores its own inputs, so commit one real cursor compaction and read the tied
        // row back out of it: querying before this would merge the inputs at READ time and say nothing
        // about what compaction wrote.
        commitCompaction(cfs, cfs.getLiveSSTables(), true, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
        UntypedResultSet tieResult = execute(selectStmt(metadata, regularColumns),
                                            (Object[]) Arrays.copyOf(tieKey, primaryColumnCount));
        assertEquals("the designated tie row must survive compaction: it is written above the wall clock, " +
                     "so no wall-clock write or delete in this example can shadow it", 1, tieResult.size());
        UntypedResultSet.Row survivor = tieResult.one();
        // For a simple column, the cell with the greater value bytes wins a tie of equal
        // timestamps. This is the last rule of resolveRegular, and it is an unsigned comparison of
        // the whole value.
        //
        // The loop below skips the complex columns. A tie on a complex column resolves separately
        // for each cell path, and the result holds the paths of both writes. It does not compare the
        // whole value of one write against the whole value of the other, which is the only rule this
        // test models. The EdgeCase and Pathological tests cover ties on complex columns.
        //
        // The arrangement loop above put each column's winner in a different round, so a merge that
        // resolved this tie by write order fails on any column whose candidate values are not all
        // equal. A low-cardinality column type can make them equal, which costs an example rather
        // than producing a false failure. Byte equality between the two paths cannot see a rule they
        // both get wrong.
        for (ColumnMetadata col : regularColumns)
        {
            if (col.isComplex())
                continue;
            int valueIndex = selectOrderIndex.get(col.name.toString());
            ByteBuffer expected = tieWrites.get(0)[valueIndex];
            for (ByteBuffer[] candidate : tieWrites)
                if (ByteBufferUtil.compareUnsigned(candidate[valueIndex], expected) > 0)
                    expected = candidate[valueIndex];
            assertEquals("the greater raw value bytes must win the same-timestamp tie on " + col.name +
                         " (" + tieWrites.size() + " candidates at timestamp " + tieTimestamp + ')',
                         expected, survivor.getBytes(col.name.toString()));
        }
    }

    /** Appends {@code columns[i].name}, joined by separator. */
    private static void appendNames(StringBuilder sb, List<ColumnMetadata> columns, String separator)
    {
        for (int i = 0; i < columns.size(); i++)
        {
            if (i > 0) sb.append(separator);
            sb.append(columns.get(i).name.toCQLString());
        }
    }

    /** Appends "name = ?" equality predicates over columns, joined by separator. */
    private static void appendEqPredicates(StringBuilder sb, List<ColumnMetadata> columns, String separator)
    {
        for (int i = 0; i < columns.size(); i++)
        {
            if (i > 0) sb.append(separator);
            sb.append(columns.get(i).name.toCQLString()).append(" = ?");
        }
    }

    /** Appends {@code count} "?" placeholders, joined by ", ". */
    private static void appendPlaceholders(StringBuilder sb, int count)
    {
        for (int i = 0; i < count; i++)
        {
            if (i > 0) sb.append(", ");
            sb.append('?');
        }
    }

    private static String insertStmt(TableMetadata metadata)
    {
        List<ColumnMetadata> cols = ImmutableList.copyOf(metadata.allColumnsInSelectOrder());
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(metadata).append(" (");
        appendNames(sb, cols, ", ");
        sb.append(") VALUES (");
        appendPlaceholders(sb, cols.size());
        return sb.append(')').toString();
    }

    /** INSERT binding only the primary key columns: row liveness without any cells. */
    private static String pkOnlyInsertStmt(TableMetadata metadata)
    {
        List<ColumnMetadata> keys = primaryKeyColumns(metadata);
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(metadata).append(" (");
        appendNames(sb, keys, ", ");
        sb.append(") VALUES (");
        appendPlaceholders(sb, keys.size());
        return sb.append(')').toString();
    }

    /** UPDATE setting every regular column: cells without primary-key liveness. */
    private static String updateStmt(TableMetadata metadata, List<ColumnMetadata> regularColumns)
    {
        StringBuilder sb = new StringBuilder("UPDATE ").append(metadata).append(" SET ");
        appendEqPredicates(sb, regularColumns, ", ");
        sb.append(" WHERE ");
        appendEqPredicates(sb, primaryKeyColumns(metadata), " AND ");
        return sb.toString();
    }

    private static Object[] updateParams(ByteBuffer[] row, List<ColumnMetadata> regularColumns,
                                         Map<String, Integer> selectOrderIndex, int primaryColumnCount)
    {
        Object[] params = new Object[regularColumns.size() + primaryColumnCount];
        for (int i = 0; i < regularColumns.size(); i++)
            params[i] = row[selectOrderIndex.get(regularColumns.get(i).name.toString())];
        for (int i = 0; i < primaryColumnCount; i++)
            params[regularColumns.size() + i] = row[i];
        return params;
    }

    /** DELETE col1, col2 FROM t WHERE first {@code keyColumnCount} primary key columns bound. */
    private static String cellDeleteStmt(TableMetadata metadata, List<ColumnMetadata> columns, int keyColumnCount)
    {
        StringBuilder sb = new StringBuilder("DELETE ");
        appendNames(sb, columns, ", ");
        sb.append(" FROM ").append(metadata).append(" WHERE ");
        appendEqPredicates(sb, primaryKeyColumns(metadata).subList(0, keyColumnCount), " AND ");
        return sb.toString();
    }

    /**
     * DELETE with equality on the partition key plus the first {@code eqDepth} clustering
     * columns, and a single-sided {@code op} bound on clustering column {@code eqDepth}.
     */
    private static String rangeDeleteStmt(TableMetadata metadata, int eqDepth, String op)
    {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(metadata).append(" WHERE ");
        List<ColumnMetadata> keys = primaryKeyColumns(metadata);
        int partitionColumnCount = metadata.partitionKeyColumns().size();
        int bound = partitionColumnCount + eqDepth;
        appendEqPredicates(sb, keys.subList(0, bound), " AND ");
        sb.append(" AND ").append(keys.get(bound).name.toCQLString()).append(' ').append(op).append(" ?");
        return sb.toString();
    }

    private static List<ColumnMetadata> primaryKeyColumns(TableMetadata metadata)
    {
        return ImmutableList.<ColumnMetadata>builder()
                            .addAll(metadata.partitionKeyColumns())
                            .addAll(metadata.clusteringColumns())
                            .build();
    }

    private static List<ColumnMetadata> randomSubset(List<ColumnMetadata> columns, Random workload)
    {
        List<ColumnMetadata> shuffled = new ArrayList<>(columns);
        java.util.Collections.shuffle(shuffled, workload);
        return shuffled.subList(0, 1 + workload.nextInt(shuffled.size()));
    }

    /** DELETE with the first {@code keyColumnCount} primary key columns bound (partition or full row). */
    private static String deleteStmt(TableMetadata metadata, int keyColumnCount)
    {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(metadata).append(" WHERE ");
        appendEqPredicates(sb, primaryKeyColumns(metadata).subList(0, keyColumnCount), " AND ");
        return sb.toString();
    }

    /** SELECT of {@code columns} at one fully-bound primary key. */
    private static String selectStmt(TableMetadata metadata, List<ColumnMetadata> columns)
    {
        StringBuilder sb = new StringBuilder("SELECT ");
        appendNames(sb, columns, ", ");
        sb.append(" FROM ").append(metadata).append(" WHERE ");
        appendEqPredicates(sb, primaryKeyColumns(metadata), " AND ");
        return sb.toString();
    }

    /**
     * Content hash of a row's primary key. Stable across writes that share a primary key, which is what
     * lets the explicit-timestamp pool offset be a function of the key: two pooled writes to one key must
     * land on the SAME timestamp, or the pool only orders them and no tie is ever formed.
     * <p>
     * {@code ByteBuffer.hashCode} is content-based but position-relative, so this is only stable because
     * the bound buffers are never consumed: CQLTester pre-converts every parameter with
     * {@code BytesType.decompose}, whose serializer duplicates the caller's buffer.
     */
    private static int primaryKeyHash(ByteBuffer[] row, int primaryColumnCount)
    {
        return Arrays.hashCode(Arrays.copyOf(row, primaryColumnCount));
    }

    /** Seed chaining copied from RandomSchemaTest so failures reproduce the same way. */
    private static final class SeedRunner
    {
        private static final long multiplier = 0x5DEECE66DL;
        private static final long addend = 0xBL;
        private static final long mask = (1L << 48) - 1;

        private long seed = CassandraRelevantProperties.TEST_DIFFERENTIAL_SEED.getLong(System.currentTimeMillis());
        private final int examples;

        SeedRunner(int examples)
        {
            this.examples = examples;
        }

        /** Dead code on purpose: plug a failing seed in here to reproduce. */
        @SuppressWarnings("unused")
        SeedRunner withFixedSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        interface SeededTest
        {
            void run(long seed) throws Throwable;
        }

        void run(SeededTest test) throws Throwable
        {
            for (int i = 0; i < examples; i++)
            {
                if (i > 0)
                    seed = (seed * multiplier + addend) & mask;
                try
                {
                    test.run(seed);
                }
                catch (AssumptionViolatedException a)
                {
                    // an Assume skip has to stay a skip: JUnit decides skip-vs-fail on the type
                    // thrown, not on its cause, so wrapping this below would turn the soak red
                    throw a;
                }
                catch (Throwable t)
                {
                    // keep the cause's detail in the message: junit XML only preserves the
                    // top-level message reliably
                    throw new AssertionError("Failure for seed " + seed + " (example " + i + "): " + t.getMessage(), t);
                }
            }
        }
    }
}
