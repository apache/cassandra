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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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
 * Randomized soak for the cursor-vs-iterator differential harness: random supported schemas and
 * multi-round workloads flushed into overlapping sstables, then byte+logical comparison of both paths.
 */
public class RandomDifferentialCompactionTest extends DifferentialCompactionTester
{
    static
    {
        // make generated blobs deterministic per seed
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);
    }

    private static final int DEFAULT_EXAMPLES = 10;
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_DIFFERENTIAL_EXAMPLES.getInt(DEFAULT_EXAMPLES);

    /** Long enough that expiry can never fall between the two differential runs. */
    private static final int SOAK_TTL_SECONDS = 30 * 24 * 60 * 60;

    /** Base of the small explicit-timestamp pool, kept a year above the wall clock so its ties always win. */
    private static final long TIE_POOL_BASE =
        TimeUnit.DAYS.toMicros(TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis()) + 365);

    /** Width of the pool the random workload draws from; the designated tie sits just above it. */
    private static final int TIE_POOL_WIDTH = 3;

    /** Upper bound of the per-example draw for rows written into a hub partition per round; the floor is a quarter of it. */
    private static final int HUB_ROWS_PER_ROUND_MAX =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_HUB_ROWS_PER_ROUND.getInt();
    private static final int HUB_ROWS_PER_ROUND_MIN =
        Math.min(HUB_ROWS_PER_ROUND_MAX, Math.max(2, HUB_ROWS_PER_ROUND_MAX / 4));

    /** column_index_size values in KiB, one drawn per example, weighted low. */
    private static final int[] COLUMN_INDEX_SIZES_KIB = { 1, 1, 1, 2, 2, 4, 8, 16 };

    /** Redraws allowed before a wide clustering that will not fit the key limit is treated as a failure. */
    private static final int CLUSTERING_REDRAWS = 32;

    /** Run-level index-block coverage; asserted at the end of {@link #randomizedDifferential}. */
    private int examplesWithPromotedRowIndex;
    private int promotedRowIndexPartitions;

    @Test
    public void randomizedDifferential() throws Throwable
    {
        // a zero or negative example count would pass having compared nothing
        assertTrue("cassandra.test.differential.examples must be > 0, got " + EXAMPLES, EXAMPLES > 0);
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        assertTrue("the explicit-timestamp pool must sit above the wall clock: TIE_POOL_BASE=" +
                   TIE_POOL_BASE + " nowMicros=" + nowMicros, TIE_POOL_BASE > nowMicros);
        new SeedRunner(EXAMPLES).run(this::runOneExample);

        // assert the soak actually built a row index somewhere, so it does not silently cover nothing
        logger.info("{} of {} examples produced a partition with a promoted row index; {} indexed " +
                    "partitions in total", examplesWithPromotedRowIndex, EXAMPLES, promotedRowIndexPartitions);
        assertTrue("no example produced a partition with a promoted row index over " + EXAMPLES +
                   " examples: every partition stayed below column_index_size, so BtiCursorIndexWriter " +
                   "took trieRoot -1 everywhere and no row trie was written or read back",
                   examplesWithPromotedRowIndex > 0);
    }

    /** Generates a row whose clustering fits the 64KiB key limit, redrawing until it does. */
    private static ByteBuffer[] generateRowWithLegalClustering(Gen<ByteBuffer[]> dataGen,
                                                               JavaRandom qtRandom,
                                                               int partitionColumnCount,
                                                               int primaryColumnCount)
    {
        for (int attempt = 0; attempt < CLUSTERING_REDRAWS; attempt++)
        {
            ByteBuffer[] row = dataGen.generate(qtRandom);
            int sum = 0;
            for (int i = partitionColumnCount; i < primaryColumnCount; i++)
                sum += row[i] == null ? 0 : row[i].remaining();
            // one sum check covers both of validate's rejections
            if (sum <= FBUtilities.MAX_UNSIGNED_SHORT)
                return row;
        }
        throw new AssertionError("no generated row in " + CLUSTERING_REDRAWS + " draws had a clustering " +
                                 "under " + FBUtilities.MAX_UNSIGNED_SHORT + " bytes");
    }

    private void runOneExample(long seed) throws Throwable
    {
        JavaRandom qtRandom = new JavaRandom(seed);
        Random workload = new Random(seed);

        TableMetadata metadata = generateSupportedMetadata(qtRandom, workload);

        maybeCreateUDTs(metadata);
        String createTableCql = metadata.toCqlString(true, false, false)
                                        .replaceAll("org.apache.cassandra.db.marshal.", "");
        logger.info("randomizedDifferential seed={} schema:\n{}", seed, createTableCql);
        createTable(KEYSPACE, createTableCql);
        // the CQL embeds the generator's table name; createTable's returned name is not it
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, metadata.name);
        cfs.disableAutoCompaction();

        Example example = new Example(metadata, qtRandom, workload);
        example.writeRounds();

        // set column_index_size for the compaction only, so the row index under test is the one compaction built
        int originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(example.columnIndexSizeKiB);
        try
        {
            assertCursorMatchesIterator(cfs);

            // one flush per round leaves the tie candidates in different sstables, so the tie-break is reached
            int inputSSTables = cfs.getLiveSSTables().size();
            assertTrue("one flush per round must leave one input sstable per round, got " + inputSSTables +
                       " for " + example.rounds + " rounds", inputSSTables >= example.rounds);
            // commit one real cursor compaction so the tied row can be read back out of what compaction wrote
            commitCompaction(cfs, cfs.getLiveSSTables(), true, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));

            // count indexed partitions over the committed output, to confirm the soak built a row index
            int indexedThisExample = 0;
            for (SSTableReader output : cfs.getLiveSSTables())
                indexedThisExample += assertEveryRowReadableThroughASlice(output);
            promotedRowIndexPartitions += indexedThisExample;
            if (indexedThisExample > 0)
                examplesWithPromotedRowIndex++;
            logger.info("seed={} column_index_size={}KiB clustering={} hubs={} hubRows/round={} -> {} indexed partitions",
                        seed, example.columnIndexSizeKiB, example.clusteringColumnCount, example.hubCount,
                        example.hubRowsPerRound, indexedThisExample);

            example.assertDesignatedTieResolved();
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
        }
    }

    /** Generates a random table restricted to the surface the cursor pipeline supports. */
    private static TableMetadata generateSupportedMetadata(JavaRandom qtRandom, Random workload)
    {
        Gen<String> udtName = Generators.unique(IDENTIFIER_GEN);
        TypeGenBuilder safePrimary = AbstractTypeGenerators.withoutUnsafeEquality().withUDTNames(udtName);
        TableMetadata metadata;
        do
        {
            // clustering width: usually 0-3, one draw in four at 33-36 to exercise a second value header
            int clusteringColumns = workload.nextInt(4) == 0 ? 33 + workload.nextInt(4)
                                                             : workload.nextInt(4);
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
                       .withClusteringColumnsCount(clusteringColumns)
                       .withRegularColumnsBetween(1, 5)
                       .withStaticColumnsBetween(0, 2)
                       .build(qtRandom);
        }
        // reject unsupported metadata, and invalid CQL: static columns require clustering columns
        while (CursorCompactor.unsupportedMetadata(metadata)
               || (metadata.clusteringColumns().isEmpty() && !metadata.staticColumns().isEmpty()));

        return metadata;
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

    /** Content hash of a row's primary key, stable across writes that share it. */
    private static int primaryKeyHash(ByteBuffer[] row, int primaryColumnCount)
    {
        return Arrays.hashCode(Arrays.copyOf(row, primaryColumnCount));
    }

    /** Whether this row's partition key is one of the hub keys. */
    private static boolean inHubPartition(ByteBuffer[] row, List<ByteBuffer[]> hubKeys, int partitionColumnCount)
    {
        for (ByteBuffer[] hub : hubKeys)
        {
            boolean same = true;
            for (int i = 0; i < partitionColumnCount && same; i++)
                same = row[i].equals(hub[i]);
            if (same)
                return true;
        }
        return false;
    }

    /** One example's schema-derived statements and generated workload. */
    private final class Example
    {
        private final TableMetadata metadata;
        private final JavaRandom qtRandom;
        private final Random workload;
        private final Gen<ByteBuffer[]> dataGen;

        private final int partitionColumnCount;
        private final int clusteringColumnCount;
        private final int primaryColumnCount;

        private final String insertStmt;
        private final String deleteRowStmt;
        private final String deletePartitionStmt;

        /** select-order index of every column, for UPDATE/cell-delete binding */
        private final Map<String, Integer> selectOrderIndex = new HashMap<>();
        private final List<ColumnMetadata> regularColumns;
        private final List<ColumnMetadata> staticColumns;

        private final List<ByteBuffer[]> rows = new ArrayList<>();
        // partition-delete victims are drawn from here, not from `rows`, to keep hub partitions indexable
        private final List<ByteBuffer[]> nonHubRows = new ArrayList<>();

        private final int rounds;
        /** which round wins the designated tie */
        private final int tieWinnerOffset;
        private final long tieTimestamp;
        private final ByteBuffer[] tieKey;
        private final List<ByteBuffer[]> tieWrites = new ArrayList<>();

        private final int hubCount;
        // a whole generated row, of which only the leading partitionColumnCount components are ever read
        private final List<ByteBuffer[]> hubKeys = new ArrayList<>();
        private final int hubRowsPerRound;
        private final int columnIndexSizeKiB;

        Example(TableMetadata metadata, JavaRandom qtRandom, Random workload)
        {
            this.metadata = metadata;
            this.qtRandom = qtRandom;
            this.workload = workload;

            // ~12% of non-key values are null: cell tombstones on simple columns, empty buffers on clustering columns
            Gen<ValueDomain> valueDomains = SourceDSL.integers().between(0, 99)
                                                     .map(i -> i < 12 ? ValueDomain.NULL : ValueDomain.NORMAL);
            this.dataGen = CassandraGenerators.data(metadata, valueDomains);

            this.partitionColumnCount = metadata.partitionKeyColumns().size();
            this.clusteringColumnCount = metadata.clusteringColumns().size();
            this.primaryColumnCount = partitionColumnCount + clusteringColumnCount;
            this.insertStmt = insertStmt(metadata);
            this.deleteRowStmt = deleteStmt(metadata, primaryColumnCount);
            this.deletePartitionStmt = deleteStmt(metadata, partitionColumnCount);

            Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
            for (int i = 0; it.hasNext(); i++)
                selectOrderIndex.put(it.next().name.toString(), i);
            this.regularColumns = ImmutableList.copyOf(metadata.regularColumns());
            this.staticColumns = ImmutableList.copyOf(metadata.staticColumns());

            this.rounds = 2 + workload.nextInt(3); // 2-4 sstables
            this.tieWinnerOffset = workload.nextInt(rounds);

            // one deterministic cross-sstable same-timestamp tie per example: the same primary key written
            // once per round with fresh values at one timestamp, so the tie-break is reached in every example
            this.tieTimestamp = TIE_POOL_BASE + TIE_POOL_WIDTH;
            // no value domain: every tie candidate is a live cell, so the value comparison alone decides the winner
            Gen<ByteBuffer[]> tieDataGen = CassandraGenerators.data(metadata, null);
            this.tieKey = tieDataGen.generate(qtRandom);
            for (int round = 0; round < rounds; round++)
            {
                ByteBuffer[] tieRow = tieDataGen.generate(qtRandom);
                System.arraycopy(tieKey, 0, tieRow, 0, primaryColumnCount);
                tieWrites.add(tieRow);
            }
            arrangeTieWinnersAcrossRounds();

            // hub partitions: a partition key many rows share, written every round so the partition grows wide
            // enough to promote a row index. Skipped without a clustering, since then every partition holds one row.
            this.hubCount = clusteringColumnCount == 0 ? 0 : 1 + workload.nextInt(2);
            for (int i = 0; i < hubCount; i++)
                hubKeys.add(dataGen.generate(qtRandom));
            this.hubRowsPerRound = hubCount == 0 ? 0
                                   : HUB_ROWS_PER_ROUND_MIN
                                     + workload.nextInt(HUB_ROWS_PER_ROUND_MAX - HUB_ROWS_PER_ROUND_MIN + 1);
            this.columnIndexSizeKiB = COLUMN_INDEX_SIZES_KIB[workload.nextInt(COLUMN_INDEX_SIZES_KIB.length)];
        }

        /** Spreads each column's winning value across rounds, so no "always keep sstable k" rule can pass. */
        private void arrangeTieWinnersAcrossRounds()
        {
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
        }

        /** Writes the whole workload: one round per input sstable, each round flushed. */
        void writeRounds() throws Throwable
        {
            for (int round = 0; round < rounds; round++)
            {
                writeInserts();
                writeHubRows();
                writeRowDeletes();
                writeRangeDeletes();
                writeCellDeletes();
                maybeWritePartitionDelete();

                // the designated tie: the same primary key, the same timestamp, fresh values, once per round
                execute(insertStmt + " USING TIMESTAMP " + tieTimestamp, (Object[]) tieWrites.get(round));

                flush(KEYSPACE, metadata.name);
            }
        }

        private void writeInserts() throws Throwable
        {
            // watermark at round start: the explicit-timestamp branch overwrites only keys from earlier rounds,
            // so its tie forms across sstables rather than inside one memtable
            int rowsBeforeThisRound = rows.size();
            int inserts = 15 + workload.nextInt(26); // 15-40 rows
            for (int i = 0; i < inserts; i++)
                writeOneInsert(rowsBeforeThisRound);
        }

        private void writeOneInsert(int rowsBeforeThisRound) throws Throwable
        {
            ByteBuffer[] row = generateRowWithLegalClustering(dataGen, qtRandom,
                                                              partitionColumnCount, primaryColumnCount);
            boolean overwrite = !rows.isEmpty() && workload.nextInt(100) < 30;
            // a separate draw: keeps only the partition key, putting a few rows into an ordinary partition
            boolean sharePartition = !overwrite && !rows.isEmpty() && clusteringColumnCount > 0
                                     && workload.nextInt(100) < 20;
            if (overwrite)
            {
                // overwrite: keep a previously used primary key with fresh non-key values, so the merge reconciles
                ByteBuffer[] prev = rows.get(workload.nextInt(rows.size()));
                System.arraycopy(prev, 0, row, 0, primaryColumnCount);
            }
            else if (sharePartition)
            {
                ByteBuffer[] prev = rows.get(workload.nextInt(rows.size()));
                System.arraycopy(prev, 0, row, 0, partitionColumnCount);
            }

            int mode = workload.nextInt(100);
            writeGeneratedRow(row, mode, overwrite, rowsBeforeThisRound);
            rows.add(row);
            if (!inHubPartition(row, hubKeys, partitionColumnCount))
                nonHubRows.add(row);
        }

        /** Writes one generated row: an explicit-timestamp collision, or one of the four shapes {@code mode} selects. */
        private void writeGeneratedRow(ByteBuffer[] row, int mode, boolean overwrite, int rowsBeforeThisRound)
        throws Throwable
        {
            if (overwrite && rowsBeforeThisRound > 0 && workload.nextInt(100) < 40)
            {
                // explicit-timestamp collision: re-keyed onto an earlier round's row and stamped from a
                // pool offset derived from the key, so two pooled writes to one key land on the same timestamp
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
        }

        /** Writes the hub rows: one shared partition key, fresh clustering and values. */
        private void writeHubRows() throws Throwable
        {
            for (int i = 0; i < hubRowsPerRound; i++)
            {
                ByteBuffer[] row = generateRowWithLegalClustering(dataGen, qtRandom,
                                                                  partitionColumnCount, primaryColumnCount);
                System.arraycopy(hubKeys.get(workload.nextInt(hubKeys.size())), 0, row, 0, partitionColumnCount);
                execute(insertStmt, (Object[]) row);
                rows.add(row);
            }
        }

        /** Writes row deletes against known keys. */
        private void writeRowDeletes() throws Throwable
        {
            for (int i = 0; i < 3 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                execute(deleteRowStmt, (Object[]) Arrays.copyOf(victim, primaryColumnCount));
            }
        }

        /** Writes range deletes (clustering tables only): single-sided slices and clustering-prefix deletes. */
        private void writeRangeDeletes() throws Throwable
        {
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
        }

        /** Writes cell deletes: a random subset of regular columns at a known row, or occasionally a static cell. */
        private void writeCellDeletes() throws Throwable
        {
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
        }

        /** Writes an occasional partition delete; hub partitions are excluded. */
        private void maybeWritePartitionDelete() throws Throwable
        {
            if (workload.nextInt(100) < 40 && !nonHubRows.isEmpty())
            {
                ByteBuffer[] victim = nonHubRows.get(workload.nextInt(nonHubRows.size()));
                execute(deletePartitionStmt, (Object[]) Arrays.copyOf(victim, partitionColumnCount));
            }
        }

        /** Asserts the greater raw value bytes won the same-timestamp tie on every simple regular column. */
        void assertDesignatedTieResolved() throws Throwable
        {
            UntypedResultSet tieResult = execute(selectStmt(metadata, regularColumns),
                                                (Object[]) Arrays.copyOf(tieKey, primaryColumnCount));
            assertEquals("the designated tie row must survive compaction: it is written above the wall clock, " +
                         "so no wall-clock write or delete in this example can shadow it", 1, tieResult.size());
            UntypedResultSet.Row survivor = tieResult.one();
            // for a simple column, the cell with the greater value bytes wins a tie of equal timestamps;
            // complex columns are skipped, as they resolve per cell path (covered by the EdgeCase and Pathological tests)
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
    }

    /** Chains seeds across examples so a failure reproduces. */
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

        /** Plugs a failing seed in to reproduce it. */
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
                    // let an Assume skip stay a skip rather than a failure
                    throw a;
                }
                catch (Throwable t)
                {
                    // keep the cause's detail in the top-level message, the only one junit XML preserves reliably
                    throw new AssertionError("Failure for seed " + seed + " (example " + i + "): " + t.getMessage(), t);
                }
            }
        }
    }
}
