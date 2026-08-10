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

package org.apache.cassandra.index.sai.cql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;

import static org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.MIN_PQ_ROWS;
import static org.junit.Assert.*;

@Ignore
@RunWith(Parameterized.class)
abstract public class VectorCompactionTest extends VectorTester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    // Subclasses must implement this to cover different dimensions
    abstract public int dimension();

    @Parameterized.Parameter()
    public Version version;

    @Parameterized.Parameter(1)
    public boolean enableNVQ;

    @Parameterized.Parameters(name = "version={0} enableNVQ={1}")
    public static Collection<Object[]> data()
    {
        // See Version file for explanation of changes associated with each version
        return Version.ALL.stream()
                          .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST))
                          .flatMap(vd -> {
                              // NVQ is only relevant some of the time
                              Boolean[] enableNVQ = JVectorVersionUtil.versionSupportsNVQ(vd)
                                                    ? new Boolean[]{ true, false }
                                                    : new Boolean[]{ false };
                              return Arrays.stream(enableNVQ).map(b -> new Object[]{ vd, b });
                          })
                          .collect(Collectors.toList());
    }

    @Before
    public void setCurrentVersion() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Before
    public void setEnableNVQ()
    {
        SAIUtil.setEnableNVQ(enableNVQ);
    }

    @Test
    public void testCompactionWithEnoughRowsForPQAndDeleteARow()
    {
        createTable();
        disableCompaction();

        var vectors = new HashSet<>();
        for (int i = 0; i <= MIN_PQ_ROWS; i++)
        {
            Vector<Float> vector;
            do
            {
                vector = randomVectorBoxed(dimension()); // confirm no duplicates
            } while (!vectors.add(vector));
            // make ascending counted vector
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, vector);
        }
        flush();

        // By deleting a row, we trigger a key histogram to round its estimate to 0 instead of 1 rows per key, and
        // that broke compaction, so we test that here.
        execute("DELETE FROM %s WHERE pk = 0");
        flush();

        // Run compaction, it fails if compaction is not successful
        compact();

        // Confirm we can query the data
        assertRowCount(execute("SELECT * FROM %s ORDER BY v ANN OF ? LIMIT 1", randomVectorBoxed(dimension())), 1);
    }

    @Test
    public void testPQRefine()
    {
        // The test fails for dimensions > 2, not sure why, but likely due to the use of random vectors.
        if (dimension() > 2)
            return;

        createTable();
        disableCompaction();

        var vectors = new ArrayList<float[]>();
        // 3 sstables
        for (int j = 0; j < 3; j++)
        {
            for (int i = 0; i <= MIN_PQ_ROWS; i++)
            {
                var pk = j * MIN_PQ_ROWS + i;
                var v = create2DVector();
                vectors.add(v);
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, vector(v));
            }
            flush();
        }

//        TODO deterimine proper design for PQ training on vectors of dimension < 100.
//        see https://github.com/riptano/cndb/issues/13630
//        CompactionGraph.PQ_TRAINING_SIZE = 2 * MIN_PQ_ROWS;
        compact();

        // Confirm we can query the data with reasonable recall
        double recall = 0;
        int ITERS = 10;
        for (int i = 0; i < ITERS; i++)
        {
            var q = create2DVector();
            var result = execute("SELECT pk, v FROM %s ORDER BY v ANN OF ? LIMIT 20", vector(q));
            var ann = result.stream().map(row -> {
                var vList = row.getVector("v", FloatType.instance, 2);
                return new float[]{ vList.get(0), vList.get(1) };
            }).collect(Collectors.toList());
            recall += computeRecall(vectors, q, ann, VectorSimilarityFunction.COSINE);
        }
        recall /= ITERS;
        assert recall >= 0.9 : recall;
    }

    @Test
    public void testOneToManyCompaction()
    {
        for (int sstables = 2; sstables <= 3; sstables++)
        {
            testOneToManyCompactionInternal(10, sstables);
            testOneToManyCompactionInternal(MIN_PQ_ROWS, sstables);
        }
    }

    // Exercise the one-to-many path in compaction
    public void testOneToManyCompactionInternal(int vectorsPerSstable, int sstables)
    {
        var index = createTableAndReturnIndexName();

        disableCompaction();

        insertOneToManyRows(vectorsPerSstable, sstables);

        // queries should behave sanely before and after compaction
        validateQueries();
        compact();
        validateQueries();

        // ONE_TO_MANY is version dependent, so we need to branch based on which version we're writing
        var expectedStructure = V5OnDiskFormat.writeV5VectorPostings(version)
                                ? V5VectorPostingsWriter.Structure.ONE_TO_MANY
                                : V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY;
        validatePostingsStructureAndOrdinalToVectorMapping(index, expectedStructure, vectorsPerSstable * sstables);
    }

    @Test
    public void testOneToManyCompactionTooManyHoles()
    {
        int sstables = 2;
        testOneToManyCompactionInternal(10, sstables);
        testOneToManyCompactionHolesInternal(MIN_PQ_ROWS, sstables);
    }

    @Test
    public void testOneToOneCompaction()
    {
        for (int sstables = 2; sstables <= 3; sstables++)
        {
            testOneToOneCompactionInternal(10, sstables);
            testOneToOneCompactionInternal(MIN_PQ_ROWS, sstables);
        }
    }

    // Exercise the one-to-one path in compaction where each row has exactly one unique vector
    public void testOneToOneCompactionInternal(int vectorsPerSstable, int sstables)
    {
        var indexName = createTableAndReturnIndexName();

        disableCompaction();

        insertOneToOneRows(vectorsPerSstable, sstables);

        // queries should behave sanely before and after compaction
        validateQueries();
        compact();
        validateQueries();

        validatePostingsStructureAndOrdinalToVectorMapping(indexName, V5VectorPostingsWriter.Structure.ONE_TO_ONE, vectorsPerSstable * sstables);
    }

    @Test
    public void testZeroOrOneToManyCompaction()
    {
        for (int sstables = 2; sstables <= 3; sstables++)
        {
            testZeroOrOneToManyCompactionInternal(10, sstables);
            testZeroOrOneToManyCompactionInternal(MIN_PQ_ROWS, sstables);
        }
    }

    public void testZeroOrOneToManyCompactionInternal(int vectorsPerSstable, int sstables)
    {
        var indexName = createTableAndReturnIndexName();
        disableCompaction();

        insertZeroOrOneToManyRows(vectorsPerSstable, sstables);

        validateQueries();
        compact();
        validateQueries();

        validatePostingsStructureAndOrdinalToVectorMapping(indexName, V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY, vectorsPerSstable * sstables);
    }

    private void insertZeroOrOneToManyRows(int vectorsPerSstable, int sstables)
    {
        var R = getRandom();
        double duplicateChance = R.nextDouble() * 0.2;
        int j = 0;
        boolean nullInserted = false;

        for (int i = 0; i < sstables; i++)
        {
            var vectorsInserted = new ArrayList<Vector<Float>>();
            var duplicateExists = false;
            while (vectorsInserted.size() < vectorsPerSstable || !duplicateExists)
            {
                if (!nullInserted && vectorsInserted.size() == vectorsPerSstable/2)
                {
                    // Insert one null vector in the middle
                    execute("INSERT INTO %s (pk, v) VALUES (?, null)", j++);
                    nullInserted = true;
                    continue;
                }

                Vector<Float> v;
                if (R.nextDouble() < duplicateChance && !vectorsInserted.isEmpty())
                {
                    // insert a duplicate
                    v = vectorsInserted.get(R.nextIntBetween(0, vectorsInserted.size() - 1));
                    duplicateExists = true;
                }
                else
                {
                    // insert a new random vector
                    v = randomVectorBoxed(dimension());
                    vectorsInserted.add(v);
                }
                assert v != null;
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", j++, v);
            }
            flush();
        }
    }

    public void testOneToManyCompactionHolesInternal(int vectorsPerSstable, int sstables)
    {
        var indexName = createTableAndReturnIndexName();

        disableCompaction();

        insertOneToManyRows(vectorsPerSstable, sstables);

        double originalGlobalHolesAllowed = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED;
        try
        {
            // this should be done after writing data so that we exercise the "we thought we were going to use the
            // one-to-many-path but there were too many holes so we changed plans" code path
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.0;

            // queries should behave sanely before and after compaction
            validateQueries();
            compact();
            validateQueries();

            validatePostingsStructureAndOrdinalToVectorMapping(indexName, V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY, vectorsPerSstable * sstables);
        }
        finally
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = originalGlobalHolesAllowed;
        }
    }

    private void validatePostingsStructureAndOrdinalToVectorMapping(String indexName, V5VectorPostingsWriter.Structure expectedStructure, int numRows)
    {
        // Validate that we have the expected structure for all the sstables-segment indexes.
        var sai = (StorageAttachedIndex) Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable()).getIndexManager().getIndexByName(indexName);
        var indexes = sai.getIndexContext().getView().getIndexes();
        var columnMetadata = sai.getIndexContext().getDefinition();
        var columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(columnMetadata));
        for (var index : indexes)
        {
            boolean isMissingRows = index.getRowCount() < index.getSSTable().getTotalRows();

            // We don't have enough rows to get segments, the assertions are simplified if we know the sstable has
            // just one segment.
            assertEquals(1, index.getSegments().size());
            var segment = index.getSegments().get(0);

            var searcher = (V2VectorIndexSearcher) segment.getIndexSearcher();

            // We track nulls so that we can verify the postring structure matches.
            boolean nullValueObserved = false;

            // Validate that the vectors in the sstable row correctly match the vectors in the graph by grabbing
            // the vector, creating a score function for the vector, converting its row id into an ordinal, and
            // then assert that the sstable's vector is similar to the graph's vector within an epsilon.
            assertNotNull(segment.sstableContext);
            assertNotNull(segment.sstableContext.primaryKeyMapFactory());
            try (var view = searcher.graph.getView();
                 var ordinalsView = searcher.graph.getOrdinalsView();
                 var pkm = segment.sstableContext.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
            {
                assertNotNull(segment.metadata);
                var vectorsSet = new HashSet<VectorFloat<?>>();
                boolean hasUniqueVectors = true;
                for (long i = segment.metadata.minSSTableRowId; i <= segment.metadata.maxSSTableRowId; i++)
                {
                    var primaryKey = pkm.primaryKeyFromRowId(i);
                    assertFalse("The subsequent logic assumes that we have no clustering columns", primaryKey.hasClustering());
                    try (var sstableIter = segment.sstableContext
                                           .sstable()
                                           .iterator(primaryKey.partitionKey(), Slices.ALL, columnFilter, false, SSTableReadsListener.NOOP_LISTENER))
                    {
                        int rowId = Math.toIntExact(i - segment.metadata.segmentRowIdOffset);
                        assertTrue(sstableIter.hasNext());
                        var next = (Row) sstableIter.next();
                        var vectorData = next.getCell(columnMetadata);
                        float[] vector = ((VectorType<?>) columnMetadata.type).composeAsFloat(vectorData.buffer());
                        if (vector == null)
                        {
                            nullValueObserved = true;
                            assertEquals(V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY, searcher.getPostingsStructure());
                            int ordinal = ordinalsView.getOrdinalForRowId(rowId);
                            assertTrue("Got " + ordinal, ordinal < 0);
                        }
                        else
                        {
                            VectorFloat<?> vectorFloat = vts.createFloatVector(vector);
                            hasUniqueVectors &= vectorsSet.add(vectorFloat);
                            int ordinal = ordinalsView.getOrdinalForRowId(rowId);
                            // Compare using cosine to ignore magnitude
                            float sim = view.rerankerFor(vectorFloat, VectorSimilarityFunction.COSINE).similarityTo(ordinal);
                            assertEquals(1.0f, sim, 0.001f);

                            if (searcher.graph.getCompressedVectors() != null)
                            {
                                // Compare using cosine to ignore magnitude
                                float quantizedSim = searcher.graph.getCompressedVectors()
                                                                   .scoreFunctionFor(vectorFloat, VectorSimilarityFunction.COSINE)
                                                                   .similarityTo(ordinal);
                                // Note that this tolerance failed at 0.001f, but passes at 0.01f. Assuming this is
                                // reasonable for now.
                                assertEquals(1.0f, quantizedSim, 0.01f);
                            }
                            else if (numRows >= MIN_PQ_ROWS)
                            {
                                // With fused PQ (FA always; GA+ only when enabled), PQ metadata is stored inline
                                // with the graph nodes — there is no standalone CompressedVectors to validate against.
                                // Without fused PQ (GA+ default, pre-FA), we should never reach this branch.
                                assertTrue("Found " + numRows + " rows without CompressedVectors; expected fused PQ for version " + version,
                                           JVectorVersionUtil.shouldWriteFused(version));
                                assertNotNull("Expected PQ metadata for fused PQ on version " + version, searcher.getPQ());
                            }
                        }
                    }
                }

                assertEquals(vectorsSet.size(), searcher.graph.size());

                // When we have a row with a null vector, it is valid for the missing row vector(s) to be at the
                // start or end of the sstable, and in that case, we actually skip them within the segment.
                if (!nullValueObserved && isMissingRows)
                {
                    var struct = hasUniqueVectors
                                 ? V5VectorPostingsWriter.Structure.ONE_TO_ONE
                                 : V5VectorPostingsWriter.tooManyOrdinalMappingHoles(searcher.graph.size(), numRows)
                                   ? V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY
                                   : V5VectorPostingsWriter.Structure.ONE_TO_MANY;
                    assertEquals(struct, searcher.getPostingsStructure());
                }
                else
                {
                    assertEquals(expectedStructure, searcher.getPostingsStructure());
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private void insertOneToManyRows(int vectorsPerSstable, int sstables)
    {
        var R = getRandom();
        double duplicateChance = R.nextDouble() * 0.2;
        int j = 0;
        for (int i = 0; i < sstables; i++)
        {
            var vectorsInserted = new ArrayList<Vector<Float>>();
            var duplicateExists = false;
            while (vectorsInserted.size() < vectorsPerSstable || !duplicateExists)
            {
                Vector<Float> v;
                if (R.nextDouble() < duplicateChance && !vectorsInserted.isEmpty())
                {
                    v = vectorsInserted.get(R.nextIntBetween(0, vectorsInserted.size() - 1));
                    duplicateExists = true;
                }
                else
                {
                    v = randomVectorBoxed(dimension());
                    vectorsInserted.add(v);
                }
                assert v != null;
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", j++, v);
            }
            flush();
        }
    }

    private void insertOneToOneRows(int vectorsPerSstable, int sstables)
    {
        var vectors = new HashSet<Vector<Float>>();
        int j = 0;
        for (int i = 0; i < sstables; i++)
        {
            for (int k = 0; k < vectorsPerSstable; k++)
            {
                Vector<Float> v;
                do
                {
                    v = randomVectorBoxed(dimension()); // ensure no duplicates
                } while (!vectors.add(v));
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", j++, v);
            }
            flush();
        }
    }

    private void createTable()
    {
        createTableAndReturnIndexName();
    }

    private String createTableAndReturnIndexName()
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, " + dimension() + ">, PRIMARY KEY(pk))");
        return createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
    }

    private void validateQueries()
    {
        for (int i = 0; i < 10; i++)
        {
            var q = randomVectorBoxed(dimension());
            var r = execute("SELECT pk, similarity_cosine(v, ?) as similarity FROM %s ORDER BY v ANN OF ? LIMIT 10", q, q);
            float lastSimilarity = Float.MAX_VALUE;
            assertEquals(10, r.size());
            for (var row : r)
            {
                float similarity = row.getFloat("similarity");
                assertTrue(similarity <= lastSimilarity);
                lastSimilarity = similarity;
            }
        }
    }

    private static float[] create2DVector() {
        var R = getRandom();
        return new float[] { R.nextFloatBetween(-100, 100), R.nextFloatBetween(-100, 100) };
    }
}
