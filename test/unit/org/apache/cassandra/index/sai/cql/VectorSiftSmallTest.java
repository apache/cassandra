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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertTrue;

public class VectorSiftSmallTest extends SAITester
{
    @Test
    public void testSiftSmall() throws Throwable
    {
        var siftName = "siftsmall";
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", siftName, siftName));
        var queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", siftName, siftName));
        var groundTruth = readIvecs(String.format("test/data/%s/%s_groundtruth.ivecs", siftName, siftName));

        // Create table and index
        createTable("CREATE TABLE %s (pk int, val vector<float, 128>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        insertVectors(baseVectors);
        double memoryRecall = testRecall(queryVectors, groundTruth);
        assertTrue("Memory recall is " + memoryRecall, memoryRecall > 0.975);

        flush();
        var diskRecall = testRecall(queryVectors, groundTruth);
        assertTrue("Disk recall is " + diskRecall, diskRecall > 0.95);
    }

    @Test
    public void testSiftSmallWithBooleanPredicatesOfVaryingSelectivity() throws Throwable
    {
        var siftName = "siftsmall";
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", siftName, siftName));
        var queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", siftName, siftName));

        // Create table with regular id column and add SAI index on it
        createTable("CREATE TABLE %s (pk int, id int, val vector<float, 128>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(id) USING 'StorageAttachedIndex'");

        // Insert all vectors into unique partitions with id column
        insertVectorsWithId(baseVectors);

        int totalVectors = baseVectors.size();
        int topK = 100;

        // Test with tiny range restriction (1% of data)
        int tinyRangeEnd = totalVectors / 100;
        // Test with small range restriction (10% of data)
        int smallRangeEnd = totalVectors / 10;
        // Test with medium range restriction (50% of data)
        int mediumRangeEnd = totalVectors / 2;

        // Execute queries of different selectivity. We compute the ground truth for each query vector by brute
        // force in this test class because the dataset doesn't exist.
        beforeAndAfterFlush(() -> {
            double tinyRangeRecall = testRecallWithIdRange(queryVectors, baseVectors, 0, tinyRangeEnd, topK);
            assertTrue("Tiny range recall is " + tinyRangeRecall, tinyRangeRecall >= 0.99);

            double smallRangeRecall = testRecallWithIdRange(queryVectors, baseVectors, 0, smallRangeEnd, topK);
            assertTrue("Small range recall is " + smallRangeRecall, smallRangeRecall >= 0.975);

            double mediumRangeRecall = testRecallWithIdRange(queryVectors, baseVectors, 0, mediumRangeEnd, topK);
            assertTrue("Medium range recall is " + mediumRangeRecall, mediumRangeRecall >= 0.975);
        });

        // Finish by deleting all rows and verifying queries return correctly.
        // Using this test because it has many vectors already
        disableCompaction();
        for (int i = 0; i < totalVectors; i++)
            execute("DELETE FROM %s WHERE pk = ?", i);

        float[] vec = baseVectors.get(0);
        beforeAndAfterFlush(() -> {
            // Confirm all queries produce 0 rows. These queries hit several edge cases that are otherwise hard to
            // test, so we add them at the end of this hybrid sift test.
            assertRows(execute("SELECT pk FROM %s WHERE id >= ? AND id <= ? ORDER BY val ANN OF ? LIMIT ?", 0, tinyRangeEnd, vector(vec), 10));
            assertRows(execute("SELECT pk FROM %s WHERE id >= ? AND id <= ? ORDER BY val ANN OF ? LIMIT ?", 0, smallRangeEnd, vector(vec), 10));
            assertRows(execute("SELECT pk FROM %s WHERE id >= ? AND id <= ? ORDER BY val ANN OF ? LIMIT ?", 0, mediumRangeEnd, vector(vec), 10));
        });
    }

    public static ArrayList<float[]> readFvecs(String filePath) throws IOException
    {
        var vectors = new ArrayList<float[]>();
        try (var dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath))))
        {
            while (dis.available() > 0)
            {
                var dimension = Integer.reverseBytes(dis.readInt());
                assert dimension > 0 : dimension;
                var buffer = new byte[dimension * Float.BYTES];
                dis.readFully(buffer);
                var byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

                var vector = new float[dimension];
                for (var i = 0; i < dimension; i++)
                {
                    vector[i] = byteBuffer.getFloat();
                }
                vectors.add(vector);
            }
        }
        return vectors;
    }

    private static ArrayList<HashSet<Integer>> readIvecs(String filename)
    {
        var groundTruthTopK = new ArrayList<HashSet<Integer>>();

        try (var dis = new DataInputStream(new FileInputStream(filename)))
        {
            while (dis.available() > 0)
            {
                var numNeighbors = Integer.reverseBytes(dis.readInt());
                var neighbors = new HashSet<Integer>(numNeighbors);

                for (var i = 0; i < numNeighbors; i++)
                {
                    var neighbor = Integer.reverseBytes(dis.readInt());
                    neighbors.add(neighbor);
                }

                groundTruthTopK.add(neighbors);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return groundTruthTopK;
    }

    public double testRecall(List<float[]> queryVectors, List<HashSet<Integer>> groundTruth)
    {
        AtomicInteger topKfound = new AtomicInteger(0);
        int topK = 100;

        // Perform query and compute recall
        var stream = IntStream.range(0, queryVectors.size()).parallel();
        stream.forEach(i -> {
            float[] queryVector = queryVectors.get(i);
            String queryVectorAsString = Arrays.toString(queryVector);

            try
            {
                UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ANN OF " + queryVectorAsString + " LIMIT " + topK);
                var gt = groundTruth.get(i);

                int n = (int) result.stream().filter(row -> gt.contains(row.getInt("pk"))).count();
                topKfound.addAndGet(n);
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        });

        return (double) topKfound.get() / (queryVectors.size() * topK);
    }

    private void insertVectors(List<float[]> baseVectors)
    {
        IntStream.range(0, baseVectors.size()).parallel().forEach(i -> {
            float[] arrayVector = baseVectors.get(i);
            String vectorAsString = Arrays.toString(arrayVector);
            try
            {
                execute("INSERT INTO %s " + String.format("(pk, val) VALUES (%d, %s)", i, vectorAsString));
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        });
    }

    private void insertVectorsWithId(List<float[]> baseVectors)
    {
        IntStream.range(0, baseVectors.size()).parallel().forEach(i -> {
            float[] arrayVector = baseVectors.get(i);
            String vectorAsString = Arrays.toString(arrayVector);
            try
            {
                execute("INSERT INTO %s " + String.format("(pk, id, val) VALUES (%d, %d, %s)", i, i, vectorAsString));
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        });
    }

    private double testRecallWithIdRange(List<float[]> queryVectors, List<float[]> baseVectors,
                                         int idStart, int idEnd, int topK)
    {
        AtomicInteger topKfound = new AtomicInteger(0);

        // Perform query with id range restriction and compute recall
        IntStream.range(0, queryVectors.size()).parallel().forEach(i -> {
            float[] queryVector = queryVectors.get(i);
            String queryVectorAsString = Arrays.toString(queryVector);

            try
            {
                // Compute ground truth for this filtered range by brute force
                var filteredGroundTruth = computeGroundTruthForRange(queryVector, baseVectors, idStart, idEnd, topK);

                String query = String.format("SELECT pk FROM %%s WHERE id >= %d AND id <= %d ORDER BY val ANN OF %s LIMIT %d",
                                             idStart, idEnd, queryVectorAsString, topK);
                UntypedResultSet result = execute(query);

                // Count how many results are in the ground truth
                int n = (int) result.stream().filter(row -> filteredGroundTruth.contains(row.getInt("pk"))).count();
                topKfound.addAndGet(n);
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        });

        return (double) topKfound.get() / (queryVectors.size() * topK);
    }

    private HashSet<Integer> computeGroundTruthForRange(float[] queryVector, List<float[]> baseVectors,
                                                         int idStart, int idEnd, int topK)
    {
        // Create a list of (id, distance) pairs for vectors in the range
        var candidates = new ArrayList<java.util.Map.Entry<Integer, Float>>();

        for (int id = idStart; id <= idEnd && id < baseVectors.size(); id++)
        {
            float distance = euclideanDistance(queryVector, baseVectors.get(id));
            candidates.add(new java.util.AbstractMap.SimpleEntry<>(id, distance));
        }

        // Sort by distance and take top K
        candidates.sort(java.util.Map.Entry.comparingByValue());

        var groundTruth = new HashSet<Integer>();
        int limit = Math.min(topK, candidates.size());
        for (int i = 0; i < limit; i++)
        {
            groundTruth.add(candidates.get(i).getKey());
        }

        return groundTruth;
    }

    private float euclideanDistance(float[] a, float[] b)
    {
        float sum = 0.0f;
        for (int i = 0; i < a.length; i++)
        {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return (float) Math.sqrt(sum);
    }
}
