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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.lucene.codecs.CodecUtil;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.vector.JVectorUtils;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that SAI vector indexes stay compatible with those written by jvector 1.0.2. The fixture under
 * {@code test/data/legacy-sai/vector} was flushed before the jvector 4 upgrade, from the rows {@link #vectors()}
 * returns, into the table and index this test creates.
 */
public class VectorLegacyFormatTest extends VectorTester
{
    private static final Path FIXTURE = Paths.get("test/data/legacy-sai/vector");
    private static final String TABLE = "CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 8>)";
    private static final int ROWS = 1100; // above OnHeapGraph.MIN_PQ_ROWS, so the index has compressed vectors
    private static final int DIMENSION = 8;
    private static final int CLUSTERS = 256;
    private static final VectorType<Float> TYPE = VectorType.getInstance(FloatType.instance, DIMENSION);

    @Test
    public void testReadsIndexWrittenByJVector1() throws Throwable
    {
        createTable(TABLE);
        createIndex("CREATE CUSTOM INDEX legacy_vector_idx ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Path dataDirectory = cfs.getDirectories().getDirectoryForNewSSTables().toPath();
        for (Path file : fixtureFiles())
            Files.copy(file, dataDirectory.resolve(file.getFileName()));
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).hasSize(1);

        float[][] vectors = vectors();
        for (int pk = 0; pk < ROWS; pk += 50)
        {
            UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ANN OF ? LIMIT 10", vector(vectors[pk]));
            assertThat(result).hasSize(10);
            assertThat(result.iterator().next().getInt("pk")).isEqualTo(pk);
        }

        Random random = new Random(7);
        for (int q = 0; q < 20; q++)
        {
            float[] query = randomVector(random);
            List<float[]> actual = new ArrayList<>();
            for (UntypedResultSet.Row row : execute("SELECT val FROM %s ORDER BY val ANN OF ? LIMIT 10", vector(query)))
                actual.add(TYPE.composeAsFloat(row.getBytes("val")));
            assertThat(recallMatch(exactNearest(vectors, query, 10), actual, 10)).isGreaterThanOrEqualTo(0.9);
        }

        // Unrestricted searches always walk the graph. Restricting the token range to part of the segment instead
        // brute-forces the rows in range or walks the graph filtered to them, depending on forceBruteForceQueries.
        List<Integer> byToken = IntStream.range(0, ROWS).boxed()
                                         .sorted(Comparator.comparingLong(pk -> token(cfs, pk)))
                                         .collect(Collectors.toList());
        // more rows than the LIMIT, so brute force ranks the rows by their compressed vectors and then reranks
        checkRestrictedSearches(cfs, vectors, byToken.subList(400, 700), random);
        // fewer rows than the LIMIT, so brute force scores every row exactly
        checkRestrictedSearches(cfs, vectors, byToken.subList(500, 508), random);

        // Rebuilding the index would rewrite these, so unchanged files mean the jvector 1.0.2 index was read.
        // Loading gives the sstable a new generation, so match the files by component name.
        for (Path file : fixtureFiles())
        {
            String name = file.getFileName().toString();
            if (name.contains("SAI"))
                assertThat(file(dataDirectory, name.substring(name.indexOf("SAI")))).hasSameBinaryContentAs(file);
        }
    }

    /**
     * Searches the rows with the given primary keys, which must be consecutive in token order, and checks the results
     * against an exact search of those rows.
     */
    private void checkRestrictedSearches(ColumnFamilyStore cfs, float[][] vectors, List<Integer> pks, Random random)
    {
        String select = "SELECT pk, val FROM %s WHERE token(pk) >= ? AND token(pk) <= ? ORDER BY val ANN OF ? LIMIT 10";
        long minToken = token(cfs, pks.get(0));
        long maxToken = token(cfs, pks.get(pks.size() - 1));
        float[][] inRange = pks.stream().map(pk -> vectors[pk]).toArray(float[][]::new);
        int expectedSize = Math.min(10, pks.size());

        for (int i = 0; i < pks.size(); i += Math.max(1, pks.size() / 10))
        {
            int pk = pks.get(i);
            UntypedResultSet result = execute(select, minToken, maxToken, vector(vectors[pk]));
            assertThat(result).hasSize(expectedSize);
            assertThat(result.iterator().next().getInt("pk")).isEqualTo(pk);
        }

        for (int q = 0; q < 10; q++)
        {
            float[] query = randomVector(random);
            List<float[]> actual = new ArrayList<>();
            for (UntypedResultSet.Row row : execute(select, minToken, maxToken, vector(query)))
            {
                assertThat(pks).contains(row.getInt("pk"));
                actual.add(TYPE.composeAsFloat(row.getBytes("val")));
            }
            assertThat(actual).hasSize(expectedSize);
            List<float[]> expected = exactNearest(inRange, query, expectedSize);
            if (pks.size() <= 10)
                assertThat(actual.stream().map(VectorLegacyFormatTest::asList))
                .containsExactlyElementsOf(expected.stream().map(VectorLegacyFormatTest::asList).collect(Collectors.toList()));
            else
                assertThat(recallMatch(expected, actual, expectedSize)).isGreaterThanOrEqualTo(0.9);
        }
    }

    private static long token(ColumnFamilyStore cfs, int pk)
    {
        return cfs.metadata().partitioner.getToken(Int32Type.instance.decompose(pk)).getLongValue();
    }

    @Test
    public void testFixtureHasJVector1Layout() throws IOException
    {
        assertThat(checkGraphLayout(fixtureComponent("TermsData"))).isEqualTo(2 * IndexWriterConfig.DEFAULT_MAXIMUM_NODE_CONNECTIONS);
        checkCompressedVectorsLayout(fixtureComponent("CompressedVectors"));
    }

    @Test
    public void testWritesJVector1Layout() throws Throwable
    {
        try (WithProperties ignore = new WithProperties().set(SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS, true))
        {
            createTable(TABLE);
            createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'maximum_node_connections': '12'}");
            disableCompaction();
            float[][] vectors = vectors();
            for (int pk = 0; pk < ROWS; pk++)
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk, vector(vectors[pk]));
            flush();

            Path directory = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next().descriptor.directory.toPath();
            assertThat(checkGraphLayout(component(directory, "TermsData"))).isEqualTo(OnHeapGraph.maxDegree(12)).isEqualTo(24);
            checkCompressedVectorsLayout(component(directory, "CompressedVectors"));
        }
    }

    /**
     * Checks that a graph has the jvector 1.0.2 layout: node count, dimension, entry node and maximum degree, then
     * for each node its ordinal, its vector, and its neighbors padded to the maximum degree with -1. Also checks that
     * the entry node is where jvector 1.0.2 moved it: no neighbor of it has a lower total similarity to its own
     * neighbors.
     *
     * @return the maximum degree
     */
    private static int checkGraphLayout(Path termsData) throws IOException
    {
        Set<List<Float>> expectedVectors = Stream.of(vectors()).map(VectorLegacyFormatTest::asList).collect(Collectors.toSet());
        ByteBuffer graph = segment(termsData);
        assertThat(graph.getInt()).isEqualTo(ROWS);
        assertThat(graph.getInt()).isEqualTo(DIMENSION);
        int entryNode = graph.getInt();
        assertThat(entryNode).isBetween(0, ROWS - 1);
        int maxDegree = graph.getInt();
        float[][] vectors = new float[ROWS][];
        int[][] neighbors = new int[ROWS][];
        for (int node = 0; node < ROWS; node++)
        {
            assertThat(graph.getInt()).isEqualTo(node);
            vectors[node] = new float[DIMENSION];
            for (int d = 0; d < DIMENSION; d++)
                vectors[node][d] = graph.getFloat();
            assertThat(expectedVectors).contains(asList(vectors[node]));

            neighbors[node] = new int[graph.getInt()];
            assertThat(neighbors[node].length).isBetween(1, maxDegree);
            for (int n = 0; n < maxDegree; n++)
            {
                int neighbor = graph.getInt();
                if (n < neighbors[node].length)
                    assertThat(neighbors[node][n] = neighbor).isBetween(0, ROWS - 1).isNotEqualTo(node);
                else
                    assertThat(neighbor).isEqualTo(-1);
            }
        }
        assertThat(graph.hasRemaining()).isFalse();

        // jvector 1.0.2 summed with DoubleStream.sum, which can round differently, so allow for near ties
        double entrySimilarity = neighborSimilarity(vectors, neighbors, entryNode);
        for (int neighbor : neighbors[entryNode])
            assertThat(neighborSimilarity(vectors, neighbors, neighbor)).isGreaterThanOrEqualTo(entrySimilarity - 1e-6);
        return maxDegree;
    }

    private static double neighborSimilarity(float[][] vectors, int[][] neighbors, int node)
    {
        double total = 0;
        for (int neighbor : neighbors[node])
            total += JVectorUtils.compare(VectorSimilarityFunction.COSINE, vectors[node], vectors[neighbor]);
        return total;
    }

    /**
     * Checks that compressed vectors have the jvector 1.0.2 PQ layout: a presence flag, the codebooks (global
     * centroid, subspace sizes, cluster count and centroids), then the vector count, subspace count and codes.
     */
    private static void checkCompressedVectorsLayout(Path compressedVectors) throws IOException
    {
        ByteBuffer pq = segment(compressedVectors);
        assertThat(pq.get()).isEqualTo((byte) 1);
        assertThat(pq.getInt()).isZero();
        int subspaces = pq.getInt();
        assertThat(subspaces).isEqualTo(DIMENSION / 2);
        int dimension = 0;
        for (int i = 0; i < subspaces; i++)
            dimension += pq.getInt();
        assertThat(dimension).isEqualTo(DIMENSION);
        assertThat(pq.getInt()).isEqualTo(CLUSTERS);
        pq.position(pq.position() + CLUSTERS * DIMENSION * Float.BYTES);
        assertThat(pq.getInt()).isEqualTo(ROWS);
        assertThat(pq.getInt()).isEqualTo(subspaces);
        pq.position(pq.position() + ROWS * subspaces);
        assertThat(pq.hasRemaining()).isFalse();
    }

    /**
     * @return the content of a single-segment SAI component, without its codec header and footer
     */
    private static ByteBuffer segment(Path component) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(component));
        assertThat(buffer.getInt()).isEqualTo(CodecUtil.CODEC_MAGIC);
        int versionLength = buffer.get();
        assertThat(versionLength).isPositive(); // a single-byte vint
        buffer.position(buffer.position() + versionLength);
        buffer.limit(buffer.capacity() - CodecUtil.footerLength());
        return buffer.slice();
    }

    private static List<Path> fixtureFiles() throws IOException
    {
        try (Stream<Path> files = Files.list(FIXTURE))
        {
            return files.collect(Collectors.toList());
        }
    }

    private static Path fixtureComponent(String name) throws IOException
    {
        return component(FIXTURE, name);
    }

    private static Path component(Path directory, String name) throws IOException
    {
        return file(directory, '+' + name + ".db");
    }

    private static Path file(Path directory, String suffix) throws IOException
    {
        try (Stream<Path> files = Files.list(directory))
        {
            List<Path> matches = files.filter(f -> f.getFileName().toString().endsWith(suffix)).collect(Collectors.toList());
            assertThat(matches).hasSize(1);
            return matches.get(0);
        }
    }

    private static List<float[]> exactNearest(float[][] vectors, float[] query, int k)
    {
        return Stream.of(vectors)
                     .sorted(Comparator.comparingDouble((float[] v) -> VectorSimilarityFunction.COSINE.compare(JVectorUtils.toVectorFloat(query), JVectorUtils.toVectorFloat(v))).reversed())
                     .limit(k)
                     .collect(Collectors.toList());
    }

    /**
     * @return the rows the fixture was written from, with each row's primary key as its index
     */
    private static float[][] vectors()
    {
        Random random = new Random(42);
        float[][] vectors = new float[ROWS][];
        for (int pk = 0; pk < ROWS; pk++)
            vectors[pk] = randomVector(random);
        return vectors;
    }

    private static float[] randomVector(Random random)
    {
        float[] vector = new float[DIMENSION];
        for (int d = 0; d < DIMENSION; d++)
            vector[d] = random.nextFloat() - 0.5f;
        return vector;
    }

    private static List<Float> asList(float[] vector)
    {
        return IntStream.range(0, vector.length).mapToObj(d -> vector[d]).collect(Collectors.toList());
    }
}
