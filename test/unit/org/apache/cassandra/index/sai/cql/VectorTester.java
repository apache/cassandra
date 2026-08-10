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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.ConcurrentVectorValues;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorTester extends SAITester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    @Before
    public void setup() throws Throwable
    {
        // override maxBruteForceRows to a random number between 0 and 4 so that we make sure
        // the non-brute-force path gets called during tests (which mostly involve small numbers of rows)
        var n = getRandom().nextIntBetween(0, 4);
        setMaxBruteForceRows(n);
        // override the global holes allowed so that the one-to-many path gets exercised
        V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 1.0;
    }

    public static void setMaxBruteForceRows(int n)
    {
        V2VectorIndexSearcher.GLOBAL_BRUTE_FORCE_ROWS = n;
        V2VectorIndexSearcher.BRUTE_FORCE_EXPENSE_FACTOR = 1.0;
        VectorMemtableIndex.GLOBAL_BRUTE_FORCE_ROWS = n;
    }

    public static double rawIndexedRecall(Collection<float[]> rawVectors, float[] rawQuery, List<float[]> result, int topK)
    {
        ConcurrentVectorValues vectorValues = new ConcurrentVectorValues(rawQuery.length);
        var q = vts.createFloatVector(rawQuery);
        int ordinal = 0;

        var graphBuilder = new GraphIndexBuilder(vectorValues,
                                                 VectorSimilarityFunction.COSINE,
                                                 16,
                                                 100,
                                                 1.2f,
                                                 1.4f,
                                                 false);

        for (float[] raw : rawVectors)
        {
            var v = vts.createFloatVector(raw);
            vectorValues.add(ordinal, v);
            graphBuilder.addGraphNode(ordinal++, v);
        }

        var results = GraphSearcher.search(q,
                                           topK,
                                           vectorValues,
                                           VectorSimilarityFunction.COSINE,
                                           graphBuilder.getGraph(),
                                           Bits.ALL);

        var nearestNeighbors = new ArrayList<float[]>();
        for (var ns : results.getNodes())
            nearestNeighbors.add(((ArrayVectorFloat) vectorValues.getVector(ns.node)).get());

        return recallMatch(nearestNeighbors, result, topK);
    }

    public static double recallMatch(List<float[]> expected, List<float[]> actual, int topK)
    {
        if (expected.isEmpty() && actual.isEmpty())
            return 1.0;

        int matches = 0;
        for (float[] in : expected)
        {
            for (float[] out : actual)
            {
                if (Arrays.compare(in, out) == 0)
                {
                    matches++;
                    break;
                }
            }
        }

        return (double) matches / topK;
    }

    protected void verifyChecksum() {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        cfs.indexManager.listIndexes().stream().forEach(index -> {
            try
            {
                var indexContext = (IndexContext) FieldUtils
                                                  .getDeclaredField(index.getClass(), "indexContext", true)
                                                  .get(index);
                logger.info("Verifying checksum for index {}", index.getIndexMetadata().name);
                boolean checksumValid = verifyChecksum(indexContext);
                assertThat(checksumValid).isTrue();
            } catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    public static double computeRecall(List<float[]> vectors, float[] query, List<float[]> result, VectorSimilarityFunction vsf)
    {
        List<float[]> sortedVectors = new ArrayList<>(vectors);
        sortedVectors.sort((a, b) -> Double.compare(vsf.compare(vts.createFloatVector(b), vts.createFloatVector(query)),
                                                    vsf.compare(vts.createFloatVector(a), vts.createFloatVector(query))));

        assertThat(sortedVectors).containsAll(result);

        List<float[]> nearestNeighbors = sortedVectors.subList(0, result.size());

        int matches = 0;
        for (float[] in : nearestNeighbors)
        {
            for (float[] out : result)
            {
                if (Arrays.compare(in, out) == 0)
                {
                    matches++;
                    break;
                }
            }
        }

        return matches * 1.0 / result.size();
    }

    protected String vectorString(float[] vector)
    {
        return Arrays.toString(vector);
    }

    /**
     * {@link VectorTester} parameterized for all {@link Version}s supporting vector indexes.
     */
    @Ignore
    @RunWith(Parameterized.class)
    abstract static class Versioned extends VectorTester
    {
        @Parameterized.Parameter
        public Version version;

        @Parameterized.Parameter(1)
        public boolean ENABLE_NVQ;

        @Parameterized.Parameter(2)
        public boolean ENABLE_FUSED;

        @Parameterized.Parameters(name = "version={0} enableNVQ={1} enableFused={2}")
        public static Collection<Object[]> data()
        {
            // See Version file for explanation of changes associated with each version
            return Version.ALL.stream()
                              .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST))
                              .flatMap(v -> {
                                  var enableNVQ = JVectorVersionUtil.versionSupportsNVQ(v)
                                                  ? new Boolean[]{ true, false }
                                                  : new Boolean[]{ false };
                                  // FA always uses FusedPQ regardless of the flag, so only test enableFused=true there.
                                  // FB+ allows toggling the flag, so test both values.
                                  // Pre-FA versions don't support FusedPQ at all.
                                  var enableFused = v.onOrAfter(Version.FB)
                                                    ? new Boolean[]{ true, false }
                                                    : JVectorVersionUtil.versionSupportsFused(v)
                                                      ? new Boolean[]{ true }   // FA: always-on
                                                      : new Boolean[]{ false };  // pre-FA: unsupported
                                  return Arrays.stream(enableNVQ).flatMap(nvq ->
                                      Arrays.stream(enableFused).map(fused -> new Object[]{ v, nvq, fused })
                                  );
                              }).collect(Collectors.toList());
        }

        @Before
        public void setCurrentVersion() throws Throwable
        {
            SAIUtil.setCurrentVersion(version);
        }

        @Before
        public void setEnableNVQ()
        {
            SAIUtil.setEnableNVQ(ENABLE_NVQ);
        }

        @Before
        public void setEnableFused()
        {
            SAIUtil.setEnableFused(ENABLE_FUSED);
        }
    }

    /**
     * {@link Versioned} that verifies checksums on flushing and compaction.
     */
    abstract static class VersionedWithChecksums extends Versioned
    {
        @Override
        public void flush()
        {
            super.flush();
            verifyChecksum();
        }

        @Override
        public void compact()
        {
            super.compact();
            verifyChecksum();
        }
    }
}
