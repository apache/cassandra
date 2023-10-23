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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorSegmentationTest extends VectorTester
{
    private static final int dimension = 100;

    @Test
    public void testMultipleSegmentsForCreatingIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, " + dimension + ">, PRIMARY KEY(pk))");

        int vectorCount = 100;
        List<float[]> vectors = new ArrayList<>();
        for (int row = 0; row < vectorCount; row++)
        {
            float[] vector = nextVector();
            vectors.add(vector);
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", row, vector(vector));
        }

        flush();

        SegmentBuilder.updateLastValidSegmentRowId(17); // 17 rows per segment
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int limit = 35;
        float[] queryVector = nextVector();
        UntypedResultSet resultSet = execute("SELECT * FROM %s ORDER BY val ANN OF ? LIMIT " + limit, vector(queryVector));
        assertThat(resultSet.size()).isEqualTo(limit);

        List<float[]> resultVectors = getVectorsFromResult(resultSet);
        double recall = rawIndexedRecall(vectors, queryVector, resultVectors, limit);
        assertThat(recall).isGreaterThanOrEqualTo(0.9);
    }

    @Test
    public void testMultipleSegmentsForCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, " + dimension + ">, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        List<float[]> vectors = new ArrayList<>();
        int rowsPerSSTable = 10;
        int sstables = 5;
        int pk = 0;
        for (int i = 0; i < sstables; i++)
        {
            for (int row = 0; row < rowsPerSSTable; row++)
            {
                float[] vector = nextVector();
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk++, vector(vector));
                vectors.add(vector);
            }

            flush();
        }

        int limit = 30;
        float[] queryVector = nextVector();
        UntypedResultSet resultSet = execute("SELECT * FROM %s ORDER BY val ANN OF ? LIMIT " + limit, vector(queryVector));
        assertThat(resultSet.size()).isEqualTo(limit);

        List<float[]> resultVectors = getVectorsFromResult(resultSet);
        double recall = rawIndexedRecall(vectors, queryVector, resultVectors, limit);
        assertThat(recall).isGreaterThanOrEqualTo(0.99);


        SegmentBuilder.updateLastValidSegmentRowId(11); // 11 rows per segment
        compact();

        queryVector = nextVector();
        resultSet = execute("SELECT * FROM %s ORDER BY val ANN OF ? LIMIT " + limit, vector(queryVector));
        assertThat(resultSet.size()).isEqualTo(limit);

        resultVectors = getVectorsFromResult(resultSet);
        recall = rawIndexedRecall(vectors, queryVector, resultVectors, limit);
        assertThat(recall).isGreaterThanOrEqualTo(0.99);
    }

    private static float[] nextVector()
    {
        float[] rawVector = new float[dimension];
        for (int i = 0; i < dimension; i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return rawVector;
    }

    private static List<float[]> getVectorsFromResult(UntypedResultSet result)
    {
        List<float[]> vectors = new ArrayList<>();
        VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, dimension);

        // verify results are part of inserted vectors
        for (UntypedResultSet.Row row: result)
        {
            vectors.add(vectorType.composeAsFloat(row.getBytes("val")));
        }

        return vectors;
    }
}
