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

package org.apache.cassandra.db.guardrails;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.primitives.Floats;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.junit.Assert.assertEquals;

/**
 * Tests the guardrails around the size of SAI vector terms
 *
 * @see Guardrails#saiVectorTermSize
 */
public class GuardrailSaiVectorTermSizeTest extends ValueThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailSaiVectorTermSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.saiVectorTermSize,
              Guardrails::setSaiVectorTermSizeThreshold,
              Guardrails::getSaiVectorTermSizeWarnThreshold,
              Guardrails::getSaiVectorTermSizeFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @Override
    protected int warnThreshold()
    {
        return WARN_THRESHOLD;
    }

    @Override
    protected int failThreshold()
    {
        return FAIL_THRESHOLD;
    }

    @Test
    public void testWarn() throws Throwable
    {
        int warnDimensions = warnThreshold() / 4; // 4 bytes per dimension
        List<Float> warnVector = Floats.asList(new float[warnDimensions + 1]);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v vector<float, " + warnVector.size() + ">)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, warnDimensions + 1);
        assertWarns(() -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", vectorType.decompose(warnVector)),
                    "Value of column 'v' has size");
    }

    @Test
    public void testFail() throws Throwable
    {
        int failDimensions = failThreshold() / 4; // 4 bytes per dimension
        List<Float> failVector = Floats.asList(new float[failDimensions + 1]);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v vector<float, " + failVector.size() + ">)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, failDimensions + 1);
        assertFails(() -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", vectorType.decompose(failVector)),
                    "Value of column 'v' has size");
    }

    @Test
    public void testWarningVectorOnBuild()
    {
        int warnDimensions = warnThreshold() / 4; // 4 bytes per dimension
        List<Float> largeVector = Floats.asList(new float[warnDimensions + 1]);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v vector<float, " + largeVector.size() + ">)");

        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, warnDimensions + 1);
        ByteBuffer vectorBytes = vectorType.decompose(largeVector);
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", vectorBytes);

        createIndex("CREATE INDEX ON %s(v) USING 'sai' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // verify that the large vector is written on initial index build
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s ORDER BY v ANN OF ? LIMIT 10", vectorBytes)).result.size(), 1);
    }

    @Test
    public void testFailingVectorOnBuild()
    {
        int failDimensions = failThreshold() / 4; // 4 bytes per dimension
        List<Float> oversizedVector = Floats.asList(new float[failDimensions + 1]);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v vector<float, " + oversizedVector.size() + ">)");

        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, failDimensions + 1);
        ByteBuffer vectorBytes = vectorType.decompose(oversizedVector);
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", vectorBytes);
        
        createIndex("CREATE INDEX ON %s(v) USING 'sai' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // verify that the oversized vector isn't written on initial index build
        assertEquals(((ResultMessage.Rows) execute("SELECT k, v FROM %s")).result.size(), 1);
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s ORDER BY v ANN OF ? LIMIT 10", vectorBytes)).result.size(), 0);
    }
}
