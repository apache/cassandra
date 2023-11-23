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

package org.apache.cassandra.cql3.functions;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(Parameterized.class)
public class VectorFctsTest extends CQLTester
{
    @Parameterized.Parameter
    public String function;

    @Parameterized.Parameter(1)
    public VectorSimilarityFunction luceneFunction;

    @Parameterized.Parameters(name = "{index}: function={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][]{
        { "system.similarity_cosine", VectorSimilarityFunction.COSINE },
        { "system.similarity_euclidean", VectorSimilarityFunction.EUCLIDEAN },
        { "system.similarity_dot_product", VectorSimilarityFunction.DOT_PRODUCT }
        });
    }

    @Test
    public void testVectorSimilarityFunction()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int PRIMARY KEY, value vector<float, 2>, " +
                              "l list<float>, " + // lists shouldn't be accepted by the functions
                              "fl frozen<list<float>>, " + // frozen lists shouldn't be accepted by the functions
                              "v1 vector<float, 1>, " + // 1-dimension vector to test missmatching dimensions
                              "v_int vector<int, 2>, " + // int vectors shouldn't be accepted by the functions
                              "v_double vector<double, 2>)");// double vectors shouldn't be accepted by the functions

        float[] values = new float[]{ 1f, 2f };
        CQLTester.Vector<Float> vector = vector(ArrayUtils.toObject(values));
        Object[] similarity = row(luceneFunction.compare(values, values));

        // basic functionality
        execute("INSERT INTO %s (pk, value, l, fl, v1, v_int, v_double) VALUES (0, ?, ?, ?, ?, ?, ?)",
                vector, list(1f, 2f), list(1f, 2f), vector(1f), vector(1, 2), vector(1d, 2d));
        assertRows(execute("SELECT " + function + "(value, value) FROM %s"), similarity);

        // literals
        assertRows(execute("SELECT " + function + "(value, [1, 2]) FROM %s"), similarity);
        assertRows(execute("SELECT " + function + "([1, 2], value) FROM %s"), similarity);
        assertRows(execute("SELECT " + function + "([1, 2], [1, 2]) FROM %s"), similarity);

        // bind markers
        assertRows(execute("SELECT " + function + "(value, ?) FROM %s", vector), similarity);
        assertRows(execute("SELECT " + function + "(?, value) FROM %s", vector), similarity);
        assertThatThrownBy(() -> execute("SELECT " + function + "(?, ?) FROM %s", vector, vector))
        .hasMessageContaining("Cannot infer type of argument ?");

        // bind markers with type hints
        assertRows(execute("SELECT " + function + "((vector<float, 2>) ?, ?) FROM %s", vector, vector), similarity);
        assertRows(execute("SELECT " + function + "(?, (vector<float, 2>) ?) FROM %s", vector, vector), similarity);
        assertRows(execute("SELECT " + function + "((vector<float, 2>) ?, (vector<float, 2>) ?) FROM %s", vector, vector), similarity);

        // bind markers and literals
        assertRows(execute("SELECT " + function + "([1, 2], ?) FROM %s", vector), similarity);
        assertRows(execute("SELECT " + function + "(?, [1, 2]) FROM %s", vector), similarity);
        assertRows(execute("SELECT " + function + "([1, 2], ?) FROM %s", vector), similarity);

        // wrong column types with columns
        assertThatThrownBy(() -> execute("SELECT " + function + "(l, value) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument l of type list<float>");
        assertThatThrownBy(() -> execute("SELECT " + function + "(fl, value) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument fl of type frozen<list<float>>");
        assertThatThrownBy(() -> execute("SELECT " + function + "(value, l) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument l of type list<float>");
        assertThatThrownBy(() -> execute("SELECT " + function + "(value, fl) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument fl of type frozen<list<float>>");

        // wrong column types with columns and literals
        assertThatThrownBy(() -> execute("SELECT " + function + "(l, [1, 2]) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument l of type list<float>");
        assertThatThrownBy(() -> execute("SELECT " + function + "(fl, [1, 2]) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument fl of type frozen<list<float>>");
        assertThatThrownBy(() -> execute("SELECT " + function + "([1, 2], l) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument l of type list<float>");
        assertThatThrownBy(() -> execute("SELECT " + function + "([1, 2], fl) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument fl of type frozen<list<float>>");

        // wrong column types with cast literals
        assertThatThrownBy(() -> execute("SELECT " + function + "((List<Float>)[1, 2], [3, 4]) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument (list<float>)[1, 2] of type frozen<list<float>>");
        assertThatThrownBy(() -> execute("SELECT " + function + "((List<Float>)[1, 2], (List<Float>)[3, 4]) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument (list<float>)[1, 2] of type frozen<list<float>>");
        assertThatThrownBy(() -> execute("SELECT " + function + "([1, 2], (List<Float>)[3, 4]) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument (list<float>)[3, 4] of type frozen<list<float>>");

        // wrong non-float vectors
        assertThatThrownBy(() -> execute("SELECT " + function + "(v_int, [1, 2]) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument v_int of type vector<int, 2>");
        assertThatThrownBy(() -> execute("SELECT " + function + "(v_double, [1, 2]) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument v_double of type vector<double, 2>");
        assertThatThrownBy(() -> execute("SELECT " + function + "([1, 2], v_int) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument v_int of type vector<int, 2>");
        assertThatThrownBy(() -> execute("SELECT " + function + "([1, 2], v_double) FROM %s"))
        .hasMessageContaining("Function " + function + " requires a float vector argument, but found argument v_double of type vector<double, 2>");

        // mismatching dimensions with literals
        assertThatThrownBy(() -> execute("SELECT " + function + "([1, 2], [3]) FROM %s", vector(1)))
        .hasMessageContaining("All arguments must have the same vector dimensions");
        assertThatThrownBy(() -> execute("SELECT " + function + "(value, [1]) FROM %s", vector(1)))
        .hasMessageContaining("All arguments must have the same vector dimensions");
        assertThatThrownBy(() -> execute("SELECT " + function + "([1], value) FROM %s", vector(1)))
        .hasMessageContaining("All arguments must have the same vector dimensions");

        // mismatching dimensions with bind markers
        assertThatThrownBy(() -> execute("SELECT " + function + "((vector<float, 1>) ?, value) FROM %s", vector(1)))
        .hasMessageContaining("All arguments must have the same vector dimensions");
        assertThatThrownBy(() -> execute("SELECT " + function + "(value, (vector<float, 1>) ?) FROM %s", vector(1)))
        .hasMessageContaining("All arguments must have the same vector dimensions");
        assertThatThrownBy(() -> execute("SELECT " + function + "((vector<float, 2>) ?, (vector<float, 1>) ?) FROM %s", vector(1, 2), vector(1)))
        .hasMessageContaining("All arguments must have the same vector dimensions");

        // mismatching dimensions with columns
        assertThatThrownBy(() -> execute("SELECT " + function + "(value, v1) FROM %s"))
        .hasMessageContaining("All arguments must have the same vector dimensions");
        assertThatThrownBy(() -> execute("SELECT " + function + "(v1, value) FROM %s"))
        .hasMessageContaining("All arguments must have the same vector dimensions");

        // null arguments with literals
        assertRows(execute("SELECT " + function + "(value, null) FROM %s"), row((Float) null));
        assertRows(execute("SELECT " + function + "(null, value) FROM %s"), row((Float) null));
        assertThatThrownBy(() -> execute("SELECT " + function + "(null, null) FROM %s"))
        .hasMessageContaining("Cannot infer type of argument NULL in call to function " + function);

        // null arguments with bind markers
        assertRows(execute("SELECT " + function + "(value, ?) FROM %s", (CQLTester.Vector<Float>) null), row((Float) null));
        assertRows(execute("SELECT " + function + "(?, value) FROM %s", (CQLTester.Vector<Float>) null), row((Float) null));
        assertThatThrownBy(() -> execute("SELECT " + function + "(?, ?) FROM %s", null, null))
        .hasMessageContaining("Cannot infer type of argument ? in call to function " + function);

        // test all-zero vectors, only cosine similarity should reject them
        if (luceneFunction == VectorSimilarityFunction.COSINE)
        {
            String expected = "Function " + function + " doesn't support all-zero vectors";
            assertThatThrownBy(() -> execute("SELECT " + function + "(value, [0, 0]) FROM %s")) .hasMessageContaining(expected);
            assertThatThrownBy(() -> execute("SELECT " + function + "([0, 0], value) FROM %s")).hasMessageContaining(expected);
        }
        else
        {
            float expected = luceneFunction.compare(values, new float[]{ 0, 0 });
            assertRows(execute("SELECT " + function + "(value, [0, 0]) FROM %s"), row(expected));
            assertRows(execute("SELECT " + function + "([0, 0], value) FROM %s"), row(expected));
        }

        // not-assignable element types
        assertThatThrownBy(() -> execute("SELECT " + function + "(value, ['a', 'b']) FROM %s WHERE pk=0"))
            .hasMessageContaining("Type error: ['a', 'b'] cannot be passed as argument 1");
        assertThatThrownBy(() -> execute("SELECT " + function + "(['a', 'b'], value) FROM %s WHERE pk=0"))
            .hasMessageContaining("Type error: ['a', 'b'] cannot be passed as argument 0");
        assertThatThrownBy(() -> execute("SELECT " + function + "(['a', 'b'], ['a', 'b']) FROM %s WHERE pk=0"))
            .hasMessageContaining("Type error: ['a', 'b'] cannot be passed as argument 0");
    }
}
