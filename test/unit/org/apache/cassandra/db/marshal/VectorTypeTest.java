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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class VectorTypeTest
{
    @Test
    public void composeAsFloat()
    {
        // dim
        // data
        Gen<Integer> dimGen = SourceDSL.integers().between(1, 100);
        Gen<Float> floatGen = SourceDSL.floats().any();
        Gen<Case> caseGen = rnd -> {
            int dim = dimGen.generate(rnd);
            float[] array = new float[dim];
            for (int i = 0; i < dim; i++)
                array[i] = floatGen.generate(rnd);
            return new Case(dim, array);
        };
        qt().forAll(caseGen).checkAssert(c -> {
            VectorType<Float> type = VectorType.getInstance(FloatType.instance, c.dim);
            ByteBuffer bb = type.decompose(c.box());
            assertThat(type.composeAsFloat(bb)).isEqualTo(c.values);
            assertThat(c.unbox(type.compose(bb))).isEqualTo(c.values);
            assertThat(type.decomposeAsFloat(c.values)).isEqualTo(bb);
        });
    }

    private static class Case
    {
        final int dim;
        final float[] values;

        private Case(int dim, float[] values)
        {
            this.dim = dim;
            this.values = values;
        }

        List<Float> box()
        {
            List<Float> list = new ArrayList<>(dim);
            for (int i = 0; i < dim; i++)
                list.add(values[i]);
            return list;
        }

        float[] unbox(List<Float> list)
        {
            assertThat(list).hasSize(dim);
            float[] array = new float[dim];
            for (int i = 0; i < dim; i++)
                array[i] = list.get(i);
            return array;
        }
    }
}