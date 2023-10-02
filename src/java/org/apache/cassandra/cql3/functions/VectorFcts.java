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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

public class VectorFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(createSimilarityFunctionFactory("similarity_cosine", VectorSimilarityFunction.COSINE, false));
        functions.add(createSimilarityFunctionFactory("similarity_euclidean", VectorSimilarityFunction.EUCLIDEAN, true));
        functions.add(createSimilarityFunctionFactory("similarity_dot_product", VectorSimilarityFunction.DOT_PRODUCT, true));
    }

    private static FunctionFactory createSimilarityFunctionFactory(String name,
                                                                   VectorSimilarityFunction vectorSimilarityFunction,
                                                                   boolean supportsZeroVectors)
    {
        return new FunctionFactory(name,
                                   FunctionParameter.sameAs(1, false, FunctionParameter.vector(CQL3Type.Native.FLOAT)),
                                   FunctionParameter.sameAs(0, false, FunctionParameter.vector(CQL3Type.Native.FLOAT)))
        {
            @Override
            @SuppressWarnings("unchecked")
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                // check that all arguments have the same vector dimensions
                VectorType<Float> firstArgType = (VectorType<Float>) argTypes.get(0);
                int dimensions = firstArgType.dimension;
                if (!argTypes.stream().allMatch(t -> ((VectorType<?>) t).dimension == dimensions))
                    throw new InvalidRequestException("All arguments must have the same vector dimensions");
                return createSimilarityFunction(name.name, firstArgType, vectorSimilarityFunction, supportsZeroVectors);
            }
        };
    }

    private static NativeFunction createSimilarityFunction(String name,
                                                           VectorType<Float> type,
                                                           VectorSimilarityFunction f,
                                                           boolean supportsZeroVectors)
    {
        return new NativeScalarFunction(name, FloatType.instance, type, type)
        {
            @Override
            public Arguments newArguments(ProtocolVersion version)
            {
                return new FunctionArguments(version,
                                             (v, b) -> type.composeAsFloat(b),
                                             (v, b) -> type.composeAsFloat(b));
            }

            @Override
            public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
            {
                if (arguments.containsNulls())
                    return null;

                float[] v1 = arguments.get(0);
                float[] v2 = arguments.get(1);

                if (!supportsZeroVectors)
                {
                    if (isAllZero(v1) || isAllZero(v2))
                        throw new InvalidRequestException("Function " + name + " doesn't support all-zero vectors.");
                }

                return FloatType.instance.decompose(f.compare(v1, v2));
            }

            private boolean isAllZero(float[] v)
            {
                for (float f : v)
                    if (f != 0)
                        return false;
                return true;
            }
        };
    }
}
